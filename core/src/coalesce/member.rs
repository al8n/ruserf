use std::{collections::HashMap, marker::PhantomData};

use async_channel::Sender;
use either::Either;
use memberlist_core::{
  transport::{AddressResolver, Node, Transport},
  types::TinyVec,
  CheapClone,
};

use crate::{
  delegate::Delegate,
  event::{Event, EventKind, MemberEvent, MemberEventType},
  types::Member,
};

use super::Coalescer;

pub(crate) struct CoalesceEvent<I, A> {
  ty: MemberEventType,
  member: Member<I, A>,
}

#[derive(Default)]
pub(crate) struct MemberEventCoalescer<T: Transport, D> {
  last_events:
    HashMap<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, MemberEventType>,
  latest_events: HashMap<
    Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    CoalesceEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  >,
  _m: PhantomData<D>,
}

impl<T: Transport, D> MemberEventCoalescer<T, D> {
  pub(crate) fn new() -> Self {
    Self {
      last_events: HashMap::new(),
      latest_events: HashMap::new(),
      _m: PhantomData,
    }
  }
}

impl<T, D> Coalescer for MemberEventCoalescer<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Delegate = D;
  type Transport = T;

  fn name(&self) -> &'static str {
    "member_event_coalescer"
  }

  fn handle(&self, event: &Event<Self::Transport, Self::Delegate>) -> bool {
    match &event.0 {
      Either::Left(e) => matches!(e, EventKind::Member(_)),
      Either::Right(e) => matches!(&**e, EventKind::Member(_)),
    }
  }

  fn coalesce(&mut self, event: Event<Self::Transport, Self::Delegate>) {
    match event.0 {
      Either::Left(ev) => {
        let EventKind::Member(event) = ev else {
          unreachable!();
        };

        let (ty, members) = event.into();
        for member in members {
          self
            .latest_events
            .insert(member.node().cheap_clone(), CoalesceEvent { ty, member });
        }
      }
      Either::Right(ev) => {
        let event = match &*ev {
          EventKind::Member(ev) => ev,
          _ => unreachable!(),
        };

        let ty = event.ty();
        for member in event.members() {
          self.latest_events.insert(
            member.node().cheap_clone(),
            CoalesceEvent {
              ty,
              member: member.clone(),
            },
          );
        }
      }
    }
  }

  async fn flush(
    &mut self,
    out_tx: &Sender<Event<Self::Transport, Self::Delegate>>,
  ) -> Result<(), super::ClosedOutChannel> {
    let mut events: HashMap<
      MemberEventType,
      MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    > = HashMap::with_capacity(self.latest_events.len());
    // Coalesce the various events we got into a single set of events.
    for (id, cev) in self.latest_events.drain() {
      match self.last_events.get(&id) {
        Some(&previous) if previous == cev.ty && cev.ty != MemberEventType::Update => {
          continue;
        }
        Some(_) | None => {
          // Update our last event
          self.last_events.insert(id, cev.ty);

          // Add it to our event
          match events.entry(cev.ty) {
            std::collections::hash_map::Entry::Occupied(mut ent) => {
              ent.get_mut().members.push(cev.member);
            }
            std::collections::hash_map::Entry::Vacant(ent) => {
              ent.insert(MemberEvent {
                ty: cev.ty,
                members: TinyVec::from(cev.member),
              });
            }
          }
        }
      }
    }

    // Send out those events
    for event in events.into_values() {
      if out_tx.send(Event::from(event)).await.is_err() {
        return Err(super::ClosedOutChannel);
      }
    }
    Ok(())
  }
}
