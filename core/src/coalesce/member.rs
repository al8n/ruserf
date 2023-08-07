use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use async_channel::Sender;
use either::Either;
use showbiz_core::{transport::Transport, NodeId};

use crate::{
  delegate::MergeDelegate,
  event::{Event, EventKind, MemberEvent, MemberEventType},
  Member,
};

use super::Coalescer;

pub(crate) struct CoalesceEvent {
  ty: MemberEventType,
  member: Arc<Member>,
}

#[derive(Default)]
pub(crate) struct MemberEventCoalescer<D, T> {
  last_events: HashMap<NodeId, MemberEventType>,
  latest_events: HashMap<NodeId, CoalesceEvent>,
  _m: PhantomData<(D, T)>,
}

impl<D, T> MemberEventCoalescer<D, T> {
  pub(crate) fn new() -> Self {
    Self {
      last_events: HashMap::new(),
      latest_events: HashMap::new(),
      _m: PhantomData,
    }
  }
}

#[showbiz_core::async_trait::async_trait]
impl<D, T> Coalescer for MemberEventCoalescer<D, T>
where
  D: MergeDelegate,
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
            .insert(member.id().clone(), CoalesceEvent { ty, member });
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
            member.id().clone(),
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
    let mut events: HashMap<MemberEventType, MemberEvent> =
      HashMap::with_capacity(self.latest_events.len());
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
                members: vec![cev.member],
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
