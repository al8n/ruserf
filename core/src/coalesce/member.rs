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
  pub(super) ty: MemberEventType,
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

#[cfg(test)]
mod tests {
  use std::{net::SocketAddr, time::Duration};

  use agnostic_lite::{tokio::TokioRuntime, RuntimeLite};
  use futures::FutureExt;
use memberlist_core::transport::{
    resolver::socket_addr::SocketAddrResolver, tests::UnimplementedTransport, Lpe,
  };
  use ruserf_types::{MemberStatus, UserEventMessage};
  use smol_str::SmolStr;

  use crate::{coalesce::coalesced_event, event::EventType, DefaultDelegate};

  use super::*;

  type Transport = UnimplementedTransport<
    SmolStr,
    SocketAddrResolver<TokioRuntime>,
    Lpe<SmolStr, SocketAddr>,
    TokioRuntime,
  >;

  type Delegate = DefaultDelegate<Transport>;

  #[tokio::test]
  async fn test_member_event_coealesce_basic() {
    let (tx, rx) = async_channel::unbounded();
    let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let coalescer = MemberEventCoalescer::<Transport, Delegate>::new();

    let in_ = coalesced_event(tx, shutdown_rx, Duration::from_millis(5), Duration::from_millis(5), coalescer);

    let send = vec![
      MemberEvent {
        ty: MemberEventType::Join,
        members: TinyVec::from(Member::new(Node::new("foo".into(), "127.0.0.1:8080".parse().unwrap()), Default::default(), MemberStatus::None)),
      },
      MemberEvent {
        ty: MemberEventType::Leave,
        members: TinyVec::from(Member::new(Node::new("foo".into(), "127.0.0.1:8080".parse().unwrap()), Default::default(), MemberStatus::None)),
      },
      MemberEvent {
        ty: MemberEventType::Leave,
        members: TinyVec::from(Member::new(Node::new("bar".into(), "127.0.0.1:8080".parse().unwrap()), Default::default(), MemberStatus::None)),
      },
      MemberEvent {
        ty: MemberEventType::Update,
        members: TinyVec::from(Member::new(Node::new("zip".into(), "127.0.0.1:8080".parse().unwrap()), [("role", "foo")].into_iter().collect(), MemberStatus::None)),
      },
      MemberEvent {
        ty: MemberEventType::Update,
        members: TinyVec::from(Member::new(Node::new("zip".into(), "127.0.0.1:8080".parse().unwrap()), [("role", "bar")].into_iter().collect(), MemberStatus::None)),
      },
      MemberEvent {
        ty: MemberEventType::Reap,
        members: TinyVec::from(Member::new(Node::new("dead".into(), "127.0.0.1:8080".parse().unwrap()), Default::default(), MemberStatus::None)),
      },
    ];

    for event in send {
      in_.send(Event::from(event)).await.unwrap();
    }

    let mut events = HashMap::new();
    let timeout = TokioRuntime::sleep(Duration::from_millis(10));
    futures::pin_mut!(timeout);


      loop {
        futures::select! {
          e = rx.recv().fuse() => {
            let e = e.unwrap();
            events.insert(e.ty(), e.clone());
          }
          _ = (&mut timeout).fuse() => {
            break;
          },
        }
      }

      assert_eq!(events.len(), 3);

      match events.get(&EventType::Member(MemberEventType::Leave)) {
        None => panic!(""),
        Some(e) => {
          match e.kind() {
            EventKind::Member(MemberEvent { members, .. }) => {
              assert_eq!(members.len(), 2);

              let expected = ["bar", "foo"];
              let mut names = [members[0].node.id().clone(), members[1].node.id().clone()];
              names.sort();

              assert_eq!(names, expected);
            }
            _ => panic!(""),
          }
        },
      }

      match events.get(&EventType::Member(MemberEventType::Update)) {
        None => panic!(""),
        Some(e) => {
          match e.kind() {
            EventKind::Member(MemberEvent { members, .. }) => {
              assert_eq!(members.len(), 1);
              assert_eq!(members[0].node.id(), "zip");
              assert_eq!(members[0].tags().get("role").unwrap(), "bar");
            }
            _ => panic!(""),
          }
        },
      }

      match events.get(&EventType::Member(MemberEventType::Reap)) {
        None => panic!(""),
        Some(e) => {
          match e.kind() {
            EventKind::Member(MemberEvent { members, .. }) => {
              assert_eq!(members.len(), 1);
              assert_eq!(members[0].node.id(), "dead");
            }
            _ => panic!(""),
          }
        },
      }
  }

  #[tokio::test]
  async fn test_member_event_coalesce_tag_update() {
    let (tx, rx) = async_channel::unbounded();
    let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let coalescer = MemberEventCoalescer::<Transport, Delegate>::new();

    let in_ = coalesced_event(tx, shutdown_rx, Duration::from_millis(5), Duration::from_millis(5), coalescer);

    in_.send(Event::from(MemberEvent {
      ty: MemberEventType::Update,
      members: TinyVec::from(Member::new(Node::new("foo".into(), "127.0.0.1:8080".parse().unwrap()), [("role", "foo")].into_iter().collect(), MemberStatus::None)),
    })).await.unwrap();

    TokioRuntime::sleep(Duration::from_millis(30)).await;

    futures::select! {
      e = rx.recv().fuse() => {
        let e = e.unwrap();

        match e.kind() {
          EventKind::Member(MemberEvent { ty, .. }) => {
            assert!(matches!(ty, MemberEventType::Update));
          }
          _ => panic!("expected update"),
        }
      }
      default => panic!("expected update"),
    }

    // Second update should not be suppressed even though
	  // last event was an update
    in_.send(Event::from(MemberEvent {
      ty: MemberEventType::Update,
      members: TinyVec::from(Member::new(Node::new("foo".into(), "127.0.0.1:8080".parse().unwrap()), [("role", "bar")].into_iter().collect(), MemberStatus::None)),
    })).await.unwrap();
    TokioRuntime::sleep(Duration::from_millis(10)).await; 

    futures::select! {
      e = rx.recv().fuse() => {
        let e = e.unwrap();

        match e.kind() {
          EventKind::Member(MemberEvent { ty, .. }) => {
            assert!(matches!(ty, MemberEventType::Update));
          }
          _ => panic!("expected update"),
        }
      }
      default => panic!("expected update"),
    }
  }

  #[test]
  fn test_member_event_coalesce_pass_through() {
    let cases = [
      (Event::from(UserEventMessage::default()), false),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Join,
          members: TinyVec::new(),
        }),
        true,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Leave,
          members: TinyVec::new(),
        }),
        true,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Failed,
          members: TinyVec::new(),
        }),
        true,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Update,
          members: TinyVec::new(),
        }),
        true,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Reap,
          members: TinyVec::new(),
        }),
        true,
      ),
    ];

    for (event, handle) in cases.iter() {
      let coalescer = MemberEventCoalescer::<Transport, Delegate>::new();
      assert_eq!(coalescer.handle(event), *handle);
    }
  }
}
