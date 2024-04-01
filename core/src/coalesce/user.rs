use std::marker::PhantomData;

use indexmap::IndexMap;
use memberlist::types::TinyVec;
use ruserf_types::UserEventMessage;
use smol_str::SmolStr;

use crate::{event::EventKind, types::LamportTime};

use super::*;

struct LatestUserEvents {
  ltime: LamportTime,
  events: TinyVec<UserEventMessage>,
}

#[derive(Default)]
#[repr(transparent)]
pub(crate) struct UserEventCoalescer<T, D> {
  events: IndexMap<SmolStr, LatestUserEvents>,
  _m: PhantomData<(D, T)>,
}

impl<T, D> UserEventCoalescer<T, D> {
  pub(crate) fn new() -> Self {
    Self {
      events: IndexMap::new(),
      _m: PhantomData,
    }
  }
}

impl<T, D> Coalescer for UserEventCoalescer<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Delegate = D;
  type Transport = T;

  fn name(&self) -> &'static str {
    "user_event_coalescer"
  }

  fn handle(&self, event: &Event<Self::Transport, Self::Delegate>) -> bool {
    match event.kind() {
      EventKind::User(e) => e.cc(),
      _ => false,
    }
  }

  fn coalesce(&mut self, event: Event<Self::Transport, Self::Delegate>) {
    let event = match event.kind() {
      EventKind::User(e) => e.clone(),
      _ => unreachable!(),
    };

    let ltime = event.ltime();
    match self.events.get_mut(event.name()) {
      None => {
        self.events.insert(
          event.name().clone(),
          LatestUserEvents {
            ltime,
            events: TinyVec::from(event),
          },
        );
      }
      Some(latest) => {
        if latest.ltime < ltime {
          latest.events.clear();
          latest.ltime = ltime;
          latest.events.push(event);
          return;
        }

        // If the the same age, save it
        if latest.ltime == ltime {
          latest.events.push(event);
        }
      }
    }
  }

  async fn flush(
    &mut self,
    out_tx: &Sender<Event<Self::Transport, Self::Delegate>>,
  ) -> Result<(), super::ClosedOutChannel> {
    for (_, latest) in self.events.drain(..) {
      for event in latest.events {
        if out_tx.send(Event::from(event)).await.is_err() {
          return Err(super::ClosedOutChannel);
        }
      }
    }
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use agnostic_lite::tokio::TokioRuntime;
  use memberlist::transport::{
    resolver::socket_addr::SocketAddrResolver, tests::UnimplementedTransport, Lpe,
  };

  use crate::{
    event::{MemberEvent, MemberEventType},
    DefaultDelegate,
  };

  use super::*;

  type Transport = UnimplementedTransport<
    SmolStr,
    SocketAddrResolver<TokioRuntime>,
    Lpe<SmolStr, SocketAddr>,
    TokioRuntime,
  >;

  type Delegate = DefaultDelegate<Transport>;

  #[tokio::test]
  async fn test_user_event_coalesce_basic() {
    let (tx, rx) = async_channel::unbounded();
    let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
    let coalescer = UserEventCoalescer::<Transport, Delegate>::new();

    let in_ = coalesced_event(
      tx,
      shutdown_rx,
      Duration::from_millis(5),
      Duration::from_millis(5),
      coalescer,
    );

    let send = vec![
      UserEventMessage::default()
        .with_name("foo".into())
        .with_cc(true)
        .with_ltime(1.into()),
      UserEventMessage::default()
        .with_name("foo".into())
        .with_cc(true)
        .with_ltime(2.into()),
      UserEventMessage::default()
        .with_name("bar".into())
        .with_cc(true)
        .with_ltime(2.into())
        .with_payload("test1".into()),
      UserEventMessage::default()
        .with_name("bar".into())
        .with_cc(true)
        .with_ltime(2.into())
        .with_payload("test2".into()),
    ];

    for event in send {
      in_.send(Event::from(event)).await.unwrap();
    }

    let mut got_foo = false;
    let mut got_bar1 = false;
    let mut got_bar2 = false;

    loop {
      futures::select! {
        _ = TokioRuntime::sleep(Duration::from_millis(10)).fuse() => break,
        event = rx.recv().fuse() => {
          let event = event.unwrap();
          match event.kind() {
            EventKind::User(e) => {
              match e.name().as_str() {
                "foo" => {
                  assert_eq!(e.ltime(), 2.into(), "bad ltime for foo");
                  got_foo = true;
                }
                "bar" => {
                  assert_eq!(e.ltime(), 2.into(), "bad ltime for bar");
                  if e.payload().eq("test1".as_bytes()) {
                    got_bar1 = true;
                  }

                  if e.payload().eq("test2".as_bytes()) {
                    got_bar2 = true;
                  }
                }
                _ => unreachable!(),
              }
            }
            _ => unreachable!(),
          }
        }
      }
    }

    assert!(got_foo && got_bar1 && got_bar2, "missing events");
  }

  #[test]
  fn test_user_event_coalesce_pass_through() {
    let cases = [
      (Event::from(UserEventMessage::default()), false),
      (Event::from(UserEventMessage::default().with_cc(true)), true),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Join,
          members: TinyVec::new(),
        }),
        false,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Leave,
          members: TinyVec::new(),
        }),
        false,
      ),
      (
        Event::from(MemberEvent {
          ty: MemberEventType::Failed,
          members: TinyVec::new(),
        }),
        false,
      ),
    ];

    let coalescer = UserEventCoalescer::<Transport, Delegate>::new();

    for (idx, (event, should_coalesce)) in cases.iter().enumerate() {
      assert_eq!(coalescer.handle(event), *should_coalesce, "bad: {idx}");
    }
  }
}
