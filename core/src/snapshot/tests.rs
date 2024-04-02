use std::net::SocketAddr;

use memberlist_core::{
  agnostic_lite::tokio::TokioRuntime,
  transport::{resolver::socket_addr::SocketAddrResolver, tests::UnimplementedTransport, Lpe},
};
use ruserf_types::{Member, MemberStatus};
use smol_str::SmolStr;

use crate::{event::QueryEvent, DefaultDelegate, SNAPSHOT_SIZE_LIMIT};

use super::*;

type Transport = UnimplementedTransport<
  SmolStr,
  SocketAddrResolver<TokioRuntime>,
  Lpe<SmolStr, SocketAddr>,
  TokioRuntime,
>;

type Delegate = DefaultDelegate<Transport>;

#[tokio::test]
async fn test_snapshotter() {
  let dir = tempfile::tempdir().unwrap();

  let clock = LamportClock::new();
  let (out_tx, out_rx) = async_channel::bounded(64);
  let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let res = open_and_replay_snapshot::<_, _, Delegate, _>(&dir, false).unwrap();
  let (event_tx, _, handle) = Snapshot::<Transport, Delegate>::from_replay_result(
    res,
    SNAPSHOT_SIZE_LIMIT,
    false,
    clock.clone(),
    Some(out_tx),
    shutdown_rx.clone(),
    #[cfg(feature = "metrics")]
    Default::default(),
  )
  .unwrap();

  // Write some user events
  let ue = UserEventMessage::default()
    .with_ltime(42.into())
    .with_name("bar".into());
  event_tx.send(ue.clone().into()).await.unwrap();

  // Write some queries
  let qe = QueryEvent {
    ltime: 50.into(),
    name: "bar".into(),
    payload: Default::default(),
    ctx: todo!(),
    id: 0,
    from: Node::new("baz".into(), "127.0.0.1:8000".parse().unwrap()),
    relay_factor: 0,
  };
  event_tx.send(qe.clone().into()).await.unwrap();

  // Write some membership events
  clock.witness(100.into());

  let mejoin = MemberEvent {
    ty: MemberEventType::Join,
    members: TinyVec::from(Member::new(
      Node::new("foo".into(), "127.0.0.1:5000".parse().unwrap()),
      Default::default(),
      MemberStatus::None,
    )),
  };

  let mefail = MemberEvent {
    ty: MemberEventType::Failed,
    members: TinyVec::from(Member::new(
      Node::new("foo".into(), "127.0.0.1:5000".parse().unwrap()),
      Default::default(),
      MemberStatus::None,
    )),
  };

  event_tx.send(mejoin.clone().into()).await.unwrap();
  event_tx.send(mefail.clone().into()).await.unwrap();
  event_tx.send(mejoin.clone().into()).await.unwrap();

  // Check these get passed through
  futures::select! {
    e = out_rx.recv().fuse() => {
      let e = e.unwrap();
      match e.kind() {
        EventKind::User(e) => {
          assert_eq!(e, &ue);
        },
        _ => panic!("expected user event"),
      }
    },
    _ = TokioRuntime::sleep(Duration::from_millis(200)).fuse() => {
      panic!("timeout");
    }
  }

  futures::select! {
    e = out_rx.recv().fuse() => {
      let e = e.unwrap();
      match e.kind() {
        EventKind::Query(e) => {
          if qe.ne(e) {
            panic!("expected query event mismatch");
          }
        },
        _ => panic!("expected query event"),
      }
    },
    _ = TokioRuntime::sleep(Duration::from_millis(200)).fuse() => {
      panic!("timeout");
    }
  }

  futures::select! {
    e = out_rx.recv().fuse() => {
      let e = e.unwrap();
      match e.kind() {
        EventKind::Member(e) => {
          assert_eq!(e, &mejoin);
        },
        _ => panic!("expected member event"),
      }
    },
    _ = TokioRuntime::sleep(Duration::from_millis(200)).fuse() => {
      panic!("timeout");
    }
  }

  futures::select! {
    e = out_rx.recv().fuse() => {
      let e = e.unwrap();
      match e.kind() {
        EventKind::Member(e) => {
          assert_eq!(e, &mefail);
        },
        _ => panic!("expected member event"),
      }
    },
    _ = TokioRuntime::sleep(Duration::from_millis(200)).fuse() => {
      panic!("timeout");
    }
  }

  futures::select! {
    e = out_rx.recv().fuse() => {
      let e = e.unwrap();
      match e.kind() {
        EventKind::Member(e) => {
          assert_eq!(e, &mejoin);
        },
        _ => panic!("expected member event"),
      }
    },
    _ = TokioRuntime::sleep(Duration::from_millis(200)).fuse() => {
      panic!("timeout");
    }
  }

  // Close the snapshoter
  shutdown_tx.close();
  handle.wait().await;

  // Open the snapshoter
  let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let res = open_and_replay_snapshot::<_, _, Delegate, _>(&dir, false).unwrap();

  assert_eq!(res.last_clock, 100.into());
  assert_eq!(res.last_event_clock, 42.into());
  assert_eq!(res.last_query_clock, 50.into());

  let (out_tx, _out_rx) = async_channel::bounded(64);
  let (_event_tx, alive_nodes, handle) = Snapshot::<Transport, Delegate>::from_replay_result(
    res,
    SNAPSHOT_SIZE_LIMIT,
    false,
    clock.clone(),
    Some(out_tx),
    shutdown_rx.clone(),
    #[cfg(feature = "metrics")]
    Default::default(),
  )
  .unwrap();

  assert_eq!(alive_nodes.len(), 1);
  let n = &alive_nodes[0];
  assert_eq!(n.id(), "foo");
  assert_eq!(n.address().into_resolved().unwrap(), "127.0.0.1:5000".parse().unwrap());

  // Close the snapshoter
  shutdown_tx.close();
  handle.wait().await;

  // Open the snapshotter, make sure nothing dies reading with coordinates
	// disabled.
  let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let res = open_and_replay_snapshot::<_, _, Delegate, _>(&dir, false).unwrap();

  let (out_tx, _out_rx) = async_channel::bounded(64);
  let (_event_tx, _, handle) = Snapshot::<Transport, Delegate>::from_replay_result(
    res,
    SNAPSHOT_SIZE_LIMIT,
    false,
    clock.clone(),
    Some(out_tx),
    shutdown_rx.clone(),
    #[cfg(feature = "metrics")]
    Default::default(),
  )
  .unwrap();
  shutdown_tx.close();
  handle.wait().await;
}
