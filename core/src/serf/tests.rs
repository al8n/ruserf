use std::time::{Duration, Instant};

use futures::FutureExt;
use memberlist::{
  agnostic_lite::RuntimeLite, bytes::Bytes, delegate::NodeDelegate, transport::MaybeResolvedAddress, types::TinyVec
};
use ruserf_types::{MessageType, Node, QueryFlag, QueryMessage, SerfMessage, UserEventMessage};
use smol_str::SmolStr;

use crate::{
  delegate::TransformDelegate,
  event::{InternalQueryEvent, MemberEvent, MemberEventType},
};

use self::internal_query::SerfQueries;

use super::*;

fn test_config() -> Options {
  let mut opts = Options::new();
  opts.memberlist_options = opts
    .memberlist_options
    .with_gossip_interval(Duration::from_millis(5))
    .with_probe_interval(Duration::from_millis(50))
    .with_probe_timeout(Duration::from_millis(25))
    .with_timeout(Duration::from_millis(100))
    .with_suspicion_mult(1);
  opts
    .with_reap_interval(Duration::from_secs(1))
    .with_reconnect_interval(Duration::from_millis(100))
    .with_reconnect_timeout(Duration::from_micros(1))
    .with_tombstone_timeout(Duration::from_micros(1))
}

async fn wait_until_num_nodes<T, D>(desired_nodes: usize, serfs: &[Serf<T, D>])
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  let start = Instant::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    for (idx, s) in serfs.iter().enumerate() {
      let n = s.num_nodes().await;
      if n == desired_nodes {
        continue;
      }

      if start.elapsed() > Duration::from_secs(7) {
        panic!("s{} got {} expected {}", idx + 1, n, desired_nodes);
      }
    }
  }
}

/// Unit test for queries pass through functionality
pub async fn queries_pass_through<T>(s: Serf<T>)
where
  T: Transport,
{
  let (tx, rx) = async_channel::bounded(4);
  let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let event_tx = SerfQueries::<T, DefaultDelegate<T>>::new(tx, shutdown_rx);

  // Push a user event
  let event = Event::from(
    UserEventMessage::default()
      .with_name("foo".into())
      .with_ltime(42.into()),
  );
  event_tx.send(event.clone()).await.unwrap();

  // Push a query
  let query = s.query_event(QueryMessage {
    ltime: 42.into(),
    id: 1,
    from: s.memberlist().advertise_node(),
    filters: TinyVec::new(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "foo".into(),
    payload: Bytes::new(),
  });
  event_tx.send(Event::from(query)).await.unwrap();

  // Push a member event
  let event = Event::from(MemberEvent {
    ty: MemberEventType::Join,
    members: TinyVec::new(),
  });
  event_tx.send(event).await.unwrap();

  // Should get passed through
  for _ in 0..3 {
    let sleep = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(100));
    futures::select! {
      _ = rx.recv().fuse() => {},
      _ = sleep.fuse() => panic!("timeout"),
    }
  }
}

/// Unit test for queries ping functionality
pub async fn queries_ping<T>(s: Serf<T>)
where
  T: Transport,
{
  let (tx, rx) = async_channel::bounded(4);
  let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let event_tx = SerfQueries::<T, DefaultDelegate<T>>::new(tx, shutdown_rx);

  // Push a query
  let query = s.query_event(QueryMessage {
    ltime: 42.into(),
    id: 1,
    from: s.memberlist().advertise_node(),
    filters: TinyVec::new(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "ping".into(),
    payload: Bytes::new(),
  });
  event_tx
    .send(Event::from((InternalQueryEvent::Ping, query)))
    .await
    .unwrap();

  let sleep = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(50));
  futures::select! {
    _ = rx.recv().fuse() =>  panic!("should not passthrough query!"),
    _ = sleep.fuse() => {},
  }
}

/// Unit test for queries conflict functionality
pub async fn queries_conflict_same_name<T>(s: Serf<T>)
where
  T: Transport,
{
  let (tx, rx) = async_channel::bounded(4);
  let (_shutdown_tx, shutdown_rx) = async_channel::bounded(1);
  let event_tx = SerfQueries::<T, DefaultDelegate<T>>::new(tx, shutdown_rx);

  // Push a query
  let query = s.query_event(QueryMessage {
    ltime: 42.into(),
    id: 1,
    from: s.memberlist().advertise_node(),
    filters: TinyVec::new(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "conflict".into(),
    payload: Bytes::new(),
  });
  let id = s.memberlist().local_id().clone();
  event_tx
    .send(Event::from((InternalQueryEvent::Conflict(id), query)))
    .await
    .unwrap();

  let sleep = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(50));
  futures::select! {
    _ = rx.recv().fuse() =>  panic!("should not passthrough query!"),
    _ = sleep.fuse() => {},
  }
}

/// Unit test for queries list key response functionality.
///
/// This test requires the transport to support encryption.
#[cfg(feature = "encryption")]
pub async fn estimate_max_keys_in_list_key_response_factor<T>(
  transport_opts: T::Options,
  opts: Options,
) where
  T: Transport,
{
  use memberlist::types::SecretKey;
  use ruserf_types::KeyResponseMessage;

  let size_limit = opts.query_response_size_limit() * 10;
  let opts = opts.with_query_response_size_limit(size_limit);
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();
  let query = s.query_event(QueryMessage {
    ltime: 0.into(),
    id: 0,
    from: s.memberlist().advertise_node(),
    filters: TinyVec::new(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: Default::default(),
    payload: Default::default(),
  });

  let mut resp = KeyResponseMessage::default();
  for _ in 0..=(size_limit / 25) {
    resp.keys.push(SecretKey::from([1; 16]));
  }

  let mut found = 0;
  for i in (0..=resp.keys.len()).rev() {
    let encoded_len = <DefaultDelegate<T> as TransformDelegate>::message_encoded_len(&resp);
    let mut dst = vec![0; encoded_len];
    <DefaultDelegate<T> as TransformDelegate>::encode_message(&resp, &mut dst).unwrap();

    let qresp = query.create_response(dst.into());
    let encoded_len = <DefaultDelegate<T> as TransformDelegate>::message_encoded_len(&qresp);
    let mut dst = vec![0; encoded_len];
    <DefaultDelegate<T> as TransformDelegate>::encode_message(&qresp, &mut dst).unwrap();

    if query.check_response_size(&dst).is_err() {
      resp.keys.truncate(i);
      continue;
    }
    found = i;
    break;
  }

  assert_ne!(found, 0, "Do not find anything!");

  println!(
    "max keys in response with {} bytes: {}",
    size_limit,
    resp.keys.len()
  );
  println!("factor: {}", size_limit / resp.keys.len());
}

/// Unit test for queries list key response functionality.
///
/// This test requires the transport to support encryption.
#[cfg(feature = "encryption")]
pub async fn key_list_key_response_with_correct_size<T>(transport_opts: T::Options, opts: Options)
where
  T: Transport,
{
  use memberlist::types::SecretKey;
  use ruserf_types::{Encodable, KeyResponseMessage};

  let opts = opts.with_query_response_size_limit(1024);
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();
  let query = s.query_event(QueryMessage {
    ltime: 0.into(),
    id: 0,
    from: s.memberlist().advertise_node(),
    filters: TinyVec::new(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: Default::default(),
    payload: Default::default(),
  });

  let k = [0; 16];
  let encoded_len = SecretKey::from(k).encoded_len();
  let cases = [
    (0, false, KeyResponseMessage::default()),
    (1, false, {
      let mut msg = KeyResponseMessage::default();
      msg.add_key(SecretKey::from(k));
      msg
    }),
    // has 50 keys which makes the response bigger than 1024 bytes.
    (50, true, {
      let mut msg = KeyResponseMessage::default();
      for _ in 0..50 {
        msg.add_key(SecretKey::from(k));
      }
      msg
    }),
    // this test when the list of keys length is less than the max allowed, in this test case 1024/encoded_len
    (encoded_len, true, {
      let mut msg = KeyResponseMessage::default();
      for _ in 0..encoded_len - 2 {
        msg.add_key(SecretKey::from(k));
      }
      msg
    }),
    // this test when the list of keys length is equal the max allowed, in this test case 1024/25 = 40
    (encoded_len, true, {
      let mut msg = KeyResponseMessage::default();
      for _ in 0..encoded_len {
        msg.add_key(SecretKey::from(k));
      }
      msg
    }),
    // this test when the list of keys length is equal the max allowed, in this test case 1024/25 = 40
    (18, true, {
      let mut msg = KeyResponseMessage::default();
      for _ in 0..18 {
        msg.add_key(SecretKey::from(k));
      }
      msg
    }),
  ];

  for (expected, has_msg, mut resp) in cases {
    if let Err(e) = SerfQueries::key_list_response_with_correct_size(&query, &mut resp) {
      println!("error: {:?}", e);
      continue;
    }

    if resp.keys.len() != expected {
      println!("expected: {}, got: {}", expected, resp.keys.len());
    }

    if has_msg && !resp.message.contains("truncated") {
      println!("truncation message should be set");
    }
  }
}

/// Unit test for delegate node meta
pub async fn delegate_nodemeta<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s = Serf::<T>::new(
    transport_opts,
    opts.with_tags([("role", "test")].into_iter()),
  )
  .await
  .unwrap();
  let meta = s.inner.memberlist.delegate().unwrap().node_meta(32).await;

  let (_, tags) = <DefaultDelegate<T> as TransformDelegate>::decode_tags(&meta).unwrap();
  assert_eq!(tags.get("role"), Some(&SmolStr::new("test")));
}

/// Unit test for delegate node meta panic
pub async fn delegate_nodemeta_panic<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s = Serf::<T>::new(
    transport_opts,
    opts.with_tags([("role", "test")].into_iter()),
  )
  .await
  .unwrap();
  s.inner.memberlist.delegate().unwrap().node_meta(1).await;
}

pub async fn delegate_local_state<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts1, opts).await.unwrap();

  let opts = test_config();
  let s2 = Serf::<T>::new(transport_opts2, opts).await.unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let (id, addr) = serfs[1].memberlist().advertise_node().into_components();

  serfs[0].join(Node::new(id, MaybeResolvedAddress::resolved(addr)), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[0].user_event("test", Bytes::from_static(b"test"), false).await.unwrap();

  serfs[0].query("foo", Bytes::new(), None).await.unwrap();

  // s2 can leave now
  serfs[1].leave().await.unwrap();

  // Do a state dump
  let buf = serfs[0].memberlist().delegate().unwrap().local_state(false).await;

  // Verify
  assert_eq!(buf[0], MessageType::PushPull as u8, "bad message type");

  // Attempt a decode
  let (_, pp) = <DefaultDelegate<T> as TransformDelegate>::decode_message(MessageType::PushPull, &buf[1..]).unwrap();

  let SerfMessage::PushPull(pp) = pp else { panic!("bad message") };

  // Verify lamport clock
  assert_eq!(pp.ltime(), serfs[0].inner.clock.time(), "bad lamport clock");

  // Verify the status
  // Leave waits until propagation so this should only have one member
  assert_eq!(pp.status_ltimes().len(), 1, "missing ltimes");
  assert_eq!(pp.left_members().len(), 0, "should have no left memebers");
  assert_eq!(pp.event_ltime(), serfs[0].inner.event_clock.time(), "bad event clock");
  assert_eq!(pp.events().len(), serfs[0].inner.event_core.read().await.buffer.len(), "should send full event buffer");
  assert_eq!(pp.query_ltime(), serfs[0].inner.query_clock.time(), "bad query clock");
}
