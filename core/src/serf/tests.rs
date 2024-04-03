use std::time::{Duration, Instant};

use async_channel::Receiver;
use futures::FutureExt;
use memberlist_core::{
  agnostic_lite::RuntimeLite,
  bytes::Bytes,
  delegate::NodeDelegate,
  transport::MaybeResolvedAddress,
  types::{OneOrMore, TinyVec},
};
use ruserf_types::{
  MessageType, Node, PushPullMessage, QueryFlag, QueryMessage, SerfMessage, UserEvent,
  UserEventMessage,
};
use smol_str::SmolStr;

use crate::{
  delegate::TransformDelegate,
  event::{EventKind, EventType, InternalQueryEvent, MemberEvent, MemberEventType},
};

use self::internal_query::SerfQueries;

use super::*;

mod delegate;
pub use delegate::*;

mod internals;
pub use internals::*;

mod query;
pub use query::*;

mod snapshot;
pub use snapshot::*;

mod serf;
pub use serf::*;

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
    let mut conds = Vec::with_capacity(serfs.len());
    for (idx, s) in serfs.iter().enumerate() {
      let n = s.num_nodes().await;
      if n == desired_nodes {
        conds.push(true);
        continue;
      }

      if start.elapsed() > Duration::from_secs(7) {
        panic!("s{} got {} expected {}", idx + 1, n, desired_nodes);
      }
    }
    if conds.len() == serfs.len() {
      break;
    }
  }
}

async fn wait_until_intent_queue_len<T, D>(desired_len: usize, serfs: &[Serf<T, D>])
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  let start = Instant::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;
    let mut conds = Vec::with_capacity(serfs.len());
    for (idx, s) in serfs.iter().enumerate() {
      let stats = s.stats().await;
      if stats.get_intent_queue() == desired_len {
        conds.push(true);
        continue;
      }

      if start.elapsed() > Duration::from_secs(7) {
        panic!(
          "s{} got {} expected {}",
          idx + 1,
          stats.get_intent_queue(),
          desired_len
        );
      }
    }
    if conds.len() == serfs.len() {
      break;
    }
  }
}

/// tests that the given node had the given sequence of events
/// on the event channel.
async fn test_events<T, D>(rx: Receiver<Event<T, D>>, node: T::Id, expected: Vec<EventType>)
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  let mut actual = Vec::with_capacity(expected.len());

  loop {
    futures::select! {
      event = rx.recv().fuse() => {
        let event = event.unwrap();
        match event.kind() {
          EventKind::Member(MemberEvent { ty, members }) => {
            let mut found = false;

            for m in members.iter() {
              if node.eq(m.node.id()) {
                found = true;
                break;
              }
            }

            if found {
              actual.push(EventType::Member(*ty));
            }
          }
          _ => continue,
        }
      }
      _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(10)).fuse() => {
        break;
      }
    }
  }

  assert_eq!(actual, expected, "bad events for node {:?}", node);
}

/// tests that the given sequence of usr events
/// on the event channel took place.
async fn test_user_events<T, D>(
  rx: Receiver<Event<T, D>>,
  expected_name: Vec<SmolStr>,
  expected_payload: Vec<Bytes>,
) where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  let mut actual_name = Vec::with_capacity(expected_name.len());
  let mut actual_payload = Vec::with_capacity(expected_payload.len());

  loop {
    futures::select! {
      event = rx.recv().fuse() => {
        let Ok(event) = event else { break };
        match event.kind() {
          EventKind::User(e) => {
            actual_name.push(e.name.clone());
            actual_payload.push(e.payload.clone());
          }
          _ => continue,
        }
      }
      _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(10)).fuse() => {
        break;
      }
    }
  }

  assert_eq!(actual_name, expected_name);
  assert_eq!(actual_payload, expected_payload);
}

/// tests that the given sequence of query events
/// on the event channel took place.
async fn test_query_events<T, D>(
  rx: Receiver<Event<T, D>>,
  expected_name: Vec<SmolStr>,
  expected_payload: Vec<Bytes>,
) where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  let mut actual_name = Vec::with_capacity(expected_name.len());
  let mut actual_payload = Vec::with_capacity(expected_payload.len());

  loop {
    futures::select! {
      event = rx.recv().fuse() => {
        let Ok(event) = event else { break };
        match event.kind() {
          EventKind::Query(e) => {
            actual_name.push(e.name.clone());
            actual_payload.push(e.payload.clone());
          }
          EventKind::InternalQuery { query, .. } => {
            actual_name.push(query.name.clone());
            actual_payload.push(query.payload.clone());
          }
          _ => continue,
        }
      }
      _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(10)).fuse() => {
        break;
      }
    }
  }

  assert_eq!(actual_name, expected_name);
  assert_eq!(actual_payload, expected_payload);
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
  use memberlist_core::types::SecretKey;
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
  use memberlist_core::types::SecretKey;
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
