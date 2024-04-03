use std::sync::atomic::{AtomicUsize, Ordering};

use memberlist_core::{tests::AnyError, transport::Id};
use ruserf_types::{Member, MemberStatus};

use crate::types::MemberState;

use super::*;

/// Unit tests for the leave related functionalities
pub mod leave;
/// Unit tests for the reconnect related functionalities
pub mod reconnect;

fn test_member_status<I: Id, A>(
  members: HashMap<I, MemberState<I, A>>,
  id: I,
  status: MemberStatus,
) -> Result<(), AnyError> {
  for member in members.values() {
    if id.eq(member.member.node.id()) {
      if member.member.status != status {
        return Err(AnyError::from(format!(
          "expected member {} to have status {:?}, got {:?}",
          id, status, member.member.status
        )));
      }
      return Ok(());
    }
  }
  Err(AnyError::from(format!("member {} not found", id)))
}

/// Unit tests for the events failed
pub async fn serf_events_failed<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].shutdown().await.unwrap();

  wait_until_num_nodes(1, &serfs[..1]).await;

  // Since s2 shutdown, we check the events to make sure we got failures.
  let node = serfs[1].inner.memberlist.local_id().clone();
  test_events(
    event_rx,
    node,
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Failed),
      EventType::Member(MemberEventType::Reap),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}

/// Unit tests for the events join
pub async fn serf_events_join<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].shutdown().await.unwrap();

  wait_until_num_nodes(1, &serfs[..1]).await;

  // Since s2 shutdown, we check the events to make sure we got failures.
  let node = serfs[1].inner.memberlist.local_id().clone();
  test_events(
    event_rx,
    node,
    [EventType::Member(MemberEventType::Join)]
      .into_iter()
      .collect(),
  )
  .await;
}

/// Unit tests for the events leave
/// Unit tests for the events failed
pub async fn serf_events_leave<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(
    transport_opts1,
    test_config().with_reap_interval(Duration::from_secs(30)),
    event_tx,
  )
  .await
  .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].leave().await.unwrap();

  let start = Instant::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    if test_member_status(
      members.states.clone(),
      node.id().clone(),
      MemberStatus::Left,
    )
    .is_ok()
    {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // Now that s2 has left, we check the events to make sure we got
  // a leave event in s1 about the leave.
  let node = serfs[1].inner.memberlist.local_id().clone();
  test_events(
    event_rx,
    node,
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Leave),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}

#[derive(Debug, Clone)]
struct DropJoins {
  drop: Arc<AtomicUsize>,
}

impl MessageDropper for DropJoins {
  fn should_drop(&self, ty: MessageType) -> bool {
    match ty {
      MessageType::Join | MessageType::PushPull => self.drop.load(Ordering::SeqCst) == 1,
      _ => false,
    }
  }
}

impl DropJoins {
  fn new() -> Self {
    Self {
      drop: Arc::new(AtomicUsize::new(0)),
    }
  }
}

/// Unit tests for the events leave avoid infinite leave rebroadcast
pub async fn serf_events_leave_avoid_infinite_rebroadcast<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
  transport_opts3: T::Options,
  transport_opts4: T::Options,
) where
  T: Transport,
  T::Options: Clone,
{
  // This test is a variation of the normal leave test that is crafted
  // specifically to handle a situation where two unique leave events for the
  // same node reach two other nodes in the wrong order which causes them to
  // infinitely rebroadcast the leave event without updating their own
  // lamport clock for that node.
  let config_local = |opts: Options| opts.with_reap_interval(Duration::from_secs(30));

  let (event_tx1, event_rx1) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(transport_opts1, config_local(test_config()), event_tx1)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2.clone(), config_local(test_config()))
    .await
    .unwrap();

  // Allow s3 and s4 to drop joins in the future.
  let d = DropJoins::new();
  let s3 = Serf::<T>::with_message_dropper(
    transport_opts3,
    config_local(test_config()),
    Box::new(d.clone()),
  )
  .await
  .unwrap();
  let s4 = Serf::<T>::with_message_dropper(
    transport_opts4,
    config_local(test_config()),
    Box::new(d.clone()),
  )
  .await
  .unwrap();

  let mut serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);

  serfs[0].join(node.clone(), false).await.unwrap();
  s3.join(node.clone(), false).await.unwrap();
  s4.join(node.clone(), false).await.unwrap();

  // S2 leaves gracefully
  serfs[1].leave().await.unwrap();
  serfs[1].shutdown().await.unwrap();

  // Make s3 and s4 drop inbound join messages and push-pulls for a bit so it won't see
  // s2 rejoin
  d.drop.store(1, Ordering::SeqCst);

  // Bring back s2 by mimicking its name and address
  <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(2)).await;

  let s2 = Serf::<T>::new(
    transport_opts2,
    config_local(test_config().with_rejoin_after_leave(true)),
  )
  .await
  .unwrap();

  let s1node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  s2.join(s1node.clone(), false).await.unwrap();
  serfs[1] = s2;
  let mut serfs = serfs
    .into_iter()
    .chain([s3, s4].into_iter())
    .collect::<Vec<_>>();
  wait_until_num_nodes(4, &serfs).await;

  // Now leave a second time but before s3 and s4 see the rejoin (due to the gate)
  serfs[1].leave().await.unwrap();

  let s2 = serfs.remove(1);
  wait_until_intent_queue_len(0, &serfs).await;

  let start = Instant::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let mut cond3 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if !cond1 {
      let members = serfs[0].inner.members.read().await;
      if test_member_status(
        members.states.clone(),
        node.id().clone(),
        MemberStatus::Left,
      )
      .is_ok()
      {
        cond1 = true;
      }
    }

    if !cond2 {
      let members = serfs[1].inner.members.read().await;
      if test_member_status(
        members.states.clone(),
        node.id().clone(),
        MemberStatus::Left,
      )
      .is_ok()
      {
        cond2 = true;
      }
    }

    if !cond3 {
      let members = serfs[2].inner.members.read().await;
      if test_member_status(
        members.states.clone(),
        node.id().clone(),
        MemberStatus::Left,
      )
      .is_ok()
      {
        cond3 = true;
      }
    }

    if cond1 && cond2 && cond3 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // Now that s2 has left, we check the events to make sure we got
  // a leave event in s1 about the leave.
  test_events(
    event_rx1,
    s2.inner.memberlist.local_id().clone(),
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Leave),
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Leave),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}

/// Unit tests for the remove failed events leave
pub async fn serf_remove_failed_events_leave<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
) where
  T: Transport,
{
  let (event_tx, event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].shutdown().await.unwrap();

  let t = serfs[1].inner.opts.memberlist_options.probe_interval();
  <T::Runtime as RuntimeLite>::sleep(t * 5).await;

  serfs[0]
    .remove_failed_node(node.id().clone())
    .await
    .unwrap();

  let start = Instant::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    if test_member_status(
      members.states.clone(),
      node.id().clone(),
      MemberStatus::Left,
    )
    .is_ok()
    {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // Now that s2 has failed and been marked as left, we check the
  // events to make sure we got a leave event in s1 about the leave.
  test_events(
    event_rx,
    serfs[1].inner.memberlist.local_id().clone(),
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Failed),
      EventType::Member(MemberEventType::Leave),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}

/// Unit tests for the events user
pub async fn serf_event_user<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();
  let s2 = Serf::<T>::with_event_sender(transport_opts2, test_config(), event_tx)
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  // Fire a user event
  serfs[0]
    .user_event("event!", Bytes::from_static(b"test"), false)
    .await
    .unwrap();

  // Fire a user event
  serfs[0]
    .user_event("second", Bytes::from_static(b"foobar"), false)
    .await
    .unwrap();

  // check the events to make sure we got
  // a leave event in s1 about the leave.
  test_user_events(
    event_rx,
    ["event!", "second"].into_iter().map(Into::into).collect(),
    vec![Bytes::from_static(b"test"), Bytes::from_static(b"foobar")],
  )
  .await;
}

/// Unit tests for the events user size limit
pub async fn serf_event_user_size_limit<T>(transport_opts1: T::Options)
where
  T: Transport,
{
  let (event_tx, _event_rx) = async_channel::bounded(4);
  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();

  let serfs = [s1];
  wait_until_num_nodes(1, &serfs).await;

  let p = Bytes::copy_from_slice(&serfs[0].inner.opts.max_user_event_size.to_be_bytes());
  let err = serfs[0]
    .user_event("this is too large an event", p, false)
    .await
    .unwrap_err()
    .to_string();
  assert!(err.contains("user event exceeds"));
}

/// Unit tests for the get queue max
pub async fn serf_get_queue_max<T>(
  transport_opts: T::Options,
  mut get_addr: impl FnMut(usize) -> <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
  T::Options: Clone,
{
  let s = Serf::<T>::new(transport_opts.clone(), test_config())
    .await
    .unwrap();

  // We don't need a running Serf so fake it out with the required
  // state.
  {
    let mut members = s.inner.members.write().await;
    for i in 0..100 {
      let name: SmolStr = format!("Member{i}").into();
      members.states.insert(
        name.clone(),
        MemberState {
          member: Member::new(
            Node::new(name.clone(), get_addr(i)),
            Default::default(),
            MemberStatus::Alive,
          ),
          status_time: 0.into(),
          leave_time: None,
        },
      );
    }
  }

  // Default mode just uses the max depth.
  let got = s.get_queue_max().await;
  let want = 4096;
  assert_eq!(got, want);

  // Now configure a min which should take precedence.
  s.shutdown().await.unwrap();
  <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(2)).await;

  let s = Serf::<T>::new(
    transport_opts.clone(),
    test_config().with_max_queue_depth(1024),
  )
  .await
  .unwrap();

  {
    let mut members = s.inner.members.write().await;
    for i in 0..100 {
      let name: SmolStr = format!("Member{i}").into();
      members.states.insert(
        name.clone(),
        MemberState {
          member: Member::new(
            Node::new(name.clone(), get_addr(i)),
            Default::default(),
            MemberStatus::Alive,
          ),
          status_time: 0.into(),
          leave_time: None,
        },
      );
    }
  }

  let got = s.get_queue_max().await;
  let want = 1024;
  assert_eq!(got, want);

  s.shutdown().await.unwrap();
  <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(2)).await;

  // Bring it under the number of nodes, so the calculation based on
  // the number of nodes takes precedence.
  let s = Serf::<T>::new(transport_opts, test_config().with_max_queue_depth(16))
    .await
    .unwrap();

  {
    let mut members = s.inner.members.write().await;
    for i in 0..100 {
      let name: SmolStr = format!("Member{i}").into();
      members.states.insert(
        name.clone(),
        MemberState {
          member: Member::new(
            Node::new(name.clone(), get_addr(i)),
            Default::default(),
            MemberStatus::Alive,
          ),
          status_time: 0.into(),
          leave_time: None,
        },
      );
    }
  }

  let got = s.get_queue_max().await;
  let want = 200;
  assert_eq!(got, want);

  // Try adjusting the node count.
  let mut members = s.inner.members.write().await;
  let name = SmolStr::new("another");
  members.states.insert(
    name.clone(),
    MemberState {
      member: Member::new(
        Node::new(name.clone(), get_addr(10000)),
        Default::default(),
        MemberStatus::Alive,
      ),
      status_time: 0.into(),
      leave_time: None,
    },
  );

  let got = s.get_queue_max().await;
  let want = 202;
  assert_eq!(got, want);
}

/// Unit tests for the join leave
pub async fn serf_join_leave<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();
  let reap_interval = s1.inner.opts.reap_interval();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].leave().await.unwrap();

  <T::Runtime as RuntimeLite>::sleep(reap_interval * 2).await;

  wait_until_num_nodes(1, &serfs).await;
}
