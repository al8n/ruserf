use std::sync::atomic::{AtomicUsize, Ordering};

use memberlist_core::{tests::AnyError, transport::Id};

use ruserf_types::{Member, MemberStatus, Tags};

use crate::{event::EventProducer, types::MemberState};

use super::*;

/// Unit tests for the serf leave related functionalities
pub mod leave;

/// Unit tests for the serf join related functionalities
pub mod join;

/// Unit tests for the serf ping delegate related functionalities
pub mod delegate;

/// Unit tests for the serf reconnect related functionalities
pub mod reconnect;

/// Unit tests for the serf remove related functionalities
pub mod remove;

/// Unit tests for serf reap related functionalities
pub mod reap;

/// Unit tests for the serf snapshot related functionalities
pub mod snapshot;

/// Unit tests for the serf query related functionalities
pub mod query;

fn test_member_status<I: Id, A>(
  members: &HashMap<I, MemberState<I, A>>,
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
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
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
    event_rx.rx,
    node,
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Failed),
      CrateEventType::Member(MemberEventType::Reap),
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
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
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
    event_rx.rx,
    node,
    [CrateEventType::Member(MemberEventType::Join)]
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
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(
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

  let start = Epoch::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    if test_member_status(&members.states, node.id().clone(), MemberStatus::Left).is_ok() {
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
    event_rx.rx,
    node,
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Leave),
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

  let (event_tx1, event_rx1) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, config_local(test_config()), event_tx1)
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

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let mut cond3 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if !cond1 {
      let members = serfs[0].inner.members.read().await;
      if test_member_status(&members.states, node.id().clone(), MemberStatus::Left).is_ok() {
        cond1 = true;
      }
    }

    if !cond2 {
      let members = serfs[1].inner.members.read().await;
      if test_member_status(&members.states, node.id().clone(), MemberStatus::Left).is_ok() {
        cond2 = true;
      }
    }

    if !cond3 {
      let members = serfs[2].inner.members.read().await;
      if test_member_status(&members.states, node.id().clone(), MemberStatus::Left).is_ok() {
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
    event_rx1.rx,
    s2.inner.memberlist.local_id().clone(),
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Leave),
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Leave),
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
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
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

  let start = Epoch::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    if test_member_status(&members.states, node.id().clone(), MemberStatus::Left).is_ok() {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // Now that s2 has failed and been marked as left, we check the
  // events to make sure we got a leave event in s1 about the leave.
  test_events(
    event_rx.rx,
    serfs[1].inner.memberlist.local_id().clone(),
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Failed),
      CrateEventType::Member(MemberEventType::Leave),
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
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();
  let s2 = Serf::<T>::with_event_producer(transport_opts2, test_config(), event_tx)
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
    event_rx.rx,
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
  let (event_tx, _event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
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

/// Unit tests for the update
pub async fn serf_update<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
  T::Options: Clone,
{
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2.clone(), test_config())
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

  wait_until_num_nodes(2, &serfs).await;
  // Now force the shutdown of s2 so it appears to fail.
  serfs[1].shutdown().await.unwrap();

  // Don't wait for a failure to be detected. Bring back s2 immediately
  let start = Epoch::now();
  let s2 = loop {
    match Serf::<T>::new(
      transport_opts2.clone(),
      test_config().with_tags([("foo", "bar")].into_iter()),
    )
    .await
    {
      Ok(s) => break s,
      Err(e) => {
        <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).await;
        if start.elapsed() > Duration::from_secs(2) {
          panic!("timed out: {}", e);
        }
      }
    }
  };

  let s1node = serfs[0].advertise_node();
  s2.join(s1node.map_address(MaybeResolvedAddress::resolved), false)
    .await
    .unwrap();
  serfs[1] = s2;
  wait_until_num_nodes(2, &serfs).await;

  test_events(
    event_rx.rx,
    node.id().clone(),
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Update),
    ]
    .into_iter()
    .collect(),
  )
  .await;

  // Verify that the member data got updated.
  let mut found = false;
  let members = serfs[0].inner.members.read().await;

  for member in members.states.values() {
    if member.member.node.id().eq(node.id())
      && member.member.tags().get("foo").map(|v| v.as_str()) == Some("bar")
    {
      found = true;
      break;
    }
  }
  assert!(found, "did not found s2 in members");
}

/// Unit tests for the role
pub async fn serf_role<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let s1 = Serf::<T>::new(
    transport_opts1,
    test_config().with_tags([("role", "web")].into_iter()),
  )
  .await
  .unwrap();
  let s2 = Serf::<T>::new(
    transport_opts2,
    test_config().with_tags([("role", "lb")].into_iter()),
  )
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

  let mut roles = HashMap::new();

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    for m in members.states.values() {
      roles.insert(
        m.member.node.id().clone(),
        m.member.tags().get("role").cloned().unwrap(),
      );
    }

    if let Some(role) = roles.get(node.id()) {
      if role == "lb" {
        cond1 = true;
      }
    }

    if let Some(role) = roles.get(serfs[0].local_id()) {
      if role == "web" {
        cond2 = true;
      }
    }

    if cond1 && cond2 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }
}

/// Unit test for serf state
pub async fn serf_state<T>(transport_opts1: T::Options)
where
  T: Transport,
{
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();

  assert_eq!(s1.state(), SerfState::Alive);

  s1.leave().await.unwrap();

  assert_eq!(s1.state(), SerfState::Left);

  s1.shutdown().await.unwrap();

  assert_eq!(s1.state(), SerfState::Shutdown);
}

/// Unit tests for serf set tags
pub async fn serf_set_tags<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
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

  // Update the tags
  serfs[0]
    .set_tags([("port", "8080")].into_iter().collect())
    .await
    .unwrap();

  serfs[1]
    .set_tags([("datacenter", "east-aws")].into_iter().collect())
    .await
    .unwrap();

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let mut cond3 = false;
  let mut cond4 = false;

  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let m1m = serfs[0].members().await;
    let mut m1m_tags = HashMap::with_capacity(2);
    for m in m1m {
      m1m_tags.insert(m.node.id().clone(), m.tags.clone());
    }

    if m1m_tags.get(node.id()).map(|t| t.get("port")) == Some(Some(&"8080".into())) {
      cond1 = true;
    }

    if m1m_tags.get(serfs[0].local_id()).map(|t| t.get("port")) == Some(Some(&"8080".into())) {
      cond2 = true;
    }

    let m2m = serfs[1].members().await;
    let mut m2m_tags = HashMap::with_capacity(2);
    for m in m2m {
      m2m_tags.insert(m.node.id().clone(), m.tags.clone());
    }

    if m2m_tags.get(node.id()).map(|t| t.get("datacenter")) == Some(Some(&"east-aws".into())) {
      cond3 = true;
    }

    if m2m_tags
      .get(serfs[1].local_id())
      .map(|t| t.get("datacenter"))
      == Some(Some(&"east-aws".into()))
    {
      cond4 = true;
    }

    if cond1 && cond2 && cond3 && cond4 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // we check the events to make sure we got failures.
  test_events(
    event_rx.rx,
    node.id().clone(),
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Update),
    ]
    .into_iter()
    .collect(),
  )
  .await;

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit tests for serf num nodes
pub async fn serf_num_nodes<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  assert_eq!(s1.num_nodes().await, 1);

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;
}

/// Unit tests for serf coordinates
pub async fn serf_coordinates<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
  transport_opts3: T::Options,
) where
  T: Transport,
{
  const PROBE_INTERVAL: Duration = Duration::from_millis(2);

  let opts = test_config()
    .with_disable_coordinates(false)
    .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL));
  let s1 = Serf::<T>::new(transport_opts1, opts.clone()).await.unwrap();
  let s2 = Serf::<T>::new(transport_opts2, opts).await.unwrap();

  let mut serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  // Make sure both nodes start out the origin so we can prove they did
  // an update later.
  let c1 = serfs[0].cooridate().unwrap();
  let c2 = serfs[1].cooridate().unwrap();

  const ZERO_THRESHOLD: f64 = 20.0e-6;

  assert!(
    c1.distance_to(&c2).as_secs_f64() <= ZERO_THRESHOLD,
    "coordinates didn't start at the origin"
  );

  // Join the two nodes together and give them time to probe each other.
  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let mut cond3 = false;
  let mut cond4 = false;
  let s2id = serfs[1].local_id().clone();
  let s1id = serfs[0].local_id().clone();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    // See if they know about each other.

    if serfs[0].cached_coordinate(&s2id.clone()).is_ok() {
      cond1 = true;
    } else if start.elapsed() > Duration::from_secs(7) {
      panic!("s1 didn't get a coordinate for s2");
    }

    if serfs[1].cached_coordinate(&s1id.clone()).is_ok() {
      cond2 = true;
    } else if start.elapsed() > Duration::from_secs(7) {
      panic!("s2 didn't get a coordinate for s1");
    }

    // With only one ping they won't have a good estimate of the other node's
    // coordinate, but they should both have updated their own coordinate.
    let c1 = serfs[0].cooridate().unwrap();
    let c2 = serfs[1].cooridate().unwrap();

    if c1.distance_to(&c2).as_secs_f64() >= ZERO_THRESHOLD {
      cond3 = true;
    } else if start.elapsed() > Duration::from_secs(7) {
      panic!("coordinates didn't update after probes");
    }

    // Make sure they cached their own current coordinate after the update.
    let c1c = serfs[0].cached_coordinate(&s1id.clone()).unwrap();
    match c1c {
      None => {
        if start.elapsed() > Duration::from_secs(7) {
          panic!("s1 didn't cache its own coordinate");
        }
      }
      Some(c1c) => {
        if c1 == c1c {
          cond4 = true;
        } else if start.elapsed() > Duration::from_secs(7) {
          panic!("s1 coordinates are not equal");
        }
      }
    }

    if cond1 && cond2 && cond3 && cond4 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  // Break up the cluster and make sure the coordinates get removed by
  // the reaper.
  serfs[1].shutdown().await.unwrap();

  <T::Runtime as RuntimeLite>::sleep(serfs[1].inner.opts.reap_interval * 2).await;

  wait_until_num_nodes(1, &serfs[..1]).await;

  let start = Epoch::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if serfs[0].cached_coordinate(&s2id.clone()).is_err() {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("s1 should have removed s2's cached coordinate");
    }
  }

  // Try a setup with coordinates disabled.
  let s3 = Serf::<T>::new(
    transport_opts3,
    test_config()
      .with_disable_coordinates(true)
      .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL)),
  )
  .await
  .unwrap();

  serfs[1] = s3;
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[0]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[1].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    // See if they know about each other.

    if let Err(e) = serfs[1].cooridate() {
      if e.to_string().contains("coordinates are disabled") {
        cond1 = true;
      }
    }

    if serfs[1].cached_coordinate(&s1id.clone()).is_err() {
      cond2 = true;
    }

    if cond1 && cond2 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit tests for serf name resolution
///
/// set_id is a function that takes the transport options and the id of the node, and returns the
/// transport options with the id set to the given id.
pub async fn serf_name_resolution<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
  transport_opts3: T::Options,
  set_id: impl FnOnce(T::Options, T::Id) -> T::Options,
) where
  T: Transport,
{
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();
  let s3 = Serf::<T>::new(
    set_id(transport_opts3, s1.local_id().clone()),
    test_config(),
  )
  .await
  .unwrap();

  let serfs = [s1, s2, s3];
  wait_until_num_nodes(1, &serfs).await;

  // Join s1 to s2 first. s2 should vote for s1 in conflict
  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  wait_until_num_nodes(2, &serfs[..2]).await;
  wait_until_num_nodes(1, &serfs[2..]).await;

  let node = serfs[2]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  // Wait for the query period to end
  <T::Runtime as RuntimeLite>::sleep(serfs[0].default_query_timeout().await * 2).await;

  let start = Epoch::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let mut cond3 = false;
  loop {
    // s3 should have shutdown, while s1 is running
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if serfs[0].state() == SerfState::Alive {
      cond1 = true;
    }

    if serfs[1].state() == SerfState::Alive {
      cond2 = true;
    }

    if serfs[2].state() == SerfState::Shutdown {
      cond3 = true;
    }

    if cond1 && cond2 && cond3 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("timed out");
    }
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit test for serf local member
pub async fn serf_local_member<T>(opts: T::Options)
where
  T: Transport,
{
  let s = Serf::<T>::new(opts, test_config()).await.unwrap();

  let local = s.local_member().await;
  assert_eq!(local.node.id(), s.local_id());

  assert_eq!(local.tags, s.inner.opts.tags());
  assert_eq!(local.status, MemberStatus::Alive);

  let new_tags = [("foo", "bar"), ("test", "ing")]
    .into_iter()
    .collect::<Tags>();
  s.set_tags(new_tags.clone()).await.unwrap();

  let local = s.local_member().await;
  assert_eq!(&*local.tags, &new_tags);
}

/// Unit test for serf stats
pub async fn serf_stats<T>(opts: T::Options)
where
  T: Transport,
{
  let s = Serf::<T>::new(opts, test_config()).await.unwrap();

  let stats = s.stats().await;
  assert_eq!(stats.get_event_queue(), 0);
  assert_eq!(stats.get_event_time(), 1);
  assert_eq!(stats.get_failed(), 0);
  assert_eq!(stats.get_intent_queue(), 0);
  assert_eq!(stats.get_left(), 0);
  assert_eq!(stats.get_health_score(), 0);
  assert_eq!(stats.get_member_time(), 1);
  assert_eq!(stats.get_members(), 1);
  assert!(!stats.get_encrypted());
}

/// Unit test for serf write keying file
#[cfg(feature = "encryption")]
pub async fn serf_write_keying_file<T>(
  get_transport_opts: impl FnOnce(memberlist_core::types::SecretKeyring) -> T::Options,
) where
  T: Transport,
{
  use std::io::Read;

  use base64::{engine::general_purpose, Engine as _};

  const EXISTING: &str = "T9jncgl9mbLus+baTTa7q7nPSUrXwbDi2dhbtqir37s=";
  const NEW_KEY: &str = "HvY8ubRZMgafUOWvrOadwOckVa1wN3QWAo46FVKbVN8=";

  let td = tempfile::tempdir().unwrap();
  let mut p = td.path().join("serf_write_keying_file");
  p.set_extension("json");

  let existing_bytes = general_purpose::STANDARD.decode(EXISTING).unwrap();
  let sk = memberlist_core::types::SecretKey::try_from(existing_bytes.as_slice()).unwrap();
  let keyring = memberlist_core::types::SecretKeyring::new(sk);

  let s = Serf::<T>::new(get_transport_opts(keyring), test_config())
    .await
    .unwrap();
  assert!(
    s.encryption_enabled(),
    "write keyring file test only works on encrypted serf"
  );

  let manager = s.key_manager();
  let new_key = general_purpose::STANDARD.decode(NEW_KEY).unwrap();
  let new_sk = memberlist_core::types::SecretKey::try_from(new_key.as_slice()).unwrap();
  manager.install_key(new_sk, None).await.unwrap();

  let mut keyring_file = std::fs::File::open(&p).unwrap();
  let mut s = String::new();
  keyring_file.read_to_string(&mut s).unwrap();

  let lines = s.split('\n').collect::<Vec<_>>();
  assert_eq!(lines.len(), 4);

  // Ensure both the original key and the new key are present in the file
  assert!(s.contains(EXISTING));
  assert!(s.contains(NEW_KEY));

  // Ensure the existing key remains primary. This is in position 1 because
  // the file writer will use json.MarshalIndent(), leaving the first line as
  // the opening bracket.
  assert!(lines[1].contains(EXISTING));

  // Swap primary keys
  manager.use_key(new_sk, None).await.unwrap();

  let mut keyring_file = std::fs::File::open(&p).unwrap();
  let mut s = String::new();
  keyring_file.read_to_string(&mut s).unwrap();

  let lines = s.split('\n').collect::<Vec<_>>();
  assert_eq!(lines.len(), 4);

  // Key order should have changed in keyring file
  assert!(lines[1].contains(NEW_KEY));

  // Remove the old key
  manager.remove_key(sk, None).await.unwrap();

  let mut keyring_file = std::fs::File::open(&p).unwrap();
  let mut s = String::new();
  keyring_file.read_to_string(&mut s).unwrap();

  let lines = s.split('\n').collect::<Vec<_>>();
  // Only the new key should now be present in the keyring file
  assert_eq!(lines.len(), 3);

  assert!(lines[1].contains(NEW_KEY));
}

#[test]
fn test_recent_intent() {
  assert!(recent_intent::<SmolStr>(&HashMap::new(), &"foo".into(), MessageType::Join).is_none());

  let now = Epoch::now();
  let expire = || now - Duration::from_secs(2);
  let save = || now;

  let mut intents = HashMap::<SmolStr, _>::new();
  assert!(recent_intent(&intents, &"foo".into(), MessageType::Join).is_none());

  assert!(upsert_intent(
    &mut intents,
    &"foo".into(),
    MessageType::Join,
    1.into(),
    expire
  ));
  assert!(upsert_intent(
    &mut intents,
    &"bar".into(),
    MessageType::Leave,
    2.into(),
    expire
  ));
  assert!(upsert_intent(
    &mut intents,
    &"baz".into(),
    MessageType::Join,
    3.into(),
    save
  ));
  assert!(upsert_intent(
    &mut intents,
    &"bar".into(),
    MessageType::Join,
    4.into(),
    expire
  ));
  assert!(!upsert_intent(
    &mut intents,
    &"bar".into(),
    MessageType::Join,
    0.into(),
    expire
  ));
  assert!(upsert_intent(
    &mut intents,
    &"bar".into(),
    MessageType::Join,
    5.into(),
    expire
  ));

  let ltime = recent_intent(&intents, &"foo".into(), MessageType::Join).unwrap();
  assert_eq!(ltime, 1.into());

  let ltime = recent_intent(&intents, &"bar".into(), MessageType::Join).unwrap();
  assert_eq!(ltime, 5.into());

  let ltime = recent_intent(&intents, &"baz".into(), MessageType::Join).unwrap();
  assert_eq!(ltime, 3.into());

  assert!(recent_intent(&intents, &"tubez".into(), MessageType::Join).is_none());

  reap_intents(&mut intents, Epoch::now(), Duration::from_secs(1));
  assert!(recent_intent(&intents, &"foo".into(), MessageType::Join).is_none());
  assert!(recent_intent(&intents, &"bar".into(), MessageType::Join).is_none());
  let ltime = recent_intent(&intents, &"baz".into(), MessageType::Join).unwrap();
  assert_eq!(ltime, 3.into());
  assert!(recent_intent(&intents, &"tubez".into(), MessageType::Join).is_none());
  reap_intents(
    &mut intents,
    Epoch::now() + Duration::from_secs(2),
    Duration::from_secs(1),
  );
  assert!(recent_intent(&intents, &"baz".into(), MessageType::Join).is_none());
}
