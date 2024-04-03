use std::sync::atomic::{AtomicUsize, Ordering};

use memberlist_core::{tests::AnyError, transport::Id};
use ruserf_types::MemberStatus;

use crate::types::MemberState;

use super::*;

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

    if start.elapsed() > Duration::from_secs(25) {
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

    if start.elapsed() > Duration::from_secs(25) {
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
