use memberlist_core::{tests::AnyError, transport::Id};
use ruserf_types::{Member, MemberStatus};

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
