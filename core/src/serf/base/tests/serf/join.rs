use std::marker::PhantomData;

use crate::delegate::MergeDelegate;

use super::*;

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

/// Unit test for serf join leave join
pub async fn serf_join_leave_join<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
  T::Options: Clone,
{
  let s1 = Serf::<T>::new(
    transport_opts1,
    test_config().with_reap_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();
  let s2 = Serf::<T>::new(
    transport_opts2.clone(),
    test_config().with_reap_interval(Duration::from_secs(10)),
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

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].leave().await.unwrap();
  serfs[1].shutdown().await.unwrap();

  let t = serfs[1].inner.opts.memberlist_options.probe_interval() * 5;
  // Give the reaper time to reap nodes
  <T::Runtime as RuntimeLite>::sleep(t).await;

  // s1 should see the node as having left
  let start = Epoch::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    let mut any_left = false;
    for member in members.states.values() {
      if member.member.status == MemberStatus::Left {
        any_left = true;
        break;
      }
    }

    if any_left {
      break;
    }

    if !any_left && start.elapsed() > Duration::from_secs(7) {
      panic!("Node should have left");
    }
  }

  // Bring the node back
  let s2 = Serf::<T>::new(
    transport_opts2,
    test_config().with_reap_interval(Duration::from_secs(10)),
  )
  .await
  .unwrap();

  serfs[1] = s2;
  wait_until_num_nodes(1, &serfs[1..]).await;

  // Re-attempt the join
  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), false).await.unwrap();

  let start = Epoch::now();
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    let members = serfs[0].inner.members.read().await;
    let mut any_left = false;
    for member in members.states.values() {
      if member.member.status == MemberStatus::Left {
        any_left = true;
        break;
      }
    }

    if !any_left {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("all nodes should be alive!");
    }
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit test for serf join ignore old
pub async fn serf_join_ignore_old<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let s1 = Serf::<T>::new(transport_opts1, test_config())
    .await
    .unwrap();

  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s2 = Serf::<T>::with_event_producer(transport_opts2, test_config(), event_tx)
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  // Fire a user event
  serfs[0]
    .user_event("event!", Bytes::from_static(b"test"), false)
    .await
    .unwrap();
  <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(10)).await;

  serfs[0]
    .user_event("second", Bytes::from_static(b"foobar"), false)
    .await
    .unwrap();
  <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(10)).await;

  // join with ignoreOld set to true! should not get events
  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node.clone(), true).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  // check the events to make sure we got nothing
  test_user_events(event_rx.rx, vec![], vec![]).await;

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

#[derive(Debug, thiserror::Error)]
#[error("merge canceled")]
struct CancelMergeError;

#[derive(Clone)]
struct CancelMergeDelegate<A: CheapClone + Send + Sync + 'static> {
  invoked: Arc<AtomicBool>,
  _phantom: PhantomData<A>,
}

impl<A: CheapClone + Send + Sync + 'static> MergeDelegate for CancelMergeDelegate<A> {
  type Error = CancelMergeError;

  type Id = SmolStr;

  type Address = A;

  async fn notify_merge(
    &self,
    _members: TinyVec<Member<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    self.invoked.store(true, Ordering::SeqCst);
    Err(CancelMergeError)
  }
}

/// Unit test for serf join cancel
pub async fn serf_join_cancel<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport<Id = SmolStr>,
{
  let cmd1 = CancelMergeDelegate {
    invoked: Arc::new(AtomicBool::new(false)),
    _phantom: PhantomData,
  };
  let s1 = Serf::<T, _>::with_delegate(
    transport_opts1,
    test_config(),
    DefaultDelegate::<T>::new().with_merge_delegate(cmd1.clone()),
  )
  .await
  .unwrap();
  let cmd2 = CancelMergeDelegate {
    invoked: Arc::new(AtomicBool::new(false)),
    _phantom: PhantomData,
  };
  let s2 = Serf::<T, _>::with_delegate(
    transport_opts2,
    test_config(),
    DefaultDelegate::<T>::new().with_merge_delegate(cmd2.clone()),
  )
  .await
  .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(0, &serfs).await;

  let node = serfs[1]
    .inner
    .memberlist
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);

  let err = serfs[0].join(node.clone(), false).await.unwrap_err();
  assert!(err.to_string().contains("merge canceled"));

  wait_until_num_nodes(0, &serfs).await;

  assert!(cmd1.invoked.load(Ordering::SeqCst));
  assert!(cmd2.invoked.load(Ordering::SeqCst));

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}
