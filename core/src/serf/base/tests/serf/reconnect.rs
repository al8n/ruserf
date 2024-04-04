use std::{marker::PhantomData, net::IpAddr};

use crate::delegate::ReconnectDelegate;

use super::*;

/// Unit test for reconnect
pub async fn serf_reconnect<T>(transport_opts1: T::Options, transport_opts2: T::Options)
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
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].shutdown().await.unwrap();

  let t = serfs[1].inner.opts.memberlist_options.probe_interval();
  <T::Runtime as RuntimeLite>::sleep(t * 5).await;

  // Bring back s2
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();
  serfs[1] = s2;

  wait_until_num_nodes(2, &serfs).await;

  let node = serfs[1].local_id().clone();
  test_events(
    event_rx.rx,
    node,
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Failed),
      CrateEventType::Member(MemberEventType::Join),
    ]
    .into_iter()
    .collect(),
  )
  .await;

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit test for reconnect
pub async fn serf_reconnect_same_ip<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
  get_ip: impl Fn(&<T::Resolver as AddressResolver>::ResolvedAddress) -> IpAddr,
) where
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

  let ip1 = get_ip(s1.inner.memberlist.advertise_address());
  let ip2 = get_ip(s2.inner.memberlist.advertise_address());
  assert_eq!(ip1, ip2, "require same ip address 1: {ip1} 2: {ip2}");

  let mut serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[1].shutdown().await.unwrap();

  let t = serfs[1].inner.opts.memberlist_options.probe_interval();
  <T::Runtime as RuntimeLite>::sleep(t * 5).await;

  // Bring back s2
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();
  serfs[1] = s2;

  wait_until_num_nodes(2, &serfs).await;

  let node = serfs[1].local_id().clone();
  test_events(
    event_rx.rx,
    node,
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Failed),
      CrateEventType::Member(MemberEventType::Join),
    ]
    .into_iter()
    .collect(),
  )
  .await;

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

#[derive(Clone)]
struct ReconnectOverride<A> {
  timeout: Duration,
  called: Arc<AtomicBool>,
  _marker: PhantomData<A>,
}

impl<A> ReconnectDelegate for ReconnectOverride<A>
where
  A: CheapClone + Send + Sync + 'static,
{
  type Id = SmolStr;

  type Address = A;

  fn reconnect_timeout(
    &self,
    _member: &Member<Self::Id, Self::Address>,
    _timeout: Duration,
  ) -> Duration {
    self.timeout
  }
}

/// Unit test for serf per node reconnect timeout
pub async fn serf_per_node_reconnect_timeout<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
) where
  T: Transport<Id = SmolStr>,
{
  let (event_tx, event_rx) = EventProducer::bounded(4);

  let ro1 = ReconnectOverride {
    timeout: Duration::from_secs(1),
    called: Arc::new(AtomicBool::new(false)),
    _marker: PhantomData,
  };

  let s1 = Serf::<T, _>::with_event_producer_and_delegate(
    transport_opts1,
    test_config().with_reconnect_timeout(Duration::from_secs(30)),
    event_tx,
    DefaultDelegate::<T>::new().with_reconnect_delegate(ro1.clone()),
  )
  .await
  .unwrap();
  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let start = Instant::now();
  let mut cond1 = false;
  let mut cond2 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;
    let n = s1.num_nodes().await;
    if n == 1 {
      cond1 = true;
    }
    if start.elapsed() > Duration::from_secs(7) {
      panic!("s1 got {} expected {}", n, 1);
    }

    let n = s2.num_nodes().await;
    if n == 1 {
      cond2 = true;
    }
    if start.elapsed() > Duration::from_secs(7) {
      panic!("s2 got {} expected {}", n, 1);
    }

    if cond1 && cond2 {
      break;
    }
  }

  let node = s2
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  s1.join(node, false).await.unwrap();

  let start = Instant::now();
  let mut cond1 = false;
  let mut cond2 = false;
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;
    let n = s1.num_nodes().await;
    if n == 2 {
      cond1 = true;
    }
    if start.elapsed() > Duration::from_secs(7) {
      panic!("s1 got {} expected {}", n, 2);
    }

    let n = s2.num_nodes().await;
    if n == 2 {
      cond2 = true;
    }
    if start.elapsed() > Duration::from_secs(7) {
      panic!("s2 got {} expected {}", n, 2);
    }

    if cond1 && cond2 {
      break;
    }
  }

  s2.shutdown().await.unwrap();

  let serfs = [s1];

  wait_until_num_nodes(1, &serfs).await;

  // Since s2 shutdown, we check the events to make sure we got failures.
  test_events(
    event_rx.rx,
    s2.local_id().clone(),
    [
      CrateEventType::Member(MemberEventType::Join),
      CrateEventType::Member(MemberEventType::Failed),
      CrateEventType::Member(MemberEventType::Reap),
    ]
    .into_iter()
    .collect(),
  )
  .await;

  assert!(
    ro1.called.load(Ordering::SeqCst),
    "reconnect override was not used"
  );
  serfs[0].shutdown().await.unwrap();
}
