use memberlist_core::transport::resolver::socket_addr::SocketAddrResolver;

use super::*;

/// Unit test for reconnect
pub async fn serf_reconnect<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
  T::Options: Clone,
{
  let (event_tx, event_rx) = async_channel::bounded(64);

  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
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
    event_rx,
    node,
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Failed),
      EventType::Member(MemberEventType::Join),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}

/// Unit test for reconnect
pub async fn serf_reconnect_same_ip<T, R>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport<Resolver = SocketAddrResolver<R>, Runtime = R>,
  T::Options: Clone,
  R: RuntimeLite,
{
  let (event_tx, event_rx) = async_channel::bounded(64);

  let s1 = Serf::<T>::with_event_sender(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();
  let s2 = Serf::<T>::new(transport_opts2.clone(), test_config())
    .await
    .unwrap();

  let ip1 = s1.inner.memberlist.advertise_address().ip();
  let ip2 = s2.inner.memberlist.advertise_address().ip();
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
    event_rx,
    node,
    [
      EventType::Member(MemberEventType::Join),
      EventType::Member(MemberEventType::Failed),
      EventType::Member(MemberEventType::Join),
    ]
    .into_iter()
    .collect(),
  )
  .await;
}
