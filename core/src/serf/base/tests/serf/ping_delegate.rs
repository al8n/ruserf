use super::*;

/// Unit test for serf ping delegate versioning
pub async fn serf_ping_delegate_versioning<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
) where
  T: Transport,
{
  const PROBE_INTERVAL: Duration = Duration::from_millis(2);

  let opts = test_config().with_disable_coordinates(false);
  let s1 = Serf::<T>::new(
    transport_opts1,
    opts
      .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL)),
  )
  .await
  .unwrap();

  let opts = test_config().with_disable_coordinates(false);
  let s2 = Serf::<T>::new(
    transport_opts2,
    opts
      .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL)),
  )
  .await
  .unwrap();

  // Monkey patch s1 to send weird versions of the ping messages.
  s1.memberlist()
    .delegate()
    .unwrap()
    .ping_versioning_test
    .store(true, Ordering::SeqCst);

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .memberlist()
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  // They both should show 2 members, but only s1 should know about s2
  // in the cache, since s1 spoke an alien ping protocol.
  wait_until_num_nodes(2, &serfs).await;

  let start = Instant::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let s1id = serfs[0].local_id().clone();
  let s2id = serfs[1].local_id().clone();

  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if serfs[0].cached_coordinate(&s2id).is_ok() {
      cond1 = true;
    } else if start.elapsed() > Duration::from_secs(5) {
      panic!("s1 didn't get a coordinate for s2");
    }

    if serfs[1].cached_coordinate(&s1id).is_err() {
      cond2 = true;
    } else if start.elapsed() > Duration::from_secs(5) {
      panic!("s2 got an unexpected coordinate for s1");
    }

    if cond1 && cond2 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("s1: {} s2: {}", cond1, cond2);
    }
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit test for serf ping delegate rogue coordinate
pub async fn serf_ping_delegate_rogue_coordinate<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
) where
  T: Transport,
{
  const PROBE_INTERVAL: Duration = Duration::from_millis(2);

  let opts = test_config().with_disable_coordinates(false);
  let s1 = Serf::<T>::new(
    transport_opts1,
    opts
      .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL)),
  )
  .await
  .unwrap();

  let opts = test_config().with_disable_coordinates(false);
  let s2 = Serf::<T>::new(
    transport_opts2,
    opts
      .with_memberlist_options(memberlist_core::Options::lan().with_probe_interval(PROBE_INTERVAL)),
  )
  .await
  .unwrap();

  // Monkey patch s1 to send ping messages with bad coordinates.
  s1.memberlist()
    .delegate()
    .unwrap()
    .ping_dimension_test
    .store(true, Ordering::SeqCst);

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .memberlist()
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  // They both should show 2 members, but only s1 should know about s2
  // in the cache, since s1 spoke an alien ping protocol.
  wait_until_num_nodes(2, &serfs).await;

  let start = Instant::now();
  let mut cond1 = false;
  let mut cond2 = false;
  let s1id = serfs[0].local_id().clone();
  let s2id = serfs[1].local_id().clone();

  // They both should show 2 members, but only s1 should know about s2
  // in the cache, since s1 sent a bad coordinate.
  loop {
    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;

    if serfs[0].cached_coordinate(&s2id).is_ok() {
      cond1 = true;
    } else if start.elapsed() > Duration::from_secs(5) {
      panic!("s1 didn't get a coordinate for s2");
    }

    if serfs[1].cached_coordinate(&s1id).is_err() {
      cond2 = true;
    } else if start.elapsed() > Duration::from_secs(5) {
      panic!("s2 got an unexpected coordinate for s1");
    }

    if cond1 && cond2 {
      break;
    }

    if start.elapsed() > Duration::from_secs(7) {
      panic!("s1: {} s2: {}", cond1, cond2);
    }
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}
