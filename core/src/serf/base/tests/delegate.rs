use super::*;

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

/// Unit test for delegate local state
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

  serfs[0]
    .join(Node::new(id, MaybeResolvedAddress::resolved(addr)), false)
    .await
    .unwrap();

  wait_until_num_nodes(2, &serfs).await;

  serfs[0]
    .user_event("test", Bytes::from_static(b"test"), false)
    .await
    .unwrap();

  serfs[0].query("foo", Bytes::new(), None).await.unwrap();

  // s2 can leave now
  serfs[1].leave().await.unwrap();

  // Do a state dump
  let buf = serfs[0]
    .memberlist()
    .delegate()
    .unwrap()
    .local_state(false)
    .await;

  // Verify
  assert_eq!(buf[0], MessageType::PushPull as u8, "bad message type");

  // Attempt a decode
  let (_, pp) =
    <DefaultDelegate<T> as TransformDelegate>::decode_message(MessageType::PushPull, &buf[1..])
      .unwrap();

  let SerfMessage::PushPull(pp) = pp else {
    panic!("bad message")
  };

  // Verify lamport clock
  assert_eq!(pp.ltime(), serfs[0].inner.clock.time(), "bad lamport clock");

  // Verify the status
  // Leave waits until propagation so this should only have one member
  assert_eq!(pp.status_ltimes().len(), 1, "missing ltimes");
  assert_eq!(pp.left_members().len(), 0, "should have no left memebers");
  assert_eq!(
    pp.event_ltime(),
    serfs[0].inner.event_clock.time(),
    "bad event clock"
  );
  assert_eq!(
    pp.events().len(),
    serfs[0].inner.event_core.read().await.buffer.len(),
    "should send full event buffer"
  );
  assert_eq!(
    pp.query_ltime(),
    serfs[0].inner.query_clock.time(),
    "bad query clock"
  );

  for s in serfs {
    s.shutdown().await.unwrap();
  }
}

/// Unit test for delegate merge remote state
pub async fn delegate_merge_remote_state<A, T>(transport_opts: T::Options)
where
  // A: AddressResolver<ResolvedAddress = SocketAddr>,
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();
  let d = s.memberlist().delegate().unwrap();

  // Make a fake push pull
  let pp = PushPullMessage {
    ltime: 42.into(),
    status_ltimes: [
      (SmolStr::new("test"), 20.into()),
      (SmolStr::new("foo"), 15.into()),
    ]
    .into_iter()
    .collect(),
    left_members: ["test".into()].into_iter().collect(),
    event_ltime: 50.into(),
    events: TinyVec::from(Some(UserEvents {
      ltime: 45.into(),
      events: OneOrMore::from(UserEvent {
        name: "test".into(),
        payload: Bytes::new(),
      }),
    })),
    query_ltime: 100.into(),
  };

  let mut buf = vec![0; <DefaultDelegate<T> as TransformDelegate>::message_encoded_len(&pp)];
  <DefaultDelegate<T> as TransformDelegate>::encode_message(&pp, &mut buf).unwrap();

  // Merge in fake state
  d.merge_remote_state(buf.into(), false).await;

  // Verify lamport
  assert_eq!(s.inner.clock.time(), 42.into(), "bad lamport clock");

  let members = s.inner.members.read().await;
  // Verify pending join for test
  let ltime = recent_intent(
    &members.recent_intents,
    &SmolStr::new("test"),
    MessageType::Join,
  )
  .unwrap();
  assert_eq!(ltime, 20.into(), "bad join ltime");
  // Verify pending leave for foo
  let ltime = recent_intent(
    &members.recent_intents,
    &SmolStr::new("foo"),
    MessageType::Leave,
  )
  .unwrap();
  assert_eq!(ltime, 16.into(), "bad leave ltime");

  // Verify event clock
  assert_eq!(s.inner.event_clock.time(), 50.into(), "bad event clock");
  let buf = s.inner.event_core.read().await;
  assert!(buf.buffer[45].is_some(), "missing event buffer for time");
  assert_eq!(buf.buffer[45].as_ref().unwrap().events[0].name, "test");
  assert_eq!(s.inner.query_clock.time(), 100.into(), "bad query clock");
}
