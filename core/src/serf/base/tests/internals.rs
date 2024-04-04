use crate::event::EventProducer;

use super::*;

/// Unit tests for the join leave ltime logic
pub async fn join_leave_ltime<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts1, opts).await.unwrap();
  let opts = test_config();
  let s2 = Serf::<T>::new(transport_opts2, opts).await.unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let (id, addr) = serfs[1].inner.memberlist.advertise_node().into_components();
  serfs[0]
    .join(Node::new(id, MaybeResolvedAddress::resolved(addr)), false)
    .await
    .unwrap();

  wait_until_num_nodes(2, &serfs).await;

  let now = Instant::now();

  loop {
    let members = serfs[1].inner.members.read().await;
    let mut cond1 = false;
    let mut cond2 = false;
    if let Some(m) = members.states.get(serfs[0].inner.memberlist.local_id()) {
      if m.status_time == 1.into() {
        cond1 = true;
      }

      if serfs[1].inner.clock.time() > m.status_time {
        cond2 = true;
      }
    }

    if cond1 && cond2 {
      break;
    }

    if now.elapsed() > Duration::from_secs(7) {
      panic!("timed out waiting for status time to be updated");
    }

    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;
  }

  let old_clock = serfs[1].inner.clock.time();

  serfs[0].leave().await.unwrap();

  loop {
    let mut cond1 = false;

    if serfs[1].inner.clock.time() > old_clock {
      cond1 = true;
    }

    if cond1 {
      break;
    }

    if now.elapsed() > Duration::from_secs(7) {
      panic!(
        "leave should increment ({} / {})",
        serfs[1].inner.clock.time(),
        old_clock
      );
    }

    <T::Runtime as RuntimeLite>::sleep(Duration::from_millis(25)).await;
  }

  for s in serfs.iter() {
    s.shutdown().await.unwrap();
  }
}

/// Unit tests for the join pending intent logic
pub async fn join_pending_intent<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();
  {
    let mut members = s1.inner.members.write().await;
    upsert_intent::<SmolStr>(
      &mut members.recent_intents,
      &"test".into(),
      MessageType::Join,
      5.into(),
      Instant::now,
    );
  }

  s1.handle_node_join(Arc::new(NodeState {
    id: "test".into(),
    addr,
    meta: Meta::empty(),
    state: memberlist_core::types::State::Alive,
    protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
    delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
  }))
  .await;

  {
    let members = s1.inner.members.read().await;
    let m = members.states.get("test").unwrap();
    assert_eq!(m.status_time, 5.into());
    assert_eq!(m.member.status, MemberStatus::Alive);
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the join pending intent logic
pub async fn join_pending_intents<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();
  {
    let mut members = s1.inner.members.write().await;
    upsert_intent::<SmolStr>(
      &mut members.recent_intents,
      &"test".into(),
      MessageType::Join,
      5.into(),
      Instant::now,
    );
    upsert_intent::<SmolStr>(
      &mut members.recent_intents,
      &"test".into(),
      MessageType::Leave,
      6.into(),
      Instant::now,
    );
  }

  s1.handle_node_join(Arc::new(NodeState {
    id: "test".into(),
    addr,
    meta: Meta::empty(),
    state: memberlist_core::types::State::Alive,
    protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
    delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
  }))
  .await;

  {
    let members = s1.inner.members.read().await;
    let m = members.states.get("test").unwrap();
    assert_eq!(m.status_time, 6.into());
    assert_eq!(m.member.status, MemberStatus::Leaving);
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the leave intent buffer early
pub async fn leave_intent_buffer_early<T>(transport_opts: T::Options)
where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  // Deliver a leave intent message early
  let j = LeaveMessage {
    ltime: 10.into(),
    id: "test".into(),
    prune: false,
  };

  assert!(!s1.handle_node_leave_intent(&j).await, "should rebroadcast");
  assert!(
    s1.handle_node_leave_intent(&j).await,
    "should not rebroadcast"
  );

  // Check that we buffered
  {
    let members = s1.inner.members.read().await;
    let ltime = recent_intent(&members.recent_intents, &"test".into(), MessageType::Leave).unwrap();
    assert_eq!(ltime, 10.into(), "bad buffer");
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the leave intent old message
pub async fn leave_intent_old_message<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  {
    let mut members = s1.inner.members.write().await;
    members.states.insert(
      "test".into(),
      MemberState {
        member: Member {
          node: Node::new("test".into(), addr),
          tags: Arc::new(Default::default()),
          status: MemberStatus::Alive,
          memberlist_protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
          memberlist_delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
          protocol_version: ruserf_types::ProtocolVersion::V1,
          delegate_version: ruserf_types::DelegateVersion::V1,
        },
        status_time: 12.into(),
        leave_time: None,
      },
    );
  }

  let j = LeaveMessage {
    ltime: 10.into(),
    id: "test".into(),
    prune: false,
  };

  assert!(
    s1.handle_node_leave_intent(&j).await,
    "should not rebroadcast"
  );

  {
    let members = s1.inner.members.read().await;
    assert!(
      recent_intent(&members.recent_intents, &"test".into(), MessageType::Leave).is_none(),
      "should not have buffered intent"
    );
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the leave intent newer
pub async fn leave_intent_newer<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();
  {
    let mut members = s1.inner.members.write().await;
    members.states.insert(
      "test".into(),
      MemberState {
        member: Member {
          node: Node::new("test".into(), addr),
          tags: Arc::new(Default::default()),
          status: MemberStatus::Alive,
          memberlist_protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
          memberlist_delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
          protocol_version: ruserf_types::ProtocolVersion::V1,
          delegate_version: ruserf_types::DelegateVersion::V1,
        },
        status_time: 12.into(),
        leave_time: None,
      },
    );
  }

  let j = LeaveMessage {
    ltime: 14.into(),
    id: "test".into(),
    prune: false,
  };

  assert!(!s1.handle_node_leave_intent(&j).await, "should rebroadcast");

  {
    let members = s1.inner.members.read().await;
    assert!(
      recent_intent(&members.recent_intents, &"test".into(), MessageType::Leave).is_none(),
      "should not have buffered intent"
    );

    let m = members.states.get("test").unwrap();
    assert_eq!(
      m.member.status,
      MemberStatus::Leaving,
      "should update status"
    );
    assert_eq!(s1.inner.clock.time(), 15.into(), "should update clock");
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the join intent buffer early
pub async fn join_intent_buffer_early<T>(transport_opts: T::Options)
where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  // Deliver a join intent message early
  let j = JoinMessage {
    ltime: 10.into(),
    id: "test".into(),
  };

  assert!(!s1.handle_node_join_intent(&j).await, "should rebroadcast");
  assert!(
    s1.handle_node_join_intent(&j).await,
    "should not rebroadcast"
  );

  // Check that we buffered
  {
    let members = s1.inner.members.read().await;
    let ltime = recent_intent(&members.recent_intents, &"test".into(), MessageType::Join).unwrap();
    assert_eq!(ltime, 10.into(), "bad buffer");
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the join intent old message
pub async fn join_intent_old_message<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  {
    let mut members = s1.inner.members.write().await;
    members.states.insert(
      "test".into(),
      MemberState {
        member: Member {
          node: Node::new("test".into(), addr),
          tags: Arc::new(Default::default()),
          status: MemberStatus::Alive,
          memberlist_protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
          memberlist_delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
          protocol_version: ruserf_types::ProtocolVersion::V1,
          delegate_version: ruserf_types::DelegateVersion::V1,
        },
        status_time: 12.into(),
        leave_time: None,
      },
    );
  }

  let j = JoinMessage {
    ltime: 10.into(),
    id: "test".into(),
  };

  assert!(
    s1.handle_node_join_intent(&j).await,
    "should not rebroadcast"
  );

  // Check that we didn't buffer anything
  {
    let members = s1.inner.members.read().await;
    assert!(
      recent_intent(&members.recent_intents, &"test".into(), MessageType::Join).is_none(),
      "should not have buffered intent"
    );
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the join intent newer
pub async fn join_intent_newer<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();
  {
    let mut members = s1.inner.members.write().await;
    members.states.insert(
      "test".into(),
      MemberState {
        member: Member {
          node: Node::new("test".into(), addr),
          tags: Arc::new(Default::default()),
          status: MemberStatus::Alive,
          memberlist_protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
          memberlist_delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
          protocol_version: ruserf_types::ProtocolVersion::V1,
          delegate_version: ruserf_types::DelegateVersion::V1,
        },
        status_time: 12.into(),
        leave_time: None,
      },
    );
  }

  let j = JoinMessage {
    ltime: 14.into(),
    id: "test".into(),
  };

  assert!(!s1.handle_node_join_intent(&j).await, "should rebroadcast");

  {
    let members = s1.inner.members.read().await;
    assert!(
      recent_intent(&members.recent_intents, &"test".into(), MessageType::Join).is_none(),
      "should not have buffered intent"
    );

    let m = members.states.get("test").unwrap();
    assert_eq!(m.status_time, 14.into(), "should update join time");
    assert_eq!(s1.inner.clock.time(), 15.into(), "should update clock");
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the join intent reset leaving
pub async fn join_intent_reset_leaving<T>(
  transport_opts: T::Options,
  addr: <T::Resolver as AddressResolver>::ResolvedAddress,
) where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  {
    let mut members = s1.inner.members.write().await;
    members.states.insert(
      "test".into(),
      MemberState {
        member: Member {
          node: Node::new("test".into(), addr),
          tags: Arc::new(Default::default()),
          status: MemberStatus::Leaving,
          memberlist_protocol_version: ruserf_types::MemberlistProtocolVersion::V1,
          memberlist_delegate_version: ruserf_types::MemberlistDelegateVersion::V1,
          protocol_version: ruserf_types::ProtocolVersion::V1,
          delegate_version: ruserf_types::DelegateVersion::V1,
        },
        status_time: 12.into(),
        leave_time: None,
      },
    );
  }

  let j = JoinMessage {
    ltime: 14.into(),
    id: "test".into(),
  };

  assert!(!s1.handle_node_join_intent(&j).await, "should rebroadcast");

  {
    let members = s1.inner.members.read().await;
    assert!(
      recent_intent(&members.recent_intents, &"test".into(), MessageType::Join).is_none(),
      "should not have buffered intent"
    );

    let m = members.states.get("test").unwrap();
    assert_eq!(m.status_time, 14.into(), "should update join time");
    assert_eq!(m.member.status, MemberStatus::Alive, "should update status");
    assert_eq!(s1.inner.clock.time(), 15.into(), "should update clock");
  }

  s1.shutdown().await.unwrap();
}

/// Unit tests for the user event old message
pub async fn user_event_old_message<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let event_buffer = opts.event_buffer_size;
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();

  // increase the ltime artificially
  s1.inner
    .clock
    .witness(((event_buffer + 1000) as u64).into());
  assert!(
    !s1
      .handle_user_event(
        UserEventMessage::default()
          .with_ltime(1.into())
          .with_name("old".into())
      )
      .await,
    "should not rebroadcast"
  );
  s1.shutdown().await.unwrap();
}

/// Unit tests for the user event smae clock
pub async fn user_event_same_clock<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts, opts, event_tx)
    .await
    .unwrap();

  let msg = UserEventMessage::default()
    .with_ltime(1.into())
    .with_name("first".into())
    .with_payload(Bytes::from_static(b"test"));
  assert!(s1.handle_user_event(msg).await, "should rebroadcast");

  let msg = UserEventMessage::default()
    .with_ltime(1.into())
    .with_name("first".into())
    .with_payload(Bytes::from_static(b"newpayload"));
  assert!(s1.handle_user_event(msg).await, "should rebroadcast");

  let msg = UserEventMessage::default()
    .with_ltime(1.into())
    .with_name("second".into())
    .with_payload(Bytes::from_static(b"other"));
  assert!(s1.handle_user_event(msg).await, "should rebroadcast");

  test_user_events(
    event_rx.rx,
    ["first", "first", "second"]
      .into_iter()
      .map(Into::into)
      .collect(),
    ["test", "newpayload", "other"]
      .into_iter()
      .map(Into::into)
      .collect(),
  )
  .await;

  s1.shutdown().await.unwrap();
}

/// Unit tests for the query old message
pub async fn query_old_message<T>(
  transport_opts: T::Options,
  from: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport,
{
  let opts = test_config();
  let event_buffer = opts.query_buffer_size;
  let s1 = Serf::<T>::new(transport_opts, opts).await.unwrap();
  // increase the ltime artificially
  s1.inner
    .clock
    .witness(((event_buffer + 1000) as u64).into());
  assert!(
    !s1
      .handle_query(
        QueryMessage {
          ltime: 1.into(),
          id: 0,
          from,
          filters: Default::default(),
          flags: QueryFlag::empty(),
          relay_factor: 0,
          timeout: Default::default(),
          name: "old".into(),
          payload: Bytes::new(),
        },
        None
      )
      .await,
    "should not rebroadcast"
  );

  s1.shutdown().await.unwrap();
}

/// Unit tests for the query same clock
pub async fn query_same_clock<T>(
  transport_opts: T::Options,
  from: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
) where
  T: Transport,
{
  let opts = test_config();
  let (event_tx, event_rx) = EventProducer::bounded(4);
  let s1 = Serf::<T>::with_event_producer(transport_opts, opts, event_tx)
    .await
    .unwrap();

  let msg = QueryMessage {
    ltime: 1.into(),
    id: 1,
    from: from.clone(),
    filters: Default::default(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "foo".into(),
    payload: Bytes::from_static(b"test"),
  };

  assert!(
    s1.handle_query(msg.clone(), None).await,
    "should rebroadcast"
  );
  assert!(
    !s1.handle_query(msg.clone(), None).await,
    "should not rebroadcast"
  );

  let msg = QueryMessage {
    ltime: 1.into(),
    id: 2,
    from: from.clone(),
    filters: Default::default(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "bar".into(),
    payload: Bytes::from_static(b"newpayload"),
  };

  assert!(
    s1.handle_query(msg.clone(), None).await,
    "should rebroadcast"
  );
  assert!(
    !s1.handle_query(msg.clone(), None).await,
    "should not rebroadcast"
  );

  let msg = QueryMessage {
    ltime: 1.into(),
    id: 3,
    from: from.clone(),
    filters: Default::default(),
    flags: QueryFlag::empty(),
    relay_factor: 0,
    timeout: Default::default(),
    name: "baz".into(),
    payload: Bytes::from_static(b"other"),
  };
  assert!(
    s1.handle_query(msg.clone(), None).await,
    "should rebroadcast"
  );
  assert!(
    !s1.handle_query(msg.clone(), None).await,
    "should not rebroadcast"
  );

  test_query_events(
    event_rx.rx,
    ["foo", "bar", "baz"].into_iter().map(Into::into).collect(),
    ["test", "newpayload", "other"]
      .into_iter()
      .map(Into::into)
      .collect(),
  )
  .await;

  s1.shutdown().await.unwrap();
}
