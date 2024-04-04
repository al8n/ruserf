use ruserf_types::{Filter, FilterType};

use super::*;

/// Unit test for default query
pub async fn default_query<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();
  let timeout = s.default_query_timeout().await;
  assert_eq!(
    timeout,
    s.inner.opts.memberlist_options.gossip_interval() * (s.inner.opts.query_timeout_mult as u32)
  );

  let params = s.default_query_param().await;

  assert!(params.filters.is_empty());
  assert!(!params.request_ack);
  assert_eq!(params.timeout, timeout);
}

/// Unit test for query params encode filters
pub async fn query_params_encode_filters<T>(transport_opts: T::Options)
where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();
  let mut params = s.default_query_param().await;
  params
    .filters
    .push(Filter::Id(["foo".into(), "bar".into()].into()));
  params.filters.push(Filter::Tag {
    tag: "role".into(),
    expr: "^web".into(),
  });
  params.filters.push(Filter::Tag {
    tag: "datacenter".into(),
    expr: "aws$".into(),
  });

  let filters = params.encode_filters::<DefaultDelegate<T>>().unwrap();
  assert_eq!(filters.len(), 3);

  let node_filt = &filters[0];
  assert_eq!(node_filt[0], FilterType::Id as u8);

  let tag_filt = &filters[1];
  assert_eq!(tag_filt[0], FilterType::Tag as u8);

  let tag_filt = &filters[2];
  assert_eq!(tag_filt[0], FilterType::Tag as u8);
}

/// Unit test for should process functionallity
pub async fn should_process<T>(transport_opts: T::Options)
where
  T: Transport<Id = SmolStr>,
{
  let opts = test_config();
  let s = Serf::<T>::new(
    transport_opts,
    opts.with_tags([("role", "webserver"), ("datacenter", "east-aws")].into_iter()),
  )
  .await
  .unwrap();
  let mut params = s.default_query_param().await;
  params.filters.push(Filter::Id(
    [
      "foo".into(),
      "bar".into(),
      s.memberlist().local_id().clone(),
    ]
    .into_iter()
    .collect(),
  ));
  params.filters.push(Filter::Tag {
    tag: "role".into(),
    expr: "^web".into(),
  });
  params.filters.push(Filter::Tag {
    tag: "datacenter".into(),
    expr: "aws$".into(),
  });

  let filters = params.encode_filters::<DefaultDelegate<T>>().unwrap();
  assert_eq!(filters.len(), 3);

  assert!(s.should_process_query(&filters));

  // Omit node
  let mut params = s.default_query_param().await;
  params
    .filters
    .push(Filter::Id(["foo".into(), "bar".into()].into()));

  let filters = params.encode_filters::<DefaultDelegate<T>>().unwrap();
  assert!(!s.should_process_query(&filters));

  // Filter on missing tag
  let mut params = s.default_query_param().await;
  params.filters.push(Filter::Tag {
    tag: "other".into(),
    expr: "cool".into(),
  });

  let filters = params.encode_filters::<DefaultDelegate<T>>().unwrap();
  assert!(!s.should_process_query(&filters));

  // Bad tag
  let mut params = s.default_query_param().await;
  params.filters.push(Filter::Tag {
    tag: "role".into(),
    expr: "db".into(),
  });

  let filters = params.encode_filters::<DefaultDelegate<T>>().unwrap();
  assert!(!s.should_process_query(&filters));
}

/// Unit test for serf query
pub async fn serf_query<T>(transport_opts1: T::Options, transport_opts2: T::Options)
where
  T: Transport,
{
  let (event_tx, event_rx) = EventProducer::bounded(4);

  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();

  let wg = Arc::new(());
  let wg1 = wg.clone();
  let (ctx_tx, ctx_rx) = async_channel::bounded::<()>(1);
  scopeguard::defer!(ctx_tx.close(););

  <T::Runtime as RuntimeLite>::spawn_detach(async move {
    let _wg = wg1;
    loop {
      futures::select! {
        _ = ctx_rx.recv().fuse() => {
          break;
        },
        e = event_rx.rx.recv().fuse() => {
          let e = e.unwrap();
          match e.ty() {
            CrateEventType::Query => {
              match e {
                CrateEvent::Query(q) => {
                  q.respond(Bytes::from_static(b"test")).await.unwrap();
                  break;
                }
                _ => unreachable!(),
              }
            },
            _ => continue,
          }
        },
        _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).fuse() => {
          tracing::error!("timeout");
          break;
        },
      }
    }
  });

  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let serfs = [s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  // Start a query from s2;
  let mut params = serfs[1].default_query_param().await;
  params.request_ack = true;
  let resp = serfs[1]
    .query("load", Bytes::from_static(b"sup girl"), Some(params))
    .await
    .unwrap();

  let mut acks = vec![];
  let mut responses = vec![];

  let ack_rx = resp.ack_rx().unwrap();
  let resp_rx = resp.response_rx();

  for _ in 0..3 {
    futures::select! {
      a = ack_rx.recv().fuse() => {
        let a = a.unwrap();
        acks.push(a);
      },
      r = resp_rx.recv().fuse() => {
        let r = r.unwrap();
        assert_eq!(r.from, serfs[0].advertise_node());
        assert_eq!(r.payload, Bytes::from_static(b"test"));
        responses.push(r);
      },
      _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).fuse() => {
        panic!("timeout");
      },
    }
  }

  assert_eq!(acks.len(), 2, "missing acks {acks:?}");
  assert_eq!(responses.len(), 1, "missing responses {responses:?}");
}

/// Unit test for serf query filter
pub async fn serf_query_filter<T>(
  transport_opts1: T::Options,
  transport_opts2: T::Options,
  transport_opts3: T::Options,
) where
  T: Transport,
{
  let (event_tx, event_rx) = EventProducer::bounded(4);

  let s1 = Serf::<T>::with_event_producer(transport_opts1, test_config(), event_tx)
    .await
    .unwrap();

  let wg = Arc::new(());
  let wg1 = wg.clone();
  let (ctx_tx, ctx_rx) = async_channel::bounded::<()>(1);
  scopeguard::defer!(ctx_tx.close(););

  <T::Runtime as RuntimeLite>::spawn_detach(async move {
    let _wg = wg1;
    loop {
      futures::select! {
        _ = ctx_rx.recv().fuse() => {
          break;
        },
        e = event_rx.rx.recv().fuse() => {
          let e = e.unwrap();
          match e.ty() {
            CrateEventType::Query => {
              match e {
                CrateEvent::Query(q) => {
                  q.respond(Bytes::from_static(b"test")).await.unwrap();
                  break;
                }
                _ => unreachable!(),
              }
            },
            _ => continue,
          }
        },
        _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).fuse() => {
          tracing::error!("timeout");
          break;
        },
      }
    }
  });

  let s2 = Serf::<T>::new(transport_opts2, test_config())
    .await
    .unwrap();

  let mut serfs = vec![s1, s2];
  wait_until_num_nodes(1, &serfs).await;

  let node = serfs[1]
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(2, &serfs).await;

  let s3 = Serf::<T>::new(transport_opts3, test_config())
    .await
    .unwrap();
  serfs.push(s3);

  wait_until_num_nodes(1, &serfs[2..]).await;

  let node = serfs[2]
    .advertise_node()
    .map_address(MaybeResolvedAddress::resolved);
  serfs[0].join(node, false).await.unwrap();

  wait_until_num_nodes(3, &serfs).await;

  // Filter to only s1!
  let mut params = serfs[1].default_query_param().await;
  params.filters.push(Filter::Id(
    [serfs[0].memberlist().local_id().clone()]
      .into_iter()
      .collect(),
  ));
  params.request_ack = true;
  params.relay_factor = 1;

  // Start a query from s2;
  let resp = serfs[1]
    .query("load", Bytes::from_static(b"sup girl"), Some(params))
    .await
    .unwrap();

  let mut acks = vec![];
  let mut responses = vec![];

  let ack_rx = resp.ack_rx().unwrap();
  let resp_rx = resp.response_rx();

  for _ in 0..2 {
    futures::select! {
      a = ack_rx.recv().fuse() => {
        let a = a.unwrap();
        acks.push(a);
      },
      r = resp_rx.recv().fuse() => {
        let r = r.unwrap();
        assert_eq!(r.from, serfs[0].advertise_node());
        assert_eq!(r.payload, Bytes::from_static(b"test"));
        responses.push(r);
      },
      _ = <T::Runtime as RuntimeLite>::sleep(Duration::from_secs(1)).fuse() => {
        panic!("timeout");
      },
    }
  }

  assert_eq!(acks.len(), 1, "missing acks {acks:?}");
  assert_eq!(responses.len(), 1, "missing responses {responses:?}");
}

/// Unit test for serf query deduplicate
pub async fn serf_query_deduplicate<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();

  // Set up a dummy query and response
  let mq = QueryMessage {
    ltime: 123.into(),
    id: 123,
    from: s.advertise_node(),
    filters: Default::default(),
    flags: QueryFlag::ACK,
    relay_factor: 0,
    timeout: Duration::from_secs(1),
    name: Default::default(),
    payload: Default::default(),
  };
  let query = QueryResponse::from_query(&mq, 3);
  let mut response = QueryResponseMessage {
    ltime: mq.ltime,
    id: mq.id,
    from: s.advertise_node(),
    flags: QueryFlag::empty(),
    payload: Default::default(),
  };
  {
    let mut qc = s.inner.query_core.write().await;
    qc.responses.insert(mq.ltime, query.clone());
  }

  // Send a few duplicate responses
  s.handle_query_response(response.clone()).await;
  s.handle_query_response(response.clone()).await;
  response.flags |= QueryFlag::ACK;
  s.handle_query_response(response.clone()).await;
  s.handle_query_response(response.clone()).await;

  // Ensure we only get one NodeResponse off the channel
  let resp_rx = query.response_rx();
  futures::select! {
    _ = resp_rx.recv().fuse() => {},
    default => {
      panic!("should have a response")
    }
  }

  let ack_rx = query.ack_rx().unwrap();
  futures::select! {
    _ = ack_rx.recv().fuse() => {},
    default => {
      panic!("should have an ack")
    }
  }

  futures::select! {
    _ = resp_rx.recv().fuse() => {
      panic!("should not have a second response")
    },
    default => {}
  }

  futures::select! {
    _ = ack_rx.recv().fuse() => {
      panic!("should not have a second ack")
    },
    default => {}
  }
}

/// Unit test for serf query size limit
pub async fn serf_query_size_limit<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let size_limit = opts.query_size_limit;
  let s = Serf::<T>::new(transport_opts, opts).await.unwrap();

  let name = "this is too large a query";
  let payload = vec![0; size_limit];
  let Err(err) = s.query(name, payload, None).await else {
    panic!("expected error");
  };
  assert!(err.to_string().contains("query exceeds limit of"));
}

/// Unit test for serf query size limit increased
pub async fn serf_query_size_limit_increased<T>(transport_opts: T::Options)
where
  T: Transport,
{
  let opts = test_config();
  let size_limit = opts.query_size_limit;
  let s = Serf::<T>::new(transport_opts, opts.with_query_size_limit(size_limit * 2))
    .await
    .unwrap();

  let name = "this is too large a query";
  let payload = vec![0; size_limit];
  s.query(name, payload, None).await.unwrap();
}
