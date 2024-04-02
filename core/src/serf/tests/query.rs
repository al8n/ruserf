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
  assert_eq!(timeout, s.inner.opts.memberlist_options.gossip_interval() * (s.inner.opts.query_timeout_mult as u32));

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
  params.filters.push(Filter::Id(["foo".into(), "bar".into()].into()));
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
  let s = Serf::<T>::new(transport_opts, opts.with_tags([("role", "webserver"), ("datacenter", "east-aws")].into_iter())).await.unwrap();
  let mut params = s.default_query_param().await;
  params.filters.push(Filter::Id(["foo".into(), "bar".into(), s.memberlist().local_id().clone()].into_iter().collect()));
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
  params.filters.push(Filter::Id(["foo".into(), "bar".into()].into()));

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


