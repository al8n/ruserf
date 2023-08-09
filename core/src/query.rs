use std::{
  collections::{HashMap, HashSet},
  sync::{atomic::Ordering, Arc},
  time::{Duration, Instant},
};

use agnostic::Runtime;
use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use serde::{Deserialize, Serialize};
use showbiz_core::{
  bytes::Bytes,
  futures_util::{self, FutureExt},
  humantime_serde, tracing,
  transport::Transport,
  Name, NodeId,
};

use crate::{
  clock::LamportTime,
  delegate::MergeDelegate,
  error::Error,
  types::{
    decode_message, encode_filter, encode_relay_message, FilterType, FilterTypeRef, MessageType,
    QueryResponseMessage, Tag, TagRef,
  },
  Member, MemberStatus, ReconnectTimeoutOverrider, Serf,
};

/// Provided to [`Serf::query`] to configure the parameters of the
/// query. If not provided, sane defaults will be used.
#[viewit::viewit]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
  /// If provided, we restrict the nodes that should respond to those
  /// with names in this list
  filter_nodes: Vec<NodeId>,

  /// Maps a tag name to a regular expression that is applied
  /// to restrict the nodes that should respond
  filter_tags: HashMap<String, String>,

  /// If true, we are requesting an delivery acknowledgement from
  /// every node that meets the filter requirement. This means nodes
  /// the receive the message but do not pass the filters, will not
  /// send an ack.
  request_ack: bool,

  /// RelayFactor controls the number of duplicate responses to relay
  /// back to the sender through other nodes for redundancy.
  relay_factor: u8,

  /// The timeout limits how long the query is left open. If not provided,
  /// then a default timeout is used based on the configuration of Serf
  #[serde(with = "humantime_serde")]
  timeout: Duration,
}

impl QueryParam {
  /// Used to convert the filters into the wire format
  pub(crate) fn encode_filters(&self) -> Result<Vec<Bytes>, rmp_serde::encode::Error> {
    let mut filters = Vec::with_capacity(self.filter_nodes.len() + self.filter_tags.len());

    // Add the node filter
    if !self.filter_nodes.is_empty() {
      let filter = FilterTypeRef::Node(&self.filter_nodes);
      filters.push(encode_filter(filter)?);
    }

    // Add the tag filter
    for (t, expr) in self.filter_tags.iter() {
      let filter = FilterTypeRef::Tag(TagRef { tag: t, expr });
      filters.push(encode_filter(filter)?);
    }

    Ok(filters)
  }
}

struct QueryResponseChannel {
  /// Used to send the name of a node for which we've received an ack
  ack_ch: Option<(Sender<NodeId>, Receiver<NodeId>)>,
  /// Used to send a response from a node
  resp_ch: (Sender<NodeResponse>, Receiver<NodeResponse>),
}

struct QueryResponseCore {
  closed: bool,
}

pub(crate) struct QueryResponseInner {
  core: Mutex<QueryResponseCore>,
  channel: QueryResponseChannel,
}

/// Returned for each new Query. It is used to collect
/// Ack's as well as responses and to provide those back to a client.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Clone)]
pub struct QueryResponse {
  /// The duration of the query
  deadline: Instant,

  /// The query id
  id: u32,

  /// Stores the LTime of the query
  ltime: LamportTime,

  #[viewit(getter(vis = "pub(crate)", const, style = "ref"))]
  inner: Arc<QueryResponseInner>,

  #[viewit(getter(skip))]
  acks: HashSet<NodeId>,

  #[viewit(getter(skip))]
  responses: HashSet<NodeId>,
}

impl QueryResponse {
  #[inline]
  pub fn new(id: u32, ltime: LamportTime, num_nodes: usize, deadline: Instant, ack: bool) -> Self {
    let (ack_ch, acks) = if ack {
      (
        Some(async_channel::bounded(num_nodes)),
        HashSet::with_capacity(num_nodes),
      )
    } else {
      (None, HashSet::new())
    };

    Self {
      deadline,
      id,
      ltime,
      inner: Arc::new(QueryResponseInner {
        core: Mutex::new(QueryResponseCore {
          closed: false,
        }),
        channel: QueryResponseChannel {
          ack_ch,
          resp_ch: async_channel::bounded(num_nodes),
        },
      }),
      acks,
      responses: HashSet::with_capacity(num_nodes),
    }
  }

  /// Returns a channel that can be used to listen for acks.
  /// Channel will be closed when the query is finished. This is nil,
  /// if the query did not specify RequestAck.
  #[inline]
  pub fn ack_rx(&self) -> Option<async_channel::Receiver<NodeId>> {
    self.inner.channel.ack_ch.as_ref().map(|(_, r)| r.clone())
  }

  /// Returns a channel that can be used to listen for responses.
  /// Channel will be closed when the query is finished.
  #[inline]
  pub fn response_rx(&self) -> async_channel::Receiver<NodeResponse> {
    self.inner.channel.resp_ch.1.clone()
  }

  /// Returns if the query is finished running
  #[inline]
  pub async fn finished(&self) -> bool {
    let c = self.inner.core.lock().await;
    c.closed || (Instant::now() > self.deadline)
  }

  /// Used to close the query, which will close the underlying
  /// channels and prevent further deliveries
  #[inline]
  pub async fn close(&self) {
    let mut c = self.inner.core.lock().await;
    if c.closed {
      return;
    }

    c.closed = true;

    if let Some((tx, _)) = &self.inner.channel.ack_ch {
      tx.close();
    }

    self.inner.channel.resp_ch.0.close();
  }

  /// Sends a response on the response channel ensuring the channel is not closed.
  #[inline]
  pub(crate) async fn send_response<T, D, O>(&mut self, nr: NodeResponse) -> Result<(), Error<T, D, O>>
  where
    D: MergeDelegate,
    T: Transport,
    O: ReconnectTimeoutOverrider,
  {
    let c = self.inner.core.lock().await;
    if c.closed {
      return Ok(());
    }

    let id = nr.from.clone();
    futures_util::select! {
      _ = self.inner.channel.resp_ch.0.send(nr).fuse() => {
        self.responses.insert(id);
      },
      default => {
        return Err(Error::QueryResponseDeliveryFailed);
      }
    }
    Ok(())
  }

  /// Sends a response on the ack channel ensuring the channel is not closed.
  #[inline]
  pub(crate) async fn send_ack<T, D, O>(
    &mut self,
    nr: &QueryResponseMessage,
  ) -> Result<(), Error<T, D, O>>
  where
    D: MergeDelegate,
    T: Transport,
    O: ReconnectTimeoutOverrider,
  {
    let c = self.inner.core.lock().await;
    if c.closed {
      return Ok(());
    }

    if let Some((tx, _)) = &self.inner.channel.ack_ch {
      futures_util::select! {
        _ = tx.send(nr.from.clone()).fuse() => {
          self.acks.insert(nr.from.clone());
        },
        default => {
          return Err(Error::QueryResponseDeliveryFailed);
        }
      }
    }
    Ok(())
  }

  #[inline]
  pub(crate) fn acks(&self) {
    // self.inner.
  }
}

/// Used to represent a single response from a node
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NodeResponse {
  from: NodeId,
  payload: Bytes,
}

impl<T, D, O> Serf<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as std::future::Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as futures_util::Stream>::Item: Send,
{
  /// Returns the default timeout value for a query
  /// Computed as
  /// ```text
  /// gossip_interval * query_timeout_mult * log(N+1)
  /// ```
  pub async fn default_query_timeout(&self) -> Duration {
    let n = self.inner.memberlist.alive_members().await;
    let mut timeout = self.inner.opts.showbiz_options.gossip_interval();
    timeout *= self.inner.opts.query_timeout_mult as u32;
    timeout *= (f64::log10((n + 1) as f64) + 0.5) as u32; // Using ceil approximation
    timeout
  }

  /// Used to return the default query parameters
  pub async fn default_query_param(&self) -> QueryParam {
    QueryParam {
      filter_nodes: vec![],
      filter_tags: HashMap::new(),
      request_ack: false,
      relay_factor: 0,
      timeout: self.default_query_timeout().await,
    }
  }

  pub(crate) fn should_process_query(&self, filters: &[Bytes]) -> bool {
    for filter in filters.iter() {
      if filter.is_empty() {
        tracing::warn!(target = "ruserf", "empty filter");
        return false;
      }

      match filter[0] {
        FilterType::NODE => {
          // Decode the filter
          let nodes = match decode_message::<Vec<NodeId>>(&filter[1..]) {
            Ok(nodes) => nodes,
            Err(err) => {
              tracing::warn!(target = "ruserf", err=%err, "failed to decode node filter");
              return false;
            }
          };

          // Check if we are being targeted
          let found = nodes
            .iter()
            .any(|n| n.name() == self.inner.opts.showbiz_options.name());
          if !found {
            return false;
          }
        }
        FilterType::TAG => {
          // Decode the filter
          let filter = match decode_message::<Tag>(&filter[1..]) {
            Ok(filter) => filter,
            Err(err) => {
              tracing::warn!(target = "ruserf", err=%err, "failed to decode tag filter");
              return false;
            }
          };

          // Check if we match this regex
          if let Some(tags) = &*self.inner.opts.tags.load() {
            if let Some(expr) = tags.get(&filter.tag) {
              match regex::Regex::new(&filter.expr) {
                Ok(re) => {
                  if !re.is_match(expr) {
                    return false;
                  }
                }
                Err(err) => {
                  tracing::warn!(target = "ruserf", err=%err, "failed to compile filter regex ({})", filter.expr);
                  return false;
                }
              }
            } else {
              return false;
            }
          } else {
            return false;
          }
        }
        val => {
          tracing::warn!(
            target = "ruserf",
            "query has unrecognized filter type {}",
            val
          );
          return false;
        }
      }
    }
    true
  }

  pub(crate) async fn relay_response(
    &self,
    relay_factor: u8,
    node_id: NodeId,
    resp: QueryResponseMessage,
  ) -> Result<(), Error<T, D, O>> {
    if relay_factor == 0 {
      return Ok(());
    }

    // Needs to be worth it; we need to have at least relayFactor *other*
    // nodes. If you have a tiny cluster then the relayFactor shouldn't
    // be needed.
    let members = {
      let members = self.inner.members.read().await;
      if members.states.len() < relay_factor as usize + 1 {
        return Ok(());
      }
      members
        .states
        .iter()
        .filter_map(|(id, m)| {
          if m.member.status.load(Ordering::SeqCst) != MemberStatus::Alive
            || id == self.inner.memberlist.local_id()
          {
            None
          } else {
            Some(m.member.clone())
          }
        })
        .collect::<Vec<_>>()
    };

    if members.is_empty() {
      return Ok(());
    }

    // Prep the relay message, which is a wrapped version of the original.
    let raw = encode_relay_message(MessageType::QueryResponse, node_id, &resp)?;

    if raw.len() > self.inner.opts.query_response_size_limit {
      return Err(Error::RelayedResponseTooLarge(
        self.inner.opts.query_response_size_limit,
      ));
    }

    // Relay to a random set of peers.
    let relay_members = random_members(relay_factor as usize, members);

    let futs = relay_members.into_iter().map(|m| {
      // TODO: make send to accept Arc<Message> to avoid cloning
      let raw = raw.clone();
      async move {
        self
          .inner
          .memberlist
          .send(&m.id, raw)
          .await
          .map_err(|e| (m, e))
      }
    });

    let errs = futures_util::future::join_all(futs)
      .await
      .into_iter()
      .filter_map(|rst| {
        if let Err((m, e)) = rst {
          Some((m, e))
        } else {
          None
        }
      })
      .collect::<Vec<_>>();

    if !errs.is_empty() {
      return Err(Error::Relay(From::from(errs)));
    }

    Ok(())
  }
}

#[inline]
fn random_members(k: usize, mut members: Vec<Arc<Member>>) -> Vec<Arc<Member>> {
  let n = members.len();
  if n == 0 {
    return Vec::new();
  }

  // The modified Fisher-Yates algorithm, but up to 3*n times to ensure exhaustive search for small n.
  let rounds = 3 * n;
  let mut i = 0;

  while i < rounds && i < n {
    let j = rand::random::<usize>() % (n - i) + i;
    members.swap(i, j);
    i += 1;
    if i >= k && i >= rounds {
      break;
    }
  }

  members.truncate(k);
  members
}
