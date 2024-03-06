use std::{
  collections::HashSet,
  future::Future,
  sync::{atomic::Ordering, Arc},
  time::{Duration, Instant},
};

use async_channel::{Receiver, Sender};
use async_lock::Mutex;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use memberlist_core::{
  agnostic::Runtime,
  bytes::{BufMut, Bytes, BytesMut},
  tracing,
  transport::{AddressResolver, Node, Transport},
  types::{OneOrMore, SmallVec, TinyVec},
  CheapClone,
};

use crate::{
  clock::LamportTime,
  delegate::{Delegate, TransformDelegate},
  error::Error,
  types::QueryResponseMessage,
  Filter, Member, MemberStatus, MessageType, Serf,
};

/// Provided to [`Serf::query`] to configure the parameters of the
/// query. If not provided, sane defaults will be used.
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryParam<I, A> {
  // /// If provided, we restrict the nodes that should respond to those
  // /// with names in this list
  // filter_nodes: Vec<Node<I, A>>,

  // /// Maps a tag name to a regular expression that is applied
  // /// to restrict the nodes that should respond
  // filter_tags: HashMap<SmolStr, SmolStr>,
  filters: OneOrMore<Filter<I, A>>,

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
  #[cfg_attr(feature = "serde", serde(with = "humantime_serde"))]
  timeout: Duration,
}

impl<I, A> QueryParam<I, A> {
  /// Used to convert the filters into the wire format
  pub(crate) fn encode_filters<W: TransformDelegate>(&self) -> Result<SmallVec<Bytes>, W::Error> {
    let mut filters = TinyVec::with_capacity(self.filters.len());
    for filter in self.filters.iter() {
      filters.push(W::encode_filter(filter)?);
    }

    Ok(filters)
  }
}

struct QueryResponseChannel<I, A> {
  /// Used to send the name of a node for which we've received an ack
  ack_ch: Option<(Sender<Node<I, A>>, Receiver<Node<I, A>>)>,
  /// Used to send a response from a node
  resp_ch: (Sender<NodeResponse<I, A>>, Receiver<NodeResponse<I, A>>),
}

pub(crate) struct QueryResponseCore<I, A> {
  closed: bool,
  acks: HashSet<Node<I, A>>,
  responses: HashSet<Node<I, A>>,
}

pub(crate) struct QueryResponseInner<I, A> {
  core: Mutex<QueryResponseCore<I, A>>,
  channel: QueryResponseChannel<I, A>,
}

/// Returned for each new Query. It is used to collect
/// Ack's as well as responses and to provide those back to a client.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Clone)]
pub struct QueryResponse<I, A> {
  /// The duration of the query
  deadline: Instant,

  /// The query id
  id: u32,

  /// Stores the LTime of the query
  ltime: LamportTime,

  #[viewit(getter(vis = "pub(crate)", const, style = "ref"))]
  inner: Arc<QueryResponseInner<I, A>>,
}

impl<I, A> QueryResponse<I, A> {
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
          acks,
          responses: HashSet::with_capacity(num_nodes),
        }),
        channel: QueryResponseChannel {
          ack_ch,
          resp_ch: async_channel::bounded(num_nodes),
        },
      }),
    }
  }

  /// Returns a channel that can be used to listen for acks.
  /// Channel will be closed when the query is finished. This is nil,
  /// if the query did not specify RequestAck.
  #[inline]
  pub fn ack_rx(&self) -> Option<async_channel::Receiver<Node<I, A>>> {
    self.inner.channel.ack_ch.as_ref().map(|(_, r)| r.clone())
  }

  /// Returns a channel that can be used to listen for responses.
  /// Channel will be closed when the query is finished.
  #[inline]
  pub fn response_rx(&self) -> async_channel::Receiver<NodeResponse<I, A>> {
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

  #[inline]
  pub(crate) async fn handle_query_response<T, D>(&self, resp: QueryResponseMessage<I, A>)
  where
    I: Eq + std::hash::Hash + CheapClone,
    A: Eq + std::hash::Hash + CheapClone,
    D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
    T: Transport,
  {
    // Check if the query is closed
    let mut c = self.inner.core.lock().await;
    if c.closed || (Instant::now() > self.deadline) {
      return;
    }

    // Process each type of response
    if resp.ack() {
      if let Some(Err(e)) = self.send_ack::<T, D>(&mut *c, &resp).await {
        tracing::warn!(target = "ruserf", "{}", e);
      }
    } else if let Some(Err(e)) = self
      .send_response::<T, D>(
        &mut *c,
        NodeResponse {
          from: resp.from,
          payload: resp.payload,
        },
      )
      .await
    {
      tracing::warn!(target = "ruserf", "{}", e);
    }
  }

  /// Sends a response on the response channel ensuring the channel is not closed.
  #[inline]
  pub(crate) async fn send_response<T, D>(
    &self,
    c: &mut QueryResponseCore<I, A>,
    nr: NodeResponse<I, A>,
  ) -> Option<Result<(), Error<T, D>>>
  where
    I: Eq + std::hash::Hash + CheapClone,
    A: Eq + std::hash::Hash + CheapClone,
    D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
    T: Transport,
  {
    // Exit early if this is a duplicate ack
    if c.responses.contains(&nr.from) {
      // TODO: metrics
      return None;
    }

    Some({
      // TODO: metrics
      if c.closed {
        Ok(())
      } else {
        let id = nr.from.cheap_clone();
        futures::select! {
          _ = self.inner.channel.resp_ch.0.send(nr).fuse() => {
            c.responses.insert(id);
            Ok(())
          },
          default => {
            Err(Error::QueryResponseDeliveryFailed)
          }
        }
      }
    })
  }

  /// Sends a response on the ack channel ensuring the channel is not closed.
  #[inline]
  pub(crate) async fn send_ack<T, D>(
    &self,
    c: &mut QueryResponseCore<I, A>,
    nr: &QueryResponseMessage<I, A>,
  ) -> Option<Result<(), Error<T, D>>>
  where
    I: Eq + std::hash::Hash + CheapClone,
    A: Eq + std::hash::Hash + CheapClone,
    D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
    T: Transport,
  {
    // Exit early if this is a duplicate ack
    if c.acks.contains(&nr.from) {
      // TODO: metrics
      return None;
    }

    Some({
      // TODO: metrics

      if c.closed {
        Ok(())
      } else if let Some((tx, _)) = &self.inner.channel.ack_ch {
        futures::select! {
          _ = tx.send(nr.from.clone()).fuse() => {
            c.acks.insert(nr.from.clone());
            Ok(())
          },
          default => {
            Err(Error::QueryResponseDeliveryFailed)
          }
        }
      } else {
        Ok(())
      }
    })
  }
}

/// Used to represent a single response from a node
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct NodeResponse<I, A> {
  from: Node<I, A>,
  payload: Bytes,
}

impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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
  pub async fn default_query_param(
    &self,
  ) -> QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
    QueryParam {
      filters: OneOrMore::new(),
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

      let filter = match <D as TransformDelegate>::decode_filter(&filter) {
        Ok(filter) => filter,
        Err(err) => {
          tracing::warn!(
            target = "ruserf",
            err = %err,
            "failed to decode filter"
          );
          return false;
        }
      };

      match filter {
        Filter::Node(nodes) => {
          // Check if we are being targeted
          let found = nodes
            .iter()
            .any(|n| n.id().eq(self.inner.memberlist.local_id()));
          if !found {
            return false;
          }
        }
        Filter::Tag(tag) => {
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
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
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
        .collect::<TinyVec<_>>()
    };

    if members.is_empty() {
      return Ok(());
    }

    // Prep the relay message, which is a wrapped version of the original.
    // let relay_msg = SerfRelayMessage::new(node, SerfMessage::QueryResponse(resp));
    let expected_encoded_len = <D as TransformDelegate>::node_encoded_len(&node)
      + <D as TransformDelegate>::message_encoded_len(&resp);
    if expected_encoded_len > self.inner.opts.query_response_size_limit {
      return Err(Error::RelayedResponseTooLarge(
        self.inner.opts.query_response_size_limit,
      ));
    }

    let mut raw = BytesMut::with_capacity(expected_encoded_len + 1 + 1); // +1 for relay message type byte, +1 for the message type byte
    raw.put_u8(MessageType::Relay as u8);
    raw.resize(expected_encoded_len + 1 + 1, 0);
    let mut encoded = 1;
    encoded += <D as TransformDelegate>::encode_node(&node, &mut raw[encoded..])?;
    raw[encoded] = MessageType::QueryResponse as u8;
    encoded += 1;
    encoded += <D as TransformDelegate>::encode_message(&resp, &mut raw[encoded..])?;

    debug_assert_eq!(
      encoded - 1 - 1,
      expected_encoded_len,
      "expected encoded len {} mismatch the actual encoded len {}",
      expected_encoded_len,
      encoded
    );

    let raw = Bytes::from(raw);
    // Relay to a random set of peers.
    let relay_members = random_members(relay_factor as usize, members);

    let mut futs = relay_members
      .into_iter()
      .map(|m| {
        let raw = raw.clone();
        async move {
          self
            .inner
            .memberlist
            .send(&m.id, raw)
            .await
            .map_err(|e| (m, e))
        }
      })
      .collect::<FuturesUnordered<_>>();

    let errs = futs
      .filter_map(|res| {
        if let Err((m, e)) = res {
          Some((m, e))
        } else {
          None
        }
      })
      .collect::<TinyVec<_>>()
      .await;

    if !errs.is_empty() {
      return Err(Error::Relay(From::from(errs)));
    }

    Ok(())
  }
}

#[inline]
fn random_members<I, A>(
  k: usize,
  mut members: SmallVec<Arc<Member<I, A>>>,
) -> SmallVec<Arc<Member<I, A>>> {
  let n = members.len();
  if n == 0 {
    return SmallVec::new();
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
