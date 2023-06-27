use std::{
  future::Future,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
  },
  time::Duration,
};

use super::*;

use agnostic::Runtime;
use arc_swap::{ArcSwap, ArcSwapOption};
use async_lock::{Mutex, RwLock};
use showbiz_core::{
  bytes::{BufMut, BytesMut},
  futures_util::Stream,
  queue::{NodeCalculator, TransmitLimitedQueue},
  tracing,
  transport::Transport,
  Name, Showbiz,
};

use crate::{
  broadcast::SerfBroadcast,
  clock::LamportClock,
  delegate::{MergeDelegate, SerfDelegate},
  error::Error,
  query::{QueryParam, QueryResponse},
  types::{encode_message, MessageUserEvent, QueryFlag, QueryMessage},
  Options,
};

const MAGIC_BYTE: u8 = 255;

pub(crate) struct SerfNodeCalculator {
  num_nodes: Arc<AtomicU32>,
}

impl NodeCalculator for SerfNodeCalculator {
  fn num_nodes(&self) -> usize {
    self.num_nodes.load(Ordering::SeqCst) as usize
  }
}

pub(crate) struct SerfQueryCore {
  responses: Arc<RwLock<HashMap<LamportTime, QueryResponse>>>,
}

#[viewit::viewit]
pub(crate) struct SerfCore<D: MergeDelegate, T: Transport> {
  clock: LamportClock,
  event_clock: LamportClock,
  query_clock: LamportClock,

  broadcasts: TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>,
  // opts: Options,
  memberlist: Showbiz<SerfDelegate<D, T>, T>,
  members: RwLock<HashMap<Name, MemberState>>,

  merge_delegate: Option<D>,

  event_broadcasts: TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>,

  query_core: SerfQueryCore,

  opts: Options<T>,

  /// The tags for this role, if any. This is used to provide arbitrary
  /// key/value metadata per-node. For example, a "role" tag may be used to
  /// differentiate "load-balancer" from a "web" role as parts of the same cluster.
  /// Tags are deprecating 'Role', and instead it acts as a special key in this
  /// map.
  tags: ArcSwap<HashMap<String, String>>,
}

/// Serf is a single node that is part of a single cluster that gets
/// events about joins/leaves/failures/etc. It is created with the Create
/// method.
///
/// All functions on the Serf structure are safe to call concurrently.
#[repr(transparent)]
pub struct Serf<D: MergeDelegate, T: Transport> {
  pub(crate) inner: Arc<SerfCore<D, T>>,
}

impl<D: MergeDelegate, T: Transport> Clone for Serf<D, T> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

// ------------------------------------Public methods------------------------------------
impl<D, T> Serf<D, T>
where
  D: MergeDelegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  /// A predicate that determines whether or not encryption
  /// is enabled, which can be possible in one of 2 cases:
  ///   - Single encryption key passed at agent start (no persistence)
  ///   - Keyring file provided at agent start
  #[inline]
  pub fn encryption_enabled(&self) -> bool {
    self.inner.memberlist.encryption_enabled()
  }

  /// Returns a point-in-time snapshot of the members of this cluster.
  #[inline]
  pub async fn members(&self) -> usize {
    self.inner.members.read().await.len()
  }

  /// Used to dynamically update the tags associated with
  /// the local node. This will propagate the change to the rest of
  /// the cluster. Blocks until a the message is broadcast out.
  #[inline]
  pub async fn set_tags(&self, tags: HashMap<String, String>) -> Result<(), Error<D, T>> {
    // Check that the meta data length is okay
    if self.encode_tags(&tags)?.len() > showbiz_core::META_MAX_SIZE {
      return Err(Error::TagsTooLarge(showbiz_core::META_MAX_SIZE));
    }
    // update the config
    self.inner.tags.store(Arc::new(tags));

    // trigger a memberlist update
    self
      .inner
      .memberlist
      .update_node(self.inner.opts.broadcast_timeout)
      .await
      .map_err(From::from)
  }

  /// Used to broadcast a custom user event with a given
  /// name and payload. If the configured size limit is exceeded and error will be returned.
  /// If coalesce is enabled, nodes are allowed to coalesce this event.
  #[inline]
  pub async fn user_event(
    &self,
    name: Name,
    payload: Bytes,
    coalesce: bool,
  ) -> Result<(), Error<D, T>> {
    let payload_size_before_encoding = name.len() + payload.len();

    // Check size before encoding to prevent needless encoding and return early if it's over the specified limit.
    if payload_size_before_encoding > self.inner.opts.max_user_event_size {
      return Err(Error::UserEventTooLarge(payload_size_before_encoding));
    }

    if payload_size_before_encoding > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventTooLarge(payload_size_before_encoding));
    }

    // Create a message
    let msg = MessageUserEvent {
      ltime: self.inner.event_clock.time(),
      name: name.clone(),
      payload,
      cc: coalesce,
    };

    // Start broadcasting the event
    let raw = encode_message(MessageType::UserEvent, &msg)?;

    // Check the size after encoding to be sure again that
    // we're not attempting to send over the specified size limit.
    if raw.len() > self.inner.opts.max_user_event_size {
      return Err(Error::RawUserEventTooLarge(raw.len()));
    }

    if raw.len() > USER_EVENT_SIZE_LIMIT {
      return Err(Error::RawUserEventTooLarge(raw.len()));
    }

    self.inner.event_clock.increment();

    // Process update locally
    self.handle_user_event().await;

    self
      .inner
      .event_broadcasts
      .queue_broadcast(SerfBroadcast {
        name,
        msg: raw,
        notify_tx: ArcSwapOption::from_pointee(None),
      })
      .await;
    Ok(())
  }

  /// Used to broadcast a new query. The query must be fairly small,
  /// and an error will be returned if the size limit is exceeded. This is only
  /// available with protocol version 4 and newer. Query parameters are optional,
  /// and if not provided, a sane set of defaults will be used.
  pub async fn query(
    &self,
    name: Name,
    payload: Bytes,
    params: Option<QueryParam>,
  ) -> Result<QueryResponse, Error<D, T>> {
    // Provide default parameters if none given.
    let params = match params {
      Some(params) => params,
      None => self.default_query_param().await,
    };

    // Get the local node
    let local = self.inner.memberlist.local_node().await;

    // Encode the filters
    let filters = params.encode_filters()?;

    // Setup the flags
    let flags = if params.request_ack {
      QueryFlag::empty()
    } else {
      QueryFlag::ACK
    };

    // Create the message
    let q = QueryMessage {
      ltime: self.inner.query_clock.time(),
      id: rand::random(),
      from: local.id().clone(),
      filters,
      flags: flags.bits(),
      relay_factor: params.relay_factor,
      timeout: params.timeout,
      name: name.clone(),
      payload,
    };

    // Encode the query
    let raw = encode_message(MessageType::Query, &q)?;

    // Check the size
    if raw.len() > self.inner.opts.query_size_limit {
      return Err(Error::QueryTooLarge(raw.len()));
    }

    // Register QueryResponse to track acks and responses
    let resp = q.response(self.inner.memberlist.num_members().await);
    self
      .register_query_response(params.timeout, resp.clone())
      .await;

    // Process query locally
    self.handle_query(q).await;

    // Start broadcasting the event
    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
        name,
        msg: raw,
        notify_tx: ArcSwapOption::from_pointee(None),
      })
      .await;
    Ok(resp)
  }
}

// ---------------------------------Private Methods-------------------------------

impl<D: MergeDelegate, T: Transport> Serf<D, T> {
  async fn handle_user_event(&self) -> bool {
    // TODO: implement this method
    true
  }

  async fn handle_query(&self, q: QueryMessage) {
    todo!()
  }

  /// Used to setup the listeners for the query,
  /// and to schedule closing the query after the timeout.
  async fn register_query_response(&self, timeout: Duration, resp: QueryResponse) {
    let tresps = self.inner.query_core.responses.clone();
    let mut resps = self.inner.query_core.responses.write().await;
    // Map the LTime to the QueryResponse. This is necessarily 1-to-1,
    // since we increment the time for each new query.
    let ltime = resp.ltime;
    resps.insert(ltime, resp);

    // Setup a timer to close the response and deregister after the timeout
    <T::Runtime as Runtime>::delay(timeout, async move {
      let mut resps = tresps.write().await;
      if let Some(resp) = resps.remove(&ltime) {
        resp.close().await;
      }
    });
  }

  /// Used to encode a tag map
  fn encode_tags(&self, tag: &HashMap<String, String>) -> Result<Bytes, Error<D, T>> {
    struct EncodeHelper(BytesMut);

    impl std::io::Write for EncodeHelper {
      fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.put_slice(buf);
        Ok(buf.len())
      }

      fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
      }
    }

    let mut buf = EncodeHelper(BytesMut::with_capacity(128));
    // Use a magic byte prefix and msgpack encode the tags
    buf.0.put_u8(MAGIC_BYTE);
    match tag.serialize(&mut rmp_serde::Serializer::new(&mut buf)) {
      Ok(_) => Ok(buf.0.freeze()),
      Err(e) => {
        tracing::error!(target = "reserf", err=%e, "failed to encode tags");
        Err(Error::Encode(e))
      }
    }
  }

  /// Used to decode a tag map
  fn decode_tags(&self, src: &[u8]) -> HashMap<String, String> {
    // Decode the tags
    let r = std::io::Cursor::new(&src[1..]);
    let mut de = rmp_serde::Deserializer::new(r);
    match HashMap::<String, String>::deserialize(&mut de) {
      Ok(tags) => tags,
      Err(e) => {
        tracing::error!(target = "reserf", err=%e, "failed to decode tags");
        HashMap::new()
      }
    }
  }
}
