use std::{
  future::Future,
  sync::{
    atomic::{AtomicBool, AtomicU32, Ordering},
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
  metrics,
  queue::{NodeCalculator, TransmitLimitedQueue},
  tracing,
  transport::Transport,
  Address, Name, Showbiz, META_MAX_SIZE,
};
use smol_str::SmolStr;

use crate::{
  broadcast::SerfBroadcast,
  clock::LamportClock,
  coalesce::{coalesced_event, MemberEventCoalescer, UserEventCoalescer},
  coordinate::{Coordinate, CoordinateClient, CoordinateOptions},
  delegate::{MergeDelegate, SerfDelegate},
  error::{Error, JoinError},
  event::Event,
  internal_query::SerfQueries,
  query::{QueryParam, QueryResponse},
  snapshot::{open_and_replay_snapshot, SnapshotHandle},
  types::{encode_message, JoinMessage, MessageUserEvent, QueryFlag, QueryMessage},
  Options,
};

const MAGIC_BYTE: u8 = 255;
/// Maximum 128 KB snapshot
const SNAPSHOT_SIZE_LIMIT: usize = 128 * 1024;

/// Maximum 9KB for event name and payload
const USER_EVENT_SIZE_LIMIT: usize = 9 * 1024;

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

pub(crate) struct Members {
  pub(crate) states: HashMap<NodeId, MemberState>,
  recent_intents: HashMap<NodeId, NodeIntent>,
}

#[viewit::viewit]
pub(crate) struct SerfCore<D: MergeDelegate, T: Transport> {
  clock: LamportClock,
  event_clock: LamportClock,
  query_clock: LamportClock,

  broadcasts: TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>,

  memberlist: Showbiz<SerfDelegate<D, T>, T>,
  members: RwLock<Members>,

  merge_delegate: Option<D>,

  event_broadcasts: TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>,
  event_join_ignore: AtomicBool,

  query_core: SerfQueryCore,

  opts: Options<T>,

  state: Mutex<SerfState>,

  join_lock: Mutex<()>,

  snapshot: Option<SnapshotHandle>,

  coord_client: CoordinateClient,
  coord_cache: Arc<parking_lot::RwLock<HashMap<Name, Coordinate>>>,
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
  pub async fn new(opts: Options<T>) -> Result<Self, Error<D, T>> {
    Self::with_event_in(None, opts).await
  }

  pub async fn with_event(
    ev: async_channel::Sender<Event<D, T>>,
    opts: Options<T>,
  ) -> Result<Self, Error<D, T>> {
    Self::with_event_in(Some(ev), opts).await
  }

  async fn with_event_in(
    ev: Option<async_channel::Sender<Event<D, T>>>,
    opts: Options<T>,
  ) -> Result<Self, Error<D, T>> {
    if opts.max_user_event_size > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventLimitTooLarge(USER_EVENT_SIZE_LIMIT));
    }

    // Check that the meta data length is okay
    if let Some(tags) = &*opts.tags.load() {
      let encoded_tags = encode_tags(&tags)?;
      if encoded_tags.len() > META_MAX_SIZE {
        return Err(Error::TagsTooLarge(encoded_tags.len()));
      }
    }

    let (shutdown_tx, shutdown_rx) = async_channel::bounded(1);

    let event_tx = ev.map(|mut event_tx| {
      // Check if serf member event coalescing is enabled
      if opts.coalesce_period > Duration::ZERO && opts.quiescent_period > Duration::ZERO {
        let c = MemberEventCoalescer::new();

        event_tx = coalesced_event(
          event_tx,
          shutdown_rx.clone(),
          opts.coalesce_period,
          opts.quiescent_period,
          c,
        );
      }

      // Check if user event coalescing is enabled
      if opts.user_coalesce_period > Duration::ZERO && opts.user_quiescent_period > Duration::ZERO {
        let c = UserEventCoalescer::new();
        event_tx = coalesced_event(
          event_tx,
          shutdown_rx.clone(),
          opts.user_coalesce_period,
          opts.user_quiescent_period,
          c,
        );
      }

      // Listen for internal Serf queries. This is setup before the snapshotter, since
      // we want to capture the query-time, but the internal listener does not passthrough
      // the queries
      SerfQueries::new(event_tx, shutdown_rx.clone())
    });

    // Try access the snapshot
    if let Some(sp) = opts.snapshot_path.as_ref() {
      let snapshot = open_and_replay_snapshot(sp, opts.rejoin_after_leave)?;
    }

    // Set up network coordinate client.
    let coord = (!opts.disable_coordinates).then_some({
      let mut coordinate_opts = CoordinateOptions::default();
      //TODO:  setup metrics
      // coordinate_opts.
      CoordinateClient::with_options(coordinate_opts)
    });

    let broadcast = TransmitLimitedQueue::new(calc, opts.showbiz_options.retransmit_mult);
    let event_broadcast = TransmitLimitedQueue::new(calc, opts.showbiz_options.retransmit_mult);
    let query_broadcast = TransmitLimitedQueue::new(calc, opts.showbiz_options.retransmit_mult);

    // Ok()
    todo!()
  }

  /// A predicate that determines whether or not encryption
  /// is enabled, which can be possible in one of 2 cases:
  ///   - Single encryption key passed at agent start (no persistence)
  ///   - Keyring file provided at agent start
  #[inline]
  pub fn encryption_enabled(&self) -> bool {
    self.inner.memberlist.encryption_enabled()
  }

  /// The current state of this Serf instance.
  #[inline]
  pub async fn state(&self) -> SerfState {
    *self.inner.state.lock().await
  }

  /// Returns a point-in-time snapshot of the members of this cluster.
  #[inline]
  pub async fn members(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Used to provide operator debugging information
  #[inline]
  pub async fn stats(&self) -> Stats {
    let members = self.inner.members.read().await;
    todo!()
  }

  /// Returns the number of nodes in the serf cluster, regardless of
  /// their health or status.
  #[inline]
  pub async fn num_nodes(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Used to dynamically update the tags associated with
  /// the local node. This will propagate the change to the rest of
  /// the cluster. Blocks until a the message is broadcast out.
  #[inline]
  pub async fn set_tags(
    &self,
    tags: impl Iterator<Item = (SmolStr, SmolStr)>,
  ) -> Result<(), Error<D, T>> {
    let tags = tags.collect();
    // Check that the meta data length is okay
    if encode_tags(&tags)?.len() > showbiz_core::META_MAX_SIZE {
      return Err(Error::TagsTooLarge(showbiz_core::META_MAX_SIZE));
    }
    // update the config
    if !tags.is_empty() {
      self.inner.opts.tags.store(Some(Arc::new(tags)));
    } else {
      self.inner.opts.tags.store(None);
    }

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
    name: SmolStr,
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
        id: name.into(),
        msg: raw,
        notify_tx: None,
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
    name: SmolStr,
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
    let resp = q.response(self.inner.memberlist.alive_members().await);
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
        id: name.into(),
        msg: raw,
        notify_tx: None,
      })
      .await;
    Ok(resp)
  }

  /// Joins an existing Serf cluster. Returns the id of nodes
  /// successfully contacted. If `ignore_old` is true, then any
  /// user messages sent prior to the join will be ignored.
  pub async fn join(
    &self,
    existing: impl Iterator<Item = (Address, Name)>,
    ignore_old: bool,
  ) -> Result<Vec<NodeId>, JoinError<D, T>> {
    // Do a quick state check
    if self.state().await != SerfState::Alive {
      return Err(JoinError {
        joined: vec![],
        errors: existing
          .into_iter()
          .map(|(addr, _)| (addr, Error::BadStatus))
          .collect(),
        broadcast_error: None,
      });
    }

    // Hold the joinLock, this is to make eventJoinIgnore safe
    let _join_lock = self.inner.join_lock.lock().await;

    // Ignore any events from a potential join. This is safe since we hold
    // the joinLock and nobody else can be doing a Join
    if ignore_old {
      self.inner.event_join_ignore.store(true, Ordering::SeqCst);
      scopeguard::defer!(self.inner.event_join_ignore.store(false, Ordering::SeqCst));
    }

    // Have memberlist attempt to join
    match self.inner.memberlist.join(existing).await {
      Ok(joined) => {
        // Start broadcasting the update
        if let Err(e) = self.broadcast_join(self.inner.clock.time()).await {
          return Err(JoinError {
            joined,
            errors: Default::default(),
            broadcast_error: Some(e),
          });
        }
        Ok(joined)
      }
      Err(e) => {
        let (joined, errors) = e.into();
        // If we joined any nodes, broadcast the join message
        if !joined.is_empty() {
          // Start broadcasting the update
          if let Err(e) = self.broadcast_join(self.inner.clock.time()).await {
            return Err(JoinError {
              joined,
              errors: errors
                .into_iter()
                .map(|(addr, err)| (addr, err.into()))
                .collect(),
              broadcast_error: Some(e),
            });
          }

          Err(JoinError {
            joined,
            errors: errors
              .into_iter()
              .map(|(addr, err)| (addr, err.into()))
              .collect(),
            broadcast_error: None,
          })
        } else {
          Err(JoinError {
            joined,
            errors: errors
              .into_iter()
              .map(|(addr, err)| (addr, err.into()))
              .collect(),
            broadcast_error: None,
          })
        }
      }
    }
  }

  pub async fn leave(&self) -> Result<(), Error<D, T>> {
    Ok(())
  }

  /// Returns the network coordinate of the local node.
  pub fn get_cooridate(&self) -> Result<Coordinate, Error<D, T>> {
    if !self.inner.opts.disable_coordinates {
      return Ok(self.inner.coord_client.get_coordinate());
    }

    Err(Error::CoordinatesDisabled)
  }

  /// Returns the network coordinate for the node with the given
  /// name. This will only be valid if `disable_coordinates` is set to `false`.
  pub fn get_cached_coordinate(&self, name: &Name) -> Result<Option<Coordinate>, Error<D, T>> {
    if !self.inner.opts.disable_coordinates {
      return Ok(self.inner.coord_cache.read().get(name).cloned());
    }

    Err(Error::CoordinatesDisabled)
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

  /// Called when a node broadcasts a
  /// join message to set the lamport time of its join
  async fn handle_node_join_intent(&self, join_msg: &JoinMessage) -> bool {
    // Witness a potentially newer time
    self.inner.clock.witness(join_msg.ltime);

    let mut members = self.inner.members.write().await;
    match members.states.get_mut(&join_msg.node) {
      Some(member) => {
        // Check if this time is newer than what we have
        if join_msg.ltime <= member.status_time {
          return false;
        }

        // Update the LTime
        member.status_time = join_msg.ltime;

        // If we are in the leaving state, we should go back to alive,
        // since the leaving message must have been for an older time
        let _ = member.member.status.compare_exchange(
          MemberStatus::Leaving,
          MemberStatus::Alive,
          Ordering::SeqCst,
          Ordering::SeqCst,
        );

        true
      }
      None => {
        // Rebroadcast only if this was an update we hadn't seen before.
        upsert_intent(
          &mut members.recent_intents,
          &join_msg.node,
          MessageType::Join,
          join_msg.ltime,
          Instant::now,
        )
      }
    }
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

  /// Takes a Serf message type, encodes it for the wire, and queues
  /// the broadcast. If a notify channel is given, this channel will be closed
  /// when the broadcast is sent.
  async fn broadcast(
    &self,
    t: MessageType,
    node: NodeId,
    msg: impl Serialize,
    notify_tx: Option<async_channel::Sender<()>>,
  ) -> Result<(), Error<D, T>> {
    let raw = encode_message(t, &msg)?;

    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
        id: node.into(),
        msg: raw,
        notify_tx,
      })
      .await;
    Ok(())
  }

  /// Broadcasts a new join intent with a
  /// given clock value. It is used on either join, or if
  /// we need to refute an older leave intent. Cannot be called
  /// with the memberLock held.
  async fn broadcast_join(&self, ltime: LamportTime) -> Result<(), Error<D, T>> {
    // Construct message to update our lamport clock
    let msg = JoinMessage::new(ltime, self.inner.memberlist.local_id().clone());
    self.inner.clock().witness(ltime);

    // Process update locally
    self.handle_node_join_intent(&msg).await;

    // Start broadcasting the update
    if let Err(e) = self
      .broadcast(MessageType::Join, msg.node.clone(), &msg, None)
      .await
    {
      tracing::warn!(target = "ruserf", err=%e, "failed to broadcast join intent");
      return Err(e);
    }

    Ok(())
  }

  /// Serialize the current keyring and save it to a file.
  pub(crate) fn write_keyring_file(&self) -> std::io::Result<()> {
    use base64::{engine::general_purpose, Engine as _};

    let Some(path) = self.inner.opts.keyring_file() else {
      return Ok(());
    };

    if let Some(keyring) = self.inner.memberlist.keyring() {
      let encoded_keys = keyring
        .keys()
        .map(|k| general_purpose::STANDARD.encode(k))
        .collect::<Vec<_>>();

      #[cfg(unix)]
      {
        use std::os::unix::fs::OpenOptionsExt;
        let mut opts = std::fs::OpenOptions::new();
        opts.truncate(true).write(true).create(true).mode(0o600);
        return opts.open(path).and_then(|file| {
          serde_json::to_writer_pretty(file, &encoded_keys)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        });
      }
      // TODO: I don't know how to set permissions on windows
      // need helps :)
      #[cfg(windows)]
      {
        let mut opts = std::fs::OpenOptions::new();
        opts.truncate(true).write(true).create(true);
        return opts.open(path).and_then(|file| {
          serde_json::to_writer_pretty(file, &encoded_keys)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        });
      }
    }

    Ok(())
  }
}

#[viewit::viewit(vis_all = "", getters(prefix = "get"))]
#[cfg_attr(feature = "async-graphql", derive(async_graphql::SimpleObject))]
pub struct Stats {
  members: usize,
  failed: usize,
  left: usize,
  health_score: usize,
  member_time: u64,
  event_time: u64,
  query_time: u64,
  intent_time: u64,
  event_queue: usize,
  query_queue: usize,
  encrypted: bool,
}

fn upsert_intent(
  intents: &mut HashMap<NodeId, NodeIntent>,
  node: &NodeId,
  t: MessageType,
  ltime: LamportTime,
  stamper: impl FnOnce() -> Instant,
) -> bool {
  match intents.entry(node.clone()) {
    std::collections::hash_map::Entry::Occupied(mut ent) => {
      let intent = ent.get_mut();
      if ltime > intent.ltime {
        intent.ty = t;
        intent.ltime = ltime;
        intent.wall_time = stamper();
        true
      } else {
        false
      }
    }
    std::collections::hash_map::Entry::Vacant(mut ent) => {
      ent.insert(NodeIntent {
        ty: t,
        wall_time: stamper(),
        ltime,
      });
      true
    }
  }
}

/// Used to encode a tag map
fn encode_tags<D: MergeDelegate, T: Transport>(
  tag: &HashMap<SmolStr, SmolStr>,
) -> Result<Bytes, Error<D, T>> {
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
fn decode_tags(src: &[u8]) -> HashMap<SmolStr, SmolStr> {
  // Decode the tags
  let r = std::io::Cursor::new(&src[1..]);
  let mut de = rmp_serde::Deserializer::new(r);
  match HashMap::<SmolStr, SmolStr>::deserialize(&mut de) {
    Ok(tags) => tags,
    Err(e) => {
      tracing::error!(target = "reserf", err=%e, "failed to decode tags");
      HashMap::new()
    }
  }
}
