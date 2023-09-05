use std::{
  collections::HashMap,
  future::Future,
  sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use agnostic::Runtime;
use async_graphql::async_trait::async_trait;
use async_lock::{Mutex, RwLock};
use atomic::Atomic;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use showbiz_core::{
  bytes::{BufMut, Bytes, BytesMut},
  delegate::Delegate,
  futures_util::{self, FutureExt, Stream},
  queue::{NodeCalculator, TransmitLimitedQueue},
  tracing,
  transport::Transport,
  Address, DelegateVersion, Name, NodeId, ProtocolVersion, Showbiz, META_MAX_SIZE,
};
use smol_str::SmolStr;

use crate::{
  broadcast::SerfBroadcast,
  clock::{LamportClock, LamportTime},
  coalesce::{coalesced_event, MemberEventCoalescer, UserEventCoalescer},
  codec::NodeIdCodec,
  coordinate::{Coordinate, CoordinateClient, CoordinateOptions},
  delegate::{DefaultMergeDelegate, MergeDelegate, SerfDelegate},
  error::{Error, JoinError},
  event::{Event, InternalQueryEventType, MemberEvent, MemberEventType},
  internal_query::SerfQueries,
  query::{QueryParam, QueryResponse},
  snapshot::{open_and_replay_snapshot, Snapshot, SnapshotHandle},
  types::{
    decode_message, encode_message, JoinMessage, Leave, MessageType, MessageUserEvent, QueryFlag,
    QueryMessage, QueryResponseMessage,
  },
  KeyManager, Options, QueueOptions,
};

const MAGIC_BYTE: u8 = 255;

/// Maximum 128 KB snapshot
const SNAPSHOT_SIZE_LIMIT: u64 = 128 * 1024;

/// Maximum 9KB for event name and payload
const USER_EVENT_SIZE_LIMIT: usize = 9 * 1024;

/// Used to buffer events to prevent re-delivery
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UserEvents {
  pub(crate) ltime: LamportTime,
  pub(crate) events: Vec<UserEvent>,
}

/// Stores all the user events at a specific time
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UserEvent {
  pub(crate) name: SmolStr,
  pub(crate) payload: Bytes,
}

/// Stores all the query ids at a specific time
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Queries {
  ltime: LamportTime,
  query_ids: Vec<u32>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum MemberStatus {
  None = 0,
  Alive = 1,
  Leaving = 2,
  Left = 3,
  Failed = 4,
}

impl core::fmt::Display for MemberStatus {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl MemberStatus {
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::None => "none",
      Self::Alive => "alive",
      Self::Leaving => "leaving",
      Self::Left => "left",
      Self::Failed => "failed",
    }
  }
}

mod serde_member_status {
  use super::*;

  pub fn serialize<S>(status: &Atomic<MemberStatus>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::ser::Serializer,
  {
    let status = status.load(atomic::Ordering::Relaxed);
    serializer.serialize_u8(status as u8)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Atomic<MemberStatus>, D::Error>
  where
    D: serde::de::Deserializer<'de>,
  {
    let status = u8::deserialize(deserializer)?;
    let status = match status {
      0 => MemberStatus::None,
      1 => MemberStatus::Alive,
      2 => MemberStatus::Leaving,
      3 => MemberStatus::Left,
      4 => MemberStatus::Failed,
      _ => {
        return Err(serde::de::Error::custom(format!(
          "invalid member status: {}",
          status
        )));
      }
    };
    Ok(Atomic::new(status))
  }
}

/// Implemented to allow overriding the reconnect timeout for individual members.
#[async_trait]
pub trait ReconnectTimeoutOverrider: Send + Sync + 'static {
  async fn reconnect_timeout(&self, member: &Member, timeout: Duration) -> Duration;
}

/// Noop implementation of `ReconnectTimeoutOverrider`.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopReconnectTimeoutOverrider;

#[async_trait]
impl ReconnectTimeoutOverrider for NoopReconnectTimeoutOverrider {
  async fn reconnect_timeout(&self, _member: &Member, timeout: Duration) -> Duration {
    timeout
  }
}

/// A single member of the Serf cluster.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Member {
  id: NodeId,
  tags: HashMap<SmolStr, SmolStr>,
  #[serde(with = "serde_member_status")]
  status: Atomic<MemberStatus>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

/// Used to track members that are no longer active due to
/// leaving, failing, partitioning, etc. It tracks the member along with
/// when that member was marked as leaving.
#[viewit::viewit]
#[derive(Clone)]
pub(crate) struct MemberState {
  member: Arc<Member>,
  /// lamport clock time of last received message
  status_time: LamportTime,
  /// wall clock time of leave
  leave_time: Instant,
}

impl MemberState {
  fn zero_leave_time() -> Instant {
    static ZERO: once_cell::sync::Lazy<Instant> =
      once_cell::sync::Lazy::new(|| Instant::now() - std::time::UNIX_EPOCH.elapsed().unwrap());
    *ZERO
  }
}

/// Used to buffer intents for out-of-order deliveries.
pub(crate) struct NodeIntent {
  ty: MessageType,

  wall_time: Instant,

  ltime: LamportTime,
}

/// The state of the Serf instance.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum SerfState {
  Alive,
  Leaving,
  Left,
  Shutdown,
}

impl SerfState {
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Alive => "alive",
      Self::Leaving => "leaving",
      Self::Left => "left",
      Self::Shutdown => "shutdown",
    }
  }
}

impl core::fmt::Display for SerfState {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

#[derive(Clone)]
pub(crate) struct SerfNodeCalculator {
  members: Arc<RwLock<Members>>,
}

impl SerfNodeCalculator {
  pub(crate) fn new(members: Arc<RwLock<Members>>) -> Self {
    Self { members }
  }
}

impl NodeCalculator for SerfNodeCalculator {
  fn num_nodes(&self) -> usize {
    use pollster::FutureExt as _;
    self.members.read().block_on().states.len()
  }
}

pub(crate) struct CoordCore {
  pub(crate) client: CoordinateClient,
  pub(crate) cache: parking_lot::RwLock<HashMap<Name, Coordinate>>,
}

#[derive(Default)]
pub(crate) struct QueryCore {
  responses: HashMap<LamportTime, QueryResponse>,
  min_time: LamportTime,
  buffer: Vec<Option<Queries>>,
}

#[derive(Default)]
pub(crate) struct Members {
  pub(crate) states: HashMap<NodeId, MemberState>,
  recent_intents: HashMap<NodeId, NodeIntent>,
  pub(crate) left_members: Vec<MemberState>,
  failed_members: Vec<MemberState>,
}

impl Members {
  fn recent_intent(&self, node: &NodeId, ty: MessageType) -> Option<LamportTime> {
    match self.recent_intents.get(node) {
      Some(intent) if intent.ty == ty => Some(intent.ltime),
      _ => None,
    }
  }
}

#[viewit::viewit]
pub(crate) struct EventCore {
  min_time: LamportTime,
  buffer: Vec<Option<UserEvents>>,
}

pub(crate) struct SerfCore<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) clock: LamportClock,
  pub(crate) event_clock: LamportClock,
  pub(crate) query_clock: LamportClock,

  pub(crate) broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  pub(crate) event_broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  pub(crate) query_broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,

  pub(crate) memberlist: Showbiz<T, SerfDelegate<T, D, O>>,
  pub(crate) members: Arc<RwLock<Members>>,
  event_tx: Option<async_channel::Sender<Event<T, D, O>>>,
  pub(crate) event_join_ignore: AtomicBool,

  pub(crate) event_core: RwLock<EventCore>,
  query_core: Arc<RwLock<QueryCore>>,

  pub(crate) opts: Options<T>,

  state: parking_lot::Mutex<SerfState>,

  join_lock: Mutex<()>,

  snapshot: Option<SnapshotHandle>,
  key_manager: KeyManager<T, D, O>,

  shutdown_tx: async_channel::Sender<()>,
  shutdown_rx: async_channel::Receiver<()>,
  reconnector: Option<Arc<O>>,

  pub(crate) coord_core: Option<Arc<CoordCore>>,
}

impl<T, D, O> Drop for SerfCore<D, T, O>
where
  T: Transport,
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn drop(&mut self) {
    use showbiz_core::pollster::FutureExt as _;

    let mut s = self.state.lock();
    if *s != SerfState::Left {
      tracing::warn!(target: "serf", "shutdown without a leave");
    }

    // Wait to close the shutdown channel until after we've shut down the
    // memberlist and its associated network resources, since the shutdown
    // channel signals that we are cleaned up outside of Serf.
    *s = SerfState::Shutdown;
    self.shutdown_tx.close();

    // Wait for the snapshoter to finish if we have one
    if let Some(ref snapshot) = self.snapshot {
      snapshot.wait().block_on();
    }
  }
}

/// Serf is a single node that is part of a single cluster that gets
/// events about joins/leaves/failures/etc. It is created with the Create
/// method.
///
/// All functions on the Serf structure are safe to call concurrently.
#[repr(transparent)]
pub struct Serf<
  T: Transport,
  D: MergeDelegate = DefaultMergeDelegate,
  O: ReconnectTimeoutOverrider = NoopReconnectTimeoutOverrider,
> where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) inner: Arc<SerfCore<D, T, O>>,
}

impl<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> Clone for Serf<T, D, O>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}

impl<T> Serf<T>
where
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn new(
    opts: Options<T>,
  ) -> Result<Self, Error<T, DefaultMergeDelegate, NoopReconnectTimeoutOverrider>> {
    Self::new_in(None, None, None, opts).await
  }

  pub async fn with_event_sender(
    ev: async_channel::Sender<Event<T, DefaultMergeDelegate, NoopReconnectTimeoutOverrider>>,
    opts: Options<T>,
  ) -> Result<Self, Error<T, DefaultMergeDelegate, NoopReconnectTimeoutOverrider>> {
    Self::new_in(Some(ev), None, None, opts).await
  }
}

impl<T, O> Serf<T, DefaultMergeDelegate, O>
where
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn with_reconnect_timeout_overrider(
    overrider: O,
    opts: Options<T>,
  ) -> Result<Self, Error<T, DefaultMergeDelegate, O>> {
    Self::new_in(None, None, Some(overrider), opts).await
  }

  pub async fn with_event_sender_and_reconnect_timeout_overrider(
    ev: async_channel::Sender<Event<T, DefaultMergeDelegate, O>>,
    overrider: O,
    opts: Options<T>,
  ) -> Result<Self, Error<T, DefaultMergeDelegate, O>> {
    Self::new_in(Some(ev), None, Some(overrider), opts).await
  }
}

impl<T, D> Serf<T, D>
where
  D: MergeDelegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn with_delegate(
    delegate: D,
    opts: Options<T>,
  ) -> Result<Self, Error<T, D, NoopReconnectTimeoutOverrider>> {
    Self::new_in(None, Some(delegate), None, opts).await
  }

  pub async fn with_event_sender_and_delegate(
    ev: async_channel::Sender<Event<T, D, NoopReconnectTimeoutOverrider>>,
    delegate: D,
    opts: Options<T>,
  ) -> Result<Self, Error<T, D, NoopReconnectTimeoutOverrider>> {
    Self::new_in(Some(ev), Some(delegate), None, opts).await
  }
}

// ------------------------------------Public methods------------------------------------
impl<T, D, O> Serf<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn with_event_sender_and_delegate_and_overrider(
    ev: async_channel::Sender<Event<T, D, O>>,
    delegate: D,
    overrider: O,
    opts: Options<T>,
  ) -> Result<Self, Error<T, D, O>> {
    Self::new_in(Some(ev), Some(delegate), Some(overrider), opts).await
  }

  async fn new_in(
    ev: Option<async_channel::Sender<Event<T, D, O>>>,
    delegate: Option<D>,
    reconnector: Option<O>,
    opts: Options<T>,
  ) -> Result<Self, Error<T, D, O>> {
    if opts.max_user_event_size > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventLimitTooLarge(USER_EVENT_SIZE_LIMIT));
    }

    // Check that the meta data length is okay
    if let Some(tags) = &*opts.tags.load() {
      let encoded_tags = encode_tags(tags)?;
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

    let clock = LamportClock::new();
    let event_clock = LamportClock::new();
    let query_clock = LamportClock::new();
    let mut event_min_time = LamportTime::ZERO;
    let mut query_min_time = LamportTime::ZERO;

    // Try access the snapshot
    let (old_clock, old_event_clock, old_query_clock, event_tx, alive_nodes, handle) =
      if let Some(sp) = opts.snapshot_path.as_ref() {
        let rs = open_and_replay_snapshot(sp, opts.rejoin_after_leave)?;
        let old_clock = rs.last_clock;
        let old_event_clock = rs.last_event_clock;
        let old_query_clock = rs.last_query_clock;
        let (event_tx, alive_nodes, handle) = Snapshot::from_replay_result(
          rs,
          SNAPSHOT_SIZE_LIMIT,
          opts.rejoin_after_leave,
          clock.clone(),
          event_tx,
          shutdown_rx.clone(),
          #[cfg(feature = "metrics")]
          opts.showbiz_options.metric_labels().clone(),
        )?;
        event_min_time = old_event_clock + LamportTime::new(1);
        query_min_time = old_query_clock + LamportTime::new(1);
        (
          old_clock,
          old_event_clock,
          old_query_clock,
          Some(event_tx),
          alive_nodes,
          Some(handle),
        )
      } else {
        (
          LamportTime::new(0),
          LamportTime::new(0),
          LamportTime::new(0),
          event_tx,
          vec![],
          None,
        )
      };

    // Set up network coordinate client.
    let coord = (!opts.disable_coordinates).then_some({
      CoordinateClient::with_options(CoordinateOptions {
        #[cfg(feature = "metrics")]
        metric_labels: opts.showbiz_options.metric_labels().clone(),
        ..Default::default()
      })
    });
    let members = Arc::new(RwLock::new(Members::default()));

    // Setup the various broadcast queues, which we use to send our own
    // custom broadcasts along the gossip channel.
    let nc = SerfNodeCalculator::new(members.clone());
    let broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      nc.clone(),
      opts.showbiz_options.retransmit_mult,
    ));
    let event_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      nc.clone(),
      opts.showbiz_options.retransmit_mult,
    ));
    let query_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      nc,
      opts.showbiz_options.retransmit_mult,
    ));

    // Create a buffer for events and queries
    let event_buffer = Vec::with_capacity(opts.event_buffer_size);
    let query_buffer = Vec::with_capacity(opts.query_buffer_size);

    // Ensure our lamport clock is at least 1, so that the default
    // join LTime of 0 does not cause issues
    clock.increment();
    event_clock.increment();
    query_clock.increment();

    // Restore the clock from snap if we have one
    clock.witness(old_clock);
    event_clock.witness(old_event_clock);
    query_clock.witness(old_query_clock);

    // Create the underlying memberlist that will manage membership
    // and failure detection for the Serf instance.
    let memberlist =
      Showbiz::with_delegate(SerfDelegate::new(delegate), opts.showbiz_options.clone()).await?;
    let c = SerfCore {
      clock,
      event_clock,
      query_clock,
      broadcasts,
      memberlist,
      members,
      event_broadcasts,
      event_join_ignore: AtomicBool::new(false),
      event_core: RwLock::new(EventCore {
        min_time: event_min_time,
        buffer: event_buffer,
      }),
      query_broadcasts,
      query_core: Arc::new(RwLock::new(QueryCore {
        min_time: query_min_time,
        responses: HashMap::new(),
        buffer: query_buffer,
      })),
      opts,
      state: parking_lot::Mutex::new(SerfState::Alive),
      join_lock: Mutex::new(()),
      snapshot: handle,
      key_manager: KeyManager::new(),
      shutdown_tx,
      shutdown_rx: shutdown_rx.clone(),
      coord_core: coord.map(|cc| {
        Arc::new(CoordCore {
          client: cc,
          cache: parking_lot::RwLock::new(HashMap::new()),
        })
      }),
      event_tx,
      reconnector: reconnector.map(Arc::new),
    };
    let this = Serf { inner: Arc::new(c) };
    // update delegate
    let that = this.clone();
    let delegate = this.inner.memberlist.delegate().unwrap();
    delegate.store(that);
    let local_node = this.inner.memberlist.local_node().await;
    delegate
      .notify_join(local_node)
      .await
      .map_err(|e| showbiz_core::error::Error::delegate(e))?;

    // update key manager
    let that = this.clone();
    this.inner.key_manager.store(that);

    // Start the background tasks. See the documentation above each method
    // for more information on their role.
    Reaper {
      coord_core: this.inner.coord_core.clone(),
      reconnector: this.inner.reconnector.clone(),
      members: this.inner.members.clone(),
      event_tx: this.inner.event_tx.clone(),
      shutdown_rx: shutdown_rx.clone(),
      reap_interval: this.inner.opts.reap_interval,
      reconnect_timeout: this.inner.opts.reconnect_timeout,
    }
    .spawn();

    Reconnector {
      members: this.inner.members.clone(),
      memberlist: this.inner.memberlist.clone(),
      shutdown_rx: shutdown_rx.clone(),
      reconnect_interval: this.inner.opts.reconnect_interval,
    }
    .spawn();

    QueueChecker {
      name: "Intent",
      queue: this.inner.broadcasts.clone(),
      members: this.inner.members.clone(),
      opts: this.inner.opts.queue_opts(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn::<T::Runtime>();

    QueueChecker {
      name: "Event",
      queue: this.inner.event_broadcasts.clone(),
      members: this.inner.members.clone(),
      opts: this.inner.opts.queue_opts(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn::<T::Runtime>();

    QueueChecker {
      name: "Query",
      queue: this.inner.query_broadcasts.clone(),
      members: this.inner.members.clone(),
      opts: this.inner.opts.queue_opts(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn::<T::Runtime>();

    // Attempt to re-join the cluster if we have known nodes
    if !alive_nodes.is_empty() {
      let showbiz = this.inner.memberlist.clone();
      Self::handle_rejoin(showbiz, alive_nodes);
    }
    Ok(this)
  }

  /// A predicate that determines whether or not encryption
  /// is enabled, which can be possible in one of 2 cases:
  ///   - Single encryption key passed at agent start (no persistence)
  ///   - Keyring file provided at agent start
  #[inline]
  pub fn encryption_enabled(&self) -> bool {
    self.inner.memberlist.encryption_enabled()
  }

  /// Returns a receiver that can be used to wait for
  /// Serf to shutdown.
  #[inline]
  pub fn shutdown_rx(&self) -> async_channel::Receiver<()> {
    self.inner.shutdown_rx.clone()
  }

  /// The current state of this Serf instance.
  #[inline]
  pub fn state(&self) -> SerfState {
    *self.inner.state.lock()
  }

  /// Returns a point-in-time snapshot of the members of this cluster.
  #[inline]
  pub async fn members(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Used to provide operator debugging information
  #[inline]
  pub async fn stats(&self) -> Stats {
    let _members = self.inner.members.read().await;
    todo!()
  }

  /// Returns the number of nodes in the serf cluster, regardless of
  /// their health or status.
  #[inline]
  pub async fn num_nodes(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Returns the Member information for the local node
  #[inline]
  pub async fn local_member(&self) -> Arc<Member> {
    self
      .inner
      .members
      .read()
      .await
      .states
      .get(self.inner.memberlist.local_id())
      .unwrap()
      .member
      .clone()
  }

  /// Used to dynamically update the tags associated with
  /// the local node. This will propagate the change to the rest of
  /// the cluster. Blocks until a the message is broadcast out.
  #[inline]
  pub async fn set_tags(
    &self,
    tags: impl Iterator<Item = (SmolStr, SmolStr)>,
  ) -> Result<(), Error<T, D, O>> {
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
  ) -> Result<(), Error<T, D, O>> {
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
    self.handle_user_event(msg).await;

    self
      .inner
      .event_broadcasts
      .queue_broadcast(SerfBroadcast {
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
  ) -> Result<QueryResponse, Error<T, D, O>> {
    self.query_in(name, payload, params, None).await
  }

  async fn internal_query(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam>,
    ty: InternalQueryEventType,
  ) -> Result<QueryResponse, Error<T, D, O>> {
    self.query_in(name, payload, params, Some(ty)).await
  }

  async fn query_in(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam>,
    ty: Option<InternalQueryEventType>,
  ) -> Result<QueryResponse, Error<T, D, O>> {
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
    self.handle_query(q, ty).await;

    // Start broadcasting the event
    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
        msg: raw,
        notify_tx: None,
      })
      .await;
    Ok(resp)
  }

  /// Joins an existing Serf cluster. Returns the id of nodes
  /// successfully contacted. If `ignore_old` is true, then any
  /// user messages sent prior to the join will be ignored.
  pub async fn join_many(
    &self,
    existing: impl Iterator<Item = (Address, Name)>,
    ignore_old: bool,
  ) -> Result<Vec<NodeId>, JoinError<T, D, O>> {
    // Do a quick state check
    let current_state = self.state();
    if current_state != SerfState::Alive {
      return Err(JoinError {
        joined: vec![],
        errors: existing
          .into_iter()
          .map(|(addr, _)| (addr, Error::BadJoinStatus(current_state)))
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
    match self.inner.memberlist.join_many(existing).await {
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

  /// Gracefully exits the cluster. It is safe to call this multiple
  /// times.
  /// If the Leave broadcast timeout, Leave() will try to finish the sequence as best effort.
  pub async fn leave(&self) -> Result<(), Error<T, D, O>> {
    // Check the current state
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Left => return Ok(()),
        SerfState::Leaving => return Err(Error::BadLeaveStatus(*s)),
        SerfState::Shutdown => return Err(Error::BadLeaveStatus(*s)),
        _ => {
          // Set the state to leaving
          *s = SerfState::Leaving;
        }
      }
    }

    // If we have a snapshot, mark we are leaving
    if let Some(ref snap) = self.inner.snapshot {
      snap.leave().await;
    }

    // Construct the message for the graceful leave
    let msg = Leave {
      ltime: self.inner.clock.time(),
      node: self.inner.memberlist.local_id().clone(),
      prune: false,
    };

    self.inner.clock.increment();

    // Process the leave locally
    self.handle_node_leave_intent(&msg).await;

    // Only broadcast the leave message if there is at least one
    // other node alive.
    if self.has_alive_members().await {
      let (notify_tx, notify_rx) = async_channel::bounded(1);
      self
        .broadcast(MessageType::Leave, &msg, Some(notify_tx))
        .await?;

      futures_util::select! {
        _ = notify_rx.recv().fuse() => {
          // We got a response, so we are done
        }
        _ = <T::Runtime as Runtime>::sleep(self.inner.opts.broadcast_timeout).fuse() => {
          tracing::warn!(target = "ruserf", "timeout while waiting for graceful leave");
        }
      }
    }

    // Attempt the memberlist leave
    if let Err(e) = self
      .inner
      .memberlist
      .leave(self.inner.opts.broadcast_timeout)
      .await
    {
      tracing::warn!(
        target = "ruserf",
        "timeout waiting for leave broadcast: {}",
        e
      );
    }

    // Wait for the leave to propagate through the cluster. The broadcast
    // timeout is how long we wait for the message to go out from our own
    // queue, but this wait is for that message to propagate through the
    // cluster. In particular, we want to stay up long enough to service
    // any probes from other nodes before they learn about us leaving.
    <T::Runtime as Runtime>::sleep(self.inner.opts.leave_propagate_delay).await;

    // Transition to Left only if we not already shutdown
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Shutdown => {}
        _ => {
          *s = SerfState::Left;
        }
      }
    }
    Ok(())
  }

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  pub async fn remove_failed_node(&self, node: NodeId) -> Result<(), Error<T, D, O>> {
    self.force_leave(node, false).await
  }

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  pub async fn remove_failed_node_prune(&self, node: NodeId) -> Result<(), Error<T, D, O>> {
    self.force_leave(node, true).await
  }

  /// Forcefully shuts down the Serf instance, stopping all network
  /// activity and background maintenance associated with the instance.
  ///
  /// This is not a graceful shutdown, and should be preceded by a call
  /// to Leave. Otherwise, other nodes in the cluster will detect this node's
  /// exit as a node failure.
  ///
  /// It is safe to call this method multiple times.
  pub async fn shutdown(&self) -> Result<(), Error<T, D, O>> {
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Shutdown => return Ok(()),
        SerfState::Left => {}
        _ => {
          tracing::warn!(target = "ruserf", "shutdown without a leave");
        }
      }

      // Wait to close the shutdown channel until after we've shut down the
      // memberlist and its associated network resources, since the shutdown
      // channel signals that we are cleaned up outside of Serf.
      *s = SerfState::Shutdown;
    }

    self.inner.memberlist.shutdown().await?;
    self.inner.shutdown_tx.close();

    // Wait for the snapshoter to finish if we have one
    if let Some(ref snap) = self.inner.snapshot {
      snap.wait().await;
    }

    Ok(())
  }

  /// Returns the network coordinate of the local node.
  pub fn get_cooridate(&self) -> Result<Coordinate, Error<T, D, O>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.client.get_coordinate());
    }

    Err(Error::CoordinatesDisabled)
  }

  /// Returns the network coordinate for the node with the given
  /// name. This will only be valid if `disable_coordinates` is set to `false`.
  pub fn get_cached_coordinate(&self, name: &Name) -> Result<Option<Coordinate>, Error<T, D, O>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.cache.read().get(name).cloned());
    }

    Err(Error::CoordinatesDisabled)
  }
}

// ---------------------------------Private Methods-------------------------------

impl<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> Serf<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  async fn has_alive_members(&self) -> bool {
    let members = self.inner.members.read().await;
    for member in members.states.values() {
      if member.member.id() == self.inner.memberlist.local_id() {
        continue;
      }

      if member.member.status.load(Ordering::Relaxed) == MemberStatus::Alive {
        return true;
      }
    }

    false
  }

  /// Used to setup the listeners for the query,
  /// and to schedule closing the query after the timeout.
  async fn register_query_response(&self, timeout: Duration, resp: QueryResponse) {
    let tresps = self.inner.query_core.clone();
    let mut resps = self.inner.query_core.write().await;
    // Map the LTime to the QueryResponse. This is necessarily 1-to-1,
    // since we increment the time for each new query.
    let ltime = resp.ltime;
    resps.responses.insert(ltime, resp);

    // Setup a timer to close the response and deregister after the timeout
    <T::Runtime as Runtime>::delay(timeout, async move {
      let mut resps = tresps.write().await;
      if let Some(resp) = resps.responses.remove(&ltime) {
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
    msg: impl Serialize,
    notify_tx: Option<async_channel::Sender<()>>,
  ) -> Result<(), Error<T, D, O>> {
    let raw = encode_message(t, &msg)?;

    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
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
  async fn broadcast_join(&self, ltime: LamportTime) -> Result<(), Error<T, D, O>> {
    // Construct message to update our lamport clock
    let msg = JoinMessage::new(ltime, self.inner.memberlist.local_id().clone());
    self.inner.clock.witness(ltime);

    // Process update locally
    self.handle_node_join_intent(&msg).await;

    // Start broadcasting the update
    if let Err(e) = self
      .broadcast(MessageType::Join, &msg, None)
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

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  async fn force_leave(&self, node: NodeId, prune: bool) -> Result<(), Error<T, D, O>> {
    // Construct the message to broadcast
    let msg = Leave {
      ltime: self.inner.clock.time(),
      node,
      prune,
    };

    // Process our own event
    self.handle_node_leave_intent(&msg).await;

    // If we have no members, then we don't need to broadcast
    if !self.has_alive_members().await {
      return Ok(());
    }

    // Broadcast the remove
    let (ntx, nrx) = async_channel::bounded(1);
    self
      .broadcast(MessageType::Leave, &msg, Some(ntx))
      .await?;

    // Wait for the broadcast
    futures_util::select! {
      _ = nrx.recv().fuse() => Ok(()),
      _ = <T::Runtime as Runtime>::sleep(self.inner.opts.broadcast_timeout).fuse() => Err(Error::RemovalBroadcastTimeout),
    }
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

struct Reaper<T, D, O>
where
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
{
  coord_core: Option<Arc<CoordCore>>,
  reconnector: Option<Arc<O>>,
  members: Arc<RwLock<Members>>,
  event_tx: Option<async_channel::Sender<Event<T, D, O>>>,
  shutdown_rx: async_channel::Receiver<()>,
  reap_interval: Duration,
  reconnect_timeout: Duration,
}

macro_rules! erase_node {
  ($tx:ident <- $coord:ident($members:ident[$id:ident].$m:ident)) => {{
    // takes a node completely out of the member list
    $members.states.remove($id);

    // Tell the coordinate client the node has gone away and delete
    // its cached coordinates.
    if let Some(cc) = $coord {
      let name = $id.name();
      cc.client.forget_node(name);
      cc.cache.write().remove(name);
    }

    // Send out event
    if let Some(tx) = $tx {
      let _ = tx
        .send(Event::from(MemberEvent {
          ty: MemberEventType::Reap,
          members: vec![$m.member.clone()],
        }))
        .await;
    }
  }};
}

macro_rules! reap {
  (
    $tx:ident <- $reconnector:ident($timeout: ident($members: ident.$ty: ident, $coord:ident))
  ) => {{
    let mut n = $members.$ty.len();
    let mut i = 0;
    while i < n {
      let m = $members.$ty[i].clone();
      let mut member_timeout = $timeout;
      if let Some(r) = $reconnector {
        member_timeout = r.reconnect_timeout(&m.member, member_timeout).await;
      }

      // Skip if the timeout is not yet reached
      if m.leave_time.elapsed() <= member_timeout {
        i += 1;
        continue;
      }

      // Delete from the list
      $members.$ty.swap_remove(i);
      n -= 1;

      // Delete from members and send out event
      let id = m.member.id();
      tracing::info!(target = "ruserf", "event member reap: {}", id);

      erase_node!($tx <- $coord($members[id].m));
    }
  }};
}

impl<T, D, O> Reaper<T, D, O>
where
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
{
  fn spawn(self) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          _ = <T::Runtime as Runtime>::sleep(self.reap_interval).fuse() => {
            let mut ms = self.members.write().await;
            Self::reap_failed(&mut ms, self.event_tx.as_ref(), self.reconnector.as_deref(), self.coord_core.as_deref(), self.reconnect_timeout).await;
            Self::reap_left(&mut ms, self.event_tx.as_ref(), self.reconnector.as_deref(), self.coord_core.as_deref(), self.reconnect_timeout).await;
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    });
  }

  async fn reap_failed(
    old: &mut Members,
    event_tx: Option<&async_channel::Sender<Event<T, D, O>>>,
    reconnector: Option<&O>,
    coord: Option<&CoordCore>,
    timeout: Duration,
  ) {
    reap!(event_tx <- reconnector(timeout(old.failed_members, coord)))
  }

  async fn reap_left(
    old: &mut Members,
    event_tx: Option<&async_channel::Sender<Event<T, D, O>>>,
    reconnector: Option<&O>,
    coord: Option<&CoordCore>,
    timeout: Duration,
  ) {
    reap!(event_tx <- reconnector(timeout(old.left_members, coord)))
  }
}

struct Reconnector<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  members: Arc<RwLock<Members>>,
  memberlist: Showbiz<T, SerfDelegate<T, D, O>>,
  shutdown_rx: async_channel::Receiver<()>,
  reconnect_interval: Duration,
}

impl<T, D, O> Reconnector<T, D, O>
where
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
{
  fn spawn(self) {
    let mut rng = rand::rngs::StdRng::from_rng(rand::thread_rng()).unwrap();

    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          _ = <T::Runtime as Runtime>::sleep(self.reconnect_interval).fuse() => {
            let mu = self.members.read().await;
            let num_failed = mu.failed_members.len();
            // Nothing to do if there are no failed members
            if num_failed == 0 {
              continue;
            }

            // Probability we should attempt to reconect is given
            // by num failed / (num members - num failed - num left)
            // This means that we probabilistically expect the cluster
            // to attempt to connect to each failed member once per
            // reconnect interval

            let num_alive = (mu.states.len() - num_failed - mu.left_members.len()).max(1);
            let prob = num_failed as f32 / num_alive as f32;
            let r: f32 = rng.gen();
            if r > prob {
              tracing::debug!(target = "ruserf", "forgoing reconnect for random throttling");
              continue;
            }

            // Select a random member to try and join
            let idx: usize = rng.gen_range(0..num_failed);
            let member = &mu.failed_members[idx];

            let id = member.member.id().clone();
            drop(mu); // release read lock
            tracing::info!(target = "ruserf", "attempting to reconnect to {}", id);
            // Attempt to join at the memberlist level
            if let Err(e) = self.memberlist.join_node(&id).await {
              tracing::warn!(target = "ruserf", "failed to reconnect {}: {}", id, e);
            } else {
              tracing::info!(target = "ruserf", "successfully reconnected to {}", id);
            }
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    });
  }
}

struct QueueChecker {
  name: &'static str,
  queue: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  members: Arc<RwLock<Members>>,
  opts: QueueOptions,
  shutdown_rx: async_channel::Receiver<()>,
}

impl QueueChecker {
  fn spawn<R: Runtime>(self)
  where
    <<R as Runtime>::Sleep as Future>::Output: Send,
  {
    R::spawn_detach(async move {
      loop {
        futures_util::select! {
          _ = R::sleep(self.opts.check_interval).fuse() => {
            let numq = self.queue.num_queued();
            // TODO: metrics
            if numq >= self.opts.depth_warning {
              tracing::warn!(target = "ruserf", "queue {} depth: {}", self.name, numq);
            }

            let max = self.get_queue_max().await;
            if numq >= max {
              tracing::warn!(target = "ruserf", "{} queue depth ({}) exceeds limit ({}), dropping messages!", self.name, numq, max);
              self.queue.prune(max).await;
            }
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    });
  }

  async fn get_queue_max(&self) -> usize {
    let mut max = self.opts.max_queue_depth;
    if self.opts.min_queue_depth > 0 {
      let num_members = self.members.read().await.states.len();
      max = num_members * 2;

      if max < self.opts.min_queue_depth {
        max = self.opts.min_queue_depth;
      }
    }
    max
  }
}

// ---------------------------------Hanlders Methods-------------------------------
impl<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> Serf<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  /// Called when a user event broadcast is
  /// received. Returns if the message should be rebroadcast.
  pub(crate) async fn handle_user_event(&self, msg: MessageUserEvent) -> bool {
    // Witness a potentially newer time
    self.inner.event_clock.witness(msg.ltime);

    let mut el = self.inner.event_core.write().await;

    // Ignore if it is before our minimum event time
    if msg.ltime < el.min_time {
      return false;
    }

    // Check if this message is too old
    let bltime = LamportTime::new(el.buffer.len() as u64);
    let cur_time = self.inner.event_clock.time();
    if cur_time > bltime && msg.ltime < cur_time - bltime {
      tracing::warn!(
        target = "ruserf",
        "received old event {} from time {} (current: {})",
        msg.name,
        msg.ltime,
        cur_time
      );
      return false;
    }

    // Check if we've already seen this
    let idx = (msg.ltime % bltime).0 as usize;
    let seen: Option<&mut UserEvents> = el.buffer[idx].as_mut();
    let user_event = UserEvent {
      name: msg.name.clone(),
      payload: msg.payload.clone(),
    };
    if let Some(seen) = seen {
      for prev in seen.events.iter() {
        if user_event.eq(prev) {
          return false;
        }
      }
      seen.events.push(user_event);
    } else {
      el.buffer[idx] = Some(UserEvents {
        ltime: msg.ltime,
        events: vec![user_event],
      });
    }

    // TODO: metrics

    if let Some(ref tx) = self.inner.event_tx {
      if let Err(e) = tx
        .send(
          crate::event::UserEvent {
            ltime: msg.ltime,
            name: msg.name,
            payload: msg.payload,
            coalesce: msg.cc,
          }
          .into(),
        )
        .await
      {
        tracing::error!(target = "ruserf", "failed to send user event: {}", e);
      }
    }
    true
  }

  /// Called when a query broadcast is
  /// received. Returns if the message should be rebroadcast.
  pub(crate) async fn handle_query(
    &self,
    q: QueryMessage,
    ty: Option<InternalQueryEventType>,
  ) -> bool {
    // Witness a potentially newer time
    self.inner.query_clock.witness(q.ltime);

    let mut query = self.inner.query_core.write().await;

    // Ignore if it is before our minimum query time
    if q.ltime < query.min_time {
      return false;
    }

    // Check if this message is too old
    let cur_time = self.inner.query_clock.time();
    let q_time = LamportTime::new(query.buffer.len() as u64);
    if cur_time > q_time && q_time < cur_time - q_time {
      tracing::warn!(
        target = "ruserf",
        "received old query {} from time {} (current: {})",
        q.name,
        q.ltime,
        cur_time
      );
      return false;
    }

    // Check if we've already seen this
    let idx = (q.ltime % q_time).0 as usize;
    let seen = query.buffer[idx].as_mut();
    if let Some(seen) = seen {
      for &prev in seen.query_ids.iter() {
        if q.id == prev {
          // Seen this ID already
          return false;
        }
      }
      seen.query_ids.push(q.id);
    } else {
      query.buffer[idx] = Some(Queries {
        ltime: q.ltime,
        query_ids: vec![q.id],
      });
    }

    // TODO: metrics

    // Check if we should rebroadcast, this may be disabled by a flag
    let mut rebroadcast = true;
    if q.no_broadcast() {
      rebroadcast = false;
    }

    // Filter the query
    if !self.should_process_query(&q.filters) {
      return rebroadcast;
    }

    // Send ack if requested, without waiting for client to respond()
    if q.ack() {
      let ack = QueryResponseMessage {
        ltime: q.ltime,
        id: q.id,
        from: self.inner.memberlist.local_id().clone(),
        flags: QueryFlag::ACK.bits(),
        payload: Bytes::new(),
      };

      match encode_message(MessageType::QueryResponse, &ack) {
        Ok(raw) => {
          if let Err(e) = self.inner.memberlist.send(q.from(), raw).await {
            tracing::error!(target = "ruserf", err=%e, "failed to send ack");
          }

          if let Err(e) = self
            .relay_response(q.relay_factor, q.from.clone(), ack)
            .await
          {
            tracing::error!(target = "ruserf", err=%e, "failed to relay ack");
          }
        }
        Err(e) => {
          tracing::error!(target = "ruserf", err=%e, "failed to format ack");
        }
      }
    }

    if let Some(ref tx) = self.inner.event_tx {
      let ev = crate::event::QueryEvent {
        ltime: q.ltime,
        name: q.name,
        payload: q.payload,
        ctx: Arc::new(crate::event::QueryContext {
          query_timeout: q.timeout,
          span: Mutex::new(Some(Instant::now())),
          this: self.clone(),
        }),
        id: q.id,
        from: q.from,
        relay_factor: q.relay_factor,
      };

      if let Err(e) = tx
        .send(match ty {
          Some(ty) => (ty, ev).into(),
          None => ev.into(),
        })
        .await
      {
        tracing::error!(target = "ruserf", err=%e, "failed to send query");
      }
    }

    rebroadcast
  }

  /// Called when a query response is
  /// received.
  pub(crate) async fn handle_query_response(&self, resp: QueryResponseMessage) {
    // Look for a corresponding QueryResponse
    let qc = self
      .inner
      .query_core
      .read()
      .await
      .responses
      .get(&resp.ltime)
      .cloned();
    if let Some(query) = qc {
      // Verify the ID matches
      if query.id != resp.id {
        tracing::warn!(
          target = "ruserf",
          "query reply ID mismatch (local: {}, response: {})",
          query.id,
          resp.id
        );
        return;
      }

      query.handle_query_response::<T, D, O>(resp).await;
    } else {
      tracing::warn!(
        target = "ruserf",
        "reply for non-running query (LTime: {}, ID: {}) From: {}",
        resp.ltime,
        resp.id,
        resp.from
      );
    }
  }

  /// Called when a node join event is received
  /// from memberlist.
  pub(crate) async fn handle_node_join(&self, n: Arc<showbiz_core::Node>) {
    let mut members = self.inner.members.write().await;

    // TODO: message dropper?

    let id = n.id();
    let (old_status, fut) = if let Some(member) = members.states.get_mut(id) {
      let old_status = member.member.status.load(Ordering::Acquire);
      let dead_time = member.leave_time.elapsed();
      if old_status == MemberStatus::Failed && dead_time < self.inner.opts.flap_timeout {
        // TODO: metrics
      }

      *member = MemberState {
        member: Arc::new(Member {
          id: id.clone(),
          tags: decode_tags(n.meta()),
          status: Atomic::new(MemberStatus::Alive),
          protocol_version: ProtocolVersion::V0,
          delegate_version: DelegateVersion::V0,
        }),
        status_time: member.status_time,
        leave_time: MemberState::zero_leave_time(),
      };

      (
        old_status,
        self.inner.event_tx.as_ref().map(|tx| {
          tx.send(
            MemberEvent {
              ty: MemberEventType::Join,
              members: vec![member.member.clone()],
            }
            .into(),
          )
        }),
      )
    } else {
      // Check if we have a join or leave intent. The intent buffer
      // will only hold one event for this node, so the more recent
      // one will take effect.
      let mut status = MemberStatus::Alive;
      let mut status_ltime = LamportTime::new(0);
      if let Some(t) = members.recent_intent(n.id(), MessageType::Join) {
        status_ltime = t;
      }

      if let Some(t) = members.recent_intent(n.id(), MessageType::Leave) {
        status_ltime = t;
        status = MemberStatus::Leaving;
      }

      let ms = MemberState {
        member: Arc::new(Member {
          id: id.clone(),
          tags: decode_tags(n.meta()),
          status: Atomic::new(status),
          // TODO:
          protocol_version: ProtocolVersion::V0,
          delegate_version: DelegateVersion::V0,
        }),
        status_time: status_ltime,
        leave_time: MemberState::zero_leave_time(),
      };
      let member = ms.member.clone();
      members.states.insert(id.clone(), ms);
      (
        MemberStatus::None,
        self.inner.event_tx.as_ref().map(|tx| {
          tx.send(
            MemberEvent {
              ty: MemberEventType::Join,
              members: vec![member],
            }
            .into(),
          )
        }),
      )
    };

    if matches!(old_status, MemberStatus::Failed | MemberStatus::Left) {
      remove_old_member(&mut members.failed_members, id);
      remove_old_member(&mut members.left_members, id);
    }

    // TODO: update metrics

    tracing::info!(target = "ruserf", "member join: {}", id);
    if let Some(fut) = fut {
      if let Err(e) = fut.await {
        tracing::error!(target = "ruserf", err=%e, "failed to send member event");
      }
    }
  }

  /// Called when a node broadcasts a
  /// join message to set the lamport time of its join
  pub(crate) async fn handle_node_join_intent(&self, join_msg: &JoinMessage) -> bool {
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

  pub(crate) async fn handle_node_leave(&self, n: Arc<showbiz_core::Node>) {
    let mut members = self.inner.members.write().await;

    let Some(member_state) = members.states.get_mut(n.id()) else {
      return;
    };

    let mut ms = member_state.member.status.load(Ordering::Acquire);
    let member = match ms {
      MemberStatus::Leaving => {
        member_state
          .member
          .status
          .store(MemberStatus::Left, Ordering::Release);
        ms = MemberStatus::Left;
        member_state.leave_time = Instant::now();
        let member_state = member_state.clone();
        let member = member_state.member.clone();
        members.left_members.push(member_state);
        member
      }
      MemberStatus::Alive => {
        member_state
          .member
          .status
          .store(MemberStatus::Failed, Ordering::Release);
        ms = MemberStatus::Failed;
        member_state.leave_time = Instant::now();
        let member_state = member_state.clone();
        let member = member_state.member.clone();
        members.failed_members.push(member_state);
        member
      }
      _ => {
        tracing::warn!(target = "ruserf", "Bad state when leave: {}", ms);
        return;
      }
    };

    // Send an event along
    let ty = if ms != MemberStatus::Left {
      MemberEventType::Failed
    } else {
      MemberEventType::Leave
    };

    // Update some metrics
    // TODO: metrics

    tracing::info!(target = "ruserf", "{}: {}", ty.as_str(), member.id());

    if let Some(ref tx) = self.inner.event_tx {
      if let Err(e) = tx
        .send(
          MemberEvent {
            ty,
            members: vec![member],
          }
          .into(),
        )
        .await
      {
        tracing::error!(target = "ruserf", err=%e, "failed to send member event: {}", e);
      }
    }
  }

  pub(crate) async fn handle_node_leave_intent(&self, msg: &Leave) -> bool {
    let state = self.state();

    // Witness a potentially newer time
    self.inner.clock.witness(msg.ltime);

    let mut members = self.inner.members.write().await;

    // TODO: There are plenty of duplicated code(to avoid borrow checker), I do not have a good idea how to refactor it currently...
    if msg.prune {
      if let Some(mut member) = members.states.remove(&msg.node) {
        // If the message is old, then it is irrelevant and we can skip it
        if msg.ltime <= member.status_time {
          return false;
        }

        // Refute us leaving if we are in the alive state
        // Must be done in another goroutine since we have the memberLock
        if msg.node.eq(self.inner.memberlist.local_id()) && state == SerfState::Alive {
          tracing::debug!(target = "ruserf", "refuting an older leave intent");
          let this = self.clone();
          let ltime = self.inner.clock.time();
          <T::Runtime as Runtime>::spawn_detach(async move {
            if let Err(e) = this.broadcast_join(ltime).await {
              tracing::error!(target = "ruserf", err=%e, "failed to broadcast join");
            }
          });
          return false;
        }

        // Always update the lamport time even when the status does not change
        // (despite the variable naming implying otherwise).
        //
        // By updating this statusLTime here we ensure that the earlier conditional
        // on "leaveMsg.LTime <= member.statusLTime" will prevent an infinite
        // rebroadcast when seeing two successive leave message for the same
        // member. Without this fix a leave message that arrives after a member is
        // already marked as leaving/left will cause it to be rebroadcast without
        // marking it locally as witnessed. If more than one serf instance in the
        // cluster experiences this series of events then they will rebroadcast
        // each other's messages about the affected node indefinitely.
        //
        // This eventually leads to overflowing serf intent queues
        // - https://github.com/hashicorp/consul/issues/8179
        // - https://github.com/hashicorp/consul/issues/7960
        member.status_time = msg.ltime;
        // State transition depends on current state
        let ms = member.member.status.load(Ordering::Acquire);
        match ms {
          MemberStatus::Alive => {
            member
              .member
              .status
              .store(MemberStatus::Leaving, Ordering::Release);
            let id = member.member.id();
            tracing::info!(target = "ruserf", "EventMemberReap (forced): {}", id);

            let tx = self.inner.event_tx.as_ref();
            let coord = self.inner.coord_core.as_deref();
            erase_node!(tx <- coord(members[id].member));
            true
          }
          MemberStatus::Failed => {
            member
              .member
              .status
              .store(MemberStatus::Left, Ordering::Release);

            // We must push a message indicating the node has now
            // left to allow higher-level applications to handle the
            // graceful leave.
            tracing::info!(
              target = "ruserf",
              "EventMemberLeave: {}",
              member.member.id()
            );

            let msg = self.inner.event_tx.as_ref().map(|tx| {
              tx.send(
                MemberEvent {
                  ty: MemberEventType::Leave,
                  members: vec![member.member.clone()],
                }
                .into(),
              )
            });
            // Remove from the failed list and add to the left list. We add
            // to the left list so that when we do a sync, other nodes will
            // remove it from their failed list.
            remove_old_member(&mut members.failed_members, member.member.id());
            members.left_members.push(member);

            if let Some(fut) = msg {
              if let Err(e) = fut.await {
                tracing::error!(target = "ruserf", err=%e, "failed to send member event");
              }
            }

            true
          }
          MemberStatus::Leaving | MemberStatus::Left => {
            if ms == MemberStatus::Leaving {
              <T::Runtime as Runtime>::sleep(
                self.inner.opts.broadcast_timeout + self.inner.opts.leave_propagate_delay,
              )
              .await;
            }

            let id = member.member.id();
            tracing::info!(target = "ruserf", "EventMemberReap (forced): {}", id);

            // If we are leaving or left we may be in that list of members
            if matches!(ms, MemberStatus::Leaving | MemberStatus::Left) {
              remove_old_member(&mut members.left_members, id);
            }

            let tx = self.inner.event_tx.as_ref();
            let coord = self.inner.coord_core.as_deref();
            erase_node!(tx <- coord(members[id].member));
            true
          }
          _ => false,
        }
      } else {
        // Rebroadcast only if this was an update we hadn't seen before.
        upsert_intent(
          &mut members.recent_intents,
          &msg.node,
          MessageType::Leave,
          msg.ltime,
          Instant::now,
        )
      }
    } else if let Some(member) = members.states.get_mut(&msg.node) {
      // If the message is old, then it is irrelevant and we can skip it
      if msg.ltime <= member.status_time {
        return false;
      }

      // Refute us leaving if we are in the alive state
      // Must be done in another goroutine since we have the memberLock
      if msg.node.eq(self.inner.memberlist.local_id()) && state == SerfState::Alive {
        tracing::debug!(target = "ruserf", "refuting an older leave intent");
        let this = self.clone();
        let ltime = self.inner.clock.time();
        <T::Runtime as Runtime>::spawn_detach(async move {
          if let Err(e) = this.broadcast_join(ltime).await {
            tracing::error!(target = "ruserf", err=%e, "failed to broadcast join");
          }
        });
        return false;
      }

      // Always update the lamport time even when the status does not change
      // (despite the variable naming implying otherwise).
      //
      // By updating this statusLTime here we ensure that the earlier conditional
      // on "leaveMsg.LTime <= member.statusLTime" will prevent an infinite
      // rebroadcast when seeing two successive leave message for the same
      // member. Without this fix a leave message that arrives after a member is
      // already marked as leaving/left will cause it to be rebroadcast without
      // marking it locally as witnessed. If more than one serf instance in the
      // cluster experiences this series of events then they will rebroadcast
      // each other's messages about the affected node indefinitely.
      //
      // This eventually leads to overflowing serf intent queues
      // - https://github.com/hashicorp/consul/issues/8179
      // - https://github.com/hashicorp/consul/issues/7960
      member.status_time = msg.ltime;
      // State transition depends on current state
      match member.member.status.load(Ordering::Acquire) {
        MemberStatus::Alive => {
          member
            .member
            .status
            .store(MemberStatus::Leaving, Ordering::Release);
          true
        }
        MemberStatus::Failed => {
          member
            .member
            .status
            .store(MemberStatus::Left, Ordering::Release);

          // Remove from the failed list and add to the left list. We add
          // to the left list so that when we do a sync, other nodes will
          // remove it from their failed list.
          let member = member.clone();
          let msg = self.inner.event_tx.as_ref().map(|tx| {
            tx.send(
              MemberEvent {
                ty: MemberEventType::Leave,
                members: vec![member.member.clone()],
              }
              .into(),
            )
          });

          members
            .failed_members
            .retain(|m| m.member.id() != member.member.id());
          members.left_members.push(member);
          if let Some(fut) = msg {
            if let Err(e) = fut.await {
              tracing::error!(target = "ruserf", err=%e, "failed to send member event");
            }
          }
          true
        }
        MemberStatus::Leaving | MemberStatus::Left => true,
        _ => false,
      }
    } else {
      // Rebroadcast only if this was an update we hadn't seen before.
      upsert_intent(
        &mut members.recent_intents,
        &msg.node,
        MessageType::Leave,
        msg.ltime,
        Instant::now,
      )
    }
  }

  /// Called when a node meta data update
  /// has taken place
  pub(crate) async fn handle_node_update(&self, n: Arc<showbiz_core::Node>) {
    let mut members = self.inner.members.write().await;
    let id = n.id();
    if let Some(ms) = members.states.get_mut(id) {
      // Update the member attributes
      ms.member = Arc::new(Member {
        id: id.clone(),
        tags: decode_tags(n.meta()),
        status: Atomic::new(ms.member.status.load(Ordering::Relaxed)),
        protocol_version: ProtocolVersion::V0,
        delegate_version: DelegateVersion::V0,
      });

      // TODO: metrics

      tracing::info!(target = "ruserf", "member update: {}", id);
      if let Some(ref tx) = self.inner.event_tx {
        if let Err(e) = tx
          .send(
            MemberEvent {
              ty: MemberEventType::Update,
              members: vec![ms.member.clone()],
            }
            .into(),
          )
          .await
        {
          tracing::error!(target = "ruserf", err=%e, "failed to send member event");
        }
      }
    }
  }

  /// Waits for nodes that are leaving and then forcibly
  /// erases a member from the list of members
  pub(crate) async fn handle_prune(&self, member: &MemberState, members: &mut Members) {
    let ms = member.member.status.load(Ordering::Relaxed);
    if ms == MemberStatus::Leaving {
      <T::Runtime as Runtime>::sleep(
        self.inner.opts.broadcast_timeout + self.inner.opts.leave_propagate_delay,
      )
      .await;
    }

    let id = member.member.id();
    tracing::info!(target = "ruserf", "EventMemberReap (forced): {}", id);

    // If we are leaving or left we may be in that list of members
    if matches!(ms, MemberStatus::Leaving | MemberStatus::Left) {
      remove_old_member(&mut members.left_members, id);
    }

    let tx = self.inner.event_tx.as_ref();
    let coord = self.inner.coord_core.as_deref();
    erase_node!(tx <- coord(members[id].member))
  }

  /// Invoked when a join detects a conflict over a name.
  /// This means two different nodes (IP/Port) are claiming the same name. Memberlist
  /// will reject the "new" node mapping, but we can still be notified.
  pub(crate) async fn handle_node_conflict(
    &self,
    existing: Arc<showbiz_core::Node>,
    other: Arc<showbiz_core::Node>,
  ) {
    // Log a basic warning if the node is not us...
    if existing.id() != self.inner.memberlist.local_id() {
      tracing::warn!(
        target = "ruserf",
        "node conflict detected between {} and {}",
        existing.id(),
        other.id()
      );
      return;
    }

    // The current node is conflicting! This is an error
    tracing::error!(
      target = "ruserf",
      "node id conflicts with another node at {}. node id must be unique! (resolution enabled: {})",
      other.id(),
      self.inner.opts.enable_id_conflict_resolution
    );

    // If automatic resolution is enabled, kick off the resolution
    if self.inner.opts.enable_id_conflict_resolution {
      let this = self.clone();
      <T::Runtime as Runtime>::spawn_detach(async move {
        use showbiz_core::futures_util::StreamExt;

        // Get the local node
        let local = this.inner.memberlist.local_node().await;
        let local_id = local.id();
        let mut payload = BytesMut::with_capacity(<NodeId as NodeIdCodec>::encoded_len(local_id));
        <NodeId as NodeIdCodec>::encode(local_id, &mut payload);
        // Start an id resolution query
        let ty = InternalQueryEventType::Conflict;
        let resp = match this
          .internal_query(SmolStr::new(ty.as_str()), payload.freeze(), None, ty)
          .await
        {
          Ok(resp) => resp,
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to start node id resolution query");
            return;
          }
        };

        // Counter to determine winner
        let mut responses = 0usize;
        let mut matching = 0usize;

        // Gather responses
        while let Some(r) = resp.response_rx().next().await {
          // Decode the response
          if r.payload.is_empty() || r.payload[0] != MessageType::ConflictResponse as u8 {
            tracing::warn!(
              target = "ruserf",
              "invalid conflict query response type: {:?}",
              r.payload.as_ref()
            );
            continue;
          }

          match decode_message::<Member>(&r.payload[1..]) {
            Ok(member) => {
              // Update the counters
              responses += 1;
              if member.id().eq(local_id) {
                matching += 1;
              }
            }
            Err(e) => {
              tracing::error!(target = "ruserf", err=%e, "failed to decode conflict query response");
              continue;
            }
          }
        }

        // Query over, determine if we should live
        let majority = (responses / 2) + 1;
        if matching >= majority {
          tracing::info!(
            target = "ruserf",
            "majority in node id conflict resolution [{} / {}]",
            matching,
            responses
          );
          return;
        }

        // Since we lost the vote, we need to exit
        tracing::warn!(
          target = "ruserf",
          "minority in name conflict resolution, quiting [{} / {}]",
          matching,
          responses
        );
        if let Err(e) = this.shutdown().await {
          tracing::error!(target = "ruserf", err=%e, "failed to shutdown");
        }
      });
    }
  }

  fn handle_rejoin(showbiz: Showbiz<T, SerfDelegate<T, D, O>>, alive_nodes: Vec<NodeId>) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      for prev in alive_nodes {
        // Do not attempt to join ourself
        if prev.eq(showbiz.local_id()) {
          continue;
        }

        tracing::info!(
          target = "ruserf",
          "attempting re-join to previously known node {}",
          prev
        );
        if let Err(e) = showbiz.join_node(&prev).await {
          tracing::warn!(
            target = "ruserf",
            "failed to re-join to previously known node {}: {}",
            prev,
            e
          );
        } else {
          tracing::info!(
            target = "ruserf",
            "re-joined to previously known node: {}",
            prev
          );
          return;
        }
      }

      tracing::warn!(
        target = "ruserf",
        "failed to re-join to any previously known node"
      );
    });
  }
}

/// Used to remove an old member from a list of old
/// members.
fn remove_old_member(old: &mut Vec<MemberState>, id: &NodeId) {
  old.retain(|m| m.member.id() != id);
}

/// Clears out any intents that are older than the timeout. Make sure
/// the memberLock is held when passing in the Serf instance's recentIntents
/// member.
fn reap_intents(intents: &mut HashMap<NodeId, NodeIntent>, timeout: Duration) {
  intents.retain(|_, intent| intent.wall_time.elapsed() <= timeout);
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
    std::collections::hash_map::Entry::Vacant(ent) => {
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
pub(crate) fn encode_tags<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  tag: &HashMap<SmolStr, SmolStr>,
) -> Result<Bytes, Error<T, D, O>>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
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
pub(crate) fn decode_tags(src: &[u8]) -> HashMap<SmolStr, SmolStr> {
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
