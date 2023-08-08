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
  coordinate::{Coordinate, CoordinateClient, CoordinateOptions},
  delegate::{DefaultMergeDelegate, MergeDelegate, SerfDelegate},
  error::{Error, JoinError},
  event::{Event, MemberEvent, MemberEventType},
  internal_query::SerfQueries,
  query::{QueryParam, QueryResponse},
  snapshot::{open_and_replay_snapshot, Snapshot, SnapshotHandle},
  types::{encode_message, JoinMessage, MessageType, MessageUserEvent, QueryFlag, QueryMessage},
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
  ltime: LamportTime,
  events: Vec<UserEvent>,
}

/// Stores all the user events at a specific time
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct UserEvent {
  name: String,
  payload: Bytes,
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
  None,
  Alive,
  Leaving,
  Left,
  Failed,
}

fn serialize_atomic_member_status<S>(
  status: &Atomic<MemberStatus>,
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: serde::ser::Serializer,
{
  let status = status.load(atomic::Ordering::Relaxed);
  serializer.serialize_u8(status as u8)
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
#[derive(Debug, serde::Serialize)]
pub struct Member {
  id: NodeId,
  tags: Arc<HashMap<SmolStr, SmolStr>>,
  #[serde(serialize_with = "serialize_atomic_member_status")]
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

#[derive(Clone)]
pub(crate) struct SerfNodeCalculator {
  members: Arc<RwLock<Members>>,
}

impl SerfNodeCalculator {
  pub(crate) fn new(members: Arc<RwLock<Members>>) -> Self {
    Self { members }
  }
}

#[showbiz_core::async_trait::async_trait]
impl NodeCalculator for SerfNodeCalculator {
  async fn num_nodes(&self) -> usize {
    self.members.read().await.states.len()
  }
}

pub(crate) struct CoordCore {
  client: CoordinateClient,
  cache: parking_lot::RwLock<HashMap<Name, Coordinate>>,
}

#[derive(Default)]
pub(crate) struct QueryCore {
  responses: HashMap<LamportTime, QueryResponse>,
  query_min_time: LamportTime,
  buffer: Vec<Option<QueryResponse>>,
}

#[derive(Default)]
pub(crate) struct Members {
  pub(crate) states: HashMap<NodeId, MemberState>,
  recent_intents: HashMap<NodeId, NodeIntent>,
  left_members: Vec<MemberState>,
  failed_members: Vec<MemberState>,
}

pub(crate) struct EventCore {
  event_min_time: LamportTime,
  event_buffer: Vec<Option<UserEvents>>,
}

#[viewit::viewit]
pub(crate) struct SerfCore<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> {
  clock: LamportClock,
  event_clock: LamportClock,
  query_clock: LamportClock,

  broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  memberlist: Showbiz<T, SerfDelegate<T, D, O>>,
  members: Arc<RwLock<Members>>,

  event_tx: Option<async_channel::Sender<Event<T, D, O>>>,
  event_broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  event_join_ignore: AtomicBool,
  event_core: RwLock<EventCore>,

  query_broadcasts: Arc<TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>>,
  query_core: Arc<RwLock<QueryCore>>,

  opts: Options<T>,

  state: Mutex<SerfState>,

  join_lock: Mutex<()>,

  snapshot: Option<SnapshotHandle>,
  key_manager: KeyManager<T, D, O>,

  shutdown_tx: async_channel::Sender<()>,
  reconnector: Option<Arc<O>>,

  coord_core: Option<Arc<CoordCore>>,
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
> {
  pub(crate) inner: Arc<SerfCore<D, T, O>>,
}

impl<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> Clone for Serf<T, D, O> {
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
        event_min_time,
        event_buffer,
      }),
      query_broadcasts,
      query_core: Arc::new(RwLock::new(QueryCore {
        query_min_time,
        responses: HashMap::new(),
        buffer: query_buffer,
      })),
      opts,
      state: Mutex::new(SerfState::Alive),
      join_lock: Mutex::new(()),
      snapshot: handle,
      key_manager: KeyManager::new(),
      shutdown_tx,
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
    this.inner.memberlist.delegate().unwrap().store(that);
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
  pub async fn join_many(
    &self,
    existing: impl Iterator<Item = (Address, Name)>,
    ignore_old: bool,
  ) -> Result<Vec<NodeId>, JoinError<T, D, O>> {
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

  pub async fn leave(&self) -> Result<(), Error<T, D, O>> {
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
  pub(crate) async fn handle_user_event(&self) -> bool {
    // TODO: implement this method
    true
  }

  pub(crate) async fn handle_query(&self, q: QueryMessage) {
    todo!()
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
    node: NodeId,
    msg: impl Serialize,
    notify_tx: Option<async_channel::Sender<()>>,
  ) -> Result<(), Error<T, D, O>> {
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
  async fn broadcast_join(&self, ltime: LamportTime) -> Result<(), Error<T, D, O>> {
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

      // takes a node completely out of the member list
      $members.states.remove(id);

      // Tell the coordinate client the node has gone away and delete
      // its cached coordinates.
      if let Some(cc) = $coord {
        cc.client.forget_node(id.name());
        cc.cache.write().remove(id.name());
      }

      // Send out event
      if let Some(tx) = $tx {
        let _ = tx
          .send(Event::from(MemberEvent {
            ty: MemberEventType::Reap,
            members: vec![m.member.clone()],
          }))
          .await;
      }
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

struct Reconnector<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
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
            let numq = self.queue.num_queued().await;
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
fn encode_tags<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  tag: &HashMap<SmolStr, SmolStr>,
) -> Result<Bytes, Error<T, D, O>> {
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
