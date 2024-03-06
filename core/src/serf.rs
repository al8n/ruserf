use std::{
  collections::HashMap,
  future::Future,
  hash::Hash,
  sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
  },
  time::{Duration, Instant},
};

use async_lock::{Mutex, RwLock};
use atomic::Atomic;
use futures::{FutureExt, Stream};
use memberlist_core::{
  agnostic::Runtime,
  bytes::{BufMut, Bytes, BytesMut},
  queue::TransmitLimitedQueue,
  tracing,
  transport::{AddressResolver, MaybeResolvedAddress, Node, Transport},
  types::{DelegateVersion, Meta, NodeState, OneOrMore, ProtocolVersion, SmallVec, TinyVec},
  CheapClone, Memberlist,
};
use rand::{Rng, SeedableRng};
use smol_str::SmolStr;

use crate::{
  broadcast::SerfBroadcast,
  clock::{LamportClock, LamportTime},
  coalesce::{coalesced_event, MemberEventCoalescer, UserEventCoalescer},
  coordinate::{Coordinate, CoordinateClient, CoordinateOptions},
  delegate::{CompositeDelegate, Delegate, TransformDelegate},
  error::{Error, JoinError},
  event::{Event, InternalQueryEvent, MemberEvent, MemberEventType},
  internal_query::SerfQueries,
  query::{QueryParam, QueryResponse},
  snapshot::{open_and_replay_snapshot, Snapshot, SnapshotHandle},
  types::{
    JoinMessage, Leave, MessageUserEvent, QueryFlag, QueryMessage, QueryResponseMessage,
    SerfMessage,
  },
  MessageType, Options, QueueOptions, Tags,
};

#[cfg(feature = "encryption")]
use crate::key_manager::KeyManager;

mod delegate;
pub(crate) use delegate::SerfDelegate;

/// Exports the default delegate type
pub type DefaultDelegate<T> = CompositeDelegate<
  <T as Transport>::Id,
  <<T as Transport>::Resolver as AddressResolver>::ResolvedAddress,
>;

const MAGIC_BYTE: u8 = 255;

/// Maximum 128 KB snapshot
const SNAPSHOT_SIZE_LIMIT: u64 = 128 * 1024;

/// Maximum 9KB for event name and payload
const USER_EVENT_SIZE_LIMIT: usize = 9 * 1024;

/// Used to buffer events to prevent re-delivery
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct UserEvents {
  pub(crate) ltime: LamportTime,
  pub(crate) events: Vec<UserEvent>,
}

/// Stores all the user events at a specific time
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct UserEvent {
  pub(crate) name: SmolStr,
  pub(crate) payload: Bytes,
}

/// Stores all the query ids at a specific time
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Queries {
  ltime: LamportTime,
  query_ids: Vec<u32>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, bytemuck::NoUninit)]
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

#[cfg(feature = "serde")]
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

/// A single member of the Serf cluster.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Member<I, A> {
  node: Node<I, A>,
  tags: Tags,
  #[cfg_attr(feature = "serde", serde(with = "serde_member_status"))]
  status: Atomic<MemberStatus>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

/// Used to track members that are no longer active due to
/// leaving, failing, partitioning, etc. It tracks the member along with
/// when that member was marked as leaving.
#[viewit::viewit]
#[derive(Clone)]
pub(crate) struct MemberState<I, A> {
  member: Arc<Member<I, A>>,
  /// lamport clock time of last received message
  status_time: LamportTime,
  /// wall clock time of leave
  leave_time: Instant,
}

impl<I, A> MemberState<I, A> {
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

// #[derive(Clone)]
// pub(crate) struct SerfNodeCalculator {
//   members: Arc<RwLock<Members>>,
// }

// impl SerfNodeCalculator {
//   pub(crate) fn new(members: Arc<RwLock<Members>>) -> Self {
//     Self { members }
//   }
// }

// impl NodeCalculator for SerfNodeCalculator {
//   fn num_nodes(&self) -> usize {
//     use pollster::FutureExt as _;
//     self.members.read().block_on().states.len()
//   }
// }

pub(crate) struct CoordCore<I> {
  pub(crate) client: CoordinateClient<I>,
  pub(crate) cache: parking_lot::RwLock<HashMap<I, Coordinate>>,
}

#[derive(Default)]
pub(crate) struct QueryCore<I, A> {
  responses: HashMap<LamportTime, QueryResponse<I, A>>,
  min_time: LamportTime,
  buffer: Vec<Option<Queries>>,
}

#[derive(Default)]
pub(crate) struct Members<I, A> {
  pub(crate) states: HashMap<I, MemberState<I, A>>,
  recent_intents: HashMap<I, NodeIntent>,
  pub(crate) left_members: OneOrMore<MemberState<I, A>>,
  failed_members: OneOrMore<MemberState<I, A>>,
}

impl<I: Eq + Hash, A: Eq + Hash> Members<I, A> {
  fn recent_intent(&self, id: &I, ty: MessageType) -> Option<LamportTime> {
    match self.recent_intents.get(id) {
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

pub(crate) struct SerfCore<T: Transport, D = DefaultDelegate<T>>
where
  D: Delegate,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) clock: LamportClock,
  pub(crate) event_clock: LamportClock,
  pub(crate) query_clock: LamportClock,

  pub(crate) broadcasts: Arc<
    TransmitLimitedQueue<SerfBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  >,
  pub(crate) event_broadcasts: Arc<
    TransmitLimitedQueue<SerfBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  >,
  pub(crate) query_broadcasts: Arc<
    TransmitLimitedQueue<SerfBroadcast<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  >,

  pub(crate) memberlist: Memberlist<T, SerfDelegate<T, D>>,
  pub(crate) members:
    Arc<RwLock<Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,
  pub(crate) num_members: Arc<AtomicUsize>,
  event_tx: Option<async_channel::Sender<Event<T, D>>>,
  pub(crate) event_join_ignore: AtomicBool,

  pub(crate) event_core: RwLock<EventCore>,
  query_core: Arc<RwLock<QueryCore<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,

  pub(crate) opts: Options,

  state: parking_lot::Mutex<SerfState>,

  join_lock: Mutex<()>,

  snapshot: Option<SnapshotHandle>,
  #[cfg(feature = "encryption")]
  key_manager: KeyManager<T, D>,

  shutdown_tx: async_channel::Sender<()>,
  shutdown_rx: async_channel::Receiver<()>,

  pub(crate) coord_core: Option<Arc<CoordCore<T::Id>>>,
}

impl<T, D> Drop for SerfCore<T, D>
where
  T: Transport,
  D: Delegate,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn drop(&mut self) {
    use pollster::FutureExt as _;

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
pub struct Serf<T: Transport, D = DefaultDelegate<T>>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) inner: Arc<SerfCore<T, D>>,
}

impl<T: Transport, D: Delegate> Clone for Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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
  pub async fn new(transport: T, opts: Options) -> Result<Self, Error<T, DefaultDelegate<T>>> {
    Self::new_in(transport, None, None, opts).await
  }

  pub async fn with_event_sender(
    transport: T,
    ev: async_channel::Sender<Event<T, DefaultDelegate<T>>>,
    opts: Options,
  ) -> Result<Self, Error<T, DefaultDelegate<T>>> {
    Self::new_in(transport, Some(ev), None, opts).await
  }
}

impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn with_delegate(
    transport: T,
    delegate: D,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    Self::new_in(transport, None, Some(delegate), opts).await
  }
}

// ------------------------------------Public methods------------------------------------
impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub async fn with_event_sender_and_delegate(
    transport: T,
    delegate: D,
    ev: async_channel::Sender<Event<T, D>>,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    Self::new_in(transport, Some(ev), Some(delegate), opts).await
  }

  async fn new_in(
    transport: T,
    ev: Option<async_channel::Sender<Event<T, D>>>,
    delegate: Option<D>,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    if opts.max_user_event_size > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventLimitTooLarge(USER_EVENT_SIZE_LIMIT));
    }

    // Check that the meta data length is okay
    if let Some(tags) = opts.tags.load() {
      let len = <D as TransformDelegate>::tags_encoded_len(tags);
      if len > Meta::MAX_SIZE {
        return Err(Error::TagsTooLarge(len));
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
    let num_members = Arc::new(AtomicUsize::new(0));
    let bn_members = num_members.clone();
    let en_members = num_members.clone();
    let qn_members = num_members.clone();

    // Setup the various broadcast queues, which we use to send our own
    // custom broadcasts along the gossip channel.
    let broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.showbiz_options.retransmit_mult,
      move || bn_members.load(Ordering::SeqCst),
    ));
    let event_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.showbiz_options.retransmit_mult,
      move || en_members.load(Ordering::SeqCst),
    ));
    let query_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.showbiz_options.retransmit_mult,
      move || qn_members.load(Ordering::SeqCst),
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
    let memberlist = Memberlist::with_delegate(
      transport,
      SerfDelegate::new(delegate),
      opts.showbiz_options.clone(),
    )
    .await?;

    let c = SerfCore {
      clock,
      event_clock,
      query_clock,
      broadcasts,
      memberlist,
      members,
      num_members,
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
      #[cfg(feature = "encryption")]
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
      .map_err(|e| memberlist_core::error::Error::delegate(e))?;

    // update key manager
    #[cfg(feature = "encryption")]
    {
      let that = this.clone();
      this.inner.key_manager.store(that);
    }

    // Start the background tasks. See the documentation above each method
    // for more information on their role.
    Reaper {
      coord_core: this.inner.coord_core.clone(),
      memberlist: this.inner.memberlist.clone(),
      num_members: this.inner.num_members.clone(),
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
  pub async fn local_member(
    &self,
  ) -> Arc<Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
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
  pub async fn set_tags(&self, tags: Tags) -> Result<(), Error<T, D>> {
    // Check that the meta data length is okay
    let tags_encoded_len = <D as TransformDelegate>::tags_encoded_len(&tags);
    if tags_encoded_len > Meta::MAX_SIZE {
      return Err(Error::TagsTooLarge(tags_encoded_len));
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
  ) -> Result<(), Error<T, D>> {
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
    let msg = SerfMessage::UserEvent(msg);

    // Start broadcasting the event
    let len = <D as TransformDelegate>::message_encoded_len(&msg);

    // Check the size after encoding to be sure again that
    // we're not attempting to send over the specified size limit.
    if len > self.inner.opts.max_user_event_size {
      return Err(Error::RawUserEventTooLarge(len));
    }

    if len > USER_EVENT_SIZE_LIMIT {
      return Err(Error::RawUserEventTooLarge(len));
    }

    let mut raw = BytesMut::with_capacity(len + 1); // + 1 for message type byte
    raw.put_u8(MessageType::UserEvent as u8);
    raw.resize(len + 1, 0);

    let actual_encoded_len = <D as TransformDelegate>::encode_message(&msg, &mut raw[1..])?;
    debug_assert_eq!(
      actual_encoded_len, len,
      "expected encoded len {} mismatch the actual encoded len {}",
      len, actual_encoded_len
    );

    self.inner.event_clock.increment();

    // Process update locally
    self.handle_user_event(msg).await;

    self
      .inner
      .event_broadcasts
      .queue_broadcast(SerfBroadcast {
        msg: raw.freeze(),
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
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    self.query_in(name, payload, params, None).await
  }

  async fn internal_query(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ty: InternalQueryEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    self.query_in(name, payload, params, Some(ty)).await
  }

  async fn query_in(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ty: Option<InternalQueryEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    // Provide default parameters if none given.
    let params = match params {
      Some(params) => params,
      None => self.default_query_param().await,
    };

    // Get the local node
    let local = self.inner.memberlist.advertise_node();

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
      from: local.cheap_clone(),
      filters,
      flags: flags.bits(),
      relay_factor: params.relay_factor,
      timeout: params.timeout,
      name: name.clone(),
      payload,
    };
    // let msg_ref = SerfMessageRef::Query(&q);

    // Encode the query
    let len = <D as TransformDelegate>::message_encoded_len(&q);

    // Check the size
    if len > self.inner.opts.query_size_limit {
      return Err(Error::QueryTooLarge(len));
    }

    let mut raw = BytesMut::with_capacity(len + 1); // + 1 for message type byte
    raw.put_u8(MessageType::Query as u8);
    raw.resize(len + 1, 0);
    let actual_encoded_len = <D as TransformDelegate>::encode_message(&q, &mut raw[1..])?;
    debug_assert_eq!(
      actual_encoded_len, len,
      "expected encoded len {} mismatch the actual encoded len {}",
      len, actual_encoded_len
    );

    // Register QueryResponse to track acks and responses
    let resp = q.response(self.inner.memberlist.num_online_members().await);
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
        msg: raw.freeze(),
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
    existing: impl Iterator<Item = Node<T::Id, MaybeResolvedAddress<T>>>,
    ignore_old: bool,
  ) -> Result<
    SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    JoinError<T, D>,
  > {
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
  pub async fn leave(&self) -> Result<(), Error<T, D>> {
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

    let msg = SerfMessage::Leave(msg);

    // Only broadcast the leave message if there is at least one
    // other node alive.
    if self.has_alive_members().await {
      let (notify_tx, notify_rx) = async_channel::bounded(1);
      self.broadcast(msg, Some(notify_tx)).await?;

      futures::select! {
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
  pub async fn remove_failed_node(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.force_leave(node, false).await
  }

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  pub async fn remove_failed_node_prune(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
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
  pub async fn shutdown(&self) -> Result<(), Error<T, D>> {
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
  pub fn get_cooridate(&self) -> Result<Coordinate, Error<T, D>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.client.get_coordinate());
    }

    Err(Error::CoordinatesDisabled)
  }

  /// Returns the network coordinate for the node with the given
  /// name. This will only be valid if `disable_coordinates` is set to `false`.
  pub fn get_cached_coordinate(&self, id: &T::Id) -> Result<Option<Coordinate>, Error<T, D>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.cache.read().get(id).cloned());
    }

    Err(Error::CoordinatesDisabled)
  }
}

// ---------------------------------Private Methods-------------------------------

impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
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
  async fn register_query_response(
    &self,
    timeout: Duration,
    resp: QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
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
    msg: SerfMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    notify_tx: Option<async_channel::Sender<()>>,
  ) -> Result<(), Error<T, D>> {
    let ty = MessageType::from(&msg);
    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&msg);
    let mut raw = BytesMut::with_capacity(expected_encoded_len + 1); // + 1 for message type byte
    raw.put_u8(ty as u8);
    raw.resize(expected_encoded_len + 1, 0);
    let len =
      <D as TransformDelegate>::encode_message(&msg, &mut raw[1..]).map_err(Error::transform)?;
    debug_assert_eq!(
      len, expected_encoded_len,
      "expected encoded len {} mismatch the actual encoded len {}",
      expected_encoded_len, len
    );

    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
        msg: raw.into(),
        notify_tx,
      })
      .await;
    Ok(())
  }

  /// Broadcasts a new join intent with a
  /// given clock value. It is used on either join, or if
  /// we need to refute an older leave intent. Cannot be called
  /// with the memberLock held.
  async fn broadcast_join(&self, ltime: LamportTime) -> Result<(), Error<T, D>> {
    // Construct message to update our lamport clock
    let msg = JoinMessage::new(ltime, self.inner.memberlist.advertise_node());
    self.inner.clock.witness(ltime);

    // Process update locally
    self.handle_node_join_intent(&msg).await;

    let msg = SerfMessage::Join(msg);
    // Start broadcasting the update
    if let Err(e) = self.broadcast(msg, None).await {
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
  async fn force_leave(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    prune: bool,
  ) -> Result<(), Error<T, D>> {
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

    let msg = SerfMessage::Leave(msg);
    // Broadcast the remove
    let (ntx, nrx) = async_channel::bounded(1);
    self.broadcast(&msg, Some(ntx)).await?;

    // Wait for the broadcast
    <T::Runtime as Runtime>::timeout(self.inner.opts.broadcast_timeout, nrx.recv())
      .await
      .map_err(|_| Error::RemovalBroadcastTimeout)
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

struct Reaper<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  coord_core: Option<Arc<CoordCore<T::Id>>>,
  memberlist: Memberlist<T, SerfDelegate<T, D>>,
  members: Arc<RwLock<Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,
  num_members: Arc<AtomicUsize>,
  event_tx: Option<async_channel::Sender<Event<T, D>>>,
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
      cc.client.forget_node($id);
      cc.cache.write().remove($id);
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

impl<T, D> Reaper<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn spawn(self) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = <T::Runtime as Runtime>::sleep(self.reap_interval).fuse() => {
            let mut ms = self.members.write().await;
            Self::reap_failed(&mut ms, self.event_tx.as_ref(), self.memberlist.delegate(), self.coord_core.as_deref(), self.reconnect_timeout).await;
            Self::reap_left(&mut ms, self.event_tx.as_ref(), self.memberlist.delegate(), self.coord_core.as_deref(), self.reconnect_timeout).await;
            self.num_members.store(ms.states.len(), Ordering::SeqCst);
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    });
  }

  async fn reap_failed(
    old: &mut Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    event_tx: Option<&async_channel::Sender<Event<T, D>>>,
    reconnector: Option<&D>,
    coord: Option<&CoordCore<T::Id>>,
    timeout: Duration,
  ) {
    reap!(event_tx <- reconnector(timeout(old.failed_members, coord)))
  }

  async fn reap_left(
    old: &mut Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    event_tx: Option<&async_channel::Sender<Event<T, D>>>,
    reconnector: Option<&D>,
    coord: Option<&CoordCore<T::Id>>,
    timeout: Duration,
  ) {
    reap!(event_tx <- reconnector(timeout(old.left_members, coord)))
  }
}

struct Reconnector<T, D>
where
  T: Transport,
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  members: Arc<RwLock<Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,
  memberlist: Memberlist<T, SerfDelegate<T, D>>,
  shutdown_rx: async_channel::Receiver<()>,
  reconnect_interval: Duration,
}

impl<T, D> Reconnector<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn spawn(self) {
    let mut rng = rand::rngs::StdRng::from_rng(rand::thread_rng()).unwrap();

    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures::select! {
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

struct QueueChecker<I, A> {
  name: &'static str,
  queue: Arc<TransmitLimitedQueue<SerfBroadcast<I, A>>>,
  members: Arc<RwLock<Members<I, A>>>,
  opts: QueueOptions,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<I, A> QueueChecker<I, A> {
  fn spawn<R: Runtime>(self)
  where
    <<R as Runtime>::Sleep as Future>::Output: Send,
  {
    R::spawn_detach(async move {
      loop {
        futures::select! {
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
impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
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
    q: QueryMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    ty: Option<InternalQueryEvent<T, D>>,
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
        from: self.inner.memberlist.advertise_node(),
        flags: QueryFlag::ACK.bits(),
        payload: Bytes::new(),
      };

      let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&ack);
      let mut raw = BytesMut::with_capacity(expected_encoded_len + 1); // + 1 for message type byte
      raw.put_u8(MessageType::QueryResponse as u8);
      raw.resize(expected_encoded_len + 1, 0);

      match <D as TransformDelegate>::encode_message(&ack, &mut raw[1..]) {
        Ok(len) => {
          debug_assert_eq!(
            len, expected_encoded_len,
            "expected encoded len {} mismatch the actual encoded len {}",
            expected_encoded_len, len
          );
          if let Err(e) = self
            .inner
            .memberlist
            .send(q.from().address(), raw.freeze())
            .await
          {
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
  pub(crate) async fn handle_query_response(
    &self,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
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

      query.handle_query_response::<T, D>(resp).await;
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
  pub(crate) async fn handle_node_join(
    &self,
    n: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) {
    let mut members = self.inner.members.write().await;

    // TODO: message dropper?

    let node = n.node();
    let tags = match <D as TransformDelegate>::decode_tags(n.meta()) {
      Ok(tags) => tags,
      Err(e) => {
        tracing::error!(target = "ruserf", err=%e, "failed to decode tags");
        return;
      }
    };

    let (old_status, fut) = if let Some(member) = members.states.get_mut(node.id()) {
      let old_status = member.member.status.load(Ordering::Acquire);
      let dead_time = member.leave_time.elapsed();
      if old_status == MemberStatus::Failed && dead_time < self.inner.opts.flap_timeout {
        // TODO: metrics
      }

      *member = MemberState {
        member: Arc::new(Member {
          node: node.cheap_clone(),
          tags,
          status: Atomic::new(MemberStatus::Alive),
          protocol_version: ProtocolVersion::V0,
          delegate_version: DelegateVersion::V0,
        }),
        status_time: member.status_time,
        leave_time:
          MemberState::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::zero_leave_time(),
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
          node: node.cheap_clone(),
          tags,
          status: Atomic::new(status),
          protocol_version: ProtocolVersion::V0,
          delegate_version: DelegateVersion::V0,
        }),
        status_time: status_ltime,
        leave_time:
          MemberState::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::zero_leave_time(),
      };
      let member = ms.member.clone();
      members.states.insert(node.id().cheap_clone(), ms);
      self
        .inner
        .num_members
        .store(members.states.len(), Ordering::SeqCst);
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
      remove_old_member(&mut members.failed_members, node.id());
      remove_old_member(&mut members.left_members, node.id());
    }

    // TODO: update metrics

    tracing::info!(target = "ruserf", "member join: {}", node);
    if let Some(fut) = fut {
      if let Err(e) = fut.await {
        tracing::error!(target = "ruserf", err=%e, "failed to send member event");
      }
    }
  }

  /// Called when a node broadcasts a
  /// join message to set the lamport time of its join
  pub(crate) async fn handle_node_join_intent(
    &self,
    join_msg: &JoinMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> bool {
    // Witness a potentially newer time
    self.inner.clock.witness(join_msg.ltime);

    let mut members = self.inner.members.write().await;
    match members.states.get_mut(join_msg.node.id()) {
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
          join_msg.node.id(),
          MessageType::Join,
          join_msg.ltime,
          Instant::now,
        )
      }
    }
  }

  pub(crate) async fn handle_node_leave(
    &self,
    n: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) {
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

    tracing::info!(target = "ruserf", "{}: {}", ty.as_str(), member.node());

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

  pub(crate) async fn handle_node_leave_intent(
    &self,
    msg: &Leave<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> bool {
    let state = self.state();

    // Witness a potentially newer time
    self.inner.clock.witness(msg.ltime);

    let mut members = self.inner.members.write().await;

    // TODO: There are plenty of duplicated code(to avoid borrow checker), I do not have a good idea how to refactor it currently...
    if msg.prune {
      if let Some(mut member) = members.states.remove(msg.node.id()) {
        self
          .inner
          .num_members
          .store(members.states.len(), Ordering::SeqCst);

        // If the message is old, then it is irrelevant and we can skip it
        if msg.ltime <= member.status_time {
          return false;
        }

        // Refute us leaving if we are in the alive state
        // Must be done in another goroutine since we have the memberLock
        if msg.node.id().eq(self.inner.memberlist.local_id()) && state == SerfState::Alive {
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
            let node = member.member.node;
            let id = node.id();
            tracing::info!(target = "ruserf", "EventMemberReap (forced): {}", node);

            let tx = self.inner.event_tx.as_ref();
            let coord = self.inner.coord_core.as_deref();
            erase_node!(tx <- coord(members[id].member));
            self
              .inner
              .num_members
              .store(members.states.len(), Ordering::SeqCst);
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
              member.member.node
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
            remove_old_member(&mut members.failed_members, member.member.node.id());
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

            let node = member.member.node;
            let id = node.id();
            tracing::info!(target = "ruserf", "EventMemberReap (forced): {}", node);

            // If we are leaving or left we may be in that list of members
            if matches!(ms, MemberStatus::Leaving | MemberStatus::Left) {
              remove_old_member(&mut members.left_members, id);
            }

            let tx = self.inner.event_tx.as_ref();
            let coord = self.inner.coord_core.as_deref();
            erase_node!(tx <- coord(members[id].member));
            self
              .inner
              .num_members
              .store(members.states.len(), Ordering::SeqCst);
            true
          }
          _ => false,
        }
      } else {
        // Rebroadcast only if this was an update we hadn't seen before.
        upsert_intent(
          &mut members.recent_intents,
          msg.node.id(),
          MessageType::Leave,
          msg.ltime,
          Instant::now,
        )
      }
    } else if let Some(member) = members.states.get_mut(msg.node.id()) {
      // If the message is old, then it is irrelevant and we can skip it
      if msg.ltime <= member.status_time {
        return false;
      }

      // Refute us leaving if we are in the alive state
      // Must be done in another goroutine since we have the memberLock
      if msg.node.id().eq(self.inner.memberlist.local_id()) && state == SerfState::Alive {
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
            .retain(|m| m.member.node.id().ne(member.member.node.id()));
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
        msg.node.id(),
        MessageType::Leave,
        msg.ltime,
        Instant::now,
      )
    }
  }

  /// Called when a node meta data update
  /// has taken place
  pub(crate) async fn handle_node_update(
    &self,
    n: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) {
    let tags = match <D as TransformDelegate>::decode_tags(n.meta()) {
      Ok(tags) => tags,
      Err(e) => {
        tracing::error!(target = "ruserf", err=%e, "failed to decode tags");
        return;
      }
    };
    let mut members = self.inner.members.write().await;
    let id = n.id();
    if let Some(ms) = members.states.get_mut(id) {
      // Update the member attributes
      ms.member = Arc::new(Member {
        node: n.node(),
        tags,
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
  pub(crate) async fn handle_prune(
    &self,
    member: &MemberState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    members: &mut Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
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
    erase_node!(tx <- coord(members[id].member));
    self
      .inner
      .num_members
      .store(members.states.len(), Ordering::SeqCst);
  }

  /// Invoked when a join detects a conflict over a name.
  /// This means two different nodes (IP/Port) are claiming the same name. Memberlist
  /// will reject the "new" node mapping, but we can still be notified.
  pub(crate) async fn handle_node_conflict(
    &self,
    existing: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    other: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
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
        // Get the local node
        let local_id = this.inner.memberlist.local_id();
        let encoded_id_len = <D as TransformDelegate>::id_encoded_len(local_id);
        let mut payload = vec![0u8; encoded_id_len];

        if let Err(e) = <D as TransformDelegate>::encode_id(local_id, &mut payload) {
          tracing::error!(target = "ruserf", err=%e, "failed to encode local id");
          return;
        }

        // Start an id resolution query
        // let ty = InternalQueryEvent::Conflict(local_id.clone());
        let ty = InternalQueryEvent::Conflict;
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

          match <D as TransformDelegate>::decode_message(&r.payload[1..]) {
            Ok((_, decoded)) => {
              match decoded {
                SerfMessage::ConflictResponse(member) => {
                  // Update the counters
                  responses += 1;
                  if member.node.id().eq(local_id) {
                    matching += 1;
                  }
                }
                msg => {
                  tracing::warn!(
                    target = "ruserf",
                    "invalid conflict query response type: {}",
                    msg.as_str()
                  );
                  continue;
                }
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

  fn handle_rejoin(
    memberlist: Memberlist<T, SerfDelegate<T, D>>,
    alive_nodes: TinyVec<Node<T::Id, MaybeResolvedAddress<T>>>,
  ) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      for prev in alive_nodes {
        // Do not attempt to join ourself
        if prev.id().eq(memberlist.local_id()) {
          continue;
        }

        tracing::info!(
          target = "ruserf",
          "attempting re-join to previously known node {}",
          prev
        );
        if let Err(e) = memberlist.join(prev).await {
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
fn remove_old_member<I, A>(old: &mut OneOrMore<MemberState<I, A>>, id: &I) {
  old.retain(|m| m.member.node.id() != id);
}

/// Clears out any intents that are older than the timeout. Make sure
/// the memberLock is held when passing in the Serf instance's recentIntents
/// member.
fn reap_intents<I>(intents: &mut HashMap<I, NodeIntent>, timeout: Duration) {
  intents.retain(|_, intent| intent.wall_time.elapsed() <= timeout);
}

fn upsert_intent<I>(
  intents: &mut HashMap<I, NodeIntent>,
  node: &I,
  t: MessageType,
  ltime: LamportTime,
  stamper: impl FnOnce() -> Instant,
) -> bool
where
  I: CheapClone + Eq + core::hash::Hash,
{
  match intents.entry(node.cheap_clone()) {
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

// /// Used to encode a tag map
// pub(crate) fn encode_tags<T: Transport, D: MergeDelegate, O: ReconnectDelegate>(
//   tag: &HashMap<SmolStr, SmolStr>,
// ) -> Result<Bytes, Error<T, D, O>>
// where
//   <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
//   <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
// {
//   struct EncodeHelper(BytesMut);

//   impl std::io::Write for EncodeHelper {
//     fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
//       self.0.put_slice(buf);
//       Ok(buf.len())
//     }

//     fn flush(&mut self) -> std::io::Result<()> {
//       Ok(())
//     }
//   }

//   let mut buf = EncodeHelper(BytesMut::with_capacity(128));
//   // Use a magic byte prefix and msgpack encode the tags
//   buf.0.put_u8(MAGIC_BYTE);
//   match tag.serialize(&mut rmp_serde::Serializer::new(&mut buf)) {
//     Ok(_) => Ok(buf.0.freeze()),
//     Err(e) => {
//       tracing::error!(target = "reserf", err=%e, "failed to encode tags");
//       Err(Error::Encode(e))
//     }
//   }
// }

// /// Used to decode a tag map
// pub(crate) fn decode_tags(src: &[u8]) -> HashMap<SmolStr, SmolStr> {
//   // Decode the tags
//   let r = std::io::Cursor::new(&src[1..]);
//   let mut de = rmp_serde::Deserializer::new(r);
//   match HashMap::<SmolStr, SmolStr>::deserialize(&mut de) {
//     Ok(tags) => tags,
//     Err(e) => {
//       tracing::error!(target = "reserf", err=%e, "failed to decode tags");
//       HashMap::new()
//     }
//   }
// }
