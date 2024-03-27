use std::{
  sync::atomic::{AtomicUsize, Ordering},
  time::{Duration, Instant},
};

use atomic::Atomic;
use futures::{Future, FutureExt, StreamExt};
use memberlist_core::{
  agnostic_lite::{AsyncSpawner, Detach, RuntimeLite},
  bytes::{BufMut, Bytes, BytesMut},
  delegate::EventDelegate,
  tracing,
  transport::{MaybeResolvedAddress, Node},
  types::{DelegateVersion, Meta, NodeState, OneOrMore, ProtocolVersion, TinyVec},
  CheapClone,
};
use rand::{Rng, SeedableRng};
use smol_str::SmolStr;

use crate::{
  coalesce::{coalesced_event, MemberEventCoalescer, UserEventCoalescer},
  coordinate::{Coordinate, CoordinateOptions},
  delegate::TransformDelegate,
  error::Error,
  event::{InternalQueryEvent, MemberEvent, MemberEventType, QueryContext, QueryEvent},
  snapshot::{open_and_replay_snapshot, Snapshot},
  types::{
    JoinMessage, LeaveMessage, Member, MemberState, MemberStatus, MessageType, NodeIntent,
    QueryFlag, QueryMessage, QueryResponseMessage, SerfMessage, SerfMessageRef, Tags, UserEvent,
    UserEventMessage,
  },
  QueueOptions,
};

use self::internal_query::SerfQueries;

use super::*;

impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) async fn new_in(
    ev: Option<async_channel::Sender<Event<T, D>>>,
    delegate: Option<D>,
    transport: T::Options,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    if opts.max_user_event_size > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventLimitTooLarge(USER_EVENT_SIZE_LIMIT));
    }

    // Check that the meta data length is okay
    if let Some(tags) = opts.tags.load().as_ref() {
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
        let rs = open_and_replay_snapshot::<_, _, D, _>(sp, opts.rejoin_after_leave)?;
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
          opts.memberlist_options.metric_labels().clone(),
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
          TinyVec::new(),
          None,
        )
      };

    // Set up network coordinate client.
    let coord = (!opts.disable_coordinates).then_some({
      CoordinateClient::with_options(CoordinateOptions {
        #[cfg(feature = "metrics")]
        metric_labels: opts.memberlist_options.metric_labels().clone(),
        ..Default::default()
      })
    });
    let members = Arc::new(RwLock::new(Members::default()));
    let num_members = NumMembers::from(members.clone());
    // Setup the various broadcast queues, which we use to send our own
    // custom broadcasts along the gossip channel.
    let broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.memberlist_options.retransmit_mult(),
      num_members.clone(),
    ));
    let event_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.memberlist_options.retransmit_mult(),
      num_members.clone(),
    ));
    let query_broadcasts = Arc::new(TransmitLimitedQueue::<SerfBroadcast, _>::new(
      opts.memberlist_options.retransmit_mult(),
      num_members.clone(),
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
      SerfDelegate::new(delegate),
      transport,
      opts.memberlist_options.clone(),
    )
    .await?;

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
      #[cfg(feature = "encryption")]
      key_manager: crate::key_manager::KeyManager::new(),
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
    let memberlist_delegate = this.inner.memberlist.delegate().unwrap();
    memberlist_delegate.store(that);
    let local_node = this.inner.memberlist.local_state().await;
    memberlist_delegate.notify_join(local_node).await;

    // update key manager
    let that = this.clone();
    this.inner.key_manager.store(that);

    // Start the background tasks. See the documentation above each method
    // for more information on their role.
    Reaper {
      coord_core: this.inner.coord_core.clone(),
      memberlist: this.inner.memberlist.clone(),
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
      name: "ruserf.queue.intent",
      queue: this.inner.broadcasts.clone(),
      members: this.inner.members.clone(),
      opts: this.inner.opts.queue_opts(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn::<T::Runtime>();

    QueueChecker {
      name: "ruserf.queue.event",
      queue: this.inner.event_broadcasts.clone(),
      members: this.inner.members.clone(),
      opts: this.inner.opts.queue_opts(),
      shutdown_rx: shutdown_rx.clone(),
    }
    .spawn::<T::Runtime>();

    QueueChecker {
      name: "ruserf.queue.query",
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

  pub(crate) async fn has_alive_members(&self) -> bool {
    let members = self.inner.members.read().await;
    for member in members.states.values() {
      if member.member.node.id() == self.inner.memberlist.local_id() {
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
  pub(crate) async fn register_query_response(
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
    <T::Runtime as RuntimeLite>::spawn_after(timeout, async move {
      let mut resps = tresps.write().await;
      if let Some(resp) = resps.responses.remove(&ltime) {
        resp.close().await;
      }
    })
    .detach();
  }

  /// Takes a Serf message type, encodes it for the wire, and queues
  /// the broadcast. If a notify channel is given, this channel will be closed
  /// when the broadcast is sent.
  pub(crate) async fn broadcast(
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
  pub(crate) async fn broadcast_join(&self, ltime: LamportTime) -> Result<(), Error<T, D>> {
    // Construct message to update our lamport clock
    let msg = JoinMessage::new(ltime, self.inner.memberlist.advertise_node());
    self.inner.clock.witness(ltime);

    // Process update locally
    self.handle_node_join_intent(&msg).await;

    let msg = SerfMessage::Join(msg);
    // Start broadcasting the update
    if let Err(e) = self.broadcast(msg, None).await {
      tracing::warn!(err=%e, "ruserf: failed to broadcast join intent");
      return Err(e);
    }

    Ok(())
  }

  /// Serialize the current keyring and save it to a file.
  #[cfg(feature = "encryption")]
  pub(crate) async fn write_keyring_file(&self) -> std::io::Result<()> {
    use base64::{engine::general_purpose, Engine as _};

    let Some(path) = self.inner.opts.keyring_file() else {
      return Ok(());
    };

    if let Some(keyring) = self.inner.memberlist.keyring() {
      let encoded_keys = keyring
        .keys()
        .await
        .map(|k| general_purpose::STANDARD.encode(k))
        .collect::<TinyVec<_>>();

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
  pub(crate) async fn force_leave(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    prune: bool,
  ) -> Result<(), Error<T, D>> {
    // Construct the message to broadcast
    let msg = LeaveMessage {
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
    self.broadcast(msg, Some(ntx)).await?;

    // Wait for the broadcast
    <T::Runtime as RuntimeLite>::timeout(self.inner.opts.broadcast_timeout, nrx.recv())
      .await
      .map_err(|_| Error::RemovalBroadcastTimeout)?
      .map_err(|_| Error::BroadcastChannelClosed)
  }
}

struct Reaper<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  coord_core: Option<Arc<CoordCore<T::Id>>>,
  memberlist: Memberlist<T, SerfDelegate<T, D>>,
  members: Arc<RwLock<Members<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,
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
          members: TinyVec::from($m.member.clone()),
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
        member_timeout = r.reconnect_timeout(&m.member, member_timeout);
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
      let id = m.member.node.id();
      tracing::info!("ruserf: event member reap: {}", id);

      erase_node!($tx <- $coord($members[id].m));
    }
  }};
}

impl<T, D> Reaper<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn spawn(self) {
    <T::Runtime as RuntimeLite>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = <T::Runtime as RuntimeLite>::sleep(self.reap_interval).fuse() => {
            let mut ms = self.members.write().await;
            Self::reap_failed(&mut ms, self.event_tx.as_ref(), self.memberlist.delegate().and_then(|d| d.delegate()), self.coord_core.as_deref(), self.reconnect_timeout).await;
            Self::reap_left(&mut ms, self.event_tx.as_ref(), self.memberlist.delegate().and_then(|d| d.delegate()), self.coord_core.as_deref(), self.reconnect_timeout).await;
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
{
  fn spawn(self) {
    let mut rng = rand::rngs::StdRng::from_rng(rand::thread_rng()).unwrap();

    <T::Runtime as RuntimeLite>::spawn_detach(async move {
      loop {
        futures::select! {
          _ = <T::Runtime as RuntimeLite>::sleep(self.reconnect_interval).fuse() => {
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
              tracing::debug!("ruserf: forgoing reconnect for random throttling");
              continue;
            }

            // Select a random member to try and join
            let idx: usize = rng.gen_range(0..num_failed);
            let member = &mu.failed_members[idx];

            let (id, address) = member.member.node().cheap_clone().into_components();
            drop(mu); // release read lock
            tracing::info!("ruserf: attempting to reconnect to {}", id);
            // Attempt to join at the memberlist level
            if let Err(e) = self.memberlist.join(Node::new(id.cheap_clone(), MaybeResolvedAddress::resolved(address))).await {
              tracing::warn!("ruserf: failed to reconnect {}: {}", id, e);
            } else {
              tracing::info!("ruserf: successfully reconnected to {}", id);
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
  queue: Arc<TransmitLimitedQueue<SerfBroadcast, NumMembers<I, A>>>,
  members: Arc<RwLock<Members<I, A>>>,
  opts: QueueOptions,
  shutdown_rx: async_channel::Receiver<()>,
}

impl<I, A> QueueChecker<I, A>
where
  I: Send + Sync + 'static,
  A: Send + Sync + 'static,
{
  fn spawn<R: RuntimeLite>(self) {
    R::spawn_detach(async move {
      loop {
        futures::select! {
          _ = R::sleep(self.opts.check_interval).fuse() => {
            let numq = self.queue.num_queued().await;
            // TODO: metrics
            #[cfg(feature = "metrics")]
            {
              metrics::gauge!(self.name, self.opts.metric_labels.iter()).set(numq as f64);
            }
            if numq >= self.opts.depth_warning {
              tracing::warn!("ruserf: queue {} depth: {}", self.name, numq);
            }

            let max = self.get_queue_max().await;
            if numq >= max {
              tracing::warn!("ruserf: {} queue depth ({}) exceeds limit ({}), dropping messages!", self.name, numq, max);
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
{
  /// Called when a user event broadcast is
  /// received. Returns if the message should be rebroadcast.
  pub(crate) async fn handle_user_event(&self, msg: UserEventMessage) -> bool {
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
        "ruserf: received old event {} from time {} (current: {})",
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
        events: OneOrMore::from(user_event),
      });
    }

    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "ruserf.events",
        self.inner.opts.memberlist_options.metric_labels().iter()
      )
      .increment(1);

      // TODO: how to avoid allocating here?
      let named = format!("ruserf.events.{}", msg.name);
      metrics::counter!(
        named,
        self.inner.opts.memberlist_options.metric_labels().iter()
      )
      .increment(1);
    }

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
        tracing::error!("ruserf: failed to send user event: {}", e);
      }
    }
    true
  }

  /// Called when a query broadcast is
  /// received. Returns if the message should be rebroadcast.
  pub(crate) async fn handle_query(
    &self,
    q: QueryMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    ty: Option<InternalQueryEvent<T::Id>>,
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
        "ruserf: received old query {} from time {} (current: {})",
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
        query_ids: MediumVec::from(q.id),
      });
    }

    // update some metrics
    #[cfg(feature = "metrics")]
    {
      metrics::counter!(
        "ruserf.queries",
        self.inner.opts.memberlist_options.metric_labels().iter()
      )
      .increment(1);

      // TODO: how to avoid allocating here?
      let named = format!("ruserf.queries.{}", q.name);
      metrics::counter!(
        named,
        self.inner.opts.memberlist_options.metric_labels().iter()
      )
      .increment(1);
    }

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
            tracing::error!(err=%e, "ruserf: failed to send ack");
          }

          if let Err(e) = self
            .relay_response(q.relay_factor, q.from.clone(), ack)
            .await
          {
            tracing::error!(err=%e, "ruserf: failed to relay ack");
          }
        }
        Err(e) => {
          tracing::error!(err=%e, "ruserf: failed to format ack");
        }
      }
    }

    if let Some(ref tx) = self.inner.event_tx {
      let ev = QueryEvent {
        ltime: q.ltime,
        name: q.name,
        payload: q.payload,
        ctx: Arc::new(QueryContext {
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
        tracing::error!(err=%e, "ruserf: failed to send query");
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
          "ruserf: query reply ID mismatch (local: {}, response: {})",
          query.id,
          resp.id
        );
        return;
      }

      query
        .handle_query_response::<T, D>(
          resp,
          #[cfg(feature = "metrics")]
          self.inner.opts.memberlist_options.metric_labels(),
        )
        .await;
    } else {
      tracing::warn!(
        "ruserf: reply for non-running query (LTime: {}, ID: {}) From: {}",
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
      Ok((readed, tags)) => {
        tracing::trace!(read = %readed, tags=?tags, "ruserf: decode tags successfully");
        tags
      }
      Err(e) => {
        tracing::error!(err=%e, "ruserf: failed to decode tags");
        return;
      }
    };

    let (old_status, fut) = if let Some(member) = members.states.get_mut(node.id()) {
      let old_status = member.member.status.load(Ordering::Acquire);
      let dead_time = member.leave_time.elapsed();
      #[cfg(feature = "metrics")]
      if old_status == MemberStatus::Failed && dead_time < self.inner.opts.flap_timeout {
        metrics::counter!(
          "ruserf.member.flap",
          self.inner.opts.memberlist_options.metric_labels().iter()
        )
        .increment(1);
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
              members: TinyVec::from(member.member.clone()),
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
          // TODO:
          protocol_version: ProtocolVersion::V0,
          delegate_version: DelegateVersion::V0,
        }),
        status_time: status_ltime,
        leave_time:
          MemberState::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>::zero_leave_time(),
      };
      let member = ms.member.clone();
      members.states.insert(node.id().cheap_clone(), ms);
      (
        MemberStatus::None,
        self.inner.event_tx.as_ref().map(|tx| {
          tx.send(
            MemberEvent {
              ty: MemberEventType::Join,
              members: TinyVec::from(member),
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

    // update some metrics
    #[cfg(feature = "metrics")]
    metrics::counter!(
      "ruserf.member.join",
      self.inner.opts.memberlist_options.metric_labels().iter()
    )
    .increment(1);

    tracing::info!("ruserf: member join: {}", node);
    if let Some(fut) = fut {
      if let Err(e) = fut.await {
        tracing::error!(err=%e, "ruserf: failed to send member event");
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
        tracing::warn!("ruserf: bad state when leave: {}", ms);
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
    #[cfg(feature = "metrics")]
    metrics::counter!(
      "ruserf.member.leave",
      self.inner.opts.memberlist_options.metric_labels().iter()
    )
    .increment(1);

    tracing::info!("ruserf: {}: {}", ty.as_str(), member.node());

    if let Some(ref tx) = self.inner.event_tx {
      if let Err(e) = tx
        .send(
          MemberEvent {
            ty,
            members: TinyVec::from(member),
          }
          .into(),
        )
        .await
      {
        tracing::error!(err=%e, "ruserf: failed to send member event: {}", e);
      }
    }
  }

  pub(crate) async fn handle_node_leave_intent(
    &self,
    msg: &LeaveMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> bool {
    let state = self.state();

    // Witness a potentially newer time
    self.inner.clock.witness(msg.ltime);

    let mut members = self.inner.members.write().await;

    // TODO: There are plenty of duplicated code(to avoid borrow checker), I do not have a good idea how to refactor it currently...
    if msg.prune {
      if let Some(mut member) = members.states.remove(msg.node.id()) {
        // If the message is old, then it is irrelevant and we can skip it
        if msg.ltime <= member.status_time {
          return false;
        }

        // Refute us leaving if we are in the alive state
        // Must be done in another goroutine since we have the memberLock
        if msg.node.id().eq(self.inner.memberlist.local_id()) && state == SerfState::Alive {
          tracing::debug!("ruserf: refuting an older leave intent");
          let this = self.clone();
          let ltime = self.inner.clock.time();
          <T::Runtime as RuntimeLite>::spawn_detach(async move {
            if let Err(e) = this.broadcast_join(ltime).await {
              tracing::error!(err=%e, "ruserf: failed to broadcast join");
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
            let node = member.member.node();
            let id = node.id();
            tracing::info!("ruserf: EventMemberReap (forced): {}", node);

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
            tracing::info!("ruserf: EventMemberLeave: {}", member.member.node);

            let msg = self.inner.event_tx.as_ref().map(|tx| {
              tx.send(
                MemberEvent {
                  ty: MemberEventType::Leave,
                  members: TinyVec::from(member.member.clone()),
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
                tracing::error!(err=%e, "ruserf: failed to send member event");
              }
            }

            true
          }
          MemberStatus::Leaving | MemberStatus::Left => {
            if ms == MemberStatus::Leaving {
              <T::Runtime as RuntimeLite>::sleep(
                self.inner.opts.broadcast_timeout + self.inner.opts.leave_propagate_delay,
              )
              .await;
            }

            let node = member.member.node();
            let id = node.id();
            tracing::info!("ruserf: EventMemberReap (forced): {}", node);

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
        tracing::debug!("ruserf: refuting an older leave intent");
        let this = self.clone();
        let ltime = self.inner.clock.time();
        <T::Runtime as RuntimeLite>::spawn_detach(async move {
          if let Err(e) = this.broadcast_join(ltime).await {
            tracing::error!(err=%e, "ruserf: failed to broadcast join");
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
                members: TinyVec::from(member.member.clone()),
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
              tracing::error!(err=%e, "ruserf: failed to send member event");
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
      Ok((readed, tags)) => {
        tracing::trace!(read = %readed, tags=?tags, "ruserf: decode tags successfully");
        tags
      }
      Err(e) => {
        tracing::error!(err=%e, "ruserf: failed to decode tags");
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

      #[cfg(feature = "metrics")]
      metrics::counter!(
        "ruserf.member.update",
        self.inner.opts.memberlist_options.metric_labels().iter()
      )
      .increment(1);

      tracing::info!("ruserf: member update: {}", id);
      if let Some(ref tx) = self.inner.event_tx {
        if let Err(e) = tx
          .send(
            MemberEvent {
              ty: MemberEventType::Update,
              members: TinyVec::from(ms.member.clone()),
            }
            .into(),
          )
          .await
        {
          tracing::error!(err=%e, "ruserf: failed to send member event");
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
      <T::Runtime as RuntimeLite>::sleep(
        self.inner.opts.broadcast_timeout + self.inner.opts.leave_propagate_delay,
      )
      .await;
    }

    let node = member.member.node();
    let id = node.id();
    tracing::info!("ruserf: EventMemberReap (forced): {}", node);

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
    existing: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    other: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) {
    // Log a basic warning if the node is not us...
    if existing.id() != self.inner.memberlist.local_id() {
      tracing::warn!(
        "ruserf: node conflict detected between {} and {}",
        existing.id(),
        other.id()
      );
      return;
    }

    // The current node is conflicting! This is an error
    tracing::error!(
      "ruserf: node id conflicts with another node at {}. node id must be unique! (resolution enabled: {})",
      other.id(),
      self.inner.opts.enable_id_conflict_resolution
    );

    // If automatic resolution is enabled, kick off the resolution
    if self.inner.opts.enable_id_conflict_resolution {
      let this = self.clone();
      <T::Runtime as RuntimeLite>::spawn_detach(async move {
        // Get the local node
        let local_id = this.inner.memberlist.local_id();
        let encoded_id_len = <D as TransformDelegate>::id_encoded_len(local_id);
        let mut payload = vec![0u8; encoded_id_len];

        if let Err(e) = <D as TransformDelegate>::encode_id(local_id, &mut payload) {
          tracing::error!(err=%e, "ruserf: failed to encode local id");
          return;
        }

        // Start an id resolution query
        let ty = InternalQueryEvent::Conflict(local_id.clone());
        let resp = match this
          .internal_query(SmolStr::new(ty.as_str()), payload.into(), None, ty)
          .await
        {
          Ok(resp) => resp,
          Err(e) => {
            tracing::error!(err=%e, "ruserf: failed to start node id resolution query");
            return;
          }
        };

        // Counter to determine winner
        let mut responses = 0usize;
        let mut matching = 0usize;

        // Gather responses
        let resp_rx = resp.response_rx();
        futures::pin_mut!(resp_rx);
        while let Some(r) = resp_rx.next().await {
          // Decode the response
          if r.payload.is_empty() || r.payload[0] != MessageType::ConflictResponse as u8 {
            tracing::warn!(
              "ruserf: invalid conflict query response type: {:?}",
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
                    "ruserf: invalid conflict query response type: {}",
                    msg.as_str()
                  );
                  continue;
                }
              }
            }
            Err(e) => {
              tracing::error!(err=%e, "ruserf: failed to decode conflict query response");
              continue;
            }
          }
        }

        // Query over, determine if we should live
        let majority = (responses / 2) + 1;
        if matching >= majority {
          tracing::info!(
            "ruserf: majority in node id conflict resolution [{} / {}]",
            matching,
            responses
          );
          return;
        }

        // Since we lost the vote, we need to exit
        tracing::warn!(
          "ruserf: minority in name conflict resolution, quiting [{} / {}]",
          matching,
          responses
        );

        if let Err(e) = this.shutdown().await {
          tracing::error!(err=%e, "ruserf: failed to shutdown");
        }
      });
    }
  }

  pub(crate) fn handle_rejoin(
    memberlist: Memberlist<T, SerfDelegate<T, D>>,
    alive_nodes: TinyVec<Node<T::Id, MaybeResolvedAddress<T>>>,
  ) {
    <T::Runtime as RuntimeLite>::spawn_detach(async move {
      for prev in alive_nodes {
        // Do not attempt to join ourself
        if prev.id().eq(memberlist.local_id()) {
          continue;
        }

        tracing::info!(
          "ruserf: attempting re-join to previously known node {}",
          prev
        );
        if let Err(e) = memberlist.join(prev.cheap_clone()).await {
          tracing::warn!(
            "ruserf: failed to re-join to previously known node {}: {}",
            prev,
            e
          );
        } else {
          tracing::info!("ruserf: re-joined to previously known node: {}", prev);
          return;
        }
      }

      tracing::warn!("ruserf: failed to re-join to any previously known node");
    });
  }
}

/// Used to remove an old member from a list of old
/// members.
fn remove_old_member<I: Eq, A>(old: &mut OneOrMore<MemberState<I, A>>, id: &I) {
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
//   <<T::Runtime as RuntimeLite>::Sleep as Future>::Output: Send,
//   <<T::Runtime as RuntimeLite>::Interval as Stream>::Item: Send,
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
