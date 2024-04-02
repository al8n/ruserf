use std::{
  borrow::Cow,
  collections::HashSet,
  fs::{File, OpenOptions},
  io::{BufRead, BufReader, BufWriter, Seek, Write},
  mem,
  path::PathBuf,
  time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::prelude::OpenOptionsExt;

use async_channel::{Receiver, Sender};
use either::Either;
use futures::FutureExt;
use memberlist_core::{
  agnostic_lite::RuntimeLite,
  bytes::{BufMut, BytesMut},
  tracing,
  transport::{AddressResolver, Id, MaybeResolvedAddress, Node, Transport},
  types::TinyVec,
  CheapClone,
};
use rand::seq::SliceRandom;
use ruserf_types::UserEventMessage;

use crate::{
  delegate::{Delegate, TransformDelegate},
  event::{Event, EventKind, MemberEvent, MemberEventType},
  invalid_data_io_error,
  types::{LamportClock, LamportTime},
};

#[cfg(any(test, feature = "test"))]
pub(crate) mod tests;

/// How often we force a flush of the snapshot file
const FLUSH_INTERVAL: Duration = Duration::from_millis(500);

/// How often we fetch the current lamport time of the cluster and write to the snapshot file
const CLOCK_UPDATE_INTERVAL: Duration = Duration::from_millis(500);

/// The extention we use for the temporary file during compaction
const TMP_EXT: &str = ".compact";

/// How often we attempt to recover from
/// errors writing to the snapshot file.
const SNAPSHOT_ERROR_RECOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// The size of the event buffers between Serf and the
/// consuming application. If this is exhausted we will block Serf and Memberlist.
const EVENT_CH_SIZE: usize = 2048;

/// The time limit to write pending events to the snapshot during a shutdown
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_millis(250);

/// An estimated bytes per node to snapshot
const SNAPSHOT_BYTES_PER_NODE: usize = 128;

/// The threshold we apply to
/// the snapshot size estimate (nodes * bytes per node) before compacting.
const SNAPSHOT_COMPACTION_THRESHOLD: usize = 2;

#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
  #[error("failed to open snapshot: {0}")]
  Open(std::io::Error),
  #[error("failed to open new snapshot: {0}")]
  OpenNew(std::io::Error),
  #[error("failed to flush new snapshot: {0}")]
  FlushNew(std::io::Error),
  #[error("failed to flush snapshot: {0}")]
  Flush(std::io::Error),
  #[error("failed to fsync snapshot: {0}")]
  Sync(std::io::Error),
  #[error("failed to stat snapshot: {0}")]
  Stat(std::io::Error),
  #[error("failed to remove old snapshot: {0}")]
  Remove(std::io::Error),
  #[error("failed to install new snapshot: {0}")]
  Install(std::io::Error),
  #[error("failed to write to new snapshot: {0}")]
  WriteNew(std::io::Error),
  #[error("failed to write to snapshot: {0}")]
  Write(std::io::Error),
  #[error("failed to seek to beginning of snapshot: {0}")]
  SeekStart(std::io::Error),
  #[error("failed to seek to end of snapshot: {0}")]
  SeekEnd(std::io::Error),
  #[error("failed to replay snapshot: {0}")]
  Replay(std::io::Error),
}

#[allow(dead_code)]
enum SnapshotRecord<'a, I: Clone, A: Clone> {
  Alive(Cow<'a, Node<I, A>>),
  NotAlive(Cow<'a, Node<I, A>>),
  Clock(LamportTime),
  EventClock(LamportTime),
  QueryClock(LamportTime),
  Coordinate,
  Leave,
  Comment,
}

const MAX_INLINED_BYTES: usize = 64;

impl<'a, I, A> SnapshotRecord<'a, I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  const ALIVE: u8 = 0;
  const NOT_ALIVE: u8 = 1;
  const CLOCK: u8 = 2;
  const EVENT_CLOCK: u8 = 3;
  const QUERY_CLOCK: u8 = 4;
  const COORDINATE: u8 = 5;
  const LEAVE: u8 = 6;
  const COMMENT: u8 = 7;

  fn encode<T: TransformDelegate<Id = I, Address = A>, W: Write>(
    &self,
    w: &mut W,
  ) -> std::io::Result<usize> {
    macro_rules! encode {
      ($node: ident::$status: ident) => {{
        let node = $node.as_ref();
        let encoded_node_len = T::node_encoded_len(node);
        let encoded_len = 2 + encoded_node_len;
        if encoded_len <= MAX_INLINED_BYTES {
          let mut buf = [0u8; MAX_INLINED_BYTES];
          buf[0] = Self::$status;
          T::encode_node(node, &mut buf[1..]).map_err(invalid_data_io_error)?;
          buf[1 + encoded_node_len] = b'\n';
          w.write_all(&buf[..encoded_len]).map(|_| encoded_len)
        } else {
          let mut buf = BytesMut::with_capacity(encoded_len);
          buf.put_u8(Self::$status);
          T::encode_node(node, &mut buf).map_err(invalid_data_io_error)?;
          buf.put_u8(b'\n');
          w.write_all(&buf).map(|_| encoded_len)
        }
      }};
      ($t: ident($status: ident)) => {{
        const N: usize = 2 * mem::size_of::<u8>() + mem::size_of::<u64>();
        let mut data = [0u8; N];
        data[0] = Self::$status;
        data[1..N - 1].copy_from_slice(&$t.to_be_bytes());
        data[N - 1] = b'\n';
        w.write_all(&data).map(|_| N)
      }};
      ($ident: ident) => {{
        w.write_all(&[Self::$ident, b'\n']).map(|_| 2)
      }};
    }

    match self {
      Self::Alive(id) => encode!(id::ALIVE),
      Self::NotAlive(id) => encode!(id::NOT_ALIVE),
      Self::Clock(t) => encode!(t(CLOCK)),
      Self::EventClock(t) => encode!(t(EVENT_CLOCK)),
      Self::QueryClock(t) => encode!(t(QUERY_CLOCK)),
      Self::Coordinate => encode!(COORDINATE),
      Self::Leave => encode!(LEAVE),
      Self::Comment => encode!(COMMENT),
    }
  }
}

#[viewit::viewit]
pub(crate) struct ReplayResult<I, A> {
  alive_nodes: HashSet<Node<I, A>>,
  last_clock: LamportTime,
  last_event_clock: LamportTime,
  last_query_clock: LamportTime,
  offset: u64,
  fh: File,
  path: PathBuf,
}

pub(crate) fn open_and_replay_snapshot<
  I: Id,
  A: CheapClone + core::hash::Hash + Eq + Send + Sync + 'static,
  T: TransformDelegate<Id = I, Address = A>,
  P: AsRef<std::path::Path>,
>(
  p: &P,
  rejoin_after_leave: bool,
) -> Result<ReplayResult<I, A>, SnapshotError> {
  // Try to open the file
  #[cfg(unix)]
  let fh = OpenOptions::new()
    .create(true)
    .append(true)
    .read(true)
    .mode(0o644)
    .open(p)
    .map_err(SnapshotError::Open)?;
  #[cfg(not(unix))]
  let fh = OpenOptions::new()
    .create(true)
    .append(true)
    .read(true)
    .write(true)
    .open(p)
    .map_err(SnapshotError::Open)?;

  // Determine the offset
  let offset = fh.metadata().map_err(SnapshotError::Stat)?.len();

  // Read each line
  let mut reader = BufReader::new(fh);
  let mut buf = Vec::new();
  let mut alive_nodes = HashSet::new();
  let mut last_clock = LamportTime::ZERO;
  let mut last_event_clock = LamportTime::ZERO;
  let mut last_query_clock = LamportTime::ZERO;

  loop {
    if reader.read_until(b'\n', &mut buf).is_err() {
      break;
    }

    // Skip the newline
    let src = &buf[..buf.len() - 1];
    // Match on the prefix
    match src[0] {
      SnapshotRecord::<I, A>::ALIVE => {
        let (_, node) =
          T::decode_node(&src[1..]).map_err(|e| SnapshotError::Replay(invalid_data_io_error(e)))?;
        alive_nodes.insert(node);
      }
      SnapshotRecord::<I, A>::NOT_ALIVE => {
        let (_, node) =
          T::decode_node(&src[1..]).map_err(|e| SnapshotError::Replay(invalid_data_io_error(e)))?;
        alive_nodes.remove(&node);
      }
      SnapshotRecord::<I, A>::CLOCK => {
        let t = u64::from_be_bytes([
          src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
        ]);
        last_clock = LamportTime::new(t);
      }
      SnapshotRecord::<I, A>::EVENT_CLOCK => {
        let t = u64::from_be_bytes([
          src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
        ]);
        last_event_clock = LamportTime::new(t);
      }
      SnapshotRecord::<I, A>::QUERY_CLOCK => {
        let t = u64::from_be_bytes([
          src[1], src[2], src[3], src[4], src[5], src[6], src[7], src[8],
        ]);
        last_query_clock = LamportTime::new(t);
      }
      SnapshotRecord::<I, A>::COORDINATE => {
        continue;
      }
      SnapshotRecord::<I, A>::LEAVE => {
        // Ignore a leave if we plan on re-joining
        if rejoin_after_leave {
          tracing::info!("ruserf: ignoring previous leave in snapshot");
          continue;
        }
        alive_nodes.clear();
        last_clock = LamportTime::ZERO;
        last_event_clock = LamportTime::ZERO;
        last_query_clock = LamportTime::ZERO;
      }
      SnapshotRecord::<I, A>::COMMENT => {
        continue;
      }
      v => {
        tracing::warn!("ruserf: unrecognized snapshot record {v}: {:?}", buf);
      }
    }
  }

  // Seek to the end
  let mut f = reader.into_inner();

  f.seek(std::io::SeekFrom::End(0))
    .map(|_| ReplayResult {
      alive_nodes,
      last_clock,
      last_event_clock,
      last_query_clock,
      offset,
      fh: f,
      path: p.as_ref().to_path_buf(),
    })
    .map_err(SnapshotError::SeekEnd)
}

pub(crate) struct SnapshotHandle {
  wait_rx: Receiver<()>,
  shutdown_rx: Receiver<()>,
  leave_tx: Sender<()>,
}

impl SnapshotHandle {
  /// Used to wait until the snapshotter finishes shut down
  pub(crate) async fn wait(&self) {
    let _ = self.wait_rx.recv().await;
  }

  /// Used to remove known nodes to prevent a restart from
  /// causing a join. Otherwise nodes will re-join after leaving!
  pub(crate) async fn leave(&self) {
    futures::select! {
      _ = self.leave_tx.send(()).fuse() => {},
      _ = self.shutdown_rx.recv().fuse() => {},
    }
  }
}

/// Responsible for ingesting events and persisting
/// them to disk, and providing a recovery mechanism at start time.
pub(crate) struct Snapshot<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  alive_nodes: HashSet<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  clock: LamportClock,
  fh: Option<BufWriter<File>>,
  last_flush: Instant,
  last_clock: LamportTime,
  last_event_clock: LamportTime,
  last_query_clock: LamportTime,
  leave_rx: Receiver<()>,
  leaving: bool,
  min_compact_size: u64,
  path: PathBuf,
  offset: u64,
  rejoin_after_leave: bool,
  stream_rx: Receiver<Event<T, D>>,
  shutdown_rx: Receiver<()>,
  wait_tx: Sender<()>,
  last_attempted_compaction: Instant,
  #[cfg(feature = "metrics")]
  metric_labels: std::sync::Arc<memberlist_core::types::MetricLabels>,
}

impl<D, T> Snapshot<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[allow(clippy::type_complexity)]
  pub(crate) fn from_replay_result(
    replay_result: ReplayResult<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    min_compact_size: u64,
    rejoin_after_leave: bool,
    clock: LamportClock,
    out_tx: Option<Sender<Event<T, D>>>,
    shutdown_rx: Receiver<()>,
    #[cfg(feature = "metrics")] metric_labels: std::sync::Arc<memberlist_core::types::MetricLabels>,
  ) -> Result<
    (
      Sender<Event<T, D>>,
      TinyVec<Node<T::Id, MaybeResolvedAddress<T>>>,
      SnapshotHandle,
    ),
    SnapshotError,
  > {
    let (in_tx, in_rx) = async_channel::bounded(EVENT_CH_SIZE);
    let (stream_tx, stream_rx) = async_channel::bounded(EVENT_CH_SIZE);
    let (leave_tx, leave_rx) = async_channel::bounded(1);
    let (wait_tx, wait_rx) = async_channel::bounded(1);

    let ReplayResult {
      alive_nodes,
      last_clock,
      last_event_clock,
      last_query_clock,
      offset,
      fh,
      path,
    } = replay_result;

    // Create the snapshotter
    let this = Self {
      alive_nodes,
      clock,
      fh: Some(BufWriter::new(fh)),
      last_flush: Instant::now(),
      last_clock,
      last_event_clock,
      last_query_clock,
      leave_rx,
      leaving: false,
      min_compact_size,
      path,
      offset,
      rejoin_after_leave,
      stream_rx,
      shutdown_rx: shutdown_rx.clone(),
      wait_tx,
      last_attempted_compaction: Instant::now(),
      #[cfg(feature = "metrics")]
      metric_labels,
    };

    let mut alive_nodes = this
      .alive_nodes
      .iter()
      .map(|n| {
        let id = n.id().cheap_clone();
        let addr = n.address().cheap_clone();
        Node::new(id, MaybeResolvedAddress::resolved(addr))
      })
      .collect::<TinyVec<_>>();
    alive_nodes.shuffle(&mut rand::thread_rng());

    // Start handling new commands
    <T::Runtime as RuntimeLite>::spawn_detach(Self::tee_stream(
      in_rx,
      stream_tx,
      out_tx,
      shutdown_rx.clone(),
    ));
    <T::Runtime as RuntimeLite>::spawn_detach(this.stream());

    Ok((
      in_tx,
      alive_nodes,
      SnapshotHandle {
        wait_rx,
        shutdown_rx,
        leave_tx,
      },
    ))
  }

  /// A long running routine that is used to copy events
  /// to the output channel and the internal event handler.
  async fn tee_stream(
    in_rx: Receiver<Event<T, D>>,
    stream_tx: Sender<Event<T, D>>,
    out_tx: Option<Sender<Event<T, D>>>,
    shutdown_rx: Receiver<()>,
  ) {
    macro_rules! flush_event {
      ($event:ident) => {{
        // Forward to the internal stream, do not block
        futures::select! {
          _ = stream_tx.send($event.clone()).fuse() => {}
          default => {}
        }

        // Forward the event immediately, do not block
        if let Some(ref out_tx) = out_tx {
          futures::select! {
            _ = out_tx.send($event).fuse() => {}
            default => {}
          }
        }
      }};
    }

    loop {
      futures::select! {
        ev = in_rx.recv().fuse() => {
          match ev {
            Ok(ev) => {
              let ev = ev.into_right();
              flush_event!(ev)
            },
            Err(e) => {
              tracing::error!(err=%e, "ruserf: tee stream: fail to receive from inbound channel");
              return;
            }
          }
        }
        _ = shutdown_rx.recv().fuse() => {
          break;
        }
      }
    }

    // Drain any remaining events before exiting
    loop {
      futures::select! {
        ev = in_rx.recv().fuse() => {
          match ev {
            Ok(ev) => {
              let ev = ev.into_right();
              flush_event!(ev)
            },
            Err(e) => {
              tracing::error!(err=%e, "ruserf: tee stream: fail to receive from inbound channel");
              return;
            }
          }
        }
        default => return,
      }
    }
  }

  /// Long running routine that is used to handle events
  async fn stream(mut self) {
    let mut clock_ticker = <T::Runtime as RuntimeLite>::interval(CLOCK_UPDATE_INTERVAL);

    // flushEvent is used to handle writing out an event
    macro_rules! flush_event {
      ($this:ident <- $event:ident) => {{
        // Stop recording events after a leave is issued
        if $this.leaving {
          return;
        }

        match $event.0 {
          Either::Left(e) => match &e {
            EventKind::Member(e) => $this.process_member_event(e),
            EventKind::User(e) => $this.process_user_event(e),
            EventKind::Query(e) => $this.process_query_event(e.ltime),
            EventKind::InternalQuery { query, .. } => $this.process_query_event(query.ltime),
          },
          Either::Right(e) => match &*e {
            EventKind::Member(e) => $this.process_member_event(e),
            EventKind::User(e) => $this.process_user_event(e),
            EventKind::Query(e) => $this.process_query_event(e.ltime),
            EventKind::InternalQuery { query, .. } => $this.process_query_event(query.ltime),
          },
        }
      }};
    }

    loop {
      futures::select! {
        _ = self.leave_rx.recv().fuse() => {
          self.leaving = true;

          // If we plan to re-join, keep our state
          if self.rejoin_after_leave {
            self.alive_nodes.clear();
          }
          self.try_append(SnapshotRecord::Leave);
          let fh = self.fh.as_mut().unwrap();
          if let Err(e) = fh.flush() {
            tracing::error!(target="ruserf", err=%SnapshotError::Flush(e), "failed to flush leave to snapshot");
          }

          if let Err(e) = fh.get_mut().sync_all() {
            tracing::error!(target="ruserf", err=%SnapshotError::Sync(e), "failed to sync leave to snapshot");
          }
        }
        ev = self.stream_rx.recv().fuse() => {
          match ev {
            Ok(ev) => flush_event!(self <- ev),
            Err(e) => {
              tracing::error!(target="ruserf", err=%e, "stream channel is closed");
              return;
            },
          }
        }
        _ = futures::StreamExt::next(&mut clock_ticker).fuse() => {
          self.update_clock();
        }
        _ = self.shutdown_rx.recv().fuse() => {
          // Setup a timeout
          let flush_timeout = <T::Runtime as RuntimeLite>::sleep(SHUTDOWN_FLUSH_TIMEOUT);
          futures::pin_mut!(flush_timeout);

          // snapshot the clock
          self.update_clock();

          // Clear out the buffers
          loop {
            futures::select! {
              ev = self.stream_rx.recv().fuse() => {
                match ev {
                  Ok(ev) => flush_event!(self <- ev),
                  Err(e) => {
                    tracing::error!(target="ruserf", err=%e, "stream channel is closed");
                    return;
                  },
                }
              }
              _ = (&mut flush_timeout).fuse() => {
                break;
              }
              default => {
                break;
              }
            }
          }

          let fh = self.fh.as_mut().unwrap();
          if let Err(e) = fh.flush() {
            tracing::error!(target="ruserf", err=%SnapshotError::Flush(e), "failed to flush leave to snapshot");
          }

          if let Err(e) = fh.get_mut().sync_all() {
            tracing::error!(target="ruserf", err=%SnapshotError::Sync(e), "failed to sync leave to snapshot");
          }
          self.wait_tx.close();
          return;
        }
      }
    }
  }

  /// Used to handle a single user event
  fn process_user_event(&mut self, e: &UserEventMessage) {
    // Ignore old clocks
    let ltime = e.ltime();
    if ltime <= self.last_event_clock {
      return;
    }

    self.last_event_clock = ltime;
    self.try_append(SnapshotRecord::EventClock(ltime));
  }

  /// Used to handle a single query event
  fn process_query_event(&mut self, ltime: LamportTime) {
    // Ignore old clocks
    if ltime <= self.last_event_clock {
      return;
    }

    self.last_query_clock = ltime;
    self.try_append(SnapshotRecord::QueryClock(ltime));
  }

  /// Used to handle a single member event
  fn process_member_event(
    &mut self,
    e: &MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
    match e.ty {
      MemberEventType::Join => {
        for m in e.members() {
          let node = m.node();
          self.alive_nodes.insert(node.cheap_clone());
          self.try_append(SnapshotRecord::Alive(Cow::Borrowed(node)))
        }
      }
      MemberEventType::Leave | MemberEventType::Failed => {
        for m in e.members() {
          let node = m.node();
          self.alive_nodes.remove(node);
          self.try_append(SnapshotRecord::NotAlive(Cow::Borrowed(node)));
        }
      }
      _ => {}
    }
    self.update_clock();
  }

  /// Called periodically to check if we should udpate our
  /// clock value. This is done after member events but should also be done
  /// periodically due to race conditions with join and leave intents
  fn update_clock(&mut self) {
    let last_seen = self.clock.time() - LamportTime::new(1);
    if last_seen > self.last_clock {
      self.last_clock = last_seen;
      self.try_append(SnapshotRecord::Clock(self.last_clock));
    }
  }

  fn try_append(
    &mut self,
    l: SnapshotRecord<'_, T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) {
    if let Err(e) = self.append_line(l) {
      tracing::error!(err = %e, "ruserf: failed to update snapshot");
      if self.last_attempted_compaction.elapsed() > SNAPSHOT_ERROR_RECOVERY_INTERVAL {
        self.last_attempted_compaction = Instant::now();
        tracing::info!("ruserf: attempting compaction to recover from error...");
        if let Err(e) = self.compact() {
          tracing::error!(err = %e, "ruserf: compaction failed, will reattempt after {}s", SNAPSHOT_ERROR_RECOVERY_INTERVAL.as_secs());
        } else {
          tracing::info!("ruserf: finished compaction, successfully recovered from error state");
        }
      }
    }
  }

  fn append_line(
    &mut self,
    l: SnapshotRecord<'_, T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), SnapshotError> {
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    #[cfg(feature = "metrics")]
    let metric_labels = self.metric_labels.clone();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("ruserf.snapshot.append_line", metric_labels.iter())
        .record(start.elapsed().as_millis() as f64)
    );

    let f = self.fh.as_mut().unwrap();
    let n = l.encode::<D, _>(f).map_err(SnapshotError::Write)?;

    // check if we should flush
    if self.last_flush.elapsed() > FLUSH_INTERVAL {
      self.last_flush = Instant::now();
      self
        .fh
        .as_mut()
        .unwrap()
        .flush()
        .map_err(SnapshotError::Flush)?;
    }

    // Check if a compaction is necessary
    self.offset += n as u64;
    if self.offset > self.snapshot_max_size() {
      self.compact()?;
    }
    Ok(())
  }

  /// Computes the maximum size and is used to force periodic compaction.
  fn snapshot_max_size(&self) -> u64 {
    let nodes = self.alive_nodes.len() as u64;
    let est_size = nodes * SNAPSHOT_BYTES_PER_NODE as u64;

    (est_size * SNAPSHOT_COMPACTION_THRESHOLD as u64).min(self.min_compact_size)
  }

  /// Used to compact the snapshot once it is too large
  fn compact(&mut self) -> Result<(), SnapshotError> {
    #[cfg(feature = "metrics")]
    let start = std::time::Instant::now();

    #[cfg(feature = "metrics")]
    let metric_labels = self.metric_labels.clone();
    #[cfg(feature = "metrics")]
    scopeguard::defer!(
      metrics::histogram!("ruserf.snapshot.compact", metric_labels.iter())
        .record(start.elapsed().as_millis() as f64)
    );

    // Try to open the file to new file
    let new_path = self.path.with_extension(TMP_EXT);
    #[cfg(unix)]
    let fh = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .mode(0o755)
      .open(&new_path)
      .map_err(SnapshotError::OpenNew)?;

    #[cfg(not(unix))]
    let fh = OpenOptions::new()
      .create(true)
      .write(true)
      .truncate(true)
      .open(&new_path)
      .map_err(SnapshotError::OpenNew)?;

    // Create a buffered writer
    let mut buf = BufWriter::new(fh);

    // Write out the live nodes
    let mut offset = 0u64;
    for node in self.alive_nodes.iter() {
      offset += SnapshotRecord::Alive(Cow::Borrowed(node))
        .encode::<D, _>(&mut buf)
        .map_err(SnapshotError::WriteNew)? as u64;
    }

    // Write out the clocks
    offset += SnapshotRecord::Clock(self.last_clock)
      .encode::<D, _>(&mut buf)
      .map_err(SnapshotError::WriteNew)? as u64;

    offset += SnapshotRecord::EventClock(self.last_event_clock)
      .encode::<D, _>(&mut buf)
      .map_err(SnapshotError::WriteNew)? as u64;

    offset += SnapshotRecord::QueryClock(self.last_query_clock)
      .encode::<D, _>(&mut buf)
      .map_err(SnapshotError::WriteNew)? as u64;

    // Flush the new snapshot
    buf.flush().map_err(SnapshotError::Flush)?;

    // Sync the new snapshot
    buf.get_ref().sync_all().map_err(SnapshotError::Sync)?;
    drop(buf);

    // We now need to swap the old snapshot file with the new snapshot.
    // Turns out, Windows won't let us rename the files if we have
    // open handles to them or if the destination already exists. This
    // means we are forced to close the existing handles, delete the
    // old file, move the new one in place, and then re-open the file
    // handles.

    // Flush the existing snapshot, ignoring errors since we will
    // delete it momentarily.
    let mut old = self.fh.take().unwrap();
    let _ = old.flush();
    drop(old);

    // Delete the old file
    std::fs::remove_file(&self.path).map_err(SnapshotError::Remove)?;

    // Move the new file into place
    std::fs::rename(&new_path, &self.path).map_err(SnapshotError::Install)?;

    // Open the new snapshot
    #[cfg(unix)]
    let fh = OpenOptions::new()
      .create(true)
      .append(true)
      .read(true)
      .mode(0o755)
      .open(&self.path)
      .map_err(SnapshotError::Open)?;

    #[cfg(not(unix))]
    let fh = OpenOptions::new()
      .create(true)
      .append(true)
      .read(true)
      .write(true)
      .open(&self.path)
      .map_err(SnapshotError::Open)?;

    self.fh = Some(BufWriter::new(fh));
    self.offset = offset;
    self.last_flush = Instant::now();
    Ok(())
  }
}
