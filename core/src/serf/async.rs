use std::sync::{
  atomic::{AtomicU32, Ordering},
  Arc,
};

use super::*;

use agnostic::Runtime;
use async_lock::RwLock;
use showbiz_core::{
  queue::{NodeCalculator, TransmitLimitedQueue},
  transport::Transport,
  Showbiz,
};

use crate::{
  broadcast::SerfBroadcast,
  clock::LamportClock,
  delegate::{MergeDelegate, SerfDelegate},
  Options,
};

struct SerfNodeCalculator {
  num_nodes: Arc<AtomicU32>,
}

impl NodeCalculator for SerfNodeCalculator {
  fn num_nodes(&self) -> usize {
    self.num_nodes.load(Ordering::SeqCst) as usize
  }
}

pub(crate) struct SerfCore<D: MergeDelegate, T: Transport, R: Runtime> {
  clock: LamportClock,
  event_clock: LamportClock,
  query_clock: LamportClock,

  broadcasts: TransmitLimitedQueue<SerfBroadcast, SerfNodeCalculator>,
  // opts: Options,
  memberlist: Showbiz<SerfDelegate<D, T, R>, T, R>,

  merge_delegate: Option<D>,

  opts: Options<T>,
}

/// Serf is a single node that is part of a single cluster that gets
/// events about joins/leaves/failures/etc. It is created with the Create
/// method.
///
/// All functions on the Serf structure are safe to call concurrently.
#[repr(transparent)]
pub struct Serf<D: MergeDelegate, T: Transport, R: Runtime> {
  inner: Arc<SerfCore<D, T, R>>,
}

impl<D: MergeDelegate, T: Transport, S: Runtime> Clone for Serf<D, T, S> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}
