use std::{collections::HashMap, sync::{atomic::AtomicBool, Arc}};

use async_lock::{Mutex, RwLock};
use memberlist_core::{queue::TransmitLimitedQueue, transport::{AddressResolver, Transport}, types::MediumVec, Memberlist};

use super::{coordinate::{CoordinateClient, Coordinate}, delegate::{CompositeDelegate, Delegate}, types::{LamportClock, LamportTime, Members}, Options};

mod query;
pub use query::*;

/// Exports the default delegate type
pub type DefaultDelegate<T> = CompositeDelegate<
  <T as Transport>::Id,
  <<T as Transport>::Resolver as AddressResolver>::ResolvedAddress,
>;

pub(crate) struct CoordCore<I> {
  pub(crate) client: CoordinateClient<I>,
  pub(crate) cache: parking_lot::RwLock<HashMap<I, Coordinate>>,
}

/// Stores all the query ids at a specific time
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Queries {
  ltime: LamportTime,
  query_ids: MediumVec<u32>,
}

#[derive(Default)]
pub(crate) struct QueryCore<I, A> {
  responses: HashMap<LamportTime, QueryResponse<I, A>>,
  min_time: LamportTime,
  buffer: Vec<Option<Queries>>,
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

pub(crate) struct SerfCore<T: Transport, D = DefaultDelegate<T>>
where
  D: Delegate,
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
  event_tx: Option<async_channel::Sender<Event<T, D>>>,
  pub(crate) event_join_ignore: AtomicBool,

  pub(crate) event_core: RwLock<EventCore>,
  query_core: Arc<RwLock<QueryCore<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>,

  pub(crate) opts: Options,

  state: parking_lot::Mutex<SerfState>,

  join_lock: Mutex<()>,

  // snapshot: Option<SnapshotHandle>,
  // key_manager: KeyManager<T, D>,

  shutdown_tx: async_channel::Sender<()>,
  shutdown_rx: async_channel::Receiver<()>,

  pub(crate) coord_core: Option<Arc<CoordCore<T::Id>>>,
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