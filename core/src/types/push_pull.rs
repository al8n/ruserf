use std::collections::HashMap;

use indexmap::IndexSet;
use memberlist_core::{transport::Node, types::TinyVec};

use super::{LamportTime, UserEvents};

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(
  bound(
    serialize = "I: core::cmp::Eq + core::hash::Hash + serde::Serialize, A: core::cmp::Eq + core::hash::Hash + serde::Serialize",
    deserialize = "I: core::cmp::Eq + core::hash::Hash + serde::Deserialize<'de>, A: core::cmp::Eq + core::hash::Hash + serde::Deserialize<'de>"
  )
))]
pub struct PushPull<I, A> {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: HashMap<I, LamportTime>,
  /// List of left nodes
  left_members: IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: TinyVec<Option<UserEvents>>,
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PushPullRef<'a, I, A> {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: &'a HashMap<I, LamportTime>,
  /// List of left nodes
  left_members: &'a IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: &'a [Option<UserEvents>],
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

impl<'a, I, A> Clone for PushPullRef<'a, I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, I, A> Copy for PushPullRef<'a, I, A> {}
