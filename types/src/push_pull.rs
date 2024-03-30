use std::collections::HashMap;

use indexmap::IndexSet;
use memberlist_types::TinyVec;

use super::{LamportTime, Node, UserEvents};

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
  feature = "serde",
  serde(bound(
    serialize = "I: core::cmp::Eq + core::hash::Hash + serde::Serialize, A: core::cmp::Eq + core::hash::Hash + serde::Serialize",
    deserialize = "I: core::cmp::Eq + core::hash::Hash + serde::Deserialize<'de>, A: core::cmp::Eq + core::hash::Hash + serde::Deserialize<'de>"
  ))
)]
pub struct PushPullMessage<I, A> {
  /// Current node lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time")),
    setter(const, attrs(doc = "Sets the lamport time (Builder pattern)"))
  )]
  ltime: LamportTime,
  /// Maps the node to its status time
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the maps the node to its status time")
    ),
    setter(attrs(doc = "Sets the maps the node to its status time (Builder pattern)"))
  )]
  status_ltimes: HashMap<Node<I, A>, LamportTime>,
  /// List of left nodes
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the list of left nodes")),
    setter(attrs(doc = "Sets the list of left nodes (Builder pattern)"))
  )]
  left_members: IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for event clock")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for event clock (Builder pattern)")
    )
  )]
  event_ltime: LamportTime,
  /// Recent events
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the recent events")),
    setter(attrs(doc = "Sets the recent events (Builder pattern)"))
  )]
  events: TinyVec<Option<UserEvents>>,
  /// Lamport time for query clock
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for query clock")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for query clock (Builder pattern)")
    )
  )]
  query_ltime: LamportTime,
}

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit(getters(skip), setters(skip))]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct PushPullMessageRef<'a, I, A> {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: &'a HashMap<Node<I, A>, LamportTime>,
  /// List of left nodes
  left_members: &'a IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: &'a [Option<UserEvents>],
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

impl<'a, I, A> Clone for PushPullMessageRef<'a, I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, I, A> Copy for PushPullMessageRef<'a, I, A> {}
