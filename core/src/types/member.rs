use memberlist_core::types::OneOrMore;
use ruserf_types::Member;

use std::{collections::HashMap, time::Instant};

use super::{LamportTime, MessageType};

/// Used to track members that are no longer active due to
/// leaving, failing, partitioning, etc. It tracks the member along with
/// when that member was marked as leaving.
#[viewit::viewit]
#[derive(Clone)]
pub(crate) struct MemberState<I, A> {
  member: Member<I, A>,
  /// lamport clock time of last received message
  status_time: LamportTime,
  /// wall clock time of leave
  leave_time: Option<Instant>,
}

/// Used to buffer intents for out-of-order deliveries.
pub(crate) struct NodeIntent {
  pub(crate) ty: MessageType,
  pub(crate) wall_time: Instant,
  pub(crate) ltime: LamportTime,
}

pub(crate) struct Members<I, A> {
  pub(crate) states: HashMap<I, MemberState<I, A>>,
  pub(crate) recent_intents: HashMap<I, NodeIntent>,
  pub(crate) left_members: OneOrMore<MemberState<I, A>>,
  pub(crate) failed_members: OneOrMore<MemberState<I, A>>,
}

impl<I, A> Default for Members<I, A> {
  fn default() -> Self {
    Self {
      states: Default::default(),
      recent_intents: Default::default(),
      left_members: Default::default(),
      failed_members: Default::default(),
    }
  }
}

impl<I: Eq + core::hash::Hash, A: Eq + core::hash::Hash> Members<I, A> {
  pub(crate) fn recent_intent(&self, id: &I, ty: MessageType) -> Option<LamportTime> {
    match self.recent_intents.get(id) {
      Some(intent) if intent.ty == ty => Some(intent.ltime),
      _ => None,
    }
  }
}
