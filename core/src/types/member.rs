use atomic::Atomic;
use memberlist_core::{transport::Node, types::{DelegateVersion, OneOrMore, ProtocolVersion}};

use std::{collections::HashMap, sync::Arc, time::Instant};

use super::{LamportTime, MessageType, Tags};

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
    let status = <u8 as serde::Deserialize>::deserialize(deserializer)?;
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
  pub(crate) fn zero_leave_time() -> Instant {
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

#[derive(Default)]
pub(crate) struct Members<I, A> {
  pub(crate) states: HashMap<I, MemberState<I, A>>,
  recent_intents: HashMap<I, NodeIntent>,
  pub(crate) left_members: OneOrMore<MemberState<I, A>>,
  failed_members: OneOrMore<MemberState<I, A>>,
}

impl<I: Eq + core::hash::Hash, A: Eq + core::hash::Hash> Members<I, A> {
  fn recent_intent(&self, id: &I, ty: MessageType) -> Option<LamportTime> {
    match self.recent_intents.get(id) {
      Some(intent) if intent.ty == ty => Some(intent.ltime),
      _ => None,
    }
  }
}