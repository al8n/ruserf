#[cfg(feature = "async")]
mod r#async;
use std::{collections::HashMap, time::Instant};

#[cfg(feature = "async")]
pub use r#async::*;
use serde::{Deserialize, Serialize};
use showbiz_core::{bytes::Bytes, DelegateVersion, NodeId, ProtocolVersion};

use crate::{clock::LamportTime, types::MessageType};

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

/// A single member of the Serf cluster.
pub struct Member {
  id: NodeId,
  tags: HashMap<String, String>,
  status: MemberStatus,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

/// Used to track members that are no longer active due to
/// leaving, failing, partitioning, etc. It tracks the member along with
/// when that member was marked as leaving.
pub(crate) struct MemberState {
  member: Member,
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
pub enum SerfState {
  Alive,
  Leaving,
  Left,
  Shutdown,
}
