#[cfg(feature = "async")]
mod r#async;
use std::{collections::HashMap, sync::Arc, time::Instant};

use atomic::Atomic;
#[cfg(feature = "async")]
pub use r#async::*;
use serde::{Deserialize, Serialize};
use showbiz_core::{bytes::Bytes, DelegateVersion, NodeId, ProtocolVersion};
use smol_str::SmolStr;

use crate::{clock::LamportTime, types::MessageType};

/// Maximum 128 KB snapshot
const SNAPSHOT_SIZE_LIMIT: usize = 128 * 1024;

/// Maximum 9KB for event name and payload
pub const USER_EVENT_SIZE_LIMIT: usize = 9 * 1024;

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

fn serialize_atomic_member_status<S>(
  status: &Atomic<MemberStatus>,
  serializer: S,
) -> Result<S::Ok, S::Error>
where
  S: serde::ser::Serializer,
{
  let status = status.load(atomic::Ordering::Relaxed);
  serializer.serialize_u8(status as u8)
}

/// A single member of the Serf cluster.
#[viewit::viewit(vis_all = "pub(crate)")]
#[derive(Debug, serde::Serialize)]
pub struct Member {
  id: NodeId,
  tags: Arc<HashMap<SmolStr, SmolStr>>,
  #[serde(serialize_with = "serialize_atomic_member_status")]
  status: Atomic<MemberStatus>,
  protocol_version: ProtocolVersion,
  delegate_version: DelegateVersion,
}

/// Used to track members that are no longer active due to
/// leaving, failing, partitioning, etc. It tracks the member along with
/// when that member was marked as leaving.
#[viewit::viewit]
#[derive(Clone)]
pub(crate) struct MemberState {
  member: Arc<Member>,
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
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum SerfState {
  Alive,
  Leaving,
  Left,
  Shutdown,
}
