use memberlist_core::{bytes::Bytes, types::OneOrMore};
use smol_str::SmolStr;

use super::LamportTime;

/// Used to buffer events to prevent re-delivery
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvents {
  pub(crate) ltime: LamportTime,
  pub(crate) events: OneOrMore<UserEvent>,
}

/// Stores all the user events at a specific time
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvent {
  pub(crate) name: SmolStr,
  pub(crate) payload: Bytes,
}

/// Used for user-generated events
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEventMessage {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,
  /// "Can Coalesce".
  cc: bool,
}
