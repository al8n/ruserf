use memberlist_types::{bytes::Bytes, OneOrMore};
use smol_str::SmolStr;

use super::LamportTime;

/// Used to buffer events to prevent re-delivery
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvents {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,

  /// The user events
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the user events")),
    setter(
      attrs(doc = "Sets the user events (Builder pattern)")
    )
  )]
  events: OneOrMore<UserEvent>,
}

/// Stores all the user events at a specific time
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvent {
  /// The name of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the name of the event")),
    setter(
      attrs(doc = "Sets the name of the event (Builder pattern)")
    )
  )]
  name: SmolStr,
  /// The payload of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the payload of the event")),
    setter(
      attrs(doc = "Sets the payload of the event (Builder pattern)")
    )
  )]
  payload: Bytes,
}

/// Used for user-generated events
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEventMessage {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// The name of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the name of the event")),
    setter(
      attrs(doc = "Sets the name of the event (Builder pattern)")
    )
  )]
  name: SmolStr,
  /// The payload of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the payload of the event")),
    setter(
      attrs(doc = "Sets the payload of the event (Builder pattern)")
    )
  )]
  payload: Bytes,
  /// "Can Coalesce".
  #[viewit(
    getter(const, attrs(doc = "Returns if this message can be coalesced")),
    setter(
      const,
      attrs(doc = "Sets if this message can be coalesced (Builder pattern)")
    )
  )]
  cc: bool,
}
