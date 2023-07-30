use std::sync::Arc;

use either::Either;
use showbiz_core::bytes::Bytes;
use smol_str::SmolStr;

use super::{clock::LamportTime, serf::Member};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum EventType {
  Member(MemberEventType),
  User,
  Query,
}

#[derive(Clone)]
pub struct Event(pub(crate) Either<EventKind, Arc<EventKind>>);

impl Event {
  pub(crate) fn into_right(self) -> Self {
    match self.0 {
      Either::Left(e) => Self(Either::Right(Arc::new(e))),
      Either::Right(e) => Self(Either::Right(e)),
    }
  }
}

impl From<MemberEvent> for Event {
  fn from(value: MemberEvent) -> Self {
    Self(Either::Left(EventKind::Member(value)))
  }
}

impl From<UserEvent> for Event {
  fn from(value: UserEvent) -> Self {
    Self(Either::Left(EventKind::User(value)))
  }
}

impl From<QueryEvent> for Event {
  fn from(value: QueryEvent) -> Self {
    Self(Either::Left(EventKind::Query(value)))
  }
}

#[derive(Clone)]
pub(crate) enum EventKind {
  Member(MemberEvent),
  User(UserEvent),
  Query(QueryEvent),
}

impl EventKind {
  #[inline]
  pub const fn member(event: MemberEvent) -> Self {
    Self::Member(event)
  }

  #[inline]
  pub const fn user(event: UserEvent) -> Self {
    Self::User(event)
  }

  #[inline]
  pub const fn query(event: QueryEvent) -> Self {
    Self::Query(event)
  }

  #[inline]
  pub fn event_type(&self) -> EventType {
    match self {
      Self::Member(event) => EventType::Member(event.ty),
      Self::User(_) => EventType::User,
      Self::Query(_) => EventType::Query,
    }
  }
}

impl From<MemberEvent> for EventKind {
  fn from(event: MemberEvent) -> Self {
    Self::Member(event)
  }
}

impl From<UserEvent> for EventKind {
  fn from(event: UserEvent) -> Self {
    Self::User(event)
  }
}

impl From<QueryEvent> for EventKind {
  fn from(event: QueryEvent) -> Self {
    Self::Query(event)
  }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum MemberEventType {
  #[cfg_attr(feature = "serde", serde(rename = "member-join"))]
  Join,
  #[cfg_attr(feature = "serde", serde(rename = "member-leave"))]
  Leave,
  #[cfg_attr(feature = "serde", serde(rename = "member-failed"))]
  Failed,
  #[cfg_attr(feature = "serde", serde(rename = "member-update"))]
  Update,
  #[cfg_attr(feature = "serde", serde(rename = "member-reap"))]
  Reap,
}

impl core::fmt::Display for MemberEventType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Join => write!(f, "member-join"),
      Self::Leave => write!(f, "member-leave"),
      Self::Failed => write!(f, "member-failed"),
      Self::Update => write!(f, "member-update"),
      Self::Reap => write!(f, "member-reap"),
    }
  }
}

/// MemberEvent is the struct used for member related events
/// Because Serf coalesces events, an event may contain multiple members.
#[derive(Debug, Clone)]
pub struct MemberEvent {
  pub(crate) ty: MemberEventType,
  pub(crate) members: Vec<Arc<Member>>,
}

impl core::fmt::Display for MemberEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.ty)
  }
}

impl MemberEvent {
  pub fn new(ty: MemberEventType, members: Vec<Arc<Member>>) -> Self {
    Self { ty, members }
  }

  pub fn ty(&self) -> MemberEventType {
    self.ty
  }

  pub fn members(&self) -> &[Arc<Member>] {
    &self.members
  }
}

impl From<MemberEvent> for (MemberEventType, Vec<Arc<Member>>) {
  fn from(event: MemberEvent) -> Self {
    (event.ty, event.members)
  }
}

/// UserEvent is the struct used for events that are triggered
/// by the user and are not related to members

#[viewit::viewit(vis_all = "pub(crate)", setters(prefix = "with"))]
#[derive(Clone)]
pub struct UserEvent {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,
  pub coalesce: bool,
}

impl core::fmt::Display for UserEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "user")
  }
}

/// Query is the struct used by EventQuery type events
#[viewit::viewit(vis_all = "pub(crate)", setters(prefix = "with"))]
#[derive(Debug, Clone)]
pub struct QueryEvent {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,
}

impl core::fmt::Display for QueryEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "query")
  }
}
