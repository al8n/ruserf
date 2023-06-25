use std::sync::Arc;

use showbiz_core::bytes::Bytes;

use super::{
  clock::LamportTime,
  serf::Member,
};

pub trait Event: core::fmt::Display + Send + Sync + 'static {
  type Type: Copy + Send + Sync + 'static;

  fn event_type(&self) -> EventType<Self::Type>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VoidEvent;

impl core::fmt::Display for VoidEvent {
  fn fmt(&self, _f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    Ok(())
  }
}

impl Event for VoidEvent {
  type Type = ();

  fn event_type(&self) -> EventType<Self::Type> {
    EventType::Custom(())
  }
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum EventType<C>
where
  C: Copy + Send + Sync + 'static,
{
  Member(MemberEventType),
  User,
  Query,
  Custom(C),
}

pub enum Events<E: Event> {
  Member(MemberEvent),
  User(UserEvent),
  Query(QueryEvent),
  Custom(E),
}

impl<E: Event> Events<E> {
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
  pub const fn custom(event: E) -> Self {
    Self::Custom(event)
  }

  #[inline]
  pub fn event_type(&self) -> EventType<E::Type> {
    match self {
      Self::Member(event) => EventType::Member(event.ty),
      Self::User(event) => EventType::User,
      Self::Query(event) => EventType::Query,
      Self::Custom(event) => event.event_type(),
    }
  }
}

impl<E: Event> From<MemberEvent> for Events<E> {
  fn from(event: MemberEvent) -> Self {
    Event::Member(event)
  }
}

impl<E: Event> From<UserEvent> for Events<E> {
  fn from(event: UserEvent) -> Self {
    Event::User(event)
  }
}

impl<E: Event> From<QueryEvent> for Events<E> {
  fn from(event: QueryEvent) -> Self {
    Event::Query(event)
  }
}

#[derive(Debug, Clone, Copy)]
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
pub struct MemberEvent {
  ty: MemberEventType,
  member: Vec<Arc<Member>>,
}

impl core::fmt::Display for MemberEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.ty)
  }
}

impl MemberEvent {
  pub fn new(ty: MemberEventType, member: Vec<Arc<Member>>) -> Self {
    Self { ty, member }
  }

  pub fn ty(&self) -> MemberEventType {
    self.ty
  }

  pub fn members(&self) -> &[Arc<Member>] {
    &self.member
  }
}

impl Event for MemberEvent {
  type Type = MemberEventType;

  fn event_type(&self) -> EventType<Self::Type> {
    EventType::Member(self.ty)
  }
}

/// UserEvent is the struct used for events that are triggered
/// by the user and are not related to members

pub struct UserEvent {
  ltime: LamportTime,
  name: String,
  payload: Bytes,
  pub coalesce: bool,
}

impl core::fmt::Display for UserEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "user")
  }
}

impl Event for UserEvent {
  type Type = ();

  fn event_type(&self) -> EventType<Self::Type> {
    EventType::User
  }
}

/// Query is the struct used by EventQuery type events
pub struct QueryEvent {
  ltime: LamportTime,
  name: String,
  payload: Bytes,
}

impl core::fmt::Display for QueryEvent {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "query")
  }
}

impl Event for QueryEvent {
  type Type = ();

  fn event_type(&self) -> EventType<Self::Type> {
    EventType::Query
  }
}