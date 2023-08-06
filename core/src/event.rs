use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use agnostic::Runtime;
use either::Either;
use parking_lot::Mutex;
use showbiz_core::{
  bytes::Bytes,
  futures_util::{Future, FutureExt, Stream},
  transport::Transport,
  Message, NodeId, Showbiz,
};
use smol_str::SmolStr;

use crate::{delegate::MergeDelegate, error::Error, types::QueryResponseMessage, Serf};

use super::{clock::LamportTime, serf::Member};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum EventType {
  Member(MemberEventType),
  User,
  Query,
}

pub struct Event<D: MergeDelegate, T: Transport>(
  pub(crate) Either<EventKind<D, T>, Arc<EventKind<D, T>>>,
);

impl<D, T> Clone for Event<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<D: MergeDelegate, T: Transport> Event<D, T> {
  pub(crate) fn into_right(self) -> Self {
    match self.0 {
      Either::Left(e) => Self(Either::Right(Arc::new(e))),
      Either::Right(e) => Self(Either::Right(e)),
    }
  }

  pub(crate) fn kind(&self) -> &EventKind<D, T> {
    match &self.0 {
      Either::Left(e) => e,
      Either::Right(e) => &**e,
    }
  }

  pub(crate) fn is_internal_query(&self) -> bool {
    matches!(self.kind(), EventKind::InternalQuery { .. })
  }
}

impl<D: MergeDelegate, T: Transport> From<MemberEvent> for Event<D, T> {
  fn from(value: MemberEvent) -> Self {
    Self(Either::Left(EventKind::Member(value)))
  }
}

impl<D: MergeDelegate, T: Transport> From<UserEvent> for Event<D, T> {
  fn from(value: UserEvent) -> Self {
    Self(Either::Left(EventKind::User(value)))
  }
}

impl<D: MergeDelegate, T: Transport> From<QueryEvent<D, T>> for Event<D, T> {
  fn from(value: QueryEvent<D, T>) -> Self {
    Self(Either::Left(EventKind::Query(value)))
  }
}

pub(crate) enum EventKind<D: MergeDelegate, T: Transport> {
  Member(MemberEvent),
  User(UserEvent),
  Query(QueryEvent<D, T>),
  InternalQuery {
    ty: InternalQueryEventType,
    query: QueryEvent<D, T>,
  },
}

impl<D, T> Clone for EventKind<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  fn clone(&self) -> Self {
    match self {
      Self::Member(e) => Self::Member(e.clone()),
      Self::User(e) => Self::User(e.clone()),
      Self::Query(e) => Self::Query(e.clone()),
      Self::InternalQuery { ty, query } => Self::InternalQuery {
        ty: *ty,
        query: query.clone(),
      },
    }
  }
}

impl<D: MergeDelegate, T: Transport> EventKind<D, T> {
  #[inline]
  pub const fn member(event: MemberEvent) -> Self {
    Self::Member(event)
  }

  #[inline]
  pub const fn user(event: UserEvent) -> Self {
    Self::User(event)
  }

  #[inline]
  pub const fn query(event: QueryEvent<D, T>) -> Self {
    Self::Query(event)
  }

  // #[inline]
  // pub fn event_type(&self) -> EventType {
  //   match self {
  //     Self::Member(event) => EventType::Member(event.ty),
  //     Self::User(_) => EventType::User,
  //     Self::Query(_) => EventType::Query,
  //     Self::InternalQuery { .. } => unreachable!(),
  //   }
  // }
}

impl<D: MergeDelegate, T: Transport> From<MemberEvent> for EventKind<D, T> {
  fn from(event: MemberEvent) -> Self {
    Self::Member(event)
  }
}

impl<D: MergeDelegate, T: Transport> From<UserEvent> for EventKind<D, T> {
  fn from(event: UserEvent) -> Self {
    Self::User(event)
  }
}

impl<D: MergeDelegate, T: Transport> From<QueryEvent<D, T>> for EventKind<D, T> {
  fn from(event: QueryEvent<D, T>) -> Self {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub(crate) enum InternalQueryEventType {
  Ping,
  Conflict,
  InstallKey,
  UseKey,
  RemoveKey,
  ListKey,
}

pub(crate) struct QueryContext<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  query_timeout: Duration,
  span: Mutex<Option<Instant>>,
  this: Serf<D, T>,
}

/// Query is the struct used by EventQuery type events
#[viewit::viewit(vis_all = "pub(crate)", setters(prefix = "with"))]
pub struct QueryEvent<D: MergeDelegate, T: Transport> {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,

  #[viewit(getter(skip), setter(skip))]
  ctx: Arc<QueryContext<D, T>>,
  id: u32,
  /// source node
  from: NodeId,
  /// Number of duplicate responses to relay back to sender
  relay_factor: u8,
}

impl<D, T> Clone for QueryEvent<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  fn clone(&self) -> Self {
    Self {
      ltime: self.ltime,
      name: self.name.clone(),
      payload: self.payload.clone(),
      ctx: self.ctx.clone(),
      id: self.id,
      from: self.from.clone(),
      relay_factor: self.relay_factor,
    }
  }
}

impl<D, T> core::fmt::Display for QueryEvent<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "query")
  }
}

impl<D, T> QueryEvent<D, T>
where
  D: MergeDelegate,
  T: Transport,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
{
  pub(crate) fn create_response(&self, buf: Bytes) -> QueryResponseMessage {
    QueryResponseMessage {
      ltime: self.ltime,
      id: self.id,
      from: self.ctx.this.inner.memberlist.local_id().clone(),
      flags: 0,
      payload: buf,
    }
  }

  fn check_response_size(&self, resp: &[u8]) -> Result<(), Error<D, T>> {
    let resp_len = resp.len();
    if resp_len > self.ctx.this.inner.opts.query_response_size_limit {
      Err(Error::QueryResponseTooLarge {
        limit: self.ctx.this.inner.opts.query_response_size_limit,
        got: resp_len,
      })
    } else {
      Ok(())
    }
  }

  pub(crate) async fn respond_with_message_and_response(
    &self,
    raw: Message,
    resp: QueryResponseMessage,
  ) -> Result<(), Error<D, T>> {
    self.check_response_size(raw.as_slice())?;

    let mut mu = self.ctx.span.lock();

    if let Some(span) = *mu {
      // Ensure we aren't past our response deadline
      if span.elapsed() > self.ctx.query_timeout {
        return Err(Error::QueryTimeout);
      }

      // Send the response directly to the originator
      self.ctx.this.inner.memberlist.send(&self.from, raw).await?;

      // Relay the response through up to relayFactor other nodes
      self
        .ctx
        .this
        .relay_response(self.relay_factor, self.from.clone(), resp)
        .await?;

      // Clear the deadline, responses sent
      *mu = None;
      Ok(())
    } else {
      Err(Error::QueryAlreadyResponsed)
    }
  }
}
