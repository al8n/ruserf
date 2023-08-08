use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use agnostic::Runtime;
use async_lock::Mutex;
use either::Either;
use showbiz_core::{
  bytes::Bytes,
  futures_util::{Future, Stream},
  transport::Transport,
  Message, NodeId,
};
use smol_str::SmolStr;

use crate::{
  delegate::MergeDelegate,
  error::Error,
  types::{encode_message, MessageType, QueryResponseMessage},
  ReconnectTimeoutOverrider, Serf,
};

use super::{clock::LamportTime, serf::Member};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum EventType {
  Member(MemberEventType),
  User,
  Query,
}

pub struct Event<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  pub(crate) Either<EventKind<T, D, O>, Arc<EventKind<T, D, O>>>,
);

impl<D, T, O> Clone for Event<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
{
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> Event<T, D, O> {
  pub(crate) fn into_right(self) -> Self {
    match self.0 {
      Either::Left(e) => Self(Either::Right(Arc::new(e))),
      Either::Right(e) => Self(Either::Right(e)),
    }
  }

  pub(crate) fn kind(&self) -> &EventKind<T, D, O> {
    match &self.0 {
      Either::Left(e) => e,
      Either::Right(e) => &**e,
    }
  }

  pub(crate) fn is_internal_query(&self) -> bool {
    matches!(self.kind(), EventKind::InternalQuery { .. })
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<MemberEvent>
  for Event<T, D, O>
{
  fn from(value: MemberEvent) -> Self {
    Self(Either::Left(EventKind::Member(value)))
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<UserEvent>
  for Event<T, D, O>
{
  fn from(value: UserEvent) -> Self {
    Self(Either::Left(EventKind::User(value)))
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<QueryEvent<T, D, O>>
  for Event<T, D, O>
{
  fn from(value: QueryEvent<T, D, O>) -> Self {
    Self(Either::Left(EventKind::Query(value)))
  }
}

pub(crate) enum EventKind<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  Member(MemberEvent),
  User(UserEvent),
  Query(QueryEvent<T, D, O>),
  InternalQuery {
    ty: InternalQueryEventType,
    query: QueryEvent<T, D, O>,
  },
}

impl<D, T, O> Clone for EventKind<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
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

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> EventKind<T, D, O> {
  #[inline]
  pub const fn member(event: MemberEvent) -> Self {
    Self::Member(event)
  }

  #[inline]
  pub const fn user(event: UserEvent) -> Self {
    Self::User(event)
  }

  #[inline]
  pub const fn query(event: QueryEvent<T, D, O>) -> Self {
    Self::Query(event)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<MemberEvent>
  for EventKind<T, D, O>
{
  fn from(event: MemberEvent) -> Self {
    Self::Member(event)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<UserEvent>
  for EventKind<T, D, O>
{
  fn from(event: UserEvent) -> Self {
    Self::User(event)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> From<QueryEvent<T, D, O>>
  for EventKind<T, D, O>
{
  fn from(event: QueryEvent<T, D, O>) -> Self {
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

impl InternalQueryEventType {
  pub(crate) const fn as_str(&self) -> &'static str {
    match self {
      InternalQueryEventType::Ping => "ruserf-ping",
      InternalQueryEventType::Conflict => "ruserf-conflict",
      InternalQueryEventType::InstallKey => "ruserf-install-key",
      InternalQueryEventType::UseKey => "ruserf-use-key",
      InternalQueryEventType::RemoveKey => "ruserf-remove-key",
      InternalQueryEventType::ListKey => "ruserf-list-keys",
    }
  }
}

#[viewit::viewit]
pub(crate) struct QueryContext<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  query_timeout: Duration,
  span: Mutex<Option<Instant>>,
  this: Serf<T, D, O>,
}

/// Query is the struct used by EventQuery type events
#[viewit::viewit(vis_all = "pub(crate)", setters(prefix = "with"))]
pub struct QueryEvent<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,

  #[viewit(getter(skip), setter(skip))]
  ctx: Arc<QueryContext<T, D, O>>,
  id: u32,
  /// source node
  from: NodeId,
  /// Number of duplicate responses to relay back to sender
  relay_factor: u8,
}

impl<D, T, O> AsRef<QueryEvent<T, D, O>> for QueryEvent<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
{
  fn as_ref(&self) -> &QueryEvent<T, D, O> {
    self
  }
}

impl<D, T, O> Clone for QueryEvent<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
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

impl<D, T, O> core::fmt::Display for QueryEvent<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "query")
  }
}

impl<D, T, O> QueryEvent<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
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

  pub(crate) fn check_response_size(&self, resp: &[u8]) -> Result<(), Error<T, D, O>> {
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
  ) -> Result<(), Error<T, D, O>> {
    self.check_response_size(raw.as_slice())?;

    let mut mu = self.ctx.span.lock().await;

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

  /// Used to send a response to the user query
  pub async fn respond(&self, msg: Bytes) -> Result<(), Error<T, D, O>> {
    let resp = self.create_response(msg);

    // Encode response
    match encode_message(MessageType::QueryResponse, &resp) {
      Ok(raw) => self.respond_with_message_and_response(raw, resp).await,
      Err(e) => Err(Error::Encode(e)),
    }
  }
}
