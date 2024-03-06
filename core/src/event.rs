use std::{
  sync::Arc,
  time::{Duration, Instant},
};

use async_lock::Mutex;
use either::Either;
use futures::{Future, Stream};
use memberlist_core::{
  agnostic::Runtime,
  bytes::{BufMut, Bytes, BytesMut},
  transport::{AddressResolver, Node, Transport},
  types::TinyVec,
};
use smol_str::SmolStr;

use super::{
  clock::LamportTime,
  delegate::{Delegate, MergeDelegate, TransformDelegate},
  error::Error,
  serf::Member,
  types::{QueryResponseMessage, SerfMessageRef},
  Serf,
};

#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum EventType {
  Member(MemberEventType),
  User,
  Query,
}

pub struct Event<T, D>(pub(crate) Either<EventKind<T, D>, Arc<EventKind<T, D>>>)
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport;

impl<D, T> Clone for Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn clone(&self) -> Self {
    Self(self.0.clone())
  }
}

impl<D, T> Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) fn into_right(self) -> Self {
    match self.0 {
      Either::Left(e) => Self(Either::Right(Arc::new(e))),
      Either::Right(e) => Self(Either::Right(e)),
    }
  }

  pub(crate) fn kind(&self) -> &EventKind<T, D> {
    match &self.0 {
      Either::Left(e) => e,
      Either::Right(e) => &**e,
    }
  }

  pub(crate) fn is_internal_query(&self) -> bool {
    matches!(self.kind(), EventKind::InternalQuery { .. })
  }
}

impl<D, T> From<MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
  for Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>) -> Self {
    Self(Either::Left(EventKind::Member(value)))
  }
}

impl<D, T> From<UserEvent> for Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: UserEvent) -> Self {
    Self(Either::Left(EventKind::User(value)))
  }
}

impl<D, T> From<QueryEvent<T, D>> for Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: QueryEvent<T, D>) -> Self {
    Self(Either::Left(EventKind::Query(value)))
  }
}

// impl<D, T> From<(InternalQueryEventType, QueryEvent<T, D>)> for Event<T, D>
// where
//   D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
//   T: Transport,
//   <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
//   <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
// {
//   fn from(value: (InternalQueryEventType, QueryEvent<T, D>)) -> Self {
//     Self(Either::Left(EventKind::InternalQuery {
//       ty: value.0,
//       query: value.1,
//     }))
//   }
// }

pub(crate) enum EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  Member(Arc<MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>),
  User(UserEvent),
  Query(QueryEvent<T, D>),
  InternalQuery(InternalQueryEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
}

impl<D, T> Clone for EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn clone(&self) -> Self {
    match self {
      Self::Member(e) => Self::Member(e.clone()),
      Self::User(e) => Self::User(e.clone()),
      Self::Query(e) => Self::Query(e.clone()),
      Self::InternalQuery(e) => Self::InternalQuery(e.clone()),
    }
  }
}

impl<D, T> EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[inline]
  pub const fn member(
    event: MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Self {
    Self::Member(event)
  }

  #[inline]
  pub const fn user(event: UserEvent) -> Self {
    Self::User(event)
  }

  #[inline]
  pub const fn query(event: QueryEvent<T, D>) -> Self {
    Self::Query(event)
  }
}

impl<D, T> From<MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>
  for EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(event: MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>) -> Self {
    Self::Member(event)
  }
}

impl<D, T> From<UserEvent> for EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(event: UserEvent) -> Self {
    Self::User(event)
  }
}

impl<D, T> From<QueryEvent<T, D>> for EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(event: QueryEvent<T, D>) -> Self {
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

impl MemberEventType {
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Join => "member-join",
      Self::Leave => "member-leave",
      Self::Failed => "member-failed",
      Self::Update => "member-update",
      Self::Reap => "member-reap",
    }
  }
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
pub struct MemberEvent<I, A> {
  pub(crate) ty: MemberEventType,
  // TODO: use an enum to avoid allocate when there is only one member
  pub(crate) members: Vec<Arc<Member<I, A>>>,
}

impl<I, A> core::fmt::Display for MemberEvent<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.ty)
  }
}

impl<I, A> MemberEvent<I, A> {
  pub fn new(ty: MemberEventType, members: Vec<Arc<Member<I, A>>>) -> Self {
    Self { ty, members }
  }

  pub fn ty(&self) -> MemberEventType {
    self.ty
  }

  pub fn members(&self) -> &[Arc<Member<I, A>>] {
    &self.members
  }
}

// impl<I, A> From<MemberEvent<I, A>> for (MemberEventType, TinyVec<Arc<Member<I, A>>>) {
//   fn from(event: MemberEvent<I, A>) -> Self {
//     (event.ty, event.members)
//   }
// }

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

pub struct ConflictQueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) ltime: LamportTime,
  pub(crate) name: SmolStr,
  pub(crate) conflict: T::Id,

  pub(crate) ctx: Arc<QueryContext<T, D>>,
  pub(crate) id: u32,
  /// source node
  pub(crate) from: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  /// Number of duplicate responses to relay back to sender
  pub(crate) relay_factor: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case", untagged))]
pub enum InternalQueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  Ping,
  // Conflict(ConflictQueryEvent<T, D>),
  Conflict,
  #[cfg(feature = "encryption")]
  InstallKey,
  #[cfg(feature = "encryption")]
  UseKey,
  #[cfg(feature = "encryption")]
  RemoveKey,
  #[cfg(feature = "encryption")]
  ListKey,
}

impl<I, A> InternalQueryEvent<I, A> {
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Ping => "ruserf-ping",
      Self::Conflict => "ruserf-conflict",
      Self::InstallKey => "ruserf-install-key",
      Self::UseKey => "ruserf-use-key",
      Self::RemoveKey => "ruserf-remove-key",
      Self::ListKey => "ruserf-list-keys",
    }
  }
}

pub(crate) struct QueryContext<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) query_timeout: Duration,
  pub(crate) span: Mutex<Option<Instant>>,
  pub(crate) this: Serf<T, D>,
}

/// Query is the struct used by EventQuery type events
pub struct QueryEvent<T: Transport, D: Delegate>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) ltime: LamportTime,
  pub(crate) name: SmolStr,
  pub(crate) payload: Bytes,

  pub(crate) ctx: Arc<QueryContext<T, D>>,
  pub(crate) id: u32,
  /// source node
  pub(crate) from: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  /// Number of duplicate responses to relay back to sender
  pub(crate) relay_factor: u8,
}

impl<D, T> AsRef<QueryEvent<T, D>> for QueryEvent<T, D>
where
  D: Delegate,
  T: Transport,
{
  fn as_ref(&self) -> &QueryEvent<T, D> {
    self
  }
}

impl<D, T> Clone for QueryEvent<T, D>
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

impl<D, T> core::fmt::Display for QueryEvent<T, D>
where
  D: Delegate,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "query")
  }
}

impl<D, T> QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) fn create_response(
    &self,
    buf: Bytes,
  ) -> QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
    QueryResponseMessage {
      ltime: self.ltime,
      id: self.id,
      from: self.ctx.this.inner.memberlist.local_id().clone(),
      flags: 0,
      payload: buf,
    }
  }

  pub(crate) fn check_response_size(&self, resp: &[u8]) -> Result<(), Error<T, D>> {
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
    raw: Bytes,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.check_response_size(raw.as_ref())?;

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
  pub async fn respond(&self, msg: Bytes) -> Result<(), Error<T, D>> {
    let resp = self.create_response(msg);
    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&resp);
    let mut buf = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type byte
    buf.put_u8(SerfMessageRef::QueryResponse as u8);
    buf.resize(expected_encoded_len + 1, 0);
    let len = <D as TransformDelegate>::encode_message(&resp, &mut buf[1..])?;
    debug_assert_eq!(
      len, expected_encoded_len,
      "expected encoded len {expected_encoded_len} is not match the actual encoded len {len}"
    );
    self
      .respond_with_message_and_response(buf.freeze(), resp)
      .await
  }
}
