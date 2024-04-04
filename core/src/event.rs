use std::{pin::Pin, sync::Arc, task::Poll, time::{Duration, Instant}};

use crate::delegate::TransformDelegate;

use self::error::Error;

use super::{*, delegate::Delegate};

mod crate_event;
use async_channel::Sender;
use async_lock::Mutex;
pub(crate) use crate_event::*;
use either::Either;
use futures::Stream;
use memberlist_core::{bytes::{BufMut, Bytes, BytesMut}, transport::{AddressResolver, Transport}, types::TinyVec, CheapClone};
use ruserf_types::{LamportTime, Member, MessageType, Node, QueryFlag, QueryResponseMessage, UserEventMessage};
use smol_str::SmolStr;

pub(crate) struct QueryContext<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) query_timeout: Duration,
  pub(crate) span: Mutex<Option<Instant>>,
  pub(crate) this: Serf<T, D>,
}

impl<T, D> QueryContext<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn check_response_size(&self, resp: &[u8]) -> Result<(), Error<T, D>> {
    let resp_len = resp.len();
    if resp_len > self.this.inner.opts.query_response_size_limit {
      Err(Error::QueryResponseTooLarge {
        limit: self.this.inner.opts.query_response_size_limit,
        got: resp_len,
      })
    } else {
      Ok(())
    }
  }

  async fn respond_with_message_and_response(
    &self,
    relay_factor: u8,
    raw: Bytes,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.check_response_size(raw.as_ref())?;

    let mut mu = self.span.lock().await;

    if let Some(span) = *mu {
      // Ensure we aren't past our response deadline
      if span.elapsed() > self.query_timeout {
        return Err(Error::QueryTimeout);
      }

      // Send the response directly to the originator
      self
        .this
        .inner
        .memberlist
        .send(resp.from.address(), raw)
        .await?;

      // Relay the response through up to relayFactor other nodes
      self
        .this
        .relay_response(relay_factor, resp.from.cheap_clone(), resp)
        .await?;

      // Clear the deadline, responses sent
      *mu = None;
      Ok(())
    } else {
      Err(Error::QueryAlreadyResponsed)
    }
  }

  async fn respond(
    &self,
    id: u32,
    ltime: LamportTime,
    relay_factor: u8,
    from: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    let resp = QueryResponseMessage {
      ltime,
      id,
      from,
      flags: QueryFlag::empty(),
      payload: msg,
    };
    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&resp);
    let mut buf = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type byte
    buf.put_u8(MessageType::QueryResponse as u8);
    buf.resize(expected_encoded_len + 1, 0);
    let len =
      <D as TransformDelegate>::encode_message(&resp, &mut buf[1..]).map_err(Error::transform)?;
    debug_assert_eq!(
      len, expected_encoded_len,
      "expected encoded len {expected_encoded_len} is not match the actual encoded len {len}"
    );
    self
      .respond_with_message_and_response(relay_factor, buf.freeze(), resp)
      .await
  }
}

/// Query is the struct used by EventQuery type events
pub struct QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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

impl<D, T> PartialEq for QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn eq(&self, other: &Self) -> bool {
    self.id == other.id
      && self.from == other.from
      && self.relay_factor == other.relay_factor
      && self.ltime == other.ltime
      && self.name == other.name
      && self.payload == other.payload
  }
}

impl<D, T> AsRef<QueryEvent<T, D>> for QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn as_ref(&self) -> &QueryEvent<T, D> {
    self
  }
}

impl<D, T> Clone for QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
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
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
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
      from: self.ctx.this.inner.memberlist.advertise_node(),
      flags: QueryFlag::empty(),
      payload: buf,
    }
  }

  pub(crate) fn check_response_size(&self, resp: &[u8]) -> Result<(), Error<T, D>> {
    self.ctx.check_response_size(resp)
  }

  pub(crate) async fn respond_with_message_and_response(
    &self,
    raw: Bytes,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self
      .ctx
      .respond_with_message_and_response(self.relay_factor, raw, resp)
      .await
  }

  /// Used to send a response to the user query
  pub async fn respond(&self, msg: Bytes) -> Result<(), Error<T, D>> {
    self
      .ctx
      .respond(
        self.id,
        self.ltime,
        self.relay_factor,
        self.from.cheap_clone(),
        msg,
      )
      .await
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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MemberEventMut<I, A> {
  pub(crate) ty: MemberEventType,
  pub(crate) members: TinyVec<Member<I, A>>,
}

impl<I, A> MemberEventMut<I, A> {
  pub(crate) fn freeze(self) -> MemberEvent<I, A> {
    MemberEvent {
      ty: self.ty,
      members: Arc::new(self.members),
    }
  }
}

/// MemberEvent is the struct used for member related events
/// Because Serf coalesces events, an event may contain multiple members.
#[derive(Debug, Clone, PartialEq)]
pub struct MemberEvent<I, A> {
  pub(crate) ty: MemberEventType,
  pub(crate) members: Arc<TinyVec<Member<I, A>>>,
}

impl<I, A> core::fmt::Display for MemberEvent<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.ty)
  }
}

impl<I, A> MemberEvent<I, A> {
  pub fn new(ty: MemberEventType, members: TinyVec<Member<I, A>>) -> Self {
    Self { ty, members: Arc::new(members) }
  }

  pub fn ty(&self) -> MemberEventType {
    self.ty
  }

  pub fn members(&self) -> &[Member<I, A>] {
    &self.members
  }
}

impl<I, A> From<MemberEvent<I, A>> for (MemberEventType, Arc<TinyVec<Member<I, A>>>) {
  fn from(event: MemberEvent<I, A>) -> Self {
    (event.ty, event.members)
  }
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

pub(crate) enum EventKind<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  Member(MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  User(UserEventMessage),
  Query(QueryEvent<T, D>),
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
    }
  }
}

pub struct EventProducer<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) tx: Sender<CrateEvent<T, D>>,
}

impl<T, D> EventProducer<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub fn bounded(size: usize) -> (Self, EventSubscriber<T, D>) {
    let (tx, rx) = async_channel::bounded(size);
    (Self { tx }, EventSubscriber { rx })
  }

  pub fn unbounded() -> (Self, EventSubscriber<T, D>) {
    let (tx, rx) = async_channel::unbounded();
    (Self { tx }, EventSubscriber { rx })
  }
}

/// Subscribe the events from the Serf instance.
#[pin_project::pin_project]
pub struct EventSubscriber<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[pin]
  pub(crate) rx: async_channel::Receiver<CrateEvent<T, D>>,
}

impl<T, D> Stream for EventSubscriber<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Item = Event<T, D>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
    match <async_channel::Receiver<CrateEvent<T, D>> as Stream>::poll_next(self.project().rx, cx) {
      Poll::Ready(Some(event)) => match event.0 {
        Either::Left(e) => match e {
          CrateEventKind::Member(e) => Poll::Ready(Some(Event(Either::Left(EventKind::Member(e))))),
          CrateEventKind::User(e) => Poll::Ready(Some(Event(Either::Left(EventKind::User(e))))),
          CrateEventKind::Query(e) => Poll::Ready(Some(Event(Either::Left(EventKind::Query(e))))),
          CrateEventKind::InternalQuery { .. } => Poll::Pending,
        },
        Either::Right(kind) => match &*kind {
          CrateEventKind::Member(e) => Poll::Ready(Some(Event(Either::Right(kind)))),
          CrateEventKind::User(e) => Poll::Ready(Some(Event(Either::Left(EventKind::User(e))))),
          CrateEventKind::Query(e) => Poll::Ready(Some(Event(Either::Left(EventKind::Query(e))))),
          CrateEventKind::InternalQuery { .. } => Poll::Pending,
        },
      },
      Poll::Ready(None) => Poll::Ready(None),
      Poll::Pending => Poll::Pending,
    }
  }
}
