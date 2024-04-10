use std::{pin::Pin, sync::Arc, task::Poll, time::Duration};

use crate::delegate::TransformDelegate;

use self::error::Error;

use super::{delegate::Delegate, types::Epoch, *};

mod crate_event;

use async_channel::Sender;
pub use async_channel::{RecvError, TryRecvError};

use async_lock::Mutex;
pub(crate) use crate_event::*;
use futures::Stream;
use memberlist_core::{
  bytes::{BufMut, Bytes, BytesMut},
  transport::{AddressResolver, Transport},
  types::TinyVec,
  CheapClone,
};
use ruserf_types::{
  LamportTime, Member, MessageType, Node, QueryFlag, QueryResponseMessage, UserEventMessage,
};
use smol_str::SmolStr;

pub(crate) struct QueryContext<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) query_timeout: Duration,
  pub(crate) span: Mutex<Option<Epoch>>,
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
      Err(Error::query_response_too_large(
        self.this.inner.opts.query_response_size_limit,
        resp_len,
      ))
    } else {
      Ok(())
    }
  }

  async fn respond_with_message_and_response(
    &self,
    respond_to: &<T::Resolver as AddressResolver>::ResolvedAddress,
    relay_factor: u8,
    raw: Bytes,
    resp: QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.check_response_size(raw.as_ref())?;

    let mut mu = self.span.lock().await;

    if let Some(span) = *mu {
      // Ensure we aren't past our response deadline
      if span.elapsed() > self.query_timeout {
        return Err(Error::query_timeout());
      }

      // Send the response directly to the originator
      self.this.inner.memberlist.send(respond_to, raw).await?;

      // Relay the response through up to relayFactor other nodes
      self
        .this
        .relay_response(relay_factor, resp.from.cheap_clone(), resp)
        .await?;

      // Clear the deadline, responses sent
      *mu = None;
      Ok(())
    } else {
      Err(Error::query_already_responsed())
    }
  }

  async fn respond(
    &self,
    respond_to: &<T::Resolver as AddressResolver>::ResolvedAddress,
    id: u32,
    ltime: LamportTime,
    relay_factor: u8,
    msg: Bytes,
  ) -> Result<(), Error<T, D>> {
    let resp = QueryResponseMessage {
      ltime,
      id,
      from: self.this.advertise_node(),
      flags: QueryFlag::empty(),
      payload: msg,
    };
    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&resp);
    let mut buf = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type byte
    buf.put_u8(MessageType::QueryResponse as u8);
    buf.resize(expected_encoded_len + 1, 0);
    let len = <D as TransformDelegate>::encode_message(&resp, &mut buf[1..])
      .map_err(Error::transform_delegate)?;
    debug_assert_eq!(
      len, expected_encoded_len,
      "expected encoded len {expected_encoded_len} is not match the actual encoded len {len}"
    );
    self
      .respond_with_message_and_response(respond_to, relay_factor, buf.freeze(), resp)
      .await
  }
}

/// Query event
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

impl<D, T> QueryEvent<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  /// Returns the lamport time of the query
  #[inline]
  pub const fn lamport_time(&self) -> LamportTime {
    self.ltime
  }

  /// Returns the name of the query
  #[inline]
  pub const fn name(&self) -> &SmolStr {
    &self.name
  }

  /// Returns the payload of the query
  #[inline]
  pub const fn payload(&self) -> &Bytes {
    &self.payload
  }

  /// Returns the id of the query
  #[inline]
  pub const fn id(&self) -> u32 {
    self.id
  }

  /// Returns the source node of the query
  #[inline]
  pub const fn from(&self) -> &Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
    &self.from
  }
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
      .respond_with_message_and_response(self.from.address(), self.relay_factor, raw, resp)
      .await
  }

  /// Used to send a response to the user query
  pub async fn respond(&self, msg: Bytes) -> Result<(), Error<T, D>> {
    self
      .ctx
      .respond(
        self.from().address(),
        self.id,
        self.ltime,
        self.relay_factor,
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
#[derive(Debug, PartialEq)]
pub struct MemberEvent<I, A> {
  pub(crate) ty: MemberEventType,
  pub(crate) members: Arc<TinyVec<Member<I, A>>>,
}

impl<I, A> Clone for MemberEvent<I, A> {
  fn clone(&self) -> Self {
    Self {
      ty: self.ty,
      members: self.members.clone(),
    }
  }
}

impl<I, A> CheapClone for MemberEvent<I, A> {}

impl<I, A> core::fmt::Display for MemberEvent<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.ty)
  }
}

impl<I, A> MemberEvent<I, A> {
  pub fn new(ty: MemberEventType, members: TinyVec<Member<I, A>>) -> Self {
    Self {
      ty,
      members: Arc::new(members),
    }
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

/// The event produced by the Serf instance.
#[derive(derive_more::From)]
pub enum Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  /// Member related events
  Member(MemberEvent<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  /// User events
  User(UserEventMessage),
  /// Query events
  Query(QueryEvent<T, D>),
}

impl<D, T> Clone for Event<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn clone(&self) -> Self {
    match self {
      Self::Member(e) => Self::Member(e.cheap_clone()),
      Self::User(e) => Self::User(e.cheap_clone()),
      Self::Query(e) => Self::Query(e.clone()),
    }
  }
}

/// The producer of the Serf events.
#[derive(Debug)]
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
  /// Creates a bounded producer and subscriber.
  ///
  /// The created subscriber has space to hold at most cap events at a time.
  /// Users must actively consume the events from the subscriber to prevent the producer from blocking.
  pub fn bounded(size: usize) -> (Self, EventSubscriber<T, D>) {
    let (tx, rx) = async_channel::bounded(size);
    (Self { tx }, EventSubscriber { rx })
  }

  /// Creates an unbounded producer and subscriber.
  ///
  /// The created subscriber has no limit on the number of events it can hold.
  pub fn unbounded() -> (Self, EventSubscriber<T, D>) {
    let (tx, rx) = async_channel::unbounded();
    (Self { tx }, EventSubscriber { rx })
  }
}

/// Subscribe the events from the Serf instance.
#[pin_project::pin_project]
#[derive(Debug)]
pub struct EventSubscriber<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[pin]
  pub(crate) rx: async_channel::Receiver<CrateEvent<T, D>>,
}

impl<T, D> EventSubscriber<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  /// Receives a event from the subscriber.
  ///
  /// If the subscriber is empty, this method waits until there is a event.
  ///
  /// If the subscriber is closed, this method receives a event or returns an error if there are no more events
  pub async fn recv(&self) -> Result<Event<T, D>, RecvError> {
    loop {
      match self.rx.recv().await {
        Ok(CrateEvent::InternalQuery { .. }) => continue,
        Ok(CrateEvent::Member(e)) => return Ok(Event::Member(e)),
        Ok(CrateEvent::User(e)) => return Ok(Event::User(e)),
        Ok(CrateEvent::Query(e)) => return Ok(Event::Query(e)),
        Err(e) => return Err(e),
      }
    }
  }

  /// Tries to receive a event from the subscriber.
  ///
  /// If the subscriber is empty, this method returns an error.
  /// If the subscriber is closed, this method receives a event or returns an error if there are no more events
  pub fn try_recv(&self) -> Result<Event<T, D>, TryRecvError> {
    loop {
      match self.rx.try_recv() {
        Ok(CrateEvent::InternalQuery { .. }) => continue,
        Ok(CrateEvent::Member(e)) => return Ok(Event::Member(e)),
        Ok(CrateEvent::User(e)) => return Ok(Event::User(e)),
        Ok(CrateEvent::Query(e)) => return Ok(Event::Query(e)),
        Err(e) => return Err(e),
      }
    }
  }

  /// Returns `true` if the subscriber is empty.
  pub fn is_empty(&self) -> bool {
    self.rx.is_empty()
  }

  /// Returns `true` if the channel is closed.
  pub fn is_closed(&self) -> bool {
    self.rx.is_closed()
  }

  /// Returns the number of events in the subscriber.
  pub fn len(&self) -> usize {
    self.rx.len()
  }
}

impl<T, D> Stream for EventSubscriber<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  type Item = Event<T, D>;

  fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Option<Self::Item>> {
    match <async_channel::Receiver<CrateEvent<T, D>> as Stream>::poll_next(self.project().rx, cx) {
      Poll::Ready(Some(event)) => match event {
        CrateEvent::Member(e) => Poll::Ready(Some(Event::Member(e))),
        CrateEvent::User(e) => Poll::Ready(Some(Event::User(e))),
        CrateEvent::Query(e) => Poll::Ready(Some(Event::Query(e))),
        CrateEvent::InternalQuery { .. } => Poll::Pending,
      },
      Poll::Ready(None) => Poll::Ready(None),
      Poll::Pending => Poll::Pending,
    }
  }
}
