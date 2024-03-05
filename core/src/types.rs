use std::{
  collections::HashMap,
  time::{Duration, Instant},
};

use indexmap::IndexSet;
use memberlist_core::{
  bytes::Bytes,
  transport::{Address, Id, Node},
  types::{OneOrMore, TinyVec},
};
use smol_str::SmolStr;

use crate::{clock::LamportTime, query::QueryResponse, UserEvent, UserEvents};

/// Tags of a node
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Tags(HashMap<SmolStr, SmolStr>);

impl core::ops::Deref for Tags {
  type Target = HashMap<SmolStr, SmolStr>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl core::ops::DerefMut for Tags {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

impl IntoIterator for Tags {
  type Item = (SmolStr, SmolStr);
  type IntoIter = std::collections::hash_map::IntoIter<SmolStr, SmolStr>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl FromIterator<(SmolStr, SmolStr)> for Tags {
  fn from_iter<T: IntoIterator<Item = (SmolStr, SmolStr)>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl Tags {
  pub fn new() -> Self {
    Self(HashMap::new())
  }

  pub fn with_capacity(cap: usize) -> Self {
    Self(HashMap::with_capacity(cap))
  }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[repr(u8)]
#[non_exhaustive]
pub enum MessageType {
  Leave,
  Join,
  PushPull,
  UserEvent,
  Query,
  QueryResponse,
  ConflictResponse,
  KeyRequest,
  KeyResponse,
  Relay,
}

impl MessageType {
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::Leave => "leave",
      Self::Join => "join",
      Self::PushPull => "push pull",
      Self::UserEvent => "user event",
      Self::Query => "query",
      Self::QueryResponse => "query response",
      Self::ConflictResponse => "conflict response",
      Self::KeyRequest => "key request",
      Self::KeyResponse => "key response",
      Self::Relay => "relay",
    }
  }
}

#[derive(Debug, Clone, Eq, PartialEq, derive_more::From)]
pub struct SerfRelayMessage<I, A> {
  target: Node<I, A>,
  msg: SerfMessage<I, A>,
}

impl<I, A> From<SerfRelayMessage<I, A>> for SerfMessage<I, A> {
  fn from(msg: SerfRelayMessage<I, A>) -> Self {
    msg.msg
  }
}

impl<I, A> SerfRelayMessage<I, A> {
  pub fn new(target: Node<I, A>, msg: SerfMessage<I, A>) -> Self {
    Self { target, msg }
  }

  pub fn target(&self) -> &Node<I, A> {
    &self.target
  }

  pub fn target_mut(&mut self) -> &mut Node<I, A> {
    &mut self.target
  }

  pub fn message(&self) -> &SerfMessage<I, A> {
    &self.msg
  }

  pub fn message_mut(&mut self) -> &mut SerfMessage<I, A> {
    &mut self.msg
  }

  pub fn into_components(self) -> (Node<I, A>, SerfMessage<I, A>) {
    (self.target, self.msg)
  }
}

pub trait AsMessageRef<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum SerfMessageRef<'a, I, A> {
  Leave(&'a Leave<I, A>),
  Join(&'a JoinMessage<I, A>),
  PushPull(PushPullRef<'a, I, A>),
  UserEvent(&'a MessageUserEvent),
  Query(&'a QueryMessage<I, A>),
  QueryResponse(&'a QueryResponseMessage<I, A>),
  ConflictResponse,
  KeyRequest,
  KeyResponse,
  Relay,
}

impl<'a, I, A> Clone for SerfMessageRef<'a, I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, I, A> Copy for SerfMessageRef<'a, I, A> {}

impl<'a, I, A> AsMessageRef<I, A> for SerfMessageRef<'a, I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    *self
  }
}

/// The types of gossip messages Serf will send along
/// showbiz.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum SerfMessage<I, A> {
  Leave(Leave<I, A>),
  Join(JoinMessage<I, A>),
  PushPull(PushPull<I, A>),
  UserEvent(MessageUserEvent),
  Query(QueryMessage<I, A>),
  QueryResponse(QueryResponseMessage<I, A>),
  ConflictResponse,
  KeyRequest,
  KeyResponse,
}

impl<I, A> AsMessageRef<I, A> for QueryMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::Query(self)
  }
}

impl<I, A> AsMessageRef<I, A> for QueryResponseMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::QueryResponse(self)
  }
}

impl<I, A> AsMessageRef<I, A> for JoinMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::Join(self)
  }
}

impl<I, A> AsMessageRef<I, A> for MessageUserEvent {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::UserEvent(self)
  }
}

impl<'a, I, A> AsMessageRef<I, A> for &'a QueryMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::Query(self)
  }
}

impl<'a, I, A> AsMessageRef<I, A> for &'a QueryResponseMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::QueryResponse(self)
  }
}

impl<'a, I, A> AsMessageRef<I, A> for &'a JoinMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::Join(self)
  }
}

impl<'a, I, A> AsMessageRef<I, A> for PushPullRef<'a, I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::PushPull(self)
  }
}

impl<'a, I, A> AsMessageRef<I, A> for &'a MessageUserEvent {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    SerfMessageRef::UserEvent(self)
  }
}

impl<I, A> AsMessageRef<I, A> for SerfMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    match self {
      Self::Leave(l) => SerfMessageRef::Leave(l),
      Self::Join(j) => SerfMessageRef::Join(j),
      Self::PushPull(pp) => SerfMessageRef::PushPull(PushPullRef {
        ltime: pp.ltime,
        status_ltimes: &pp.status_ltimes,
        left_members: &pp.left_members,
        event_ltime: pp.event_ltime,
        events: &pp.events,
        query_ltime: pp.query_ltime,
      }),
      Self::UserEvent(u) => SerfMessageRef::UserEvent(u),
      Self::Query(q) => SerfMessageRef::Query(q),
      Self::QueryResponse(q) => SerfMessageRef::QueryResponse(q),
      Self::ConflictResponse => SerfMessageRef::ConflictResponse,
      Self::KeyRequest => SerfMessageRef::KeyRequest,
      Self::KeyResponse => SerfMessageRef::KeyResponse,
    }
  }
}

impl<'b, I, A> AsMessageRef<I, A> for &'b SerfMessage<I, A> {
  fn as_message_ref(&self) -> SerfMessageRef<I, A> {
    match self {
      Self::Leave(l) => SerfMessageRef::Leave(l),
      Self::Join(j) => SerfMessageRef::Join(j),
      Self::PushPull(pp) => SerfMessageRef::PushPull(PushPullRef {
        ltime: pp.ltime,
        status_ltimes: &pp.status_ltimes,
        left_members: &pp.left_members,
        event_ltime: pp.event_ltime,
        events: &pp.events,
        query_ltime: pp.query_ltime,
      }),
      Self::UserEvent(u) => SerfMessageRef::UserEvent(u),
      Self::Query(q) => SerfMessageRef::Query(q),
      Self::QueryResponse(q) => SerfMessageRef::QueryResponse(q),
      Self::ConflictResponse => SerfMessageRef::ConflictResponse,
      Self::KeyRequest => SerfMessageRef::KeyRequest,
      Self::KeyResponse => SerfMessageRef::KeyResponse,
    }
  }
}

impl<I, A> core::fmt::Display for SerfMessage<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl<I, A> SerfMessage<I, A> {
  pub(crate) const fn as_str(&self) -> &'static str {
    match self {
      Self::Leave(_) => "leave",
      Self::Join(_) => "join",
      Self::PushPull(_) => "push pull",
      Self::UserEvent(_) => "user event",
      Self::Query(_) => "query",
      Self::QueryResponse(_) => "query response",
      Self::ConflictResponse => "conflict response",
      Self::KeyRequest => "key request",
      Self::KeyResponse => "key response",
      Self::Relay => "relay",
    }
  }
}

bitflags::bitflags! {
  #[derive(PartialEq, Eq)]
  pub(crate) struct QueryFlag: u32 {
    /// Ack flag is used to force receiver to send an ack back
    const ACK = 1 << 0;
    /// NoBroadcast is used to prevent re-broadcast of a query.
    /// this can be used to selectively send queries to individual members
    const NO_BROADCAST = 1 << 1;
  }
}

/// Used with a queryFilter to specify the type of
/// filter we are sending
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Filter<I, A> {
  /// Filter by nodes
  Node(TinyVec<Node<I, A>>),
  /// Filter by tag
  Tag(Tag),
}

impl<I, A> Filter<I, A> {
  pub(crate) const NODE: u8 = 0;
  pub(crate) const TAG: u8 = 1;
}

// impl FilterType {
//   pub(crate) const fn as_str(&self) -> &'static str {
//     match self {
//       Self::Node => "node",
//       Self::Tag => "tag",
//     }
//   }
// }

// impl core::fmt::Display for FilterType {
//   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//     write!(f, "{}", self.as_str())
//   }
// }

/// The message broadcasted after we join to
/// associated the node with a lamport clock
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct JoinMessage<I, A> {
  ltime: LamportTime,
  node: Node<I, A>,
}

impl<I, A> JoinMessage<I, A> {
  pub fn new(ltime: LamportTime, node: Node<I, A>) -> Self {
    Self { ltime, node }
  }
}

/// The message broadcasted to signal the intentional to
/// leave.
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Leave<I, A> {
  ltime: LamportTime,
  node: Node<I, A>,
  prune: bool,
}

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct PushPull<I, A> {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: HashMap<Node<I, A>, LamportTime>,
  /// List of left nodes
  left_members: IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: TinyVec<Option<UserEvents>>,
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[viewit::viewit]
#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub(crate) struct PushPullRef<'a, I, A> {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: &'a HashMap<I, LamportTime>,
  /// List of left nodes
  left_members: &'a IndexSet<Node<I, A>>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: &'a [Option<UserEvents>],
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

impl<'a, I, A> Clone for PushPullRef<'a, I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<'a, I, A> Copy for PushPullRef<'a, I, A> {}

/// Used for user-generated events
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct MessageUserEvent {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,
  /// "Can Coalesce".
  cc: bool,
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct QueryMessage<I, A> {
  /// Event lamport time
  ltime: LamportTime,
  /// query id, randomly generated
  id: u32,
  /// source node
  from: Node<I, A>,
  /// Potential query filters
  filters: TinyVec<Bytes>,
  /// Used to provide various flags
  flags: u32,
  /// Used to set the number of duplicate relayed responses
  relay_factor: u8,
  /// Maximum time between delivery and response
  timeout: Duration,
  /// Query nqme
  name: SmolStr,
  /// Query payload
  payload: Bytes,
}

impl<I, A> QueryMessage<I, A> {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::ACK) != QueryFlag::empty()
  }

  /// checks if the no broadcast flag is set
  #[inline]
  pub(crate) fn no_broadcast(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::NO_BROADCAST) != QueryFlag::empty()
  }

  #[inline]
  pub(crate) fn response(&self, num_nodes: usize) -> QueryResponse<I, A> {
    QueryResponse::new(
      self.id,
      self.ltime,
      num_nodes,
      Instant::now() + self.timeout,
      self.ack(),
    )
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryResponseMessage<I, A> {
  /// Event lamport time
  ltime: LamportTime,
  /// query id
  id: u32,
  /// node
  from: Node<I, A>,
  /// Used to provide various flags
  flags: u32,
  /// Optional response payload
  payload: Bytes,
}

impl<I, A> QueryResponseMessage<I, A> {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::ACK) != QueryFlag::empty()
  }
}

/// Used to store the end destination of a relayed message
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
#[repr(transparent)]
pub(crate) struct RelayHeader<I, A> {
  pub(crate) dest: Node<I, A>,
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Tag {
  tag: SmolStr,
  expr: SmolStr,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct TagRef<'a> {
  tag: &'a str,
  expr: &'a str,
}

// pub(crate) fn encode_message<T>(t: SerfMessage, msg: &T) -> Result<Message, EncodeError>
// where
//   T: Serialize + ?Sized,
// {
//   let mut wr = Message::with_capacity(128);
//   wr.put_u8(t as u8);
//   msg.serialize(&mut RmpSerializer::new(&mut wr)).map(|_| wr)
// }

// pub(crate) fn decode_message<T>(src: &[u8]) -> Result<T, DecodeError>
// where
//   T: DeserializeOwned,
// {
//   T::deserialize(&mut RmpDeserializer::new(src))
// }

// pub(crate) fn decode_message_from_reader<T, R>(src: &mut R) -> Result<T, DecodeError>
// where
//   T: DeserializeOwned,
//   R: std::io::Read,
// {
//   T::deserialize(&mut RmpDeserializer::new(src))
// }

// /// Wraps a message in the `SerfMessage::Relay`, adding the length and
// /// address of the end recipient to the front of the message
// pub(crate) fn encode_relay_message<T>(
//   t: SerfMessage,
//   dest: Node,
//   msg: &T,
// ) -> Result<Message, EncodeError>
// where
//   T: Serialize + ?Sized,
// {
//   let mut wr = Message::with_capacity(128);
//   wr.put_u8(SerfMessage::Relay as u8);
//   {
//     let ser = &mut RmpSerializer::new(&mut wr);
//     RelayHeader { dest }.serialize(ser)?;
//   }

//   wr.put_u8(t as u8);
//   msg.serialize(&mut RmpSerializer::new(&mut wr)).map(|_| wr)
// }

// pub(crate) fn encode_filter(f: FilterTypeRef<'_>) -> Result<Bytes, EncodeError> {
//   // match f {
//   //   FilterTypeRef::Node(nodes) => rmp_serde::to_vec(nodes).map(Into::into),
//   //   FilterTypeRef::Tag(t) => rmp_serde::to_vec(&t).map(Into::into),
//   // }
//   todo!()
// }

// #[test]
// fn test_x() {
//   let id = NodeId::new("test".try_into().unwrap(), "127.0.0.1:8080".parse().unwrap());
//   let data = encode_relay_message(SerfMessage::Leave, id.clone(), &Leave {
//     ltime: LamportTime(0),
//     node: NodeId::new("test1".try_into().unwrap(), "127.0.0.1:8080".parse().unwrap()),
//     prune: false,
//   }).unwrap();

//   let header = decode_message::<RelayHeader>(&data[1..]).unwrap();
//   assert_eq!(header.dest, id);
//   println!("{}", header.dest);
// }
