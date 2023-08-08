use std::{
  collections::HashMap,
  time::{Duration, Instant},
};

use rmp_serde::{
  decode::Error as DecodeError, encode::Error as EncodeError, Deserializer as RmpDeserializer,
  Serializer as RmpSerializer,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use showbiz_core::{bytes::Bytes, Message, NodeId};
use smol_str::SmolStr;

use crate::{clock::LamportTime, query::QueryResponse, UserEvents};

/// The types of gossip messages Serf will send along
/// showbiz.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum MessageType {
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

impl core::fmt::Display for MessageType {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl MessageType {
  pub(crate) const fn as_str(&self) -> &'static str {
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
pub(crate) enum FilterType {
  Node(Vec<NodeId>),
  Tag(Tag),
}

impl FilterType {
  pub(crate) const NODE: u8 = 0;
  pub(crate) const TAG: u8 = 1;

  pub(crate) fn as_ref(&self) -> FilterTypeRef {
    match self {
      FilterType::Node(nodes) => FilterTypeRef::Node(nodes),
      FilterType::Tag(t) => FilterTypeRef::Tag(TagRef {
        tag: t.tag.as_str(),
        expr: t.expr.as_str(),
      }),
    }
  }
}

pub(crate) enum FilterTypeRef<'a> {
  Node(&'a [NodeId]),
  Tag(TagRef<'a>),
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct JoinMessage {
  ltime: LamportTime,
  node: NodeId,
}

impl JoinMessage {
  pub fn new(ltime: LamportTime, node: NodeId) -> Self {
    Self { ltime, node }
  }
}

/// The message broadcasted to signal the intentional to
/// leave.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Leave {
  ltime: LamportTime,
  node: NodeId,
  prune: bool,
}

/// Used when doing a state exchange. This
/// is a relatively large message, but is sent infrequently
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct PushPull {
  /// Current node lamport time
  ltime: LamportTime,
  /// Maps the node to its status time
  status_ltimes: HashMap<NodeId, LamportTime>,
  /// List of left nodes
  left_members: Vec<NodeId>,
  /// Lamport time for event clock
  event_ltime: LamportTime,
  /// Recent events
  events: Vec<UserEvents>,
  /// Lamport time for query clock
  query_ltime: LamportTime,
}

/// Used for user-generated events
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct MessageUserEvent {
  ltime: LamportTime,
  name: SmolStr,
  payload: Bytes,
  /// "Can Coalesce".
  cc: bool,
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct QueryMessage {
  /// Event lamport time
  ltime: LamportTime,
  /// query id, randomly generated
  id: u32,
  /// source node
  from: NodeId,
  /// Potential query filters
  filters: Vec<Bytes>,
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

impl QueryMessage {
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
  pub(crate) fn response(&self, num_nodes: usize) -> QueryResponse {
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct QueryResponseMessage {
  /// Event lamport time
  ltime: LamportTime,
  /// query id
  id: u32,
  /// node
  from: NodeId,
  /// Used to provide various flags
  flags: u32,
  /// Optional response payload
  payload: Bytes,
}

impl QueryResponseMessage {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::ACK) != QueryFlag::empty()
  }
}

/// Used to store the end destination of a relayed message
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub(crate) struct RelayHeader {
  dest: NodeId,
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Tag {
  tag: SmolStr,
  expr: SmolStr,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct TagRef<'a> {
  tag: &'a str,
  expr: &'a str,
}

pub(crate) fn encode_message<T>(t: MessageType, msg: &T) -> Result<Message, EncodeError>
where
  T: Serialize + ?Sized,
{
  let mut wr = Message::with_capacity(128);
  wr.put_u8(t as u8);
  msg.serialize(&mut RmpSerializer::new(&mut wr)).map(|_| wr)
}

pub(crate) fn decode_message<T>(src: &[u8]) -> Result<T, DecodeError>
where
  T: DeserializeOwned,
{
  T::deserialize(&mut RmpDeserializer::new(src))
}

/// Wraps a message in the `MessageType::Relay`, adding the length and
/// address of the end recipient to the front of the message
pub(crate) fn encode_relay_message<T>(
  t: MessageType,
  dest: NodeId,
  msg: &T,
) -> Result<Message, EncodeError>
where
  T: Serialize + ?Sized,
{
  let mut wr = Message::with_capacity(128);
  wr.put_u8(MessageType::Relay as u8);
  {
    let ser = &mut RmpSerializer::new(&mut wr);
    RelayHeader { dest }.serialize(ser)?;
  }

  wr.put_u8(t as u8);
  msg.serialize(&mut RmpSerializer::new(&mut wr)).map(|_| wr)
}

pub(crate) fn encode_filter(f: FilterTypeRef<'_>) -> Result<Bytes, EncodeError> {
  match f {
    FilterTypeRef::Node(nodes) => rmp_serde::to_vec(nodes).map(Into::into),
    FilterTypeRef::Tag(t) => rmp_serde::to_vec(&t).map(Into::into),
  }
}
