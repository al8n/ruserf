use std::{collections::HashMap, time::Duration, sync::Arc};

use rmp_serde::{
  decode::Error as DecodeError, encode::Error as EncodeError, Deserializer as RmpDeserializer,
  Serializer as RmpSerializer,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize, Serializer};
use showbiz_core::{bytes::{Bytes, BytesMut, BufMut}, security::SecretKey, Message, NodeId, Name};

use crate::{clock::LamportTime, UserEvents};

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
#[repr(u8)]
pub(crate) enum FilterType {
  Node(Vec<NodeId>),
  Tag(Tag),
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct Join {
  ltime: LamportTime,
  node: NodeId,
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
  name: Name,
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
  name: Name,
  /// Query payload
  payload: Bytes,
}

impl QueryMessage {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag { bits: self.flags } & QueryFlag::ACK) != QueryFlag::empty()
  }

  /// checks if the no broadcast flag is set
  #[inline]
  pub(crate) fn no_broadcast(&self) -> bool {
    (QueryFlag { bits: self.flags } & QueryFlag::NO_BROADCAST) != QueryFlag::empty()
  }

  #[inline]
  pub(crate) fn response(&self, num_nodes: usize) -> Arc<QueryResponse> {
    let resp = QueryResponse {
      ltime: todo!(),
      id: todo!(),
      from: todo!(),
      flags: todo!(),
      payload: todo!(),
    };
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
    (QueryFlag { bits: self.flags } & QueryFlag::ACK) != QueryFlag::empty()
  }
}

/// Used to store the end destination of a relayed message
pub(crate) struct RelayHeader {
  dest: NodeId,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct Tag {
  tag: String,
  expr: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Copy)]
#[repr(transparent)]
pub(crate) struct KeyRequest {
  key: SecretKey,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyResponse {
  /// Map of node name to response message
  messages: HashMap<NodeId, Message>,
  /// Total nodes memberlist knows of
  num_nodes: usize,
  /// Total responses received
  num_resp: usize,
  /// Total errors from request
  num_err: usize,

  /// A mapping of the value of the key to the
  /// number of nodes that have the key installed.
  keys: HashMap<SecretKey, usize>,

  /// A mapping of the value of the primary
  /// key bytes to the number of nodes that have the key installed.
  primary_keys: HashMap<SecretKey, usize>,
}

/// Used to contain optional parameters for a keyring operation
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(transparent)]
pub struct KeyRequestOptions {
  relay_factor: u8,
}

impl KeyRequestOptions {
  #[inline]
  pub const fn new(relay_factor: u8) -> Self {
    Self { relay_factor }
  }

  /// Get the number of duplicate query responses to send by relaying through
  /// other nodes, for redundancy
  #[inline]
  pub const fn replay_factor(&self) -> u8 {
    self.relay_factor
  }

  /// Set the number of duplicate query responses to send by relaying through
  /// other nodes, for redundancy
  #[inline]
  pub const fn set_relay_factor(mut self, factor: u8) -> Self {
    self.relay_factor = factor;
    self
  }
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
