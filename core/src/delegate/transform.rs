use memberlist_core::{
  bytes::Bytes,
  transport::{Id, Node, Transformable},
  CheapClone,
};

use crate::{
  coordinate::Coordinate, types::{AsMessageRef, Filter, SerfMessage, Tags, UnknownMessageType}
};

/// A delegate for encoding and decoding.
pub trait TransformDelegate: Send + Sync + 'static {
  /// The error type for the transformation.
  type Error: std::error::Error + From<UnknownMessageType> + Send + Sync + 'static;
  /// The Id type.
  type Id: Id;
  /// The Address type.
  type Address: CheapClone + Send + Sync + 'static;

  fn encode_filter(filter: &Filter<Self::Id, Self::Address>) -> Result<Bytes, Self::Error>;

  fn decode_filter(bytes: &[u8]) -> Result<Filter<Self::Id, Self::Address>, Self::Error>;

  fn node_encoded_len(node: &Node<Self::Id, Self::Address>) -> usize;

  fn encode_node(
    node: &Node<Self::Id, Self::Address>,
    dst: &mut [u8],
  ) -> Result<usize, Self::Error>;

  /// Decodes [`Node`] from the given bytes, returning the number of bytes consumed and the node.
  fn decode_node(bytes: impl AsRef<[u8]>) -> Result<(usize, Node<Self::Id, Self::Address>), Self::Error>;

  fn id_encoded_len(id: &Self::Id) -> usize;

  fn encode_id(id: &Self::Id, dst: &mut [u8]) -> Result<usize, Self::Error>;

  fn decode_id(bytes: &[u8]) -> Result<(usize, Self::Id), Self::Error>;

  fn address_encoded_len(address: &Self::Address) -> usize;

  fn encode_address(address: &Self::Address, dst: &mut [u8]) -> Result<usize, Self::Error>;

  fn decode_address(bytes: &[u8]) -> Result<(usize, Self::Address), Self::Error>;

  fn cooradinate_encoded_len(coordinate: &Coordinate) -> usize;

  fn encode_coordinate(coordinate: &Coordinate, dst: &mut [u8]) -> Result<usize, Self::Error>;

  fn decode_coordinate(bytes: &[u8]) -> Result<Coordinate, Self::Error>;

  fn tags_encoded_len(tags: &Tags) -> usize;

  fn encode_tags(tags: &Tags, dst: &mut [u8]) -> Result<usize, Self::Error>;

  fn decode_tags(bytes: &[u8]) -> Result<Tags, Self::Error>;

  fn message_encoded_len(msg: impl AsMessageRef<Self::Id, Self::Address>) -> usize;

  /// Encodes the message into the given buffer, returning the number of bytes written.
  /// 
  /// **NOTE**:
  /// 
  /// 1. The buffer must be large enough to hold the encoded message.
  /// The length of the buffer can be obtained by calling [`TransformDelegate::message_encoded_len`].
  /// 2. A message type byte will be automatically prepended to the buffer,
  /// so users do not need to encode the message type byte by themselves.
  fn encode_message(
    msg: impl AsMessageRef<Self::Id, Self::Address>,
    dst: impl AsMut<[u8]>,
  ) -> Result<usize, Self::Error>; 

  fn decode_message(
    bytes: impl AsRef<[u8]>,
  ) -> Result<SerfMessage<Self::Id, Self::Address>, Self::Error>;
}

/// The error type for the LPE transformation.
pub enum LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  /// Id transformation error.
  Id(<I as Transformable>::Error),
  /// Address transformation error.
  Address(<A as Transformable>::Error),
  /// Node transformation error.
  Node(<Node<I, A> as Transformable>::Error),
  /// Unknown message type error.
  UnknownMessage(UnknownMessageType),
}

impl<I, A> core::fmt::Debug for LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Id(err) => write!(f, "{err:?}"),
      Self::Address(err) => write!(f, "{err:?}"),
      Self::Node(err) => write!(f, "{err:?}"),
      Self::UnknownMessage(err) => write!(f, "{err:?}"),
    }
  }
}

impl<I, A> core::fmt::Display for LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    match self {
      Self::Id(err) => write!(f, "{err}"),
      Self::Address(err) => write!(f, "{err}"),
      Self::Node(err) => write!(f, "{err}"),
      Self::UnknownMessage(err) => write!(f, "{err}"),
    }
  }
}

impl<I, A> std::error::Error for LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
}

impl<I, A> From<UnknownMessageType> for LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn from(err: UnknownMessageType) -> Self {
    Self::UnknownMessage(err)
  }
}

pub struct LpeTransfromDelegate<I, A>(std::marker::PhantomData<(I, A)>);

impl<I, A> Default for LpeTransfromDelegate<I, A> {
  fn default() -> Self {
    Self(Default::default())
  }
}

impl<I, A> Clone for LpeTransfromDelegate<I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for LpeTransfromDelegate<I, A> {}

impl<I, A> TransformDelegate for LpeTransfromDelegate<I, A>
where
  I: Id,
  A: Transformable + CheapClone + Send + Sync + 'static,
{
  type Error = LpeTransformError<Self::Id, Self::Address>;
  type Id = I;
  type Address = A;

  fn encode_filter(filter: &Filter<Self::Id, Self::Address>) -> Result<Bytes, Self::Error> {
    todo!()
  }

  fn decode_filter(bytes: &[u8]) -> Result<Filter<Self::Id, Self::Address>, Self::Error> {
    todo!()
  }

  fn node_encoded_len(node: &Node<Self::Id, Self::Address>) -> usize {
    Transformable::encoded_len(node)
  }

  fn encode_node(
    node: &Node<Self::Id, Self::Address>,
    dst: &mut [u8],
  ) -> Result<usize, Self::Error> {
    Transformable::encode(node, dst).map_err(Self::Error::Node)
  }

  fn decode_node(bytes: impl AsRef<[u8]>) -> Result<(usize, Node<Self::Id, Self::Address>), Self::Error> {
    Transformable::decode(bytes.as_ref()).map_err(Self::Error::Node)
  }

  fn id_encoded_len(id: &Self::Id) -> usize {
    Transformable::encoded_len(id)
  }

  fn encode_id(id: &Self::Id, dst: &mut [u8]) -> Result<usize, Self::Error> {
    Transformable::encode(id, dst).map_err(Self::Error::Id)
  }

  fn decode_id(bytes: &[u8]) -> Result<(usize, Self::Id), Self::Error> {
    Transformable::decode(bytes).map_err(Self::Error::Id)
  }

  fn address_encoded_len(address: &Self::Address) -> usize {
    Transformable::encoded_len(address)
  }

  fn encode_address(address: &Self::Address, dst: &mut [u8]) -> Result<usize, Self::Error> {
    Transformable::encode(address, dst).map_err(Self::Error::Address)
  }

  fn decode_address(bytes: &[u8]) -> Result<(usize, Self::Address), Self::Error> {
    Transformable::decode(bytes).map_err(Self::Error::Address)
  }

  fn cooradinate_encoded_len(coordinate: &Coordinate) -> usize {
    todo!()
  }

  fn encode_coordinate(coordinate: &Coordinate, dst: &mut [u8]) -> Result<usize, Self::Error> {
    todo!()
  }

  fn decode_coordinate(bytes: &[u8]) -> Result<Coordinate, Self::Error> {
    todo!()
  }

  fn tags_encoded_len(tags: &Tags) -> usize {
    todo!()
  }

  fn encode_tags(tags: &Tags, dst: &mut [u8]) -> Result<usize, Self::Error> {
    todo!()
  }

  fn decode_tags(bytes: &[u8]) -> Result<Tags, Self::Error> {
    todo!()
  }

  fn message_encoded_len(msg: impl AsMessageRef<Self::Id, Self::Address>) -> usize {
    todo!()
  }

  fn encode_message(
    msg: impl AsMessageRef<Self::Id, Self::Address>,
    dst: impl AsMut<[u8]>,
  ) -> Result<usize, Self::Error> {
    todo!()
  }

  fn decode_message(
    bytes: impl AsRef<[u8]>,
  ) -> Result<SerfMessage<Self::Id, Self::Address>, Self::Error> {
    todo!()
  }
}
