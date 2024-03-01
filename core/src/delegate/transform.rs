use memberlist_core::{
  bytes::Bytes,
  transport::{Id, Node, Transformable},
  CheapClone,
};

use crate::Filter;

/// A delegate for encoding and decoding.
pub trait TransformDelegate: Send + Sync + 'static {
  /// The error type for the transformation.
  type Error: std::error::Error + Send + Sync + 'static;
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

  fn decode_node(bytes: &[u8]) -> Result<Node<Self::Id, Self::Address>, Self::Error>;
}

pub enum LpeTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  Node(<Node<I, A> as Transformable>::Error),
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

  fn decode_node(bytes: &[u8]) -> Result<Node<Self::Id, Self::Address>, Self::Error> {
    Transformable::decode(bytes).map_err(Self::Error::Node)
  }
}
