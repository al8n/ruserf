use either::Either;
use memberlist_core::{
  transport::{Id, Node},
  types::TinyVec,
  CheapClone,
};

use crate::{coordinate::Coordinate, AsMessageRef, Member, SerfMessage, SerfRelayMessage, Tags};

use super::{
  DefaultMergeDelegate, Delegate, LpeTransfromDelegate, MergeDelegate, NoopReconnectDelegate,
  ReconnectDelegate, TransformDelegate,
};

pub struct CompositeDelegate<
  I,
  A,
  M = DefaultMergeDelegate<I, A>,
  R = NoopReconnectDelegate<I, A>,
  T = LpeTransfromDelegate<I, A>,
> {
  merge: M,
  reconnect: R,
  transform: T,
  _m: std::marker::PhantomData<(I, A)>,
}

impl<I, A> Default for CompositeDelegate<I, A> {
  fn default() -> Self {
    Self::new()
  }
}

impl<I, A> CompositeDelegate<I, A> {
  pub fn new() -> Self {
    Self {
      merge: Default::default(),
      reconnect: Default::default(),
      transform: Default::default(),
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, A, M, R, T> CompositeDelegate<I, A, M, R, T>
where
  M: MergeDelegate<Id = I, Address = A>,
{
  pub fn with_merge_delegate<NM>(self, merge: NM) -> CompositeDelegate<I, A, NM, R, T> {
    Self {
      merge,
      reconnect: self.reconnect,
      transform: self.transform,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, A, M, R, T> CompositeDelegate<I, A, M, R, T> {
  pub fn with_reconnect_delegate<NR>(self, reconnect: NR) -> CompositeDelegate<I, A, M, NR, T> {
    Self {
      merge: self.merge,
      reconnect,
      transform: self.transform,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, A, M, R, T> CompositeDelegate<I, A, M, R, T> {
  pub fn with_transform_delegate<NT>(self, transform: NT) -> CompositeDelegate<I, A, M, R, NT> {
    Self {
      merge: self.merge,
      reconnect: self.reconnect,
      transform,
      _m: std::marker::PhantomData,
    }
  }
}

impl<I, A, M, R, T> MergeDelegate for CompositeDelegate<I, A, M, R, T>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
  M: MergeDelegate<Id = I, Address = A>,
  R: Send + Sync + 'static,
  T: Send + Sync + 'static,
{
  type Error = M::Error;

  type Id = M::Id;

  type Address = M::Address;

  async fn notify_merge(
    &self,
    members: TinyVec<Member<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    self.merge.notify_merge(members).await
  }
}

impl<I, A, M, R, T> ReconnectDelegate for CompositeDelegate<I, A, M, R, T>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
  R: ReconnectDelegate<Id = I, Address = A>,
{
  type Id = R::Id;

  type Address = R::Address;

  fn reconnect_timeout(
    &self,
    member: &Member<Self::Id, Self::Address>,
    timeout: std::time::Duration,
  ) -> std::time::Duration {
    self.reconnect.reconnect_timeout(member, timeout)
  }
}

impl<I, A, M, R, T> TransformDelegate for CompositeDelegate<I, A, M, R, T>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
  T: TransformDelegate<Id = I, Address = A>,
{
  type Error = T::Error;

  type Id = T::Id;

  type Address = T::Address;

  fn encode_filter(
    filter: &crate::Filter<Self::Id, Self::Address>,
  ) -> Result<memberlist_core::bytes::Bytes, Self::Error> {
    T::encode_filter(filter)
  }

  fn decode_filter(bytes: &[u8]) -> Result<crate::Filter<Self::Id, Self::Address>, Self::Error> {
    T::decode_filter(bytes)
  }

  fn node_encoded_len(node: &Node<Self::Id, Self::Address>) -> usize {
    T::node_encoded_len(node)
  }

  fn encode_node(
    node: &Node<Self::Id, Self::Address>,
    dst: &mut [u8],
  ) -> Result<usize, Self::Error> {
    T::encode_node(node, dst)
  }

  fn decode_node(
    bytes: impl AsRef<[u8]>,
  ) -> Result<(usize, Node<Self::Id, Self::Address>), Self::Error> {
    T::decode_node(bytes)
  }

  fn id_encoded_len(id: &Self::Id) -> usize {
    T::id_encoded_len(id)
  }

  fn encode_id(id: &Self::Id, dst: &mut [u8]) -> Result<usize, Self::Error> {
    T::encode_id(id, dst)
  }

  fn decode_id(bytes: &[u8]) -> Result<Self::Id, Self::Error> {
    T::decode_id(bytes)
  }

  fn address_encoded_len(address: &Self::Address) -> usize {
    T::address_encoded_len(address)
  }

  fn encode_address(address: &Self::Address, dst: &mut [u8]) -> Result<usize, Self::Error> {
    T::encode_address(address, dst)
  }

  fn decode_address(bytes: &[u8]) -> Result<Self::Address, Self::Error> {
    T::decode_address(bytes)
  }

  fn cooradinate_encoded_len(coordinate: &Coordinate) -> usize {
    T::cooradinate_encoded_len(coordinate)
  }

  fn encode_coordinate(coordinate: &Coordinate, dst: &mut [u8]) -> Result<usize, Self::Error> {
    T::encode_coordinate(coordinate, dst)
  }

  fn decode_coordinate(bytes: &[u8]) -> Result<Coordinate, Self::Error> {
    T::decode_coordinate(bytes)
  }

  fn tags_encoded_len(tags: &crate::Tags) -> usize {
    T::tags_encoded_len(tags)
  }

  fn encode_tags(tags: &crate::Tags, dst: &mut [u8]) -> Result<usize, Self::Error> {
    T::encode_tags(tags, dst)
  }

  fn decode_tags(bytes: &[u8]) -> Result<Tags, Self::Error> {
    T::decode_tags(bytes)
  }

  fn message_encoded_len(msg: impl AsMessageRef<Self::Id, Self::Address>) -> usize {
    T::message_encoded_len(msg)
  }

  fn encode_message(
    msg: impl AsMessageRef<Self::Id, Self::Address>,
    dst: &mut [u8],
  ) -> Result<usize, Self::Error> {
    T::encode_message(msg, dst)
  }

  fn check_message_type(msg: impl AsRef<[u8]>) -> Option<crate::MessageType> {
    T::check_message_type(msg)
  }

  fn decode_message(
    bytes: impl AsRef<[u8]>,
  ) -> Result<SerfMessage<Self::Id, Self::Address>, Self::Error> {
    T::decode_message(bytes)
  }
}

impl<I, A, M, R, T> Delegate for CompositeDelegate<I, A, M, R, T>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
  M: MergeDelegate<Id = I, Address = A>,
  R: ReconnectDelegate<Id = I, Address = A>,
  T: TransformDelegate<Id = I, Address = A>,
{
  type Id = I;

  type Address = A;
}
