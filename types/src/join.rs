use byteorder::{ByteOrder, NetworkEndian};
use memberlist_types::NodeTransformError;
use transformable::utils::*;

use crate::LamportTimeTransformError;

use super::{LamportTime, Node, Transformable};

/// The message broadcasted after we join to
/// associated the node with a lamport clock
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JoinMessage<I, A> {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// The node
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the node")),
    setter(attrs(doc = "Sets the node (Builder pattern)"))
  )]
  node: Node<I, A>,
}

impl<I, A> JoinMessage<I, A> {
  /// Create a new join message
  pub fn new(ltime: LamportTime, node: Node<I, A>) -> Self {
    Self { ltime, node }
  }

  /// Set the lamport time
  #[inline]
  pub fn set_ltime(&mut self, ltime: LamportTime) -> &mut Self {
    self.ltime = ltime;
    self
  }

  /// Set the node
  #[inline]
  pub fn set_node(&mut self, node: Node<I, A>) -> &mut Self {
    self.node = node;
    self
  }
}

/// Error that can occur when transforming a JoinMessage
#[derive(thiserror::Error)]
pub enum JoinMessageTransformError<I: Transformable, A: Transformable> {
  /// Not enough bytes to decode JoinMessage
  #[error("not enough bytes to decode JoinMessage")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  EncodeBufferTooSmall,
  /// Error transforming Node
  #[error(transparent)]
  Node(#[from] NodeTransformError<I, A>),

  /// Error transforming LamportTime
  #[error(transparent)]
  LamportTime(#[from] LamportTimeTransformError),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for JoinMessageTransformError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<I, A> Transformable for JoinMessage<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = JoinMessageTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..offset + 4], encoded_len as u32);
    offset += 4;

    offset += self.ltime.encode(&mut dst[offset..])?;
    offset += self.node.encode(&mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );
    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + encoded_len_varint(self.ltime.0) + self.node.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < 4 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let encoded_len = NetworkEndian::read_u32(&src[..4]) as usize;
    if src.len() < encoded_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 4;
    let (n, ltime) = LamportTime::decode(&src[offset..])?;
    offset += n;

    let (n, node) = Node::decode(&src[offset..])?;
    offset += n;

    debug_assert_eq!(
      offset, encoded_len,
      "expect read {} bytes, but actual read {} bytes",
      encoded_len, offset
    );
    Ok((encoded_len, Self { ltime, node }))
  }
}

#[cfg(test)]
mod tests {
  use rand::{distributions::Alphanumeric, random, thread_rng, Rng};
  use smol_str::SmolStr;
  use std::net::SocketAddr;

  use super::*;

  impl JoinMessage<SmolStr, SocketAddr> {
    fn random(size: usize) -> Self {
      let id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let id = String::from_utf8(id).unwrap().into();
      let addr = SocketAddr::from(([127, 0, 0, 1], random::<u16>()));

      Self {
        ltime: LamportTime::random(),
        node: Node::new(id, addr),
      }
    }
  }

  #[test]
  fn test_transfrom_encode_decode() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let filter = JoinMessage::random(i);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) = JoinMessage::<SmolStr, SocketAddr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          JoinMessage::<SmolStr, SocketAddr>::decode_from_reader(&mut std::io::Cursor::new(&buf))
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) = JoinMessage::<SmolStr, SocketAddr>::decode_from_async_reader(
          &mut futures::io::Cursor::new(&buf),
        )
        .await
        .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);
      }
    });
  }
}
