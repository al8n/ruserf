use byteorder::{ByteOrder, NetworkEndian};
use memberlist_types::{Node, NodeTransformError, TinyVec};
use smol_str::SmolStr;
use transformable::StringTransformError;

use super::Transformable;

/// Transform error type for [`Filter`]
#[derive(thiserror::Error)]
pub enum FilterTransformError<I: Transformable, A: Transformable> {
  /// Returned when there are not enough bytes to decode
  #[error("not enough bytes to decode")]
  NotEnoughBytes(usize),
  /// Returned when the buffer is too small to encode
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Returned when there is an error decoding a node
  #[error(transparent)]
  Node(#[from] NodeTransformError<I, A>),
  /// Returned when there is an error decoding a tag
  #[error(transparent)]
  Tag(#[from] StringTransformError),
  /// Returned when there is an error decoding
  #[error("not enough nodes, expected {expected} nodes, got {got} nodes")]
  NotEnoughNodes {
    /// expected number of nodes
    expected: usize,
    /// got number of nodes
    got: usize,
  },
  /// Returned when there is an unknown filter type
  #[error("unknown filter type: {0}")]
  UnknownFilterType(u8),
}

impl<I: Transformable, A: Transformable> core::fmt::Debug for FilterTransformError<I, A> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

/// Used with a queryFilter to specify the type of
/// filter we are sending
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Filter<I, A> {
  /// Filter by nodes
  Node(TinyVec<Node<I, A>>),
  /// Filter by tag
  Tag {
    /// The tag to filter by
    tag: SmolStr,
    /// The expression to filter by
    expr: SmolStr,
  },
}

impl<I, A> Transformable for Filter<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = FilterTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..offset + 4], encoded_len as u32);
    offset += 4;
    match self {
      Self::Node(nodes) => {
        dst[offset] = 0;
        offset += 1;
        let len = nodes.len() as u32;
        NetworkEndian::write_u32(&mut dst[offset..offset + 4], len);
        offset += 4;
        for node in nodes.iter() {
          offset += node.encode(&mut dst[offset..])?;
        }
        Ok(offset)
      }
      Self::Tag { tag, expr } => {
        dst[offset] = 1;
        offset += 1;
        offset += tag.encode(&mut dst[offset..])?;
        offset += expr.encode(&mut dst[offset..])?;
        Ok(offset)
      }
    }
  }

  fn encoded_len(&self) -> usize {
    4 + match self {
      Self::Node(nodes) => 1 + 4 + nodes.iter().map(Transformable::encoded_len).sum::<usize>(),
      Self::Tag { tag, expr } => 1 + tag.encoded_len() + expr.encoded_len(),
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < 5 {
      return Err(Self::Error::NotEnoughBytes(5));
    }

    let len = NetworkEndian::read_u32(&src[0..4]) as usize;
    if src_len < len {
      return Err(Self::Error::NotEnoughBytes(len));
    }

    let ty = src[4];
    let mut offset = 5;
    match ty {
      0 => {
        let total_nodes = NetworkEndian::read_u32(&src[offset..offset + 4]) as usize;
        offset += 4;
        let mut nodes = TinyVec::with_capacity(total_nodes);
        for _ in 0..total_nodes {
          let (n, node) = Node::decode(&src[offset..])?;
          nodes.push(node);
          offset += n;
        }

        debug_assert_eq!(
          len, offset,
          "expected read {} bytes, but actual read {} bytes",
          len, offset
        );

        Ok((offset, Self::Node(nodes)))
      }
      1 => {
        let (n, tag) = SmolStr::decode(&src[offset..])?;
        offset += n;
        let (n, expr) = SmolStr::decode(&src[offset..])?;
        offset += n;

        debug_assert_eq!(
          len, offset,
          "expected read {} bytes, but actual read {} bytes",
          len, offset
        );

        Ok((offset, Self::Tag { tag, expr }))
      }
      other => Err(Self::Error::UnknownFilterType(other)),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use rand::{distributions::Alphanumeric, thread_rng, Rng};

  use super::*;

  impl Filter<SmolStr, SocketAddr> {
    fn random_node(size: usize, num_nodes: usize) -> Self {
      let mut nodes = TinyVec::with_capacity(num_nodes);

      for _ in 0..num_nodes {
        let id = thread_rng()
          .sample_iter(Alphanumeric)
          .take(size)
          .collect::<Vec<u8>>();
        let id = String::from_utf8(id).unwrap().into();
        let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
        nodes.push(Node::new(id, addr));
      }
      Self::Node(nodes)
    }

    fn random_tag(size: usize) -> Self {
      let rng = rand::thread_rng();
      let tag = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let tag = String::from_utf8(tag).unwrap();
      let rng = rand::thread_rng();
      let expr = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let expr = String::from_utf8(expr).unwrap();
      Self::Tag {
        tag: tag.into(),
        expr: expr.into(),
      }
    }
  }

  #[test]
  fn test_transfrom_encode_decode() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let filter = Filter::random_tag(i);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) = Filter::<SmolStr, SocketAddr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          Filter::<SmolStr, SocketAddr>::decode_from_reader(&mut std::io::Cursor::new(&buf))
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) = Filter::<SmolStr, SocketAddr>::decode_from_async_reader(
          &mut futures::io::Cursor::new(&buf),
        )
        .await
        .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);
      }

      for i in 0..100 {
        let filter = Filter::random_node(i, i % 10);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) = Filter::<SmolStr, SocketAddr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          Filter::<SmolStr, SocketAddr>::decode_from_reader(&mut std::io::Cursor::new(&buf))
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) = Filter::<SmolStr, SocketAddr>::decode_from_async_reader(
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
