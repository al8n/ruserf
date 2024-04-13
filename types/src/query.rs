use byteorder::{ByteOrder, NetworkEndian};
use smol_str::SmolStr;
use transformable::{
  BytesTransformError, DurationTransformError, StringTransformError, Transformable,
};

use std::time::Duration;

use memberlist_types::{bytes::Bytes, Node, NodeTransformError, TinyVec};

use super::{LamportTime, LamportTimeTransformError};

bitflags::bitflags! {
  /// Flags for query message
  #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
  #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
  #[cfg_attr(feature = "serde", serde(transparent))]
  pub struct QueryFlag: u32 {
    /// Ack flag is used to force receiver to send an ack back
    const ACK = 1 << 0;
    /// NoBroadcast is used to prevent re-broadcast of a query.
    /// this can be used to selectively send queries to individual members
    const NO_BROADCAST = 1 << 1;
  }
}

/// Query message
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryMessage<I, A> {
  /// Event lamport time
  #[viewit(
    getter(const, style = "move", attrs(doc = "Returns the event lamport time")),
    setter(const, attrs(doc = "Sets the event lamport time (Builder pattern)"))
  )]
  ltime: LamportTime,
  /// query id, randomly generated
  #[viewit(
    getter(const, style = "move", attrs(doc = "Returns the query id")),
    setter(attrs(doc = "Sets the query id (Builder pattern)"))
  )]
  id: u32,
  /// source node
  #[viewit(
    getter(const, attrs(doc = "Returns the from node")),
    setter(attrs(doc = "Sets the from node (Builder pattern)"))
  )]
  from: Node<I, A>,
  /// Potential query filters
  #[viewit(
    getter(const, attrs(doc = "Returns the potential query filters")),
    setter(attrs(doc = "Sets the potential query filters (Builder pattern)"))
  )]
  filters: TinyVec<Bytes>,
  /// Used to provide various flags
  #[viewit(
    getter(const, style = "move", attrs(doc = "Returns the flags")),
    setter(attrs(doc = "Sets the flags (Builder pattern)"))
  )]
  flags: QueryFlag,
  /// Used to set the number of duplicate relayed responses
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the number of duplicate relayed responses")
    ),
    setter(attrs(doc = "Sets the number of duplicate relayed responses (Builder pattern)"))
  )]
  relay_factor: u8,
  /// Maximum time between delivery and response
  #[viewit(
    getter(
      const,
      style = "move",
      attrs(doc = "Returns the maximum time between delivery and response")
    ),
    setter(attrs(doc = "Sets the maximum time between delivery and response (Builder pattern)"))
  )]
  timeout: Duration,
  /// Query nqme
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the name of the query")),
    setter(attrs(doc = "Sets the name of the query (Builder pattern)"))
  )]
  name: SmolStr,
  /// Query payload
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the payload")),
    setter(attrs(doc = "Sets the payload (Builder pattern)"))
  )]
  payload: Bytes,
}

impl<I, A> QueryMessage<I, A> {
  /// Checks if the ack flag is set
  #[inline]
  pub fn ack(&self) -> bool {
    self.flags.contains(QueryFlag::ACK)
  }

  /// Checks if the no broadcast flag is set
  #[inline]
  pub fn no_broadcast(&self) -> bool {
    self.flags.contains(QueryFlag::NO_BROADCAST)
  }
}

/// Error that can occur when transforming a [`QueryMessage`].
#[derive(thiserror::Error)]
pub enum QueryMessageTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  /// Not enough bytes to decode QueryMessage
  #[error("not enough bytes to decode QueryMessage")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Error transforming `from` field
  #[error(transparent)]
  From(#[from] NodeTransformError<I, A>),
  /// Error transforming `ltime` field
  #[error(transparent)]
  LamportTime(#[from] LamportTimeTransformError),
  /// Error transforming `payload` field
  #[error(transparent)]
  Payload(BytesTransformError),

  /// Error transforming `filters` field
  #[error(transparent)]
  Filters(BytesTransformError),

  /// Error transforming `name` field
  #[error(transparent)]
  Name(#[from] StringTransformError),

  /// Error transforming `timeout` field
  #[error(transparent)]
  Timeout(#[from] DurationTransformError),
}

impl<I, A> core::fmt::Debug for QueryMessageTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<I, A> Transformable for QueryMessage<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = QueryMessageTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += 4;
    offset += self.ltime.encode(&mut dst[offset..])?;
    NetworkEndian::write_u32(&mut dst[offset..], self.id);
    offset += 4;
    offset += self.from.encode(&mut dst[offset..])?;
    NetworkEndian::write_u32(&mut dst[offset..], self.filters.len() as u32);
    offset += 4;
    for filter in self.filters.iter() {
      offset += filter
        .encode(&mut dst[offset..])
        .map_err(Self::Error::Filters)?;
    }
    NetworkEndian::write_u32(&mut dst[offset..], self.flags.bits());
    offset += 4;
    dst[offset] = self.relay_factor;
    offset += 1;
    offset += self.timeout.encode(&mut dst[offset..])?;
    offset += self.name.encode(&mut dst[offset..])?;
    offset += self
      .payload
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Payload)?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + self.ltime.encoded_len()
      + 4 // id
      + self.from.encoded_len()
      + 4 // num filters
      + self.filters.iter().map(|f| f.encoded_len()).sum::<usize>()
      + 4 // flags
      + 1 // relay_factor
      + self.timeout.encoded_len()
      + self.name.encoded_len()
      + self.payload.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src.len() < 4 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 0;
    let len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if src.len() < len {
      return Err(Self::Error::NotEnoughBytes);
    }
    offset += 4;

    let (n, ltime) = LamportTime::decode(&src[offset..])?;
    offset += n;

    if offset + 4 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let id = NetworkEndian::read_u32(&src[offset..]);
    offset += 4;

    let (n, from) = Node::decode(&src[offset..])?;
    offset += n;

    if offset + 4 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let num_filters = NetworkEndian::read_u32(&src[offset..]) as usize;
    offset += 4;

    let mut filters = TinyVec::with_capacity(num_filters);
    for _ in 0..num_filters {
      let (n, filter) = Bytes::decode(&src[offset..]).map_err(Self::Error::Filters)?;
      filters.push(filter);
      offset += n;
    }

    if offset + 4 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let flags = QueryFlag::from_bits_retain(NetworkEndian::read_u32(&src[offset..]));
    offset += 4;

    if offset + 1 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let relay_factor = src[offset];
    offset += 1;

    let (n, timeout) = Duration::decode(&src[offset..])?;
    offset += n;

    let (n, name) = SmolStr::decode(&src[offset..])?;
    offset += n;

    let (n, payload) = Bytes::decode(&src[offset..]).map_err(Self::Error::Payload)?;
    offset += n;

    debug_assert_eq!(
      offset, len,
      "expect read {} bytes, but actual read {} bytes",
      len, offset
    );

    Ok((
      offset,
      Self {
        ltime,
        id,
        from,
        filters,
        flags,
        relay_factor,
        timeout,
        name,
        payload,
      },
    ))
  }
}

/// Query response message
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryResponseMessage<I, A> {
  /// Event lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// query id
  #[viewit(
    getter(const, attrs(doc = "Returns the query id")),
    setter(attrs(doc = "Sets the query id (Builder pattern)"))
  )]
  id: u32,
  /// node
  #[viewit(
    getter(const, attrs(doc = "Returns the from node")),
    setter(attrs(doc = "Sets the from node (Builder pattern)"))
  )]
  from: Node<I, A>,
  /// Used to provide various flags
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the flags")),
    setter(attrs(doc = "Sets the flags (Builder pattern)"))
  )]
  flags: QueryFlag,
  /// Optional response payload
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the payload")),
    setter(attrs(doc = "Sets the payload (Builder pattern)"))
  )]
  payload: Bytes,
}

impl<I, A> QueryResponseMessage<I, A> {
  /// Checks if the ack flag is set
  #[inline]
  pub fn ack(&self) -> bool {
    self.flags.contains(QueryFlag::ACK)
  }

  /// Checks if the no broadcast flag is set
  #[inline]
  pub fn no_broadcast(&self) -> bool {
    self.flags.contains(QueryFlag::NO_BROADCAST)
  }
}

/// Error that can occur when transforming a [`QueryResponseMessage`].
#[derive(thiserror::Error)]
pub enum QueryResponseMessageTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  /// Not enough bytes to decode QueryResponseMessage
  #[error("not enough bytes to decode QueryResponseMessage")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Error transforming Node
  #[error(transparent)]
  Node(#[from] NodeTransformError<I, A>),
  /// Error transforming LamportTime
  #[error(transparent)]
  LamportTime(#[from] LamportTimeTransformError),
  /// Error transforming payload
  #[error(transparent)]
  Payload(#[from] BytesTransformError),
}

impl<I, A> core::fmt::Debug for QueryResponseMessageTransformError<I, A>
where
  I: Transformable,
  A: Transformable,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<I, A> Transformable for QueryResponseMessage<I, A>
where
  I: Transformable,
  A: Transformable,
{
  type Error = QueryResponseMessageTransformError<I, A>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += 4;
    offset += self.ltime.encode(&mut dst[offset..])?;
    NetworkEndian::write_u32(&mut dst[offset..], self.id);
    offset += 4;
    offset += self.from.encode(&mut dst[offset..])?;
    NetworkEndian::write_u32(&mut dst[offset..], self.flags.bits());
    offset += 4;
    offset += self.payload.encode(&mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + self.ltime.encoded_len() + 4 + self.from.encoded_len() + 4 + self.payload.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src.len() < 4 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 0;
    let len = NetworkEndian::read_u32(&src[offset..]) as usize;
    if src.len() < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    offset += 4;
    let (n, ltime) = LamportTime::decode(&src[offset..])?;
    offset += n;

    if offset + 4 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }
    let id = NetworkEndian::read_u32(&src[offset..]);
    offset += 4;

    let (n, from) = Node::decode(&src[offset..])?;
    offset += n;

    if offset + 4 > src_len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let flags = QueryFlag::from_bits_retain(NetworkEndian::read_u32(&src[offset..]));
    offset += 4;

    let (n, payload) = Bytes::decode(&src[offset..])?;
    offset += n;

    debug_assert_eq!(
      offset, len,
      "expect read {} bytes, but actual read {} bytes",
      len, offset
    );

    Ok((
      offset,
      Self {
        ltime,
        id,
        from,
        flags,
        payload,
      },
    ))
  }
}

#[cfg(test)]
mod tests {
  use std::net::SocketAddr;

  use rand::{distributions::Alphanumeric, random, thread_rng, Rng};

  use super::*;

  impl QueryMessage<SmolStr, SocketAddr> {
    fn random(size: usize, num_filters: usize) -> Self {
      let ltime = LamportTime::random();
      let id = random();
      let from_id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let from_id = String::from_utf8(from_id).unwrap().into();
      let addr = SocketAddr::from(([127, 0, 0, 1], random::<u16>()));
      let from = Node::new(from_id, addr);
      let filters = (0..num_filters)
        .map(|_| {
          let payload = thread_rng()
            .sample_iter(Alphanumeric)
            .take(size)
            .collect::<Vec<u8>>();
          payload.into()
        })
        .collect();
      let flags = QueryFlag::empty();
      let relay_factor = random();
      let timeout = Duration::from_secs(random::<u64>());
      let name = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let name = SmolStr::from(String::from_utf8(name).unwrap());
      let payload = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let payload = Bytes::from(payload);
      Self {
        ltime,
        id,
        from,
        filters,
        flags,
        relay_factor,
        timeout,
        name,
        payload,
      }
    }
  }

  impl QueryResponseMessage<SmolStr, SocketAddr> {
    fn random(size: usize) -> Self {
      let id = rand::random();

      let from_id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let from_id = String::from_utf8(from_id).unwrap().into();
      let addr = SocketAddr::from(([127, 0, 0, 1], random::<u16>()));
      let from = Node::new(from_id, addr);
      let flags = QueryFlag::empty();
      let payload = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      Self {
        ltime: LamportTime::random(),
        id,
        from,
        flags,
        payload: payload.into(),
      }
    }
  }

  #[test]
  fn test_query_response_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let filter = QueryResponseMessage::random(i);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) =
          QueryResponseMessage::<SmolStr, SocketAddr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          QueryResponseMessage::<SmolStr, SocketAddr>::decode_from_reader(
            &mut std::io::Cursor::new(&buf),
          )
          .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          QueryResponseMessage::<SmolStr, SocketAddr>::decode_from_async_reader(
            &mut futures::io::Cursor::new(&buf),
          )
          .await
          .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);
      }
    });
  }

  #[test]
  fn test_query_message_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let filter = QueryMessage::random(i, i % 10);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) = QueryMessage::<SmolStr, SocketAddr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          QueryMessage::<SmolStr, SocketAddr>::decode_from_reader(&mut std::io::Cursor::new(&buf))
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) = QueryMessage::<SmolStr, SocketAddr>::decode_from_async_reader(
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
