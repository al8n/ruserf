use byteorder::{ByteOrder, NetworkEndian};

use super::{LamportTime, LamportTimeTransformError, Transformable};

/// The message broadcasted to signal the intentional to
/// leave.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaveMessage<I> {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// The id of the node
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the node")),
    setter(attrs(doc = "Sets the node (Builder pattern)"))
  )]
  id: I,

  /// If prune or not
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns if prune or not")),
    setter(attrs(doc = "Sets prune or not (Builder pattern)"))
  )]
  prune: bool,
}

/// Error that can occur when transforming a [`LeaveMessage`].
#[derive(thiserror::Error)]
pub enum LeaveMessageTransformError<I: Transformable> {
  /// Not enough bytes to decode LeaveMessage
  #[error("not enough bytes to decode LeaveMessage")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  EncodeBufferTooSmall,
  /// Error transforming Node
  #[error(transparent)]
  Id(I::Error),
  /// Error transforming LamportTime
  #[error(transparent)]
  LamportTime(#[from] LamportTimeTransformError),
}

impl<I: Transformable> core::fmt::Debug for LeaveMessageTransformError<I> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<I> Transformable for LeaveMessage<I>
where
  I: Transformable,
{
  type Error = LeaveMessageTransformError<I>;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::EncodeBufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += 4;
    dst[offset] = self.prune as u8;
    offset += 1;
    offset += self.ltime.encode(&mut dst[offset..])?;
    offset += self
      .id
      .encode(&mut dst[offset..])
      .map_err(Self::Error::Id)?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + 1 + self.id.encoded_len() + self.ltime.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.len() < 5 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = NetworkEndian::read_u32(&src[0..4]) as usize;
    if src.len() + 5 < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 4;
    let prune = src[offset] != 0;
    offset += 1;

    let (read, ltime) = LamportTime::decode(&src[offset..])?;
    offset += read;

    let (read, id) = I::decode(&src[offset..]).map_err(Self::Error::Id)?;
    offset += read;

    debug_assert_eq!(
      offset, len,
      "expect read {} bytes, but actual read {} bytes",
      len, offset
    );

    Ok((offset, Self { ltime, id, prune }))
  }
}

#[cfg(test)]
mod tests {

  use rand::{distributions::Alphanumeric, thread_rng, Rng};
  use smol_str::SmolStr;

  use super::*;

  impl LeaveMessage<SmolStr> {
    fn random(size: usize) -> Self {
      let id = thread_rng()
        .sample_iter(Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let id = String::from_utf8(id).unwrap().into();

      Self {
        ltime: LamportTime::random(),
        id,
        prune: thread_rng().gen(),
      }
    }
  }

  #[test]
  fn test_leave_message_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let filter = LeaveMessage::random(i);
        let mut buf = vec![0; filter.encoded_len()];
        let encoded_len = filter.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, filter.encoded_len());

        let (decoded_len, decoded) = LeaveMessage::<SmolStr>::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          LeaveMessage::<SmolStr>::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);

        let (decoded_len, decoded) =
          LeaveMessage::<SmolStr>::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, filter);
      }
    });
  }
}
