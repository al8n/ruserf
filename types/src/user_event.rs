use byteorder::{ByteOrder, NetworkEndian};
use memberlist_types::{bytes::Bytes, OneOrMore};
use smol_str::SmolStr;
use transformable::{BytesTransformError, StringTransformError, Transformable};

use super::LamportTime;

/// Used to buffer events to prevent re-delivery
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvents {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,

  /// The user events
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the user events")),
    setter(attrs(doc = "Sets the user events (Builder pattern)"))
  )]
  events: OneOrMore<UserEvent>,
}

/// Stores all the user events at a specific time
#[viewit::viewit(getters(style = "ref"), setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEvent {
  /// The name of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the name of the event")),
    setter(attrs(doc = "Sets the name of the event (Builder pattern)"))
  )]
  name: SmolStr,
  /// The payload of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the payload of the event")),
    setter(attrs(doc = "Sets the payload of the event (Builder pattern)"))
  )]
  payload: Bytes,
}

/// Error that can occur when transforming a [`UserEvent`]
#[derive(Debug, thiserror::Error)]
pub enum UserEventTransformError {
  /// Not enough bytes to decode UserEvent
  #[error("not enough bytes to decode `UserEvent`")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,

  /// Error transforming SmolStr
  #[error(transparent)]
  Name(#[from] StringTransformError),

  /// Error transforming Bytes
  #[error(transparent)]
  Payload(#[from] BytesTransformError),
}

impl Transformable for UserEvent {
  type Error = UserEventTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..], encoded_len as u32);
    offset += 4;

    offset += self.name.encode(&mut dst[offset..])?;
    offset += self.payload.encode(&mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, actual read {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + self.name.encoded_len() + self.payload.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < 4 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = NetworkEndian::read_u32(&src[0..4]) as usize;
    if src_len < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 4;
    let (name_offset, name) = SmolStr::decode(&src[offset..])?;
    offset += name_offset;
    let (payload_offset, payload) = Bytes::decode(&src[offset..])?;
    offset += payload_offset;

    debug_assert_eq!(
      offset, len,
      "expect read {} bytes, actual read {} bytes",
      len, offset
    );

    Ok((len, Self { name, payload }))
  }
}

/// Used for user-generated events
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct UserEventMessage {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// The name of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the name of the event")),
    setter(attrs(doc = "Sets the name of the event (Builder pattern)"))
  )]
  name: SmolStr,
  /// The payload of the event
  #[viewit(
    getter(const, attrs(doc = "Returns the payload of the event")),
    setter(attrs(doc = "Sets the payload of the event (Builder pattern)"))
  )]
  payload: Bytes,
  /// "Can Coalesce".
  #[viewit(
    getter(const, attrs(doc = "Returns if this message can be coalesced")),
    setter(
      const,
      attrs(doc = "Sets if this message can be coalesced (Builder pattern)")
    )
  )]
  cc: bool,
}

#[cfg(test)]
mod tests {
  use rand::{distributions::Alphanumeric, Rng};

  use super::*;

  impl UserEvent {
    fn random(size: usize) -> Self {
      let rng = rand::thread_rng();
      let name = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();
      let name = String::from_utf8(name).unwrap();

      let rng = rand::thread_rng();
      let payload = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect::<Vec<u8>>();

      Self {
        name: name.into(),
        payload: payload.into(),
      }
    }
  }

  #[test]
  fn test_user_event_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let event = UserEvent::random(i);
        let mut buf = vec![0; event.encoded_len()];
        let encoded_len = event.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, event.encoded_len());

        let (decoded_len, decoded) = UserEvent::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);

        let (decoded_len, decoded) =
          UserEvent::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);

        let (decoded_len, decoded) =
          UserEvent::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);
      }
    })
  }
}
