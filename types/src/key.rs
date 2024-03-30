use byteorder::{ByteOrder, NetworkEndian};
use indexmap::IndexMap;
use memberlist_types::{SecretKey, SecretKeyTransformError, SecretKeys, SecretKeysTransformError};
use smol_str::SmolStr;
use transformable::{StringTransformError, Transformable};

/// KeyRequest is used to contain input parameters which get broadcasted to all
/// nodes as part of a key query operation.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct KeyRequestMessage {
  /// The secret key
  #[viewit(
    getter(const, attrs(doc = "Returns the secret key")),
    setter(const, attrs(doc = "Sets the secret key (Builder pattern)"))
  )]
  key: Option<SecretKey>,
}

/// The error that can occur when transforming a [`KeyRequestMessage`]
#[derive(Debug, thiserror::Error)]
pub enum OptionSecretKeyTransformError {
  /// Not enough bytes to decode [`Option<SecretKey>`]
  #[error("not enough bytes to decode")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Error transforming a secret key
  #[error(transparent)]
  SecretKey(#[from] SecretKeyTransformError),
}

impl Transformable for KeyRequestMessage {
  type Error = OptionSecretKeyTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    match &self.key {
      None => {
        dst[0] = 0;
        Ok(1)
      }
      Some(key) => key.encode(dst).map_err(Self::Error::SecretKey),
    }
  }

  fn encoded_len(&self) -> usize {
    match &self.key {
      Some(key) => key.encoded_len(),
      None => 1,
    }
  }

  fn encode_to_writer<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<usize> {
    match &self.key {
      None => {
        writer.write_all(&[0])?;
        Ok(1)
      }
      Some(key) => key.encode_to_writer(writer),
    }
  }

  async fn encode_to_async_writer<W: futures::AsyncWrite + Send + Unpin>(
    &self,
    writer: &mut W,
  ) -> std::io::Result<usize> {
    use futures::AsyncWriteExt;

    match &self.key {
      None => {
        writer.write_all(&[0]).await?;
        Ok(1)
      }
      Some(key) => key.encode_to_async_writer(writer).await,
    }
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    if src.is_empty() {
      return Err(Self::Error::NotEnoughBytes);
    }

    match src[0] {
      0 => Ok((1, Self { key: None })),
      _ => {
        let (n, key) = SecretKey::decode(src).map_err(Self::Error::SecretKey)?;
        Ok((n, Self { key: Some(key) }))
      }
    }
  }

  fn decode_from_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf)?;

    match buf[0] {
      0 => Ok((1, Self { key: None })),
      16 => {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf)?;
        Ok((
          17,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      24 => {
        let mut buf = [0u8; 24];
        reader.read_exact(&mut buf)?;
        Ok((
          25,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      32 => {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok((
          33,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      _ => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "unknown secret key kind",
      )),
    }
  }

  async fn decode_from_async_reader<R: futures::AsyncRead + Send + Unpin>(
    reader: &mut R,
  ) -> std::io::Result<(usize, Self)>
  where
    Self: Sized,
  {
    use futures::AsyncReadExt;

    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).await?;

    match buf[0] {
      0 => Ok((1, Self { key: None })),
      16 => {
        let mut buf = [0u8; 16];
        reader.read_exact(&mut buf).await?;
        Ok((
          17,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      24 => {
        let mut buf = [0u8; 24];
        reader.read_exact(&mut buf).await?;
        Ok((
          25,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      32 => {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf).await?;
        Ok((
          33,
          Self {
            key: Some(SecretKey::from(buf)),
          },
        ))
      }
      _ => Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        "unknown secret key kind",
      )),
    }
  }
}

/// Key response message
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
#[cfg(feature = "encryption")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct KeyResponseMessage {
  /// Indicates true/false if there were errors or not
  #[viewit(
    getter(const, attrs(doc = "Returns true/false if there were errors or not")),
    setter(
      const,
      attrs(doc = "Sets true/false if there were errors or not (Builder pattern)")
    )
  )]
  result: bool,
  /// Contains error messages or other information
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the error messages or other information")
    ),
    setter(attrs(doc = "Sets the error messages or other information (Builder pattern)"))
  )]
  message: SmolStr,
  /// Used in listing queries to relay a list of installed keys
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns a list of installed keys")),
    setter(attrs(doc = "Sets the the list of installed keys (Builder pattern)"))
  )]
  keys: SecretKeys,
  /// Used in listing queries to relay the primary key
  #[viewit(
    getter(const, attrs(doc = "Returns the primary key")),
    setter(attrs(doc = "Sets the primary key (Builder pattern)"))
  )]
  primary_key: Option<SecretKey>,
}

/// Error that can occur when transforming a [`KeyResponseMessage`].
#[derive(Debug, thiserror::Error)]
pub enum KeyResponseMessageTransformError {
  /// Not enough bytes to decode KeyResponseMessage
  #[error("not enough bytes to decode `KeyResponseMessage`")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Error transforming a message field
  #[error(transparent)]
  Message(#[from] StringTransformError),
  /// Error transforming a `primary_key` field
  #[error(transparent)]
  PrimaryKey(#[from] OptionSecretKeyTransformError),
  /// Error transforming a `keys` field
  #[error(transparent)]
  Keys(#[from] SecretKeysTransformError),
}

impl Transformable for KeyResponseMessage {
  type Error = KeyResponseMessageTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }


    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..offset + 4], encoded_len as u32);
    offset += 4;
    dst[offset] = self.result as u8;
    offset += 1;
    offset += self.message.encode(&mut dst[offset..])?;
    offset += self.keys.encode(&mut dst[offset..])?;
    offset += KeyRequestMessage {key: self.primary_key}.encode(&mut dst[offset..])?;

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + 1 + self.message.encoded_len() + self.keys.encoded_len() + KeyRequestMessage {key: self.primary_key}.encoded_len()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < 5 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let mut offset = 0;
    let encoded_len = NetworkEndian::read_u32(&src[offset..offset + 4]) as usize;
    if src_len < encoded_len {
      return Err(Self::Error::NotEnoughBytes);
    }
    offset += 4;

    let result = src[offset] != 0;
    offset += 1;
    let (n, message) = SmolStr::decode(&src[offset..]).map_err(Self::Error::Message)?;
    offset += n;
    let (n, keys) = SecretKeys::decode(&src[offset..]).map_err(Self::Error::Keys)?;
    offset += n;
    let (n, primary_key) = KeyRequestMessage::decode(&src[offset..]).map_err(Self::Error::PrimaryKey)?;
    offset += n;

    debug_assert_eq!(
      offset, encoded_len,
      "expect read {} bytes, but actual read {} bytes",
      encoded_len, offset
    );

    Ok((offset, Self { result, message, keys, primary_key: primary_key.key }))
  }
}

/// KeyResponse is used to relay a query for a list of all keys in use.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Default)]
pub struct KeyResponse<I> {
  /// Map of node id to response message
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(doc = "Returns the map of node id to response message")
    ),
    setter(attrs(doc = "Sets the map of node id to response message (Builder pattern)"))
  )]
  messages: IndexMap<I, SmolStr>,
  /// Total nodes memberlist knows of
  #[viewit(
    getter(const, attrs(doc = "Returns the total nodes memberlist knows of")),
    setter(
      const,
      attrs(doc = "Sets total nodes memberlist knows of (Builder pattern)")
    )
  )]
  num_nodes: usize,
  /// Total responses received
  #[viewit(
    getter(const, attrs(doc = "Returns the total responses received")),
    setter(
      const,
      attrs(doc = "Sets the total responses received (Builder pattern)")
    )
  )]
  num_resp: usize,
  /// Total errors from request
  #[viewit(
    getter(const, attrs(doc = "Returns the total errors from request")),
    setter(
      const,
      attrs(doc = "Sets the total errors from request (Builder pattern)")
    )
  )]
  num_err: usize,

  /// A mapping of the base64-encoded value of the key bytes to the
  /// number of nodes that have the key installed.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(
        doc = "Returns a mapping of the base64-encoded value of the key bytes to the number of nodes that have the key installed."
      )
    ),
    setter(attrs(
      doc = "Sets a mapping of the base64-encoded value of the key bytes to the number of nodes that have the key installed (Builder pattern)"
    ))
  )]
  keys: IndexMap<SecretKey, usize>,

  /// A mapping of the base64-encoded value of the primary
  /// key bytes to the number of nodes that have the key installed.
  #[viewit(
    getter(
      const,
      style = "ref",
      attrs(
        doc = "Returns a mapping of the base64-encoded value of the primary key bytes to the number of nodes that have the key installed."
      )
    ),
    setter(attrs(
      doc = "Sets a mapping of the base64-encoded value of the primary key bytes to the number of nodes that have the key installed. (Builder pattern)"
    ))
  )]
  primary_keys: IndexMap<SecretKey, usize>,
}

/// KeyRequestOptions is used to contain optional parameters for a keyring operation
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyRequestOptions {
  /// The number of duplicate query responses to send by relaying through
  /// other nodes, for redundancy
  pub relay_factor: u8,
}

#[cfg(test)]
mod tests {
  use rand::Rng;

  use super::*;

  impl KeyRequestMessage {
    pub(crate) fn random(kind: u8) -> Self {
      let key = if rand::random() {
        match kind {
          16 => {
            let mut buf = [0u8; 16];
            rand::thread_rng().fill(&mut buf);
            Some(SecretKey::from(buf))
          }
          24 => {
            let mut buf = [0u8; 24];
            rand::thread_rng().fill(&mut buf);
            Some(SecretKey::from(buf))
          }
          32 => {
            let mut buf = [0u8; 32];
            rand::thread_rng().fill(&mut buf);
            Some(SecretKey::from(buf))
          }
          _ => None,
        }
      } else {
        None
      };

      Self { key }
    }
  }

  impl KeyResponseMessage {
    pub(crate) fn random() -> Self {
      let mut keys = SecretKeys::default();
      for _ in 0..rand::thread_rng().gen_range(0..10) {
        let mut buf = [0u8; 32];
        rand::thread_rng().fill(&mut buf);
        keys.insert(SecretKey::from(buf));
      }

      let primary_key = if rand::random() {
        let mut buf = [0u8; 32];
        rand::thread_rng().fill(&mut buf);
        Some(SecretKey::from(buf))
      } else {
        None
      };

      Self {
        result: rand::random(),
        message: SmolStr::random(rand::thread_rng().gen_range(0..100)),
        keys,
        primary_key,
      }
    }
  }

  #[test]
  fn test_key_request_message_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let kind = match i % 4 {
          0 => 0,
          1 => 16,
          2 => 24,
          _ => 32,
        };
        let key = KeyRequestMessage::random(kind);
        let mut buf = vec![0; key.encoded_len()];
        let encoded_len = key.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, key.encoded_len());
        let mut buf1 = vec![];
        let encoded_len1 = key.encode_to_writer(&mut buf1).unwrap();
        assert_eq!(encoded_len1, key.encoded_len());
        let mut buf2 = vec![];
        let encoded_len2 = key.encode_to_async_writer(&mut buf2).await.unwrap();
        assert_eq!(encoded_len2, key.encoded_len());

        let (decoded_len, decoded) = KeyRequestMessage::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) = KeyRequestMessage::decode(&buf1).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) = KeyRequestMessage::decode(&buf2).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);

        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_reader(&mut std::io::Cursor::new(&buf1)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_reader(&mut std::io::Cursor::new(&buf2)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);

        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_async_reader(&mut futures::io::Cursor::new(&buf1))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
        let (decoded_len, decoded) =
          KeyRequestMessage::decode_from_async_reader(&mut futures::io::Cursor::new(&buf2))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, key);
      }
    });
  }
}
