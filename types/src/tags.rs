use byteorder::{ByteOrder, NetworkEndian};
use indexmap::IndexMap;
use smol_str::SmolStr;
use transformable::Transformable;

/// Tags of a node
#[derive(
  Debug,
  Default,
  PartialEq,
  Clone,
  derive_more::From,
  derive_more::Into,
  derive_more::Deref,
  derive_more::DerefMut,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Tags(IndexMap<SmolStr, SmolStr>);

impl IntoIterator for Tags {
  type Item = (SmolStr, SmolStr);
  type IntoIter = indexmap::map::IntoIter<SmolStr, SmolStr>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl FromIterator<(SmolStr, SmolStr)> for Tags {
  fn from_iter<T: IntoIterator<Item = (SmolStr, SmolStr)>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl<'a> FromIterator<(&'a str, &'a str)> for Tags {
  fn from_iter<T: IntoIterator<Item = (&'a str, &'a str)>>(iter: T) -> Self {
    Self(
      iter
        .into_iter()
        .map(|(k, v)| (SmolStr::new(k), SmolStr::new(v)))
        .collect(),
    )
  }
}

impl Tags {
  /// Create a new Tags
  #[inline]
  pub fn new() -> Self {
    Self(IndexMap::new())
  }

  /// Create a new Tags with a capacity
  pub fn with_capacity(cap: usize) -> Self {
    Self(IndexMap::with_capacity(cap))
  }
}

/// Error that can occur when transforming [`Tags`].
#[derive(Debug, thiserror::Error)]
pub enum TagsTransformError {
  /// Not enough bytes to decode Tags
  #[error("not enough bytes to decode `Tags`")]
  NotEnoughBytes,
  /// Encode buffer too small
  #[error("encode buffer too small")]
  BufferTooSmall,
  /// Error transforming a string
  #[error(transparent)]
  String(#[from] transformable::StringTransformError),
}

impl Transformable for Tags {
  type Error = TagsTransformError;

  fn encode(&self, dst: &mut [u8]) -> Result<usize, Self::Error> {
    let encoded_len = self.encoded_len();
    if dst.len() < encoded_len {
      return Err(Self::Error::BufferTooSmall);
    }

    let mut offset = 0;
    NetworkEndian::write_u32(&mut dst[offset..offset + 4], encoded_len as u32);
    offset += 4;
    let len = self.0.len() as u32;
    NetworkEndian::write_u32(&mut dst[offset..offset + 4], len);
    offset += 4;
    for (key, value) in self.0.iter() {
      offset += key.encode(&mut dst[offset..])?;
      offset += value.encode(&mut dst[offset..])?;
    }

    debug_assert_eq!(
      offset, encoded_len,
      "expect write {} bytes, but actual write {} bytes",
      encoded_len, offset
    );

    Ok(offset)
  }

  fn encoded_len(&self) -> usize {
    4 + 4
      + self
        .0
        .iter()
        .map(|(key, value)| key.encoded_len() + value.encoded_len())
        .sum::<usize>()
  }

  fn decode(src: &[u8]) -> Result<(usize, Self), Self::Error>
  where
    Self: Sized,
  {
    let src_len = src.len();
    if src_len < 8 {
      return Err(Self::Error::NotEnoughBytes);
    }

    let len = NetworkEndian::read_u32(&src[0..4]) as usize;
    if src_len < len {
      return Err(Self::Error::NotEnoughBytes);
    }

    let total_tags = NetworkEndian::read_u32(&src[4..8]) as usize;
    let mut offset = 8;
    let mut tags = IndexMap::with_capacity(total_tags);
    for _ in 0..total_tags {
      let (n, key) = SmolStr::decode(&src[offset..])?;
      offset += n;
      let (n, value) = SmolStr::decode(&src[offset..])?;
      offset += n;
      tags.insert(key, value);
    }

    debug_assert_eq!(
      len, offset,
      "expected read {} bytes, but actual read {} bytes",
      len, offset
    );

    Ok((offset, Self(tags)))
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use rand::{distributions::Alphanumeric, Rng};

  impl Tags {
    pub(crate) fn random(num_tags: usize, size: usize) -> Self {
      let mut tags = IndexMap::with_capacity(num_tags);
      for _ in 0..num_tags {
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

        tags.insert(name.into(), String::from_utf8(payload).unwrap().into());
      }
      Self(tags)
    }
  }

  #[test]
  fn test_tags_transform() {
    futures::executor::block_on(async {
      for i in 0..100 {
        let event = Tags::random(i % 10, i);
        let mut buf = vec![0; event.encoded_len()];
        let encoded_len = event.encode(&mut buf).unwrap();
        assert_eq!(encoded_len, event.encoded_len());

        let (decoded_len, decoded) = Tags::decode(&buf).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);

        let (decoded_len, decoded) =
          Tags::decode_from_reader(&mut std::io::Cursor::new(&buf)).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);

        let (decoded_len, decoded) =
          Tags::decode_from_async_reader(&mut futures::io::Cursor::new(&buf))
            .await
            .unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded, event);
      }
    });
  }
}
