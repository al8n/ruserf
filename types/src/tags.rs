use smol_str::SmolStr;
use std::collections::HashMap;

/// Tags of a node
#[derive(
  Debug, Default, derive_more::From, derive_more::Into, derive_more::Deref, derive_more::DerefMut,
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Tags(HashMap<SmolStr, SmolStr>);

impl IntoIterator for Tags {
  type Item = (SmolStr, SmolStr);
  type IntoIter = std::collections::hash_map::IntoIter<SmolStr, SmolStr>;

  fn into_iter(self) -> Self::IntoIter {
    self.0.into_iter()
  }
}

impl FromIterator<(SmolStr, SmolStr)> for Tags {
  fn from_iter<T: IntoIterator<Item = (SmolStr, SmolStr)>>(iter: T) -> Self {
    Self(iter.into_iter().collect())
  }
}

impl Tags {
  /// Create a new Tags
  #[inline]
  pub fn new() -> Self {
    Self(HashMap::new())
  }

  /// Create a new Tags with a capacity
  pub fn with_capacity(cap: usize) -> Self {
    Self(HashMap::with_capacity(cap))
  }
}
