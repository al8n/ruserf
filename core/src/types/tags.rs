use smol_str::SmolStr;
use std::collections::HashMap;

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct Tag {
  tag: SmolStr,
  expr: SmolStr,
}

#[viewit::viewit]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub(crate) struct TagRef<'a> {
  tag: &'a str,
  expr: &'a str,
}

/// Tags of a node
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct Tags(HashMap<SmolStr, SmolStr>);

impl core::ops::Deref for Tags {
  type Target = HashMap<SmolStr, SmolStr>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

impl core::ops::DerefMut for Tags {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.0
  }
}

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
  pub fn new() -> Self {
    Self(HashMap::new())
  }

  pub fn with_capacity(cap: usize) -> Self {
    Self(HashMap::with_capacity(cap))
  }
}
