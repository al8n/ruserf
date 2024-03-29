use atomic::Atomic;
use memberlist_types::{DelegateVersion, Node, ProtocolVersion};

use super::Tags;

/// The member status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, bytemuck::NoUninit)]
#[repr(u8)]
pub enum MemberStatus {
  /// None status
  None = 0,
  /// Alive status
  Alive = 1,
  /// Leaving status
  Leaving = 2,
  /// Left status
  Left = 3,
  /// Failed status
  Failed = 4,
}

impl core::fmt::Display for MemberStatus {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.as_str())
  }
}

impl MemberStatus {
  /// Get the string representation of the member status
  #[inline]
  pub const fn as_str(&self) -> &'static str {
    match self {
      Self::None => "none",
      Self::Alive => "alive",
      Self::Leaving => "leaving",
      Self::Left => "left",
      Self::Failed => "failed",
    }
  }
}

#[cfg(feature = "serde")]
mod serde_member_status {
  use super::*;

  pub fn serialize<S>(status: &Atomic<MemberStatus>, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::ser::Serializer,
  {
    let status = status.load(atomic::Ordering::Relaxed);
    serializer.serialize_u8(status as u8)
  }

  pub fn deserialize<'de, D>(deserializer: D) -> Result<Atomic<MemberStatus>, D::Error>
  where
    D: serde::de::Deserializer<'de>,
  {
    let status = <u8 as serde::Deserialize>::deserialize(deserializer)?;
    let status = match status {
      0 => MemberStatus::None,
      1 => MemberStatus::Alive,
      2 => MemberStatus::Leaving,
      3 => MemberStatus::Left,
      4 => MemberStatus::Failed,
      _ => {
        return Err(serde::de::Error::custom(format!(
          "invalid member status: {}",
          status
        )));
      }
    };
    Ok(Atomic::new(status))
  }
}

/// A single member of the Serf cluster.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Member<I, A> {
  /// The node
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the node")),
    setter(attrs(doc = "Sets the node (Builder pattern)"))
  )]
  node: Node<I, A>,
  /// The tags
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the tags")),
    setter(attrs(doc = "Sets the tags (Builder pattern)"))
  )]
  tags: Tags,
  #[cfg_attr(feature = "serde", serde(with = "serde_member_status"))]
  /// The status
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the status")),
    setter(attrs(doc = "Sets the status (Builder pattern)"))
  )]
  status: Atomic<MemberStatus>,
  /// The protocol version
  #[viewit(
    getter(const, attrs(doc = "Returns the protocol version")),
    setter(const, attrs(doc = "Sets the protocol version (Builder pattern)"))
  )]
  protocol_version: ProtocolVersion,
  /// The delegate version
  #[viewit(
    getter(const, attrs(doc = "Returns the delegate version")),
    setter(const, attrs(doc = "Sets the delegate version (Builder pattern)"))
  )]
  delegate_version: DelegateVersion,
}
