use std::sync::Arc;

use memberlist_types::CheapClone;

use super::*;

/// The member status.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, bytemuck::NoUninit)]
#[repr(u8)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
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
  tags: Arc<Tags>,
  // #[cfg_attr(feature = "serde", serde(with = "serde_member_status"))]
  /// The status
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the status")),
    setter(attrs(doc = "Sets the status (Builder pattern)"))
  )]
  status: MemberStatus,
  /// The memberlist protocol version
  #[viewit(
    getter(const, attrs(doc = "Returns the memberlist protocol version")),
    setter(
      const,
      attrs(doc = "Sets the memberlist protocol version (Builder pattern)")
    )
  )]
  memberlist_protocol_version: MemberlistProtocolVersion,
  /// The memberlist delegate version
  #[viewit(
    getter(const, attrs(doc = "Returns the memberlist delegate version")),
    setter(
      const,
      attrs(doc = "Sets the memberlist delegate version (Builder pattern)")
    )
  )]
  memberlist_delegate_version: MemberlistDelegateVersion,

  /// The ruserf protocol version
  #[viewit(
    getter(const, attrs(doc = "Returns the ruserf protocol version")),
    setter(
      const,
      attrs(doc = "Sets the ruserf protocol version (Builder pattern)")
    )
  )]
  protocol_version: ProtocolVersion,
  /// The ruserf delegate version
  #[viewit(
    getter(const, attrs(doc = "Returns the ruserf delegate version")),
    setter(
      const,
      attrs(doc = "Sets the ruserf delegate version (Builder pattern)")
    )
  )]
  delegate_version: DelegateVersion,
}

impl<I: Clone, A: Clone> Clone for Member<I, A> {
  fn clone(&self) -> Self {
    Self {
      node: self.node.clone(),
      tags: self.tags.clone(),
      status: self.status,
      memberlist_protocol_version: self.memberlist_protocol_version,
      memberlist_delegate_version: self.memberlist_delegate_version,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}

impl<I: CheapClone, A: CheapClone> CheapClone for Member<I, A> {
  fn cheap_clone(&self) -> Self {
    Self {
      node: self.node.cheap_clone(),
      tags: self.tags.cheap_clone(),
      status: self.status,
      memberlist_protocol_version: self.memberlist_protocol_version,
      memberlist_delegate_version: self.memberlist_delegate_version,
      protocol_version: self.protocol_version,
      delegate_version: self.delegate_version,
    }
  }
}
