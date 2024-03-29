use super::{LamportTime, Node};

/// The message broadcasted to signal the intentional to
/// leave.
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaveMessage<I, A> {
  /// The lamport time
  #[viewit(
    getter(const, attrs(doc = "Returns the lamport time for this message")),
    setter(
      const,
      attrs(doc = "Sets the lamport time for this message (Builder pattern)")
    )
  )]
  ltime: LamportTime,
  /// The node
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns the node")),
    setter(attrs(doc = "Sets the node (Builder pattern)"))
  )]
  node: Node<I, A>,

  /// If prune or not
  #[viewit(
    getter(const, style = "ref", attrs(doc = "Returns if prune or not")),
    setter(attrs(doc = "Sets prune or not (Builder pattern)"))
  )]
  prune: bool,
}
