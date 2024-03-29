use super::{LamportTime, Node};

/// The message broadcasted after we join to
/// associated the node with a lamport clock
#[viewit::viewit(setters(prefix = "with"))]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JoinMessage<I, A> {
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
}

impl<I, A> JoinMessage<I, A> {
  /// Create a new join message
  pub fn new(ltime: LamportTime, node: Node<I, A>) -> Self {
    Self { ltime, node }
  }

  /// Set the lamport time
  #[inline]
  pub fn set_ltime(&mut self, ltime: LamportTime) -> &mut Self {
    self.ltime = ltime;
    self
  }

  /// Set the node
  #[inline]
  pub fn set_node(&mut self, node: Node<I, A>) -> &mut Self {
    self.node = node;
    self
  }
}
