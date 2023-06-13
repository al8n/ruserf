use super::LamportTime;
use memberlist_core::transport::Node;

/// The message broadcasted after we join to
/// associated the node with a lamport clock
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct JoinMessage<I, A> {
  ltime: LamportTime,
  node: Node<I, A>,
}

impl<I, A> JoinMessage<I, A> {
  pub fn new(ltime: LamportTime, node: Node<I, A>) -> Self {
    Self { ltime, node }
  }
}
