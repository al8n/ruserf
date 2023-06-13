use super::LamportTime;
use memberlist_core::transport::Node;

/// The message broadcasted to signal the intentional to
/// leave.
#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LeaveMessage<I, A> {
  ltime: LamportTime,
  node: Node<I, A>,
  prune: bool,
}
