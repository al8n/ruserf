use memberlist_types::{Node, TinyVec};
use smol_str::SmolStr;

/// Used with a queryFilter to specify the type of
/// filter we are sending
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Filter<I, A> {
  /// Filter by nodes
  Node(TinyVec<Node<I, A>>),
  /// Filter by tag
  Tag {
    /// The tag to filter by
    tag: SmolStr,
    /// The expression to filter by
    expr: SmolStr,
  },
}

// impl<I, A> Filter<I, A> {
//   pub(crate) const NODE: u8 = 0;
//   pub(crate) const TAG: u8 = 1;
// }
