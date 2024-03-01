mod merge;
use memberlist_core::{transport::Id, CheapClone};
pub use merge::*;
mod reconnect;
pub use reconnect::*;
mod transform;
pub use transform::*;
mod composite;
pub use composite::*;

/// Error trait for [`Delegate`]
pub enum DelegateError<D: Delegate> {
  /// [`TransformDelegate`] error
  TransformDelegate(<D as TransformDelegate>::Error),
  /// [`MergeDelegate`] error
  MergeDelegate(<D as MergeDelegate>::Error),
}

impl<D: Delegate> core::fmt::Debug for DelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::TransformDelegate(err) => write!(f, "{err:?}"),
      Self::MergeDelegate(err) => write!(f, "{err:?}"),
    }
  }
}

impl<D: Delegate> core::fmt::Display for DelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::TransformDelegate(err) => write!(f, "{err}"),
      Self::MergeDelegate(err) => write!(f, "{err}"),
    }
  }
}

impl<D: Delegate> std::error::Error for DelegateError<D> {}

impl<D: Delegate> DelegateError<D> {
  /// Create a delegate error from an alive delegate error.
  #[inline]
  pub const fn alive(err: <D as TransformDelegate>::Error) -> Self {
    Self::TransformDelegate(err)
  }

  /// Create a delegate error from a merge delegate error.
  #[inline]
  pub const fn merge(err: <D as MergeDelegate>::Error) -> Self {
    Self::MergeDelegate(err)
  }
}

pub trait Delegate:
  MergeDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + TransformDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + ReconnectDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
{
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;
}
