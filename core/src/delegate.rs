use memberlist_core::{transport::Id, CheapClone};

mod merge;
pub use merge::*;

mod reconnect;
pub use reconnect::*;

mod transform;
pub use transform::*;

mod composite;
pub use composite::*;

pub trait Delegate:
  MergeDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + TransformDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
  + ReconnectDelegate<Id = <Self as Delegate>::Id, Address = <Self as Delegate>::Address>
{
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;
}
