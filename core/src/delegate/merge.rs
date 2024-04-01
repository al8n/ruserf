use memberlist_core::{transport::Id, types::TinyVec, CheapClone};
use std::future::Future;

use crate::types::Member;

pub trait MergeDelegate: Send + Sync + 'static {
  type Error: std::error::Error + Send + Sync + 'static;
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;

  fn notify_merge(
    &self,
    members: TinyVec<Member<Self::Id, Self::Address>>,
  ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultMergeDelegate<I, A>(std::marker::PhantomData<(I, A)>);

impl<I, A> Default for DefaultMergeDelegate<I, A> {
  fn default() -> Self {
    Self(Default::default())
  }
}

impl<I, A> MergeDelegate for DefaultMergeDelegate<I, A>
where
  I: Id,
  A: CheapClone + Send + Sync + 'static,
{
  type Error = std::convert::Infallible;
  type Id = I;
  type Address = A;

  async fn notify_merge(
    &self,
    _members: TinyVec<Member<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }
}
