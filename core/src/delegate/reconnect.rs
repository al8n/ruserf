use std::time::Duration;

use memberlist_core::{transport::Id, CheapClone};

use crate::Member;

/// Implemented to allow overriding the reconnect timeout for individual members.
pub trait ReconnectDelegate: Send + Sync + 'static {
  type Id: Id;
  type Address: CheapClone + Send + Sync + 'static;

  fn reconnect_timeout(
    &self,
    member: &Member<Self::Id, Self::Address>,
    timeout: Duration,
  ) -> Duration;
}

/// Noop implementation of `ReconnectDelegate`.
#[derive(Debug)]
pub struct NoopReconnectDelegate<I, A>(std::marker::PhantomData<(I, A)>);

impl<I, A> Default for NoopReconnectDelegate<I, A> {
  fn default() -> Self {
    Self(Default::default())
  }
}

impl<I, A> Clone for NoopReconnectDelegate<I, A> {
  fn clone(&self) -> Self {
    *self
  }
}

impl<I, A> Copy for NoopReconnectDelegate<I, A> {}

impl<I, A> ReconnectDelegate for NoopReconnectDelegate<I, A> {
  type Id = I;
  type Address = A;

  fn reconnect_timeout(
    &self,
    _member: &Member<Self::Id, Self::Address>,
    timeout: Duration,
  ) -> Duration {
    timeout
  }
}
