use std::{collections::HashMap, future::Future, sync::Arc};

use agnostic::Runtime;
use futures::Stream;
use memberlist_core::transport::{AddressResolver, MaybeResolvedAddress, Node, Transport};

use crate::{delegate::Delegate, Member, SerfDelegate, SerfState};

pub use crate::{delegate::DelegateError, snapshot::SnapshotError};

#[derive(Debug)]
pub struct VoidError;

impl std::fmt::Display for VoidError {
  fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Ok(())
  }
}

impl std::error::Error for VoidError {}

#[derive(Debug, thiserror::Error)]
pub enum Error<T: Transport, D: Delegate>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  #[error("ruserf: {0}")]
  Memberlist(#[from] MemberlistError<T, D>),
  #[error("ruserf: {0}")]
  Delegate(#[from] DelegateError<D>),
  #[error("ruserf: user event size limit exceeds limit of {0} bytes")]
  UserEventLimitTooLarge(usize),
  #[error("ruserf: user event exceeds sane limit of {0} bytes before encoding")]
  UserEventTooLarge(usize),
  #[error("ruserf: join called on {0} statues")]
  BadJoinStatus(SerfState),
  #[error("ruserf: leave called on {0} statues")]
  BadLeaveStatus(SerfState),
  #[error("ruserf: user event exceeds sane limit of {0} bytes after encoding")]
  RawUserEventTooLarge(usize),
  #[error("ruserf: query exceeds limit of {0} bytes")]
  QueryTooLarge(usize),
  #[error("ruserf: query response is past the deadline")]
  QueryTimeout,
  #[error("ruserf: query response ({got} bytes) exceeds limit of {limit} bytes")]
  QueryResponseTooLarge { limit: usize, got: usize },
  #[error("ruserf: query response already sent")]
  QueryAlreadyResponsed,
  #[error("ruserf: failed to truncate response so that it fits into message")]
  FailTruncateResponse,
  #[error("ruserf: encoded length of tags exceeds limit of {0} bytes")]
  TagsTooLarge(usize),
  #[error("ruserf: relayed response exceeds limit of {0} bytes")]
  RelayedResponseTooLarge(usize),
  #[error("ruserf: relay error\n{0}")]
  Relay(RelayError<T, D>),
  #[error("ruserf: failed to deliver query response, dropping")]
  QueryResponseDeliveryFailed,
  #[error("ruserf: coordinates are disabled")]
  CoordinatesDisabled,
  #[error("ruserf: {0}")]
  Snapshot(#[from] SnapshotError),
  #[error("ruserf: timed out broadcasting node removal")]
  RemovalBroadcastTimeout,
}

pub struct MemberlistError<T: Transport, D: Delegate>(
  memberlist_core::error::Error<T, SerfDelegate<T, D>>,
)
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send;

impl<D: Delegate, T: Transport> From<memberlist_core::error::Error<T, SerfDelegate<T, D>>>
  for MemberlistError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn from(value: memberlist_core::error::Error<T, SerfDelegate<T, D>>) -> Self {
    Self(value)
  }
}

impl<D: Delegate, T: Transport> core::fmt::Display for MemberlistError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<D: Delegate, T: Transport> core::fmt::Debug for MemberlistError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<D: Delegate, T: Transport> From<memberlist_core::error::Error<T, SerfDelegate<T, D>>>
  for Error<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn from(value: memberlist_core::error::Error<T, SerfDelegate<T, D>>) -> Self {
    Self::Memberlist(MemberlistError(value))
  }
}

pub struct RelayError<T: Transport, D: Delegate>(
  #[allow(clippy::type_complexity)]
  Vec<(
    Arc<Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    memberlist_core::error::Error<T, SerfDelegate<T, D>>,
  )>,
)
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send;

impl<D: Delegate, T: Transport>
  From<
    Vec<(
      Arc<Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
      memberlist_core::error::Error<T, SerfDelegate<T, D>>,
    )>,
  > for RelayError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn from(
    value: Vec<(
      Arc<Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
      memberlist_core::error::Error<T, SerfDelegate<T, D>>,
    )>,
  ) -> Self {
    Self(value)
  }
}

impl<D, T> core::fmt::Display for RelayError<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    for (member, err) in self.0.iter() {
      writeln!(
        f,
        "\tfailed to send relay response to {}: {}",
        member.id(),
        err
      )?;
    }
    Ok(())
  }
}

impl<D, T> core::fmt::Debug for RelayError<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

impl<D, T> std::error::Error for RelayError<T, D>
where
  D: Delegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
}

/// `JoinError` is returned when join is partially/totally failed.
pub struct JoinError<T: Transport, D: Delegate>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) joined: Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  pub(crate) errors: HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>>,
  pub(crate) broadcast_error: Option<Error<T, D>>,
}

impl<D: Delegate, T: Transport> JoinError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub const fn broadcast_error(&self) -> Option<&Error<T, D>> {
    self.broadcast_error.as_ref()
  }

  pub const fn errors(
    &self,
  ) -> &HashMap<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>> {
    &self.errors
  }

  pub const fn joined(
    &self,
  ) -> &Vec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    &self.joined
  }

  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }
}

impl<D: Delegate, T: Transport> core::fmt::Debug for JoinError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<D: Delegate, T: Transport> core::fmt::Display for JoinError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    if !self.joined.is_empty() {
      writeln!(f, "Successes: {:?}", self.joined)?;
    }

    if !self.errors.is_empty() {
      writeln!(f, "Failures:")?;
      for (address, err) in self.errors.iter() {
        writeln!(f, "\t{}: {}", address, err)?;
      }
    }

    if let Some(err) = &self.broadcast_error {
      writeln!(f, "Broadcast Error: {err}")?;
    }
    Ok(())
  }
}

impl<D: Delegate, T: Transport> std::error::Error for JoinError<T, D>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
}
