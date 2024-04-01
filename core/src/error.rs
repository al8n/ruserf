use std::collections::HashMap;

use memberlist::{
  transport::{AddressResolver, MaybeResolvedAddress, Node, Transport},
  types::{SmallVec, TinyVec},
};

use crate::{
  delegate::{Delegate, TransformDelegate},
  serf::{SerfDelegate, SerfState},
  types::Member,
};

pub use crate::{delegate::DelegateError, snapshot::SnapshotError};

#[derive(Debug)]
pub struct VoidError;

impl std::fmt::Display for VoidError {
  fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Ok(())
  }
}

impl std::error::Error for VoidError {}

#[derive(thiserror::Error)]
pub enum Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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
  #[error("ruserf: timed out broadcasting channel closed")]
  BroadcastChannelClosed,
}

impl<T, D> core::fmt::Debug for Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<T, D> Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[inline]
  pub fn transform(err: <D as TransformDelegate>::Error) -> Self {
    Self::Delegate(DelegateError::TransformDelegate(err))
  }
}

pub struct MemberlistError<T, D>(memberlist::error::Error<T, SerfDelegate<T, D>>)
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport;

impl<T, D> From<memberlist::error::Error<T, SerfDelegate<T, D>>> for MemberlistError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: memberlist::error::Error<T, SerfDelegate<T, D>>) -> Self {
    Self(value)
  }
}

impl<T, D> core::fmt::Display for MemberlistError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<T, D> core::fmt::Debug for MemberlistError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<T, D> std::error::Error for MemberlistError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
}

impl<T, D> From<memberlist::error::Error<T, SerfDelegate<T, D>>> for Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: memberlist::error::Error<T, SerfDelegate<T, D>>) -> Self {
    Self::Memberlist(MemberlistError(value))
  }
}

pub struct RelayError<T, D>(
  #[allow(clippy::type_complexity)]
  TinyVec<(
    Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    memberlist::error::Error<T, SerfDelegate<T, D>>,
  )>,
)
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport;

impl<T, D>
  From<
    TinyVec<(
      Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
      memberlist::error::Error<T, SerfDelegate<T, D>>,
    )>,
  > for RelayError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(
    value: TinyVec<(
      Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
      memberlist::error::Error<T, SerfDelegate<T, D>>,
    )>,
  ) -> Self {
    Self(value)
  }
}

impl<T, D> core::fmt::Display for RelayError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    for (member, err) in self.0.iter() {
      writeln!(
        f,
        "\tfailed to send relay response to {}: {}",
        member.node().id(),
        err
      )?;
    }
    Ok(())
  }
}

impl<T, D> core::fmt::Debug for RelayError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

impl<T, D> std::error::Error for RelayError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
}

/// `JoinError` is returned when join is partially/totally failed.
pub struct JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) joined: SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  pub(crate) errors: HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>>,
  pub(crate) broadcast_error: Option<Error<T, D>>,
}

impl<T, D> JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub const fn broadcast_error(&self) -> Option<&Error<T, D>> {
    self.broadcast_error.as_ref()
  }

  pub const fn errors(&self) -> &HashMap<Node<T::Id, MaybeResolvedAddress<T>>, Error<T, D>> {
    &self.errors
  }

  pub const fn joined(
    &self,
  ) -> &SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    &self.joined
  }

  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }
}

impl<T, D> core::fmt::Debug for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<T, D> core::fmt::Display for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
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

impl<T, D> std::error::Error for JoinError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
}
