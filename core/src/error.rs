use std::{borrow::Cow, collections::HashMap};

use memberlist_core::{
  transport::{AddressResolver, MaybeResolvedAddress, Node, Transport},
  types::{SmallVec, TinyVec},
};
use smol_str::SmolStr;

use crate::{
  delegate::{Delegate, MergeDelegate, TransformDelegate},
  serf::{SerfDelegate, SerfState},
  types::Member,
};

pub use crate::{delegate::DelegateError, snapshot::SnapshotError};
pub use memberlist_core::delegate::DelegateError as MemberlistDelegateError;
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
  #[error(transparent)]
  Memberlist(#[from] MemberlistError<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>),
  #[error(transparent)]
  Serf(#[from] SerfError),
  #[error(transparent)]
  Transport(T::Error),
  #[error(transparent)]
  MemberlistDelegate(#[from] MemberlistDelegateError<SerfDelegate<T, D>>),
  #[error(transparent)]
  Delegate(#[from] DelegateError<D>),
  #[error(transparent)]
  Relay(#[from] RelayError<T, D>),
}

impl<T, D> From<memberlist_core::error::Error<T, SerfDelegate<T, D>>> for Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: memberlist_core::error::Error<T, SerfDelegate<T, D>>) -> Self {
    match value {
      memberlist_core::error::Error::NotRunning => Self::Memberlist(MemberlistError::NotRunning),
      memberlist_core::error::Error::UpdateTimeout => {
        Self::Memberlist(MemberlistError::UpdateTimeout)
      }
      memberlist_core::error::Error::LeaveTimeout => {
        Self::Memberlist(MemberlistError::LeaveTimeout)
      }
      memberlist_core::error::Error::Lost(n) => Self::Memberlist(MemberlistError::Lost(n)),
      memberlist_core::error::Error::Delegate(e) => Self::MemberlistDelegate(e),
      memberlist_core::error::Error::Transport(e) => Self::Transport(e),
      memberlist_core::error::Error::UnexpectedMessage { expected, got } => {
        Self::Memberlist(MemberlistError::UnexpectedMessage { expected, got })
      }
      memberlist_core::error::Error::SequenceNumberMismatch { ping, ack } => {
        Self::Memberlist(MemberlistError::SequenceNumberMismatch { ping, ack })
      }
      memberlist_core::error::Error::Remote(e) => Self::Memberlist(MemberlistError::Remote(e)),
      memberlist_core::error::Error::Other(e) => Self::Memberlist(MemberlistError::Other(e)),
    }
  }
}

impl<T, D> core::fmt::Debug for Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Memberlist(e) => write!(f, "{e:?}"),
      Self::Serf(e) => write!(f, "{e:?}"),
      Self::Transport(e) => write!(f, "{e:?}"),
      Self::Delegate(e) => write!(f, "{e:?}"),
      Self::MemberlistDelegate(e) => write!(f, "{e:?}"),
      Self::Relay(e) => write!(f, "{e:?}"),
    }
  }
}

impl<T, D> From<SnapshotError> for Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(value: SnapshotError) -> Self {
    Self::Serf(SerfError::Snapshot(value))
  }
}

impl<T, D> Error<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  /// Create error from a transform error
  #[inline]
  pub fn transform_delegate(err: <D as TransformDelegate>::Error) -> Self {
    Self::Delegate(DelegateError::TransformDelegate(err))
  }

  /// Create a merge delegate error
  #[inline]
  pub const fn merge_delegate(err: <D as MergeDelegate>::Error) -> Self {
    Self::Delegate(DelegateError::MergeDelegate(err))
  }

  /// Create a query response too large error
  #[inline]
  pub const fn query_response_too_large(limit: usize, got: usize) -> Self {
    Self::Serf(SerfError::QueryResponseTooLarge { limit, got })
  }

  /// Create a query timeout error
  #[inline]
  pub const fn query_timeout() -> Self {
    Self::Serf(SerfError::QueryTimeout)
  }

  /// Create a query already response error
  #[inline]
  pub const fn query_already_responsed() -> Self {
    Self::Serf(SerfError::QueryAlreadyResponsed)
  }

  /// Create a query response delivery failed error
  #[inline]
  pub const fn query_response_delivery_failed() -> Self {
    Self::Serf(SerfError::QueryResponseDeliveryFailed)
  }

  /// Create a relayed response too large error
  #[inline]
  pub const fn relayed_response_too_large(size: usize) -> Self {
    Self::Serf(SerfError::RelayedResponseTooLarge(size))
  }

  /// Create a relay error
  #[inline]
  pub const fn relay(err: RelayError<T, D>) -> Self {
    Self::Relay(err)
  }

  /// Create a fail truncate response error
  #[inline]
  pub const fn fail_truncate_response() -> Self {
    Self::Serf(SerfError::FailTruncateResponse)
  }

  /// Create a tags too large error
  #[inline]
  pub const fn tags_too_large(size: usize) -> Self {
    Self::Serf(SerfError::TagsTooLarge(size))
  }

  /// Create a query too large error
  #[inline]
  pub const fn query_too_large(size: usize) -> Self {
    Self::Serf(SerfError::QueryTooLarge(size))
  }

  /// Create a user event limit too large error
  #[inline]
  pub const fn user_event_limit_too_large(size: usize) -> Self {
    Self::Serf(SerfError::UserEventLimitTooLarge(size))
  }

  /// Create a raw user event too large error
  #[inline]
  pub const fn raw_user_event_too_large(size: usize) -> Self {
    Self::Serf(SerfError::RawUserEventTooLarge(size))
  }

  /// Create a broadcast channel closed error
  #[inline]
  pub const fn broadcast_channel_closed() -> Self {
    Self::Serf(SerfError::BroadcastChannelClosed)
  }

  /// Create a removal broadcast timeout error
  #[inline]
  pub const fn removal_broadcast_timeout() -> Self {
    Self::Serf(SerfError::RemovalBroadcastTimeout)
  }

  /// Create a snapshot error
  #[inline]
  pub const fn snapshot(err: SnapshotError) -> Self {
    Self::Serf(SerfError::Snapshot(err))
  }

  /// Create a memberlist error
  #[inline]
  pub const fn memberlist(
    err: MemberlistError<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Self {
    Self::Memberlist(err)
  }

  /// Create a bad leave status error
  #[inline]
  pub const fn bad_leave_status(status: SerfState) -> Self {
    Self::Serf(SerfError::BadLeaveStatus(status))
  }

  /// Create a bad join status error
  #[inline]
  pub const fn bad_join_status(status: SerfState) -> Self {
    Self::Serf(SerfError::BadJoinStatus(status))
  }

  /// Create a coordinates disabled error
  #[inline]
  pub const fn coordinates_disabled() -> Self {
    Self::Serf(SerfError::CoordinatesDisabled)
  }
}

#[derive(Debug, thiserror::Error)]
pub enum SerfError {
  // #[error("ruserf: {0}")]
  // Delegate(#[from] DelegateError<D>),
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

#[derive(Debug, thiserror::Error)]
pub enum MemberlistError<I, A> {
  /// Returns when the node is not running.
  #[error("memberlist: node is not running, please bootstrap first")]
  NotRunning,
  /// Returns when timeout waiting for update broadcast.
  #[error("memberlist: timeout waiting for update broadcast")]
  UpdateTimeout,
  /// Returns when timeout waiting for leave broadcast.
  #[error("memberlist: timeout waiting for leave broadcast")]
  LeaveTimeout,
  /// Returns when lost connection with a peer.
  #[error("memberlist: no response from node {0}")]
  Lost(Node<I, A>),
  /// Returned when a message is received with an unexpected type.
  #[error("memberlist: unexpected message: expected {expected}, got {got}")]
  UnexpectedMessage {
    /// The expected message type.
    expected: &'static str,
    /// The actual message type.
    got: &'static str,
  },
  /// Returned when the sequence number of [`Ack`](crate::types::Ack) is not
  /// match the sequence number of [`Ping`](crate::types::Ping).
  #[error("memberlist: sequence number mismatch: ping({ping}), ack({ack})")]
  SequenceNumberMismatch {
    /// The sequence number of [`Ping`](crate::types::Ping).
    ping: u32,
    /// The sequence number of [`Ack`](crate::types::Ack).
    ack: u32,
  },
  /// Returned when a remote error is received.
  #[error("memberlist: remote error: {0}")]
  Remote(SmolStr),
  /// Returned when a custom error is created by users.
  #[error("memberlist: {0}")]
  Other(Cow<'static, str>),
}

pub struct RelayError<T, D>(
  #[allow(clippy::type_complexity)]
  TinyVec<(
    Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    memberlist_core::error::Error<T, SerfDelegate<T, D>>,
  )>,
)
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport;

impl<T, D>
  From<
    TinyVec<(
      Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
      memberlist_core::error::Error<T, SerfDelegate<T, D>>,
    )>,
  > for RelayError<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  fn from(
    value: TinyVec<(
      Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
      memberlist_core::error::Error<T, SerfDelegate<T, D>>,
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
    writeln!(f, "relay errors:")?;

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
