use std::{collections::HashMap, sync::Arc};

use showbiz_core::{transport::Transport, Address, NodeId};

use crate::{
  delegate::{MergeDelegate, SerfDelegate},
  snapshot::SnapshotError,
  Member, ReconnectTimeoutOverrider, SerfState,
};

#[derive(Debug)]
pub struct VoidError;

impl std::fmt::Display for VoidError {
  fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Ok(())
  }
}

impl std::error::Error for VoidError {}

#[derive(thiserror::Error)]
pub enum Error<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  #[error("ruserf: {0}")]
  Showbiz(#[from] ShowbizError<T, D, O>),
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
  Relay(RelayError<T, D, O>),
  #[error("ruserf: encode error: {0}")]
  Encode(#[from] rmp_serde::encode::Error),
  #[error("ruserf: decode error: {0}")]
  Decode(#[from] rmp_serde::decode::Error),
  #[error("ruserf: failed to deliver query response, dropping")]
  QueryResponseDeliveryFailed,
  #[error("ruserf: coordinates are disabled")]
  CoordinatesDisabled,
  #[error("ruserf: {0}")]
  Snapshot(#[from] SnapshotError),
  #[error("ruserf: timed out broadcasting node removal")]
  RemovalBroadcastTimeout,
}

pub struct ShowbizError<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  showbiz_core::error::Error<T, SerfDelegate<T, D, O>>,
);

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider>
  From<showbiz_core::error::Error<T, SerfDelegate<T, D, O>>> for ShowbizError<T, D, O>
{
  fn from(value: showbiz_core::error::Error<T, SerfDelegate<T, D, O>>) -> Self {
    Self(value)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> core::fmt::Display
  for ShowbizError<T, D, O>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider>
  From<showbiz_core::error::Error<T, SerfDelegate<T, D, O>>> for Error<T, D, O>
{
  fn from(value: showbiz_core::error::Error<T, SerfDelegate<T, D, O>>) -> Self {
    Self::Showbiz(ShowbizError(value))
  }
}

pub struct RelayError<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  #[allow(clippy::type_complexity)]
  Vec<(
    Arc<Member>,
    showbiz_core::error::Error<T, SerfDelegate<T, D, O>>,
  )>,
);

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider>
  From<
    Vec<(
      Arc<Member>,
      showbiz_core::error::Error<T, SerfDelegate<T, D, O>>,
    )>,
  > for RelayError<T, D, O>
{
  fn from(
    value: Vec<(
      Arc<Member>,
      showbiz_core::error::Error<T, SerfDelegate<T, D, O>>,
    )>,
  ) -> Self {
    Self(value)
  }
}

impl<D, T, O> core::fmt::Display for RelayError<T, D, O>
where
  D: MergeDelegate,
  T: Transport,
  O: ReconnectTimeoutOverrider,
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

/// `JoinError` is returned when join is partially/totally failed.
pub struct JoinError<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  pub(crate) joined: Vec<NodeId>,
  pub(crate) errors: HashMap<Address, Error<T, D, O>>,
  pub(crate) broadcast_error: Option<Error<T, D, O>>,
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> JoinError<T, D, O> {
  pub const fn broadcast_error(&self) -> Option<&Error<T, D, O>> {
    self.broadcast_error.as_ref()
  }

  pub const fn errors(&self) -> &HashMap<Address, Error<T, D, O>> {
    &self.errors
  }

  pub const fn joined(&self) -> &Vec<NodeId> {
    &self.joined
  }

  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> core::fmt::Debug
  for JoinError<T, D, O>
{
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> core::fmt::Display
  for JoinError<T, D, O>
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

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> std::error::Error
  for JoinError<T, D, O>
{
}
