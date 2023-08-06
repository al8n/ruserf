use std::{collections::HashMap, sync::Arc};

use showbiz_core::{transport::Transport, Address, NodeId};

use crate::{
  delegate::{MergeDelegate, SerfDelegate},
  snapshot::SnapshotError,
  Member,
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
pub enum Error<D: MergeDelegate, T: Transport> {
  #[error("ruserf: {0}")]
  Showbiz(#[from] ShowbizError<D, T>),
  #[error("ruserf: user event size limit exceeds limit of {0} bytes")]
  UserEventLimitTooLarge(usize),
  #[error("ruserf: user event exceeds sane limit of {0} bytes before encoding")]
  UserEventTooLarge(usize),
  #[error("ruserf: can't join after leave or shutdown")]
  BadStatus,
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
  Relay(RelayError<D, T>),
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
}

pub struct ShowbizError<D: MergeDelegate, T: Transport>(
  showbiz_core::error::Error<SerfDelegate<D, T>, T>,
);

impl<D: MergeDelegate, T: Transport> From<showbiz_core::error::Error<SerfDelegate<D, T>, T>>
  for ShowbizError<D, T>
{
  fn from(value: showbiz_core::error::Error<SerfDelegate<D, T>, T>) -> Self {
    Self(value)
  }
}

impl<D: MergeDelegate, T: Transport> core::fmt::Display for ShowbizError<D, T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<D: MergeDelegate, T: Transport> From<showbiz_core::error::Error<SerfDelegate<D, T>, T>>
  for Error<D, T>
{
  fn from(value: showbiz_core::error::Error<SerfDelegate<D, T>, T>) -> Self {
    Self::Showbiz(ShowbizError(value))
  }
}

pub struct RelayError<D: MergeDelegate, T: Transport>(
  #[allow(clippy::type_complexity)]
  Vec<(
    Arc<Member>,
    showbiz_core::error::Error<SerfDelegate<D, T>, T>,
  )>,
);

impl<D: MergeDelegate, T: Transport>
  From<
    Vec<(
      Arc<Member>,
      showbiz_core::error::Error<SerfDelegate<D, T>, T>,
    )>,
  > for RelayError<D, T>
{
  fn from(
    value: Vec<(
      Arc<Member>,
      showbiz_core::error::Error<SerfDelegate<D, T>, T>,
    )>,
  ) -> Self {
    Self(value)
  }
}

impl<D, T> core::fmt::Display for RelayError<D, T>
where
  D: MergeDelegate,
  T: Transport,
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
pub struct JoinError<D: MergeDelegate, T: Transport> {
  pub(crate) joined: Vec<NodeId>,
  pub(crate) errors: HashMap<Address, Error<D, T>>,
  pub(crate) broadcast_error: Option<Error<D, T>>,
}

impl<D: MergeDelegate, T: Transport> JoinError<D, T> {
  pub const fn broadcast_error(&self) -> Option<&Error<D, T>> {
    self.broadcast_error.as_ref()
  }

  pub const fn errors(&self) -> &HashMap<Address, Error<D, T>> {
    &self.errors
  }

  pub const fn joined(&self) -> &Vec<NodeId> {
    &self.joined
  }

  pub fn num_joined(&self) -> usize {
    self.joined.len()
  }
}

impl<D: MergeDelegate, T: Transport> core::fmt::Debug for JoinError<D, T> {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self)
  }
}

impl<D: MergeDelegate, T: Transport> core::fmt::Display for JoinError<D, T> {
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

impl<D: MergeDelegate, T: Transport> std::error::Error for JoinError<D, T> {}
