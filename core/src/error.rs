use std::sync::Arc;

use showbiz_core::transport::Transport;

use crate::{
  delegate::{MergeDelegate, SerfDelegate},
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
  Showbiz(#[from] showbiz_core::error::Error<SerfDelegate<D, T, T::Runtime>, T>),
  #[error("ruserf: user event exceeds sane limit of {0} bytes before encoding")]
  UserEventTooLarge(usize),
  #[error("ruserf: user event exceeds sane limit of {0} bytes after encoding")]
  RawUserEventTooLarge(usize),
  #[error("ruserf: query exceeds limit of {0} bytes")]
  QueryTooLarge(usize),
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
}

pub struct RelayError<D: MergeDelegate, T: Transport>(
  Vec<(
    Arc<Member>,
    showbiz_core::error::Error<SerfDelegate<D, T, T::Runtime>, T>,
  )>,
);

impl<D: MergeDelegate, T: Transport>
  From<
    Vec<(
      Arc<Member>,
      showbiz_core::error::Error<SerfDelegate<D, T, T::Runtime>, T>,
    )>,
  > for RelayError<D, T>
{
  fn from(
    value: Vec<(
      Arc<Member>,
      showbiz_core::error::Error<SerfDelegate<D, T, T::Runtime>, T>,
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
