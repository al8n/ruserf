use showbiz_core::transport::Transport;

use crate::delegate::{SerfDelegate, MergeDelegate};

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
  #[error("ruserf: encode error: {0}")]
  Encode(#[from] rmp_serde::encode::Error),
  #[error("ruserf: decode error: {0}")]
  Decode(#[from] rmp_serde::decode::Error),
}