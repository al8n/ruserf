#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/ruserf/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
// #![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub(crate) mod broadcast;

mod coalesce;

/// Coordinate.
pub mod coordinate;

pub mod event;

/// Errors for `ruserf`.
pub mod error;

/// Delegate traits and its implementations.
pub mod delegate;

mod options;
pub use options::*;

/// The types used in `ruserf`.
pub mod types;

/// Secret key management.
#[cfg(feature = "encryption")]
#[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
pub mod key_manager;

mod serf;
pub use serf::*;

mod snapshot;
pub use snapshot::*;

fn invalid_data_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
