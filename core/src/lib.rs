#![forbid(unsafe_code)]
#![allow(clippy::mutable_key_type)]

pub mod clock;
pub mod coordinate;

mod coalesce;
pub mod delegate;
pub mod error;
pub mod event;

mod serf;
mod snapshot;
use memberlist_core::bytes::Bytes;
pub use serf::*;

mod broadcast;
mod key_manager;
pub use key_manager::*;
mod types;
use types::*;

mod options;
pub use options::*;

mod internal_query;
mod query;

fn invalid_data_io_error<E: std::error::Error + Send + Sync + 'static>(e: E) -> std::io::Error {
  std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}
