#![forbid(unsafe_code)]
#![allow(clippy::mutable_key_type)]

pub mod clock;

// mod coalesce;
mod delegate;
// pub mod event;
pub mod error;

mod serf;
pub use serf::*;

mod broadcast;
mod types;

mod options;
pub use options::*;

mod query;
