#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/ruserf/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
// #![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

use memberlist_core::tracing;

/// 
pub mod coordinate;

pub mod event;

/// Delegate traits and its implementations.
pub mod delegate;

mod options;
pub use options::*;

/// The types used in `ruserf`.
pub mod types;

mod serf;
pub use serf::*;

// mod snapshot;
// pub use snapshot::*;

#[test]
fn test_() {
}