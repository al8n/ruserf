#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/ruserf/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
// #![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

/// 
pub mod coordinate;

/// Delegate traits and its implementations.
pub mod delegate;

/// The types used in `ruserf`.
pub mod types;

mod serf;
pub use serf::*;

#[test]
fn test_() {
}