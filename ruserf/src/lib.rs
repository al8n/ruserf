#![doc = include_str!("../../README.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/al8n/ruserf/main/art/logo_72x72.png")]
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![allow(clippy::type_complexity)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

pub use ruserf_core::*;

pub use memberlist::{agnostic, transport};

#[cfg(feature = "net")]
pub use memberlist::net;

#[cfg(feature = "quic")]
pub use memberlist::quic;

/// [`Serf`](ruserf_core::Serf) for `tokio` runtime.
#[cfg(feature = "tokio")]
#[cfg_attr(docsrs, doc(cfg(feature = "tokio")))]
pub mod tokio;

/// [`Serf`](ruserf_core::Serf) for `async-std` runtime.
#[cfg(feature = "async-std")]
#[cfg_attr(docsrs, doc(cfg(feature = "async-std")))]
pub mod async_std;

/// [`Serf`](ruserf_core::Serf) for `smol` runtime.
#[cfg(feature = "smol")]
#[cfg_attr(docsrs, doc(cfg(feature = "smol")))]
pub mod smol;
