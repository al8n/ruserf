
pub use ruserf_core::*;

pub use memberlist::agnostic;

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
