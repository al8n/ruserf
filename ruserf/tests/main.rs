use core::future::Future;
use ruserf_core::tests::run as run_unit_test;

#[cfg(feature = "net")]
#[path = "./main/net.rs"]
mod net;

#[cfg(feature = "quic")]
#[path = "./main/quic.rs"]
mod quic;

#[cfg(feature = "tokio")]
fn tokio_run(fut: impl Future<Output = ()>) {
  let runtime = ::tokio::runtime::Builder::new_multi_thread()
    .worker_threads(16)
    .enable_all()
    .build()
    .unwrap();
  run_unit_test(|fut| runtime.block_on(fut), fut)
}

#[cfg(feature = "smol")]
fn smol_run(fut: impl Future<Output = ()>) {
  use ruserf::agnostic::{smol::SmolRuntime, RuntimeLite};
  run_unit_test(SmolRuntime::block_on, fut);
}

#[cfg(feature = "async-std")]
fn async_std_run(fut: impl Future<Output = ()>) {
  use ruserf::agnostic::{async_std::AsyncStdRuntime, RuntimeLite};
  run_unit_test(AsyncStdRuntime::block_on, fut);
}
