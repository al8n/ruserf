mod tokio {
  use std::net::SocketAddr;

  use crate::tokio_run;
  use ruserf::{
    net::{
      resolver::socket_addr::SocketAddrResolver, stream_layer::tcp::Tcp, NetTransport,
      NetTransportOptions,
    },
    tokio::TokioRuntime,
    transport::Lpe,
  };
  use ruserf_core::tests::{delegate::delegate_nodemeta, next_socket_addr_v4};
  use smol_str::SmolStr;

  #[test]
  fn test_delegate_nodemeta() {
    let name = "tokio_delegate_nodemeta";
    let mut opts = NetTransportOptions::new(SmolStr::new(name));
    opts.add_bind_address(next_socket_addr_v4(0));
    tokio_run(delegate_nodemeta::<
      NetTransport<
        SmolStr,
        SocketAddrResolver<TokioRuntime>,
        Tcp<TokioRuntime>,
        Lpe<SmolStr, SocketAddr>,
        TokioRuntime,
      >,
    >(opts));
  }
}
