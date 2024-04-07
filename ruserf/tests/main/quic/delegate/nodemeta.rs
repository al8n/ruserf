macro_rules! test_mod {
  ($rt:ident) => {
    paste::paste! {
      mod [< $rt:snake >] {
        use std::net::SocketAddr;

        use crate::[< $rt:snake _run >];
        use ruserf::{
          transport::resolver::socket_addr::SocketAddrResolver,
          quic::{
            stream_layer::quinn::Quinn, QuicTransport,
            QuicTransportOptions,
            tests::quinn_stream_layer,
          },
          [< $rt:snake >]::[< $rt:camel Runtime >],
          transport::Lpe,
        };
        use ruserf_core::tests::{delegate::{delegate_nodemeta, delegate_nodemeta_panic}, next_socket_addr_v4, next_socket_addr_v6};
        use smol_str::SmolStr;

        #[test]
        fn test_delegate_nodemeta_v4() {
          [< $rt:snake _run >](async move {
            let name = "delegate_nodemeta_v4";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer::<[< $rt:camel Runtime >]>().await);
            opts.add_bind_address(next_socket_addr_v4(0));
            delegate_nodemeta::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts).await
          });
        }

        #[test]
        fn test_delegate_nodemeta_v6() {
          [< $rt:snake _run >](async move {
            let name = "delegate_nodemeta_v6";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer::<[< $rt:camel Runtime >]>().await);
            opts.add_bind_address(next_socket_addr_v6());
            delegate_nodemeta::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts).await
          });
        }

        #[test]
        #[should_panic]
        fn test_delegate_nodemeta_panic_v4() {
          [< $rt:snake _run >](async move {
            let name = "delegate_nodemeta_panic_v4";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer::<[< $rt:camel Runtime >]>().await);
            opts.add_bind_address(next_socket_addr_v4(0));
            delegate_nodemeta_panic::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts).await
          });
        }

        #[test]
        #[should_panic]
        fn test_delegate_nodemeta_panic_v6() {
          [< $rt:snake _run >](async move {
            let name = "delegate_nodemeta_panic_v6";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer::<[< $rt:camel Runtime >]>().await);
            opts.add_bind_address(next_socket_addr_v6());
            delegate_nodemeta_panic::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts).await
          });
        }
      }
    }
  };
}

#[cfg(feature = "tokio")]
test_mod!(tokio);

#[cfg(feature = "async-std")]
test_mod!(async_std);

#[cfg(feature = "smol")]
test_mod!(smol);
