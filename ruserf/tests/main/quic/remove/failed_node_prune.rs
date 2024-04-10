macro_rules! test_mod {
  ($rt:ident) => {
    paste::paste! {
      mod [< $rt:snake >] {
        use std::{net::SocketAddr, time::Duration};

        use crate::[< $rt:snake _run >];
        use ruserf::{
          transport::resolver::socket_addr::SocketAddrResolver,
          quic::{
            stream_layer::quinn::Quinn, QuicTransport,
            QuicTransportOptions,
            tests::quinn_stream_layer_with_connect_timeout,
          },
          [< $rt:snake >]::[< $rt:camel Runtime >],
          transport::Lpe,
        };
        use ruserf_core::tests::{remove::serf_remove_failed_node_prune, next_socket_addr_v4, next_socket_addr_v6};
        use smol_str::SmolStr;

        #[test]
        fn test_serf_remove_failed_node_prune_v4() {
          [< $rt:snake _run >](async move {
            let name = "serf_remove_failed_node_prune1_v4";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts.add_bind_address(next_socket_addr_v4(0));

            let name = "serf_remove_failed_node_prune2_v4";
            let mut opts2 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts2.add_bind_address(next_socket_addr_v4(0));

            let name = "serf_remove_failed_node_prune3_v4";
            let mut opts3 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts3.add_bind_address(next_socket_addr_v4(0));

            serf_remove_failed_node_prune::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts, opts2, opts3).await;
          });
        }

        #[test]
        fn test_serf_remove_failed_node_prune_v6() {
          [< $rt:snake _run >](async move {
            let name = "serf_remove_failed_node_prune1_v6";
            let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts.add_bind_address(next_socket_addr_v6());

            let name = "serf_remove_failed_node_prune2_v6";
            let mut opts2 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts2.add_bind_address(next_socket_addr_v6());

            let name = "serf_remove_failed_node_prune3_v6";
            let mut opts3 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await);
            opts3.add_bind_address(next_socket_addr_v6());

            serf_remove_failed_node_prune::<
              QuicTransport<
                SmolStr,
                SocketAddrResolver<[< $rt:camel Runtime >]>,
                Quinn<[< $rt:camel Runtime >]>,
                Lpe<SmolStr, SocketAddr>,
                [< $rt:camel Runtime >],
              >,
            >(opts, opts2, opts3).await;
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
