// macro_rules! test_mod {
//   ($rt:ident) => {
//     paste::paste! {
//       mod [< $rt:snake >] {
//         use std::{net::SocketAddr, time::Duration};

//         use crate::[< $rt:snake _run >];
//         use ruserf::{
//           transport::resolver::socket_addr::SocketAddrResolver,
//           quic::{
//             stream_layer::quinn::Quinn, QuicTransport,
//             QuicTransportOptions,
//             tests::quinn_stream_layer_with_connect_timeout,
//           },
//           [< $rt:snake >]::[< $rt:camel Runtime >],
//           transport::Lpe,
//         };
//         use ruserf_core::tests::{serf_update, next_socket_addr_v4, next_socket_addr_v6};
//         use smol_str::SmolStr;

//         #[test]
//         fn test_serf_update_v4() {
//           [< $rt:snake _run >](async move {
//             let name = "serf_update1_v4";
//             let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_pool_cleanup_period(Duration::from_millis(20)).with_connection_ttl(Some(Duration::from_millis(20)));
//             opts.add_bind_address(next_socket_addr_v4(0));

//             let name = "serf_update2_v4";
//             let mut opts2 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_pool_cleanup_period(Duration::from_millis(20)).with_connection_ttl(Some(Duration::from_millis(20)));
//             opts2.add_bind_address(next_socket_addr_v4(0));

//             serf_update::<
//               QuicTransport<
//                 SmolStr,
//                 SocketAddrResolver<[< $rt:camel Runtime >]>,
//                 Quinn<[< $rt:camel Runtime >]>,
//                 Lpe<SmolStr, SocketAddr>,
//                 [< $rt:camel Runtime >],
//               >,
//               _,
//             >(opts, opts2, |id, addr| async move {
//               let mut opts2 = QuicTransportOptions::with_stream_layer_options(id, quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_pool_cleanup_period(Duration::from_millis(20)).with_connection_ttl(Some(Duration::from_millis(20)));
//               opts2.add_bind_address(addr);
//               opts2
//             }).await
//           });
//         }

//         #[test]
//         fn test_serf_update_v6() {
//           [< $rt:snake _run >](async move {
//             let name = "serf_update1_v6";
//             let mut opts = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_pool_cleanup_period(Duration::from_millis(20)).with_connection_ttl(Some(Duration::from_millis(20)));
//             opts.add_bind_address(next_socket_addr_v6());

//             let name = "serf_update2_v6";
//             let mut opts2 = QuicTransportOptions::with_stream_layer_options(SmolStr::new(name), quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_pool_cleanup_period(Duration::from_millis(20)).with_connection_ttl(Some(Duration::from_millis(20)));
//             opts2.add_bind_address(next_socket_addr_v6());

//             serf_update::<
//               QuicTransport<
//                 SmolStr,
//                 SocketAddrResolver<[< $rt:camel Runtime >]>,
//                 Quinn<[< $rt:camel Runtime >]>,
//                 Lpe<SmolStr, SocketAddr>,
//                 [< $rt:camel Runtime >],
//               >,
//               _,
//             >(opts, opts2, |id, addr| async move {
//               let mut opts2 = QuicTransportOptions::with_stream_layer_options(id, quinn_stream_layer_with_connect_timeout::<[< $rt:camel Runtime >]>(Duration::from_millis(20)).await).with_connection_ttl(Some(Duration::from_millis(20)));
//               opts2.add_bind_address(addr);
//               opts2
//             }).await
//           });
//         }
//       }
//     }
//   };
// }

// #[cfg(feature = "tokio")]
// test_mod!(tokio);

// #[cfg(feature = "async-std")]
// test_mod!(async_std);

// #[cfg(feature = "smol")]
// test_mod!(smol);
