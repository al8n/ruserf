use async_channel::{bounded, Receiver, Sender};
use futures::FutureExt;
use memberlist_core::{
  agnostic_lite::{AsyncSpawner, RuntimeLite},
  bytes::{BufMut, Bytes, BytesMut},
  tracing,
  transport::{AddressResolver, Transport},
};

use crate::{
  delegate::{Delegate, TransformDelegate},
  event::{CrateEvent, InternalQueryEvent, QueryEvent},
  types::MessageType,
};

#[cfg(feature = "encryption")]
use crate::{
  error::Error,
  types::{KeyResponseMessage, SerfMessage},
};

#[cfg(feature = "encryption")]
use smol_str::SmolStr;

/// Used to compute the max number of keys in a list key
/// response. eg 1024/25 = 40. a message with max size of 1024 bytes cannot
/// contain more than 40 keys. There is a test
/// (TestSerfQueries_estimateMaxKeysInListKeyResponse) which does the
/// computation and in case of changes, the value can be adjusted.
#[cfg(feature = "encryption")]
const MIN_ENCODED_KEY_LENGTH: usize = 25;

pub(crate) struct SerfQueries<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  in_rx: Receiver<CrateEvent<T, D>>,
  out_tx: Option<Sender<CrateEvent<T, D>>>,
  shutdown_rx: Receiver<()>,
}

impl<D, T> SerfQueries<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  #[allow(clippy::new_ret_no_self)]
  pub(crate) fn new(
    out_tx: Option<Sender<CrateEvent<T, D>>>,
    shutdown_rx: Receiver<()>,
  ) -> (
    Sender<CrateEvent<T, D>>,
    <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()>,
  ) {
    let (in_tx, in_rx) = bounded(1024);
    let this = Self {
      in_rx,
      out_tx,
      shutdown_rx,
    };
    (in_tx, this.stream())
  }

  /// A long running routine to ingest the event stream
  fn stream(self) -> <<T::Runtime as RuntimeLite>::Spawner as AsyncSpawner>::JoinHandle<()> {
    <T::Runtime as RuntimeLite>::spawn(async move {
      loop {
        futures::select! {
          ev = self.in_rx.recv().fuse() => {
            match ev {
              Ok(ev) => {
                // Check if this is a query we should process
                if ev.is_internal_query() {
                  <T::Runtime as RuntimeLite>::spawn_detach(async move {
                    Self::handle_query(ev).await;
                  });
                } else if let Some(ref tx) = self.out_tx {
                  if let Err(e) = tx.send(ev).await {
                    tracing::error!(target="ruserf", err=%e, "failed to send event back in serf query thread");
                  }
                }
              },
              Err(err) => {
                tracing::error!(target="ruserf", err=%err, "failed to receive event in serf query thread");
                return;
              }
            }
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    })
  }

  async fn handle_query(ev: CrateEvent<T, D>) {
    macro_rules! handle_query {
      ($ev: expr) => {{
        match $ev {
          CrateEvent::InternalQuery { kind, query } => match kind {
            InternalQueryEvent::Ping => {}
            InternalQueryEvent::Conflict(conflict) => {
              Self::handle_conflict(&conflict, &query).await;
            }
            #[cfg(feature = "encryption")]
            InternalQueryEvent::InstallKey => {
              Self::handle_install_key(&query).await;
            }
            #[cfg(feature = "encryption")]
            InternalQueryEvent::UseKey => {
              Self::handle_use_key(&query).await;
            }
            #[cfg(feature = "encryption")]
            InternalQueryEvent::RemoveKey => {
              Self::handle_remove_key(&query).await;
            }
            #[cfg(feature = "encryption")]
            InternalQueryEvent::ListKey => {
              Self::handle_list_keys(&query).await;
            }
          },
          _ => unreachable!(),
        }
      }};
    }

    handle_query!(ev)
  }

  /// invoked when we get a query that is attempting to
  /// disambiguate a name conflict. They payload is a node name, and the response
  /// should the address we believe that node is at, if any.
  async fn handle_conflict(conflict: &T::Id, ev: &QueryEvent<T, D>) {
    // The target node id is the payload

    // Do not respond to the query if it is about us
    if conflict.eq(ev.ctx.this.inner.memberlist.local_id()) {
      return;
    }

    tracing::error!(
      "ruserf: local {} got conflict resolution query for '{}'",
      ev.ctx.this.inner.memberlist.local_id(),
      conflict
    );

    // tracing::debug!("ruserf: got conflict resolution query for '{}'", conflict);

    // Look for the member info
    let out = {
      let members = ev.ctx.this.inner.members.read().await;
      members.states.get(conflict).cloned()
    };

    // Encode the response
    match out {
      Some(state) => {
        let member = state.member();
        let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(member);
        let mut raw = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type
        raw.put_u8(MessageType::ConflictResponse as u8);
        raw.resize(expected_encoded_len + 1, 0);
        match <D as TransformDelegate>::encode_message(member, &mut raw[1..]) {
          Ok(len) => {
            debug_assert_eq!(
              len, expected_encoded_len,
              "expected encoded len {} mismatch the actual encoded len {}",
              expected_encoded_len, len
            );

            if let Err(e) = ev.respond(raw.freeze()).await {
              tracing::error!(target="ruserf", err=%e, "failed to respond to conflict query");
            }
          }
          Err(e) => {
            tracing::error!(target="ruserf", err=%e, "failed to encode conflict query response");
          }
        }
      }
      None => {
        tracing::warn!("ruserf: no member status found for '{}'", conflict);
        // TODO: consider send something back?
        if let Err(e) = ev.respond(Bytes::new()).await {
          tracing::error!(target="ruserf", err=%e, "failed to respond to conflict query");
        }
      }
    }
  }

  /// Invoked whenever a new encryption key is received from
  /// another member in the cluster, and handles the process of installing it onto
  /// the memberlist keyring. This type of query may fail if the provided key does
  /// not fit the constraints that memberlist enforces. If the query fails, the
  /// response will contain the error message so that it may be relayed.
  #[cfg(feature = "encryption")]
  async fn handle_install_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = KeyResponseMessage::default();
    let req =
      match <D as TransformDelegate>::decode_message(MessageType::KeyRequest, &q.payload[1..]) {
        Ok((_, msg)) => match msg {
          SerfMessage::KeyRequest(req) => req,
          msg => {
            tracing::error!(
              err = "unexpected message type",
              "ruserf: {}",
              msg.ty().as_str()
            );
            Self::send_key_response(q, &mut response).await;
            return;
          }
        },
        Err(e) => {
          tracing::error!(err=%e, "ruserf: failed to decode key request");
          Self::send_key_response(q, &mut response).await;
          return;
        }
      };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        err = "encryption is not enabled",
        "ruserf: fail to handle install key"
      );
      response.message = SmolStr::new("encryption is not enabled");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!("ruserf: received install-key query");
    let kr = q.ctx.this.inner.memberlist.keyring();
    match kr {
      Some(kr) => {
        kr.insert(req.key.unwrap()).await;
        if q.ctx.this.inner.opts.keyring_file.is_some() {
          if let Err(e) = q.ctx.this.write_keyring_file().await {
            tracing::error!(err=%e, "ruserf: failed to write keyring file");
            response.message = SmolStr::new(e.to_string());
            Self::send_key_response(q, &mut response).await;
            return;
          }
        }

        response.result = true;
        Self::send_key_response(q, &mut response).await;
      }
      None => {
        tracing::error!(
          err = "encryption enabled but keyring is empty",
          "ruserf: fail to handle install key"
        );
        response.message = SmolStr::new("encryption enabled but keyring is empty");
        Self::send_key_response(q, &mut response).await;
      }
    }
  }

  #[cfg(feature = "encryption")]
  async fn handle_use_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = KeyResponseMessage::default();

    let req =
      match <D as TransformDelegate>::decode_message(MessageType::KeyRequest, &q.payload[1..]) {
        Ok((_, msg)) => match msg {
          SerfMessage::KeyRequest(req) => req,
          msg => {
            tracing::error!(
              err = "unexpected message type",
              "ruserf: {}",
              msg.ty().as_str()
            );
            Self::send_key_response(q, &mut response).await;
            return;
          }
        },
        Err(e) => {
          tracing::error!(err=%e, "ruserf: failed to decode key request");
          Self::send_key_response(q, &mut response).await;
          return;
        }
      };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        err = "encryption is not enabled",
        "ruserf: fail to handle use key"
      );
      response.message = SmolStr::new("encryption is not enabled");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!("ruserf: received use-key query");
    let kr = q.ctx.this.inner.memberlist.keyring();
    match kr {
      Some(kr) => {
        if let Err(e) = kr.use_key(&req.key.unwrap()).await {
          tracing::error!(err=%e, "ruserf: failed to change primary key");
          response.message = SmolStr::new(e.to_string());
          Self::send_key_response(q, &mut response).await;
          return;
        }

        if q.ctx.this.inner.opts.keyring_file.is_some() {
          if let Err(e) = q.ctx.this.write_keyring_file().await {
            tracing::error!(err=%e, "ruserf: failed to write keyring file");
            response.message = SmolStr::new(e.to_string());
            Self::send_key_response(q, &mut response).await;
            return;
          }
        }

        response.result = true;
        Self::send_key_response(q, &mut response).await;
      }
      None => {
        tracing::error!(
          err = "encryption enabled but keyring is empty",
          "ruserf: fail to handle use key"
        );
        response.message = SmolStr::new("encryption enabled but keyring is empty");
        Self::send_key_response(q, &mut response).await;
      }
    }
  }

  #[cfg(feature = "encryption")]
  async fn handle_remove_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = KeyResponseMessage::default();

    let req =
      match <D as TransformDelegate>::decode_message(MessageType::KeyRequest, &q.payload[1..]) {
        Ok((_, msg)) => match msg {
          SerfMessage::KeyRequest(req) => req,
          msg => {
            tracing::error!(
              err = "unexpected message type",
              "ruserf: {}",
              msg.ty().as_str()
            );
            Self::send_key_response(q, &mut response).await;
            return;
          }
        },
        Err(e) => {
          tracing::error!(target="ruserf", err=%e, "failed to decode key request");
          Self::send_key_response(q, &mut response).await;
          return;
        }
      };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        err = "encryption is not enabled",
        "ruserf: fail to handle remove key"
      );
      response.message = SmolStr::new("encryption is not enabled");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!("ruserf: received remove-key query");
    let kr = q.ctx.this.inner.memberlist.keyring();
    match kr {
      Some(kr) => {
        if let Err(e) = kr.remove(&req.key.unwrap()).await {
          tracing::error!(err=%e, "ruserf: failed to remove key");
          response.message = SmolStr::new(e.to_string());
          Self::send_key_response(q, &mut response).await;
          return;
        }

        if q.ctx.this.inner.opts.keyring_file.is_some() {
          if let Err(e) = q.ctx.this.write_keyring_file().await {
            tracing::error!(err=%e, "ruserf: failed to write keyring file");
            response.message = SmolStr::new(e.to_string());
            Self::send_key_response(q, &mut response).await;
            return;
          }
        }

        response.result = true;
        Self::send_key_response(q, &mut response).await;
      }
      None => {
        tracing::error!(
          err = "encryption enabled but keyring is empty",
          "ruserf: fail to handle remove key"
        );
        response.message = SmolStr::new("encryption enabled but keyring is empty");
        Self::send_key_response(q, &mut response).await;
      }
    }
  }

  /// Invoked when a query is received to return a list of all
  /// installed keys the Serf instance knows of.
  #[cfg(feature = "encryption")]
  async fn handle_list_keys(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = KeyResponseMessage::default();
    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        err = "encryption is not enabled",
        "ruserf: fail to handle list keys"
      );
      response.message = SmolStr::new("encryption is not enabled");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!("ruserf: received list-keys query");
    let kr = q.ctx.this.inner.memberlist.keyring();
    match kr {
      Some(kr) => {
        for k in kr.keys().await {
          response.keys.push(k);
        }

        let primary_key = kr.primary_key().await;
        response.primary_key = Some(primary_key);
        response.result = true;
        Self::send_key_response(q, &mut response).await;
      }
      None => {
        tracing::error!(
          err = "encryption enabled but keyring is empty",
          "ruserf: fail to handle list keys"
        );
        response.message = SmolStr::new("encryption enabled but keyring is empty");
        Self::send_key_response(q, &mut response).await;
      }
    }
  }

  #[cfg(feature = "encryption")]
  pub(crate) fn key_list_response_with_correct_size(
    q: &QueryEvent<T, D>,
    resp: &mut KeyResponseMessage,
  ) -> Result<
    (
      Bytes,
      ruserf_types::QueryResponseMessage<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
    ),
    Error<T, D>,
  > {
    let actual = resp.keys.len();

    // if the provided list of keys is smaller then the max allowed, just iterate over it
    // to avoid an out of bound access when truncating
    let max_list_keys =
      (q.ctx.this.inner.opts.query_response_size_limit / MIN_ENCODED_KEY_LENGTH).min(actual);

    for i in (0..=max_list_keys).rev() {
      let expected_k_encoded_len = <D as TransformDelegate>::message_encoded_len(&*resp);
      let mut raw = BytesMut::with_capacity(expected_k_encoded_len + 1); // +1 for the message type
      raw.put_u8(MessageType::KeyResponse as u8);
      raw.resize(expected_k_encoded_len + 1, 0);

      let len = <D as TransformDelegate>::encode_message(&*resp, &mut raw[1..])
        .map_err(Error::transform_delegate)?;

      debug_assert_eq!(
        len, expected_k_encoded_len,
        "expected encoded len {} mismatch the actual encoded len {}",
        expected_k_encoded_len, len
      );
      let kraw = raw.freeze();

      // create response
      let qresp = q.create_response(kraw.clone());

      // encode response
      let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&qresp);
      let mut raw = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type
      raw.put_u8(MessageType::QueryResponse as u8);
      raw.resize(expected_encoded_len + 1, 0);

      let len = <D as TransformDelegate>::encode_message(&qresp, &mut raw[1..])
        .map_err(Error::transform_delegate)?;

      debug_assert_eq!(
        len, expected_encoded_len,
        "expected encoded len {} mismatch the actual encoded len {}",
        expected_encoded_len, len
      );

      let qraw = raw.freeze();

      // Check the size limit
      if q.check_response_size(&qraw).is_err() {
        resp.keys.drain(i..);
        resp.message = SmolStr::new(format!(
          "truncated key list response, showing first {} of {} keys",
          i, actual
        ));
        continue;
      }

      if actual > i {
        tracing::warn!("ruserf: {}", resp.message);
      }
      return Ok((qraw, qresp));
    }
    Err(Error::fail_truncate_response())
  }

  #[cfg(feature = "encryption")]
  async fn send_key_response(q: &QueryEvent<T, D>, resp: &mut KeyResponseMessage) {
    match q.name.as_str() {
      "ruserf-list-keys" => {
        let (raw, qresp) = match Self::key_list_response_with_correct_size(q, resp) {
          Ok((raw, qresp)) => (raw, qresp),
          Err(e) => {
            tracing::error!(target="ruserf", err=%e);
            return;
          }
        };

        if let Err(e) = q.respond_with_message_and_response(raw, qresp).await {
          tracing::error!(target="ruserf", err=%e, "failed to respond to key query");
        }
      }
      _ => {
        let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&*resp);
        let mut raw = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type
        raw.put_u8(MessageType::KeyResponse as u8);
        raw.resize(expected_encoded_len + 1, 0);
        match <D as TransformDelegate>::encode_message(&*resp, &mut raw[1..]) {
          Ok(len) => {
            debug_assert_eq!(
              len, expected_encoded_len,
              "expected encoded len {} mismatch the actual encoded len {}",
              expected_encoded_len, len
            );

            if let Err(e) = q.respond(raw.freeze()).await {
              tracing::error!(target="ruserf", err=%e, "failed to respond to key query");
            }
          }
          Err(e) => {
            tracing::error!(target="ruserf", err=%e, "failed to encode key response");
          }
        }
      }
    }
  }
}
