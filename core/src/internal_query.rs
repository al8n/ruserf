use std::future::Future;

use agnostic::Runtime;
use async_channel::{bounded, Receiver, Sender};
use either::Either;
use showbiz_core::{
  futures_util::{self, FutureExt},
  security::SecretKey,
  tracing,
  transport::Transport,
  Message, NodeId,
};
use smol_str::SmolStr;

use crate::{
  codec::NodeIdCodec,
  delegate::MergeDelegate,
  error::Error,
  event::{Event, EventKind, InternalQueryEventType, QueryEvent},
  types::{decode_message, encode_message, MessageType, QueryResponseMessage},
  KeyRequest,
};

/// Used to compute the max number of keys in a list key
/// response. eg 1024/25 = 40. a message with max size of 1024 bytes cannot
/// contain more than 40 keys. There is a test
/// (TestSerfQueries_estimateMaxKeysInListKeyResponse) which does the
/// computation and in case of changes, the value can be adjusted.
const MIN_ENCODED_KEY_LENGTH: usize = 25;

pub(crate) struct SerfQueries<T, D>
where
  D: MergeDelegate,
  T: Transport,
{
  in_rx: Receiver<Event<T, D>>,
  out_tx: Sender<Event<T, D>>,
  shutdown_rx: Receiver<()>,
}

impl<D, T> SerfQueries<T, D>
where
  D: MergeDelegate,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as futures_util::Stream>::Item: Send,
{
  pub(crate) fn new(out_tx: Sender<Event<T, D>>, shutdown_rx: Receiver<()>) -> Sender<Event<T, D>> {
    let (in_tx, in_rx) = bounded(1024);
    let this = Self {
      in_rx,
      out_tx,
      shutdown_rx,
    };
    this.stream();
    in_tx
  }

  /// A long running routine to ingest the event stream
  fn stream(self) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          ev = self.in_rx.recv().fuse() => {
            match ev {
              Ok(ev) => {
                // Check if this is a query we should process
                if ev.is_internal_query() {
                  <T::Runtime as Runtime>::spawn_detach(async move {
                    Self::handle_query(ev).await;
                  });
                } else if let Err(e) = self.out_tx.send(ev).await {
                  tracing::error!(target="ruserf", err=%e, "failed to send event back in serf query thread");
                  return;
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
    });
  }

  async fn handle_query(ev: Event<T, D>) {
    macro_rules! handle_query {
      ($ev: expr) => {{
        match $ev {
          EventKind::InternalQuery { ty, query: ev } => match ty {
            InternalQueryEventType::Ping => {}
            InternalQueryEventType::Conflict => {
              Self::handle_conflict(ev).await;
            }
            InternalQueryEventType::InstallKey => {
              Self::handle_install_key(ev).await;
            }
            InternalQueryEventType::UseKey => {
              Self::handle_use_key(ev).await;
            }
            InternalQueryEventType::RemoveKey => {
              Self::handle_remove_key(ev).await;
            }
            InternalQueryEventType::ListKey => {
              Self::handle_list_keys(ev).await;
            }
          },
          _ => unreachable!(),
        }
      }};
    }

    match ev.0 {
      Either::Left(ev) => handle_query!(ev),
      Either::Right(ev) => handle_query!(&*ev),
    }
  }

  /// invoked when we get a query that is attempting to
  /// disambiguate a name conflict. They payload is a node name, and the response
  /// should the address we believe that node is at, if any.
  async fn handle_conflict(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    // The target node id is the payload
    let id = match NodeId::decode(q.payload()) {
      Ok(id) => id,
      Err(e) => {
        tracing::error!(target = "ruserf", err = %e, "conflict handler: failed to decode node id");
        return;
      }
    };

    // Do not respond to the query if it is about us
    if id.eq(q.ctx.this.inner.memberlist.local_id()) {
      return;
    }

    tracing::debug!(
      target = "ruserf",
      "got conflict resolution query for '{}'",
      id
    );

    // Look for the member info
    let out = {
      let members = q.ctx.this.inner.members.read().await;
      members.states.get(&id).cloned()
    };

    // Encode the response
    match out {
      Some(state) => match encode_message(MessageType::ConflictResponse, state.member()) {
        Ok(msg) => {
          if let Err(e) = q.respond(msg.into()).await {
            tracing::error!(target="ruserf", err=%e, "failed to respond to conflict query");
          }
        }
        Err(e) => {
          tracing::error!(target="ruserf", err=%e, "failed to encode conflict query response");
        }
      },
      None => {
        tracing::warn!(target = "ruserf", "no member status found for '{}'", id);
        // TODO: consider send something back?
        if let Err(e) = q.respond(Default::default()).await {
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
  async fn handle_install_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = NodeKeyResponse::default();
    let req = match decode_message::<KeyRequest>(&q.payload[1..]) {
      Ok(req) => req,
      Err(e) => {
        tracing::error!(target="ruserf", err=%e, "failed to decode key request");
        Self::send_key_response(q, &mut response).await;
        return;
      }
    };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        target = "ruserf",
        err = "no keyring to modify (encryption not enabled)"
      );
      response.msg = SmolStr::new("No keyring to modify (encryption not enabled)");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!(target = "ruserf", "received install-key query");
    if let Some(kr) = q.ctx.this.inner.memberlist.keyring() {
      kr.insert(req.key.unwrap());
      if q.ctx.this.inner.opts.keyring_file.is_some() {
        if let Err(e) = q.ctx.this.write_keyring_file() {
          tracing::error!(target="ruserf", err=%e, "failed to write keyring file");
          response.msg = SmolStr::new(e.to_string());
          Self::send_key_response(q, &mut response).await;
          return;
        }
      }

      response.result = true;
      Self::send_key_response(q, &mut response).await;
    } else {
      tracing::error!(
        target = "ruserf",
        err = "encryption enabled but keyring is empty"
      );
      response.msg = SmolStr::new("encryption enabled but keyring is empty");
      Self::send_key_response(q, &mut response).await;
    }
  }

  async fn handle_use_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = NodeKeyResponse::default();

    let req = match decode_message::<KeyRequest>(&q.payload[1..]) {
      Ok(req) => req,
      Err(e) => {
        tracing::error!(target="ruserf", err=%e, "failed to decode key request");
        Self::send_key_response(q, &mut response).await;
        return;
      }
    };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        target = "ruserf",
        err = "no keyring to modify (encryption not enabled)"
      );
      response.msg = SmolStr::new("No keyring to modify (encryption not enabled)");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!(target = "ruserf", "received use-key query");
    if let Some(kr) = q.ctx.this.inner.memberlist.keyring() {
      if let Err(e) = kr.use_key(&req.key.unwrap()) {
        tracing::error!(target="ruserf", err=%e, "failed to change primary key");
        response.msg = SmolStr::new(e.to_string());
        Self::send_key_response(q, &mut response).await;
        return;
      }

      if q.ctx.this.inner.opts.keyring_file.is_some() {
        if let Err(e) = q.ctx.this.write_keyring_file() {
          tracing::error!(target="ruserf", err=%e, "failed to write keyring file");
          response.msg = SmolStr::new(e.to_string());
          Self::send_key_response(q, &mut response).await;
          return;
        }
      }

      response.result = true;
      Self::send_key_response(q, &mut response).await;
    } else {
      tracing::error!(
        target = "ruserf",
        err = "encryption enabled but keyring is empty"
      );
      response.msg = SmolStr::new("encryption enabled but keyring is empty");
      Self::send_key_response(q, &mut response).await;
    }
  }

  async fn handle_remove_key(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = NodeKeyResponse::default();
    let req = match decode_message::<KeyRequest>(&q.payload[1..]) {
      Ok(req) => req,
      Err(e) => {
        tracing::error!(target="ruserf", err=%e, "failed to decode key request");
        Self::send_key_response(q, &mut response).await;
        return;
      }
    };

    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        target = "ruserf",
        err = "no keyring to modify (encryption not enabled)"
      );
      response.msg = SmolStr::new("No keyring to modify (encryption not enabled)");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!(target = "ruserf", "received remove-key query");
    if let Some(kr) = q.ctx.this.inner.memberlist.keyring() {
      if let Err(e) = kr.remove(&req.key.unwrap()) {
        tracing::error!(target="ruserf", err=%e, "failed to remove key");
        response.msg = SmolStr::new(e.to_string());
        Self::send_key_response(q, &mut response).await;
        return;
      }

      if q.ctx.this.inner.opts.keyring_file.is_some() {
        if let Err(e) = q.ctx.this.write_keyring_file() {
          tracing::error!(target="ruserf", err=%e, "failed to write keyring file");
          response.msg = SmolStr::new(e.to_string());
          Self::send_key_response(q, &mut response).await;
          return;
        }
      }

      response.result = true;
      Self::send_key_response(q, &mut response).await;
    } else {
      tracing::error!(
        target = "ruserf",
        err = "encryption enabled but keyring is empty"
      );
      response.msg = SmolStr::new("encryption enabled but keyring is empty");
      Self::send_key_response(q, &mut response).await;
    }
  }

  /// Invoked when a query is received to return a list of all
  /// installed keys the Serf instance knows of.
  async fn handle_list_keys(ev: impl AsRef<QueryEvent<T, D>> + Send) {
    let q = ev.as_ref();
    let mut response = NodeKeyResponse::default();
    if !q.ctx.this.encryption_enabled() {
      tracing::error!(
        target = "ruserf",
        err = "keyring is empty (encryption not enabled)"
      );
      response.msg = SmolStr::new("Keyring is empty (encryption not enabled)");
      Self::send_key_response(q, &mut response).await;
      return;
    }

    tracing::info!(target = "ruserf", "received list-keys query");
    if let Some(kr) = q.ctx.this.inner.memberlist.keyring() {
      for k in kr.keys() {
        response.keys.push(k);
      }

      let primary_key = kr.primary_key();
      response.primary_key = Some(primary_key);
      response.result = true;
      Self::send_key_response(q, &mut response).await;
    } else {
      tracing::error!(
        target = "ruserf",
        err = "encryption enabled but keyring is empty"
      );
      response.msg = SmolStr::new("encryption enabled but keyring is empty");
      Self::send_key_response(q, &mut response).await;
    }
  }

  fn key_list_response_with_correct_size(
    q: &QueryEvent<T, D>,
    resp: &mut NodeKeyResponse,
  ) -> Result<(Message, QueryResponseMessage), Error<T, D>> {
    let actual = resp.keys.len();

    // if the provided list of keys is smaller then the max allowed, just iterate over it
    // to avoid an out of bound access when truncating
    let max_list_keys =
      (q.ctx.this.inner.opts.query_response_size_limit / MIN_ENCODED_KEY_LENGTH).min(actual);

    for i in (0..=max_list_keys).rev() {
      let buf = encode_message(MessageType::KeyResponse, resp)?;

      // create response
      let qresp = q.create_response(buf.into());

      // encode response
      let raw = encode_message(MessageType::QueryResponse, &qresp)?;

      // Check the size limit
      if q.check_response_size(raw.as_slice()).is_err() {
        resp.keys.drain(i..);
        resp.msg = SmolStr::new(format!(
          "truncated key list response, showing first {} of {} keys",
          i, actual
        ));
        continue;
      }

      if actual > i {
        tracing::warn!(target = "ruserf", "{}", resp.msg);
      }
      return Ok((raw, qresp));
    }
    Err(Error::FailTruncateResponse)
  }

  async fn send_key_response(q: &QueryEvent<T, D>, resp: &mut NodeKeyResponse) {
    match q.name().as_str() {
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
      _ => match encode_message(MessageType::KeyResponse, resp) {
        Ok(msg) => {
          if let Err(e) = q.respond(msg.into()).await {
            tracing::error!(target="ruserf", err=%e, "failed to respond to key query");
          }
        }
        Err(e) => {
          tracing::error!(target="ruserf", err=%e, "failed to encode key response");
        }
      },
    }
  }
}

#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(crate) struct NodeKeyResponse {
  /// Indicates true/false if there were errors or not
  result: bool,
  /// Contains error messages or other information
  msg: SmolStr,
  /// Used in listing queries to relay a list of installed keys
  keys: Vec<SecretKey>,
  /// Used in listing queries to relay the primary key
  primary_key: Option<SecretKey>,
}
