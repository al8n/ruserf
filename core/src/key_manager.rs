use std::{collections::HashMap, sync::OnceLock};

use async_channel::Receiver;
use async_lock::RwLock;
use futures::StreamExt;
use memberlist_core::{
  bytes::{BufMut, BytesMut},
  tracing,
  transport::{AddressResolver, Transport},
  types::SecretKey,
  CheapClone,
};
use smol_str::SmolStr;

use crate::event::{
  INTERNAL_INSTALL_KEY, INTERNAL_LIST_KEYS, INTERNAL_REMOVE_KEY, INTERNAL_USE_KEY,
};

use super::{
  delegate::{Delegate, TransformDelegate},
  error::Error,
  serf::{NodeResponse, QueryResponse},
  types::{KeyRequestMessage, MessageType, SerfMessage},
  Serf,
};

/// KeyResponse is used to relay a query for a list of all keys in use.
#[derive(Default)]
pub struct KeyResponse<I> {
  /// Map of node id to response message
  messages: HashMap<I, SmolStr>,
  /// Total nodes memberlist knows of
  num_nodes: usize,
  /// Total responses received
  num_resp: usize,
  /// Total errors from request
  num_err: usize,

  /// A mapping of the base64-encoded value of the key bytes to the
  /// number of nodes that have the key installed.
  keys: HashMap<SecretKey, usize>,

  /// A mapping of the base64-encoded value of the primary
  /// key bytes to the number of nodes that have the key installed.
  primary_keys: HashMap<SecretKey, usize>,
}

/// KeyRequestOptions is used to contain optional parameters for a keyring operation
pub struct KeyRequestOptions {
  /// The number of duplicate query responses to send by relaying through
  /// other nodes, for redundancy
  pub relay_factor: u8,
}

/// `KeyManager` encapsulates all functionality within Serf for handling
/// encryption keyring changes across a cluster.
pub struct KeyManager<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  serf: OnceLock<Serf<T, D>>,
  /// The lock is used to serialize keys related handlers
  l: RwLock<()>,
}

impl<T, D> KeyManager<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub(crate) fn new() -> Self {
    Self {
      serf: OnceLock::new(),
      l: RwLock::new(()),
    }
  }

  pub(crate) fn store(&self, serf: Serf<T, D>) {
    // No error handling here, because we never call this in parallel
    let _ = self.serf.set(serf);
  }

  /// Handles broadcasting a query to all members and gathering
  /// responses from each of them, returning a list of messages from each node
  /// and any applicable error conditions.
  pub async fn install_key(
    &self,
    key: SecretKey,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse<T::Id>, Error<T, D>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), INTERNAL_INSTALL_KEY, opts)
      .await
  }

  /// Handles broadcasting a primary key change to all members in the
  /// cluster, and gathering any response messages. If successful, there should
  /// be an empty KeyResponse returned.
  pub async fn use_key(
    &self,
    key: SecretKey,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse<T::Id>, Error<T, D>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), INTERNAL_USE_KEY, opts)
      .await
  }

  /// Handles broadcasting a key to the cluster for removal. Each member
  /// will receive this event, and if they have the key in their keyring, remove
  /// it. If any errors are encountered, RemoveKey will collect and relay them.
  pub async fn remove_key(
    &self,
    key: SecretKey,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse<T::Id>, Error<T, D>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), INTERNAL_REMOVE_KEY, opts)
      .await
  }

  /// Used to collect installed keys from members in a Serf cluster
  /// and return an aggregated list of all installed keys. This is useful to
  /// operators to ensure that there are no lingering keys installed on any agents.
  /// Since having multiple keys installed can cause performance penalties in some
  /// cases, it's important to verify this information and remove unneeded keys.
  pub async fn list_keys(&self) -> Result<KeyResponse<T::Id>, Error<T, D>> {
    let _mu = self.l.read().await;
    self
      .handle_key_request(None, INTERNAL_LIST_KEYS, None)
      .await
  }

  pub(crate) async fn handle_key_request(
    &self,
    key: Option<SecretKey>,
    ty: &str,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse<T::Id>, Error<T, D>> {
    let kr = KeyRequestMessage { key };
    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(&kr);
    let mut buf = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type
    buf.put_u8(MessageType::KeyRequest as u8);
    buf.resize(expected_encoded_len + 1, 0);
    // Encode the query request
    let len =
      <D as TransformDelegate>::encode_message(&kr, &mut buf[1..]).map_err(Error::transform)?;

    debug_assert_eq!(
      len, expected_encoded_len,
      "expected encoded len {} mismatch the actual encoded len {}",
      expected_encoded_len, len
    );

    let serf = self.serf.get().unwrap();
    let mut q_param = serf.default_query_param().await;
    if let Some(opts) = opts {
      q_param.relay_factor = opts.relay_factor;
    }
    let qresp: QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> = serf
      .query(SmolStr::new(ty), buf.freeze(), Some(q_param))
      .await?;

    // Handle the response stream and populate the KeyResponse
    let resp = self.stream_key_response(qresp.response_rx()).await;

    // Check the response for any reported failure conditions
    if resp.num_err > 0 {
      tracing::error!(
        target = "ruserf",
        "{}/{} nodes reported failure",
        resp.num_err,
        resp.num_nodes
      );
    }

    if resp.num_resp != resp.num_nodes {
      tracing::error!(
        target = "ruserf",
        "{}/{} nodes responded success",
        resp.num_resp,
        resp.num_nodes
      );
    }

    Ok(resp)
  }

  async fn stream_key_response(
    &self,
    ch: Receiver<NodeResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> KeyResponse<T::Id> {
    let mut resp = KeyResponse {
      num_nodes: self.serf.get().unwrap().num_nodes().await,
      messages: HashMap::new(),
      num_resp: 0,
      num_err: 0,
      keys: HashMap::new(),
      primary_keys: HashMap::new(),
    };
    futures::pin_mut!(ch);
    while let Some(r) = ch.next().await {
      resp.num_resp += 1;

      // Decode the response
      if !r.payload.is_empty() || r.payload[0] != MessageType::KeyResponse as u8 {
        resp.messages.insert(
          r.from.id().cheap_clone(),
          SmolStr::new(format!(
            "Invalid key query response type: {:?}",
            r.payload.as_ref()
          )),
        );
        resp.num_err += 1;

        if resp.num_resp == resp.num_nodes {
          return resp;
        }
        continue;
      }

      let node_response = match <D as TransformDelegate>::decode_message(&r.payload[1..]) {
        Ok((_, nr)) => match nr {
          SerfMessage::KeyResponse(kr) => kr,
          msg => {
            resp.messages.insert(
              r.from.id().cheap_clone(),
              SmolStr::new(format!(
                "Invalid key query response type: {:?}",
                msg.as_str()
              )),
            );
            resp.num_err += 1;

            if resp.num_resp == resp.num_nodes {
              return resp;
            }
            continue;
          }
        },
        Err(e) => {
          resp.messages.insert(
            r.from.id().cheap_clone(),
            SmolStr::new(format!("Failed to decode key query response: {:?}", e)),
          );
          resp.num_err += 1;

          if resp.num_resp == resp.num_nodes {
            return resp;
          }
          continue;
        }
      };

      if !node_response.result {
        resp
          .messages
          .insert(r.from.id().cheap_clone(), node_response.msg);
        resp.num_err += 1;
      } else if node_response.result && node_response.msg.is_empty() {
        tracing::warn!(target = "ruserf", "{}", node_response.msg);
        resp
          .messages
          .insert(r.from.id().cheap_clone(), node_response.msg);
      }

      // Currently only used for key list queries, this adds keys to a counter
      // and increments them for each node response which contains them.
      for k in node_response.keys {
        let count = resp.keys.entry(k).or_insert(0);
        *count += 1;
      }

      if let Some(pk) = node_response.primary_key {
        let ctr = resp.primary_keys.entry(pk).or_insert(0);
        *ctr += 1;
      }

      // Return early if all nodes have responded. This allows us to avoid
      // waiting for the full timeout when there is nothing left to do.
      if resp.num_resp == resp.num_nodes {
        return resp;
      }
    }
    resp
  }
}
