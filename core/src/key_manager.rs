use std::{collections::HashMap, future::Future, sync::OnceLock};

use agnostic::Runtime;
use async_channel::Receiver;
use async_lock::RwLock;
use showbiz_core::{
  futures_util::{self, Stream, StreamExt},
  security::SecretKey,
  tracing,
  transport::Transport,
  NodeId,
};
use smol_str::SmolStr;

use crate::{
  delegate::MergeDelegate,
  error::Error,
  event::InternalQueryEventType,
  internal_query::NodeKeyResponse,
  query::{NodeResponse, QueryResponse},
  types::{decode_message, encode_message, MessageType},
  ReconnectTimeoutOverrider, Serf,
};

/// KeyRequest is used to contain input parameters which get broadcasted to all
/// nodes as part of a key query operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(transparent)]
pub(crate) struct KeyRequest {
  pub(crate) key: Option<SecretKey>,
}

/// KeyResponse is used to relay a query for a list of all keys in use.
#[derive(Default)]
pub struct KeyResponse {
  /// Map of node id to response message
  messages: HashMap<NodeId, SmolStr>,
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
pub struct KeyManager<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  serf: OnceLock<Serf<T, D, O>>,
  /// The lock is used to serialize keys related handlers
  l: RwLock<()>,
}

impl<T, D, O> KeyManager<T, D, O>
where
  D: MergeDelegate,
  O: ReconnectTimeoutOverrider,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as std::future::Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as futures_util::Stream>::Item: Send,
{
  pub(crate) fn new() -> Self {
    Self {
      serf: OnceLock::new(),
      l: RwLock::new(()),
    }
  }

  pub(crate) fn store(&self, serf: Serf<T, D, O>) {
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
  ) -> Result<KeyResponse, Error<T, D, O>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), InternalQueryEventType::InstallKey, opts)
      .await
  }

  /// Handles broadcasting a primary key change to all members in the
  /// cluster, and gathering any response messages. If successful, there should
  /// be an empty KeyResponse returned.
  pub async fn use_key(
    &self,
    key: SecretKey,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse, Error<T, D, O>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), InternalQueryEventType::UseKey, opts)
      .await
  }

  /// Handles broadcasting a key to the cluster for removal. Each member
  /// will receive this event, and if they have the key in their keyring, remove
  /// it. If any errors are encountered, RemoveKey will collect and relay them.
  pub async fn remove_key(
    &self,
    key: SecretKey,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse, Error<T, D, O>> {
    let _mu = self.l.write().await;
    self
      .handle_key_request(Some(key), InternalQueryEventType::RemoveKey, opts)
      .await
  }

  /// Used to collect installed keys from members in a Serf cluster
  /// and return an aggregated list of all installed keys. This is useful to
  /// operators to ensure that there are no lingering keys installed on any agents.
  /// Since having multiple keys installed can cause performance penalties in some
  /// cases, it's important to verify this information and remove unneeded keys.
  pub async fn list_keys(&self) -> Result<KeyResponse, Error<T, D, O>> {
    let _mu = self.l.read().await;
    self
      .handle_key_request(None, InternalQueryEventType::ListKey, None)
      .await
  }

  pub(crate) async fn handle_key_request(
    &self,
    key: Option<SecretKey>,
    ty: InternalQueryEventType,
    opts: Option<KeyRequestOptions>,
  ) -> Result<KeyResponse, Error<T, D, O>> {
    // Encode the query request
    let req =
      encode_message(MessageType::KeyRequest, &KeyRequest { key }).map_err(Error::Encode)?;

    let serf = self.serf.get().unwrap();
    let mut q_param = serf.default_query_param().await;
    if let Some(opts) = opts {
      q_param.relay_factor = opts.relay_factor;
    }
    let qresp: QueryResponse = serf
      .query(SmolStr::new(ty.as_str()), req.into(), Some(q_param))
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

  async fn stream_key_response(&self, mut ch: Receiver<NodeResponse>) -> KeyResponse {
    let mut resp = KeyResponse {
      num_nodes: self.serf.get().unwrap().num_nodes().await,
      ..Default::default()
    };
    while let Some(r) = ch.next().await {
      resp.num_resp += 1;

      // Decode the response
      if !r.payload.is_empty() || r.payload[0] != MessageType::KeyResponse as u8 {
        resp.messages.insert(
          r.from.clone(),
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

      let node_response = match decode_message::<NodeKeyResponse>(&r.payload[1..]) {
        Ok(nr) => nr,
        Err(e) => {
          resp.messages.insert(
            r.from.clone(),
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
        resp.messages.insert(r.from.clone(), node_response.msg);
        resp.num_err += 1;
      } else if node_response.result && node_response.msg.is_empty() {
        tracing::warn!(target = "ruserf", "{}", node_response.msg);
        resp.messages.insert(r.from.clone(), node_response.msg);
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
