use memberlist_core::types::{SecretKey, SecretKeys};
use smol_str::SmolStr;

use std::collections::HashMap;

/// KeyRequest is used to contain input parameters which get broadcasted to all
/// nodes as part of a key query operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct KeyRequestMessage {
  pub(crate) key: Option<SecretKey>,
}

/// Key
#[viewit::viewit]
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
#[cfg(feature = "encryption")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct KeyResponseMessage {
  /// Indicates true/false if there were errors or not
  result: bool,
  /// Contains error messages or other information
  msg: SmolStr,
  /// Used in listing queries to relay a list of installed keys
  keys: SecretKeys,
  /// Used in listing queries to relay the primary key
  primary_key: Option<SecretKey>,
}

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
