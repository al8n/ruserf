use std::{collections::HashMap, time::Duration};

use agnostic::Runtime;
use showbiz_core::{humantime_serde, NodeId, transport::Transport};
use serde::{Serialize, Deserialize};

use crate::{Serf, delegate::MergeDelegate, clock::LamportTime};


/// Provided to [`Serf::query`] to configure the parameters of the
/// query. If not provided, sane defaults will be used.
#[viewit::viewit]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
  /// If provided, we restrict the nodes that should respond to those
	/// with names in this list
  filter_nodes: Vec<NodeId>,

  /// Maps a tag name to a regular expression that is applied
	/// to restrict the nodes that should respond
  filter_tags: HashMap<String, String>,

  /// If true, we are requesting an delivery acknowledgement from
	/// every node that meets the filter requirement. This means nodes
	/// the receive the message but do not pass the filters, will not
	/// send an ack.
  request_ack: bool,

  /// RelayFactor controls the number of duplicate responses to relay
	/// back to the sender through other nodes for redundancy.
  relay_factor: u8,

  /// The timeout limits how long the query is left open. If not provided,
	/// then a default timeout is used based on the configuration of Serf
  #[serde(with = "humantime_serde")]
  timeout: Duration,
}

/// Returned for each new Query. It is used to collect
/// Ack's as well as responses and to provide those back to a client.
pub struct QueryResponse {
  /// The duration of the query 
  deadline: Duration,

  /// The query id
  id: u32,

  /// Stores the LTime of the query
  ltime: LamportTime,

  closed: bool,
  
}

impl<D, T, R> Serf<D, T, R>
where
  D: MergeDelegate,
  T: Transport<Runtime = R>,
  R: Runtime,
{
  /// Returns the default timeout value for a query
  /// Computed as 
  /// ```text
  /// gossip_interval * query_timeout_mult * log(N+1)
  /// ```
  pub async fn default_query_timeout(&self) -> Duration {
    let n = self.inner.memberlist.num_members().await;
    let mut timeout = self.inner.opts.showbiz_options.gossip_interval();
    timeout *= self.inner.opts.query_timeout_mult as u32;
    timeout *= (f64::log10((n + 1) as f64) + 0.5) as u32; // Using ceil approximation
    timeout
  }

  /// Used to return the default query parameters
  pub async fn default_query_param(&self) -> QueryParam {
    QueryParam {
      filter_nodes: vec![],
      filter_tags: HashMap::new(),
      request_ack: false,
      relay_factor: 0,
      timeout: self.default_query_timeout().await,
    }
  }
}