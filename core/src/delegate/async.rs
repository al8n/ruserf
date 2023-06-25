use std::sync::Arc;

use agnostic::Runtime;
use showbiz_core::{
  async_trait, bytes::Bytes, delegate::Delegate as ShowbizDelegate, transport::Transport, Message,
  Node, NodeId,
};

use crate::Serf;

#[derive(Debug)]
pub struct SerfDelegateError;

impl std::fmt::Display for SerfDelegateError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "void delegate error")
  }
}

impl std::error::Error for SerfDelegateError {}

pub(crate) struct SerfDelegate<D: MergeDelegate, T: Transport<Runtime = R>, R: Runtime>(
  pub(crate) Serf<D, T, R>,
);

#[async_trait::async_trait]
impl<D: MergeDelegate, T: Transport<Runtime = R>, R: Runtime> ShowbizDelegate for SerfDelegate<D, T, R> {
  type Error = SerfDelegateError;

  fn node_meta(&self, _limit: usize) -> Bytes {
    Bytes::new()
  }

  async fn notify_user_msg(&self, _msg: Bytes) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn get_broadcasts(
    &self,
    _overhead: usize,
    _limit: usize,
  ) -> Result<Vec<Message>, Self::Error> {
    Ok(Vec::new())
  }

  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  async fn merge_remote_state(&self, _buf: Bytes, _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_join(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_leave(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_update(&self, _node: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_alive(&self, _peer: Arc<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_conflict(
    &self,
    _existing: Arc<Node>,
    _other: Arc<Node>,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_merge(&self, _peers: Vec<Node>) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    Ok(Bytes::new())
  }

  async fn notify_ping_complete(
    &self,
    _node: Arc<Node>,
    _rtt: std::time::Duration,
    _payload: Bytes,
  ) -> Result<(), Self::Error> {
    Ok(())
  }

  #[inline]
  fn disable_reliable_pings(&self, _node: &NodeId) -> bool {
    false
  }
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
pub trait MergeDelegate: Send + Sync + 'static {
  type Error: std::error::Error + Send + Sync + 'static;

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, members: &[crate::Member]) -> Result<(), Self::Error>;
}
