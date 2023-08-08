use super::*;

use std::sync::{Arc, OnceLock};

use showbiz_core::{
  async_trait, bytes::Bytes, delegate::Delegate as ShowbizDelegate, transport::Transport, Message,
  Node, NodeId,
};

pub struct SerfDelegateError<D: MergeDelegate>(D::Error);

impl<D: MergeDelegate> std::fmt::Display for SerfDelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl<D: MergeDelegate> std::fmt::Debug for SerfDelegateError<D> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(f, "{:?}", self.0)
  }
}

impl<D: MergeDelegate> std::error::Error for SerfDelegateError<D> {}

pub(crate) struct SerfDelegate<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider> {
  serf: OnceLock<Serf<T, D, O>>,
  merge_delegate: Option<D>,
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> SerfDelegate<T, D, O> {
  pub(crate) fn new(d: Option<D>) -> Self {
    Self {
      serf: OnceLock::new(),
      merge_delegate: d,
    }
  }

  pub(crate) fn store(&self, s: Serf<T, D, O>) {
    // No error, we never call this in parallel
    let _ = self.serf.set(s);
  }

  fn this(&self) -> &Serf<T, D, O> {
    self.serf.get().unwrap()
  }
}

#[async_trait::async_trait]
impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> ShowbizDelegate
  for SerfDelegate<T, D, O>
{
  type Error = SerfDelegateError<D>;

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

  async fn notify_merge(&self, peers: Vec<Node>) -> Result<(), Self::Error> {
    if let Some(ref d) = self.merge_delegate {
      return d.notify_merge(peers).await.map_err(SerfDelegateError);
    }
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
  async fn notify_merge(&self, members: Vec<Node>) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultMergeDelegate;

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl MergeDelegate for DefaultMergeDelegate {
  type Error = std::convert::Infallible;

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, _members: Vec<Node>) -> Result<(), Self::Error> {
    Ok(())
  }
}
