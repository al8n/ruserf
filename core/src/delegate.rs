use crate::{
  coordinate::Coordinate,
  types::{encode_message, MessageType, PushPull, PushPullRef},
};

use super::*;

use std::{
  future::Future,
  sync::{Arc, OnceLock},
};

use agnostic::Runtime;
use atomic::Atomic;
use showbiz_core::{
  async_trait,
  bytes::{BufMut, Bytes, BytesMut},
  delegate::Delegate as ShowbizDelegate,
  futures_util::Stream,
  tracing,
  transport::Transport,
  Message, Node, NodeId,
};

#[derive(thiserror::Error)]
pub enum SerfDelegateError<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  #[error("{0}")]
  Serf(Box<crate::error::Error<T, D, O>>),
  #[error("{0}")]
  Merge(D::Error),
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> core::fmt::Debug
  for SerfDelegateError<T, D, O>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    core::fmt::Display::fmt(self, f)
  }
}

// PingVersion is an internal version for the ping message, above the normal
// versioning we get from the protocol version. This enables small updates
// to the ping message without a full protocol bump.
const PING_VERSION: u8 = 1;

pub(crate) struct SerfDelegate<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  serf: OnceLock<Serf<T, D, O>>,
  merge_delegate: Option<D>,
}

impl<D: MergeDelegate, T: Transport, O: ReconnectTimeoutOverrider> SerfDelegate<T, D, O>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
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
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Error = SerfDelegateError<T, D, O>;

  fn node_meta(&self, limit: usize) -> Bytes {
    match self.this().inner.opts.tags() {
      Some(tags) => {
        let role_bytes = match encode_tags::<T, D, O>(&tags) {
          Ok(b) => b,
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to encode tags");
            return Bytes::new();
          }
        };

        if role_bytes.len() > limit {
          panic!(
            "node tags {:?} exceeds length limit of {} bytes",
            tags.as_ref(),
            limit
          );
        }
        role_bytes
      }
      None => Bytes::new(),
    }
  }

  async fn notify_user_msg(&self, msg: Bytes) -> Result<(), Self::Error> {
    // If we didn't actually receive any data, then ignore it.
    if msg.is_empty() {
      return Ok(());
    }

    // TODO: metrics

    let mut rebroadcast = false;
    let rebroadcast_queue = &self.this().inner.broadcasts;

    let t = msg[0];
    todo!();
  }

  async fn get_broadcasts(
    &self,
    overhead: usize,
    limit: usize,
  ) -> Result<Vec<Message>, Self::Error> {
    let this = self.this();
    let mut msgs = this.inner.broadcasts.get_broadcasts(overhead, limit).await;

    // Determine the bytes used already
    let mut bytes_used = 0;
    for msg in msgs.iter() {
      bytes_used += msg.len() + 1 + overhead; // +1 for showbiz message type
                                              // TODO: metrics
    }

    // Get any additional query broadcasts
    let query_msgs = this
      .inner
      .query_broadcasts
      .get_broadcasts(overhead, limit - bytes_used)
      .await;
    for msg in query_msgs.iter() {
      bytes_used += msg.len() + 1 + overhead; // +1 for showbiz message type
                                              // TODO: metrics
    }

    // Get any additional event broadcasts
    let event_msgs = this
      .inner
      .event_broadcasts
      .get_broadcasts(overhead, limit - bytes_used)
      .await;
    for msg in query_msgs.iter() {
      bytes_used += msg.len() + 1 + overhead; // +1 for showbiz message type
                                              // TODO: metrics
    }

    msgs.extend(query_msgs);
    msgs.extend(event_msgs);
    Ok(msgs)
  }

  async fn local_state(&self, _join: bool) -> Result<Bytes, Self::Error> {
    let this = self.this();
    let members = this.inner.members.read().await;
    let events = this.inner.event_core.read().await;

    // Create the message to send
    let pp = PushPullRef {
      ltime: this.inner.clock.time(),
      status_ltimes: members
        .states
        .iter()
        .map(|(k, v)| (k.clone(), v.status_time))
        .collect(),
      left_members: members
        .left_members
        .iter()
        .map(|v| v.member.id.clone())
        .collect(),
      event_ltime: this.inner.event_clock.time(),
      events: events.buffer.as_slice(),
      query_ltime: this.inner.query_clock.time(),
    };
    drop(members);

    match encode_message(MessageType::PushPull, &pp) {
      Ok(buf) => Ok(buf.into()),
      Err(e) => {
        tracing::error!(target = "ruserf", err=%e, "failed to encode local state");
        Ok(Bytes::new())
      }
    }
  }

  async fn merge_remote_state(&self, _buf: &[u8], _join: bool) -> Result<(), Self::Error> {
    Ok(())
  }

  async fn notify_join(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    self.this().handle_node_join(node).await;
    Ok(())
  }

  async fn notify_leave(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    self.this().handle_node_leave(node).await;
    Ok(())
  }

  async fn notify_update(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    self.this().handle_node_update(node).await;
    Ok(())
  }

  async fn notify_alive(&self, node: Arc<Node>) -> Result<(), Self::Error> {
    if let Some(ref d) = self.merge_delegate {
      let member = node_to_member(node).map_err(|e| SerfDelegateError::Serf(Box::new(e)))?;
      return d
        .notify_merge(vec![member])
        .await
        .map_err(SerfDelegateError::Merge);
    }

    Ok(())
  }

  async fn notify_conflict(
    &self,
    existing: Arc<Node>,
    other: Arc<Node>,
  ) -> Result<(), Self::Error> {
    self.this().handle_node_conflict(existing, other).await;
    Ok(())
  }

  async fn notify_merge(&self, peers: Vec<Arc<Node>>) -> Result<(), Self::Error> {
    if let Some(ref d) = self.merge_delegate {
      let peers = peers
        .into_iter()
        .map(node_to_member)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SerfDelegateError::Serf(Box::new(e)))?;
      return d
        .notify_merge(peers)
        .await
        .map_err(SerfDelegateError::Merge);
    }
    Ok(())
  }

  async fn ack_payload(&self) -> Result<Bytes, Self::Error> {
    if let Some(c) = self.this().inner.coord_core.as_ref() {
      let mut buf = Vec::new();
      buf.put_u8(PING_VERSION);

      if let Err(e) = rmp_serde::encode::write(&mut buf, &c.client.get_coordinate()) {
        tracing::error!(target = "ruserf", err=%e, "failed to encode coordinate");
      }
      Ok(buf.into())
    } else {
      Ok(Bytes::new())
    }
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<Node>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) -> Result<(), Self::Error> {
    if payload.is_empty() {
      return Ok(());
    }

    let this = self.this();

    match this.inner.coord_core {
      Some(ref c) => {
        // Verify ping version in the header.
        if payload[0] != PING_VERSION {
          tracing::error!(
            target = "ruserf",
            "unsupported ping version: {}",
            payload[0]
          );
          return Ok(());
        }

        // Process the remainder of the message as a coordinate.
        let coord = match rmp_serde::decode::from_slice::<Coordinate>(&payload[1..]) {
          Ok(c) => c,
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to decode coordinate from ping");
            return Ok(());
          }
        };

        // Apply the update.
        let before = c.client.get_coordinate();
        match c.client.update(node.id().name(), &before, rtt) {
          Ok(after) => {
            // TODO: metrics
            {
              // Publish some metrics to give us an idea of how much we are
              // adjusting each time we update.
              let _d = before.distance_to(&after).as_secs_f64() * 1.0e3;
            }

            // Cache the coordinate for the other node, and add our own
            // to the cache as well since it just got updated. This lets
            // users call GetCachedCoordinate with our node name, which is
            // more friendly.
            let mut cache = c.cache.write();
            cache.insert(node.id().name().clone(), coord);
            cache
              .entry(this.inner.opts.showbiz_options.name().clone())
              .and_modify(|x| {
                *x = c.client.get_coordinate();
              })
              .or_insert_with(|| c.client.get_coordinate());
            Ok(())
          }
          Err(e) => {
            // TODO: metrics
            tracing::error!(target = "ruserf", err=%e, "rejected coordinate from {}", node.id().name());
            return Ok(());
          }
        }
      }
      None => Ok(()),
    }
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
  async fn notify_merge(&self, members: Vec<Member>) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultMergeDelegate;

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl MergeDelegate for DefaultMergeDelegate {
  type Error = std::convert::Infallible;

  #[cfg(not(feature = "nightly"))]
  async fn notify_merge(&self, _members: Vec<Member>) -> Result<(), Self::Error> {
    Ok(())
  }
}

fn node_to_member<T: Transport, D: MergeDelegate, O: ReconnectTimeoutOverrider>(
  node: Arc<Node>,
) -> Result<Member, crate::error::Error<T, D, O>>
where
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  let status = if node.state() == showbiz_core::NodeState::Left {
    MemberStatus::Left
  } else {
    MemberStatus::None
  };

  if node.meta().len() > showbiz_core::META_MAX_SIZE {
    return Err(crate::error::Error::TagsTooLarge(node.meta().len()));
  }

  Ok(Member {
    id: node.id().clone(),
    tags: decode_tags(node.meta()),
    status: Atomic::new(status),
    protocol_version: showbiz_core::ProtocolVersion::V0,
    delegate_version: showbiz_core::DelegateVersion::V0,
  })
}
