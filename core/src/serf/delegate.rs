use crate::{
  broadcast::SerfBroadcast,
  clock::LamportTime,
  delegate::{Delegate, TransformDelegate},
  error::{DelegateError, Error},
  types::{JoinMessage, Leave, MessageUserEvent, PushPullRef, SerfMessage},
  Member, MemberStatus, MessageType, Serf,
};

use std::{
  future::Future,
  sync::{Arc, OnceLock},
};

use atomic::{Atomic, Ordering};
use futures::Stream;
use indexmap::IndexSet;
use memberlist_core::{
  agnostic::Runtime,
  bytes::{Buf, BufMut, Bytes, BytesMut},
  delegate::{
    AliveDelegate, ConflictDelegate, Delegate as MemberlistDelegate, EventDelegate,
    MergeDelegate as MemberlistMergeDelegate, NodeDelegate, PingDelegate,
  },
  tracing,
  transport::{AddressResolver, Node, Transport},
  types::{DelegateVersion, Meta, NodeState, ProtocolVersion, SmallVec, State},
  CheapClone, META_MAX_SIZE,
};

// PingVersion is an internal version for the ping message, above the normal
// versioning we get from the protocol version. This enables small updates
// to the ping message without a full protocol bump.
const PING_VERSION: u8 = 1;

pub(crate) struct SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  serf: OnceLock<Serf<T, D>>,
  merge_delegate: Option<D>,
}

impl<D, T> SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  pub(crate) fn new(d: Option<D>) -> Self {
    Self {
      serf: OnceLock::new(),
      merge_delegate: d,
    }
  }

  pub(crate) fn store(&self, s: Serf<T, D>) {
    // No error, we never call this in parallel
    let _ = self.serf.set(s);
  }

  fn this(&self) -> &Serf<T, D> {
    self.serf.get().unwrap()
  }
}

impl<D, T> NodeDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  async fn node_meta(&self, limit: usize) -> Meta {
    match self.this().inner.opts.tags() {
      Some(tags) => {
        let encoded_len = <D as TransformDelegate>::tags_encoded_len(&tags);
        let limit = limit.min(Meta::MAX_SIZE);
        if encoded_len > limit {
          panic!(
            "node tags {:?} exceeds length limit of {} bytes",
            tags, limit
          );
        }

        let mut role_bytes = vec![0; encoded_len];
        match <D as TransformDelegate>::encode_tags(&tags, &mut role_bytes) {
          Ok(len) => {
            debug_assert_eq!(
              len, encoded_len,
              "expected encoded len {} mismatch the actual encoded len {}",
              encoded_len, len
            );

            if len > limit {
              panic!(
                "node tags {:?} exceeds length limit of {} bytes",
                tags, limit
              );
            }

            role_bytes.try_into().unwrap()
          }
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to encode tags");
            Meta::empty()
          }
        }
      }
      None => Meta::empty(),
    }
  }

  async fn notify_message(&self, mut msg: Bytes) {
    // If we didn't actually receive any data, then ignore it.
    if msg.is_empty() {
      return;
    }

    // TODO: metrics

    let this = self.this();
    let mut rebroadcast = None;
    let mut rebroadcast_queue = &this.inner.broadcasts;

    match MessageType::try_from(msg[0]) {
      Ok(ty) => match ty {
        MessageType::Leave => match <D as TransformDelegate>::decode_message(&msg[1..]) {
          Ok(l) => {
            if let SerfMessage::Leave(l) = &l {
              tracing::debug!(target = "ruserf", "leave message",);
              rebroadcast = this.handle_node_leave_intent(l).await.then(|| msg.clone());
            } else {
              tracing::warn!(
                target = "ruserf",
                "receive unexpected message: {}",
                l.as_str()
              );
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode message");
          }
        },
        MessageType::Join => match <D as TransformDelegate>::decode_message(&msg[1..]) {
          Ok(j) => {
            if let SerfMessage::Join(j) = &j {
              tracing::debug!(target = "ruserf", "join message",);
              rebroadcast = this.handle_node_join_intent(j).await.then(|| msg.clone());
            } else {
              tracing::warn!(
                target = "ruserf",
                "receive unexpected message: {}",
                j.as_str()
              );
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode message");
          }
        },
        MessageType::UserEvent => match <D as TransformDelegate>::decode_message(&msg[1..]) {
          Ok(ue) => {
            if let SerfMessage::UserEvent(ue) = ue {
              tracing::debug!(target = "ruserf", "user event message",);
              rebroadcast = this.handle_user_event(ue).await.then(|| msg.clone());
              rebroadcast_queue = &this.inner.event_broadcasts;
            } else {
              tracing::warn!(
                target = "ruserf",
                "receive unexpected message: {}",
                ue.as_str()
              );
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode message");
          }
        },
        MessageType::Query => match <D as TransformDelegate>::decode_message(&msg[1..]) {
          Ok(q) => {
            if let SerfMessage::Query(q) = q {
              tracing::debug!(target = "ruserf", "query message",);
              rebroadcast = this.handle_query(q, None).await.then(|| msg.clone());
              rebroadcast_queue = &this.inner.query_broadcasts;
            } else {
              tracing::warn!(
                target = "ruserf",
                "receive unexpected message: {}",
                q.as_str()
              );
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode message");
          }
        },
        MessageType::QueryResponse => match <D as TransformDelegate>::decode_message(&msg[1..]) {
          Ok(qr) => {
            if let SerfMessage::QueryResponse(qr) = qr {
              tracing::debug!(target = "ruserf", "query response message",);
              this.handle_query_response(qr).await;
            } else {
              tracing::warn!(
                target = "ruserf",
                "receive unexpected message: {}",
                qr.as_str()
              );
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode message");
          }
        },
        MessageType::Relay => match <D as TransformDelegate>::decode_node(&msg[1..]) {
          Ok((consumed, n)) => {
            tracing::debug!(target = "ruserf", "relay message",);
            tracing::debug!(target = "ruserf", "relaying response to node: {}", n);
            // + 1 for the message type byte
            msg.advance(consumed + 1);
            if let Err(e) = this.inner.memberlist.send(n.address(), msg.clone()).await {
              tracing::error!(target = "ruserf", err=%e, "failed to forwarding message to {}", n);
            }
          }
          Err(e) => {
            tracing::warn!(target = "ruserf", err=%e, "failed to decode relay destination");
          }
        },
        ty => {
          tracing::warn!(
            target = "ruserf",
            "receive unexpected message: {}",
            ty.as_str()
          );
        }
      },
      Err(e) => {
        tracing::warn!(target = "ruserf", err=%e, "receive unknown message type");
      }
    }

    if let Some(msg) = rebroadcast {
      rebroadcast_queue
        .queue_broadcast(
          SerfBroadcast::<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress> {
            msg,
            notify_tx: None,
          },
        )
        .await;
    }
  }

  async fn broadcast_messages(&self, overhead: usize, limit: usize) -> SmallVec<Bytes> {
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

  async fn local_state(&self, _join: bool) -> Bytes {
    let this = self.this();
    let members = this.inner.members.read().await;
    let events = this.inner.event_core.read().await;

    // Create the message to send
    let status_ltimes = members
      .states
      .iter()
      .map(|(k, v)| (k.cheap_clone(), v.status_time))
      .collect();
    let left_members = members
      .left_members
      .iter()
      .map(|v| v.member.node().cheap_clone())
      .collect::<IndexSet<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>>();
    let pp = PushPullRef {
      ltime: this.inner.clock.time(),
      status_ltimes: &status_ltimes,
      left_members: &left_members,
      event_ltime: this.inner.event_clock.time(),
      events: events.buffer.as_slice(),
      query_ltime: this.inner.query_clock.time(),
    };
    drop(members);

    let expected_encoded_len = <D as TransformDelegate>::message_encoded_len(pp);
    let mut buf = BytesMut::with_capacity(expected_encoded_len + 1); // +1 for the message type byte
    buf.put_u8(MessageType::PushPull as u8);
    buf.resize(expected_encoded_len + 1, 0);
    match <D as TransformDelegate>::encode_message(pp, &mut buf[1..]) {
      Ok(encoded_len) => {
        debug_assert_eq!(
          expected_encoded_len, encoded_len,
          "expected encoded len {} mismatch the actual encoded len {}",
          expected_encoded_len, encoded_len
        );
        buf.freeze()
      }
      Err(e) => {
        tracing::error!(target = "ruserf", err=%e, "failed to encode local state");
        Bytes::new()
      }
    }
  }

  async fn merge_remote_state(&self, buf: Bytes, is_join: bool) {
    if buf.is_empty() {
      tracing::error!(target = "ruserf", "remote state is zero bytes");
      return;
    }

    // Check the message type
    let Ok(ty) = MessageType::try_from(&buf[0]) else {
      tracing::error!(
        target = "ruserf",
        "remote state has bad type prefix {}",
        buf[0]
      );
      return;
    };

    // TODO: messageDropper
    match ty {
      MessageType::PushPull => {
        match <D as TransformDelegate>::decode_message(&buf[1..]) {
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to decode remote state");
          }
          Ok(msg) => {
            match msg {
              SerfMessage::PushPull(pp) => {
                let this = self.this();
                // Witness the Lamport clocks first.
                // We subtract 1 since no message with that clock has been sent yet
                if pp.ltime > LamportTime::ZERO {
                  this.inner.clock.witness(pp.ltime - LamportTime(1));
                }
                if pp.event_ltime > LamportTime::ZERO {
                  this
                    .inner
                    .event_clock
                    .witness(pp.event_ltime - LamportTime(1));
                }
                if pp.query_ltime > LamportTime::ZERO {
                  this
                    .inner
                    .query_clock
                    .witness(pp.query_ltime - LamportTime(1));
                }

                // Process the left nodes first to avoid the LTimes from incrementing
                // in the wrong order. Note that we don't have the actual Lamport time
                // for the leave message, so we go one past the join time, since the
                // leave must have been accepted after that to get onto the left members
                // list. If we didn't do this then the message would not get processed.
                for id in &pp.left_members {
                  if let Some(&ltime) = pp.status_ltimes.get(id) {
                    this
                      .handle_node_leave_intent(&Leave {
                        ltime,
                        node: id.clone(),
                        prune: false,
                      })
                      .await;
                  } else {
                    tracing::error!(
                      target = "ruserf",
                      "{} is in left members, but cannot find the lamport time for it in status",
                      id
                    );
                  }
                }

                // Update any other LTimes
                for (id, ltime) in pp.status_ltimes {
                  // Skip the left nodes
                  if pp.left_members.contains(&id) {
                    continue;
                  }

                  // Create an artificial join message
                  this
                    .handle_node_join_intent(&JoinMessage { ltime, node: id })
                    .await;
                }

                // If we are doing a join, and eventJoinIgnore is set
                // then we set the eventMinTime to the EventLTime. This
                // prevents any of the incoming events from being processed
                let event_join_ignore = this.inner.event_join_ignore.load(Ordering::Acquire);
                if is_join && event_join_ignore {
                  let mut ec = this.inner.event_core.write().await;
                  if pp.event_ltime > ec.min_time {
                    ec.min_time = pp.event_ltime;
                  }
                }

                // Process all the events
                for events in pp.events {
                  match events {
                    Some(events) => {
                      for e in events.events {
                        this
                          .handle_user_event(MessageUserEvent {
                            ltime: events.ltime,
                            name: e.name,
                            payload: e.payload,
                            cc: false,
                          })
                          .await;
                      }
                    }
                    None => continue,
                  }
                }
              }
              msg => {
                tracing::error!(
                  target = "ruserf",
                  "remote state has bad type {}",
                  msg.as_str()
                );
                return;
              }
            }
          }
        }
      }
      ty => {
        tracing::error!(
          target = "ruserf",
          "remote state has bad type {}",
          ty.as_str()
        );
      }
    }
  }
}

impl<D, T> EventDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;
  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;

  async fn notify_join(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.this().handle_node_join(node).await;
  }

  async fn notify_leave(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.this().handle_node_leave(node).await;
  }

  async fn notify_update(&self, node: Arc<NodeState<Self::Id, Self::Address>>) {
    self.this().handle_node_update(node).await;
  }
}

impl<D, T> AliveDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;
  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;
  type Error = Error<T, D>;

  async fn notify_alive(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
  ) -> Result<(), Self::Error> {
    if let Some(ref d) = self.merge_delegate {
      let member = node_to_member(node)?;
      return d
        .notify_merge(vec![member])
        .await
        .map_err(|e| Error::Delegate(DelegateError::alive(e)));
    }

    Ok(())
  }
}

impl<D, T> MemberlistMergeDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;
  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;
  type Error = Error<T, D>;

  async fn notify_merge(
    &self,
    peers: SmallVec<Arc<NodeState<Self::Id, Self::Address>>>,
  ) -> Result<(), Self::Error> {
    if let Some(ref d) = self.merge_delegate {
      let peers = peers
        .into_iter()
        .map(node_to_member)
        .collect::<Result<SmallVec<_>, _>>()?;
      return d
        .notify_merge(peers)
        .await
        .map_err(|e| Error::Delegate(DelegateError::merge(e)));
    }
    Ok(())
  }
}

impl<D, T> ConflictDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;

  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;

  async fn notify_conflict(
    &self,
    existing: Arc<NodeState<Self::Id, Self::Address>>,
    other: Arc<NodeState<Self::Id, Self::Address>>,
  ) {
    self.this().handle_node_conflict(existing, other).await;
  }
}

impl<D, T> PingDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;

  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;

  async fn ack_payload(&self) -> Bytes {
    if let Some(c) = self.this().inner.coord_core.as_ref() {
      let mut buf = Vec::new();
      buf.put_u8(PING_VERSION);

      if let Err(e) =
        <D as TransformDelegate>::encode_coordinate(&c.client.get_coordinate(), &mut buf[1..])
      {
        tracing::error!(target = "ruserf", err=%e, "failed to encode coordinate");
      }
      buf.into()
    } else {
      Bytes::new()
    }
  }

  async fn notify_ping_complete(
    &self,
    node: Arc<NodeState<Self::Id, Self::Address>>,
    rtt: std::time::Duration,
    payload: Bytes,
  ) {
    if payload.is_empty() {
      return;
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
          return;
        }

        // Process the remainder of the message as a coordinate.
        let coord = match <D as TransformDelegate>::decode_coordinate(&payload[1..]) {
          Ok(c) => c,
          Err(e) => {
            tracing::error!(target = "ruserf", err=%e, "failed to decode coordinate from ping");
            return;
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
            return;
          }
        }
      }
      None => {}
    }
  }

  #[inline]
  fn disable_promised_pings(&self, _id: &Self::Id) -> bool {
    false
  }
}

impl<D, T> MemberlistDelegate for SerfDelegate<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  type Id = T::Id;

  type Address = <T::Resolver as AddressResolver>::ResolvedAddress;
}

fn node_to_member<T, D>(
  node: Arc<NodeState<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
) -> Result<
  Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  crate::error::Error<T, D>,
>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<T::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  let status = if node.state() == State::Left {
    MemberStatus::Left
  } else {
    MemberStatus::None
  };

  let meta = node.meta();
  if meta.len() > META_MAX_SIZE {
    return Err(crate::error::Error::TagsTooLarge(meta.len()));
  }

  Ok(Member {
    node: node.node(),
    tags: <D as TransformDelegate>::decode_tags(node.meta()),
    status: Atomic::new(status),
    protocol_version: ProtocolVersion::V0,
    delegate_version: DelegateVersion::V0,
  })
}
