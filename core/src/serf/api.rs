use std::sync::atomic::Ordering;

use futures::FutureExt;
use memberlist_core::{
  agnostic_lite::RuntimeLite,
  bytes::{BufMut, Bytes, BytesMut},
  tracing,
  transport::{MaybeResolvedAddress, Node},
  types::{Meta, SmallVec},
  CheapClone,
};
use smol_str::SmolStr;

use crate::{
  delegate::TransformDelegate,
  error::{Error, JoinError},
  event::InternalQueryEvent,
  types::{
    LeaveMessage, Member, MessageType, QueryFlag, QueryMessage, SerfMessage, Tags, UserEventMessage,
  },
};

use super::*;

impl<T, D> Serf<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
{
  pub async fn with_event_sender_and_delegate(
    transport: T,
    ev: async_channel::Sender<Event<T, D>>,
    delegate: D,
    opts: Options,
  ) -> Result<Self, Error<T, D>> {
    Self::new_in(transport, Some(ev), Some(delegate), opts).await
  }

  /// A predicate that determines whether or not encryption
  /// is enabled, which can be possible in one of 2 cases:
  ///   - Single encryption key passed at agent start (no persistence)
  ///   - Keyring file provided at agent start
  #[inline]
  #[cfg(feature = "encryption")]
  #[cfg_attr(docsrs, doc(cfg(feature = "encryption")))]
  pub fn encryption_enabled(&self) -> bool {
    self.inner.memberlist.keyring().is_some()
  }

  /// Returns a receiver that can be used to wait for
  /// Serf to shutdown.
  #[inline]
  pub fn shutdown_rx(&self) -> async_channel::Receiver<()> {
    self.inner.shutdown_rx.clone()
  }

  /// The current state of this Serf instance.
  #[inline]
  pub fn state(&self) -> SerfState {
    *self.inner.state.lock()
  }

  /// Returns a point-in-time snapshot of the members of this cluster.
  #[inline]
  pub async fn members(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Used to provide operator debugging information
  #[inline]
  pub async fn stats(&self) -> base::Stats {
    let _members = self.inner.members.read().await;
    todo!()
  }

  /// Returns the number of nodes in the serf cluster, regardless of
  /// their health or status.
  #[inline]
  pub async fn num_nodes(&self) -> usize {
    self.inner.members.read().await.states.len()
  }

  /// Returns the Member information for the local node
  #[inline]
  pub async fn local_member(
    &self,
  ) -> Arc<Member<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>> {
    self
      .inner
      .members
      .read()
      .await
      .states
      .get(self.inner.memberlist.local_id())
      .unwrap()
      .member
      .clone()
  }

  /// Used to dynamically update the tags associated with
  /// the local node. This will propagate the change to the rest of
  /// the cluster. Blocks until a the message is broadcast out.
  #[inline]
  pub async fn set_tags(&self, tags: Tags) -> Result<(), Error<T, D>> {
    // Check that the meta data length is okay
    let tags_encoded_len = <D as TransformDelegate>::tags_encoded_len(&tags);
    if tags_encoded_len > Meta::MAX_SIZE {
      return Err(Error::TagsTooLarge(tags_encoded_len));
    }
    // update the config
    if !tags.is_empty() {
      self.inner.opts.tags.store(Some(Arc::new(tags)));
    } else {
      self.inner.opts.tags.store(None);
    }

    // trigger a memberlist update
    self
      .inner
      .memberlist
      .update_node(self.inner.opts.broadcast_timeout)
      .await
      .map_err(From::from)
  }

  /// Used to broadcast a custom user event with a given
  /// name and payload. If the configured size limit is exceeded and error will be returned.
  /// If coalesce is enabled, nodes are allowed to coalesce this event.
  #[inline]
  pub async fn user_event(
    &self,
    name: SmolStr,
    payload: Bytes,
    coalesce: bool,
  ) -> Result<(), Error<T, D>> {
    let payload_size_before_encoding = name.len() + payload.len();

    // Check size before encoding to prevent needless encoding and return early if it's over the specified limit.
    if payload_size_before_encoding > self.inner.opts.max_user_event_size {
      return Err(Error::UserEventTooLarge(payload_size_before_encoding));
    }

    if payload_size_before_encoding > USER_EVENT_SIZE_LIMIT {
      return Err(Error::UserEventTooLarge(payload_size_before_encoding));
    }

    // Create a message
    let msg = UserEventMessage {
      ltime: self.inner.event_clock.time(),
      name: name.clone(),
      payload,
      cc: coalesce,
    };

    // Start broadcasting the event
    let len = <D as TransformDelegate>::message_encoded_len(&msg);

    // Check the size after encoding to be sure again that
    // we're not attempting to send over the specified size limit.
    if len > self.inner.opts.max_user_event_size {
      return Err(Error::RawUserEventTooLarge(len));
    }

    if len > USER_EVENT_SIZE_LIMIT {
      return Err(Error::RawUserEventTooLarge(len));
    }

    let mut raw = BytesMut::with_capacity(len + 1); // + 1 for message type byte
    raw.put_u8(MessageType::UserEvent as u8);
    raw.resize(len + 1, 0);

    let actual_encoded_len =
      <D as TransformDelegate>::encode_message(&msg, &mut raw[1..]).map_err(Error::transform)?;
    debug_assert_eq!(
      actual_encoded_len, len,
      "expected encoded len {} mismatch the actual encoded len {}",
      len, actual_encoded_len
    );

    self.inner.event_clock.increment();

    // Process update locally
    self.handle_user_event(msg).await;

    self
      .inner
      .event_broadcasts
      .queue_broadcast(SerfBroadcast {
        msg: raw.freeze(),
        notify_tx: None,
      })
      .await;
    Ok(())
  }

  /// Used to broadcast a new query. The query must be fairly small,
  /// and an error will be returned if the size limit is exceeded. This is only
  /// available with protocol version 4 and newer. Query parameters are optional,
  /// and if not provided, a sane set of defaults will be used.
  pub async fn query(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    self.query_in(name, payload, params, None).await
  }

  pub(crate) async fn internal_query(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ty: InternalQueryEvent<T::Id>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    self.query_in(name, payload, params, Some(ty)).await
  }

  async fn query_in(
    &self,
    name: SmolStr,
    payload: Bytes,
    params: Option<QueryParam<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    ty: Option<InternalQueryEvent<T::Id>>,
  ) -> Result<QueryResponse<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>, Error<T, D>>
  {
    // Provide default parameters if none given.
    let params = match params {
      Some(params) => params,
      None => self.default_query_param().await,
    };

    // Get the local node
    let local = self.inner.memberlist.advertise_node();

    // Encode the filters
    let filters = params.encode_filters::<D>().map_err(Error::transform)?;

    // Setup the flags
    let flags = if params.request_ack {
      QueryFlag::empty()
    } else {
      QueryFlag::ACK
    };

    // Create the message
    let q = QueryMessage {
      ltime: self.inner.query_clock.time(),
      id: rand::random(),
      from: local.cheap_clone(),
      filters,
      flags: flags.bits(),
      relay_factor: params.relay_factor,
      timeout: params.timeout,
      name: name.clone(),
      payload,
    };
    // let msg_ref = SerfMessageRef::Query(&q);

    // Encode the query
    let len = <D as TransformDelegate>::message_encoded_len(&q);

    // Check the size
    if len > self.inner.opts.query_size_limit {
      return Err(Error::QueryTooLarge(len));
    }

    let mut raw = BytesMut::with_capacity(len + 1); // + 1 for message type byte
    raw.put_u8(MessageType::Query as u8);
    raw.resize(len + 1, 0);
    let actual_encoded_len =
      <D as TransformDelegate>::encode_message(&q, &mut raw[1..]).map_err(Error::transform)?;
    debug_assert_eq!(
      actual_encoded_len, len,
      "expected encoded len {} mismatch the actual encoded len {}",
      len, actual_encoded_len
    );

    // Register QueryResponse to track acks and responses
    let resp = q.response(self.inner.memberlist.num_online_members().await);
    self
      .register_query_response(params.timeout, resp.clone())
      .await;

    // Process query locally
    self.handle_query(q, ty).await;

    // Start broadcasting the event
    self
      .inner
      .broadcasts
      .queue_broadcast(SerfBroadcast {
        msg: raw.freeze(),
        notify_tx: None,
      })
      .await;
    Ok(resp)
  }

  /// Joins an existing Serf cluster. Returns the id of nodes
  /// successfully contacted. If `ignore_old` is true, then any
  /// user messages sent prior to the join will be ignored.
  pub async fn join_many(
    &self,
    existing: impl Iterator<Item = Node<T::Id, MaybeResolvedAddress<T>>>,
    ignore_old: bool,
  ) -> Result<
    SmallVec<Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>>,
    JoinError<T, D>,
  > {
    // Do a quick state check
    let current_state = self.state();
    if current_state != SerfState::Alive {
      return Err(JoinError {
        joined: SmallVec::new(),
        errors: existing
          .into_iter()
          .map(|node| (node, Error::BadJoinStatus(current_state)))
          .collect(),
        broadcast_error: None,
      });
    }

    // Hold the joinLock, this is to make eventJoinIgnore safe
    let _join_lock = self.inner.join_lock.lock().await;

    // Ignore any events from a potential join. This is safe since we hold
    // the joinLock and nobody else can be doing a Join
    if ignore_old {
      self.inner.event_join_ignore.store(true, Ordering::SeqCst);
      scopeguard::defer!(self.inner.event_join_ignore.store(false, Ordering::SeqCst));
    }

    // Have memberlist attempt to join
    match self.inner.memberlist.join_many(existing).await {
      Ok(joined) => {
        // Start broadcasting the update
        if let Err(e) = self.broadcast_join(self.inner.clock.time()).await {
          return Err(JoinError {
            joined,
            errors: Default::default(),
            broadcast_error: Some(e),
          });
        }
        Ok(joined)
      }
      Err(e) => {
        let (joined, errors) = e.into();
        // If we joined any nodes, broadcast the join message
        if !joined.is_empty() {
          // Start broadcasting the update
          if let Err(e) = self.broadcast_join(self.inner.clock.time()).await {
            return Err(JoinError {
              joined,
              errors: errors
                .into_iter()
                .map(|(addr, err)| (addr, err.into()))
                .collect(),
              broadcast_error: Some(e),
            });
          }

          Err(JoinError {
            joined,
            errors: errors
              .into_iter()
              .map(|(addr, err)| (addr, err.into()))
              .collect(),
            broadcast_error: None,
          })
        } else {
          Err(JoinError {
            joined,
            errors: errors
              .into_iter()
              .map(|(addr, err)| (addr, err.into()))
              .collect(),
            broadcast_error: None,
          })
        }
      }
    }
  }

  /// Gracefully exits the cluster. It is safe to call this multiple
  /// times.
  /// If the Leave broadcast timeout, Leave() will try to finish the sequence as best effort.
  pub async fn leave(&self) -> Result<(), Error<T, D>> {
    // Check the current state
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Left => return Ok(()),
        SerfState::Leaving => return Err(Error::BadLeaveStatus(*s)),
        SerfState::Shutdown => return Err(Error::BadLeaveStatus(*s)),
        _ => {
          // Set the state to leaving
          *s = SerfState::Leaving;
        }
      }
    }

    // If we have a snapshot, mark we are leaving
    if let Some(ref snap) = self.inner.snapshot {
      snap.leave().await;
    }

    // Construct the message for the graceful leave
    let msg = LeaveMessage {
      ltime: self.inner.clock.time(),
      node: self.inner.memberlist.advertise_node(),
      prune: false,
    };

    self.inner.clock.increment();

    // Process the leave locally
    self.handle_node_leave_intent(&msg).await;

    let msg = SerfMessage::Leave(msg);

    // Only broadcast the leave message if there is at least one
    // other node alive.
    if self.has_alive_members().await {
      let (notify_tx, notify_rx) = async_channel::bounded(1);
      self.broadcast(msg, Some(notify_tx)).await?;

      futures::select! {
        _ = notify_rx.recv().fuse() => {
          // We got a response, so we are done
        }
        _ = <T::Runtime as RuntimeLite>::sleep(self.inner.opts.broadcast_timeout).fuse() => {
          tracing::warn!("ruserf: timeout while waiting for graceful leave");
        }
      }
    }

    // Attempt the memberlist leave
    if let Err(e) = self
      .inner
      .memberlist
      .leave(self.inner.opts.broadcast_timeout)
      .await
    {
      tracing::warn!("ruserf: timeout waiting for leave broadcast: {}", e);
    }

    // Wait for the leave to propagate through the cluster. The broadcast
    // timeout is how long we wait for the message to go out from our own
    // queue, but this wait is for that message to propagate through the
    // cluster. In particular, we want to stay up long enough to service
    // any probes from other nodes before they learn about us leaving.
    <T::Runtime as RuntimeLite>::sleep(self.inner.opts.leave_propagate_delay).await;

    // Transition to Left only if we not already shutdown
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Shutdown => {}
        _ => {
          *s = SerfState::Left;
        }
      }
    }
    Ok(())
  }

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  pub async fn remove_failed_node(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.force_leave(node, false).await
  }

  /// Forcibly removes a failed node from the cluster
  /// immediately, instead of waiting for the reaper to eventually reclaim it.
  /// This also has the effect that Serf will no longer attempt to reconnect
  /// to this node.
  pub async fn remove_failed_node_prune(
    &self,
    node: Node<T::Id, <T::Resolver as AddressResolver>::ResolvedAddress>,
  ) -> Result<(), Error<T, D>> {
    self.force_leave(node, true).await
  }

  /// Forcefully shuts down the Serf instance, stopping all network
  /// activity and background maintenance associated with the instance.
  ///
  /// This is not a graceful shutdown, and should be preceded by a call
  /// to Leave. Otherwise, other nodes in the cluster will detect this node's
  /// exit as a node failure.
  ///
  /// It is safe to call this method multiple times.
  pub async fn shutdown(&self) -> Result<(), Error<T, D>> {
    {
      let mut s = self.inner.state.lock();
      match *s {
        SerfState::Shutdown => return Ok(()),
        SerfState::Left => {}
        _ => {
          tracing::warn!("ruserf: shutdown without a leave");
        }
      }

      // Wait to close the shutdown channel until after we've shut down the
      // memberlist and its associated network resources, since the shutdown
      // channel signals that we are cleaned up outside of Serf.
      *s = SerfState::Shutdown;
    }

    self.inner.memberlist.shutdown().await?;
    self.inner.shutdown_tx.close();

    // Wait for the snapshoter to finish if we have one
    if let Some(ref snap) = self.inner.snapshot {
      snap.wait().await;
    }

    Ok(())
  }

  /// Returns the network coordinate of the local node.
  pub fn get_cooridate(&self) -> Result<Coordinate, Error<T, D>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.client.get_coordinate());
    }

    Err(Error::CoordinatesDisabled)
  }

  /// Returns the network coordinate for the node with the given
  /// name. This will only be valid if `disable_coordinates` is set to `false`.
  pub fn get_cached_coordinate(&self, id: &T::Id) -> Result<Option<Coordinate>, Error<T, D>> {
    if let Some(ref coord) = self.inner.coord_core {
      return Ok(coord.cache.read().get(id).cloned());
    }

    Err(Error::CoordinatesDisabled)
  }
}
