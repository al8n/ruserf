use async_channel::Sender;
use showbiz_core::{async_trait, broadcast::Broadcast, Message, NodeId};
use smol_str::SmolStr;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) enum BroadcastId {
  Id(NodeId),
  String(SmolStr),
}

impl From<NodeId> for BroadcastId {
  fn from(value: NodeId) -> Self {
    Self::Id(value)
  }
}

impl From<SmolStr> for BroadcastId {
  fn from(value: SmolStr) -> Self {
    Self::String(value)
  }
}

#[viewit::viewit]
pub(crate) struct SerfBroadcast {
  id: BroadcastId,
  msg: Message,
  notify_tx: Option<Sender<()>>,
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Broadcast for SerfBroadcast {
  type Id = BroadcastId;

  fn id(&self) -> &Self::Id {
    &self.id
  }

  fn invalidates(&self, _other: &Self) -> bool {
    false
  }

  fn message(&self) -> &Message {
    &self.msg
  }

  #[cfg(not(feature = "nightly"))]
  async fn finished(&self) {
    if let Some(ref tx) = self.notify_tx {
      tx.close();
    }
  }

  #[cfg(feature = "nightly")]
  fn finished<'a>(
    &'a self,
  ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move {
      if let Some(ref tx) = self.notify_tx {
        tx.close();
      }
    }
  }
}

// #[tokio::test]
// async fn test_broadcast_finished() {
//   use std::time::Duration;
//   use showbiz_core::futures_util::{self, FutureExt};

//   let (tx, rx) = async_channel::unbounded();

//   let b = SerfBroadcast {
//     name: "ruserf".try_into().unwrap(),
//     msg: Message::new(),
//     notify_tx: ArcSwapOption::from_pointee(Some(tx)),
//   };

//   b.finished().await;

//   futures_util::select! {
//     _ = rx.recv().fuse() => {}
//     _ = tokio::time::sleep(Duration::from_millis(10)).fuse() => {
//       panic!("expected broadcast to be finished")
//     }
//   }
// }
