use async_channel::Sender;
use showbiz_core::{async_trait, broadcast::Broadcast, Message};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct BroadcastId;

impl core::fmt::Display for BroadcastId {
  fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Ok(())
  }
}

#[viewit::viewit]
pub(crate) struct SerfBroadcast {
  msg: Message,
  notify_tx: Option<Sender<()>>,
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Broadcast for SerfBroadcast {
  type Id = BroadcastId;

  fn id(&self) -> Option<&Self::Id> {
    None
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
