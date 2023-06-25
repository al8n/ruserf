use arc_swap::ArcSwapOption;
use async_channel::Sender;
use showbiz_core::{async_trait, broadcast::Broadcast, Message, Name};

pub(crate) struct SerfBroadcast {
  name: Name,
  msg: Message,
  notify_tx: ArcSwapOption<Sender<()>>,
}

#[cfg_attr(not(feature = "nightly"), async_trait::async_trait)]
impl Broadcast for SerfBroadcast {
  fn name(&self) -> &Name {
    &self.name
  }

  fn invalidates(&self, _other: &Self) -> bool {
    false
  }

  fn message(&self) -> &Message {
    &self.msg
  }

  #[cfg(not(feature = "nightly"))]
  async fn finished(&self) {
    self.notify_tx.store(None);
  }

  #[cfg(feature = "nightly")]
  fn finished<'a>(
    &'a self,
  ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + 'a {
    async move {
      self.notify_tx.store(None);
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
