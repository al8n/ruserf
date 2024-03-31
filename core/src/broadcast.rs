use async_channel::Sender;
use memberlist_core::{bytes::Bytes, Broadcast};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) struct BroadcastId;

impl core::fmt::Display for BroadcastId {
  fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    Ok(())
  }
}

#[viewit::viewit]
#[derive(Debug)]
pub(crate) struct SerfBroadcast {
  msg: Bytes,
  notify_tx: Option<Sender<()>>,
}

impl Broadcast for SerfBroadcast {
  type Id = BroadcastId;
  type Message = Bytes;

  fn id(&self) -> Option<&Self::Id> {
    None
  }

  fn invalidates(&self, _other: &Self) -> bool {
    false
  }

  fn message(&self) -> &Self::Message {
    &self.msg
  }

  async fn finished(&self) {
    if let Some(ref tx) = self.notify_tx {
      tx.close();
    }
  }

  fn encoded_len(msg: &Self::Message) -> usize {
    msg.len()
  }
}

// #[tokio::test]
// async fn test_broadcast_finished() {
//   use std::time::Duration;
//   use memberlist_core::futures_util::{self, FutureExt};

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
