use std::collections::HashMap;

use crate::{clock::LamportTime, error::VoidError, event::UserEvent};

use super::*;

struct LatestUserEvents {
  ltime: LamportTime,
  events: Vec<UserEvent>,
}

#[repr(transparent)]
pub(crate) struct UserEventCoalescer {
  events: HashMap<String, LatestUserEvents>,
}

#[showbiz_core::async_trait::async_trait]
impl Coalescer for UserEventCoalescer {
  type Error = VoidError;
  type Event = UserEvent;

  fn handle(&self, event: &Self::Event) -> bool {
    event.coalesce
  }

  async fn coalesce(&mut self, event: Self::Event) -> Result<(), Self::Error> {
    // match event {
    //   Event::User(user) => {
    //     let lteim = event.lteim();
    //     let events = self.events.entry(user_id).or_default();
    //     if lteim > events.lteim {
    //       events.lteim = lteim;
    //       events.events.clear();
    //     }
    //     events.events.push(event);
    //   }
    //   _ => unreachable!(),
    // }
    Ok(())
  }

  async fn flush(&mut self, out_tx: Sender<Self::Event>) -> Result<(), Self::Error> {
    for (_, latest) in self.events.drain() {
      for event in latest.events {
        let _ = out_tx.send(event).await;
      }
    }
    Ok(())
  }
}