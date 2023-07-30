use std::collections::HashMap;

use either::Either;
use smol_str::SmolStr;

use crate::{
  clock::LamportTime,
  event::{EventKind, UserEvent},
};

use super::{Coalescer, *};

struct LatestUserEvents {
  ltime: LamportTime,
  events: Vec<UserEvent>,
}

#[repr(transparent)]
pub(crate) struct UserEventCoalescer {
  events: HashMap<SmolStr, LatestUserEvents>,
}

#[showbiz_core::async_trait::async_trait]
impl Coalescer for UserEventCoalescer {
  fn name(&self) -> &'static str {
    "user_event_coalescer"
  }

  fn handle(&self, event: &Event) -> bool {
    match &event.0 {
      Either::Left(e) => matches!(e, EventKind::User(_)),
      Either::Right(e) => matches!(&**e, EventKind::User(_)),
    }
  }

  fn coalesce(&mut self, event: Event) {
    let event = match event.0 {
      Either::Left(EventKind::User(e)) => e,
      Either::Right(e) => match &*e {
        EventKind::User(e) => e.clone(),
        _ => unreachable!(),
      },
      Either::Left(_) => unreachable!(),
    };

    let ltime = *event.ltime();
    match self.events.get_mut(event.name()) {
      Some(latest) => {
        if latest.ltime < ltime {
          latest.events.clear();
          latest.ltime = ltime;
          latest.events.push(event);
          return;
        }

        // If the the same age, save it
        if latest.ltime == ltime {
          latest.events.push(event);
        }
      }
      None => {
        self.events.insert(
          event.name().clone(),
          LatestUserEvents {
            ltime,
            events: vec![event],
          },
        );
      }
    }
  }

  async fn flush(&mut self, out_tx: &Sender<Event>) -> Result<(), super::ClosedOutChannel> {
    for (_, latest) in self.events.drain() {
      for event in latest.events {
        if out_tx.send(Event::from(event)).await.is_err() {
          return Err(super::ClosedOutChannel);
        }
      }
    }
    Ok(())
  }
}
