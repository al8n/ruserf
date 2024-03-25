use std::{collections::HashMap, marker::PhantomData};

use either::Either;
use smol_str::SmolStr;

use crate::{
  event::{EventKind, UserEvent},
  types::LamportTime,
};

use super::*;

struct LatestUserEvents {
  ltime: LamportTime,
  events: Vec<UserEvent>,
}

#[derive(Default)]
#[repr(transparent)]
pub(crate) struct UserEventCoalescer<T, D> {
  events: HashMap<SmolStr, LatestUserEvents>,
  _m: PhantomData<(D, T)>,
}

impl<T, D> UserEventCoalescer<T, D> {
  pub(crate) fn new() -> Self {
    Self {
      events: HashMap::new(),
      _m: PhantomData,
    }
  }
}

impl<T, D> Coalescer for UserEventCoalescer<T, D>
where
  D: Delegate<Id = T::Id, Address = <T::Resolver as AddressResolver>::ResolvedAddress>,
  T: Transport,
  <<T::Runtime as RuntimeLite>::Sleep as Future>::Output: Send,
  <<T::Runtime as RuntimeLite>::Interval as Stream>::Item: Send,
{
  type Delegate = D;
  type Transport = T;

  fn name(&self) -> &'static str {
    "user_event_coalescer"
  }

  fn handle(&self, event: &Event<Self::Transport, Self::Delegate>) -> bool {
    match &event.0 {
      Either::Left(e) => matches!(e, EventKind::User(_)),
      Either::Right(e) => matches!(&**e, EventKind::User(_)),
    }
  }

  fn coalesce(&mut self, event: Event<Self::Transport, Self::Delegate>) {
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

  async fn flush(
    &mut self,
    out_tx: &Sender<Event<Self::Transport, Self::Delegate>>,
  ) -> Result<(), super::ClosedOutChannel> {
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
