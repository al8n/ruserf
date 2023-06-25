
mod member;
mod user;

pub(crate) use member::*;
pub(crate) use user::*;

use super::event::{EventType, Event, Events};
#[cfg(feature = "async")]
use async_channel::Sender;

#[cfg(feature = "async")]
#[showbiz_core::async_trait::async_trait]
pub(crate) trait Coalescer
{
  type Event: Event;
  type Error: std::error::Error + Send + Sync + 'static;

  fn handle(&self, event: &Self::Event) -> bool;
  /// Invoked to coalesce the given event
  async fn coalesce(&mut self, event: Self::Event) -> Result<(), Self::Error>;

  /// Invoked to flush the coalesced events
  async fn flush(&mut self, out_tx: Sender<Self::Event>) -> Result<(), Self::Error>;
}
