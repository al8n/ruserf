mod member;
pub(crate) use member::*;
mod user;
pub(crate) use user::*;

use std::{future::Future, time::Duration};

use agnostic::Runtime;
use async_channel::{bounded, Receiver, Sender};
use showbiz_core::{
  futures_util::{self, FutureExt, Stream},
  tracing,
  transport::Transport,
};

use super::{delegate::MergeDelegate, event::Event, serf::ReconnectTimeoutOverrider};

pub(crate) struct ClosedOutChannel;

#[showbiz_core::async_trait::async_trait]
pub(crate) trait Coalescer: Send + Sync + 'static {
  type Delegate: MergeDelegate;
  type Transport: Transport;
  type Overrider: ReconnectTimeoutOverrider;

  fn name(&self) -> &'static str;

  fn handle(&self, event: &Event<Self::Transport, Self::Delegate, Self::Overrider>) -> bool
  where
    <<<Self::Transport as Transport>::Runtime as Runtime>::Sleep as Future>::Output: Send,
    <<<Self::Transport as Transport>::Runtime as Runtime>::Interval as Stream>::Item: Send;

  /// Invoked to coalesce the given event
  fn coalesce(&mut self, event: Event<Self::Transport, Self::Delegate, Self::Overrider>)
  where
    <<<Self::Transport as Transport>::Runtime as Runtime>::Sleep as Future>::Output: Send,
    <<<Self::Transport as Transport>::Runtime as Runtime>::Interval as Stream>::Item: Send;

  /// Invoked to flush the coalesced events
  async fn flush(
    &mut self,
    out_tx: &Sender<Event<Self::Transport, Self::Delegate, Self::Overrider>>,
  ) -> Result<(), ClosedOutChannel>
  where
    <<<Self::Transport as Transport>::Runtime as Runtime>::Sleep as Future>::Output: Send,
    <<<Self::Transport as Transport>::Runtime as Runtime>::Interval as Stream>::Item: Send;
}

/// Returns an event channel where the events are coalesced
/// using the given coalescer.
pub(crate) fn coalesced_event<C: Coalescer>(
  out_tx: Sender<Event<C::Transport, C::Delegate, C::Overrider>>,
  shutdown_rx: Receiver<()>,
  c_period: Duration,
  q_period: Duration,
  c: C,
) -> Sender<Event<C::Transport, C::Delegate, C::Overrider>>
where
  <<<C::Transport as Transport>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<<C::Transport as Transport>::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  let (in_tx, in_rx) = bounded(1024);
  <<C::Transport as Transport>::Runtime as Runtime>::spawn_detach(coalesce_loop::<C>(
    in_rx,
    out_tx,
    shutdown_rx,
    c_period,
    q_period,
    c,
  ));
  in_tx
}

/// A simple long-running routine that manages the high-level
/// flow of coalescing based on quiescence and a maximum quantum period.
async fn coalesce_loop<C: Coalescer>(
  in_rx: Receiver<Event<C::Transport, C::Delegate, C::Overrider>>,
  out_tx: Sender<Event<C::Transport, C::Delegate, C::Overrider>>,
  shutdown_rx: Receiver<()>,
  coalesce_peirod: Duration,
  quiescent_period: Duration,
  mut c: C,
) where
  <<<C::Transport as Transport>::Runtime as Runtime>::Sleep as Future>::Output: Send,
  <<<C::Transport as Transport>::Runtime as Runtime>::Interval as Stream>::Item: Send,
{
  let mut quiescent = None;
  let mut quantum = None;
  let mut shutdown = false;

  loop {
    futures_util::select! {
      ev = in_rx.recv().fuse() => {
        let Ok(ev) = ev else {
          // if we receive an error, it means the channel is closed. We should return
          return;
        };

        // Ignore any non handled events
        if !c.handle(&ev) {
          if let Err(e) = out_tx.send(ev).await {
            tracing::error!(target = "ruserf", err=%e, "fail send event to out channel in {} coalesce thread", c.name());
            return;
          }
          continue;
        }

        // Start a new quantum if we need to
        // and restart the quiescent timer
        if quantum.is_none() {
          quantum = Some(<<C::Transport as Transport>::Runtime as Runtime>::sleep(coalesce_peirod));
        }
        quiescent = Some(<<C::Transport as Transport>::Runtime as Runtime>::sleep(quiescent_period));

        // Coalesce the event
        c.coalesce(ev);
      }
      _ = async {
        if let Some(quantum) = quantum.take() {
          quantum.await;

        } else {
          std::future::pending::<()>().await;
        }
      }.fuse() => {
        // Flush the coalesced events
        if c.flush(&out_tx).await.is_err() {
          tracing::error!(target = "ruserf", err="closed channel", "fail send event to out channel in {} coalesce thread", c.name());
          return;
        }

        // Restart ingestion if we are not done
        if !shutdown {
          quiescent = None;
          quantum = None;
          continue;
        }

        return;
      }
      _ = async {
        if let Some(quiescent) = quiescent.take() {
          quiescent.await;
        } else {
          std::future::pending::<()>().await;
        }
      }.fuse() => {
        // Flush the coalesced events
        if c.flush(&out_tx).await.is_err() {
          tracing::error!(target = "ruserf", err="closed channel", "fail send event to out channel in {} coalesce thread", c.name());
          return;
        }

        // Restart ingestion if we are not done
        if !shutdown {
          quantum = None;
          quiescent = None;
          continue;
        }

        return;
      }
      _ = shutdown_rx.recv().fuse() => {
        shutdown = true;
      }
    }
  }
}
