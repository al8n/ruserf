use agnostic::Runtime;
use async_channel::{bounded, Receiver, Sender};
use showbiz_core::{
  futures_util::{self, FutureExt},
  tracing,
  transport::Transport,
};

use crate::{
  delegate::MergeDelegate,
  event::{Event, EventKind, InternalQueryEventType, QueryEvent},
};

pub(crate) struct SerfQueries<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  in_rx: Receiver<Event<D, T>>,
  out_tx: Sender<Event<D, T>>,
  shutdown_rx: Receiver<()>,
}

impl<D, T> SerfQueries<D, T>
where
  D: MergeDelegate,
  T: Transport,
{
  pub(crate) fn new<R: Runtime>(
    out_tx: Sender<Event<D, T>>,
    shutdown_rx: Receiver<()>,
  ) -> Sender<Event<D, T>> {
    let (in_tx, in_rx) = bounded(1024);
    let this = Self {
      in_rx,
      out_tx,
      shutdown_rx,
    };
    this.stream();
    in_tx
  }

  /// A long running routine to ingest the event stream
  fn stream(self) {
    <T::Runtime as Runtime>::spawn_detach(async move {
      loop {
        futures_util::select! {
          ev = self.in_rx.recv().fuse() => {
            match ev {
              Ok(ev) => {
                // Check if this is a query we should process
                if ev.is_internal_query() {
                  <T::Runtime as Runtime>::spawn_detach(async move {
                    Self::handle_query(ev).await
                  });
                }
              },
              Err(err) => {
                tracing::error!(target="ruserf", err=%err, "failed to receive event in serf query thread");
                return;
              }
            }
          }
          _ = self.shutdown_rx.recv().fuse() => {
            return;
          }
        }
      }
    });
  }

  async fn handle_query(ev: Event<D, T>) {
    macro_rules! handle_query {
      ($ev: expr) => {{
        match $ev {
          EventKind::InternalQuery { ty, query: ev } => match ty {
            InternalQueryEventType::Ping => {}
            InternalQueryEventType::Conflict => {
              Self::handle_conflict(&ev).await;
            }
            InternalQueryEventType::InstallKey => {
              Self::handle_install_key(&ev).await;
            }
            InternalQueryEventType::UseKey => {
              Self::handle_use_key(&ev).await;
            }
            InternalQueryEventType::RemoveKey => {
              Self::handle_remove_key(&ev).await;
            }
            InternalQueryEventType::ListKey => {
              Self::handle_list_key(&ev).await;
            }
          },
          _ => unreachable!(),
        }
      }};
    }

    match ev.0 {
      either::Either::Left(ev) => handle_query!(ev),
      either::Either::Right(ev) => handle_query!(&*ev),
    }
  }

  async fn handle_conflict(ev: &QueryEvent<D, T>) {}

  async fn handle_install_key(ev: &QueryEvent<D, T>) {}

  async fn handle_use_key(ev: &QueryEvent<D, T>) {}

  async fn handle_remove_key(ev: &QueryEvent<D, T>) {}

  async fn handle_list_key(ev: &QueryEvent<D, T>) {}
}
