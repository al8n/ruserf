use std::sync::Arc;

use showbiz_core::{transport::Transport, Showbiz, Spawner};

use crate::delegate::SerfDelegate;

pub(crate) struct SerfCore<T: Transport, S: Spawner> {
  memberlist: Showbiz<SerfDelegate<T, S>, T, S>,
}

#[repr(transparent)]
pub struct Serf<T: Transport, S: Spawner> {
  inner: Arc<SerfCore<T, S>>,
}

impl<T: Transport, S: Spawner> Clone for Serf<T, S> {
  fn clone(&self) -> Self {
    Self {
      inner: self.inner.clone(),
    }
  }
}
