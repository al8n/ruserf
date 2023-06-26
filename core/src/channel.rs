use std::sync::{atomic::AtomicBool, Arc};

pub struct Sender<T> {
  closed: Arc<AtomicBool>,
  sender: async_channel::Sender<T>,
}

impl<T> Sender<T> {}
