use std::sync::{
  atomic::{AtomicU64, Ordering},
  Arc,
};

/// A lamport time is a simple u64 that represents a point in time.
#[derive(
  Debug,
  Default,
  Clone,
  Copy,
  PartialEq,
  Eq,
  Hash,
  PartialOrd,
  Ord,
  serde::Serialize,
  serde::Deserialize,
)]
#[serde(transparent)]
pub struct LamportTime(pub(crate) u64);

impl core::fmt::Display for LamportTime {
  fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
    write!(f, "{}", self.0)
  }
}

impl From<u64> for LamportTime {
  fn from(time: u64) -> Self {
    Self(time)
  }
}

impl From<LamportTime> for u64 {
  fn from(time: LamportTime) -> Self {
    time.0
  }
}

impl LamportTime {
  pub const ZERO: Self = Self(0);

  /// Creates a new lamport time from the given u64
  #[inline]
  pub const fn new(time: u64) -> Self {
    Self(time)
  }
}

impl core::ops::Add<Self> for LamportTime {
  type Output = Self;

  #[inline]
  fn add(self, rhs: Self) -> Self::Output {
    Self(self.0 + rhs.0)
  }
}

impl core::ops::Sub<Self> for LamportTime {
  type Output = Self;

  #[inline]
  fn sub(self, rhs: Self) -> Self::Output {
    Self(self.0 - rhs.0)
  }
}

impl core::ops::Rem<Self> for LamportTime {
  type Output = Self;

  #[inline]
  fn rem(self, rhs: Self) -> Self::Output {
    Self(self.0 % rhs.0)
  }
}

/// A thread safe implementation of a lamport clock. It
/// uses efficient atomic operations for all of its functions, falling back
/// to a heavy lock only if there are enough CAS failures.
#[derive(Debug, Clone)]
pub struct LamportClock(Arc<AtomicU64>);

impl Default for LamportClock {
  fn default() -> Self {
    Self::new()
  }
}

impl LamportClock {
  /// Creates a new lamport clock with the given initial value
  #[inline]
  pub fn new() -> Self {
    Self(Arc::new(AtomicU64::new(0)))
  }

  /// Return the current value of the lamport clock
  #[inline]
  pub fn time(&self) -> LamportTime {
    LamportTime(self.0.load(Ordering::SeqCst))
  }

  /// Increment and return the value of the lamport clock
  #[inline]
  pub fn increment(&self) -> LamportTime {
    LamportTime(self.0.fetch_add(1, Ordering::SeqCst) + 1)
  }

  // Witness is called to update our local clock if necessary after
  // witnessing a clock value received from another process
  #[inline]
  pub fn witness(&self, time: LamportTime) {
    loop {
      // If the other value is old, we do not need to do anything
      let current = self.0.load(Ordering::SeqCst);
      if current >= time.0 {
        return;
      }

      // Ensure that our local clock is at least one ahead.
      if self
        .0
        .compare_exchange_weak(current, time.0 + 1, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
      {
        // The CAS failed, so we just retry. Eventually our CAS should
        // succeed or a future witness will pass us by and our witness
        // will end.
        continue;
      } else {
        return;
      }
    }
  }
}

#[test]
fn test_lamport_clock() {
  let l = LamportClock::new();

  assert_eq!(l.time(), 0.into());
  assert_eq!(l.increment(), 1.into());
  assert_eq!(l.time(), 1.into());

  l.witness(41.into());
  assert_eq!(l.time(), 42.into());

  l.witness(41.into());
  assert_eq!(l.time(), 42.into());

  l.witness(30.into());
  assert_eq!(l.time(), 42.into());
}
