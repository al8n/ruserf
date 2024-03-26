use smol_str::SmolStr;

use std::time::Duration;

use memberlist_core::{bytes::Bytes, transport::Node, types::TinyVec};

use super::LamportTime;

bitflags::bitflags! {
  #[derive(PartialEq, Eq)]
  pub(crate) struct QueryFlag: u32 {
    /// Ack flag is used to force receiver to send an ack back
    const ACK = 1 << 0;
    /// NoBroadcast is used to prevent re-broadcast of a query.
    /// this can be used to selectively send queries to individual members
    const NO_BROADCAST = 1 << 1;
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryMessage<I, A> {
  /// Event lamport time
  ltime: LamportTime,
  /// query id, randomly generated
  id: u32,
  /// source node
  from: Node<I, A>,
  /// Potential query filters
  filters: TinyVec<Bytes>,
  /// Used to provide various flags
  flags: u32,
  /// Used to set the number of duplicate relayed responses
  relay_factor: u8,
  /// Maximum time between delivery and response
  timeout: Duration,
  /// Query nqme
  name: SmolStr,
  /// Query payload
  payload: Bytes,
}

impl<I, A> QueryMessage<I, A> {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::ACK) != QueryFlag::empty()
  }

  /// checks if the no broadcast flag is set
  #[inline]
  pub(crate) fn no_broadcast(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::NO_BROADCAST) != QueryFlag::empty()
  }
}

#[viewit::viewit]
#[derive(Debug, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct QueryResponseMessage<I, A> {
  /// Event lamport time
  ltime: LamportTime,
  /// query id
  id: u32,
  /// node
  from: Node<I, A>,
  /// Used to provide various flags
  flags: u32,
  /// Optional response payload
  payload: Bytes,
}

impl<I, A> QueryResponseMessage<I, A> {
  /// checks if the ack flag is set
  #[inline]
  pub(crate) fn ack(&self) -> bool {
    (QueryFlag::from_bits_retain(self.flags) & QueryFlag::ACK) != QueryFlag::empty()
  }
}
