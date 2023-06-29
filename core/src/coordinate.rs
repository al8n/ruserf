use std::{
  collections::HashMap,
  sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
  },
  time::Duration,
};

use async_lock::RwLock;
use serde::{Deserialize, Serialize};
use showbiz_core::Name;
use smallvec::SmallVec;

/// Used to convert float seconds to nanoseconds.
const SECONDS_TO_NANOSECONDS: f64 = 1.0e9;
/// Used to decide if two coordinates are on top of each
/// other.
const ZERO_THRESHOLD: f64 = 1.0e-6;

/// The default dimensionality of the coordinate system.
const DEFAULT_DIMENSIONALITY: usize = 8;

/// The default adjustment window size.
const DEFAULT_ADJUSTMENT_WINDOW_SIZE: usize = 20;

const DEFAULT_LATENCY_FILTER_SAMPLES_SIZE: usize = 8;

#[derive(thiserror::Error)]
pub enum CoordinateError {
  #[error("dimensions aren't compatible")]
  DimensionalityMismatch,
  #[error("invalid coordinate")]
  InvalidCoordinate,
  #[error("round trip time not in valid range, duration {0} is not a value less than 10s")]
  InvalidRTT(Duration),
}

/// CoordinateOptions is used to set the parameters of the Vivaldi-based coordinate mapping
/// algorithm.
///
/// The following references are called out at various points in the documentation
/// here:
///
/// [1] Dabek, Frank, et al. "Vivaldi: A decentralized network coordinate system."
///     ACM SIGCOMM Computer Communication Review. Vol. 34. No. 4. ACM, 2004.
/// [2] Ledlie, Jonathan, Paul Gardner, and Margo I. Seltzer. "Network Coordinates
///     in the Wild." NSDI. Vol. 7. 2007.
/// [3] Lee, Sanghwan, et al. "On suitability of Euclidean embedding for
///     host-based network coordinate systems." Networking, IEEE/ACM Transactions
///     on 18.1 (2010): 27-40.
#[viewit::viewit]
#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize)]
pub struct CoordinateOptions {
  /// The dimensionality of the coordinate system. As discussed in [2], more
  /// dimensions improves the accuracy of the estimates up to a point. Per [2]
  /// we chose 8 dimensions plus a non-Euclidean height.
  dimensionality: usize,

  /// The default error value when a node hasn't yet made
  /// any observations. It also serves as an upper limit on the error value in
  /// case observations cause the error value to increase without bound.
  vivaldi_error_max: f64,

  /// A tuning factor that controls the maximum impact an
  /// observation can have on a node's confidence. See [1] for more details.
  vivaldi_ce: f64,

  /// A tuning factor that controls the maximum impact an
  /// observation can have on a node's coordinate. See [1] for more details.
  vivaldi_cc: f64,

  /// A tuning factor that determines how many samples
  /// we retain to calculate the adjustment factor as discussed in [3]. Setting
  /// this to zero disables this feature.
  adjustment_window_size: usize,

  /// The minimum value of the height parameter. Since this
  /// always must be positive, it will introduce a small amount error, so
  /// the chosen value should be relatively small compared to "normal"
  /// coordinates.
  height_min: f64,

  /// The maximum number of samples that are retained
  /// per node, in order to compute a median. The intent is to ride out blips
  /// but still keep the delay low, since our time to probe any given node is
  /// pretty infrequent. See [2] for more details.
  latency_filter_size: usize,

  /// A tuning factor that sets how much gravity has an effect
  /// to try to re-center coordinates. See [2] for more details.
  gravity_rho: f64,
}

impl Default for CoordinateOptions {
  fn default() -> Self {
    Self::new()
  }
}

impl CoordinateOptions {
  /// Returns a `CoordinateOptions` that has some default values suitable for
  /// basic testing of the algorithm, but not tuned to any particular type of cluster.
  #[inline]
  pub const fn new() -> Self {
    Self {
      dimensionality: DEFAULT_DIMENSIONALITY,
      vivaldi_error_max: 1.5,
      vivaldi_ce: 0.25,
      vivaldi_cc: 0.25,
      adjustment_window_size: 20,
      height_min: 10.0e-6,
      latency_filter_size: 3,
      gravity_rho: 150.0,
    }
  }
}

/// Used to record events that occur when updating coordinates.
#[viewit::viewit]
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoordinateClientStats {
  /// Incremented any time we reset our local coordinate because
  /// our calculations have resulted in an invalid state.
  resets: usize,
}

impl Default for CoordinateClientStats {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl CoordinateClientStats {
  #[inline]
  const fn new() -> Self {
    Self { resets: 0 }
  }
}

struct CoordinateClientInner {
  /// The current estimate of the client's network coordinate.
  coord: Coordinate,

  /// Origin is a coordinate sitting at the origin.
  origin: Coordinate,

  /// Contains the tuning parameters that govern the performance of
  /// the algorithm.
  opts: CoordinateOptions,

  /// The current index into the adjustmentSamples slice.
  adjustment_index: usize,

  /// Used to store samples for the adjustment calculation.
  adjustment_samples: SmallVec<[f64; DEFAULT_ADJUSTMENT_WINDOW_SIZE]>,

  /// Used to store the last several RTT samples,
  /// keyed by node name. We will use the config's LatencyFilterSamples
  /// value to determine how many samples we keep, per node.
  latency_filter_samples: HashMap<Name, SmallVec<[f64; DEFAULT_LATENCY_FILTER_SAMPLES_SIZE]>>,
}

impl CoordinateClientInner {
  /// Applies a small amount of gravity to pull coordinates towards
  /// the center of the coordinate system to combat drift. This assumes that the
  /// mutex is locked already.
  #[inline]
  fn update_gravity(&mut self) {
    let dist = self.origin.distance_to(&self.coord).as_secs();
    let force = -1.0 * f64::powf(dist / self.opts.gravity_rho, 2.0);
    self
      .coord
      .apply_force_in_place(self.opts.height_min, force, &self.origin);
  }

  #[inline]
  fn latency_filter(&mut self, node: &Name, rtt_seconds: f64) -> f64 {
    let samples = self
      .latency_filter_samples
      .entry(node.clone())
      .or_insert_with(|| {
        let mut v = SmallVec::with_capacity(self.opts.latency_filter_size);
        v.resize(self.opts.latency_filter_size, 0);
      });

    // Add the new sample and trim the list, if needed.
    samples.push(rtt_seconds);
    if samples.len() > self.opts.latency_filter_size {
      samples.remove(0);
    }
    // Sort a copy of the samples and return the median.
    samples.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    samples[self.opts.latency_filter_size / 2]
  }

  /// Updates the Vivaldi portion of the client's coordinate. This
  /// assumes that the mutex has been locked already.
  fn update_vivaldi(&mut self, other: &Coordinate, mut rtt_seconds: f64) {
    const ZERO_THRESHOLD: f64 = 1.0e-6;

    let dist = self.coord.distance_to(other).as_secs_f64();
    rtt_seconds = rtt_seconds.max(ZERO_THRESHOLD);

    let wrongness = ((dist - rtt_seconds) / rtt_seconds).abs();

    let total_error = (self.coord.error + other.error).max(ZERO_THRESHOLD);

    let weight = self.coord.error / total_error;

    self.coord.error = ((self.opts.vivaldi_ce * weight * wrongness)
      + (self.coord.error * (1.0 - self.opts.vivaldi_ce * weight)))
      .min(self.opts.vivaldi_error_max);

    let force = self.opts.vivaldi_cc * weight * (rtt_seconds - dist);
    self
      .coord
      .apply_force_in_place(self.opts.height_min, force, other);
  }

  /// Updates the adjustment portion of the client's coordinate, if
  /// the feature is enabled. This assumes that the mutex has been locked already.
  fn update_adjustment(&mut self, other: &Coordinate, rtt_seconds: f64) {
    if self.opts.adjustment_window_size == 0 {
      return;
    }
    // Note that the existing adjustment factors don't figure in to this
    // calculation so we use the raw distance here.
    let dist = self.coord.raw_distance_to(other);
    self.adjustment_samples[self.adjustment_index] = rtt_seconds - dist;
    self.adjustment_index = (self.adjustment_index + 1) % self.opts.adjustment_window_size;

    self.coord.adjustment =
      self.adjustment_samples.iter().sum::<f64>() / (2.0 * self.opts.adjustment_window_size as f64);
  }
}

/// Manages the estimated network coordinate for a given node, and adjusts
/// it as the node observes round trip times and estimated coordinates from other
/// nodes. The core algorithm is based on Vivaldi, see the documentation for Config
/// for more details.
///
/// `CoordinateClient` is thread-safe.
#[repr(transparent)]
pub struct CoordinateClient {
  inner: RwLock<CoordinateClientInner>,
  /// Used to record events that occur when updating coordinates.
  stats: AtomicUsize,
}

impl CoordinateClient {
  /// Creates a new client.
  #[inline]
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(CoordinateClientInner {
        coord: Coordinate::new(),
        origin: Coordinate::new(),
        opts: CoordinateOptions::new(),
        adjustment_index: 0,
        adjustment_samples: SmallVec::from_slice(&[0.0; DEFAULT_ADJUSTMENT_WINDOW_SIZE]),
        latency_filter_samples: HashMap::new(),
      })),
      stats: AtomicUsize::new(0),
    }
  }

  /// Creates a new client with given options.
  #[inline]
  pub fn with_options(opts: CoordinateOptions) -> Self {
    let mut adjustment_samples = SmallVec::with_capacity(opts.adjustment_window_size);
    adjustment_samples.resize(opts.adjustment_window_size, 0);
    Self {
      inner: Arc::new(RwLock::new(CoordinateClientInner {
        coord: Coordinate::with_options(opts),
        origin: Coordinate::with_options(opts),
        opts,
        adjustment_index: 0,
        adjustment_samples,
        latency_filter_samples: HashMap::new(),
      })),
      stats: AtomicUsize::new(0),
    }
  }

  /// Returns a copy of the coordinate for this client.
  #[inline]
  pub async fn get_coordinate(&self) -> Coordinate {
    self.inner.read().await.coord.clone()
  }

  /// Forces the client's coordinate to a known state.
  #[inline]
  pub async fn set_coordinate(&self, coord: Coordinate) -> Result<(), CoordinateError> {
    let mut l = self.inner.write().await;
    Self::check_coordinate(&l.coord, &coord).map(|_| l.coord = coord)
  }

  /// Removes any client state for the given node.
  #[inline]
  pub async fn forget_node(&self, node: &Name) {
    self.inner.write().await.latency_filter_samples.remove(node);
  }

  /// Returns a copy of stats for the client.
  #[inline]
  pub fn stats(&self) -> CoordinateClientStats {
    CoordinateClientStats {
      resets: self.stats.load(Ordering::Relaxed),
    }
  }

  /// Update takes other, a coordinate for another node, and rtt, a round trip
  /// time observation for a ping to that node, and updates the estimated position of
  /// the client's coordinate. Returns the updated coordinate.
  pub async fn update(
    &self,
    node: &Name,
    other: &Coordinate,
    rtt: Duration,
  ) -> Result<Coordinate, CoordinateError> {
    let mut l = self.inner.write().await;
    Self::check_coordinate(&l.coord, other)?;

    // The code down below can handle zero RTTs, which we have seen in
    // https://github.com/hashicorp/consul/issues/3789, presumably in
    // environments with coarse-grained monotonic clocks (we are still
    // trying to pin this down). In any event, this is ok from a code PoV
    // so we don't need to alert operators with spammy messages. We did
    // add a counter so this is still observable, though.
    const MAX_RTT: Duration = Duration::from_secs(10);

    if rtt > MAX_RTT {
      return Err(CoordinateError::InvalidRTT(rtt));
    }

    if rtt.is_zero() {
      // TODO: metrics
    }

    let rtt_seconds = l.latency_filter(node, rtt.as_secs_f64());
    l.update_vivaldi(other, rtt_seconds);
    l.update_adjustment(other, rtt_seconds);
    l.update_gravity();

    if !l.coord.is_valid() {
      self.stats.fetch_add(1, Ordering::Acquire);
      l.coord = Coordinate::with_options(l.opts);
    }

    Ok(l.coord.clone())
  }

  /// Returns the estimated RTT from the client's coordinate to other, the
  /// coordinate for another node.
  #[inline]
  pub async fn distance_to(&self, coord: &Coordinate) -> f64 {
    self.inner.read().await.coord.distance_to(coord)
  }

  /// Returns an error if the coordinate isn't compatible with
  /// this client, or if the coordinate itself isn't valid. This assumes the mutex
  /// has been locked already.
  #[inline]
  fn check_coordinate(this: &Coordinate, coord: &Coordinate) -> Result<(), CoordinateError> {
    if !this.is_compatible_with(coord) {
      return Err(CoordinateError::DimensionalityMismatch);
    }

    if !coord.is_valid() {
      return Err(CoordinateError::InvalidCoordinate);
    }

    Ok(())
  }
}

/// A specialized structure for holding network coordinates for the
/// Vivaldi-based coordinate mapping algorithm. All of the fields should be public
/// to enable this to be serialized. All values in here are in units of seconds.
#[viewit::viewit]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Coordinate {
  /// The Euclidean portion of the coordinate. This is used along
  /// with the other fields to provide an overall distance estimate. The
  /// units here are seconds.
  vec: SmallVec<[f64; DEFAULT_DIMENSIONALITY]>,
  /// Reflects the confidence in the given coordinate and is updated
  /// dynamically by the Vivaldi Client. This is dimensionless.
  error: f64,
  /// A distance offset computed based on a calculation over
  /// observations from all other nodes over a fixed window and is updated
  /// dynamically by the Vivaldi Client. The units here are seconds.
  adjustment: f64,
  /// A distance offset that accounts for non-Euclidean effects
  /// which model the access links from nodes to the core Internet. The access
  /// links are usually set by bandwidth and congestion, and the core links
  /// usually follow distance based on geography.
  height: f64,
}

impl Coordinate {
  /// Creates a new coordinate at the origin, using the default options
  /// to supply key initial values.
  #[inline]
  pub const fn new() -> Self {
    Self::with_options(CoordinateOptions::new())
  }

  /// Creates a new coordinate at the origin, using the given options
  /// to supply key initial values.
  #[inline]
  pub const fn with_options(opts: CoordinateOptions) -> Self {
    Self {
      vec: SmallVec::from_slice(&[0.0; DEFAULT_DIMENSIONALITY]),
      error: opts.vivaldi_error_max,
      adjustment: 0.0,
      height: opts.height_min,
    }
  }

  #[inline]
  pub fn is_valid(&self) -> bool {
    self.vec.iter().all(|&f| f.is_finite())
      && self.error.is_finite()
      && self.adjustment.is_finite()
      && self.height.is_finite()
  }

  #[inline]
  pub fn is_compatible_with(&self, other: &Self) -> bool {
    self.vec.len() == other.vec.len()
  }

  /// Returns the result of applying the force from the direction of the
  /// other coordinate.
  pub fn apply_force(&self, height_min: f64, force: f64, other: &Self) -> Self {
    assert!(
      self.is_compatible_with(other),
      "coordinate dimensionality does not match"
    );

    let mut ret = self.clone();
    let (mut unit, mag) = unit_vector_at(&self.vec, &other.vec);
    add_in_place(&mut ret.vec, &mul_in_place(&mut unit, force));
    if mag > ZERO_THRESHOLD {
      ret.height = (ret.height + other.height) * force / mag + ret.height;
      ret.height = ret.height.max(height_min);
    }
    ret
  }

  pub fn apply_force_in_place(&mut self, height_min: f64, force: f64, other: &Self) {
    assert!(
      self.is_compatible_with(other),
      "coordinate dimensionality does not match"
    );

    let mag = unit_vector_at_in_place(&mut self.vec, &other.vec);
    add_in_place(&mut self.vec, &mul_in_place(&mut self.vec, force));
    if mag > ZERO_THRESHOLD {
      self.height = (self.height + other.height) * force / mag + self.height;
      self.height = self.height.max(height_min);
    }
  }

  /// Returns the distance between this coordinate and the other
  /// coordinate, including adjustments.
  pub fn distance_to(&self, other: &Self) -> Duration {
    assert!(
      self.is_compatible_with(other),
      "coordinate dimensionality does not match"
    );

    let dist = self.raw_distance_to(other);
    let adjusted_dist = dist + self.adjustment + other.adjustment;
    let dist = if adjusted_dist > 0.0 {
      adjusted_dist
    } else {
      dist
    };
    Duration::from_nanos((dist * SECONDS_TO_NANOSECONDS) as u64)
  }

  #[inline]
  fn raw_distance_to(&self, other: &Self) -> f64 {
    magnitude_in_place(diff_in_place(&self.vec, &other.vec)) + self.height + other.height
  }
}

#[inline]
fn add_in_place(vec1: &mut [f64], vec2: &[f64]) {
  for (x, y) in vec1.iter_mut().zip(vec2.iter()) {
    *x += y;
  }
}

/// Returns difference between the vec1 and vec2. This assumes the
/// dimensions have already been checked to be compatible.
#[inline]
fn diff(vec1: &[f64], vec2: &[f64]) -> SmallVec<[f64; DEFAULT_DIMENSIONALITY]> {
  vec1.iter().zip(vec2).map(|(x, y)| x - y).collect()
}

/// computes difference between the vec1 and vec2 in place. This assumes the
/// dimensions have already been checked to be compatible.
#[inline]
fn diff_in_place(vec1: &[f64], vec2: &[f64]) -> impl Iterator<Item = f64> {
  vec1.iter().zip(vec2).map(|(x, y)| x - y)
}

/// computes difference between the vec1 and vec2 in place. This assumes the
/// dimensions have already been checked to be compatible.
#[inline]
fn diff_mut_in_place(vec1: &[f64], vec2: &[f64]) {
  vec1.iter_mut().zip(vec2).for_each(|(x, y)| *x = *x - *y);
}

/// multiplied by a scalar factor in place.
#[inline]
fn mul_in_place(vec: &mut [f64], factor: f64) -> &mut [f64] {
  for x in vec.iter_mut() {
    *x *= factor;
  }
  vec
}

/// Computes the magnitude of the vec.
#[inline]
fn magnitude_in_place(vec: impl Iterator<Item = f64>) -> f64 {
  vec.fold(0.0, |acc, &x| acc + x * x).sqrt()
}

/// Returns a unit vector pointing at vec1 from vec2. If the two
/// positions are the same then a random unit vector is returned. We also return
/// the distance between the points for use in the later height calculation.
fn unit_vector_at(vec1: &[f64], vec2: &[f64]) -> (SmallVec<[f64; DEFAULT_DIMENSIONALITY]>, f64) {
  let mut ret = diff(vec1, vec2);

  if let Some(mag) = magnitude_in_place(&ret).partial_recip() {
    mul_in_place(&mut ret, mag);
    return (ret, mag.recip());
  }

  let mut rng = rand::thread_rng();
  for x in ret.iter_mut() {
    *x = rng.gen::<f64>() - 0.5;
  }
  if let Some(mag) = magnitude_in_place(&ret).partial_recip() {
    mul_in_place(&mut ret, mag);
    return (ret, 0.0);
  }

  // And finally just give up and make a unit vector along the first
  // dimension. This should be exceedingly rare.
  ret.fill(0.0);
  ret[0] = 1.0;
  (ret, 0.0)
}

fn unit_vector_at_in_place(vec1: &mut [f64], vec2: &[f64]) -> f64 {
  diff_mut_in_place(vec1, vec2);

  if let Some(mag) = magnitude_in_place(vec1).partial_recip() {
    mul_in_place(vec1, mag);
    return mag.recip();
  }

  let mut rng = rand::thread_rng();
  for x in vec1.iter_mut() {
    *x = rng.gen::<f64>() - 0.5;
  }
  if let Some(mag) = magnitude_in_place(vec1).partial_recip() {
    mul_in_place(vec1, mag);
    return 0.0;
  }

  // And finally just give up and make a unit vector along the first
  // dimension. This should be exceedingly rare.
  vec1.fill(0.0);
  vec1[0] = 1.0;
  0.0
}
