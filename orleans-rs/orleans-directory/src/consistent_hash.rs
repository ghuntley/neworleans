//! Consistent hash ring implementation using virtual buckets.
//!
//! The consistent hash ring provides O(log n) lookup for determining
//! which silo owns a given hash value. Each silo is assigned multiple
//! virtual buckets to ensure even distribution of the hash space.

use crate::ring_range::RingRange;
use crate::DirectoryError;
use orleans_core::SiloAddress;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use xxhash_rust::xxh32::xxh32;

/// Default number of virtual buckets per silo.
pub const DEFAULT_BUCKETS_PER_SILO: usize = 30;

/// Trait for listening to ring range changes.
pub trait IRingRangeListener: Send + Sync {
    /// Called when the ring changes and ranges are reassigned.
    fn on_range_change(&self, old_range: &RingRange, new_range: &RingRange);
}

/// Options for the consistent hash ring.
#[derive(Debug, Clone)]
pub struct ConsistentRingOptions {
    /// Number of virtual buckets per silo.
    pub buckets_per_silo: usize,
}

impl Default for ConsistentRingOptions {
    fn default() -> Self {
        Self {
            buckets_per_silo: DEFAULT_BUCKETS_PER_SILO,
        }
    }
}

/// A bucket in the consistent hash ring.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RingBucket {
    /// The hash value of this bucket.
    hash: u32,
    /// The silo that owns this bucket.
    silo: SiloAddress,
    /// The bucket index for this silo (0..buckets_per_silo).
    bucket_index: usize,
}

/// Internal ring state.
struct RingState {
    /// Sorted list of buckets by hash value.
    buckets: Vec<RingBucket>,
    /// Map from silo to its bucket hashes.
    silo_buckets: HashMap<SiloAddress, Vec<u32>>,
}

impl RingState {
    fn new() -> Self {
        Self {
            buckets: Vec::new(),
            silo_buckets: HashMap::new(),
        }
    }

    /// Returns a sorted list of all bucket hashes.
    fn all_bucket_hashes(&self) -> Vec<u32> {
        self.buckets.iter().map(|b| b.hash).collect()
    }
}

/// A consistent hash ring using virtual buckets.
///
/// Each silo is assigned multiple virtual buckets (default: 30) to ensure
/// even distribution of the hash space. Lookups are O(log n) using binary search.
#[derive(Clone)]
pub struct ConsistentHashRing {
    options: ConsistentRingOptions,
    state: Arc<RwLock<RingState>>,
    listeners: Arc<RwLock<Vec<Arc<dyn IRingRangeListener>>>>,
}

impl ConsistentHashRing {
    /// Creates a new consistent hash ring with default options.
    pub fn new() -> Self {
        Self::with_options(ConsistentRingOptions::default())
    }

    /// Creates a new consistent hash ring with the specified options.
    pub fn with_options(options: ConsistentRingOptions) -> Self {
        Self {
            options,
            state: Arc::new(RwLock::new(RingState::new())),
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Returns the number of virtual buckets per silo.
    pub fn buckets_per_silo(&self) -> usize {
        self.options.buckets_per_silo
    }

    /// Returns true if the ring has no silos.
    pub fn is_empty(&self) -> bool {
        self.state.read().buckets.is_empty()
    }

    /// Returns the number of silos in the ring.
    pub fn silo_count(&self) -> usize {
        self.state.read().silo_buckets.len()
    }

    /// Returns all silos in the ring.
    pub fn get_silos(&self) -> Vec<SiloAddress> {
        self.state.read().silo_buckets.keys().cloned().collect()
    }

    /// Adds a silo to the ring.
    ///
    /// Creates virtual buckets for the silo and inserts them into the ring.
    /// Notifies listeners of any range changes.
    pub fn add_silo(&self, silo: SiloAddress) {
        let old_ranges = self.capture_ranges();

        {
            let mut state = self.state.write();

            // Check if already present
            if state.silo_buckets.contains_key(&silo) {
                return;
            }

            // Create virtual buckets
            let mut bucket_hashes = Vec::with_capacity(self.options.buckets_per_silo);

            for i in 0..self.options.buckets_per_silo {
                let hash = self.compute_bucket_hash(&silo, i);
                bucket_hashes.push(hash);

                state.buckets.push(RingBucket {
                    hash,
                    silo: silo.clone(),
                    bucket_index: i,
                });
            }

            // Sort buckets by hash
            state.buckets.sort_by_key(|b| b.hash);

            // Store silo's bucket hashes
            state.silo_buckets.insert(silo, bucket_hashes);
        }

        // Notify listeners
        self.notify_range_changes(&old_ranges);
    }

    /// Removes a silo from the ring.
    ///
    /// Removes all virtual buckets for the silo.
    /// Notifies listeners of any range changes.
    pub fn remove_silo(&self, silo: &SiloAddress) {
        let old_ranges = self.capture_ranges();

        {
            let mut state = self.state.write();

            // Remove from silo map
            if state.silo_buckets.remove(silo).is_none() {
                return; // Silo wasn't in the ring
            }

            // Remove buckets
            state.buckets.retain(|b| &b.silo != silo);
        }

        // Notify listeners
        self.notify_range_changes(&old_ranges);
    }

    /// Gets the primary silo for a given hash value.
    ///
    /// Returns the silo that owns the bucket immediately clockwise
    /// from the given hash value.
    pub fn get_primary_silo(&self, hash: u32) -> Result<SiloAddress, DirectoryError> {
        let state = self.state.read();

        if state.buckets.is_empty() {
            return Err(DirectoryError::EmptyRing);
        }

        // Binary search for the first bucket >= hash
        let idx = match state.buckets.binary_search_by_key(&hash, |b| b.hash) {
            Ok(i) => i,      // Exact match
            Err(i) => i,     // First bucket > hash
        };

        // Wrap around if we're past the last bucket
        let bucket_idx = if idx >= state.buckets.len() { 0 } else { idx };

        Ok(state.buckets[bucket_idx].silo.clone())
    }

    /// Gets the ring range owned by a silo.
    ///
    /// Returns the hash ranges for which this silo is the primary owner.
    pub fn get_silo_range(&self, silo: &SiloAddress) -> RingRange {
        let state = self.state.read();

        let bucket_hashes = match state.silo_buckets.get(silo) {
            Some(hashes) => hashes.clone(),
            None => return RingRange::empty(),
        };

        let all_buckets = state.all_bucket_hashes();
        RingRange::from_buckets(&bucket_hashes, &all_buckets)
    }

    /// Checks if a silo is in the ring.
    pub fn contains_silo(&self, silo: &SiloAddress) -> bool {
        self.state.read().silo_buckets.contains_key(silo)
    }

    /// Adds a ring range listener.
    pub fn add_listener(&self, listener: Arc<dyn IRingRangeListener>) {
        self.listeners.write().push(listener);
    }

    /// Computes the hash for a virtual bucket.
    fn compute_bucket_hash(&self, silo: &SiloAddress, bucket_index: usize) -> u32 {
        // Create a stable string representation
        let key = format!(
            "{}:{}:{}",
            silo.endpoint(),
            silo.generation(),
            bucket_index
        );
        xxh32(key.as_bytes(), 0)
    }

    /// Captures the current ranges for all silos.
    fn capture_ranges(&self) -> HashMap<SiloAddress, RingRange> {
        let state = self.state.read();
        let all_buckets = state.all_bucket_hashes();

        state
            .silo_buckets
            .iter()
            .map(|(silo, hashes)| {
                (
                    silo.clone(),
                    RingRange::from_buckets(hashes, &all_buckets),
                )
            })
            .collect()
    }

    /// Notifies listeners of range changes.
    fn notify_range_changes(&self, old_ranges: &HashMap<SiloAddress, RingRange>) {
        let new_ranges = self.capture_ranges();
        let listeners = self.listeners.read();

        for listener in listeners.iter() {
            // Notify for each silo whose range changed
            for (silo, new_range) in &new_ranges {
                let old_range = old_ranges
                    .get(silo)
                    .cloned()
                    .unwrap_or_else(RingRange::empty);

                listener.on_range_change(&old_range, new_range);
            }

            // Notify for removed silos
            for (silo, old_range) in old_ranges {
                if !new_ranges.contains_key(silo) {
                    listener.on_range_change(old_range, &RingRange::empty());
                }
            }
        }
    }
}

impl Default for ConsistentHashRing {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ConsistentHashRing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read();
        f.debug_struct("ConsistentHashRing")
            .field("buckets_per_silo", &self.options.buckets_per_silo)
            .field("silo_count", &state.silo_buckets.len())
            .field("bucket_count", &state.buckets.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_silo(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    #[test]
    fn test_empty_ring() {
        let ring = ConsistentHashRing::new();
        assert!(ring.is_empty());
        assert_eq!(ring.silo_count(), 0);
        assert!(ring.get_primary_silo(0).is_err());
    }

    #[test]
    fn test_single_silo() {
        let ring = ConsistentHashRing::new();
        let silo = make_silo(11111);

        ring.add_silo(silo.clone());

        assert!(!ring.is_empty());
        assert_eq!(ring.silo_count(), 1);
        assert!(ring.contains_silo(&silo));

        // All hashes should map to the single silo
        assert_eq!(ring.get_primary_silo(0).unwrap(), silo);
        assert_eq!(ring.get_primary_silo(u32::MAX).unwrap(), silo);
        assert_eq!(ring.get_primary_silo(0x80000000).unwrap(), silo);

        // Silo should own the full ring
        let range = ring.get_silo_range(&silo);
        assert!(!range.is_empty());
    }

    #[test]
    fn test_add_remove_silo() {
        let ring = ConsistentHashRing::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        ring.add_silo(silo1.clone());
        ring.add_silo(silo2.clone());

        assert_eq!(ring.silo_count(), 2);
        assert!(ring.contains_silo(&silo1));
        assert!(ring.contains_silo(&silo2));

        ring.remove_silo(&silo1);

        assert_eq!(ring.silo_count(), 1);
        assert!(!ring.contains_silo(&silo1));
        assert!(ring.contains_silo(&silo2));

        // All hashes should now map to silo2
        assert_eq!(ring.get_primary_silo(0).unwrap(), silo2);
    }

    #[test]
    fn test_deterministic_placement() {
        let ring = ConsistentHashRing::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        ring.add_silo(silo1);
        ring.add_silo(silo2);
        ring.add_silo(silo3);

        // Same hash should always map to same silo
        let hash = 0x12345678;
        let expected = ring.get_primary_silo(hash).unwrap();

        for _ in 0..100 {
            assert_eq!(ring.get_primary_silo(hash).unwrap(), expected);
        }
    }

    #[test]
    fn test_distribution() {
        let ring = ConsistentHashRing::new();
        let silos: Vec<_> = (11111..11114).map(make_silo).collect();

        for silo in &silos {
            ring.add_silo(silo.clone());
        }

        // Count how many hashes map to each silo
        let mut counts = HashMap::new();
        for i in 0u32..10000 {
            let hash = xxh32(&i.to_le_bytes(), 0);
            let silo = ring.get_primary_silo(hash).unwrap();
            *counts.entry(silo).or_insert(0) += 1;
        }

        // Each silo should get roughly 1/3 of hashes (with some variance)
        let expected = 10000 / 3;
        let tolerance = expected / 2; // 50% tolerance

        for (_silo, count) in counts {
            assert!(
                count > expected - tolerance && count < expected + tolerance,
                "Distribution too uneven: {} (expected ~{})",
                count,
                expected
            );
        }
    }

    #[test]
    fn test_add_silo_idempotent() {
        let ring = ConsistentHashRing::new();
        let silo = make_silo(11111);

        ring.add_silo(silo.clone());
        let buckets_before = ring.state.read().buckets.len();

        ring.add_silo(silo.clone());
        let buckets_after = ring.state.read().buckets.len();

        assert_eq!(buckets_before, buckets_after);
    }

    #[test]
    fn test_remove_nonexistent_silo() {
        let ring = ConsistentHashRing::new();
        let silo = make_silo(11111);

        // Should not panic
        ring.remove_silo(&silo);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_get_silos() {
        let ring = ConsistentHashRing::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        ring.add_silo(silo1.clone());
        ring.add_silo(silo2.clone());

        let silos = ring.get_silos();
        assert_eq!(silos.len(), 2);
        assert!(silos.contains(&silo1));
        assert!(silos.contains(&silo2));
    }

    #[test]
    fn test_custom_buckets_per_silo() {
        let options = ConsistentRingOptions {
            buckets_per_silo: 10,
        };
        let ring = ConsistentHashRing::with_options(options);
        let silo = make_silo(11111);

        ring.add_silo(silo);

        let state = ring.state.read();
        assert_eq!(state.buckets.len(), 10);
    }
}
