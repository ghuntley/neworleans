//! Orleans Directory - Distributed Grain Location Service
//!
//! This crate implements the Orleans grain directory, which provides a distributed
//! lookup service for grain locations. The directory uses consistent hashing to
//! partition entries across silos, with each silo responsible for a portion of
//! the 32-bit hash space.
//!
//! # Overview
//!
//! The grain directory is a key component of Orleans' location transparency.
//! When a grain is activated, its location is registered in the directory.
//! When another grain (or client) needs to communicate with a grain, it first
//! looks up the grain's location in the directory.
//!
//! # Key Components
//!
//! - [`ConsistentHashRing`]: Virtual bucket-based consistent hashing for partition assignment
//! - [`RingRange`]: Represents a portion of the hash space
//! - [`GrainDirectoryPartition`]: Local storage for grain locations
//! - [`DistributedGrainDirectory`]: Main interface combining all components
//! - [`GrainDirectoryCache`]: LRU cache for lookup performance
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress, ActivationId, GrainAddress};
//! use orleans_directory::{DistributedGrainDirectory, GrainDirectoryOptions};
//! use orleans_clustering::MembershipVersion;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create the local silo address
//! let local_silo = SiloAddress::new("127.0.0.1:11111".parse()?, 1);
//!
//! // Create the directory
//! let directory = DistributedGrainDirectory::local_only(local_silo.clone());
//!
//! // Add silos to the ring
//! directory.ring().add_silo(local_silo.clone());
//!
//! // Create a grain ID and address
//! let grain_id = GrainId::new(
//!     GrainType::create("MyGrain"),
//!     IdSpan::from_str("key1"),
//! );
//! let address = GrainAddress::new(
//!     grain_id.clone(),
//!     ActivationId::new(),
//!     Some(local_silo),
//! );
//!
//! // Register the grain
//! directory.register(MembershipVersion::default(), address.clone(), None).await?;
//!
//! // Lookup the grain
//! let found = directory.lookup(&grain_id).await?;
//! assert_eq!(found, Some(address));
//!
//! # Ok(())
//! # }
//! ```
//!
//! # Consistent Hashing
//!
//! The directory uses consistent hashing with virtual buckets to distribute
//! entries across silos. Each silo is assigned multiple virtual buckets
//! (default: 30) to ensure even distribution. When looking up a grain, its
//! ID is hashed and the resulting value determines which silo owns the
//! directory entry.
//!
//! # Caching
//!
//! The directory includes an LRU cache to reduce lookup latency. Cache entries
//! are automatically invalidated when grains move or deactivate. Cache updates
//! can also be piggybacked on messages for efficient invalidation.
//!
//! # Membership Changes
//!
//! When silos join or leave the cluster, the directory automatically rebalances.
//! Entries are transferred to new owners as needed, and entries pointing to
//! dead silos are cleaned up.

mod cache;
mod consistent_hash;
mod distributed_directory;
mod error;
mod partition;
mod ring_range;

// Re-export public API
pub use cache::{
    CacheStats, DirectoryCacheOptions, GrainAddressCacheUpdate, GrainDirectoryCache,
    DEFAULT_CACHE_SIZE,
};
pub use consistent_hash::{
    ConsistentHashRing, ConsistentRingOptions, IRingRangeListener, DEFAULT_BUCKETS_PER_SILO,
};
pub use distributed_directory::{
    DirectoryStats, DistributedGrainDirectory, GrainDirectoryOptions, IRemoteGrainDirectory,
    LocalOnlyRemoteDirectory,
};
pub use error::{DirectoryError, DirectoryResult};
pub use partition::{GrainDirectoryPartition, PartitionStats, RegistrationResult};
pub use ring_range::{RingRange, RingSegment};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_clustering::MembershipVersion;
    use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
    use xxhash_rust::xxh32::xxh32;
    use std::net::SocketAddr;

    fn make_silo(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(GrainType::create(name), IdSpan::from_str(name))
    }

    fn make_grain_address(grain_id: &GrainId, silo: &SiloAddress) -> GrainAddress {
        GrainAddress::complete(grain_id.clone(), ActivationId::new(), silo.clone())
    }

    /// Integration test: Three silos form a directory ring
    #[tokio::test]
    async fn test_three_silo_directory() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        // Create directories for each silo
        let dir1 = DistributedGrainDirectory::local_only(silo1.clone());
        let dir2 = DistributedGrainDirectory::local_only(silo2.clone());
        let dir3 = DistributedGrainDirectory::local_only(silo3.clone());

        // Add all silos to all rings
        for dir in [&dir1, &dir2, &dir3] {
            dir.ring().add_silo(silo1.clone());
            dir.ring().add_silo(silo2.clone());
            dir.ring().add_silo(silo3.clone());
        }

        // All should have same ring state
        assert_eq!(dir1.ring().silo_count(), 3);
        assert_eq!(dir2.ring().silo_count(), 3);
        assert_eq!(dir3.ring().silo_count(), 3);

        // Same grain should map to same silo across all directories
        let grain_id = make_grain_id("test-grain");

        let primary1 = dir1.get_primary_silo(&grain_id).unwrap();
        let primary2 = dir2.get_primary_silo(&grain_id).unwrap();
        let primary3 = dir3.get_primary_silo(&grain_id).unwrap();

        assert_eq!(primary1, primary2);
        assert_eq!(primary2, primary3);
    }

    /// Integration test: Grain registration and lookup across silos
    #[tokio::test]
    async fn test_cross_silo_grain_registration() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        // Create directory for silo1 (which will own the local partition)
        let dir1 = DistributedGrainDirectory::local_only(silo1.clone());
        dir1.ring().add_silo(silo1.clone());
        dir1.ring().add_silo(silo2.clone());

        // Find a grain that maps to silo1
        let mut grain_id = make_grain_id("test-grain");
        let mut i = 0;
        while dir1.get_primary_silo(&grain_id).unwrap() != silo1 {
            i += 1;
            grain_id = make_grain_id(&format!("test-grain-{}", i));
        }

        // Register the grain on silo1
        let address = make_grain_address(&grain_id, &silo1);
        let result = dir1
            .register(MembershipVersion::default(), address.clone(), None)
            .await;

        assert!(result.is_ok());

        // Lookup should succeed
        let found = dir1.lookup(&grain_id).await.unwrap();
        assert_eq!(found, Some(address));

        // Stats should show the registration
        let stats = dir1.stats();
        assert_eq!(stats.local_grain_count, 1);
    }

    /// Integration test: Consistent hash distribution is balanced
    #[tokio::test]
    async fn test_hash_distribution() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        let dir = DistributedGrainDirectory::local_only(silo1.clone());
        dir.ring().add_silo(silo1.clone());
        dir.ring().add_silo(silo2.clone());
        dir.ring().add_silo(silo3.clone());

        // Count how many grains map to each silo
        let mut counts = std::collections::HashMap::new();
        for i in 0u32..1000 {
            let grain_id = make_grain_id(&format!("grain-{}", i));
            let primary = dir.get_primary_silo(&grain_id).unwrap();
            *counts.entry(primary).or_insert(0) += 1;
        }

        // Each silo should get roughly 1/3 (with some variance)
        let expected = 1000 / 3;
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

    /// Integration test: Cache improves lookup performance
    #[tokio::test]
    async fn test_cache_performance() {
        let silo = make_silo(11111);
        let dir = DistributedGrainDirectory::local_only(silo.clone());
        dir.ring().add_silo(silo.clone());

        // Register many grains
        for i in 0..100 {
            let grain_id = make_grain_id(&format!("grain-{}", i));
            let address = make_grain_address(&grain_id, &silo);
            dir.register(MembershipVersion::default(), address, None)
                .await
                .unwrap();
        }

        // Lookup each grain twice
        for i in 0..100 {
            let grain_id = make_grain_id(&format!("grain-{}", i));
            dir.lookup(&grain_id).await.unwrap();
            dir.lookup(&grain_id).await.unwrap();
        }

        // Check cache stats - second lookups should be cache hits
        let stats = dir.cache().stats();
        assert!(stats.hits >= 100, "Expected at least 100 cache hits");
    }

    /// Integration test: Unregistration invalidates cache
    #[tokio::test]
    async fn test_unregister_invalidates_cache() {
        let silo = make_silo(11111);
        let dir = DistributedGrainDirectory::local_only(silo.clone());
        dir.ring().add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Register and lookup (caches the entry)
        dir.register(MembershipVersion::default(), address.clone(), None)
            .await
            .unwrap();
        dir.lookup(&grain_id).await.unwrap();

        // Unregister
        dir.unregister(&grain_id, address.activation_id())
            .await
            .unwrap();

        // Lookup should return None (cache was invalidated)
        let found = dir.lookup(&grain_id).await.unwrap();
        assert!(found.is_none());
    }

    /// Test: Ring segment contains check handles edge cases
    #[test]
    fn test_ring_segment_edge_cases() {
        // Segment at ring boundary
        let segment = RingSegment::new(u32::MAX - 10, 10);
        assert!(segment.contains(u32::MAX));
        assert!(segment.contains(0));
        assert!(segment.contains(10));
        assert!(!segment.contains(11));
        assert!(!segment.contains(u32::MAX - 10)); // Start is exclusive

        // Full ring
        let full = RingSegment::new(100, 100);
        assert!(full.contains(0));
        assert!(full.contains(100));
        assert!(full.contains(u32::MAX));
    }

    /// Property test: Same grain always maps to same silo
    #[test]
    fn test_consistent_mapping() {
        let ring = ConsistentHashRing::new();
        ring.add_silo(make_silo(11111));
        ring.add_silo(make_silo(22222));
        ring.add_silo(make_silo(33333));

        let grain_id = make_grain_id("test-grain");
        let hash = grain_id.get_uniform_hash_code();

        let expected = ring.get_primary_silo(hash).unwrap();

        // Should always return the same silo
        for _ in 0..1000 {
            assert_eq!(ring.get_primary_silo(hash).unwrap(), expected);
        }
    }

    /// Property test: Adding silo changes minimal mappings
    #[test]
    fn test_minimal_disruption() {
        let ring = ConsistentHashRing::new();
        ring.add_silo(make_silo(11111));
        ring.add_silo(make_silo(22222));

        // Record current mappings
        let mut before = std::collections::HashMap::new();
        for i in 0u32..1000 {
            let hash = xxh32(&i.to_le_bytes(), 0);
            before.insert(hash, ring.get_primary_silo(hash).unwrap());
        }

        // Add a third silo
        ring.add_silo(make_silo(33333));

        // Count how many mappings changed
        let mut changed = 0;
        for i in 0u32..1000 {
            let hash = xxh32(&i.to_le_bytes(), 0);
            let after = ring.get_primary_silo(hash).unwrap();
            if after != before[&hash] {
                changed += 1;
            }
        }

        // About 1/3 should change (the new silo's share)
        // Allow some variance
        assert!(
            changed > 200 && changed < 500,
            "Expected ~333 changes, got {}",
            changed
        );
    }
}
