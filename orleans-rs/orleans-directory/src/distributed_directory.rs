//! Distributed grain directory implementation.
//!
//! The distributed grain directory provides a distributed lookup service for
//! grain locations. It uses consistent hashing to partition the directory
//! across silos, with each silo responsible for a portion of the hash space.

use crate::cache::{DirectoryCacheOptions, GrainAddressCacheUpdate, GrainDirectoryCache};
use crate::consistent_hash::{ConsistentHashRing, ConsistentRingOptions};
use crate::error::{DirectoryError, DirectoryResult};
use crate::partition::{GrainDirectoryPartition, RegistrationResult};
use async_trait::async_trait;
use orleans_clustering::{MembershipTableSnapshot, MembershipVersion};
use orleans_core::{ActivationId, GrainAddress, GrainId, SiloAddress};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

/// Options for the distributed grain directory.
#[derive(Debug, Clone)]
pub struct GrainDirectoryOptions {
    /// Options for the consistent hash ring.
    pub ring_options: ConsistentRingOptions,
    /// Options for the directory cache.
    pub cache_options: DirectoryCacheOptions,
}

impl Default for GrainDirectoryOptions {
    fn default() -> Self {
        Self {
            ring_options: ConsistentRingOptions::default(),
            cache_options: DirectoryCacheOptions::default(),
        }
    }
}

/// Trait for remote directory operations.
///
/// This trait abstracts the communication with remote silos for directory
/// operations. It should be implemented by the messaging layer.
#[async_trait]
pub trait IRemoteGrainDirectory: Send + Sync {
    /// Looks up a grain in a remote silo's directory partition.
    async fn remote_lookup(
        &self,
        target_silo: &SiloAddress,
        grain_id: &GrainId,
    ) -> DirectoryResult<Option<GrainAddress>>;

    /// Registers a grain in a remote silo's directory partition.
    async fn remote_register(
        &self,
        target_silo: &SiloAddress,
        membership_version: MembershipVersion,
        address: GrainAddress,
        previous: Option<GrainAddress>,
    ) -> DirectoryResult<RegistrationResult>;

    /// Unregisters a grain from a remote silo's directory partition.
    async fn remote_unregister(
        &self,
        target_silo: &SiloAddress,
        grain_id: &GrainId,
        activation_id: &ActivationId,
    ) -> DirectoryResult<bool>;
}

/// A no-op remote directory for single-silo testing.
pub struct LocalOnlyRemoteDirectory;

#[async_trait]
impl IRemoteGrainDirectory for LocalOnlyRemoteDirectory {
    async fn remote_lookup(
        &self,
        target_silo: &SiloAddress,
        _grain_id: &GrainId,
    ) -> DirectoryResult<Option<GrainAddress>> {
        Err(DirectoryError::SiloNotReachable(target_silo.clone()))
    }

    async fn remote_register(
        &self,
        target_silo: &SiloAddress,
        _membership_version: MembershipVersion,
        _address: GrainAddress,
        _previous: Option<GrainAddress>,
    ) -> DirectoryResult<RegistrationResult> {
        Err(DirectoryError::SiloNotReachable(target_silo.clone()))
    }

    async fn remote_unregister(
        &self,
        target_silo: &SiloAddress,
        _grain_id: &GrainId,
        _activation_id: &ActivationId,
    ) -> DirectoryResult<bool> {
        Err(DirectoryError::SiloNotReachable(target_silo.clone()))
    }
}

/// The distributed grain directory.
///
/// This provides the main interface for grain location lookups and registrations.
/// It combines local partition storage, consistent hashing for routing, and
/// caching for performance.
pub struct DistributedGrainDirectory {
    /// Address of the local silo.
    local_silo: SiloAddress,

    /// Local directory partition.
    local_partition: GrainDirectoryPartition,

    /// Consistent hash ring for routing.
    ring: ConsistentHashRing,

    /// Directory cache.
    cache: GrainDirectoryCache,

    /// Remote directory operations.
    remote_directory: Arc<dyn IRemoteGrainDirectory>,

    /// Recovery membership version counter.
    /// Incremented when a non-contiguous membership change occurs.
    recovery_version: AtomicI64,
}

impl DistributedGrainDirectory {
    /// Creates a new distributed grain directory.
    pub fn new(
        local_silo: SiloAddress,
        remote_directory: Arc<dyn IRemoteGrainDirectory>,
        options: GrainDirectoryOptions,
    ) -> Self {
        Self {
            local_partition: GrainDirectoryPartition::new(local_silo.clone()),
            ring: ConsistentHashRing::with_options(options.ring_options),
            cache: GrainDirectoryCache::with_options(options.cache_options),
            remote_directory,
            local_silo,
            recovery_version: AtomicI64::new(0),
        }
    }

    /// Creates a directory for local-only testing.
    pub fn local_only(local_silo: SiloAddress) -> Self {
        Self::new(
            local_silo.clone(),
            Arc::new(LocalOnlyRemoteDirectory),
            GrainDirectoryOptions::default(),
        )
    }

    /// Returns the local silo address.
    pub fn local_silo(&self) -> &SiloAddress {
        &self.local_silo
    }

    /// Returns a reference to the consistent hash ring.
    pub fn ring(&self) -> &ConsistentHashRing {
        &self.ring
    }

    /// Returns a reference to the directory cache.
    pub fn cache(&self) -> &GrainDirectoryCache {
        &self.cache
    }

    /// Returns a reference to the local partition.
    pub fn local_partition(&self) -> &GrainDirectoryPartition {
        &self.local_partition
    }

    /// Looks up a grain's location.
    ///
    /// First checks the local cache, then queries the appropriate partition
    /// (local or remote) based on consistent hashing.
    pub async fn lookup(&self, grain_id: &GrainId) -> DirectoryResult<Option<GrainAddress>> {
        // Check cache first
        if let Some(address) = self.cache.lookup(grain_id) {
            trace!(grain_id = %grain_id, "Cache hit");
            return Ok(Some(address));
        }

        // Determine which silo owns this grain's directory entry
        let target_silo = self.get_primary_silo(grain_id)?;

        let result = if target_silo == self.local_silo {
            // Local lookup
            Ok(self.local_partition.lookup(grain_id))
        } else {
            // Remote lookup
            self.remote_directory
                .remote_lookup(&target_silo, grain_id)
                .await
        };

        // Update cache on successful lookup
        if let Ok(Some(ref address)) = result {
            self.cache
                .insert(grain_id.clone(), address.clone());
        }

        result
    }

    /// Registers a grain activation.
    ///
    /// Routes the registration to the appropriate partition based on
    /// consistent hashing. Returns the registered address on success,
    /// or the existing address if there's a conflict.
    pub async fn register(
        &self,
        membership_version: MembershipVersion,
        address: GrainAddress,
        previous: Option<GrainAddress>,
    ) -> DirectoryResult<GrainAddress> {
        let grain_id = address.grain_id();
        let initial_recovery_version = self.recovery_version.load(Ordering::Acquire);

        // Determine which silo owns this grain's directory entry
        let target_silo = self.get_primary_silo(grain_id)?;

        let result = if target_silo == self.local_silo {
            // Local registration
            self.local_partition
                .register(membership_version, address.clone(), previous)
        } else {
            // Remote registration
            self.remote_directory
                .remote_register(&target_silo, membership_version, address.clone(), previous)
                .await?
        };

        // Check if recovery happened during operation
        if initial_recovery_version != self.recovery_version.load(Ordering::Acquire) {
            warn!(
                grain_id = %grain_id,
                "Membership changed during registration, retrying"
            );
            // Retry with fresh membership (recursive call)
            return Box::pin(self.register(membership_version, address, None)).await;
        }

        match result {
            RegistrationResult::Success(addr) => {
                // Update cache
                self.cache.insert(grain_id.clone(), addr.clone());
                Ok(addr)
            }
            RegistrationResult::Conflict(existing) => {
                // Update cache with existing address
                self.cache.insert(grain_id.clone(), existing.clone());
                Err(DirectoryError::RegistrationConflict {
                    grain_id: grain_id.clone(),
                    existing,
                    requested: address,
                })
            }
        }
    }

    /// Unregisters a grain activation.
    ///
    /// Routes the unregistration to the appropriate partition.
    /// Only removes the entry if the activation ID matches.
    pub async fn unregister(
        &self,
        grain_id: &GrainId,
        activation_id: &ActivationId,
    ) -> DirectoryResult<bool> {
        // Invalidate cache
        self.cache.invalidate(grain_id, activation_id);

        // Determine which silo owns this grain's directory entry
        let target_silo = self.get_primary_silo(grain_id)?;

        if target_silo == self.local_silo {
            // Local unregistration
            Ok(self.local_partition.unregister(grain_id, activation_id))
        } else {
            // Remote unregistration
            self.remote_directory
                .remote_unregister(&target_silo, grain_id, activation_id)
                .await
        }
    }

    /// Gets the primary silo responsible for a grain's directory entry.
    pub fn get_primary_silo(&self, grain_id: &GrainId) -> DirectoryResult<SiloAddress> {
        let hash = grain_id.get_uniform_hash_code();
        self.ring.get_primary_silo(hash)
    }

    /// Applies a cache update from a message.
    pub fn apply_cache_update(&self, update: GrainAddressCacheUpdate) {
        self.cache.apply_update(update);
    }

    /// Updates the directory based on a membership change.
    ///
    /// This should be called when the cluster membership changes to update
    /// the consistent hash ring and handle any necessary handoffs.
    pub async fn on_membership_change(&self, snapshot: &MembershipTableSnapshot) {
        let current_silos: std::collections::HashSet<_> = self.ring.get_silos().into_iter().collect();

        // Add new silos
        for silo in snapshot.get_active_silos() {
            if !current_silos.contains(silo) {
                info!(silo = %silo, "Adding silo to directory ring");
                self.ring.add_silo(silo.clone());
            }
        }

        // Remove dead silos
        for silo in &current_silos {
            if !snapshot.is_silo_active(silo) {
                info!(silo = %silo, "Removing silo from directory ring");
                self.ring.remove_silo(silo);

                // Invalidate cache entries for this silo
                self.cache.invalidate_silo(silo);

                // Remove partition entries for this silo
                self.local_partition.remove_entries_for_silo(silo);
            }
        }

        // Update local partition's range
        let my_range = self.ring.get_silo_range(&self.local_silo);
        self.local_partition.set_range(my_range);

        debug!(
            silo_count = self.ring.silo_count(),
            grain_count = self.local_partition.grain_count(),
            "Directory updated after membership change"
        );
    }

    /// Handles a non-contiguous view change (e.g., network partition recovery).
    ///
    /// This increments the recovery version to signal in-flight operations
    /// that they should retry.
    pub fn on_non_contiguous_change(&self) {
        self.recovery_version.fetch_add(1, Ordering::Release);
        warn!("Non-contiguous membership change detected");
    }

    /// Returns directory statistics.
    pub fn stats(&self) -> DirectoryStats {
        DirectoryStats {
            silo_count: self.ring.silo_count(),
            local_grain_count: self.local_partition.grain_count(),
            partition_stats: self.local_partition.stats(),
            cache_stats: self.cache.stats(),
        }
    }
}

/// Statistics for the distributed directory.
#[derive(Debug, Clone)]
pub struct DirectoryStats {
    /// Number of silos in the ring.
    pub silo_count: usize,
    /// Number of grains in the local partition.
    pub local_grain_count: usize,
    /// Local partition statistics.
    pub partition_stats: crate::partition::PartitionStats,
    /// Cache statistics.
    pub cache_stats: crate::cache::CacheStats,
}

impl std::fmt::Debug for DistributedGrainDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistributedGrainDirectory")
            .field("local_silo", &self.local_silo)
            .field("silo_count", &self.ring.silo_count())
            .field("local_grain_count", &self.local_partition.grain_count())
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

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(
            orleans_core::GrainType::create(name),
            orleans_core::IdSpan::from_str(name),
        )
    }

    fn make_grain_address(grain_id: &GrainId, silo: &SiloAddress) -> GrainAddress {
        GrainAddress::complete(grain_id.clone(), ActivationId::new(), silo.clone())
    }

    #[tokio::test]
    async fn test_single_silo_register_and_lookup() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo.clone());

        // Add self to ring
        directory.ring.add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Register
        let result = directory
            .register(MembershipVersion::default(), address.clone(), None)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), address);

        // Lookup
        let found = directory.lookup(&grain_id).await;
        assert!(found.is_ok());
        assert_eq!(found.unwrap(), Some(address));
    }

    #[tokio::test]
    async fn test_registration_conflict() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo.clone());
        directory.ring.add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address1 = make_grain_address(&grain_id, &silo);
        let address2 = make_grain_address(&grain_id, &silo);

        // Register first activation
        directory
            .register(MembershipVersion::default(), address1.clone(), None)
            .await
            .unwrap();

        // Try to register second activation
        let result = directory
            .register(MembershipVersion::default(), address2, None)
            .await;

        assert!(matches!(result, Err(DirectoryError::RegistrationConflict { .. })));
    }

    #[tokio::test]
    async fn test_unregister() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo.clone());
        directory.ring.add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Register
        directory
            .register(MembershipVersion::default(), address.clone(), None)
            .await
            .unwrap();

        // Unregister
        let removed = directory
            .unregister(&grain_id, address.activation_id())
            .await
            .unwrap();
        assert!(removed);

        // Lookup should fail
        let found = directory.lookup(&grain_id).await.unwrap();
        assert!(found.is_none());
    }

    #[tokio::test]
    async fn test_cache_hit() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo.clone());
        directory.ring.add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Register (also caches)
        directory
            .register(MembershipVersion::default(), address.clone(), None)
            .await
            .unwrap();

        // First lookup (should hit cache)
        directory.lookup(&grain_id).await.unwrap();

        let stats = directory.cache.stats();
        assert!(stats.hits > 0);
    }

    #[tokio::test]
    async fn test_get_primary_silo() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        let directory = DistributedGrainDirectory::local_only(silo1.clone());
        directory.ring.add_silo(silo1.clone());
        directory.ring.add_silo(silo2);
        directory.ring.add_silo(silo3);

        // Different grains should map to (potentially) different silos
        let grain1 = make_grain_id("grain1");
        let grain2 = make_grain_id("grain2");

        let primary1 = directory.get_primary_silo(&grain1).unwrap();
        let primary2 = directory.get_primary_silo(&grain2).unwrap();

        // Both should be valid silos
        assert!(directory.ring.contains_silo(&primary1));
        assert!(directory.ring.contains_silo(&primary2));
    }

    #[tokio::test]
    async fn test_empty_ring_error() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo);

        // Don't add any silos to ring

        let grain_id = make_grain_id("test-grain");
        let result = directory.get_primary_silo(&grain_id);

        assert!(matches!(result, Err(DirectoryError::EmptyRing)));
    }

    #[tokio::test]
    async fn test_stats() {
        let silo = make_silo(11111);
        let directory = DistributedGrainDirectory::local_only(silo.clone());
        directory.ring.add_silo(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        directory
            .register(MembershipVersion::default(), address, None)
            .await
            .unwrap();

        directory.lookup(&grain_id).await.unwrap();

        let stats = directory.stats();
        assert_eq!(stats.silo_count, 1);
        assert_eq!(stats.local_grain_count, 1);
        assert!(stats.partition_stats.lookup_count > 0 || stats.cache_stats.hits > 0);
    }
}
