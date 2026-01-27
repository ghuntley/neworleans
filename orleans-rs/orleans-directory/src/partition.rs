//! Grain directory partition - local storage for grain locations.
//!
//! Each silo maintains a partition of the grain directory, responsible
//! for grains whose hash falls within its range of the consistent hash ring.

use crate::ring_range::RingRange;
use dashmap::DashMap;
use orleans_clustering::MembershipVersion;
use orleans_core::{GrainAddress, GrainId, SiloAddress};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, trace, warn};

/// Result of a directory registration attempt.
#[derive(Debug, Clone)]
pub enum RegistrationResult {
    /// Registration succeeded, this is the registered address.
    Success(GrainAddress),
    /// Another activation already exists for this grain.
    Conflict(GrainAddress),
}

impl RegistrationResult {
    /// Returns the address associated with this result.
    pub fn address(&self) -> &GrainAddress {
        match self {
            RegistrationResult::Success(addr) | RegistrationResult::Conflict(addr) => addr,
        }
    }

    /// Returns true if registration was successful.
    pub fn is_success(&self) -> bool {
        matches!(self, RegistrationResult::Success(_))
    }
}

/// Statistics for a directory partition.
#[derive(Debug, Clone, Default)]
pub struct PartitionStats {
    /// Number of registered grains.
    pub grain_count: usize,
    /// Number of lookups performed.
    pub lookup_count: u64,
    /// Number of registrations performed.
    pub registration_count: u64,
    /// Number of unregistrations performed.
    pub unregistration_count: u64,
}

/// A local partition of the grain directory.
///
/// Stores grain location mappings for grains in the hash range owned by this silo.
/// Operations are thread-safe using lock-free concurrent data structures.
pub struct GrainDirectoryPartition {
    /// The silo that owns this partition.
    local_silo: SiloAddress,

    /// The grain directory: GrainId -> GrainAddress.
    directory: DashMap<GrainId, GrainAddress>,

    /// Current hash range owned by this partition.
    current_range: RwLock<RingRange>,

    /// Statistics counters.
    lookup_count: AtomicU64,
    registration_count: AtomicU64,
    unregistration_count: AtomicU64,
}

impl GrainDirectoryPartition {
    /// Creates a new grain directory partition.
    pub fn new(local_silo: SiloAddress) -> Self {
        Self {
            local_silo,
            directory: DashMap::new(),
            current_range: RwLock::new(RingRange::empty()),
            lookup_count: AtomicU64::new(0),
            registration_count: AtomicU64::new(0),
            unregistration_count: AtomicU64::new(0),
        }
    }

    /// Returns the silo that owns this partition.
    pub fn local_silo(&self) -> &SiloAddress {
        &self.local_silo
    }

    /// Returns the number of registered grains.
    pub fn grain_count(&self) -> usize {
        self.directory.len()
    }

    /// Returns partition statistics.
    pub fn stats(&self) -> PartitionStats {
        PartitionStats {
            grain_count: self.directory.len(),
            lookup_count: self.lookup_count.load(Ordering::Relaxed),
            registration_count: self.registration_count.load(Ordering::Relaxed),
            unregistration_count: self.unregistration_count.load(Ordering::Relaxed),
        }
    }

    /// Looks up a grain in the local partition.
    ///
    /// Returns the grain's address if found.
    pub fn lookup(&self, grain_id: &GrainId) -> Option<GrainAddress> {
        self.lookup_count.fetch_add(1, Ordering::Relaxed);

        let result = self.directory.get(grain_id).map(|r| r.value().clone());

        trace!(
            grain_id = %grain_id,
            found = result.is_some(),
            "Directory lookup"
        );

        result
    }

    /// Registers a grain activation in the local partition.
    ///
    /// If the grain is already registered with a different activation,
    /// returns the existing address as a conflict.
    ///
    /// # Arguments
    /// * `membership_version` - Current membership version for consistency checking
    /// * `address` - The grain address to register
    /// * `previous` - Optional previous address (for re-registration after move)
    pub fn register(
        &self,
        _membership_version: MembershipVersion,
        address: GrainAddress,
        previous: Option<GrainAddress>,
    ) -> RegistrationResult {
        self.registration_count.fetch_add(1, Ordering::Relaxed);

        let grain_id = address.grain_id().clone();

        // Try to insert or check for conflict
        match self.directory.entry(grain_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(address.clone());
                debug!(
                    grain_id = %grain_id,
                    silo = ?address.silo_address(),
                    "Registered grain"
                );
                RegistrationResult::Success(address)
            }
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let existing = entry.get().clone();

                // Check if this is a re-registration from the same activation
                if existing.activation_id() == address.activation_id() {
                    // Same activation, update the entry
                    entry.insert(address.clone());
                    return RegistrationResult::Success(address);
                }

                // Check if we're replacing a known previous address
                if let Some(prev) = &previous {
                    if existing.activation_id() == prev.activation_id() {
                        // Expected previous, replace with new
                        entry.insert(address.clone());
                        debug!(
                            grain_id = %grain_id,
                            old_silo = ?existing.silo_address(),
                            new_silo = ?address.silo_address(),
                            "Re-registered grain (replaced previous)"
                        );
                        return RegistrationResult::Success(address);
                    }
                }

                // Conflict: another activation exists
                warn!(
                    grain_id = %grain_id,
                    existing_silo = ?existing.silo_address(),
                    requested_silo = ?address.silo_address(),
                    "Registration conflict"
                );
                RegistrationResult::Conflict(existing)
            }
        }
    }

    /// Unregisters a grain from the local partition.
    ///
    /// Only removes the entry if the activation ID matches.
    ///
    /// # Arguments
    /// * `grain_id` - The grain to unregister
    /// * `expected_activation` - The activation ID that should be registered
    ///
    /// Returns true if the grain was unregistered.
    pub fn unregister(
        &self,
        grain_id: &GrainId,
        expected_activation: &orleans_core::ActivationId,
    ) -> bool {
        self.unregistration_count.fetch_add(1, Ordering::Relaxed);

        // Only remove if the activation ID matches
        let removed = self.directory.remove_if(grain_id, |_, addr| {
            addr.activation_id() == expected_activation
        });

        if removed.is_some() {
            debug!(grain_id = %grain_id, "Unregistered grain");
        }

        removed.is_some()
    }

    /// Force-unregisters a grain regardless of activation ID.
    ///
    /// Used during failure recovery when the activation is known to be dead.
    pub fn force_unregister(&self, grain_id: &GrainId) -> Option<GrainAddress> {
        self.unregistration_count.fetch_add(1, Ordering::Relaxed);

        let removed = self.directory.remove(grain_id);

        if let Some((_, addr)) = &removed {
            debug!(
                grain_id = %grain_id,
                silo = ?addr.silo_address(),
                "Force-unregistered grain"
            );
        }

        removed.map(|(_, v)| v)
    }

    /// Removes all entries pointing to a specific silo.
    ///
    /// Used when a silo is declared dead.
    pub fn remove_entries_for_silo(&self, dead_silo: &SiloAddress) -> Vec<(GrainId, GrainAddress)> {
        let to_remove: Vec<_> = self
            .directory
            .iter()
            .filter(|r| r.value().silo_address() == Some(dead_silo))
            .map(|r| r.key().clone())
            .collect();

        let mut removed = Vec::with_capacity(to_remove.len());

        for grain_id in to_remove {
            if let Some((id, addr)) = self.directory.remove(&grain_id) {
                if addr.silo_address() == Some(dead_silo) {
                    removed.push((id, addr));
                }
            }
        }

        if !removed.is_empty() {
            debug!(
                dead_silo = %dead_silo,
                count = removed.len(),
                "Removed entries for dead silo"
            );
        }

        removed
    }

    /// Gets all entries in a specific hash range.
    ///
    /// Used during directory handoff when ranges are transferred.
    pub fn get_entries_in_range(&self, range: &RingRange) -> Vec<(GrainId, GrainAddress)> {
        self.directory
            .iter()
            .filter(|r| range.contains(r.key().get_uniform_hash_code()))
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Removes entries in a specific hash range.
    ///
    /// Used when releasing ownership of a range.
    pub fn release_range(&self, range: &RingRange) -> Vec<(GrainId, GrainAddress)> {
        let to_remove: Vec<_> = self
            .directory
            .iter()
            .filter(|r| range.contains(r.key().get_uniform_hash_code()))
            .map(|r| r.key().clone())
            .collect();

        let mut removed = Vec::with_capacity(to_remove.len());

        for grain_id in to_remove {
            if let Some((id, addr)) = self.directory.remove(&grain_id) {
                removed.push((id, addr));
            }
        }

        debug!(
            count = removed.len(),
            "Released entries in range"
        );

        removed
    }

    /// Merges entries from another partition (during handoff).
    ///
    /// Used when acquiring ownership of a range from another silo.
    pub fn merge_entries(&self, entries: Vec<(GrainId, GrainAddress)>) {
        for (grain_id, address) in entries {
            // Only insert if not already present
            self.directory.entry(grain_id).or_insert(address);
        }
    }

    /// Updates the current range owned by this partition.
    pub fn set_range(&self, range: RingRange) {
        *self.current_range.write() = range;
    }

    /// Gets the current range owned by this partition.
    pub fn get_range(&self) -> RingRange {
        self.current_range.read().clone()
    }

    /// Returns an iterator over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (GrainId, GrainAddress)> + '_ {
        self.directory
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
    }

    /// Clears all entries from the partition.
    pub fn clear(&self) {
        self.directory.clear();
    }
}

impl std::fmt::Debug for GrainDirectoryPartition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrainDirectoryPartition")
            .field("local_silo", &self.local_silo)
            .field("grain_count", &self.directory.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::ActivationId;
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

    #[test]
    fn test_register_and_lookup() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Register
        let result = partition.register(MembershipVersion::default(), address.clone(), None);
        assert!(result.is_success());

        // Lookup
        let found = partition.lookup(&grain_id);
        assert_eq!(found, Some(address));
    }

    #[test]
    fn test_registration_conflict() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let partition = GrainDirectoryPartition::new(silo1.clone());

        let grain_id = make_grain_id("test-grain");
        let address1 = make_grain_address(&grain_id, &silo1);
        let address2 = make_grain_address(&grain_id, &silo2);

        // Register first
        let result1 = partition.register(MembershipVersion::default(), address1.clone(), None);
        assert!(result1.is_success());

        // Try to register another activation
        let result2 = partition.register(MembershipVersion::default(), address2, None);
        assert!(!result2.is_success());
        assert_eq!(result2.address(), &address1);
    }

    #[test]
    fn test_re_registration_with_previous() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let partition = GrainDirectoryPartition::new(silo1.clone());

        let grain_id = make_grain_id("test-grain");
        let address1 = make_grain_address(&grain_id, &silo1);
        let address2 = make_grain_address(&grain_id, &silo2);

        // Register first
        partition.register(MembershipVersion::default(), address1.clone(), None);

        // Re-register with correct previous
        let result = partition.register(
            MembershipVersion::default(),
            address2.clone(),
            Some(address1),
        );
        assert!(result.is_success());

        // Lookup should return new address
        let found = partition.lookup(&grain_id);
        assert_eq!(found, Some(address2));
    }

    #[test]
    fn test_unregister() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        partition.register(MembershipVersion::default(), address.clone(), None);

        // Unregister with correct activation ID
        let removed = partition.unregister(&grain_id, address.activation_id());
        assert!(removed);

        // Lookup should fail
        assert!(partition.lookup(&grain_id).is_none());
    }

    #[test]
    fn test_unregister_wrong_activation() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        partition.register(MembershipVersion::default(), address.clone(), None);

        // Try to unregister with wrong activation ID
        let wrong_activation = ActivationId::new();
        let removed = partition.unregister(&grain_id, &wrong_activation);
        assert!(!removed);

        // Grain should still be registered
        assert!(partition.lookup(&grain_id).is_some());
    }

    #[test]
    fn test_remove_entries_for_silo() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let partition = GrainDirectoryPartition::new(silo1.clone());

        let grain1 = make_grain_id("grain1");
        let grain2 = make_grain_id("grain2");
        let grain3 = make_grain_id("grain3");

        let address1 = make_grain_address(&grain1, &silo1);
        let address2 = make_grain_address(&grain2, &silo2);
        let address3 = make_grain_address(&grain3, &silo2);

        partition.register(MembershipVersion::default(), address1, None);
        partition.register(MembershipVersion::default(), address2, None);
        partition.register(MembershipVersion::default(), address3, None);

        assert_eq!(partition.grain_count(), 3);

        // Remove entries for silo2
        let removed = partition.remove_entries_for_silo(&silo2);
        assert_eq!(removed.len(), 2);

        // Only grain1 should remain
        assert_eq!(partition.grain_count(), 1);
        assert!(partition.lookup(&grain1).is_some());
        assert!(partition.lookup(&grain2).is_none());
        assert!(partition.lookup(&grain3).is_none());
    }

    #[test]
    fn test_get_entries_in_range() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        // Register several grains
        for i in 0..10 {
            let grain_id = make_grain_id(&format!("grain{}", i));
            let address = make_grain_address(&grain_id, &silo);
            partition.register(MembershipVersion::default(), address, None);
        }

        // Get entries in a range
        let range = crate::ring_range::RingRange::single(0, 0x80000000);
        let entries = partition.get_entries_in_range(&range);

        // Should get some entries (not all, since hashes are distributed)
        assert!(!entries.is_empty());
        assert!(entries.len() <= 10);

        // All returned entries should be in the range
        for (grain_id, _) in &entries {
            assert!(range.contains(grain_id.get_uniform_hash_code()));
        }
    }

    #[test]
    fn test_release_range() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        // Register several grains
        for i in 0..10 {
            let grain_id = make_grain_id(&format!("grain{}", i));
            let address = make_grain_address(&grain_id, &silo);
            partition.register(MembershipVersion::default(), address, None);
        }

        let initial_count = partition.grain_count();

        // Release entries in a range
        let range = crate::ring_range::RingRange::single(0, 0x80000000);
        let released = partition.release_range(&range);

        // Some entries should be released
        assert!(!released.is_empty());
        assert!(partition.grain_count() < initial_count);
        assert_eq!(partition.grain_count(), initial_count - released.len());
    }

    #[test]
    fn test_merge_entries() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain1 = make_grain_id("grain1");
        let grain2 = make_grain_id("grain2");
        let address1 = make_grain_address(&grain1, &silo);
        let address2 = make_grain_address(&grain2, &silo);

        // Merge entries
        partition.merge_entries(vec![
            (grain1.clone(), address1.clone()),
            (grain2.clone(), address2.clone()),
        ]);

        assert_eq!(partition.grain_count(), 2);
        assert_eq!(partition.lookup(&grain1), Some(address1));
        assert_eq!(partition.lookup(&grain2), Some(address2));
    }

    #[test]
    fn test_stats() {
        let silo = make_silo(11111);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        partition.register(MembershipVersion::default(), address.clone(), None);
        partition.lookup(&grain_id);
        partition.lookup(&grain_id);
        partition.unregister(&grain_id, address.activation_id());

        let stats = partition.stats();
        assert_eq!(stats.grain_count, 0);
        assert_eq!(stats.lookup_count, 2);
        assert_eq!(stats.registration_count, 1);
        assert_eq!(stats.unregistration_count, 1);
    }
}
