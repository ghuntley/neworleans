//! Grain version manifest for tracking versions across the cluster.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock;
use orleans_core::{GrainType, SiloAddress};
use tracing::{debug, trace, instrument};

/// Grain version manifest that tracks available versions across the cluster.
///
/// The manifest maintains a mapping of interface types to their available
/// versions and the silos that support each version.
///
/// # Thread Safety
///
/// All operations are thread-safe. The manifest uses internal locking with
/// `parking_lot::RwLock` for efficiency.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::GrainVersionManifest;
/// use orleans_core::{GrainType, SiloAddress};
/// use std::net::SocketAddr;
///
/// let manifest = GrainVersionManifest::new();
/// let interface = GrainType::create("IMyGrain");
/// let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
/// let silo = SiloAddress::new(addr, 1);
///
/// // Register a silo supporting version 1
/// manifest.register_version(&interface, 1, silo.clone());
///
/// // Check available versions
/// let versions = manifest.get_available_versions(&interface);
/// assert!(versions.contains(&1));
///
/// // Get silos supporting version 1
/// let silos = manifest.get_supported_silos(&interface, 1);
/// assert!(silos.contains(&silo));
/// ```
#[derive(Debug)]
pub struct GrainVersionManifest {
    /// Mapping of (interface_type, version) -> silos that support it.
    versions: RwLock<HashMap<GrainType, HashMap<u16, HashSet<SiloAddress>>>>,

    /// Manifest version for cache invalidation.
    version: AtomicU64,
}

impl Default for GrainVersionManifest {
    fn default() -> Self {
        Self::new()
    }
}

impl GrainVersionManifest {
    /// Creates a new empty manifest.
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(HashMap::new()),
            version: AtomicU64::new(0),
        }
    }

    /// Gets the current manifest version.
    ///
    /// This version is incremented whenever the manifest changes.
    /// Used for cache invalidation.
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// Registers a silo as supporting a specific version of an interface.
    #[instrument(skip(self), level = "debug")]
    pub fn register_version(
        &self,
        interface_type: &GrainType,
        version: u16,
        silo: SiloAddress,
    ) {
        let mut versions = self.versions.write();
        let interface_versions = versions
            .entry(interface_type.clone())
            .or_insert_with(HashMap::new);
        let silos = interface_versions
            .entry(version)
            .or_insert_with(HashSet::new);

        if silos.insert(silo.clone()) {
            debug!(
                interface = %interface_type,
                version = version,
                silo = %silo,
                "Registered new version support"
            );
            self.increment_version();
        }
    }

    /// Unregisters a silo's support for a specific version.
    #[instrument(skip(self), level = "debug")]
    pub fn unregister_version(
        &self,
        interface_type: &GrainType,
        version: u16,
        silo: &SiloAddress,
    ) {
        let mut versions = self.versions.write();
        if let Some(interface_versions) = versions.get_mut(interface_type) {
            if let Some(silos) = interface_versions.get_mut(&version) {
                if silos.remove(silo) {
                    debug!(
                        interface = %interface_type,
                        version = version,
                        silo = %silo,
                        "Unregistered version support"
                    );
                    self.increment_version();

                    // Clean up empty entries
                    if silos.is_empty() {
                        interface_versions.remove(&version);
                    }
                }
            }
            if interface_versions.is_empty() {
                versions.remove(interface_type);
            }
        }
    }

    /// Removes all version registrations for a silo.
    ///
    /// Call this when a silo leaves the cluster.
    #[instrument(skip(self), level = "debug")]
    pub fn unregister_silo(&self, silo: &SiloAddress) {
        let mut versions = self.versions.write();
        let mut changed = false;

        for interface_versions in versions.values_mut() {
            for silos in interface_versions.values_mut() {
                if silos.remove(silo) {
                    changed = true;
                }
            }
            // Clean up empty version entries
            interface_versions.retain(|_, silos| !silos.is_empty());
        }

        // Clean up empty interface entries
        versions.retain(|_, v| !v.is_empty());

        if changed {
            debug!(silo = %silo, "Removed all version registrations for silo");
            self.increment_version();
        }
    }

    /// Gets all versions available in the cluster for an interface.
    pub fn get_available_versions(&self, interface_type: &GrainType) -> Vec<u16> {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .map(|v| v.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Gets the local silo's version for an interface.
    ///
    /// Returns the highest version that the specified silo supports.
    pub fn get_local_version(
        &self,
        interface_type: &GrainType,
        local_silo: &SiloAddress,
    ) -> Option<u16> {
        let versions = self.versions.read();
        versions.get(interface_type).and_then(|interface_versions| {
            interface_versions
                .iter()
                .filter(|(_, silos)| silos.contains(local_silo))
                .map(|(&v, _)| v)
                .max()
        })
    }

    /// Gets all silos that support a specific version.
    pub fn get_supported_silos(
        &self,
        interface_type: &GrainType,
        version: u16,
    ) -> Vec<SiloAddress> {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .and_then(|v| v.get(&version))
            .map(|silos| silos.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Gets all silos that support the grain type with any version.
    pub fn get_all_silos_for_interface(&self, interface_type: &GrainType) -> Vec<SiloAddress> {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .map(|interface_versions| {
                interface_versions
                    .values()
                    .flat_map(|silos| silos.iter().cloned())
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Gets silos grouped by version for a grain interface.
    pub fn get_silos_by_version(
        &self,
        interface_type: &GrainType,
    ) -> HashMap<u16, Vec<SiloAddress>> {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .map(|interface_versions| {
                interface_versions
                    .iter()
                    .map(|(&v, silos)| (v, silos.iter().cloned().collect()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Checks if any silo supports the specified version.
    pub fn is_version_available(&self, interface_type: &GrainType, version: u16) -> bool {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .and_then(|v| v.get(&version))
            .map(|silos| !silos.is_empty())
            .unwrap_or(false)
    }

    /// Gets the number of silos supporting each version.
    pub fn get_version_silo_counts(&self, interface_type: &GrainType) -> HashMap<u16, usize> {
        let versions = self.versions.read();
        versions
            .get(interface_type)
            .map(|interface_versions| {
                interface_versions
                    .iter()
                    .map(|(&v, silos)| (v, silos.len()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Clears all registrations. Used for testing.
    pub fn clear(&self) {
        let mut versions = self.versions.write();
        versions.clear();
        self.increment_version();
    }

    /// Gets total number of registered interfaces.
    pub fn interface_count(&self) -> usize {
        self.versions.read().len()
    }

    /// Gets total number of version registrations.
    pub fn total_registrations(&self) -> usize {
        self.versions.read()
            .values()
            .flat_map(|v| v.values())
            .map(|silos| silos.len())
            .sum()
    }

    fn increment_version(&self) {
        self.version.fetch_add(1, Ordering::SeqCst);
        trace!(
            new_version = self.version.load(Ordering::SeqCst),
            "Manifest version incremented"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn create_silo(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, port as i64)
    }

    fn create_interface(name: &str) -> GrainType {
        GrainType::create(name)
    }

    #[test]
    fn test_new_manifest_is_empty() {
        let manifest = GrainVersionManifest::new();
        assert_eq!(manifest.interface_count(), 0);
        assert_eq!(manifest.total_registrations(), 0);
        assert_eq!(manifest.version(), 0);
    }

    #[test]
    fn test_register_version() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo1 = create_silo(11111);
        let silo2 = create_silo(11112);

        manifest.register_version(&interface, 1, silo1.clone());
        manifest.register_version(&interface, 1, silo2.clone());
        manifest.register_version(&interface, 2, silo1.clone());

        let v1_silos = manifest.get_supported_silos(&interface, 1);
        assert_eq!(v1_silos.len(), 2);
        assert!(v1_silos.contains(&silo1));
        assert!(v1_silos.contains(&silo2));

        let v2_silos = manifest.get_supported_silos(&interface, 2);
        assert_eq!(v2_silos.len(), 1);
        assert!(v2_silos.contains(&silo1));
    }

    #[test]
    fn test_get_available_versions() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        manifest.register_version(&interface, 1, silo.clone());
        manifest.register_version(&interface, 2, silo.clone());
        manifest.register_version(&interface, 3, silo.clone());

        let versions = manifest.get_available_versions(&interface);
        assert_eq!(versions.len(), 3);
        assert!(versions.contains(&1));
        assert!(versions.contains(&2));
        assert!(versions.contains(&3));
    }

    #[test]
    fn test_get_local_version() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        manifest.register_version(&interface, 1, silo.clone());
        manifest.register_version(&interface, 2, silo.clone());
        manifest.register_version(&interface, 3, silo.clone());

        let local_version = manifest.get_local_version(&interface, &silo);
        assert_eq!(local_version, Some(3)); // Returns highest

        let other_silo = create_silo(22222);
        let local_version = manifest.get_local_version(&interface, &other_silo);
        assert_eq!(local_version, None);
    }

    #[test]
    fn test_unregister_version() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        manifest.register_version(&interface, 1, silo.clone());
        manifest.register_version(&interface, 2, silo.clone());

        manifest.unregister_version(&interface, 1, &silo);

        let versions = manifest.get_available_versions(&interface);
        assert_eq!(versions.len(), 1);
        assert!(!versions.contains(&1));
        assert!(versions.contains(&2));
    }

    #[test]
    fn test_unregister_silo() {
        let manifest = GrainVersionManifest::new();
        let interface1 = create_interface("IMyGrain1");
        let interface2 = create_interface("IMyGrain2");
        let silo1 = create_silo(11111);
        let silo2 = create_silo(11112);

        manifest.register_version(&interface1, 1, silo1.clone());
        manifest.register_version(&interface1, 1, silo2.clone());
        manifest.register_version(&interface2, 1, silo1.clone());

        manifest.unregister_silo(&silo1);

        // silo2 should still be registered
        let v1_silos = manifest.get_supported_silos(&interface1, 1);
        assert_eq!(v1_silos.len(), 1);
        assert!(v1_silos.contains(&silo2));

        // interface2 should have no silos now
        let i2_silos = manifest.get_supported_silos(&interface2, 1);
        assert!(i2_silos.is_empty());
    }

    #[test]
    fn test_version_increment() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        let v0 = manifest.version();
        assert_eq!(v0, 0);

        manifest.register_version(&interface, 1, silo.clone());
        let v1 = manifest.version();
        assert_eq!(v1, 1);

        // Registering the same version again should not increment
        manifest.register_version(&interface, 1, silo.clone());
        let v2 = manifest.version();
        assert_eq!(v2, 1);

        // Registering a different version should increment
        manifest.register_version(&interface, 2, silo.clone());
        let v3 = manifest.version();
        assert_eq!(v3, 2);
    }

    #[test]
    fn test_is_version_available() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        assert!(!manifest.is_version_available(&interface, 1));

        manifest.register_version(&interface, 1, silo.clone());
        assert!(manifest.is_version_available(&interface, 1));
        assert!(!manifest.is_version_available(&interface, 2));
    }

    #[test]
    fn test_get_silos_by_version() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo1 = create_silo(11111);
        let silo2 = create_silo(11112);

        manifest.register_version(&interface, 1, silo1.clone());
        manifest.register_version(&interface, 1, silo2.clone());
        manifest.register_version(&interface, 2, silo2.clone());

        let by_version = manifest.get_silos_by_version(&interface);
        assert_eq!(by_version.len(), 2);
        assert_eq!(by_version.get(&1).unwrap().len(), 2);
        assert_eq!(by_version.get(&2).unwrap().len(), 1);
    }

    #[test]
    fn test_get_all_silos_for_interface() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo1 = create_silo(11111);
        let silo2 = create_silo(11112);

        manifest.register_version(&interface, 1, silo1.clone());
        manifest.register_version(&interface, 2, silo2.clone());

        let all_silos = manifest.get_all_silos_for_interface(&interface);
        assert_eq!(all_silos.len(), 2);
        assert!(all_silos.contains(&silo1));
        assert!(all_silos.contains(&silo2));
    }

    #[test]
    fn test_get_version_silo_counts() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo1 = create_silo(11111);
        let silo2 = create_silo(11112);
        let silo3 = create_silo(11113);

        manifest.register_version(&interface, 1, silo1.clone());
        manifest.register_version(&interface, 1, silo2.clone());
        manifest.register_version(&interface, 2, silo3.clone());

        let counts = manifest.get_version_silo_counts(&interface);
        assert_eq!(counts.get(&1), Some(&2));
        assert_eq!(counts.get(&2), Some(&1));
    }

    #[test]
    fn test_clear() {
        let manifest = GrainVersionManifest::new();
        let interface = create_interface("IMyGrain");
        let silo = create_silo(11111);

        manifest.register_version(&interface, 1, silo);
        assert_eq!(manifest.interface_count(), 1);

        manifest.clear();
        assert_eq!(manifest.interface_count(), 0);
        assert_eq!(manifest.total_registrations(), 0);
    }

    // Property-based tests
    #[cfg(test)]
    mod property_tests {
        use super::*;
        use ::proptest::prelude::*;

        ::proptest::proptest! {
            #[test]
            fn register_unregister_roundtrip(
                version in 1u16..100,
                port in 10000u16..60000
            ) {
                let manifest = GrainVersionManifest::new();
                let interface = create_interface("ITestGrain");
                let silo = create_silo(port);

                manifest.register_version(&interface, version, silo.clone());
                prop_assert!(manifest.is_version_available(&interface, version));

                manifest.unregister_version(&interface, version, &silo);
                prop_assert!(!manifest.is_version_available(&interface, version));
            }

            #[test]
            fn version_monotonically_increases(
                operations in proptest::collection::vec(1u16..10, 1..20)
            ) {
                let manifest = GrainVersionManifest::new();
                let interface = create_interface("ITestGrain");
                let silo = create_silo(11111);

                let mut prev_version = manifest.version();
                for v in operations {
                    manifest.register_version(&interface, v, silo.clone());
                    let current = manifest.version();
                    // Version should either increase or stay the same (if duplicate)
                    prop_assert!(current >= prev_version);
                    prev_version = current;
                }
            }
        }
    }
}
