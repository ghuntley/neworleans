//! Manager components for version compatibility and selection.

use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::RwLock;
use orleans_core::{GrainType, SiloAddress};
use tracing::{debug, trace, instrument};

use crate::compatibility::{
    CompatibilityDirector, BackwardCompatible,
    create_compatibility_director,
};
use crate::selector::{
    VersionSelector, AllCompatibleVersionsSelector,
    create_version_selector,
};
use crate::manifest::GrainVersionManifest;
use crate::error::{VersionError, VersionResult};

/// Manages compatibility directors for grain interfaces.
///
/// Each interface can have its own compatibility strategy, with a configurable
/// default for interfaces without explicit configuration.
#[derive(Debug)]
pub struct CompatibilityDirectorManager {
    /// Per-interface compatibility directors.
    directors: RwLock<HashMap<GrainType, Arc<dyn CompatibilityDirector>>>,

    /// Default compatibility director.
    default: RwLock<Arc<dyn CompatibilityDirector>>,
}

impl Default for CompatibilityDirectorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CompatibilityDirectorManager {
    /// Creates a new manager with backward compatible as the default strategy.
    pub fn new() -> Self {
        Self {
            directors: RwLock::new(HashMap::new()),
            default: RwLock::new(Arc::new(BackwardCompatible)),
        }
    }

    /// Gets the compatibility director for an interface.
    ///
    /// Returns the interface-specific director if configured, otherwise the default.
    pub fn get_director(&self, interface_type: &GrainType) -> Arc<dyn CompatibilityDirector> {
        let directors = self.directors.read();
        directors
            .get(interface_type)
            .cloned()
            .unwrap_or_else(|| self.default.read().clone())
    }

    /// Gets the default compatibility director.
    pub fn default_director(&self) -> Arc<dyn CompatibilityDirector> {
        self.default.read().clone()
    }

    /// Sets the global default compatibility strategy.
    #[instrument(skip(self), level = "debug")]
    pub fn set_default(&self, strategy_name: &str) -> VersionResult<()> {
        let director = create_compatibility_director(strategy_name)
            .ok_or_else(|| VersionError::unknown_strategy(strategy_name))?;

        *self.default.write() = Arc::from(director);
        debug!(strategy = strategy_name, "Set default compatibility strategy");
        Ok(())
    }

    /// Sets the compatibility strategy for a specific interface.
    #[instrument(skip(self), level = "debug")]
    pub fn set_strategy(
        &self,
        interface_type: GrainType,
        strategy_name: &str,
    ) -> VersionResult<()> {
        let director = create_compatibility_director(strategy_name)
            .ok_or_else(|| VersionError::unknown_strategy(strategy_name))?;

        self.directors.write().insert(interface_type.clone(), Arc::from(director));
        debug!(
            interface = %interface_type,
            strategy = strategy_name,
            "Set interface compatibility strategy"
        );
        Ok(())
    }

    /// Sets a custom compatibility director for an interface.
    pub fn set_custom_director(
        &self,
        interface_type: GrainType,
        director: Arc<dyn CompatibilityDirector>,
    ) {
        self.directors.write().insert(interface_type, director);
    }

    /// Removes interface-specific configuration, reverting to default.
    pub fn remove_strategy(&self, interface_type: &GrainType) {
        self.directors.write().remove(interface_type);
    }
}

/// Manages version selectors for grain interfaces.
///
/// Each interface can have its own selector strategy, with a configurable
/// default for interfaces without explicit configuration.
#[derive(Debug)]
pub struct VersionSelectorManager {
    /// Per-interface version selectors.
    selectors: RwLock<HashMap<GrainType, Arc<dyn VersionSelector>>>,

    /// Default version selector.
    default: RwLock<Arc<dyn VersionSelector>>,
}

impl Default for VersionSelectorManager {
    fn default() -> Self {
        Self::new()
    }
}

impl VersionSelectorManager {
    /// Creates a new manager with all compatible versions as the default selector.
    pub fn new() -> Self {
        Self {
            selectors: RwLock::new(HashMap::new()),
            default: RwLock::new(Arc::new(AllCompatibleVersionsSelector)),
        }
    }

    /// Gets the version selector for an interface.
    ///
    /// Returns the interface-specific selector if configured, otherwise the default.
    pub fn get_selector(&self, interface_type: &GrainType) -> Arc<dyn VersionSelector> {
        let selectors = self.selectors.read();
        selectors
            .get(interface_type)
            .cloned()
            .unwrap_or_else(|| self.default.read().clone())
    }

    /// Gets the default version selector.
    pub fn default_selector(&self) -> Arc<dyn VersionSelector> {
        self.default.read().clone()
    }

    /// Sets the global default version selector strategy.
    #[instrument(skip(self), level = "debug")]
    pub fn set_default(&self, strategy_name: &str) -> VersionResult<()> {
        let selector = create_version_selector(strategy_name)
            .ok_or_else(|| VersionError::unknown_strategy(strategy_name))?;

        *self.default.write() = Arc::from(selector);
        debug!(strategy = strategy_name, "Set default version selector strategy");
        Ok(())
    }

    /// Sets the version selector strategy for a specific interface.
    #[instrument(skip(self), level = "debug")]
    pub fn set_strategy(
        &self,
        interface_type: GrainType,
        strategy_name: &str,
    ) -> VersionResult<()> {
        let selector = create_version_selector(strategy_name)
            .ok_or_else(|| VersionError::unknown_strategy(strategy_name))?;

        self.selectors.write().insert(interface_type.clone(), Arc::from(selector));
        debug!(
            interface = %interface_type,
            strategy = strategy_name,
            "Set interface version selector strategy"
        );
        Ok(())
    }

    /// Sets a custom version selector for an interface.
    pub fn set_custom_selector(
        &self,
        interface_type: GrainType,
        selector: Arc<dyn VersionSelector>,
    ) {
        self.selectors.write().insert(interface_type, selector);
    }

    /// Removes interface-specific configuration, reverting to default.
    pub fn remove_strategy(&self, interface_type: &GrainType) {
        self.selectors.write().remove(interface_type);
    }
}

/// Result of a suitable silos lookup.
#[derive(Debug, Clone)]
pub struct SuitableSilosResult {
    /// The suitable silos for the requested version.
    pub suitable_silos: Vec<SiloAddress>,

    /// Silos grouped by version.
    pub silos_by_version: HashMap<u16, Vec<SiloAddress>>,

    /// The manifest version when this result was computed.
    pub manifest_version: u64,
}

/// Cached entry for version selection results.
#[derive(Debug, Clone)]
pub struct CachedEntry {
    /// The manifest version when this entry was cached.
    pub manifest_version: u64,

    /// The suitable silos.
    pub suitable_silos: Vec<SiloAddress>,

    /// Silos grouped by version.
    pub silos_by_version: HashMap<u16, Vec<SiloAddress>>,
}

/// Cache key for version selection.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
struct CacheKey {
    grain_type: GrainType,
    interface_type: GrainType,
    requested_version: u16,
}

/// Caching layer for version selection that tracks manifest changes.
///
/// Caches version selection results and invalidates when the cluster
/// manifest version changes.
#[derive(Debug)]
pub struct CachedVersionSelectorManager {
    /// The underlying version manifest.
    manifest: Arc<GrainVersionManifest>,

    /// Compatibility director manager.
    compatibility_manager: Arc<CompatibilityDirectorManager>,

    /// Version selector manager.
    selector_manager: Arc<VersionSelectorManager>,

    /// Cache of suitable silos by (grain_type, interface_type, requested_version).
    cache: DashMap<CacheKey, CachedEntry>,
}

impl CachedVersionSelectorManager {
    /// Creates a new cached version selector manager.
    pub fn new(
        manifest: Arc<GrainVersionManifest>,
        compatibility_manager: Arc<CompatibilityDirectorManager>,
        selector_manager: Arc<VersionSelectorManager>,
    ) -> Self {
        Self {
            manifest,
            compatibility_manager,
            selector_manager,
            cache: DashMap::new(),
        }
    }

    /// Gets suitable silos for a grain placement request.
    ///
    /// Results are cached and automatically invalidated when the manifest changes.
    #[instrument(skip(self), level = "trace")]
    pub fn get_suitable_silos(
        &self,
        grain_type: &GrainType,
        interface_type: &GrainType,
        requested_version: u16,
    ) -> SuitableSilosResult {
        let key = CacheKey {
            grain_type: grain_type.clone(),
            interface_type: interface_type.clone(),
            requested_version,
        };

        let current_manifest_version = self.manifest.version();

        // Check cache
        if let Some(entry) = self.cache.get(&key) {
            if entry.manifest_version == current_manifest_version {
                trace!(
                    grain_type = %grain_type,
                    interface_type = %interface_type,
                    requested_version = requested_version,
                    "Cache hit"
                );
                return SuitableSilosResult {
                    suitable_silos: entry.suitable_silos.clone(),
                    silos_by_version: entry.silos_by_version.clone(),
                    manifest_version: entry.manifest_version,
                };
            }
        }

        // Cache miss or stale - compute
        let result = self.compute_suitable_silos(interface_type, requested_version);

        // Update cache
        let entry = CachedEntry {
            manifest_version: current_manifest_version,
            suitable_silos: result.suitable_silos.clone(),
            silos_by_version: result.silos_by_version.clone(),
        };
        self.cache.insert(key, entry);

        trace!(
            grain_type = %grain_type,
            interface_type = %interface_type,
            requested_version = requested_version,
            suitable_count = result.suitable_silos.len(),
            "Computed and cached suitable silos"
        );

        result
    }

    fn compute_suitable_silos(
        &self,
        interface_type: &GrainType,
        requested_version: u16,
    ) -> SuitableSilosResult {
        let available_versions = self.manifest.get_available_versions(interface_type);
        let compatibility = self.compatibility_manager.get_director(interface_type);
        let selector = self.selector_manager.get_selector(interface_type);

        // Get suitable versions based on selector strategy
        let suitable_versions =
            selector.get_suitable_versions(requested_version, &available_versions, compatibility.as_ref());

        // Get silos for each suitable version
        let mut silos_by_version: HashMap<u16, Vec<SiloAddress>> = HashMap::new();
        let mut all_suitable_silos: Vec<SiloAddress> = Vec::new();

        for version in suitable_versions {
            let silos = self.manifest.get_supported_silos(interface_type, version);
            if !silos.is_empty() {
                all_suitable_silos.extend(silos.iter().cloned());
                silos_by_version.insert(version, silos);
            }
        }

        // Deduplicate all_suitable_silos while preserving order
        let mut seen = std::collections::HashSet::new();
        all_suitable_silos.retain(|silo| seen.insert(silo.clone()));

        SuitableSilosResult {
            suitable_silos: all_suitable_silos,
            silos_by_version,
            manifest_version: self.manifest.version(),
        }
    }

    /// Invalidates all cached entries.
    pub fn invalidate_all(&self) {
        self.cache.clear();
        debug!("Invalidated all cached version selection entries");
    }

    /// Gets the number of cached entries.
    pub fn cache_size(&self) -> usize {
        self.cache.len()
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

    mod compatibility_director_manager {
        use super::*;

        #[test]
        fn test_default_is_backward_compatible() {
            let manager = CompatibilityDirectorManager::new();
            let director = manager.default_director();
            assert_eq!(director.name(), "BackwardCompatible");
        }

        #[test]
        fn test_get_director_uses_default() {
            let manager = CompatibilityDirectorManager::new();
            let interface = create_interface("IMyGrain");
            let director = manager.get_director(&interface);
            assert_eq!(director.name(), "BackwardCompatible");
        }

        #[test]
        fn test_set_default() {
            let manager = CompatibilityDirectorManager::new();
            manager.set_default("StrictVersionCompatible").unwrap();
            assert_eq!(manager.default_director().name(), "StrictVersionCompatible");
        }

        #[test]
        fn test_set_interface_strategy() {
            let manager = CompatibilityDirectorManager::new();
            let interface = create_interface("IMyGrain");

            manager.set_strategy(interface.clone(), "StrictVersionCompatible").unwrap();

            let director = manager.get_director(&interface);
            assert_eq!(director.name(), "StrictVersionCompatible");

            // Other interfaces should still use default
            let other = create_interface("IOtherGrain");
            let director = manager.get_director(&other);
            assert_eq!(director.name(), "BackwardCompatible");
        }

        #[test]
        fn test_unknown_strategy_error() {
            let manager = CompatibilityDirectorManager::new();
            let result = manager.set_default("UnknownStrategy");
            assert!(result.is_err());
        }

        #[test]
        fn test_remove_strategy() {
            let manager = CompatibilityDirectorManager::new();
            let interface = create_interface("IMyGrain");

            manager.set_strategy(interface.clone(), "StrictVersionCompatible").unwrap();
            assert_eq!(manager.get_director(&interface).name(), "StrictVersionCompatible");

            manager.remove_strategy(&interface);
            assert_eq!(manager.get_director(&interface).name(), "BackwardCompatible");
        }
    }

    mod version_selector_manager {
        use super::*;

        #[test]
        fn test_default_is_all_compatible() {
            let manager = VersionSelectorManager::new();
            let selector = manager.default_selector();
            assert_eq!(selector.name(), "AllCompatibleVersions");
        }

        #[test]
        fn test_get_selector_uses_default() {
            let manager = VersionSelectorManager::new();
            let interface = create_interface("IMyGrain");
            let selector = manager.get_selector(&interface);
            assert_eq!(selector.name(), "AllCompatibleVersions");
        }

        #[test]
        fn test_set_default() {
            let manager = VersionSelectorManager::new();
            manager.set_default("LatestVersion").unwrap();
            assert_eq!(manager.default_selector().name(), "LatestVersion");
        }

        #[test]
        fn test_set_interface_strategy() {
            let manager = VersionSelectorManager::new();
            let interface = create_interface("IMyGrain");

            manager.set_strategy(interface.clone(), "MinimumVersion").unwrap();

            let selector = manager.get_selector(&interface);
            assert_eq!(selector.name(), "MinimumVersion");

            // Other interfaces should still use default
            let other = create_interface("IOtherGrain");
            let selector = manager.get_selector(&other);
            assert_eq!(selector.name(), "AllCompatibleVersions");
        }

        #[test]
        fn test_unknown_strategy_error() {
            let manager = VersionSelectorManager::new();
            let result = manager.set_default("UnknownStrategy");
            assert!(result.is_err());
        }
    }

    mod cached_version_selector_manager {
        use super::*;

        fn setup() -> (Arc<GrainVersionManifest>, CachedVersionSelectorManager) {
            let manifest = Arc::new(GrainVersionManifest::new());
            let compat_manager = Arc::new(CompatibilityDirectorManager::new());
            let selector_manager = Arc::new(VersionSelectorManager::new());

            let cached = CachedVersionSelectorManager::new(
                manifest.clone(),
                compat_manager,
                selector_manager,
            );

            (manifest, cached)
        }

        #[test]
        fn test_get_suitable_silos_empty_manifest() {
            let (_, cached) = setup();
            let interface = create_interface("IMyGrain");
            let grain_type = create_interface("MyGrain");

            let result = cached.get_suitable_silos(&grain_type, &interface, 1);
            assert!(result.suitable_silos.is_empty());
            assert!(result.silos_by_version.is_empty());
        }

        #[test]
        fn test_get_suitable_silos_with_data() {
            let (manifest, cached) = setup();
            let interface = create_interface("IMyGrain");
            let grain_type = create_interface("MyGrain");
            let silo1 = create_silo(11111);
            let silo2 = create_silo(11112);

            manifest.register_version(&interface, 1, silo1.clone());
            manifest.register_version(&interface, 2, silo2.clone());

            // Request v1 with backward compatible: v1 and v2 are compatible
            let result = cached.get_suitable_silos(&grain_type, &interface, 1);
            assert_eq!(result.suitable_silos.len(), 2);
            assert!(result.suitable_silos.contains(&silo1));
            assert!(result.suitable_silos.contains(&silo2));
        }

        #[test]
        fn test_caching() {
            let (manifest, cached) = setup();
            let interface = create_interface("IMyGrain");
            let grain_type = create_interface("MyGrain");
            let silo = create_silo(11111);

            manifest.register_version(&interface, 1, silo.clone());

            // First call - cache miss
            let result1 = cached.get_suitable_silos(&grain_type, &interface, 1);
            assert_eq!(cached.cache_size(), 1);

            // Second call - cache hit
            let result2 = cached.get_suitable_silos(&grain_type, &interface, 1);
            assert_eq!(result1.suitable_silos, result2.suitable_silos);
        }

        #[test]
        fn test_cache_invalidation_on_manifest_change() {
            let (manifest, cached) = setup();
            let interface = create_interface("IMyGrain");
            let grain_type = create_interface("MyGrain");
            let silo1 = create_silo(11111);
            let silo2 = create_silo(11112);

            manifest.register_version(&interface, 1, silo1.clone());
            let result1 = cached.get_suitable_silos(&grain_type, &interface, 1);
            let v1 = result1.manifest_version;

            // Add another silo - manifest version changes
            manifest.register_version(&interface, 1, silo2.clone());
            let result2 = cached.get_suitable_silos(&grain_type, &interface, 1);
            let v2 = result2.manifest_version;

            // Version should have changed
            assert!(v2 > v1);
            // And we should now have both silos
            assert_eq!(result2.suitable_silos.len(), 2);
        }

        #[test]
        fn test_invalidate_all() {
            let (manifest, cached) = setup();
            let interface = create_interface("IMyGrain");
            let grain_type = create_interface("MyGrain");
            let silo = create_silo(11111);

            manifest.register_version(&interface, 1, silo);
            cached.get_suitable_silos(&grain_type, &interface, 1);
            assert_eq!(cached.cache_size(), 1);

            cached.invalidate_all();
            assert_eq!(cached.cache_size(), 0);
        }
    }
}
