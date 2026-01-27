//! LRU cache for grain directory lookups.
//!
//! The cache reduces directory lookups by storing recent grain -> address mappings.
//! Cache entries are invalidated when grains move or deactivate.

use lru::LruCache;
use orleans_core::{ActivationId, GrainAddress, GrainId};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};

/// Default cache size (number of entries).
pub const DEFAULT_CACHE_SIZE: usize = 100_000;

/// Options for the directory cache.
#[derive(Debug, Clone)]
pub struct DirectoryCacheOptions {
    /// Maximum number of entries in the cache.
    pub max_size: usize,
    /// Whether to enable the cache.
    pub enabled: bool,
}

impl Default for DirectoryCacheOptions {
    fn default() -> Self {
        Self {
            max_size: DEFAULT_CACHE_SIZE,
            enabled: true,
        }
    }
}

/// Statistics for the directory cache.
#[derive(Debug, Clone, Default)]
pub struct CacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of entries currently in the cache.
    pub size: usize,
    /// Number of invalidations.
    pub invalidations: u64,
}

impl CacheStats {
    /// Returns the hit rate as a percentage.
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            (self.hits as f64 / total as f64) * 100.0
        }
    }
}

/// Cache update for piggyback invalidation.
#[derive(Debug, Clone)]
pub struct GrainAddressCacheUpdate {
    /// Address to invalidate (if any).
    pub invalid_address: Option<GrainAddress>,
    /// New valid address (if any).
    pub valid_address: Option<GrainAddress>,
}

impl GrainAddressCacheUpdate {
    /// Creates an invalidation update.
    pub fn invalidate(address: GrainAddress) -> Self {
        Self {
            invalid_address: Some(address),
            valid_address: None,
        }
    }

    /// Creates an update with a new valid address.
    pub fn update(old: Option<GrainAddress>, new: GrainAddress) -> Self {
        Self {
            invalid_address: old,
            valid_address: Some(new),
        }
    }
}

/// LRU cache for grain directory entries.
///
/// Thread-safe cache that stores grain -> address mappings.
/// Supports invalidation for stale entries and pending invalidations.
pub struct GrainDirectoryCache {
    /// The LRU cache itself.
    cache: Mutex<LruCache<GrainId, GrainAddress>>,

    /// Set of grain IDs with pending invalidations.
    /// Lookups for these grains will return None.
    pending_invalidations: Mutex<HashSet<GrainId>>,

    /// Whether the cache is enabled.
    enabled: bool,

    /// Statistics counters.
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
}

impl GrainDirectoryCache {
    /// Creates a new directory cache with default options.
    pub fn new() -> Self {
        Self::with_options(DirectoryCacheOptions::default())
    }

    /// Creates a new directory cache with the specified options.
    pub fn with_options(options: DirectoryCacheOptions) -> Self {
        let size = NonZeroUsize::new(options.max_size).unwrap_or(NonZeroUsize::new(1).unwrap());

        Self {
            cache: Mutex::new(LruCache::new(size)),
            pending_invalidations: Mutex::new(HashSet::new()),
            enabled: options.enabled,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    /// Creates a disabled cache that never stores anything.
    pub fn disabled() -> Self {
        Self::with_options(DirectoryCacheOptions {
            max_size: 1,
            enabled: false,
        })
    }

    /// Returns whether the cache is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Looks up a grain in the cache.
    ///
    /// Returns None if:
    /// - The cache is disabled
    /// - The grain is not in the cache
    /// - The grain has a pending invalidation
    pub fn lookup(&self, grain_id: &GrainId) -> Option<GrainAddress> {
        if !self.enabled {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        // Check for pending invalidation
        {
            let pending = self.pending_invalidations.lock();
            if pending.contains(grain_id) {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        }

        // Look up in cache
        let mut cache = self.cache.lock();
        match cache.get(grain_id) {
            Some(address) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(address.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Inserts or updates a grain in the cache.
    pub fn insert(&self, grain_id: GrainId, address: GrainAddress) {
        if !self.enabled {
            return;
        }

        // Remove from pending invalidations
        {
            let mut pending = self.pending_invalidations.lock();
            pending.remove(&grain_id);
        }

        // Insert into cache
        let mut cache = self.cache.lock();
        cache.put(grain_id, address);
    }

    /// Invalidates a specific activation in the cache.
    ///
    /// Only removes the entry if the activation ID matches.
    pub fn invalidate(&self, grain_id: &GrainId, activation_id: &ActivationId) {
        if !self.enabled {
            return;
        }

        self.invalidations.fetch_add(1, Ordering::Relaxed);

        let mut cache = self.cache.lock();

        // Only remove if the activation matches
        if let Some(cached) = cache.peek(grain_id) {
            if cached.activation_id() == activation_id {
                cache.pop(grain_id);
            }
        }
    }

    /// Force-invalidates a grain regardless of activation ID.
    pub fn force_invalidate(&self, grain_id: &GrainId) {
        if !self.enabled {
            return;
        }

        self.invalidations.fetch_add(1, Ordering::Relaxed);

        let mut cache = self.cache.lock();
        cache.pop(grain_id);
    }

    /// Adds a pending invalidation for a grain.
    ///
    /// Subsequent lookups for this grain will return None until
    /// a new address is inserted.
    pub fn add_pending_invalidation(&self, grain_id: GrainId) {
        if !self.enabled {
            return;
        }

        let mut pending = self.pending_invalidations.lock();
        pending.insert(grain_id);
    }

    /// Removes a pending invalidation.
    pub fn remove_pending_invalidation(&self, grain_id: &GrainId) {
        if !self.enabled {
            return;
        }

        let mut pending = self.pending_invalidations.lock();
        pending.remove(grain_id);
    }

    /// Applies a cache update (e.g., from a message header).
    pub fn apply_update(&self, update: GrainAddressCacheUpdate) {
        if !self.enabled {
            return;
        }

        // Invalidate old address
        if let Some(invalid) = update.invalid_address {
            self.invalidate(invalid.grain_id(), invalid.activation_id());
        }

        // Insert new address
        if let Some(valid) = update.valid_address {
            self.insert(valid.grain_id().clone(), valid);
        }
    }

    /// Invalidates all entries for a specific silo.
    ///
    /// Used when a silo is declared dead.
    pub fn invalidate_silo(&self, silo: &orleans_core::SiloAddress) {
        if !self.enabled {
            return;
        }

        let mut cache = self.cache.lock();

        // Collect keys to remove (can't modify while iterating)
        let to_remove: Vec<_> = cache
            .iter()
            .filter(|(_, addr)| addr.silo_address() == Some(silo))
            .map(|(id, _)| id.clone())
            .collect();

        for grain_id in to_remove {
            cache.pop(&grain_id);
            self.invalidations.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: self.cache.lock().len(),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }

    /// Clears the cache.
    pub fn clear(&self) {
        self.cache.lock().clear();
        self.pending_invalidations.lock().clear();
    }

    /// Returns the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.cache.lock().len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.cache.lock().is_empty()
    }
}

impl Default for GrainDirectoryCache {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for GrainDirectoryCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stats = self.stats();
        f.debug_struct("GrainDirectoryCache")
            .field("enabled", &self.enabled)
            .field("size", &stats.size)
            .field("hit_rate", &format!("{:.1}%", stats.hit_rate()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn make_silo(port: u16) -> orleans_core::SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        orleans_core::SiloAddress::new(addr, 1)
    }

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(
            orleans_core::GrainType::create(name),
            orleans_core::IdSpan::from_str(name),
        )
    }

    fn make_grain_address(grain_id: &GrainId, silo: &orleans_core::SiloAddress) -> GrainAddress {
        GrainAddress::complete(grain_id.clone(), ActivationId::new(), silo.clone())
    }

    #[test]
    fn test_basic_insert_lookup() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Insert
        cache.insert(grain_id.clone(), address.clone());

        // Lookup
        let found = cache.lookup(&grain_id);
        assert_eq!(found, Some(address));

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_cache_miss() {
        let cache = GrainDirectoryCache::new();
        let grain_id = make_grain_id("nonexistent");

        let found = cache.lookup(&grain_id);
        assert!(found.is_none());

        let stats = cache.stats();
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_invalidate() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        cache.insert(grain_id.clone(), address.clone());
        assert!(cache.lookup(&grain_id).is_some());

        // Invalidate with correct activation ID
        cache.invalidate(&grain_id, address.activation_id());
        assert!(cache.lookup(&grain_id).is_none());
    }

    #[test]
    fn test_invalidate_wrong_activation() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        cache.insert(grain_id.clone(), address.clone());

        // Invalidate with wrong activation ID
        let wrong_activation = ActivationId::new();
        cache.invalidate(&grain_id, &wrong_activation);

        // Should still be in cache
        assert!(cache.lookup(&grain_id).is_some());
    }

    #[test]
    fn test_pending_invalidation() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        cache.insert(grain_id.clone(), address);

        // Add pending invalidation
        cache.add_pending_invalidation(grain_id.clone());

        // Lookup should fail
        assert!(cache.lookup(&grain_id).is_none());

        // Remove pending invalidation
        cache.remove_pending_invalidation(&grain_id);

        // Lookup should succeed again
        assert!(cache.lookup(&grain_id).is_some());
    }

    #[test]
    fn test_insert_clears_pending() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        // Add pending invalidation
        cache.add_pending_invalidation(grain_id.clone());
        assert!(cache.lookup(&grain_id).is_none());

        // Insert new address (should clear pending)
        cache.insert(grain_id.clone(), address.clone());

        // Lookup should succeed
        assert_eq!(cache.lookup(&grain_id), Some(address));
    }

    #[test]
    fn test_invalidate_silo() {
        let cache = GrainDirectoryCache::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        let grain1 = make_grain_id("grain1");
        let grain2 = make_grain_id("grain2");
        let grain3 = make_grain_id("grain3");

        cache.insert(grain1.clone(), make_grain_address(&grain1, &silo1));
        cache.insert(grain2.clone(), make_grain_address(&grain2, &silo2));
        cache.insert(grain3.clone(), make_grain_address(&grain3, &silo2));

        assert_eq!(cache.len(), 3);

        // Invalidate silo2
        cache.invalidate_silo(&silo2);

        assert_eq!(cache.len(), 1);
        assert!(cache.lookup(&grain1).is_some());
        assert!(cache.lookup(&grain2).is_none());
        assert!(cache.lookup(&grain3).is_none());
    }

    #[test]
    fn test_apply_update() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);

        let grain_id = make_grain_id("test-grain");
        let old_address = make_grain_address(&grain_id, &silo);
        let new_address = make_grain_address(&grain_id, &silo);

        cache.insert(grain_id.clone(), old_address.clone());

        // Apply update
        let update = GrainAddressCacheUpdate::update(Some(old_address), new_address.clone());
        cache.apply_update(update);

        assert_eq!(cache.lookup(&grain_id), Some(new_address));
    }

    #[test]
    fn test_disabled_cache() {
        let cache = GrainDirectoryCache::disabled();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        assert!(!cache.is_enabled());

        // Insert and lookup
        cache.insert(grain_id.clone(), address);
        assert!(cache.lookup(&grain_id).is_none());
    }

    #[test]
    fn test_lru_eviction() {
        let options = DirectoryCacheOptions {
            max_size: 2,
            enabled: true,
        };
        let cache = GrainDirectoryCache::with_options(options);
        let silo = make_silo(11111);

        let grain1 = make_grain_id("grain1");
        let grain2 = make_grain_id("grain2");
        let grain3 = make_grain_id("grain3");

        cache.insert(grain1.clone(), make_grain_address(&grain1, &silo));
        cache.insert(grain2.clone(), make_grain_address(&grain2, &silo));
        cache.insert(grain3.clone(), make_grain_address(&grain3, &silo));

        // grain1 should be evicted (LRU)
        assert!(cache.lookup(&grain1).is_none());
        assert!(cache.lookup(&grain2).is_some());
        assert!(cache.lookup(&grain3).is_some());
    }

    #[test]
    fn test_stats() {
        let cache = GrainDirectoryCache::new();
        let silo = make_silo(11111);
        let grain_id = make_grain_id("test-grain");
        let address = make_grain_address(&grain_id, &silo);

        cache.insert(grain_id.clone(), address.clone());
        cache.lookup(&grain_id); // hit
        cache.lookup(&grain_id); // hit
        cache.lookup(&make_grain_id("nonexistent")); // miss
        cache.invalidate(&grain_id, address.activation_id());

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.invalidations, 1);
        assert_eq!(stats.hit_rate(), 2.0 / 3.0 * 100.0);
    }
}
