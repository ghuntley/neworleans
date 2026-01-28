//! Silo runtime statistics for placement decisions.

use orleans_core::SiloAddress;
use serde::{Deserialize, Serialize};
use std::time::Instant;

/// Runtime statistics for a silo used in placement decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiloRuntimeStatistics {
    /// The silo address these statistics are for.
    #[serde(skip)]
    silo_address: Option<SiloAddress>,

    /// Number of active grain activations.
    activation_count: u32,

    /// Number of recently used activations (accessed within collection window).
    recently_used_activation_count: u32,

    /// Whether the silo is currently overloaded.
    is_overloaded: bool,

    /// CPU usage as a percentage (0.0 to 100.0).
    cpu_usage: f64,

    /// Memory usage as a normalized value (0.0 to 1.0).
    memory_usage: f64,

    /// Available memory in bytes.
    available_memory: u64,

    /// Maximum available memory in bytes.
    max_available_memory: u64,

    /// Timestamp when these statistics were collected.
    #[serde(skip)]
    collected_at: Option<Instant>,
}

impl Default for SiloRuntimeStatistics {
    fn default() -> Self {
        Self {
            silo_address: None,
            activation_count: 0,
            recently_used_activation_count: 0,
            is_overloaded: false,
            cpu_usage: 0.0,
            memory_usage: 0.0,
            available_memory: 0,
            max_available_memory: 0,
            collected_at: None,
        }
    }
}

impl SiloRuntimeStatistics {
    /// Creates new statistics for a silo.
    pub fn new(silo_address: SiloAddress) -> Self {
        Self {
            silo_address: Some(silo_address),
            collected_at: Some(Instant::now()),
            ..Default::default()
        }
    }

    /// Sets the silo address.
    pub fn with_silo_address(mut self, address: SiloAddress) -> Self {
        self.silo_address = Some(address);
        self
    }

    /// Sets the activation count.
    pub fn with_activation_count(mut self, count: u32) -> Self {
        self.activation_count = count;
        self
    }

    /// Sets the recently used activation count.
    pub fn with_recently_used_activation_count(mut self, count: u32) -> Self {
        self.recently_used_activation_count = count;
        self
    }

    /// Sets the overloaded flag.
    pub fn with_overloaded(mut self, overloaded: bool) -> Self {
        self.is_overloaded = overloaded;
        self
    }

    /// Sets the CPU usage percentage.
    pub fn with_cpu_usage(mut self, cpu: f64) -> Self {
        self.cpu_usage = cpu.clamp(0.0, 100.0);
        self
    }

    /// Sets the memory usage (normalized 0.0 to 1.0).
    pub fn with_memory_usage(mut self, usage: f64) -> Self {
        self.memory_usage = usage.clamp(0.0, 1.0);
        self
    }

    /// Sets the available memory in bytes.
    pub fn with_available_memory(mut self, bytes: u64) -> Self {
        self.available_memory = bytes;
        self
    }

    /// Sets the maximum available memory in bytes.
    pub fn with_max_available_memory(mut self, bytes: u64) -> Self {
        self.max_available_memory = bytes;
        self
    }

    /// Returns the silo address.
    pub fn silo_address(&self) -> Option<&SiloAddress> {
        self.silo_address.as_ref()
    }

    /// Returns the activation count.
    pub fn activation_count(&self) -> u32 {
        self.activation_count
    }

    /// Returns the recently used activation count.
    pub fn recently_used_activation_count(&self) -> u32 {
        self.recently_used_activation_count
    }

    /// Returns the total activation count (active + recently used).
    pub fn total_activation_count(&self) -> u32 {
        self.activation_count + self.recently_used_activation_count
    }

    /// Returns whether the silo is overloaded.
    pub fn is_overloaded(&self) -> bool {
        self.is_overloaded
    }

    /// Returns the CPU usage percentage.
    pub fn cpu_usage(&self) -> f64 {
        self.cpu_usage
    }

    /// Returns the memory usage (normalized 0.0 to 1.0).
    pub fn memory_usage(&self) -> f64 {
        self.memory_usage
    }

    /// Returns the available memory in bytes.
    pub fn available_memory(&self) -> u64 {
        self.available_memory
    }

    /// Returns the maximum available memory in bytes.
    pub fn max_available_memory(&self) -> u64 {
        self.max_available_memory
    }

    /// Returns the normalized available memory (0.0 to 1.0).
    pub fn normalized_available_memory(&self) -> f64 {
        if self.max_available_memory == 0 {
            0.0
        } else {
            (self.available_memory as f64 / self.max_available_memory as f64).clamp(0.0, 1.0)
        }
    }

    /// Returns the normalized max available memory (0.0 to 1.0).
    ///
    /// This is relative to the highest observed max available memory
    /// across all silos, which should be provided externally.
    pub fn normalized_max_available_memory(&self, cluster_max: u64) -> f64 {
        if cluster_max == 0 {
            0.0
        } else {
            (self.max_available_memory as f64 / cluster_max as f64).clamp(0.0, 1.0)
        }
    }

    /// Returns the timestamp when these statistics were collected.
    pub fn collected_at(&self) -> Option<Instant> {
        self.collected_at
    }

    /// Returns the age of these statistics.
    pub fn age(&self) -> Option<std::time::Duration> {
        self.collected_at.map(|t| t.elapsed())
    }

    /// Returns true if statistics are fresh (collected within the threshold).
    pub fn is_fresh(&self, max_age: std::time::Duration) -> bool {
        self.age().map(|a| a < max_age).unwrap_or(false)
    }

    /// Updates the collected timestamp to now.
    pub fn touch(&mut self) {
        self.collected_at = Some(Instant::now());
    }
}

/// Cache of silo runtime statistics.
#[derive(Debug)]
pub struct SiloStatisticsCache {
    stats: dashmap::DashMap<SiloAddress, SiloRuntimeStatistics>,
}

impl Default for SiloStatisticsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SiloStatisticsCache {
    /// Creates a new empty statistics cache.
    pub fn new() -> Self {
        Self {
            stats: dashmap::DashMap::new(),
        }
    }

    /// Updates statistics for a silo.
    pub fn update(&self, silo: SiloAddress, stats: SiloRuntimeStatistics) {
        self.stats.insert(silo, stats);
    }

    /// Gets statistics for a silo.
    pub fn get(&self, silo: &SiloAddress) -> Option<SiloRuntimeStatistics> {
        self.stats.get(silo).map(|r| r.clone())
    }

    /// Gets statistics for a silo, returning default if not found.
    pub fn get_or_default(&self, silo: &SiloAddress) -> SiloRuntimeStatistics {
        self.get(silo)
            .unwrap_or_else(|| SiloRuntimeStatistics::new(silo.clone()))
    }

    /// Removes statistics for a silo.
    pub fn remove(&self, silo: &SiloAddress) {
        self.stats.remove(silo);
    }

    /// Clears all statistics.
    pub fn clear(&self) {
        self.stats.clear();
    }

    /// Returns the number of silos with cached statistics.
    pub fn len(&self) -> usize {
        self.stats.len()
    }

    /// Returns true if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.stats.is_empty()
    }

    /// Returns all cached statistics.
    pub fn all(&self) -> Vec<(SiloAddress, SiloRuntimeStatistics)> {
        self.stats
            .iter()
            .map(|r| (r.key().clone(), r.value().clone()))
            .collect()
    }

    /// Returns the maximum activation count across all silos.
    pub fn max_activation_count(&self) -> u32 {
        self.stats
            .iter()
            .map(|r| r.activation_count())
            .max()
            .unwrap_or(1)
    }

    /// Returns the maximum available memory across all silos.
    pub fn max_available_memory(&self) -> u64 {
        self.stats
            .iter()
            .map(|r| r.max_available_memory())
            .max()
            .unwrap_or(1)
    }

    /// Returns silos that are not overloaded.
    pub fn non_overloaded_silos(&self) -> Vec<SiloAddress> {
        self.stats
            .iter()
            .filter(|r| !r.is_overloaded())
            .map(|r| r.key().clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    #[test]
    fn test_statistics_defaults() {
        let stats = SiloRuntimeStatistics::default();
        assert_eq!(stats.activation_count(), 0);
        assert_eq!(stats.recently_used_activation_count(), 0);
        assert!(!stats.is_overloaded());
        assert_eq!(stats.cpu_usage(), 0.0);
        assert_eq!(stats.memory_usage(), 0.0);
    }

    #[test]
    fn test_statistics_builder() {
        let silo = make_silo(11111);
        let stats = SiloRuntimeStatistics::new(silo.clone())
            .with_activation_count(100)
            .with_recently_used_activation_count(50)
            .with_overloaded(true)
            .with_cpu_usage(75.0)
            .with_memory_usage(0.8)
            .with_available_memory(1_000_000_000)
            .with_max_available_memory(4_000_000_000);

        assert_eq!(stats.silo_address(), Some(&silo));
        assert_eq!(stats.activation_count(), 100);
        assert_eq!(stats.recently_used_activation_count(), 50);
        assert_eq!(stats.total_activation_count(), 150);
        assert!(stats.is_overloaded());
        assert_eq!(stats.cpu_usage(), 75.0);
        assert_eq!(stats.memory_usage(), 0.8);
        assert_eq!(stats.available_memory(), 1_000_000_000);
        assert_eq!(stats.max_available_memory(), 4_000_000_000);
    }

    #[test]
    fn test_statistics_clamping() {
        let stats = SiloRuntimeStatistics::default()
            .with_cpu_usage(150.0) // Should clamp to 100
            .with_memory_usage(1.5); // Should clamp to 1.0

        assert_eq!(stats.cpu_usage(), 100.0);
        assert_eq!(stats.memory_usage(), 1.0);

        let stats = SiloRuntimeStatistics::default()
            .with_cpu_usage(-10.0)
            .with_memory_usage(-0.5);

        assert_eq!(stats.cpu_usage(), 0.0);
        assert_eq!(stats.memory_usage(), 0.0);
    }

    #[test]
    fn test_normalized_available_memory() {
        let stats = SiloRuntimeStatistics::default()
            .with_available_memory(2_000_000_000)
            .with_max_available_memory(4_000_000_000);

        assert_eq!(stats.normalized_available_memory(), 0.5);
    }

    #[test]
    fn test_normalized_available_memory_zero_max() {
        let stats = SiloRuntimeStatistics::default()
            .with_available_memory(1_000_000)
            .with_max_available_memory(0);

        assert_eq!(stats.normalized_available_memory(), 0.0);
    }

    #[test]
    fn test_normalized_max_available_memory() {
        let stats = SiloRuntimeStatistics::default().with_max_available_memory(2_000_000_000);

        assert_eq!(stats.normalized_max_available_memory(4_000_000_000), 0.5);
        assert_eq!(stats.normalized_max_available_memory(0), 0.0);
    }

    #[test]
    fn test_statistics_freshness() {
        let silo = make_silo(11111);
        let stats = SiloRuntimeStatistics::new(silo);

        assert!(stats.is_fresh(std::time::Duration::from_secs(10)));
        assert!(stats.collected_at().is_some());
    }

    #[test]
    fn test_cache_operations() {
        let cache = SiloStatisticsCache::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        // Initially empty
        assert!(cache.is_empty());
        assert!(cache.get(&silo1).is_none());

        // Insert stats
        let stats1 = SiloRuntimeStatistics::new(silo1.clone()).with_activation_count(100);
        cache.update(silo1.clone(), stats1);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&silo1).unwrap().activation_count(), 100);

        // get_or_default
        let default = cache.get_or_default(&silo2);
        assert_eq!(default.activation_count(), 0);

        // Update existing
        let stats1_updated =
            SiloRuntimeStatistics::new(silo1.clone()).with_activation_count(200);
        cache.update(silo1.clone(), stats1_updated);
        assert_eq!(cache.get(&silo1).unwrap().activation_count(), 200);

        // Remove
        cache.remove(&silo1);
        assert!(cache.get(&silo1).is_none());
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_max_activation_count() {
        let cache = SiloStatisticsCache::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        cache.update(
            silo1,
            SiloRuntimeStatistics::default().with_activation_count(100),
        );
        cache.update(
            silo2,
            SiloRuntimeStatistics::default().with_activation_count(300),
        );
        cache.update(
            silo3,
            SiloRuntimeStatistics::default().with_activation_count(200),
        );

        assert_eq!(cache.max_activation_count(), 300);
    }

    #[test]
    fn test_cache_non_overloaded_silos() {
        let cache = SiloStatisticsCache::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        cache.update(
            silo1.clone(),
            SiloRuntimeStatistics::default().with_overloaded(false),
        );
        cache.update(
            silo2.clone(),
            SiloRuntimeStatistics::default().with_overloaded(true),
        );
        cache.update(
            silo3.clone(),
            SiloRuntimeStatistics::default().with_overloaded(false),
        );

        let non_overloaded = cache.non_overloaded_silos();
        assert_eq!(non_overloaded.len(), 2);
        assert!(non_overloaded.contains(&silo1));
        assert!(!non_overloaded.contains(&silo2));
        assert!(non_overloaded.contains(&silo3));
    }

    #[test]
    fn test_cache_all() {
        let cache = SiloStatisticsCache::new();
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);

        cache.update(
            silo1.clone(),
            SiloRuntimeStatistics::default().with_activation_count(10),
        );
        cache.update(
            silo2.clone(),
            SiloRuntimeStatistics::default().with_activation_count(20),
        );

        let all = cache.all();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_cache_clear() {
        let cache = SiloStatisticsCache::new();
        let silo = make_silo(11111);
        cache.update(silo, SiloRuntimeStatistics::default());

        assert!(!cache.is_empty());
        cache.clear();
        assert!(cache.is_empty());
    }
}
