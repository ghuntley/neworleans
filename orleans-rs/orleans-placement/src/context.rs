//! Placement context for providing information to placement directors.

use crate::statistics::{SiloRuntimeStatistics, SiloStatisticsCache};
use orleans_core::{GrainId, GrainType, SiloAddress};
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

/// Silo status for placement decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiloStatus {
    /// Silo is active and accepting new activations.
    Active,
    /// Silo is shutting down and not accepting new activations.
    ShuttingDown,
    /// Silo is stopping and not accepting new activations.
    Stopping,
    /// Silo is dead.
    Dead,
}

impl SiloStatus {
    /// Returns true if the silo is terminating (shutting down or stopping).
    pub fn is_terminating(&self) -> bool {
        matches!(self, SiloStatus::ShuttingDown | SiloStatus::Stopping | SiloStatus::Dead)
    }

    /// Returns true if the silo is active and can accept activations.
    pub fn is_active(&self) -> bool {
        matches!(self, SiloStatus::Active)
    }
}

/// Target for placement decisions.
#[derive(Debug, Clone)]
pub struct PlacementTarget {
    /// The grain identity to place.
    grain_id: GrainId,

    /// The grain interface type being called.
    interface_type: GrainType,

    /// The interface version being requested.
    interface_version: u16,

    /// Request context data that may influence placement.
    request_context: HashMap<String, Arc<dyn Any + Send + Sync>>,
}

impl PlacementTarget {
    /// Creates a new placement target.
    pub fn new(grain_id: GrainId, interface_type: GrainType) -> Self {
        Self {
            grain_id,
            interface_type,
            interface_version: 0,
            request_context: HashMap::new(),
        }
    }

    /// Sets the interface version.
    pub fn with_version(mut self, version: u16) -> Self {
        self.interface_version = version;
        self
    }

    /// Adds a request context value.
    pub fn with_context<T: Any + Send + Sync>(mut self, key: impl Into<String>, value: T) -> Self {
        self.request_context.insert(key.into(), Arc::new(value));
        self
    }

    /// Returns the grain identity.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Returns the grain type.
    pub fn grain_type(&self) -> &GrainType {
        self.grain_id.grain_type()
    }

    /// Returns the interface type.
    pub fn interface_type(&self) -> &GrainType {
        &self.interface_type
    }

    /// Returns the interface version.
    pub fn interface_version(&self) -> u16 {
        self.interface_version
    }

    /// Returns true if this target has version information.
    pub fn is_version_aware(&self) -> bool {
        self.interface_version > 0
    }

    /// Gets a request context value by key.
    pub fn get_context<T: Any + Send + Sync>(&self, key: &str) -> Option<&T> {
        self.request_context
            .get(key)
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Returns the grain hash code for consistent hashing.
    pub fn get_hash_code(&self) -> u32 {
        self.grain_id.get_uniform_hash_code()
    }
}

/// Context providing information for placement decisions.
pub trait PlacementContext: Send + Sync {
    /// Returns silos compatible with the target grain type and version.
    fn get_compatible_silos(&self, target: &PlacementTarget) -> Vec<SiloAddress>;

    /// Returns the local silo address.
    fn local_silo(&self) -> &SiloAddress;

    /// Returns the local silo status.
    fn local_silo_status(&self) -> SiloStatus;

    /// Returns statistics for a specific silo.
    fn get_silo_statistics(&self, silo: &SiloAddress) -> Option<SiloRuntimeStatistics>;

    /// Returns all cached silo statistics.
    fn get_all_silo_statistics(&self) -> Vec<(SiloAddress, SiloRuntimeStatistics)>;

    /// Returns the maximum activation count across all silos.
    fn max_activation_count(&self) -> u32;

    /// Returns the maximum available memory across all silos.
    fn max_available_memory(&self) -> u64;
}

/// Simple placement context implementation.
#[derive(Debug)]
pub struct SimplePlacementContext {
    local_silo: SiloAddress,
    local_status: SiloStatus,
    compatible_silos: Vec<SiloAddress>,
    statistics: SiloStatisticsCache,
}

impl SimplePlacementContext {
    /// Creates a new context.
    pub fn new(local_silo: SiloAddress, compatible_silos: Vec<SiloAddress>) -> Self {
        Self {
            local_silo,
            local_status: SiloStatus::Active,
            compatible_silos,
            statistics: SiloStatisticsCache::new(),
        }
    }

    /// Sets the local silo status.
    pub fn with_status(mut self, status: SiloStatus) -> Self {
        self.local_status = status;
        self
    }

    /// Sets statistics for a silo.
    pub fn with_statistics(self, silo: SiloAddress, stats: SiloRuntimeStatistics) -> Self {
        self.statistics.update(silo, stats);
        self
    }

    /// Updates the compatible silos list.
    pub fn set_compatible_silos(&mut self, silos: Vec<SiloAddress>) {
        self.compatible_silos = silos;
    }

    /// Updates statistics for a silo.
    pub fn update_statistics(&self, silo: SiloAddress, stats: SiloRuntimeStatistics) {
        self.statistics.update(silo, stats);
    }
}

impl PlacementContext for SimplePlacementContext {
    fn get_compatible_silos(&self, _target: &PlacementTarget) -> Vec<SiloAddress> {
        self.compatible_silos.clone()
    }

    fn local_silo(&self) -> &SiloAddress {
        &self.local_silo
    }

    fn local_silo_status(&self) -> SiloStatus {
        self.local_status
    }

    fn get_silo_statistics(&self, silo: &SiloAddress) -> Option<SiloRuntimeStatistics> {
        self.statistics.get(silo)
    }

    fn get_all_silo_statistics(&self) -> Vec<(SiloAddress, SiloRuntimeStatistics)> {
        self.statistics.all()
    }

    fn max_activation_count(&self) -> u32 {
        self.statistics.max_activation_count()
    }

    fn max_available_memory(&self) -> u64 {
        self.statistics.max_available_memory()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::IdSpan;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    fn make_target(name: &str) -> PlacementTarget {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        PlacementTarget::new(grain_id, grain_type)
    }

    #[test]
    fn test_silo_status() {
        assert!(SiloStatus::Active.is_active());
        assert!(!SiloStatus::Active.is_terminating());

        assert!(!SiloStatus::ShuttingDown.is_active());
        assert!(SiloStatus::ShuttingDown.is_terminating());

        assert!(!SiloStatus::Stopping.is_active());
        assert!(SiloStatus::Stopping.is_terminating());

        assert!(!SiloStatus::Dead.is_active());
        assert!(SiloStatus::Dead.is_terminating());
    }

    #[test]
    fn test_placement_target_creation() {
        let grain_type = GrainType::create("test.grain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainType::create("test.interface");

        let target = PlacementTarget::new(grain_id.clone(), interface_type.clone());

        assert_eq!(target.grain_id(), &grain_id);
        assert_eq!(target.interface_type(), &interface_type);
        assert_eq!(target.interface_version(), 0);
        assert!(!target.is_version_aware());
    }

    #[test]
    fn test_placement_target_with_version() {
        let target = make_target("test.grain").with_version(2);
        assert_eq!(target.interface_version(), 2);
        assert!(target.is_version_aware());
    }

    #[test]
    fn test_placement_target_with_context() {
        let target = make_target("test.grain")
            .with_context("user_id", "user123".to_string())
            .with_context("priority", 5u32);

        assert_eq!(
            target.get_context::<String>("user_id"),
            Some(&"user123".to_string())
        );
        assert_eq!(target.get_context::<u32>("priority"), Some(&5u32));
        assert!(target.get_context::<String>("missing").is_none());
    }

    #[test]
    fn test_placement_target_hash() {
        let target1 = make_target("test.grain");
        let target2 = make_target("test.grain");
        let target3 = make_target("other.grain");

        // Same grain type should have same hash
        assert_eq!(target1.get_hash_code(), target2.get_hash_code());
        // Different grain types may have different hashes (not guaranteed)
        // but we just test it doesn't panic
        let _ = target3.get_hash_code();
    }

    #[test]
    fn test_simple_context_creation() {
        let local = make_silo(11111);
        let silos = vec![make_silo(11111), make_silo(22222), make_silo(33333)];
        let context = SimplePlacementContext::new(local.clone(), silos.clone());

        assert_eq!(context.local_silo(), &local);
        assert!(context.local_silo_status().is_active());

        let target = make_target("test.grain");
        assert_eq!(context.get_compatible_silos(&target), silos);
    }

    #[test]
    fn test_simple_context_with_status() {
        let local = make_silo(11111);
        let context =
            SimplePlacementContext::new(local, vec![]).with_status(SiloStatus::ShuttingDown);

        assert!(context.local_silo_status().is_terminating());
    }

    #[test]
    fn test_simple_context_with_statistics() {
        let local = make_silo(11111);
        let silo2 = make_silo(22222);
        let stats = SiloRuntimeStatistics::new(silo2.clone()).with_activation_count(100);

        let context = SimplePlacementContext::new(local, vec![silo2.clone()])
            .with_statistics(silo2.clone(), stats);

        let retrieved = context.get_silo_statistics(&silo2).unwrap();
        assert_eq!(retrieved.activation_count(), 100);
    }

    #[test]
    fn test_simple_context_update_statistics() {
        let local = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);

        let context = SimplePlacementContext::new(local, vec![silo2.clone(), silo3.clone()]);

        context.update_statistics(
            silo2.clone(),
            SiloRuntimeStatistics::new(silo2.clone()).with_activation_count(50),
        );
        context.update_statistics(
            silo3.clone(),
            SiloRuntimeStatistics::new(silo3.clone()).with_activation_count(100),
        );

        assert_eq!(context.max_activation_count(), 100);
        assert_eq!(context.get_all_silo_statistics().len(), 2);
    }

    #[test]
    fn test_simple_context_set_compatible_silos() {
        let local = make_silo(11111);
        let mut context = SimplePlacementContext::new(local, vec![]);

        let target = make_target("test.grain");
        assert!(context.get_compatible_silos(&target).is_empty());

        let silos = vec![make_silo(22222), make_silo(33333)];
        context.set_compatible_silos(silos.clone());
        assert_eq!(context.get_compatible_silos(&target), silos);
    }
}
