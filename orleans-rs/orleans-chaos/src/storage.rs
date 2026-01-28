//! Storage fault injection.
//!
//! This module provides fault injection capabilities for storage-related issues
//! such as read/write failures, latency injection, and data corruption.

use std::collections::HashSet;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, info, instrument, warn};

use crate::error::{ChaosError, ChaosResult};
use crate::injector::{
    FaultDescriptor, FaultId, FaultInjector, FaultParameters, FaultTarget, FaultType,
};

/// Configuration for storage fault injection.
#[derive(Debug, Clone)]
pub struct StorageFaultConfig {
    /// Default latency to inject.
    pub default_latency: Duration,
    /// Default failure probability.
    pub default_failure_probability: f64,
    /// Default corruption bit flip count.
    pub default_corruption_bits: u32,
}

impl Default for StorageFaultConfig {
    fn default() -> Self {
        Self {
            default_latency: Duration::from_millis(100),
            default_failure_probability: 1.0, // Always fail when active
            default_corruption_bits: 1,
        }
    }
}

impl StorageFaultConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default latency.
    pub fn with_default_latency(mut self, latency: Duration) -> Self {
        self.default_latency = latency;
        self
    }

    /// Set the default failure probability.
    pub fn with_default_failure_probability(mut self, prob: f64) -> Self {
        self.default_failure_probability = prob;
        self
    }

    /// Set the default corruption bits.
    pub fn with_default_corruption_bits(mut self, bits: u32) -> Self {
        self.default_corruption_bits = bits;
        self
    }

    /// Create config for testing.
    pub fn for_testing() -> Self {
        Self {
            default_latency: Duration::from_millis(10),
            default_failure_probability: 1.0,
            default_corruption_bits: 1,
        }
    }
}

/// Type of storage operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageOperation {
    /// Read operation.
    Read,
    /// Write operation.
    Write,
    /// Delete operation.
    Delete,
    /// List operation.
    List,
}

impl std::fmt::Display for StorageOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageOperation::Read => write!(f, "Read"),
            StorageOperation::Write => write!(f, "Write"),
            StorageOperation::Delete => write!(f, "Delete"),
            StorageOperation::List => write!(f, "List"),
        }
    }
}

/// State of an active storage fault.
#[derive(Debug, Clone)]
pub struct StorageFaultState {
    /// The fault ID.
    pub fault_id: FaultId,
    /// The fault type.
    pub fault_type: FaultType,
    /// Target grain ID pattern (or None for all).
    pub target_grain: Option<String>,
    /// Affected operations.
    pub affected_operations: HashSet<StorageOperation>,
    /// Parameters.
    pub parameters: FaultParameters,
    /// Whether the fault is currently active.
    pub is_active: bool,
}

/// Storage fault injector.
///
/// This injector handles storage-related faults such as read/write failures,
/// latency injection, and data corruption.
#[derive(Debug)]
pub struct StorageFaultInjector {
    /// Configuration.
    config: StorageFaultConfig,
    /// Active faults.
    active_faults: DashMap<FaultId, StorageFaultState>,
    /// Grains with read failures.
    read_failures: DashMap<String, f64>,
    /// Grains with write failures.
    write_failures: DashMap<String, f64>,
    /// Grains with latency injection.
    latency_injection: DashMap<String, Duration>,
    /// Grains with corruption injection.
    corruption_injection: DashMap<String, u32>,
}

impl StorageFaultInjector {
    /// Create a new storage fault injector.
    pub fn new(config: StorageFaultConfig) -> Self {
        info!("Creating storage fault injector");
        Self {
            config,
            active_faults: DashMap::new(),
            read_failures: DashMap::new(),
            write_failures: DashMap::new(),
            latency_injection: DashMap::new(),
            corruption_injection: DashMap::new(),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(StorageFaultConfig::default())
    }

    /// Check if read operations should fail for a grain.
    pub fn should_fail_read(&self, grain_id: &str) -> Option<f64> {
        // Check specific grain first
        if let Some(prob) = self.read_failures.get(grain_id) {
            return Some(*prob);
        }
        // Check wildcard
        if let Some(prob) = self.read_failures.get("*") {
            return Some(*prob);
        }
        None
    }

    /// Check if write operations should fail for a grain.
    pub fn should_fail_write(&self, grain_id: &str) -> Option<f64> {
        // Check specific grain first
        if let Some(prob) = self.write_failures.get(grain_id) {
            return Some(*prob);
        }
        // Check wildcard
        if let Some(prob) = self.write_failures.get("*") {
            return Some(*prob);
        }
        None
    }

    /// Get latency to inject for a grain.
    pub fn get_latency(&self, grain_id: &str) -> Option<Duration> {
        // Check specific grain first
        if let Some(latency) = self.latency_injection.get(grain_id) {
            return Some(*latency);
        }
        // Check wildcard
        if let Some(latency) = self.latency_injection.get("*") {
            return Some(*latency);
        }
        None
    }

    /// Check if data should be corrupted for a grain.
    pub fn should_corrupt(&self, grain_id: &str) -> Option<u32> {
        // Check specific grain first
        if let Some(bits) = self.corruption_injection.get(grain_id) {
            return Some(*bits);
        }
        // Check wildcard
        if let Some(bits) = self.corruption_injection.get("*") {
            return Some(*bits);
        }
        None
    }

    /// Resolve the target grain ID from a fault target.
    fn resolve_target_grain(&self, target: &FaultTarget) -> String {
        match target {
            FaultTarget::GrainStorage(grain_id) => grain_id.clone(),
            FaultTarget::AllSilos => "*".to_string(),
            _ => "*".to_string(),
        }
    }

    /// Inject a storage read failure.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_read_failure(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let grain_id = self.resolve_target_grain(&descriptor.target);
        let probability = descriptor
            .schedule
            .probability
            .min(1.0)
            .max(0.0);

        info!(
            grain_id = %grain_id,
            probability = probability,
            "Injecting storage read failure fault"
        );

        self.read_failures.insert(grain_id.clone(), probability);

        let mut affected = HashSet::new();
        affected.insert(StorageOperation::Read);

        let state = StorageFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::StorageReadFailure,
            target_grain: Some(grain_id),
            affected_operations: affected,
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a storage write failure.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_write_failure(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let grain_id = self.resolve_target_grain(&descriptor.target);
        let probability = descriptor
            .schedule
            .probability
            .min(1.0)
            .max(0.0);

        info!(
            grain_id = %grain_id,
            probability = probability,
            "Injecting storage write failure fault"
        );

        self.write_failures.insert(grain_id.clone(), probability);

        let mut affected = HashSet::new();
        affected.insert(StorageOperation::Write);

        let state = StorageFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::StorageWriteFailure,
            target_grain: Some(grain_id),
            affected_operations: affected,
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject storage latency.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_latency(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let grain_id = self.resolve_target_grain(&descriptor.target);
        let latency = descriptor
            .parameters
            .delay
            .unwrap_or(self.config.default_latency);

        info!(
            grain_id = %grain_id,
            latency_ms = latency.as_millis(),
            "Injecting storage latency fault"
        );

        self.latency_injection.insert(grain_id.clone(), latency);

        let mut affected = HashSet::new();
        affected.insert(StorageOperation::Read);
        affected.insert(StorageOperation::Write);

        let state = StorageFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::StorageLatency,
            target_grain: Some(grain_id),
            affected_operations: affected,
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject storage corruption.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_corruption(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let grain_id = self.resolve_target_grain(&descriptor.target);
        let bits = self.config.default_corruption_bits;

        info!(
            grain_id = %grain_id,
            corruption_bits = bits,
            "Injecting storage corruption fault"
        );

        self.corruption_injection.insert(grain_id.clone(), bits);

        let mut affected = HashSet::new();
        affected.insert(StorageOperation::Read);

        let state = StorageFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::StorageCorruption,
            target_grain: Some(grain_id),
            affected_operations: affected,
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }
}

#[async_trait]
impl FaultInjector for StorageFaultInjector {
    fn name(&self) -> &str {
        "StorageFaultInjector"
    }

    fn supported_fault_types(&self) -> Vec<FaultType> {
        vec![
            FaultType::StorageReadFailure,
            FaultType::StorageWriteFailure,
            FaultType::StorageLatency,
            FaultType::StorageCorruption,
        ]
    }

    #[instrument(skip(self), fields(fault_id = %descriptor.id, fault_type = %descriptor.fault_type))]
    async fn inject(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        debug!("Injecting storage fault");

        match descriptor.fault_type {
            FaultType::StorageReadFailure => self.inject_read_failure(descriptor).await,
            FaultType::StorageWriteFailure => self.inject_write_failure(descriptor).await,
            FaultType::StorageLatency => self.inject_latency(descriptor).await,
            FaultType::StorageCorruption => self.inject_corruption(descriptor).await,
            _ => Err(ChaosError::injection_failed(
                format!("Unsupported fault type: {}", descriptor.fault_type),
                descriptor.fault_type.to_string(),
            )),
        }
    }

    #[instrument(skip(self), fields(fault_id = %fault_id))]
    async fn heal(&self, fault_id: &FaultId) -> ChaosResult<()> {
        if let Some(mut state) = self.active_faults.get_mut(fault_id) {
            info!(fault_type = %state.fault_type, "Healing storage fault");

            // Remove from the appropriate tracking map
            if let Some(grain_id) = &state.target_grain {
                match state.fault_type {
                    FaultType::StorageReadFailure => {
                        self.read_failures.remove(grain_id);
                    }
                    FaultType::StorageWriteFailure => {
                        self.write_failures.remove(grain_id);
                    }
                    FaultType::StorageLatency => {
                        self.latency_injection.remove(grain_id);
                    }
                    FaultType::StorageCorruption => {
                        self.corruption_injection.remove(grain_id);
                    }
                    _ => {}
                }
            }

            state.is_active = false;
            Ok(())
        } else {
            warn!("Attempted to heal non-existent fault");
            Err(ChaosError::FaultNotFound {
                fault_id: fault_id.to_string(),
            })
        }
    }

    async fn is_active(&self, fault_id: &FaultId) -> bool {
        self.active_faults
            .get(fault_id)
            .map(|s| s.is_active)
            .unwrap_or(false)
    }

    async fn active_faults(&self) -> Vec<FaultId> {
        self.active_faults
            .iter()
            .filter(|e| e.is_active)
            .map(|e| e.fault_id.clone())
            .collect()
    }
}

/// Corrupt data by flipping random bits.
pub fn corrupt_data(data: &mut [u8], bits_to_flip: u32) {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let len = data.len();
    if len == 0 {
        return;
    }

    for _ in 0..bits_to_flip {
        let byte_idx = rng.gen_range(0..len);
        let bit_idx = rng.gen_range(0..8);
        data[byte_idx] ^= 1 << bit_idx;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::injector::FaultSchedule;

    fn create_test_injector() -> StorageFaultInjector {
        StorageFaultInjector::new(StorageFaultConfig::for_testing())
    }

    #[test]
    fn test_config_defaults() {
        let config = StorageFaultConfig::default();
        assert_eq!(config.default_latency, Duration::from_millis(100));
        assert_eq!(config.default_failure_probability, 1.0);
        assert_eq!(config.default_corruption_bits, 1);
    }

    #[test]
    fn test_config_builder() {
        let config = StorageFaultConfig::new()
            .with_default_latency(Duration::from_millis(500))
            .with_default_failure_probability(0.5)
            .with_default_corruption_bits(3);

        assert_eq!(config.default_latency, Duration::from_millis(500));
        assert_eq!(config.default_failure_probability, 0.5);
        assert_eq!(config.default_corruption_bits, 3);
    }

    #[test]
    fn test_supported_fault_types() {
        let injector = create_test_injector();
        let types = injector.supported_fault_types();
        assert!(types.contains(&FaultType::StorageReadFailure));
        assert!(types.contains(&FaultType::StorageWriteFailure));
        assert!(types.contains(&FaultType::StorageLatency));
        assert!(types.contains(&FaultType::StorageCorruption));
    }

    #[test]
    fn test_can_handle() {
        let injector = create_test_injector();
        assert!(injector.can_handle(&FaultType::StorageReadFailure));
        assert!(injector.can_handle(&FaultType::StorageWriteFailure));
        assert!(!injector.can_handle(&FaultType::NetworkDelay));
    }

    #[test]
    fn test_storage_operation_display() {
        assert_eq!(StorageOperation::Read.to_string(), "Read");
        assert_eq!(StorageOperation::Write.to_string(), "Write");
        assert_eq!(StorageOperation::Delete.to_string(), "Delete");
        assert_eq!(StorageOperation::List.to_string(), "List");
    }

    #[tokio::test]
    async fn test_inject_read_failure() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "read-failure-test",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None).with_probability(0.8),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert_eq!(injector.should_fail_read("grain-123"), Some(0.8));
        assert_eq!(injector.should_fail_read("grain-456"), None);
    }

    #[tokio::test]
    async fn test_inject_write_failure() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "write-failure-test",
            FaultType::StorageWriteFailure,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert!(injector.should_fail_write("grain-123").is_some());
    }

    #[tokio::test]
    async fn test_inject_latency() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "latency-test",
            FaultType::StorageLatency,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(200)),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert_eq!(
            injector.get_latency("grain-123"),
            Some(Duration::from_millis(200))
        );
    }

    #[tokio::test]
    async fn test_inject_corruption() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "corruption-test",
            FaultType::StorageCorruption,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert!(injector.should_corrupt("grain-123").is_some());
    }

    #[tokio::test]
    async fn test_wildcard_read_failure() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "wildcard-read-test",
            FaultType::StorageReadFailure,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        // Should affect all grains
        assert!(injector.should_fail_read("any-grain-id").is_some());
        assert!(injector.should_fail_read("another-grain").is_some());
    }

    #[tokio::test]
    async fn test_heal_read_failure() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "heal-read-test",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.should_fail_read("grain-123").is_some());

        injector.heal(&descriptor.id).await.unwrap();
        assert!(!injector.is_active(&descriptor.id).await);
        assert!(injector.should_fail_read("grain-123").is_none());
    }

    #[tokio::test]
    async fn test_heal_latency() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "heal-latency-test",
            FaultType::StorageLatency,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.get_latency("grain-123").is_some());

        injector.heal(&descriptor.id).await.unwrap();
        assert!(injector.get_latency("grain-123").is_none());
    }

    #[tokio::test]
    async fn test_heal_nonexistent_fault() {
        let injector = create_test_injector();
        let result = injector.heal(&FaultId::from_str("nonexistent")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_active_faults() {
        let injector = create_test_injector();

        let d1 = FaultDescriptor::new(
            "fault1",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-1".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let d2 = FaultDescriptor::new(
            "fault2",
            FaultType::StorageWriteFailure,
            FaultTarget::GrainStorage("grain-2".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&d1).await.unwrap();
        injector.inject(&d2).await.unwrap();

        let active = injector.active_faults().await;
        assert_eq!(active.len(), 2);

        injector.heal(&d1.id).await.unwrap();
        let active = injector.active_faults().await;
        assert_eq!(active.len(), 1);
    }

    #[tokio::test]
    async fn test_unsupported_fault_type() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "unsupported",
            FaultType::NetworkDelay,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let result = injector.inject(&descriptor).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_corrupt_data() {
        let original = vec![0u8; 100];
        let mut data = original.clone();
        corrupt_data(&mut data, 5);

        // Data should be different (with very high probability for 100 bytes and 5 bits)
        assert_ne!(data, original);
    }

    #[test]
    fn test_corrupt_empty_data() {
        let mut data: Vec<u8> = vec![];
        corrupt_data(&mut data, 5);
        assert!(data.is_empty());
    }
}
