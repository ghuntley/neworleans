//! Network fault injection.
//!
//! This module provides fault injection capabilities for network-related issues
//! such as latency, packet loss, partitions, and bandwidth throttling.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, info, instrument, warn};

use crate::error::{ChaosError, ChaosResult};
use crate::injector::{
    FaultDescriptor, FaultId, FaultInjector, FaultParameters, FaultTarget, FaultType,
};

/// Configuration for network fault injection.
#[derive(Debug, Clone)]
pub struct NetworkFaultConfig {
    /// Default delay to inject.
    pub default_delay: Duration,
    /// Default packet loss percentage.
    pub default_loss_percent: f64,
    /// Default bandwidth limit (bytes/sec).
    pub default_bandwidth_limit: Option<u64>,
    /// Whether to use real network manipulation (requires privileges).
    pub use_real_network_control: bool,
}

impl Default for NetworkFaultConfig {
    fn default() -> Self {
        Self {
            default_delay: Duration::from_millis(100),
            default_loss_percent: 0.1,
            default_bandwidth_limit: None,
            use_real_network_control: false,
        }
    }
}

impl NetworkFaultConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the default delay.
    pub fn with_default_delay(mut self, delay: Duration) -> Self {
        self.default_delay = delay;
        self
    }

    /// Set the default loss percentage.
    pub fn with_default_loss_percent(mut self, loss: f64) -> Self {
        self.default_loss_percent = loss;
        self
    }

    /// Enable real network control (requires root/admin privileges).
    pub fn with_real_network_control(mut self) -> Self {
        self.use_real_network_control = true;
        self
    }

    /// Create config for testing.
    pub fn for_testing() -> Self {
        Self {
            default_delay: Duration::from_millis(10),
            default_loss_percent: 0.05,
            default_bandwidth_limit: None,
            use_real_network_control: false,
        }
    }
}

/// State of an active network fault.
#[derive(Debug, Clone)]
pub struct NetworkFaultState {
    /// The fault ID.
    pub fault_id: FaultId,
    /// The fault type.
    pub fault_type: FaultType,
    /// Target of the fault.
    pub target: FaultTarget,
    /// Parameters.
    pub parameters: FaultParameters,
    /// Whether the fault is currently active.
    pub is_active: bool,
}

/// Network fault injector.
///
/// This injector handles network-related faults such as delays, packet loss,
/// partitions, and bandwidth throttling.
#[derive(Debug)]
pub struct NetworkFaultInjector {
    /// Configuration.
    config: NetworkFaultConfig,
    /// Active faults.
    active_faults: DashMap<FaultId, NetworkFaultState>,
    /// Partition state (which nodes are isolated from which).
    partitions: Arc<RwLock<HashMap<String, Vec<String>>>>,
}

impl NetworkFaultInjector {
    /// Create a new network fault injector.
    pub fn new(config: NetworkFaultConfig) -> Self {
        info!(
            use_real_network_control = config.use_real_network_control,
            "Creating network fault injector"
        );
        Self {
            config,
            active_faults: DashMap::new(),
            partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(NetworkFaultConfig::default())
    }

    /// Get the current partition state.
    pub fn get_partitions(&self) -> HashMap<String, Vec<String>> {
        self.partitions.read().clone()
    }

    /// Check if two endpoints are partitioned.
    pub fn is_partitioned(&self, source: &str, destination: &str) -> bool {
        let partitions = self.partitions.read();
        if let Some(isolated) = partitions.get(source) {
            if isolated.contains(&destination.to_string()) {
                return true;
            }
        }
        if let Some(isolated) = partitions.get(destination) {
            if isolated.contains(&source.to_string()) {
                return true;
            }
        }
        false
    }

    /// Get delay to inject for a connection.
    pub fn get_delay(&self, source: &str, destination: &str) -> Option<Duration> {
        for entry in self.active_faults.iter() {
            let state = entry.value();
            if !state.is_active {
                continue;
            }
            if state.fault_type != FaultType::NetworkDelay {
                continue;
            }
            if self.target_matches(&state.target, source, destination) {
                return state.parameters.delay.or(Some(self.config.default_delay));
            }
        }
        None
    }

    /// Get packet loss rate for a connection.
    pub fn get_loss_rate(&self, source: &str, destination: &str) -> Option<f64> {
        for entry in self.active_faults.iter() {
            let state = entry.value();
            if !state.is_active {
                continue;
            }
            if state.fault_type != FaultType::NetworkPacketLoss {
                continue;
            }
            if self.target_matches(&state.target, source, destination) {
                return state
                    .parameters
                    .loss_percent
                    .or(Some(self.config.default_loss_percent));
            }
        }
        None
    }

    /// Get bandwidth limit for a connection.
    pub fn get_bandwidth_limit(&self, source: &str, destination: &str) -> Option<u64> {
        for entry in self.active_faults.iter() {
            let state = entry.value();
            if !state.is_active {
                continue;
            }
            if state.fault_type != FaultType::NetworkBandwidthThrottle {
                continue;
            }
            if self.target_matches(&state.target, source, destination) {
                return state.parameters.bandwidth_limit;
            }
        }
        None
    }

    /// Check if a target matches a source/destination pair.
    fn target_matches(&self, target: &FaultTarget, source: &str, destination: &str) -> bool {
        match target {
            FaultTarget::AllSilos => true,
            FaultTarget::Silo(addr) => addr == source || addr == destination,
            FaultTarget::Connection { source: s, destination: d } => {
                (s == source && d == destination) || (s == destination && d == source)
            }
            _ => false,
        }
    }

    /// Inject a delay fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_delay(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let delay = descriptor
            .parameters
            .delay
            .unwrap_or(self.config.default_delay);

        info!(
            target = %descriptor.target,
            delay_ms = delay.as_millis(),
            "Injecting network delay fault"
        );

        let state = NetworkFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::NetworkDelay,
            target: descriptor.target.clone(),
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a packet loss fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_packet_loss(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let loss = descriptor
            .parameters
            .loss_percent
            .unwrap_or(self.config.default_loss_percent);

        info!(
            target = %descriptor.target,
            loss_percent = loss,
            "Injecting packet loss fault"
        );

        let state = NetworkFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::NetworkPacketLoss,
            target: descriptor.target.clone(),
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a network partition.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_partition(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        info!(
            target = %descriptor.target,
            "Injecting network partition fault"
        );

        // Extract partition details from target
        match &descriptor.target {
            FaultTarget::Connection { source, destination } => {
                let mut partitions = self.partitions.write();
                partitions
                    .entry(source.clone())
                    .or_default()
                    .push(destination.clone());
            }
            FaultTarget::Silo(addr) => {
                // Partition this silo from all others
                let mut partitions = self.partitions.write();
                partitions.entry(addr.clone()).or_default();
            }
            _ => {
                return Err(ChaosError::invalid_schedule(
                    "Network partition requires Silo or Connection target",
                ));
            }
        }

        let state = NetworkFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::NetworkPartition,
            target: descriptor.target.clone(),
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a bandwidth throttle fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_bandwidth_throttle(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let limit = descriptor.parameters.bandwidth_limit.ok_or_else(|| {
            ChaosError::invalid_schedule("Bandwidth throttle requires bandwidth_limit parameter")
        })?;

        info!(
            target = %descriptor.target,
            bandwidth_limit = limit,
            "Injecting bandwidth throttle fault"
        );

        let state = NetworkFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::NetworkBandwidthThrottle,
            target: descriptor.target.clone(),
            parameters: descriptor.parameters.clone(),
            is_active: true,
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Heal a network partition using the provided target.
    fn heal_partition_with_target(&self, target: &FaultTarget) {
        match target {
            FaultTarget::Connection { source, destination } => {
                let mut partitions = self.partitions.write();
                if let Some(isolated) = partitions.get_mut(source) {
                    isolated.retain(|d| d != destination);
                }
            }
            FaultTarget::Silo(addr) => {
                let mut partitions = self.partitions.write();
                partitions.remove(addr);
            }
            _ => {}
        }
    }
}

#[async_trait]
impl FaultInjector for NetworkFaultInjector {
    fn name(&self) -> &str {
        "NetworkFaultInjector"
    }

    fn supported_fault_types(&self) -> Vec<FaultType> {
        vec![
            FaultType::NetworkDelay,
            FaultType::NetworkPacketLoss,
            FaultType::NetworkPartition,
            FaultType::NetworkBandwidthThrottle,
        ]
    }

    #[instrument(skip(self), fields(fault_id = %descriptor.id, fault_type = %descriptor.fault_type))]
    async fn inject(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        debug!("Injecting network fault");

        match descriptor.fault_type {
            FaultType::NetworkDelay => self.inject_delay(descriptor).await,
            FaultType::NetworkPacketLoss => self.inject_packet_loss(descriptor).await,
            FaultType::NetworkPartition => self.inject_partition(descriptor).await,
            FaultType::NetworkBandwidthThrottle => self.inject_bandwidth_throttle(descriptor).await,
            _ => Err(ChaosError::injection_failed(
                format!("Unsupported fault type: {}", descriptor.fault_type),
                descriptor.fault_type.to_string(),
            )),
        }
    }

    #[instrument(skip(self), fields(fault_id = %fault_id))]
    async fn heal(&self, fault_id: &FaultId) -> ChaosResult<()> {
        // First, get the information we need and mark as inactive
        let partition_target = {
            if let Some(mut state) = self.active_faults.get_mut(fault_id) {
                info!(fault_type = %state.fault_type, "Healing network fault");
                state.is_active = false;

                // Extract partition target if this is a partition fault
                if state.fault_type == FaultType::NetworkPartition {
                    Some(state.target.clone())
                } else {
                    None
                }
            } else {
                warn!("Attempted to heal non-existent fault");
                return Err(ChaosError::FaultNotFound {
                    fault_id: fault_id.to_string(),
                });
            }
        };

        // Now heal the partition outside the lock
        if let Some(target) = partition_target {
            self.heal_partition_with_target(&target);
        }

        Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::injector::FaultSchedule;

    fn create_test_injector() -> NetworkFaultInjector {
        NetworkFaultInjector::new(NetworkFaultConfig::for_testing())
    }

    #[test]
    fn test_config_defaults() {
        let config = NetworkFaultConfig::default();
        assert_eq!(config.default_delay, Duration::from_millis(100));
        assert_eq!(config.default_loss_percent, 0.1);
        assert!(!config.use_real_network_control);
    }

    #[test]
    fn test_config_builder() {
        let config = NetworkFaultConfig::new()
            .with_default_delay(Duration::from_millis(500))
            .with_default_loss_percent(0.25)
            .with_real_network_control();

        assert_eq!(config.default_delay, Duration::from_millis(500));
        assert_eq!(config.default_loss_percent, 0.25);
        assert!(config.use_real_network_control);
    }

    #[test]
    fn test_supported_fault_types() {
        let injector = create_test_injector();
        let types = injector.supported_fault_types();
        assert!(types.contains(&FaultType::NetworkDelay));
        assert!(types.contains(&FaultType::NetworkPacketLoss));
        assert!(types.contains(&FaultType::NetworkPartition));
        assert!(types.contains(&FaultType::NetworkBandwidthThrottle));
    }

    #[test]
    fn test_can_handle() {
        let injector = create_test_injector();
        assert!(injector.can_handle(&FaultType::NetworkDelay));
        assert!(injector.can_handle(&FaultType::NetworkPacketLoss));
        assert!(!injector.can_handle(&FaultType::ProcessKill));
    }

    #[tokio::test]
    async fn test_inject_delay() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "delay-test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(50)),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);

        let delay = injector.get_delay("silo1", "silo2");
        assert_eq!(delay, Some(Duration::from_millis(50)));
    }

    #[tokio::test]
    async fn test_inject_packet_loss() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "loss-test",
            FaultType::NetworkPacketLoss,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_loss_percent(0.15),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);

        let loss = injector.get_loss_rate("silo1", "silo2");
        assert_eq!(loss, Some(0.15));
    }

    #[tokio::test]
    async fn test_inject_partition() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "partition-test",
            FaultType::NetworkPartition,
            FaultTarget::Connection {
                source: "silo1".to_string(),
                destination: "silo2".to_string(),
            },
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert!(injector.is_partitioned("silo1", "silo2"));
        assert!(!injector.is_partitioned("silo1", "silo3"));
    }

    #[tokio::test]
    async fn test_inject_bandwidth_throttle() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "bandwidth-test",
            FaultType::NetworkBandwidthThrottle,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_bandwidth_limit(1024 * 100), // 100KB/s
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);

        let limit = injector.get_bandwidth_limit("silo1", "silo2");
        assert_eq!(limit, Some(1024 * 100));
    }

    #[tokio::test]
    async fn test_heal_fault() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "heal-test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);

        injector.heal(&descriptor.id).await.unwrap();
        assert!(!injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_heal_partition() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "partition-heal-test",
            FaultType::NetworkPartition,
            FaultTarget::Connection {
                source: "silo1".to_string(),
                destination: "silo2".to_string(),
            },
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_partitioned("silo1", "silo2"));

        injector.heal(&descriptor.id).await.unwrap();
        assert!(!injector.is_partitioned("silo1", "silo2"));
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
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let d2 = FaultDescriptor::new(
            "fault2",
            FaultType::NetworkPacketLoss,
            FaultTarget::AllSilos,
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
    async fn test_target_specific_delay() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "target-delay",
            FaultType::NetworkDelay,
            FaultTarget::Connection {
                source: "silo1".to_string(),
                destination: "silo2".to_string(),
            },
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(200)),
        );

        injector.inject(&descriptor).await.unwrap();

        // Should affect the specific connection
        assert_eq!(
            injector.get_delay("silo1", "silo2"),
            Some(Duration::from_millis(200))
        );
        // Should also work in reverse direction
        assert_eq!(
            injector.get_delay("silo2", "silo1"),
            Some(Duration::from_millis(200))
        );
        // Should not affect other connections
        assert_eq!(injector.get_delay("silo1", "silo3"), None);
    }

    #[tokio::test]
    async fn test_unsupported_fault_type() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "unsupported",
            FaultType::ProcessKill,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let result = injector.inject(&descriptor).await;
        assert!(result.is_err());
    }
}
