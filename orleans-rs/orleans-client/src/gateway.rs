//! Gateway management for connecting to Orleans cluster silos.
//!
//! The gateway manager maintains connections to available silos in the cluster
//! and provides load balancing for outgoing requests.

use crate::error::{ClientError, ClientResult};
use crate::options::GatewayOptions;
use dashmap::DashMap;
use orleans_clustering::IMembershipTable;
use orleans_core::SiloAddress;
use orleans_messaging::{ConnectionManager, Message};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};

/// Status of a gateway connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayStatus {
    /// Gateway is healthy and accepting requests.
    Healthy,
    /// Gateway has had some failures but is still usable.
    Degraded,
    /// Gateway is unhealthy and should not be used.
    Unhealthy,
    /// Gateway is being tested for recovery.
    Recovering,
}

/// Information about a gateway.
#[derive(Debug)]
pub struct GatewayInfo {
    /// The silo address of this gateway.
    pub silo_address: SiloAddress,

    /// Current status.
    pub status: GatewayStatus,

    /// Number of consecutive failures.
    pub failure_count: u32,

    /// Last time the gateway was used successfully.
    pub last_success: Option<Instant>,

    /// Last time the gateway failed.
    pub last_failure: Option<Instant>,

    /// Number of requests sent through this gateway.
    pub request_count: AtomicUsize,
}

impl GatewayInfo {
    /// Create new gateway info.
    pub fn new(silo_address: SiloAddress) -> Self {
        Self {
            silo_address,
            status: GatewayStatus::Healthy,
            failure_count: 0,
            last_success: None,
            last_failure: None,
            request_count: AtomicUsize::new(0),
        }
    }

    /// Record a successful request.
    pub fn record_success(&mut self) {
        self.failure_count = 0;
        self.last_success = Some(Instant::now());
        self.status = GatewayStatus::Healthy;
        self.request_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed request.
    pub fn record_failure(&mut self, failure_threshold: u32) {
        self.failure_count += 1;
        self.last_failure = Some(Instant::now());

        if self.failure_count >= failure_threshold {
            self.status = GatewayStatus::Unhealthy;
        } else {
            self.status = GatewayStatus::Degraded;
        }
    }

    /// Check if this gateway is usable.
    pub fn is_usable(&self) -> bool {
        matches!(
            self.status,
            GatewayStatus::Healthy | GatewayStatus::Degraded
        )
    }

    /// Get the request count.
    pub fn request_count(&self) -> usize {
        self.request_count.load(Ordering::Relaxed)
    }
}

/// Manages connections to gateway silos.
pub struct GatewayManager {
    /// Connection manager for TCP connections.
    connection_manager: Arc<ConnectionManager>,

    /// Known gateways with their status.
    gateways: DashMap<SiloAddress, GatewayInfo>,

    /// Options for gateway management.
    options: GatewayOptions,

    /// Round-robin counter for load balancing.
    round_robin_counter: AtomicUsize,

    /// Preferred gateway index (-1 for round-robin).
    preferred_gateway_index: i32,

    /// Shutdown signal.
    shutdown: CancellationToken,

    /// Membership table for discovering gateways.
    membership_table: Option<Arc<dyn IMembershipTable>>,
}

impl GatewayManager {
    /// Create a new gateway manager.
    pub fn new(
        connection_manager: Arc<ConnectionManager>,
        options: GatewayOptions,
    ) -> Self {
        Self {
            connection_manager,
            gateways: DashMap::new(),
            options,
            round_robin_counter: AtomicUsize::new(0),
            preferred_gateway_index: -1,
            shutdown: CancellationToken::new(),
            membership_table: None,
        }
    }

    /// Create a gateway manager with a membership table for auto-discovery.
    pub fn with_membership_table(
        connection_manager: Arc<ConnectionManager>,
        options: GatewayOptions,
        membership_table: Arc<dyn IMembershipTable>,
    ) -> Self {
        let mut manager = Self::new(connection_manager, options);
        manager.membership_table = Some(membership_table);
        manager
    }

    /// Set the preferred gateway index.
    pub fn set_preferred_gateway(&mut self, index: i32) {
        self.preferred_gateway_index = index;
    }

    /// Add initial gateway endpoints.
    pub fn add_gateway(&self, silo_address: SiloAddress) {
        if !self.gateways.contains_key(&silo_address) {
            debug!(silo_address = %silo_address, "adding gateway");
            self.gateways.insert(silo_address.clone(), GatewayInfo::new(silo_address));
        }
    }

    /// Get the number of available gateways.
    pub fn gateway_count(&self) -> usize {
        self.gateways.len()
    }

    /// Get the number of healthy gateways.
    pub fn healthy_gateway_count(&self) -> usize {
        self.gateways
            .iter()
            .filter(|e| e.value().status == GatewayStatus::Healthy)
            .count()
    }

    /// Get all gateway addresses.
    pub fn get_gateway_addresses(&self) -> Vec<SiloAddress> {
        self.gateways.iter().map(|e| e.key().clone()).collect()
    }

    /// Select a gateway for sending a request.
    #[instrument(skip(self))]
    pub fn select_gateway(&self) -> ClientResult<SiloAddress> {
        // Collect usable gateways
        let usable: Vec<_> = self
            .gateways
            .iter()
            .filter(|e| e.value().is_usable())
            .map(|e| e.key().clone())
            .collect();

        if usable.is_empty() {
            return Err(ClientError::NoGatewaysAvailable);
        }

        // Use preferred gateway if set and valid
        if self.preferred_gateway_index >= 0 {
            let index = self.preferred_gateway_index as usize;
            if index < usable.len() {
                let gateway = usable[index].clone();
                trace!(gateway = %gateway, "selected preferred gateway");
                return Ok(gateway);
            }
        }

        // Round-robin selection
        let index = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) % usable.len();
        let gateway = usable[index].clone();
        trace!(gateway = %gateway, index = index, "selected gateway via round-robin");
        Ok(gateway)
    }

    /// Send a message through a gateway.
    #[instrument(skip(self, message), fields(correlation_id = %message.id()))]
    pub async fn send(&self, message: Message) -> ClientResult<()> {
        let gateway = self.select_gateway()?;

        trace!(gateway = %gateway, "sending message through gateway");

        // Update request count
        if let Some(info) = self.gateways.get_mut(&gateway) {
            info.request_count.fetch_add(1, Ordering::Relaxed);
        }

        // Get connection and send message
        let connection = self.connection_manager
            .get_connection(&gateway)
            .await
            .map_err(|e| {
                self.record_failure(&gateway);
                ClientError::GatewayConnectionFailed(e.to_string())
            })?;

        connection
            .send(message)
            .await
            .map_err(|e| {
                // Record failure
                self.record_failure(&gateway);
                ClientError::Network(e.to_string())
            })
    }

    /// Record a successful response from a gateway.
    #[instrument(skip(self))]
    pub fn record_success(&self, gateway: &SiloAddress) {
        if let Some(mut info) = self.gateways.get_mut(gateway) {
            info.record_success();
            trace!(gateway = %gateway, "recorded gateway success");
        }
    }

    /// Record a failure for a gateway.
    #[instrument(skip(self))]
    pub fn record_failure(&self, gateway: &SiloAddress) {
        if let Some(mut info) = self.gateways.get_mut(gateway) {
            info.record_failure(self.options.failure_threshold);
            warn!(
                gateway = %gateway,
                failure_count = info.failure_count,
                status = ?info.status,
                "recorded gateway failure"
            );
        }
    }

    /// Refresh the gateway list from the membership table.
    #[instrument(skip(self))]
    pub async fn refresh_gateways(&self) -> ClientResult<()> {
        let Some(membership_table) = &self.membership_table else {
            return Ok(());
        };

        debug!("refreshing gateway list from membership table");

        let table_data = membership_table
            .read_all()
            .await
            .map_err(|e| ClientError::Membership(e))?;

        let mut new_gateways = 0;
        let mut removed_gateways = 0;

        // Add new active gateways
        for entry in table_data.all_entries() {
            if entry.status.is_active() {
                let silo_address = entry.silo_address.clone();
                if !self.gateways.contains_key(&silo_address) {
                    self.add_gateway(silo_address);
                    new_gateways += 1;
                }
            }
        }

        // Remove gateways that are no longer active
        let active_addresses: std::collections::HashSet<_> = table_data
            .all_entries()
            .into_iter()
            .filter(|e| e.status.is_active())
            .map(|e| e.silo_address.clone())
            .collect();

        let to_remove: Vec<_> = self
            .gateways
            .iter()
            .filter(|e| !active_addresses.contains(e.key()))
            .map(|e| e.key().clone())
            .collect();

        for address in to_remove {
            self.gateways.remove(&address);
            removed_gateways += 1;
        }

        info!(
            new_gateways = new_gateways,
            removed_gateways = removed_gateways,
            total_gateways = self.gateways.len(),
            "refreshed gateway list"
        );

        Ok(())
    }

    /// Start the gateway manager background tasks.
    #[instrument(skip(self))]
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let refresh_interval = self.options.refresh_period;
        let recovery_period = self.options.recovery_period;
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut refresh_timer = tokio::time::interval(refresh_interval);
            let mut recovery_timer = tokio::time::interval(recovery_period);

            loop {
                tokio::select! {
                    _ = refresh_timer.tick() => {
                        // Gateway refresh would happen here if we had membership table access
                        trace!("gateway refresh tick");
                    }
                    _ = recovery_timer.tick() => {
                        // Recovery of unhealthy gateways is handled elsewhere
                        trace!("gateway recovery tick");
                    }
                    _ = shutdown.cancelled() => {
                        debug!("gateway manager shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Stop the gateway manager.
    pub fn stop(&self) {
        self.shutdown.cancel();
    }

    /// Get the cancellation token for shutdown coordination.
    pub fn shutdown_token(&self) -> CancellationToken {
        self.shutdown.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
            1,
        )
    }

    #[test]
    fn test_gateway_info_creation() {
        let silo = test_silo_address(11111);
        let info = GatewayInfo::new(silo.clone());

        assert_eq!(info.status, GatewayStatus::Healthy);
        assert_eq!(info.failure_count, 0);
        assert!(info.is_usable());
    }

    #[test]
    fn test_gateway_info_success() {
        let silo = test_silo_address(11111);
        let mut info = GatewayInfo::new(silo);

        info.record_success();

        assert_eq!(info.status, GatewayStatus::Healthy);
        assert!(info.last_success.is_some());
        assert_eq!(info.request_count(), 1);
    }

    #[test]
    fn test_gateway_info_failure_degraded() {
        let silo = test_silo_address(11111);
        let mut info = GatewayInfo::new(silo);

        info.record_failure(3); // threshold is 3

        assert_eq!(info.status, GatewayStatus::Degraded);
        assert_eq!(info.failure_count, 1);
        assert!(info.is_usable()); // Still usable when degraded
    }

    #[test]
    fn test_gateway_info_failure_unhealthy() {
        let silo = test_silo_address(11111);
        let mut info = GatewayInfo::new(silo);

        info.record_failure(3);
        info.record_failure(3);
        info.record_failure(3);

        assert_eq!(info.status, GatewayStatus::Unhealthy);
        assert_eq!(info.failure_count, 3);
        assert!(!info.is_usable());
    }

    #[tokio::test]
    async fn test_gateway_manager_creation() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        assert_eq!(manager.gateway_count(), 0);
        assert_eq!(manager.healthy_gateway_count(), 0);
    }

    #[tokio::test]
    async fn test_gateway_manager_add_gateway() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        manager.add_gateway(test_silo_address(11111));
        manager.add_gateway(test_silo_address(22222));

        assert_eq!(manager.gateway_count(), 2);
        assert_eq!(manager.healthy_gateway_count(), 2);
    }

    #[tokio::test]
    async fn test_gateway_manager_select_round_robin() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        manager.add_gateway(test_silo_address(11111));
        manager.add_gateway(test_silo_address(22222));

        let g1 = manager.select_gateway().unwrap();
        let _g2 = manager.select_gateway().unwrap();
        let g3 = manager.select_gateway().unwrap();

        // Should cycle through gateways
        assert_eq!(g1, g3); // Same gateway after cycling through both
    }

    #[tokio::test]
    async fn test_gateway_manager_select_preferred() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let mut manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        manager.add_gateway(test_silo_address(11111));
        manager.add_gateway(test_silo_address(22222));
        manager.set_preferred_gateway(0);

        let g1 = manager.select_gateway().unwrap();
        let g2 = manager.select_gateway().unwrap();

        // Should always return the preferred gateway
        assert_eq!(g1, g2);
    }

    #[tokio::test]
    async fn test_gateway_manager_no_gateways() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        let result = manager.select_gateway();
        assert!(matches!(result, Err(ClientError::NoGatewaysAvailable)));
    }

    #[tokio::test]
    async fn test_gateway_manager_record_failure() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        let gateway = test_silo_address(11111);
        manager.add_gateway(gateway.clone());

        // Record failures
        manager.record_failure(&gateway);
        manager.record_failure(&gateway);
        manager.record_failure(&gateway);

        // Gateway should be unhealthy now
        assert_eq!(manager.healthy_gateway_count(), 0);
    }

    #[tokio::test]
    async fn test_gateway_manager_record_success() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        let gateway = test_silo_address(11111);
        manager.add_gateway(gateway.clone());

        // Record some failures then success
        manager.record_failure(&gateway);
        manager.record_failure(&gateway);
        manager.record_success(&gateway);

        // Gateway should be healthy again
        assert_eq!(manager.healthy_gateway_count(), 1);
    }

    #[tokio::test]
    async fn test_gateway_manager_get_addresses() {
        let conn_manager = Arc::new(ConnectionManager::new(test_silo_address(0)));
        let manager = GatewayManager::new(conn_manager, GatewayOptions::default());

        manager.add_gateway(test_silo_address(11111));
        manager.add_gateway(test_silo_address(22222));

        let addresses = manager.get_gateway_addresses();
        assert_eq!(addresses.len(), 2);
    }
}
