//! Builder for configuring and creating Orleans cluster clients.

use crate::client::ClusterClient;
use crate::error::ClientResult;
use crate::options::ClientOptions;
use orleans_clustering::IMembershipTable;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, instrument};

/// Builder for creating and configuring a ClusterClient.
///
/// # Example
///
/// ```rust,no_run
/// use orleans_client::ClientBuilder;
/// use std::time::Duration;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let client = ClientBuilder::new()
///     .with_cluster_id("my-cluster")
///     .with_service_id("my-service")
///     .with_gateway("127.0.0.1:30000".parse()?)
///     .with_gateway("127.0.0.1:30001".parse()?)
///     .with_response_timeout(Duration::from_secs(60))
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct ClientBuilder {
    options: ClientOptions,
    membership_table: Option<Arc<dyn IMembershipTable>>,
}

impl ClientBuilder {
    /// Create a new client builder with default options.
    pub fn new() -> Self {
        Self {
            options: ClientOptions::default(),
            membership_table: None,
        }
    }

    /// Create a builder with testing-oriented defaults.
    pub fn for_testing() -> Self {
        Self {
            options: ClientOptions::for_testing(),
            membership_table: None,
        }
    }

    /// Set the cluster ID.
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.options.cluster_id = cluster_id.into();
        self
    }

    /// Set the service ID.
    pub fn with_service_id(mut self, service_id: impl Into<String>) -> Self {
        self.options.service_id = service_id.into();
        self
    }

    /// Add a gateway endpoint.
    pub fn with_gateway(mut self, endpoint: SocketAddr) -> Self {
        self.options.gateway_endpoints.push(endpoint);
        self
    }

    /// Set multiple gateway endpoints.
    pub fn with_gateways(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.options.gateway_endpoints = endpoints;
        self
    }

    /// Set the default response timeout.
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.options.response_timeout = timeout;
        self
    }

    /// Set the gateway list refresh interval.
    pub fn with_gateway_refresh_interval(mut self, interval: Duration) -> Self {
        self.options.gateway_refresh_interval = interval;
        self
    }

    /// Set the reconnect delay.
    pub fn with_reconnect_delay(mut self, delay: Duration) -> Self {
        self.options.reconnect_delay = delay;
        self
    }

    /// Set the maximum number of pending requests.
    pub fn with_max_pending_requests(mut self, max: usize) -> Self {
        self.options.max_pending_requests = max;
        self
    }

    /// Set the connection idle timeout.
    pub fn with_connection_idle_timeout(mut self, timeout: Duration) -> Self {
        self.options.connection_idle_timeout = timeout;
        self
    }

    /// Set the preferred gateway index for sticky routing.
    ///
    /// Use -1 to disable (default, uses round-robin).
    pub fn with_preferred_gateway(mut self, index: i32) -> Self {
        self.options.preferred_gateway_index = index;
        self
    }

    /// Set the maximum number of retry attempts.
    pub fn with_max_retry_attempts(mut self, attempts: u32) -> Self {
        self.options.max_retry_attempts = attempts;
        self
    }

    /// Enable or disable automatic reconnection.
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.options.auto_reconnect = enabled;
        self
    }

    /// Set a membership table for automatic gateway discovery.
    pub fn with_membership_table(mut self, table: Arc<dyn IMembershipTable>) -> Self {
        self.membership_table = Some(table);
        self
    }

    /// Build the cluster client.
    #[instrument(skip(self))]
    pub fn build(self) -> ClientResult<ClusterClient> {
        debug!(
            cluster_id = %self.options.cluster_id,
            service_id = %self.options.service_id,
            gateway_count = self.options.gateway_endpoints.len(),
            "building cluster client"
        );

        if let Some(membership_table) = self.membership_table {
            ClusterClient::with_membership_table(self.options, membership_table)
        } else {
            ClusterClient::new(self.options)
        }
    }

    /// Build the cluster client and immediately connect.
    #[instrument(skip(self))]
    pub async fn build_and_connect(self) -> ClientResult<ClusterClient> {
        let client = self.build()?;
        client.connect().await?;
        Ok(client)
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_builder_default() {
        let builder = ClientBuilder::new();
        assert_eq!(builder.options.cluster_id, "default");
    }

    #[test]
    fn test_builder_for_testing() {
        let builder = ClientBuilder::for_testing();
        assert_eq!(builder.options.cluster_id, "test-cluster");
        assert!(builder.options.response_timeout < Duration::from_secs(30));
    }

    #[test]
    fn test_builder_with_cluster_id() {
        let builder = ClientBuilder::new().with_cluster_id("my-cluster");
        assert_eq!(builder.options.cluster_id, "my-cluster");
    }

    #[test]
    fn test_builder_with_service_id() {
        let builder = ClientBuilder::new().with_service_id("my-service");
        assert_eq!(builder.options.service_id, "my-service");
    }

    #[test]
    fn test_builder_with_gateway() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let builder = ClientBuilder::new().with_gateway(addr);
        assert_eq!(builder.options.gateway_endpoints.len(), 1);
    }

    #[test]
    fn test_builder_with_multiple_gateways() {
        let addr1: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:22222".parse().unwrap();
        let builder = ClientBuilder::new()
            .with_gateway(addr1)
            .with_gateway(addr2);
        assert_eq!(builder.options.gateway_endpoints.len(), 2);
    }

    #[test]
    fn test_builder_with_gateways_vec() {
        let addrs = vec![
            "127.0.0.1:11111".parse().unwrap(),
            "127.0.0.1:22222".parse().unwrap(),
        ];
        let builder = ClientBuilder::new().with_gateways(addrs);
        assert_eq!(builder.options.gateway_endpoints.len(), 2);
    }

    #[test]
    fn test_builder_with_response_timeout() {
        let builder = ClientBuilder::new().with_response_timeout(Duration::from_secs(60));
        assert_eq!(builder.options.response_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_builder_with_max_pending() {
        let builder = ClientBuilder::new().with_max_pending_requests(500);
        assert_eq!(builder.options.max_pending_requests, 500);
    }

    #[test]
    fn test_builder_with_preferred_gateway() {
        let builder = ClientBuilder::new().with_preferred_gateway(2);
        assert_eq!(builder.options.preferred_gateway_index, 2);
    }

    #[test]
    fn test_builder_with_auto_reconnect() {
        let builder = ClientBuilder::new().with_auto_reconnect(false);
        assert!(!builder.options.auto_reconnect);
    }

    #[test]
    fn test_builder_chaining() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let builder = ClientBuilder::new()
            .with_cluster_id("cluster")
            .with_service_id("service")
            .with_gateway(addr)
            .with_response_timeout(Duration::from_secs(60))
            .with_max_retry_attempts(5)
            .with_auto_reconnect(true);

        assert_eq!(builder.options.cluster_id, "cluster");
        assert_eq!(builder.options.service_id, "service");
        assert_eq!(builder.options.gateway_endpoints.len(), 1);
        assert_eq!(builder.options.response_timeout, Duration::from_secs(60));
        assert_eq!(builder.options.max_retry_attempts, 5);
        assert!(builder.options.auto_reconnect);
    }

    #[test]
    fn test_build_success() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let result = ClientBuilder::new()
            .with_cluster_id("cluster")
            .with_gateway(addr)
            .build();
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_no_gateways() {
        let result = ClientBuilder::new()
            .with_cluster_id("cluster")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_build_empty_cluster_id() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let result = ClientBuilder::new()
            .with_cluster_id("")
            .with_gateway(addr)
            .build();
        assert!(result.is_err());
    }
}
