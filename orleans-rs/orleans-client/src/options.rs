//! Configuration options for the Orleans cluster client.

use std::net::SocketAddr;
use std::time::Duration;

/// Options for configuring the cluster client.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// The cluster ID to connect to.
    pub cluster_id: String,

    /// The service ID for this application.
    pub service_id: String,

    /// Initial gateway endpoints to try connecting to.
    pub gateway_endpoints: Vec<SocketAddr>,

    /// Default timeout for grain method calls.
    pub response_timeout: Duration,

    /// How often to refresh the gateway list from the membership table.
    pub gateway_refresh_interval: Duration,

    /// How long to wait before retrying a failed connection.
    pub reconnect_delay: Duration,

    /// Maximum number of concurrent pending requests per gateway.
    pub max_pending_requests: usize,

    /// How long to keep idle connections alive.
    pub connection_idle_timeout: Duration,

    /// Preferred gateway index for sticky routing (-1 = disabled).
    pub preferred_gateway_index: i32,

    /// Number of buckets for grain-to-gateway mapping.
    pub client_sender_buckets: usize,

    /// Maximum number of retry attempts for failed requests.
    pub max_retry_attempts: u32,

    /// Enable automatic reconnection on disconnect.
    pub auto_reconnect: bool,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            cluster_id: "default".to_string(),
            service_id: "default".to_string(),
            gateway_endpoints: Vec::new(),
            response_timeout: Duration::from_secs(30),
            gateway_refresh_interval: Duration::from_secs(60),
            reconnect_delay: Duration::from_secs(1),
            max_pending_requests: 10000,
            connection_idle_timeout: Duration::from_secs(300),
            preferred_gateway_index: -1,
            client_sender_buckets: 8192,
            max_retry_attempts: 3,
            auto_reconnect: true,
        }
    }
}

impl ClientOptions {
    /// Create new client options with the specified cluster ID.
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            ..Default::default()
        }
    }

    /// Create options for testing with shorter timeouts.
    pub fn for_testing() -> Self {
        Self {
            cluster_id: "test-cluster".to_string(),
            service_id: "test-service".to_string(),
            response_timeout: Duration::from_secs(5),
            gateway_refresh_interval: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(100),
            max_pending_requests: 1000,
            connection_idle_timeout: Duration::from_secs(30),
            max_retry_attempts: 2,
            auto_reconnect: true,
            ..Default::default()
        }
    }

    /// Set the cluster ID.
    pub fn with_cluster_id(mut self, cluster_id: impl Into<String>) -> Self {
        self.cluster_id = cluster_id.into();
        self
    }

    /// Set the service ID.
    pub fn with_service_id(mut self, service_id: impl Into<String>) -> Self {
        self.service_id = service_id.into();
        self
    }

    /// Add a gateway endpoint.
    pub fn with_gateway(mut self, endpoint: SocketAddr) -> Self {
        self.gateway_endpoints.push(endpoint);
        self
    }

    /// Set multiple gateway endpoints.
    pub fn with_gateways(mut self, endpoints: Vec<SocketAddr>) -> Self {
        self.gateway_endpoints = endpoints;
        self
    }

    /// Set the response timeout.
    pub fn with_response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = timeout;
        self
    }

    /// Set the gateway refresh interval.
    pub fn with_gateway_refresh_interval(mut self, interval: Duration) -> Self {
        self.gateway_refresh_interval = interval;
        self
    }

    /// Set the reconnect delay.
    pub fn with_reconnect_delay(mut self, delay: Duration) -> Self {
        self.reconnect_delay = delay;
        self
    }

    /// Set the maximum pending requests.
    pub fn with_max_pending_requests(mut self, max: usize) -> Self {
        self.max_pending_requests = max;
        self
    }

    /// Set the connection idle timeout.
    pub fn with_connection_idle_timeout(mut self, timeout: Duration) -> Self {
        self.connection_idle_timeout = timeout;
        self
    }

    /// Set the preferred gateway index.
    pub fn with_preferred_gateway(mut self, index: i32) -> Self {
        self.preferred_gateway_index = index;
        self
    }

    /// Set the maximum retry attempts.
    pub fn with_max_retry_attempts(mut self, attempts: u32) -> Self {
        self.max_retry_attempts = attempts;
        self
    }

    /// Enable or disable auto-reconnect.
    pub fn with_auto_reconnect(mut self, enabled: bool) -> Self {
        self.auto_reconnect = enabled;
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.cluster_id.is_empty() {
            return Err("cluster_id cannot be empty".to_string());
        }
        if self.service_id.is_empty() {
            return Err("service_id cannot be empty".to_string());
        }
        if self.gateway_endpoints.is_empty() {
            return Err("at least one gateway endpoint is required".to_string());
        }
        if self.response_timeout.is_zero() {
            return Err("response_timeout must be greater than zero".to_string());
        }
        if self.max_pending_requests == 0 {
            return Err("max_pending_requests must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Options for gateway management.
#[derive(Debug, Clone)]
pub struct GatewayOptions {
    /// How often to refresh the gateway list.
    pub refresh_period: Duration,

    /// Timeout for gateway health checks.
    pub health_check_timeout: Duration,

    /// Number of consecutive failures before marking gateway as unhealthy.
    pub failure_threshold: u32,

    /// How long to wait before retrying an unhealthy gateway.
    pub recovery_period: Duration,
}

impl Default for GatewayOptions {
    fn default() -> Self {
        Self {
            refresh_period: Duration::from_secs(60),
            health_check_timeout: Duration::from_secs(5),
            failure_threshold: 3,
            recovery_period: Duration::from_secs(30),
        }
    }
}

impl GatewayOptions {
    /// Create options for testing with shorter timeouts.
    pub fn for_testing() -> Self {
        Self {
            refresh_period: Duration::from_secs(5),
            health_check_timeout: Duration::from_secs(1),
            failure_threshold: 2,
            recovery_period: Duration::from_secs(5),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    #[test]
    fn test_default_options() {
        let options = ClientOptions::default();
        assert_eq!(options.cluster_id, "default");
        assert_eq!(options.service_id, "default");
        assert!(options.gateway_endpoints.is_empty());
        assert_eq!(options.response_timeout, Duration::from_secs(30));
        assert!(options.auto_reconnect);
    }

    #[test]
    fn test_new_options() {
        let options = ClientOptions::new("my-cluster");
        assert_eq!(options.cluster_id, "my-cluster");
    }

    #[test]
    fn test_for_testing_options() {
        let options = ClientOptions::for_testing();
        assert_eq!(options.cluster_id, "test-cluster");
        assert!(options.response_timeout < Duration::from_secs(30));
    }

    #[test]
    fn test_builder_pattern() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let options = ClientOptions::new("cluster")
            .with_service_id("service")
            .with_gateway(addr)
            .with_response_timeout(Duration::from_secs(60))
            .with_max_retry_attempts(5)
            .with_auto_reconnect(false);

        assert_eq!(options.cluster_id, "cluster");
        assert_eq!(options.service_id, "service");
        assert_eq!(options.gateway_endpoints, vec![addr]);
        assert_eq!(options.response_timeout, Duration::from_secs(60));
        assert_eq!(options.max_retry_attempts, 5);
        assert!(!options.auto_reconnect);
    }

    #[test]
    fn test_validation_success() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let options = ClientOptions::new("cluster").with_gateway(addr);
        assert!(options.validate().is_ok());
    }

    #[test]
    fn test_validation_empty_cluster_id() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let options = ClientOptions::new("").with_gateway(addr);
        let result = options.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("cluster_id"));
    }

    #[test]
    fn test_validation_no_gateways() {
        let options = ClientOptions::new("cluster");
        let result = options.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("gateway"));
    }

    #[test]
    fn test_validation_zero_timeout() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let options = ClientOptions::new("cluster")
            .with_gateway(addr)
            .with_response_timeout(Duration::ZERO);
        let result = options.validate();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("response_timeout"));
    }

    #[test]
    fn test_gateway_options_default() {
        let options = GatewayOptions::default();
        assert_eq!(options.refresh_period, Duration::from_secs(60));
        assert_eq!(options.failure_threshold, 3);
    }

    #[test]
    fn test_gateway_options_testing() {
        let options = GatewayOptions::for_testing();
        assert!(options.refresh_period < Duration::from_secs(60));
    }
}
