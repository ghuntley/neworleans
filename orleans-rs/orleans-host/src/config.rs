//! Configuration options for the silo host.

use orleans_clustering::ClusterMembershipOptions;
use orleans_directory::GrainDirectoryOptions;
use orleans_messaging::MessageCenterConfig;
use orleans_runtime::{CatalogOptions, DispatcherOptions};
use std::net::SocketAddr;
use std::time::Duration;

/// Configuration for a silo.
#[derive(Debug, Clone)]
pub struct SiloConfig {
    /// The address to bind the silo to.
    pub listen_address: SocketAddr,

    /// Generation number for this silo instance.
    /// Used to distinguish silo restarts at the same address.
    pub generation: i64,

    /// Cluster membership options.
    pub membership: ClusterMembershipOptions,

    /// Grain directory options.
    pub directory: GrainDirectoryOptions,

    /// Message center options.
    pub messaging: MessageCenterConfig,

    /// Catalog options.
    pub catalog: CatalogOptions,

    /// Dispatcher options.
    pub dispatcher: DispatcherOptions,

    /// Startup timeout - how long to wait for the silo to start.
    pub startup_timeout: Duration,

    /// Shutdown timeout - how long to wait for graceful shutdown.
    pub shutdown_timeout: Duration,
}

impl Default for SiloConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:11111".parse().unwrap(),
            generation: chrono::Utc::now().timestamp_millis(),
            membership: ClusterMembershipOptions::default(),
            directory: GrainDirectoryOptions::default(),
            messaging: MessageCenterConfig::default(),
            catalog: CatalogOptions::default(),
            dispatcher: DispatcherOptions::default(),
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl SiloConfig {
    /// Create a development configuration with faster timeouts.
    pub fn development() -> Self {
        Self {
            listen_address: "127.0.0.1:11111".parse().unwrap(),
            generation: chrono::Utc::now().timestamp_millis(),
            membership: ClusterMembershipOptions::development(),
            directory: GrainDirectoryOptions::default(),
            messaging: MessageCenterConfig::default(),
            catalog: CatalogOptions {
                idle_timeout: Duration::from_secs(30),
                collection_interval: Duration::from_secs(10),
                ..Default::default()
            },
            dispatcher: DispatcherOptions::default(),
            startup_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    /// Create a test configuration with very fast timeouts.
    pub fn test() -> Self {
        Self {
            listen_address: "127.0.0.1:0".parse().unwrap(), // Use port 0 for random port
            generation: chrono::Utc::now().timestamp_millis(),
            membership: ClusterMembershipOptions::development(),
            directory: GrainDirectoryOptions::default(),
            messaging: MessageCenterConfig::default(),
            catalog: CatalogOptions {
                idle_timeout: Duration::from_secs(5),
                collection_interval: Duration::from_secs(1),
                ..Default::default()
            },
            dispatcher: DispatcherOptions::default(),
            startup_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(2),
        }
    }
}
