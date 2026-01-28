//! Test cluster builder for integration testing.
//!
//! This module provides a builder for creating test clusters with multiple
//! silo processes for real network integration testing.

use crate::error::{TestError, TestResult};
use crate::silo_process::{SiloProcess, SiloProcessConfig};
use orleans_clustering::{IMembershipTable, InMemoryMembershipTable, MembershipTableServer, TcpMembershipTable};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Configuration for a test cluster.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Number of silos to start
    pub silo_count: u32,
    /// Cluster ID
    pub cluster_id: String,
    /// Membership server port (0 for auto-assign)
    pub membership_server_port: u16,
    /// Silo startup timeout
    pub startup_timeout: Duration,
    /// Silo shutdown timeout
    pub shutdown_timeout: Duration,
    /// Stabilization time after all silos start
    pub stabilization_time: Duration,
    /// Path to silo binary
    pub silo_binary: Option<PathBuf>,
    /// Additional environment variables
    pub env_vars: Vec<(String, String)>,
    /// Enable test mode for silos
    pub test_mode: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            silo_count: 3,
            cluster_id: format!("test-cluster-{}", uuid::Uuid::new_v4()),
            membership_server_port: 0,
            startup_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            stabilization_time: Duration::from_millis(500),
            silo_binary: None,
            env_vars: vec![],
            test_mode: false,
        }
    }
}

impl ClusterConfig {
    /// Create a new config for testing.
    pub fn for_testing() -> Self {
        Self {
            silo_count: 3,
            startup_timeout: Duration::from_secs(15),
            shutdown_timeout: Duration::from_secs(5),
            stabilization_time: Duration::from_millis(250),
            test_mode: true,
            ..Default::default()
        }
    }
}

/// Builder for creating test clusters.
pub struct TestClusterBuilder {
    config: ClusterConfig,
}

impl TestClusterBuilder {
    /// Create a new test cluster builder.
    pub fn new() -> Self {
        Self {
            config: ClusterConfig::default(),
        }
    }

    /// Use a testing configuration with faster timeouts.
    pub fn for_testing() -> Self {
        Self {
            config: ClusterConfig::for_testing(),
        }
    }

    /// Get the current configuration.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Set the number of silos.
    pub fn with_silo_count(mut self, count: u32) -> Self {
        self.config.silo_count = count;
        self
    }

    /// Set the cluster ID.
    pub fn with_cluster_id(mut self, id: impl Into<String>) -> Self {
        self.config.cluster_id = id.into();
        self
    }

    /// Set the membership server port.
    pub fn with_membership_server_port(mut self, port: u16) -> Self {
        self.config.membership_server_port = port;
        self
    }

    /// Set the startup timeout.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.config.startup_timeout = timeout;
        self
    }

    /// Set the shutdown timeout.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    /// Set the stabilization time.
    pub fn with_stabilization_time(mut self, time: Duration) -> Self {
        self.config.stabilization_time = time;
        self
    }

    /// Set the silo binary path.
    pub fn with_silo_binary(mut self, path: PathBuf) -> Self {
        self.config.silo_binary = Some(path);
        self
    }

    /// Add an environment variable.
    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.env_vars.push((key.into(), value.into()));
        self
    }

    /// Enable test mode for silos.
    pub fn with_test_mode(mut self) -> Self {
        self.config.test_mode = true;
        self
    }

    /// Build and start the cluster.
    pub async fn build(self) -> TestResult<TestCluster> {
        info!(
            silo_count = self.config.silo_count,
            cluster_id = %self.config.cluster_id,
            "Building test cluster"
        );

        // Create and start membership server
        let membership_table = Arc::new(InMemoryMembershipTable::new(&self.config.cluster_id));
        membership_table
            .initialize_membership_table(true)
            .await
            .map_err(|e| TestError::ClusterFormation(format!("Failed to initialize membership table: {}", e)))?;

        let membership_server = MembershipTableServer::new(membership_table.clone());
        let server_addr = format!("127.0.0.1:{}", self.config.membership_server_port);

        let actual_addr = membership_server
            .start(&server_addr)
            .await
            .map_err(|e| TestError::ClusterFormation(format!("Failed to start membership server: {}", e)))?;

        info!(
            address = %actual_addr,
            "Membership server started"
        );

        // Spawn silo processes
        let mut silos = Vec::with_capacity(self.config.silo_count as usize);
        let membership_server_addr = actual_addr.to_string();

        for i in 0..self.config.silo_count {
            info!(silo_index = i, "Starting silo process");

            let silo_config = SiloProcessConfig::new(&membership_server_addr)
                .with_port(0)
                .with_startup_timeout(self.config.startup_timeout);

            let silo_config = if self.config.test_mode {
                silo_config.with_test_mode()
            } else {
                silo_config
            };

            let mut process = SiloProcess::spawn(silo_config).await?;

            // Wait for silo to start
            process.wait_for_startup().await?;

            info!(
                silo_index = i,
                pid = process.pid(),
                address = ?process.silo_address(),
                "Silo started"
            );

            silos.push(process);
        }

        // Allow stabilization time
        info!(
            stabilization_ms = self.config.stabilization_time.as_millis(),
            "Waiting for cluster stabilization"
        );
        tokio::time::sleep(self.config.stabilization_time).await;

        Ok(TestCluster {
            config: self.config,
            membership_server,
            membership_server_addr: actual_addr,
            silos,
            is_stopped: false,
        })
    }
}

impl Default for TestClusterBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// A running test cluster.
pub struct TestCluster {
    config: ClusterConfig,
    membership_server: MembershipTableServer,
    membership_server_addr: SocketAddr,
    silos: Vec<SiloProcess>,
    is_stopped: bool,
}

impl TestCluster {
    /// Get the cluster configuration.
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    /// Get the number of silos.
    pub fn silo_count(&self) -> usize {
        self.silos.len()
    }

    /// Get the membership server address.
    pub fn membership_server_addr(&self) -> SocketAddr {
        self.membership_server_addr
    }

    /// Get a reference to the silos.
    pub fn silos(&self) -> &[SiloProcess] {
        &self.silos
    }

    /// Get a mutable reference to the silos.
    pub fn silos_mut(&mut self) -> &mut [SiloProcess] {
        &mut self.silos
    }

    /// Get a silo by index.
    pub fn silo(&self, index: usize) -> Option<&SiloProcess> {
        self.silos.get(index)
    }

    /// Get a mutable silo by index.
    pub fn silo_mut(&mut self, index: usize) -> Option<&mut SiloProcess> {
        self.silos.get_mut(index)
    }

    /// Get silo addresses.
    pub fn silo_addresses(&self) -> Vec<String> {
        self.silos
            .iter()
            .filter_map(|s| s.silo_address().map(|a| a.to_string()))
            .collect()
    }

    /// Check if all silos are running.
    pub fn all_silos_running(&mut self) -> bool {
        self.silos.iter_mut().all(|s| s.is_running())
    }

    /// Get the number of running silos.
    pub fn running_silo_count(&mut self) -> usize {
        let mut count = 0;
        for silo in &mut self.silos {
            if silo.is_running() {
                count += 1;
            }
        }
        count
    }

    /// Connect a TCP membership table client.
    pub fn connect_membership_client(&self) -> TcpMembershipTable {
        TcpMembershipTable::from_addr(self.membership_server_addr)
    }

    /// Kill a silo by index.
    pub async fn kill_silo(&mut self, index: usize) -> TestResult<()> {
        if let Some(silo) = self.silos.get_mut(index) {
            silo.kill().await
        } else {
            Err(TestError::Configuration(format!("Silo index {} out of range", index)))
        }
    }

    /// Stop a silo gracefully by index.
    pub async fn stop_silo(&mut self, index: usize) -> TestResult<()> {
        if let Some(silo) = self.silos.get_mut(index) {
            silo.stop().await
        } else {
            Err(TestError::Configuration(format!("Silo index {} out of range", index)))
        }
    }

    /// Stop the entire cluster.
    pub async fn stop(&mut self) -> TestResult<()> {
        if self.is_stopped {
            return Ok(());
        }

        info!(
            silo_count = self.silos.len(),
            cluster_id = %self.config.cluster_id,
            "Stopping test cluster"
        );

        // Stop all silos
        for (i, silo) in self.silos.iter_mut().enumerate() {
            if silo.is_running() {
                debug!(silo_index = i, pid = silo.pid(), "Stopping silo");
                if let Err(e) = silo.stop().await {
                    warn!(silo_index = i, error = %e, "Error stopping silo");
                }
            }
        }

        // Stop membership server
        debug!("Stopping membership server");
        self.membership_server.stop().await;

        self.is_stopped = true;

        info!(cluster_id = %self.config.cluster_id, "Test cluster stopped");

        Ok(())
    }

    /// Wait for a specific number of active silos in the membership table.
    pub async fn wait_for_active_silos(
        &self,
        expected_count: usize,
        timeout: Duration,
    ) -> TestResult<()> {
        use orleans_clustering::IMembershipTable;

        let client = self.connect_membership_client();
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            let data = client
                .read_all()
                .await
                .map_err(|e| TestError::Network(format!("Failed to read membership: {}", e)))?;

            let active_count = data
                .entries
                .iter()
                .filter(|(entry, _)| entry.status == orleans_clustering::SiloStatus::Active)
                .count();

            if active_count >= expected_count {
                info!(
                    active_count = active_count,
                    expected_count = expected_count,
                    "Expected silo count reached"
                );
                return Ok(());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(TestError::timeout(
            format!("waiting for {} active silos", expected_count),
            timeout.as_secs(),
        ))
    }
}

impl Drop for TestCluster {
    fn drop(&mut self) {
        if !self.is_stopped {
            warn!("TestCluster dropped without calling stop(), attempting cleanup");
            // Best effort cleanup - can't await in drop
            for silo in &mut self.silos {
                silo.try_kill_sync();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.silo_count, 3);
        assert!(config.cluster_id.starts_with("test-cluster-"));
    }

    #[test]
    fn test_cluster_config_for_testing() {
        let config = ClusterConfig::for_testing();
        assert_eq!(config.silo_count, 3);
        assert!(config.test_mode);
        assert!(config.startup_timeout < Duration::from_secs(30));
    }

    #[test]
    fn test_test_cluster_builder() {
        let builder = TestClusterBuilder::new()
            .with_silo_count(5)
            .with_cluster_id("my-cluster")
            .with_startup_timeout(Duration::from_secs(60));

        assert_eq!(builder.config.silo_count, 5);
        assert_eq!(builder.config.cluster_id, "my-cluster");
        assert_eq!(builder.config.startup_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_test_cluster_builder_for_testing() {
        let builder = TestClusterBuilder::for_testing();
        assert!(builder.config.test_mode);
    }
}
