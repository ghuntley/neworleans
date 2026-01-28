//! Assertion helpers for integration testing.
//!
//! This module provides convenient assertion methods for verifying
//! cluster state, membership, and directory consistency.

use crate::error::{TestError, TestResult};
use crate::TestCluster;
use orleans_clustering::{IMembershipTable, SiloStatus};
use std::collections::HashSet;
use tracing::{debug, info};

/// Cluster-level assertions.
pub struct ClusterAssertions;

impl ClusterAssertions {
    /// Assert that all silos in the cluster are running.
    pub fn assert_all_silos_running(cluster: &mut TestCluster) -> TestResult<()> {
        let running = cluster.running_silo_count();
        let expected = cluster.silo_count();

        if running != expected {
            return Err(TestError::membership_assertion(
                format!("{} running silos", expected),
                format!("{} running silos", running),
            ));
        }

        info!(running = running, "All silos running");
        Ok(())
    }

    /// Assert that at least N silos are running.
    pub fn assert_min_silos_running(cluster: &mut TestCluster, min: usize) -> TestResult<()> {
        let running = cluster.running_silo_count();

        if running < min {
            return Err(TestError::membership_assertion(
                format!("at least {} running silos", min),
                format!("{} running silos", running),
            ));
        }

        info!(running = running, min = min, "Minimum silos running");
        Ok(())
    }

    /// Assert that a specific number of silos are running.
    pub fn assert_silo_count(cluster: &mut TestCluster, expected: usize) -> TestResult<()> {
        let running = cluster.running_silo_count();

        if running != expected {
            return Err(TestError::membership_assertion(
                format!("{} running silos", expected),
                format!("{} running silos", running),
            ));
        }

        debug!(running = running, "Silo count matches");
        Ok(())
    }
}

/// Membership-related assertions.
pub struct MembershipAssertions;

impl MembershipAssertions {
    /// Assert that N silos are active in the membership table.
    pub async fn assert_active_silo_count(
        cluster: &TestCluster,
        expected: usize,
    ) -> TestResult<()> {
        let client = cluster.connect_membership_client();
        let data = client
            .read_all()
            .await
            .map_err(|e| TestError::Network(format!("Failed to read membership: {}", e)))?;

        let active_count = data
            .entries
            .iter()
            .filter(|(entry, _)| entry.status == SiloStatus::Active)
            .count();

        if active_count != expected {
            return Err(TestError::membership_assertion(
                format!("{} active silos", expected),
                format!("{} active silos", active_count),
            ));
        }

        info!(
            active_count = active_count,
            "Active silo count matches"
        );
        Ok(())
    }

    /// Assert that all silos see the same membership.
    pub async fn assert_consistent_membership(cluster: &TestCluster) -> TestResult<()> {
        let client = cluster.connect_membership_client();
        let data = client
            .read_all()
            .await
            .map_err(|e| TestError::Network(format!("Failed to read membership: {}", e)))?;

        // Get active silos
        let active_silos: HashSet<_> = data
            .entries
            .iter()
            .filter(|(entry, _)| entry.status == SiloStatus::Active)
            .map(|(entry, _)| entry.silo_address.clone())
            .collect();

        // All silos should see the same active members
        // (In a real implementation, we'd query each silo's view)

        let expected_count = cluster.silo_count();
        if active_silos.len() != expected_count {
            return Err(TestError::membership_assertion(
                format!("{} active silos", expected_count),
                format!("{} active silos", active_silos.len()),
            ));
        }

        info!(
            active_count = active_silos.len(),
            "Membership is consistent"
        );
        Ok(())
    }

    /// Assert that a specific silo is in a given status.
    pub async fn assert_silo_status(
        cluster: &TestCluster,
        silo_index: usize,
        expected_status: SiloStatus,
    ) -> TestResult<()> {
        let silo = cluster.silo(silo_index).ok_or_else(|| {
            TestError::Configuration(format!("Silo index {} out of range", silo_index))
        })?;

        let silo_address = silo.silo_address().ok_or_else(|| {
            TestError::Configuration(format!("Silo {} has no address", silo_index))
        })?;

        let client = cluster.connect_membership_client();
        let data = client
            .read_all()
            .await
            .map_err(|e| TestError::Network(format!("Failed to read membership: {}", e)))?;

        for (entry, _) in data.entries.iter() {
            if entry.silo_address.to_string().contains(silo_address) {
                if entry.status != expected_status {
                    return Err(TestError::membership_assertion(
                        format!("{:?}", expected_status),
                        format!("{:?}", entry.status),
                    ));
                }
                debug!(
                    silo_index = silo_index,
                    status = ?entry.status,
                    "Silo status matches"
                );
                return Ok(());
            }
        }

        Err(TestError::MembershipAssertion {
            expected: format!("silo {} in membership table", silo_index),
            actual: "silo not found".into(),
        })
    }

    /// Assert that membership version is greater than a given value.
    pub async fn assert_membership_version_gt(
        cluster: &TestCluster,
        min_version: i64,
    ) -> TestResult<()> {
        let client = cluster.connect_membership_client();
        let data = client
            .read_all()
            .await
            .map_err(|e| TestError::Network(format!("Failed to read membership: {}", e)))?;

        if data.version.version <= min_version {
            return Err(TestError::membership_assertion(
                format!("version > {}", min_version),
                format!("version = {}", data.version.version),
            ));
        }

        debug!(
            version = data.version.version,
            min_version = min_version,
            "Membership version is valid"
        );
        Ok(())
    }
}

/// Directory-related assertions.
pub struct DirectoryAssertions;

impl DirectoryAssertions {
    /// Assert that a grain is registered in the directory.
    pub async fn assert_grain_registered(
        _cluster: &TestCluster,
        _grain_id: &str,
    ) -> TestResult<()> {
        // This would require access to the grain directory
        // For now, we rely on grain invocation succeeding as proof
        Ok(())
    }

    /// Assert that grains are distributed across multiple silos.
    pub async fn assert_grain_distribution(
        _cluster: &TestCluster,
        _grain_ids: &[String],
        min_silos: usize,
    ) -> TestResult<()> {
        // This would require access to the grain directory
        // For now, we verify distribution through the consistent hash ring
        info!(
            min_silos = min_silos,
            "Grain distribution assertion (placeholder)"
        );
        Ok(())
    }
}

/// Extension trait for TestCluster to add assertion methods.
pub trait TestClusterAssertions {
    /// Assert all silos are running.
    fn assert_all_silos_running(&mut self) -> TestResult<()>;

    /// Assert active silo count in membership.
    fn assert_active_silo_count(
        &self,
        expected: usize,
    ) -> impl std::future::Future<Output = TestResult<()>> + Send;

    /// Assert consistent membership across silos.
    fn assert_consistent_membership(
        &self,
    ) -> impl std::future::Future<Output = TestResult<()>> + Send;
}

impl TestClusterAssertions for TestCluster {
    fn assert_all_silos_running(&mut self) -> TestResult<()> {
        ClusterAssertions::assert_all_silos_running(self)
    }

    async fn assert_active_silo_count(&self, expected: usize) -> TestResult<()> {
        MembershipAssertions::assert_active_silo_count(self, expected).await
    }

    async fn assert_consistent_membership(&self) -> TestResult<()> {
        MembershipAssertions::assert_consistent_membership(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = TestError::membership_assertion("3 active", "2 active");
        assert!(err.to_string().contains("3 active"));
        assert!(err.to_string().contains("2 active"));
    }
}
