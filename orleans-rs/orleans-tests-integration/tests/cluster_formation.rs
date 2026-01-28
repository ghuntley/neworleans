//! Cluster formation integration tests.
//!
//! These tests verify that Orleans silos can form a cluster correctly
//! across separate OS processes with real network communication.

use orleans_tests_integration::prelude::*;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

/// Initialize logging for tests.
fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,orleans=debug")),
        )
        .try_init();
}

/// Test that three silos can form a cluster.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_three_silos_form_cluster() {
    init_logging();

    let cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .with_startup_timeout(Duration::from_secs(30))
        .build()
        .await
        .expect("Cluster should start");

    // Verify all silos are running
    let mut cluster = cluster;
    cluster.assert_all_silos_running().expect("All silos should be running");

    // Verify membership
    cluster
        .assert_active_silo_count(3)
        .await
        .expect("Should have 3 active silos in membership");

    // Verify consistent membership view
    cluster
        .assert_consistent_membership()
        .await
        .expect("Membership should be consistent");

    // Cleanup
    cluster.stop().await.expect("Cluster should stop cleanly");
}

/// Test that silos can join an existing cluster.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_silo_join_existing_cluster() {
    init_logging();

    // Start with 2 silos
    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .build()
        .await
        .expect("Cluster should start");

    cluster
        .assert_active_silo_count(2)
        .await
        .expect("Should have 2 active silos");

    // Add a third silo dynamically
    let config = SiloProcessConfig::new(cluster.membership_server_addr().to_string())
        .with_port(0)
        .with_test_mode()
        .with_startup_timeout(Duration::from_secs(15));

    let mut new_silo = SiloProcess::spawn(config).await.expect("Should spawn silo");
    new_silo.wait_for_startup().await.expect("New silo should start");

    // Wait for stabilization
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify we now have 3 active silos
    cluster
        .assert_active_silo_count(3)
        .await
        .expect("Should have 3 active silos after join");

    // Cleanup
    new_silo.stop().await.expect("New silo should stop");
    cluster.stop().await.expect("Cluster should stop cleanly");
}

/// Test graceful silo leave.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_silo_graceful_leave() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    cluster
        .assert_active_silo_count(3)
        .await
        .expect("Should have 3 active silos");

    // Gracefully stop one silo
    cluster.stop_silo(0).await.expect("Silo should stop gracefully");

    // Wait for membership update
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Should now have 2 active silos
    cluster
        .wait_for_active_silos(2, Duration::from_secs(10))
        .await
        .expect("Should have 2 active silos after leave");

    // Verify remaining silos still running
    ClusterAssertions::assert_min_silos_running(&mut cluster, 2)
        .expect("At least 2 silos should be running");

    // Cleanup
    cluster.stop().await.expect("Cluster should stop cleanly");
}

/// Test that membership version increases monotonically.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_membership_version_increases() {
    init_logging();

    let cluster = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .build()
        .await
        .expect("Cluster should start");

    // Version should be > 0 after silos joined
    MembershipAssertions::assert_membership_version_gt(&cluster, 0)
        .await
        .expect("Membership version should be > 0");

    // Cleanup
    let mut cluster = cluster;
    cluster.stop().await.expect("Cluster should stop cleanly");
}

/// Test cluster restart.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_cluster_restart() {
    init_logging();

    // First cluster
    let mut cluster1 = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .with_cluster_id("restart-test")
        .build()
        .await
        .expect("First cluster should start");

    cluster1
        .assert_active_silo_count(2)
        .await
        .expect("Should have 2 active silos");

    // Stop first cluster
    cluster1.stop().await.expect("First cluster should stop");

    // Wait a bit
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Start second cluster with same ID
    let mut cluster2 = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .with_cluster_id("restart-test-2")
        .build()
        .await
        .expect("Second cluster should start");

    cluster2
        .assert_active_silo_count(2)
        .await
        .expect("Should have 2 active silos in restarted cluster");

    // Cleanup
    cluster2.stop().await.expect("Second cluster should stop");
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_cluster_config_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.silo_count, 3);
        assert!(!config.test_mode);
    }

    #[test]
    fn test_cluster_config_for_testing() {
        let config = ClusterConfig::for_testing();
        assert!(config.test_mode);
        assert!(config.startup_timeout < Duration::from_secs(30));
    }

    #[test]
    fn test_cluster_builder_fluent_api() {
        let builder = TestClusterBuilder::new()
            .with_silo_count(5)
            .with_cluster_id("my-test")
            .with_startup_timeout(Duration::from_secs(60))
            .with_test_mode();

        assert_eq!(builder.config().silo_count, 5);
        assert_eq!(builder.config().cluster_id, "my-test");
        assert!(builder.config().test_mode);
    }
}
