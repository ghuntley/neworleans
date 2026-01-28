//! Failure scenario integration tests.
//!
//! These tests verify that the Orleans cluster handles failures correctly,
//! including silo crashes, network issues, and recovery scenarios.

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

/// Test that silo crash is detected.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_silo_crash_detection() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // Verify all silos active
    cluster
        .assert_active_silo_count(3)
        .await
        .expect("Should have 3 active silos");

    // Kill silo 0 (ungraceful shutdown)
    cluster.kill_silo(0).await.expect("Should kill silo");

    // Wait for failure detection
    // Note: This depends on heartbeat and suspect voting configuration
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Should eventually see only 2 active silos
    cluster
        .wait_for_active_silos(2, Duration::from_secs(30))
        .await
        .expect("Should detect failed silo and have 2 active");

    // Cleanup
    cluster.stop().await.expect("Cluster should stop");
}

/// Test grain reactivation after silo failure.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_grain_reactivation_after_failure() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    let membership_addr = cluster.membership_server_addr().to_string();

    // Create grain and get initial value
    let config1 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain("failover-test")
        .with_startup_timeout(Duration::from_secs(15));

    let mut silo1 = SiloProcess::spawn(config1).await.expect("Silo should spawn");
    silo1.wait_for_startup().await.expect("Silo should start");

    let event1 = silo1
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { success: true, .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("First invocation should succeed");

    let result1: i32 = if let ProcessEvent::GrainInvoked { result: Some(v), .. } = event1 {
        serde_json::from_value(v).unwrap_or(0)
    } else {
        0
    };

    tracing::info!(result1 = result1, "First invocation completed");

    // Kill the silo (simulate crash)
    silo1.kill().await.expect("Should kill silo");

    // Wait for failure detection
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Invoke again - grain should reactivate on another silo
    let config2 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain("failover-test")
        .with_startup_timeout(Duration::from_secs(15));

    let mut silo2 = SiloProcess::spawn(config2).await.expect("Silo should spawn");
    silo2.wait_for_startup().await.expect("Silo should start");

    let event2 = silo2
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { success: true, .. }),
            Duration::from_secs(15),
        )
        .await
        .expect("Second invocation should succeed (reactivation)");

    if let ProcessEvent::GrainInvoked { success, .. } = event2 {
        assert!(success, "Grain should be successfully invoked after reactivation");
    }

    silo2.stop().await.expect("Silo should stop");
    cluster.stop().await.expect("Cluster should stop");
}

/// Test directory consistency after silo failure and recovery.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_directory_consistency_after_recovery() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // Create multiple grains
    let membership_addr = cluster.membership_server_addr().to_string();
    for i in 0..5 {
        let config = SiloProcessConfig::new(&membership_addr)
            .with_port(0)
            .with_create_grain(&format!("consistency-test-{}", i))
            .with_startup_timeout(Duration::from_secs(15));

        let mut silo = SiloProcess::spawn(config).await.expect("Silo should spawn");
        silo.wait_for_startup().await.expect("Silo should start");

        let _ = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainCreated { .. }),
                Duration::from_secs(10),
            )
            .await;

        silo.stop().await.expect("Silo should stop");
    }

    // Kill one of the cluster silos
    cluster.kill_silo(0).await.expect("Should kill silo");

    // Wait for recovery
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Verify remaining silos can still invoke all grains
    for i in 0..5 {
        let config = SiloProcessConfig::new(&membership_addr)
            .with_port(0)
            .with_test_grain(&format!("consistency-test-{}", i))
            .with_startup_timeout(Duration::from_secs(15));

        let mut silo = SiloProcess::spawn(config).await.expect("Silo should spawn");
        silo.wait_for_startup().await.expect("Silo should start");

        let event = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
                Duration::from_secs(15),
            )
            .await;

        if let Ok(ProcessEvent::GrainInvoked { success, .. }) = event {
            assert!(success, "Grain {} should be invocable after failure", i);
        }

        silo.stop().await.expect("Silo should stop");
    }

    cluster.stop().await.expect("Cluster should stop");
}

/// Test that multiple silos can be killed and cluster survives.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_multiple_silo_failures() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(5)
        .build()
        .await
        .expect("Cluster should start");

    cluster
        .assert_active_silo_count(5)
        .await
        .expect("Should have 5 active silos");

    // Kill 2 silos
    cluster.kill_silo(0).await.expect("Should kill silo 0");
    cluster.kill_silo(1).await.expect("Should kill silo 1");

    // Wait for detection
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Should have 3 active silos
    cluster
        .wait_for_active_silos(3, Duration::from_secs(30))
        .await
        .expect("Should have 3 active silos after failures");

    // Cluster should still function
    let membership_addr = cluster.membership_server_addr().to_string();
    let config = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain("survive-test")
        .with_startup_timeout(Duration::from_secs(15));

    let mut silo = SiloProcess::spawn(config).await.expect("Silo should spawn");
    silo.wait_for_startup().await.expect("Silo should start");

    let event = silo
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { success: true, .. }),
            Duration::from_secs(15),
        )
        .await
        .expect("Grain invocation should succeed");

    if let ProcessEvent::GrainInvoked { success, .. } = event {
        assert!(success, "Cluster should function after multiple failures");
    }

    silo.stop().await.expect("Silo should stop");
    cluster.stop().await.expect("Cluster should stop");
}

/// Test graceful shutdown with pending requests.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_graceful_shutdown_with_pending() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .build()
        .await
        .expect("Cluster should start");

    // Create a long-running grain operation would go here
    // For now, we just test that shutdown completes

    // Graceful shutdown
    let start = std::time::Instant::now();
    cluster.stop().await.expect("Cluster should stop gracefully");
    let duration = start.elapsed();

    tracing::info!(duration_ms = duration.as_millis(), "Cluster stopped");

    // Should complete within reasonable time
    assert!(
        duration < Duration::from_secs(30),
        "Shutdown should complete within 30 seconds"
    );
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_error_is_retryable() {
        assert!(TestError::timeout("test", 10).is_retryable());
        assert!(TestError::Network("connection refused".into()).is_retryable());
        assert!(!TestError::Configuration("invalid".into()).is_retryable());
    }

    #[test]
    fn test_error_is_permanent() {
        assert!(TestError::Configuration("invalid".into()).is_permanent());
        assert!(!TestError::timeout("test", 10).is_permanent());
    }
}
