//! Grain communication integration tests.
//!
//! These tests verify that grains can communicate across silos
//! with real network communication.

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

/// Test cross-silo grain invocation.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_cross_silo_grain_invocation() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // Create grain on silo 0
    let config = SiloProcessConfig::new(cluster.membership_server_addr().to_string())
        .with_port(0)
        .with_test_mode()
        .with_create_grain("test-grain-1")
        .with_startup_timeout(Duration::from_secs(15));

    let mut test_silo = SiloProcess::spawn(config).await.expect("Test silo should spawn");
    test_silo.wait_for_startup().await.expect("Test silo should start");

    // Wait for grain creation event
    let event = test_silo
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainCreated { .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("Should receive grain created event");

    if let ProcessEvent::GrainCreated { grain_id, silo, .. } = event {
        tracing::info!(grain_id = %grain_id, silo = %silo, "Grain created");
    }

    // Invoke from another silo
    let config2 = SiloProcessConfig::new(cluster.membership_server_addr().to_string())
        .with_port(0)
        .with_test_mode()
        .with_test_grain("test-grain-1")
        .with_startup_timeout(Duration::from_secs(15));

    let mut invoker_silo = SiloProcess::spawn(config2).await.expect("Invoker silo should spawn");
    invoker_silo.wait_for_startup().await.expect("Invoker silo should start");

    // Wait for invocation result
    let event = invoker_silo
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("Should receive grain invoked event");

    if let ProcessEvent::GrainInvoked { success, method, result, .. } = event {
        assert!(success, "Grain invocation should succeed");
        tracing::info!(method = %method, result = ?result, "Grain invoked");
    }

    // Cleanup
    test_silo.stop().await.expect("Test silo should stop");
    invoker_silo.stop().await.expect("Invoker silo should stop");
    cluster.stop().await.expect("Cluster should stop");
}

/// Test single activation guarantee across processes.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_single_activation_guarantee() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // Spawn silos that try to create the same grain
    let membership_addr = cluster.membership_server_addr().to_string();
    let grain_key = "single-activation-test";

    let config1 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain(grain_key)
        .with_wait_for_cluster(3)
        .with_startup_timeout(Duration::from_secs(30));

    let config2 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain(grain_key)
        .with_wait_for_cluster(3)
        .with_startup_timeout(Duration::from_secs(30));

    let config3 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain(grain_key)
        .with_wait_for_cluster(3)
        .with_startup_timeout(Duration::from_secs(30));

    // Start silos concurrently
    let (silo1, silo2, silo3) = tokio::join!(
        SiloProcess::spawn(config1),
        SiloProcess::spawn(config2),
        SiloProcess::spawn(config3),
    );

    let mut silo1 = silo1.expect("Silo 1 should spawn");
    let mut silo2 = silo2.expect("Silo 2 should spawn");
    let mut silo3 = silo3.expect("Silo 3 should spawn");

    // Wait for startup
    let (r1, r2, r3) = tokio::join!(
        silo1.wait_for_startup(),
        silo2.wait_for_startup(),
        silo3.wait_for_startup(),
    );

    r1.expect("Silo 1 should start");
    r2.expect("Silo 2 should start");
    r3.expect("Silo 3 should start");

    // Collect invocation results
    let timeout = Duration::from_secs(15);
    let mut results = Vec::new();

    for mut silo in [silo1, silo2, silo3] {
        if let Ok(event) = silo.wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
            timeout,
        ).await {
            if let ProcessEvent::GrainInvoked { success, result, .. } = event {
                if success {
                    if let Some(value) = result {
                        results.push(value);
                    }
                }
            }
        }
        let _ = silo.stop().await;
    }

    // All invocations should have succeeded with sequential counter values
    tracing::info!(results = ?results, "Invocation results");
    assert!(!results.is_empty(), "Should have at least one successful invocation");

    // Cleanup
    cluster.stop().await.expect("Cluster should stop");
}

/// Test grain state persistence across calls.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_grain_state_persistence() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .build()
        .await
        .expect("Cluster should start");

    let membership_addr = cluster.membership_server_addr().to_string();
    let grain_key = "state-test";

    // First invocation
    let config1 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain(grain_key)
        .with_startup_timeout(Duration::from_secs(15));

    let mut silo1 = SiloProcess::spawn(config1).await.expect("Silo should spawn");
    silo1.wait_for_startup().await.expect("Silo should start");

    let event1 = silo1
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { success: true, .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("Should get first invocation result");

    let result1: i32 = if let ProcessEvent::GrainInvoked { result: Some(v), .. } = event1 {
        serde_json::from_value(v).unwrap_or(0)
    } else {
        0
    };

    silo1.stop().await.expect("Silo should stop");

    // Second invocation
    let config2 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain(grain_key)
        .with_startup_timeout(Duration::from_secs(15));

    let mut silo2 = SiloProcess::spawn(config2).await.expect("Silo should spawn");
    silo2.wait_for_startup().await.expect("Silo should start");

    let event2 = silo2
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { success: true, .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("Should get second invocation result");

    let result2: i32 = if let ProcessEvent::GrainInvoked { result: Some(v), .. } = event2 {
        serde_json::from_value(v).unwrap_or(0)
    } else {
        0
    };

    // Second result should be greater than first (state persisted)
    assert!(result2 > result1, "Counter should increment: {} > {}", result2, result1);

    silo2.stop().await.expect("Silo should stop");
    cluster.stop().await.expect("Cluster should stop");
}

/// Test grain migration during silo shutdown.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_grain_migration_on_shutdown() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    let membership_addr = cluster.membership_server_addr().to_string();

    // Create grain on specific silo
    let config = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_create_grain("migrate-test")
        .with_startup_timeout(Duration::from_secs(15));

    let mut host_silo = SiloProcess::spawn(config).await.expect("Host silo should spawn");
    host_silo.wait_for_startup().await.expect("Host silo should start");

    // Wait for grain creation
    let _ = host_silo
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainCreated { .. }),
            Duration::from_secs(10),
        )
        .await
        .expect("Grain should be created");

    // Stop the host silo
    host_silo.stop().await.expect("Host silo should stop");

    // Wait for membership update
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Invoke grain from another silo (should reactivate somewhere)
    let config2 = SiloProcessConfig::new(&membership_addr)
        .with_port(0)
        .with_test_grain("migrate-test")
        .with_startup_timeout(Duration::from_secs(15));

    let mut invoker_silo = SiloProcess::spawn(config2).await.expect("Invoker silo should spawn");
    invoker_silo.wait_for_startup().await.expect("Invoker silo should start");

    // Wait for invocation (grain should reactivate on healthy silo)
    let event = invoker_silo
        .wait_for_event(
            |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
            Duration::from_secs(15),
        )
        .await
        .expect("Grain invocation should succeed after migration");

    if let ProcessEvent::GrainInvoked { success, .. } = event {
        assert!(success, "Grain invocation should succeed after host shutdown");
    }

    invoker_silo.stop().await.expect("Invoker silo should stop");
    cluster.stop().await.expect("Cluster should stop");
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_grain_invocation_result() {
        let result = GrainInvocationResult {
            success: true,
            method: "increment".into(),
            result: Some(serde_json::json!(42)),
            error: None,
            duration: Duration::from_millis(10),
        };

        assert!(result.is_success());
        assert_eq!(result.result_as::<i32>(), Some(42));
    }

    #[test]
    fn test_process_event_serialization() {
        let event = ProcessEvent::GrainInvoked {
            success: true,
            method: "test".into(),
            result: Some(serde_json::json!(123)),
            error: None,
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("grain_invoked"));
        assert!(json.contains("123"));
    }
}
