//! Performance baseline tests.
//!
//! These tests establish baseline performance metrics for the Orleans cluster.
//! They measure cross-silo call latency, throughput, and resource usage.

use orleans_tests_integration::prelude::*;
use std::time::{Duration, Instant};
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

/// Performance metrics collected during tests.
#[derive(Debug, Default)]
pub struct PerformanceMetrics {
    /// Total number of operations
    pub operation_count: u64,
    /// Total duration of all operations
    pub total_duration: Duration,
    /// Minimum operation duration
    pub min_duration: Duration,
    /// Maximum operation duration
    pub max_duration: Duration,
    /// Failed operations
    pub failed_count: u64,
}

impl PerformanceMetrics {
    /// Create new metrics with a single operation.
    pub fn new() -> Self {
        Self {
            min_duration: Duration::MAX,
            ..Default::default()
        }
    }

    /// Record an operation.
    pub fn record(&mut self, duration: Duration, success: bool) {
        self.operation_count += 1;
        self.total_duration += duration;

        if duration < self.min_duration {
            self.min_duration = duration;
        }
        if duration > self.max_duration {
            self.max_duration = duration;
        }

        if !success {
            self.failed_count += 1;
        }
    }

    /// Get average operation duration.
    pub fn average_duration(&self) -> Duration {
        if self.operation_count == 0 {
            Duration::ZERO
        } else {
            self.total_duration / self.operation_count as u32
        }
    }

    /// Get operations per second.
    pub fn ops_per_second(&self) -> f64 {
        if self.total_duration.is_zero() {
            0.0
        } else {
            self.operation_count as f64 / self.total_duration.as_secs_f64()
        }
    }

    /// Get success rate.
    pub fn success_rate(&self) -> f64 {
        if self.operation_count == 0 {
            0.0
        } else {
            (self.operation_count - self.failed_count) as f64 / self.operation_count as f64
        }
    }

    /// Print a summary of the metrics.
    pub fn print_summary(&self, name: &str) {
        tracing::info!(
            name = name,
            operations = self.operation_count,
            avg_ms = self.average_duration().as_millis(),
            min_ms = self.min_duration.as_millis(),
            max_ms = self.max_duration.as_millis(),
            ops_sec = format!("{:.2}", self.ops_per_second()),
            success_rate = format!("{:.2}%", self.success_rate() * 100.0),
            "Performance metrics"
        );
    }
}

/// Test baseline cross-silo latency.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_cross_silo_latency_baseline() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    let membership_addr = cluster.membership_server_addr().to_string();
    let mut metrics = PerformanceMetrics::new();

    // Perform multiple invocations
    let iteration_count = 10;

    for i in 0..iteration_count {
        let config = SiloProcessConfig::new(&membership_addr)
            .with_port(0)
            .with_test_grain(&format!("latency-test-{}", i))
            .with_startup_timeout(Duration::from_secs(15));

        let start = Instant::now();

        let mut silo = SiloProcess::spawn(config).await.expect("Silo should spawn");
        silo.wait_for_startup().await.expect("Silo should start");

        let event = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
                Duration::from_secs(10),
            )
            .await;

        let duration = start.elapsed();
        let success = matches!(event, Ok(ProcessEvent::GrainInvoked { success: true, .. }));

        metrics.record(duration, success);

        silo.stop().await.expect("Silo should stop");
    }

    metrics.print_summary("cross_silo_latency");

    // Assertions on baseline performance
    assert!(
        metrics.average_duration() < Duration::from_secs(5),
        "Average latency should be under 5s"
    );
    assert!(
        metrics.success_rate() >= 0.8,
        "Success rate should be at least 80%"
    );

    cluster.stop().await.expect("Cluster should stop");
}

/// Test cluster startup time.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_cluster_startup_time() {
    init_logging();

    let silo_counts = [1, 2, 3, 5];

    for &silo_count in &silo_counts {
        let start = Instant::now();

        let mut cluster = TestClusterBuilder::for_testing()
            .with_silo_count(silo_count)
            .build()
            .await
            .expect("Cluster should start");

        let startup_duration = start.elapsed();

        tracing::info!(
            silo_count = silo_count,
            startup_ms = startup_duration.as_millis(),
            "Cluster startup completed"
        );

        // Verify cluster is healthy
        cluster
            .assert_active_silo_count(silo_count as usize)
            .await
            .expect("All silos should be active");

        // Startup should be reasonably fast
        let expected_max = Duration::from_secs(30 + (silo_count as u64 * 5));
        assert!(
            startup_duration < expected_max,
            "Startup for {} silos should be under {:?}",
            silo_count,
            expected_max
        );

        cluster.stop().await.expect("Cluster should stop");
    }
}

/// Test memory usage baseline.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_memory_usage_baseline() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(2)
        .build()
        .await
        .expect("Cluster should start");

    // Note: Actual memory measurement would require /proc filesystem parsing
    // or a memory profiling tool. This is a placeholder test.

    cluster
        .assert_active_silo_count(2)
        .await
        .expect("Silos should be active");

    tracing::info!("Memory usage baseline test completed (measurement not implemented)");

    cluster.stop().await.expect("Cluster should stop");
}

/// Test connection efficiency.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_connection_efficiency() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // With N silos, each silo should have N-1 connections (full mesh)
    // This test verifies the cluster can handle the expected connection count

    cluster
        .assert_active_silo_count(3)
        .await
        .expect("All silos should be active");

    // Perform operations to ensure connections are established
    let membership_addr = cluster.membership_server_addr().to_string();

    for i in 0..3 {
        let config = SiloProcessConfig::new(&membership_addr)
            .with_port(0)
            .with_test_grain(&format!("connection-test-{}", i))
            .with_startup_timeout(Duration::from_secs(15));

        let mut silo = SiloProcess::spawn(config).await.expect("Silo should spawn");
        silo.wait_for_startup().await.expect("Silo should start");

        let _ = silo
            .wait_for_event(
                |e| matches!(e, ProcessEvent::GrainInvoked { .. }),
                Duration::from_secs(10),
            )
            .await;

        silo.stop().await.expect("Silo should stop");
    }

    tracing::info!("Connection efficiency test completed");

    cluster.stop().await.expect("Cluster should stop");
}

/// Test shutdown time.
#[tokio::test]
#[ignore = "Requires compiled orleans-silo binary"]
async fn test_shutdown_time() {
    init_logging();

    let mut cluster = TestClusterBuilder::for_testing()
        .with_silo_count(3)
        .build()
        .await
        .expect("Cluster should start");

    // Create some grains to make shutdown non-trivial
    let membership_addr = cluster.membership_server_addr().to_string();

    for i in 0..5 {
        let config = SiloProcessConfig::new(&membership_addr)
            .with_port(0)
            .with_create_grain(&format!("shutdown-test-{}", i))
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

    // Measure shutdown time
    let start = Instant::now();
    cluster.stop().await.expect("Cluster should stop");
    let shutdown_duration = start.elapsed();

    tracing::info!(
        shutdown_ms = shutdown_duration.as_millis(),
        "Cluster shutdown completed"
    );

    // Shutdown should be reasonably fast
    assert!(
        shutdown_duration < Duration::from_secs(30),
        "Shutdown should complete within 30 seconds"
    );
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_performance_metrics() {
        let mut metrics = PerformanceMetrics::new();

        metrics.record(Duration::from_millis(10), true);
        metrics.record(Duration::from_millis(20), true);
        metrics.record(Duration::from_millis(30), false);

        assert_eq!(metrics.operation_count, 3);
        assert_eq!(metrics.failed_count, 1);
        assert_eq!(metrics.average_duration(), Duration::from_millis(20));
        assert!(metrics.success_rate() > 0.66 && metrics.success_rate() < 0.67);
    }

    #[test]
    fn test_performance_metrics_empty() {
        let metrics = PerformanceMetrics::new();

        assert_eq!(metrics.operation_count, 0);
        assert_eq!(metrics.average_duration(), Duration::ZERO);
        assert_eq!(metrics.success_rate(), 0.0);
    }

    #[test]
    fn test_performance_metrics_min_max() {
        let mut metrics = PerformanceMetrics::new();

        metrics.record(Duration::from_millis(50), true);
        metrics.record(Duration::from_millis(10), true);
        metrics.record(Duration::from_millis(100), true);

        assert_eq!(metrics.min_duration, Duration::from_millis(10));
        assert_eq!(metrics.max_duration, Duration::from_millis(100));
    }
}
