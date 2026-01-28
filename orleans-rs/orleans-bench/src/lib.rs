//! Orleans-RS Performance Benchmarks
//!
//! This crate provides comprehensive performance benchmarking infrastructure
//! for the Orleans-RS distributed actor framework. It includes:
//!
//! - **Micro-benchmarks**: Low-level performance measurements for core operations
//! - **Macro-benchmarks**: End-to-end latency and throughput measurements
//! - **Scalability benchmarks**: Performance under varying load conditions
//! - **Reporting**: Result collection and analysis tools
//!
//! # Benchmark Categories
//!
//! ## Serialization Benchmarks
//! - VarInt encoding/decoding throughput
//! - Primitive type serialization
//! - Complex struct serialization
//! - Identity type serialization (GrainId, SiloAddress, etc.)
//!
//! ## Messaging Benchmarks
//! - Message creation overhead
//! - Message serialization/deserialization
//! - Correlation ID generation
//!
//! ## Activation Benchmarks
//! - Grain activation/deactivation cost
//! - Catalog lookup performance
//! - State transition overhead
//!
//! ## Directory Benchmarks
//! - Consistent hash ring operations
//! - Directory lookup latency
//! - Cache hit/miss performance
//!
//! ## End-to-End Latency Benchmarks
//! - Local grain call latency
//! - Cross-activation call latency
//!
//! # Usage
//!
//! Run all benchmarks:
//! ```bash
//! cargo bench -p orleans-bench
//! ```
//!
//! Run specific benchmark:
//! ```bash
//! cargo bench -p orleans-bench -- serialization
//! ```
//!
//! Generate HTML reports:
//! ```bash
//! cargo bench -p orleans-bench -- --save-baseline main
//! ```

pub mod harness;
pub mod reporting;

pub use harness::*;
pub use reporting::*;

use std::time::Duration;

/// Configuration for benchmark runs.
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of warmup iterations before measurement.
    pub warmup_iterations: usize,
    /// Number of measurement iterations.
    pub measurement_iterations: usize,
    /// Sample size for statistical analysis.
    pub sample_size: usize,
    /// Confidence level for statistical analysis (0.0-1.0).
    pub confidence_level: f64,
    /// Maximum time to spend on a single benchmark.
    pub measurement_time: Duration,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: 100,
            measurement_iterations: 1000,
            sample_size: 100,
            confidence_level: 0.95,
            measurement_time: Duration::from_secs(5),
        }
    }
}

impl BenchmarkConfig {
    /// Create a new benchmark configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a quick configuration for development.
    pub fn quick() -> Self {
        Self {
            warmup_iterations: 10,
            measurement_iterations: 100,
            sample_size: 20,
            confidence_level: 0.95,
            measurement_time: Duration::from_secs(1),
        }
    }

    /// Create a thorough configuration for CI.
    pub fn thorough() -> Self {
        Self {
            warmup_iterations: 500,
            measurement_iterations: 5000,
            sample_size: 500,
            confidence_level: 0.99,
            measurement_time: Duration::from_secs(30),
        }
    }

    /// Set warmup iterations.
    pub fn with_warmup_iterations(mut self, iterations: usize) -> Self {
        self.warmup_iterations = iterations;
        self
    }

    /// Set measurement iterations.
    pub fn with_measurement_iterations(mut self, iterations: usize) -> Self {
        self.measurement_iterations = iterations;
        self
    }

    /// Set sample size.
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = size;
        self
    }

    /// Set measurement time.
    pub fn with_measurement_time(mut self, time: Duration) -> Self {
        self.measurement_time = time;
        self
    }
}

/// Performance targets for Orleans-RS benchmarks.
///
/// These represent the expected performance characteristics
/// that benchmarks should validate against.
#[derive(Debug, Clone)]
pub struct PerformanceTargets {
    /// Target serialization throughput (messages/second).
    pub serialization_throughput: u64,
    /// Target grain call latency p95 (microseconds).
    pub grain_call_latency_p95_us: u64,
    /// Target cross-silo call latency p95 (microseconds).
    pub cross_silo_latency_p95_us: u64,
    /// Target activation rate (activations/second).
    pub activation_rate: u64,
    /// Target directory lookup latency p95 (microseconds).
    pub directory_lookup_p95_us: u64,
}

impl Default for PerformanceTargets {
    fn default() -> Self {
        Self {
            serialization_throughput: 1_000_000,  // 1M messages/sec
            grain_call_latency_p95_us: 1000,      // 1ms p95
            cross_silo_latency_p95_us: 5000,      // 5ms p95
            activation_rate: 10_000,              // 10K activations/sec
            directory_lookup_p95_us: 100,         // 100µs p95
        }
    }
}

impl PerformanceTargets {
    /// Check if the observed performance meets targets.
    pub fn check_serialization(&self, observed_throughput: u64) -> bool {
        observed_throughput >= self.serialization_throughput
    }

    /// Check if the observed latency meets targets.
    pub fn check_grain_call_latency(&self, observed_p95_us: u64) -> bool {
        observed_p95_us <= self.grain_call_latency_p95_us
    }

    /// Check if the observed activation rate meets targets.
    pub fn check_activation_rate(&self, observed_rate: u64) -> bool {
        observed_rate >= self.activation_rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_config_default() {
        let config = BenchmarkConfig::default();
        assert_eq!(config.warmup_iterations, 100);
        assert_eq!(config.measurement_iterations, 1000);
        assert_eq!(config.sample_size, 100);
        assert!((config.confidence_level - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn test_benchmark_config_quick() {
        let config = BenchmarkConfig::quick();
        assert_eq!(config.warmup_iterations, 10);
        assert_eq!(config.measurement_iterations, 100);
    }

    #[test]
    fn test_benchmark_config_thorough() {
        let config = BenchmarkConfig::thorough();
        assert_eq!(config.warmup_iterations, 500);
        assert_eq!(config.measurement_iterations, 5000);
    }

    #[test]
    fn test_benchmark_config_builder() {
        let config = BenchmarkConfig::new()
            .with_warmup_iterations(50)
            .with_measurement_iterations(500)
            .with_sample_size(50);
        assert_eq!(config.warmup_iterations, 50);
        assert_eq!(config.measurement_iterations, 500);
        assert_eq!(config.sample_size, 50);
    }

    #[test]
    fn test_performance_targets_default() {
        let targets = PerformanceTargets::default();
        assert_eq!(targets.serialization_throughput, 1_000_000);
        assert_eq!(targets.grain_call_latency_p95_us, 1000);
        assert_eq!(targets.activation_rate, 10_000);
    }

    #[test]
    fn test_performance_targets_check_serialization() {
        let targets = PerformanceTargets::default();
        assert!(targets.check_serialization(1_500_000));
        assert!(!targets.check_serialization(500_000));
    }

    #[test]
    fn test_performance_targets_check_grain_call_latency() {
        let targets = PerformanceTargets::default();
        assert!(targets.check_grain_call_latency(500));
        assert!(!targets.check_grain_call_latency(1500));
    }

    #[test]
    fn test_performance_targets_check_activation_rate() {
        let targets = PerformanceTargets::default();
        assert!(targets.check_activation_rate(15_000));
        assert!(!targets.check_activation_rate(5_000));
    }
}
