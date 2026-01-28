//! Benchmark harness utilities.
//!
//! This module provides utilities for setting up and running benchmarks,
//! including test data generation, timing utilities, and statistical helpers.

use bytes::{Bytes, BytesMut};
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, instrument, trace};

/// A timer for measuring operation latencies.
#[derive(Debug)]
pub struct LatencyTimer {
    start: Instant,
}

impl LatencyTimer {
    /// Start a new timer.
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Stop the timer and return elapsed duration.
    pub fn stop(&self) -> Duration {
        self.start.elapsed()
    }

    /// Stop the timer and return elapsed nanoseconds.
    pub fn stop_nanos(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    /// Stop the timer and return elapsed microseconds.
    pub fn stop_micros(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

/// Statistics from a benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkStats {
    /// Number of operations performed.
    pub count: u64,
    /// Total time spent (nanoseconds).
    pub total_nanos: u64,
    /// Minimum latency (nanoseconds).
    pub min_nanos: u64,
    /// Maximum latency (nanoseconds).
    pub max_nanos: u64,
    /// Mean latency (nanoseconds).
    pub mean_nanos: f64,
    /// Standard deviation (nanoseconds).
    pub std_dev_nanos: f64,
    /// p50 latency (nanoseconds).
    pub p50_nanos: u64,
    /// p95 latency (nanoseconds).
    pub p95_nanos: u64,
    /// p99 latency (nanoseconds).
    pub p99_nanos: u64,
}

impl BenchmarkStats {
    /// Create stats from a vector of latencies (in nanoseconds).
    #[instrument(skip(latencies), fields(sample_size = latencies.len()))]
    pub fn from_latencies(mut latencies: Vec<u64>) -> Self {
        if latencies.is_empty() {
            return Self::empty();
        }

        latencies.sort_unstable();
        let count = latencies.len() as u64;
        let total_nanos: u64 = latencies.iter().sum();
        let min_nanos = *latencies.first().unwrap();
        let max_nanos = *latencies.last().unwrap();
        let mean_nanos = total_nanos as f64 / count as f64;

        let variance: f64 = latencies
            .iter()
            .map(|&x| {
                let diff = x as f64 - mean_nanos;
                diff * diff
            })
            .sum::<f64>()
            / count as f64;
        let std_dev_nanos = variance.sqrt();

        let p50_idx = (count as f64 * 0.50) as usize;
        let p95_idx = (count as f64 * 0.95) as usize;
        let p99_idx = (count as f64 * 0.99) as usize;

        trace!(
            count,
            mean_nanos,
            min_nanos,
            max_nanos,
            "Computed benchmark statistics"
        );

        Self {
            count,
            total_nanos,
            min_nanos,
            max_nanos,
            mean_nanos,
            std_dev_nanos,
            p50_nanos: latencies.get(p50_idx).copied().unwrap_or(0),
            p95_nanos: latencies.get(p95_idx).copied().unwrap_or(0),
            p99_nanos: latencies.get(p99_idx).copied().unwrap_or(0),
        }
    }

    /// Create empty stats.
    pub fn empty() -> Self {
        Self {
            count: 0,
            total_nanos: 0,
            min_nanos: 0,
            max_nanos: 0,
            mean_nanos: 0.0,
            std_dev_nanos: 0.0,
            p50_nanos: 0,
            p95_nanos: 0,
            p99_nanos: 0,
        }
    }

    /// Get throughput in operations per second.
    pub fn throughput_per_sec(&self) -> f64 {
        if self.total_nanos == 0 {
            return 0.0;
        }
        (self.count as f64 * 1_000_000_000.0) / self.total_nanos as f64
    }

    /// Get mean latency in microseconds.
    pub fn mean_micros(&self) -> f64 {
        self.mean_nanos / 1000.0
    }

    /// Get p95 latency in microseconds.
    pub fn p95_micros(&self) -> f64 {
        self.p95_nanos as f64 / 1000.0
    }

    /// Get p99 latency in microseconds.
    pub fn p99_micros(&self) -> f64 {
        self.p99_nanos as f64 / 1000.0
    }
}

impl std::fmt::Display for BenchmarkStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "count={}, mean={:.2}µs, p50={:.2}µs, p95={:.2}µs, p99={:.2}µs, throughput={:.2}/s",
            self.count,
            self.mean_micros(),
            self.p50_nanos as f64 / 1000.0,
            self.p95_micros(),
            self.p99_micros(),
            self.throughput_per_sec()
        )
    }
}

/// Generator for test data used in benchmarks.
#[derive(Debug)]
pub struct TestDataGenerator {
    /// Random seed for reproducibility.
    seed: u64,
    /// Counter for unique IDs.
    counter: AtomicU64,
}

impl Default for TestDataGenerator {
    fn default() -> Self {
        Self::new()
    }
}

impl TestDataGenerator {
    /// Create a new test data generator.
    pub fn new() -> Self {
        Self {
            seed: 42,
            counter: AtomicU64::new(0),
        }
    }

    /// Create a generator with a specific seed.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            seed,
            counter: AtomicU64::new(0),
        }
    }

    /// Generate a random GrainId.
    #[instrument(skip(self))]
    pub fn grain_id(&self) -> GrainId {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let grain_type = GrainType::create(&format!("bench.grain.{}", counter % 100));
        let key = IdSpan::from_str(&format!("key-{}", counter));
        trace!(counter, "Generated GrainId");
        GrainId::new(grain_type, key)
    }

    /// Generate a batch of random GrainIds.
    #[instrument(skip(self))]
    pub fn grain_ids(&self, count: usize) -> Vec<GrainId> {
        debug!(count, "Generating batch of GrainIds");
        (0..count).map(|_| self.grain_id()).collect()
    }

    /// Generate a random SiloAddress.
    #[instrument(skip(self))]
    pub fn silo_address(&self) -> SiloAddress {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        let port = 30000 + (counter % 1000) as u16;
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), port));
        let generation = (counter / 1000) as i64 + 1;
        trace!(port, generation, "Generated SiloAddress");
        SiloAddress::new(addr, generation)
    }

    /// Generate a batch of random SiloAddresses.
    #[instrument(skip(self))]
    pub fn silo_addresses(&self, count: usize) -> Vec<SiloAddress> {
        debug!(count, "Generating batch of SiloAddresses");
        (0..count).map(|_| self.silo_address()).collect()
    }

    /// Generate a random ActivationId.
    #[instrument(skip(self))]
    pub fn activation_id(&self) -> ActivationId {
        let id = ActivationId::new();
        trace!(activation_id = %id, "Generated ActivationId");
        id
    }

    /// Generate a random GrainAddress.
    #[instrument(skip(self))]
    pub fn grain_address(&self) -> GrainAddress {
        let grain_id = self.grain_id();
        let activation_id = self.activation_id();
        let silo_address = self.silo_address();
        trace!("Generated complete GrainAddress");
        GrainAddress::new(grain_id, activation_id, Some(silo_address))
    }

    /// Generate random bytes.
    #[instrument(skip(self))]
    pub fn random_bytes(&self, size: usize) -> Bytes {
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..size).map(|_| rng.gen()).collect();
        trace!(size, "Generated random bytes");
        Bytes::from(bytes)
    }

    /// Generate a random string.
    #[instrument(skip(self))]
    pub fn random_string(&self, length: usize) -> String {
        let rng = rand::thread_rng();
        let s: String = rng
            .sample_iter(&Alphanumeric)
            .take(length)
            .map(char::from)
            .collect();
        trace!(length, "Generated random string");
        s
    }

    /// Generate multiple random strings.
    #[instrument(skip(self))]
    pub fn random_strings(&self, count: usize, length: usize) -> Vec<String> {
        debug!(count, length, "Generating batch of random strings");
        (0..count).map(|_| self.random_string(length)).collect()
    }

    /// Generate random i64 values.
    #[instrument(skip(self))]
    pub fn random_i64s(&self, count: usize) -> Vec<i64> {
        let mut rng = rand::thread_rng();
        debug!(count, "Generating batch of random i64s");
        (0..count).map(|_| rng.gen()).collect()
    }

    /// Generate random u64 values.
    #[instrument(skip(self))]
    pub fn random_u64s(&self, count: usize) -> Vec<u64> {
        let mut rng = rand::thread_rng();
        debug!(count, "Generating batch of random u64s");
        (0..count).map(|_| rng.gen()).collect()
    }
}

/// A simple benchmark runner for measuring operation performance.
#[derive(Debug)]
pub struct BenchmarkRunner {
    /// Name of the benchmark.
    name: String,
    /// Warmup iterations.
    warmup_iterations: usize,
    /// Measurement iterations.
    measurement_iterations: usize,
}

impl BenchmarkRunner {
    /// Create a new benchmark runner.
    #[instrument(skip(name))]
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        info!(name = %name, "Creating benchmark runner");
        Self {
            name,
            warmup_iterations: 100,
            measurement_iterations: 1000,
        }
    }

    /// Set warmup iterations.
    pub fn with_warmup(mut self, iterations: usize) -> Self {
        self.warmup_iterations = iterations;
        self
    }

    /// Set measurement iterations.
    pub fn with_iterations(mut self, iterations: usize) -> Self {
        self.measurement_iterations = iterations;
        self
    }

    /// Run a synchronous benchmark.
    #[instrument(skip(operation), fields(name = %self.name))]
    pub fn run<F>(&self, mut operation: F) -> BenchmarkStats
    where
        F: FnMut(),
    {
        info!(
            warmup = self.warmup_iterations,
            iterations = self.measurement_iterations,
            "Starting benchmark"
        );

        // Warmup
        debug!("Running warmup iterations");
        for _ in 0..self.warmup_iterations {
            operation();
        }

        // Measurement
        debug!("Running measurement iterations");
        let mut latencies = Vec::with_capacity(self.measurement_iterations);
        for _ in 0..self.measurement_iterations {
            let timer = LatencyTimer::start();
            operation();
            latencies.push(timer.stop_nanos());
        }

        let stats = BenchmarkStats::from_latencies(latencies);
        info!(
            mean_us = stats.mean_micros(),
            p95_us = stats.p95_micros(),
            throughput = stats.throughput_per_sec(),
            "Benchmark completed"
        );
        stats
    }

    /// Run a benchmark with setup and teardown.
    #[instrument(skip(setup, operation), fields(name = %self.name))]
    pub fn run_with_setup<S, F, T>(&self, mut setup: S, mut operation: F) -> BenchmarkStats
    where
        S: FnMut() -> T,
        F: FnMut(T),
    {
        info!("Starting benchmark with setup");

        // Warmup
        for _ in 0..self.warmup_iterations {
            let input = setup();
            operation(input);
        }

        // Measurement
        let mut latencies = Vec::with_capacity(self.measurement_iterations);
        for _ in 0..self.measurement_iterations {
            let input = setup();
            let timer = LatencyTimer::start();
            operation(input);
            latencies.push(timer.stop_nanos());
        }

        let stats = BenchmarkStats::from_latencies(latencies);
        info!(stats = %stats, "Benchmark with setup completed");
        stats
    }
}

/// Utility functions for common benchmark patterns.
pub mod utils {
    use super::*;

    /// Create a buffer for serialization benchmarks.
    pub fn create_buffer(capacity: usize) -> BytesMut {
        BytesMut::with_capacity(capacity)
    }

    /// Measure the overhead of an empty operation (baseline).
    #[instrument]
    pub fn measure_baseline(iterations: usize) -> BenchmarkStats {
        debug!(iterations, "Measuring baseline overhead");
        let runner = BenchmarkRunner::new("baseline")
            .with_warmup(iterations / 10)
            .with_iterations(iterations);
        runner.run(|| {
            std::hint::black_box(());
        })
    }

    /// Convert nanoseconds to a human-readable string.
    pub fn format_nanos(nanos: u64) -> String {
        if nanos < 1000 {
            format!("{}ns", nanos)
        } else if nanos < 1_000_000 {
            format!("{:.2}µs", nanos as f64 / 1000.0)
        } else if nanos < 1_000_000_000 {
            format!("{:.2}ms", nanos as f64 / 1_000_000.0)
        } else {
            format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
        }
    }

    /// Format throughput to a human-readable string.
    pub fn format_throughput(ops_per_sec: f64) -> String {
        if ops_per_sec < 1000.0 {
            format!("{:.2} ops/s", ops_per_sec)
        } else if ops_per_sec < 1_000_000.0 {
            format!("{:.2}K ops/s", ops_per_sec / 1000.0)
        } else {
            format!("{:.2}M ops/s", ops_per_sec / 1_000_000.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_latency_timer() {
        let timer = LatencyTimer::start();
        std::thread::sleep(Duration::from_millis(1));
        let elapsed = timer.stop();
        assert!(elapsed >= Duration::from_millis(1));
    }

    #[test]
    fn test_benchmark_stats_from_latencies() {
        let latencies = vec![100, 200, 300, 400, 500];
        let stats = BenchmarkStats::from_latencies(latencies);
        assert_eq!(stats.count, 5);
        assert_eq!(stats.min_nanos, 100);
        assert_eq!(stats.max_nanos, 500);
        assert!((stats.mean_nanos - 300.0).abs() < 0.001);
    }

    #[test]
    fn test_benchmark_stats_empty() {
        let stats = BenchmarkStats::from_latencies(vec![]);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.mean_nanos, 0.0);
    }

    #[test]
    fn test_benchmark_stats_display() {
        let latencies = vec![1000, 2000, 3000];
        let stats = BenchmarkStats::from_latencies(latencies);
        let display = stats.to_string();
        assert!(display.contains("count=3"));
        assert!(display.contains("µs"));
    }

    #[test]
    fn test_test_data_generator_grain_id() {
        let gen = TestDataGenerator::new();
        let id1 = gen.grain_id();
        let id2 = gen.grain_id();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_test_data_generator_silo_address() {
        let gen = TestDataGenerator::new();
        let addr1 = gen.silo_address();
        let addr2 = gen.silo_address();
        assert_ne!(addr1, addr2);
    }

    #[test]
    fn test_test_data_generator_random_bytes() {
        let gen = TestDataGenerator::new();
        let bytes = gen.random_bytes(1024);
        assert_eq!(bytes.len(), 1024);
    }

    #[test]
    fn test_test_data_generator_random_string() {
        let gen = TestDataGenerator::new();
        let s = gen.random_string(32);
        assert_eq!(s.len(), 32);
    }

    #[test]
    fn test_benchmark_runner_simple() {
        let runner = BenchmarkRunner::new("test")
            .with_warmup(10)
            .with_iterations(100);
        let mut counter = 0u64;
        let stats = runner.run(|| {
            counter += 1;
        });
        assert_eq!(stats.count, 100);
        assert_eq!(counter, 110); // 10 warmup + 100 measurement
    }

    #[test]
    fn test_utils_format_nanos() {
        assert_eq!(utils::format_nanos(500), "500ns");
        assert_eq!(utils::format_nanos(1500), "1.50µs");
        assert_eq!(utils::format_nanos(1_500_000), "1.50ms");
        assert_eq!(utils::format_nanos(1_500_000_000), "1.50s");
    }

    #[test]
    fn test_utils_format_throughput() {
        assert_eq!(utils::format_throughput(500.0), "500.00 ops/s");
        assert_eq!(utils::format_throughput(5000.0), "5.00K ops/s");
        assert_eq!(utils::format_throughput(5_000_000.0), "5.00M ops/s");
    }

    #[test]
    fn test_benchmark_stats_throughput() {
        let latencies: Vec<u64> = (0..1000).map(|_| 1000).collect(); // 1µs each
        let stats = BenchmarkStats::from_latencies(latencies);
        // 1000 ops in 1ms total = 1M ops/sec
        assert!(stats.throughput_per_sec() > 900_000.0);
    }
}
