//! Continuous benchmarking infrastructure.
//!
//! This module provides tools for tracking benchmark performance over time,
//! detecting regressions using statistical analysis, and supporting CI integration.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use tracing::{debug, info, instrument, warn};

use crate::reporting::{BenchmarkBaseline, BenchmarkResult, ReportingResult};

/// Configuration for continuous benchmarking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousConfig {
    /// Regression threshold percentage (default: 10.0).
    pub regression_threshold: f64,
    /// Improvement threshold percentage (default: 10.0).
    pub improvement_threshold: f64,
    /// Minimum number of historical results needed for trend analysis (default: 5).
    pub min_history_for_trend: usize,
    /// Number of standard deviations for statistical significance (default: 2.0).
    pub significance_std_devs: f64,
    /// Window size for moving average calculation (default: 10).
    pub moving_average_window: usize,
    /// Maximum history entries to retain per benchmark (default: 100).
    pub max_history_entries: usize,
}

impl Default for ContinuousConfig {
    fn default() -> Self {
        Self {
            regression_threshold: 10.0,
            improvement_threshold: 10.0,
            min_history_for_trend: 5,
            significance_std_devs: 2.0,
            moving_average_window: 10,
            max_history_entries: 100,
        }
    }
}

impl ContinuousConfig {
    /// Create a strict configuration for CI.
    pub fn strict() -> Self {
        Self {
            regression_threshold: 5.0,
            improvement_threshold: 5.0,
            min_history_for_trend: 10,
            significance_std_devs: 2.5,
            moving_average_window: 15,
            max_history_entries: 200,
        }
    }

    /// Create a lenient configuration for development.
    pub fn lenient() -> Self {
        Self {
            regression_threshold: 20.0,
            improvement_threshold: 20.0,
            min_history_for_trend: 3,
            significance_std_devs: 1.5,
            moving_average_window: 5,
            max_history_entries: 50,
        }
    }
}

/// A data point in the benchmark history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryPoint {
    /// Timestamp of the benchmark run.
    pub timestamp: DateTime<Utc>,
    /// Git commit hash (if available).
    pub git_commit: Option<String>,
    /// Mean latency in nanoseconds.
    pub mean_nanos: f64,
    /// p95 latency in nanoseconds.
    pub p95_nanos: u64,
    /// p99 latency in nanoseconds.
    pub p99_nanos: u64,
    /// Throughput in operations per second.
    pub throughput_per_sec: f64,
    /// Standard deviation in nanoseconds.
    pub std_dev_nanos: f64,
}

impl From<&BenchmarkResult> for HistoryPoint {
    fn from(result: &BenchmarkResult) -> Self {
        Self {
            timestamp: result.timestamp,
            git_commit: result.git_commit.clone(),
            mean_nanos: result.mean_nanos,
            p95_nanos: result.p95_nanos,
            p99_nanos: result.p99_nanos,
            throughput_per_sec: result.throughput_per_sec,
            std_dev_nanos: result.std_dev_nanos,
        }
    }
}

/// Benchmark history for a single benchmark.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BenchmarkHistory {
    /// Name of the benchmark.
    pub name: String,
    /// Category of the benchmark.
    pub category: String,
    /// Historical data points.
    pub points: Vec<HistoryPoint>,
}

impl BenchmarkHistory {
    /// Create a new benchmark history.
    pub fn new(name: impl Into<String>, category: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            category: category.into(),
            points: Vec::new(),
        }
    }

    /// Add a data point to the history.
    pub fn add_point(&mut self, point: HistoryPoint) {
        self.points.push(point);
        // Keep points sorted by timestamp
        self.points.sort_by_key(|p| p.timestamp);
    }

    /// Trim history to maximum entries.
    pub fn trim_to_max(&mut self, max_entries: usize) {
        if self.points.len() > max_entries {
            let to_remove = self.points.len() - max_entries;
            self.points.drain(0..to_remove);
        }
    }

    /// Get the most recent N points.
    pub fn recent_points(&self, n: usize) -> &[HistoryPoint] {
        let start = self.points.len().saturating_sub(n);
        &self.points[start..]
    }

    /// Calculate moving average of mean latencies.
    pub fn moving_average(&self, window: usize) -> Option<f64> {
        let recent = self.recent_points(window);
        if recent.is_empty() {
            return None;
        }
        let sum: f64 = recent.iter().map(|p| p.mean_nanos).sum();
        Some(sum / recent.len() as f64)
    }

    /// Calculate standard deviation of recent mean latencies.
    pub fn moving_std_dev(&self, window: usize) -> Option<f64> {
        let recent = self.recent_points(window);
        if recent.len() < 2 {
            return None;
        }
        let mean = self.moving_average(window)?;
        let variance: f64 = recent
            .iter()
            .map(|p| (p.mean_nanos - mean).powi(2))
            .sum::<f64>()
            / (recent.len() - 1) as f64;
        Some(variance.sqrt())
    }

    /// Get the trend direction (-1.0 to 1.0, negative = improving).
    pub fn trend_direction(&self, window: usize) -> Option<f64> {
        let recent = self.recent_points(window);
        if recent.len() < 3 {
            return None;
        }

        // Linear regression on mean latencies
        let n = recent.len() as f64;
        let x_mean = (n - 1.0) / 2.0;
        let y_mean: f64 = recent.iter().map(|p| p.mean_nanos).sum::<f64>() / n;

        let mut numerator = 0.0;
        let mut denominator = 0.0;
        for (i, point) in recent.iter().enumerate() {
            let x = i as f64;
            numerator += (x - x_mean) * (point.mean_nanos - y_mean);
            denominator += (x - x_mean).powi(2);
        }

        if denominator == 0.0 {
            return None;
        }

        // Normalize slope to -1.0 to 1.0 range
        let slope = numerator / denominator;
        let normalized = (slope / y_mean).tanh();
        Some(normalized)
    }
}

/// Statistical analysis result for a benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAnalysis {
    /// Benchmark name.
    pub name: String,
    /// Current value.
    pub current_mean_nanos: f64,
    /// Historical moving average.
    pub moving_average_nanos: Option<f64>,
    /// Historical standard deviation.
    pub moving_std_dev_nanos: Option<f64>,
    /// Z-score of current value relative to history.
    pub z_score: Option<f64>,
    /// Trend direction (-1.0 = improving, +1.0 = degrading).
    pub trend: Option<f64>,
    /// Whether this is a statistically significant regression.
    pub is_regression: bool,
    /// Whether this is a statistically significant improvement.
    pub is_improvement: bool,
    /// Confidence level (0.0-1.0).
    pub confidence: f64,
}

impl TrendAnalysis {
    /// Analyze current result against history.
    #[instrument(skip(history, current))]
    pub fn analyze(
        history: &BenchmarkHistory,
        current: &BenchmarkResult,
        config: &ContinuousConfig,
    ) -> Self {
        let moving_average = history.moving_average(config.moving_average_window);
        let moving_std_dev = history.moving_std_dev(config.moving_average_window);
        let trend = history.trend_direction(config.moving_average_window);

        let z_score = match (moving_average, moving_std_dev) {
            (Some(avg), Some(std_dev)) if std_dev > 0.0 => {
                Some((current.mean_nanos - avg) / std_dev)
            }
            _ => None,
        };

        let is_regression = z_score
            .map(|z| z > config.significance_std_devs)
            .unwrap_or(false);
        let is_improvement = z_score
            .map(|z| z < -config.significance_std_devs)
            .unwrap_or(false);

        // Calculate confidence based on history size
        let history_size = history.points.len();
        let confidence = if history_size >= config.min_history_for_trend {
            1.0 - (1.0 / (history_size as f64).sqrt())
        } else {
            history_size as f64 / config.min_history_for_trend as f64
        };

        debug!(
            name = %current.name,
            z_score = ?z_score,
            is_regression,
            is_improvement,
            confidence,
            "Trend analysis completed"
        );

        Self {
            name: current.name.clone(),
            current_mean_nanos: current.mean_nanos,
            moving_average_nanos: moving_average,
            moving_std_dev_nanos: moving_std_dev,
            z_score,
            trend,
            is_regression,
            is_improvement,
            confidence,
        }
    }
}

impl std::fmt::Display for TrendAnalysis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.is_regression {
            "REGRESSION"
        } else if self.is_improvement {
            "IMPROVEMENT"
        } else {
            "STABLE"
        };

        let trend_indicator = match self.trend {
            Some(t) if t > 0.1 => "↑",
            Some(t) if t < -0.1 => "↓",
            _ => "→",
        };

        write!(
            f,
            "{}: {} {} (z={:.2}, conf={:.0}%)",
            self.name,
            status,
            trend_indicator,
            self.z_score.unwrap_or(0.0),
            self.confidence * 100.0
        )
    }
}

/// Storage manager for continuous benchmarking.
#[derive(Debug)]
pub struct ContinuousStorage {
    /// Base directory for storage.
    base_dir: PathBuf,
    /// Configuration.
    config: ContinuousConfig,
}

impl ContinuousStorage {
    /// Create a new continuous storage manager.
    #[instrument(skip(base_dir))]
    pub fn new(base_dir: impl Into<PathBuf>, config: ContinuousConfig) -> ReportingResult<Self> {
        let base_dir = base_dir.into();
        info!(path = %base_dir.display(), "Initializing continuous storage");
        fs::create_dir_all(&base_dir)?;
        fs::create_dir_all(base_dir.join("history"))?;
        Ok(Self { base_dir, config })
    }

    /// Get default storage location.
    pub fn default_location() -> PathBuf {
        PathBuf::from("target/bench-continuous")
    }

    /// Save a benchmark history.
    #[instrument(skip(history))]
    pub fn save_history(&self, history: &BenchmarkHistory) -> ReportingResult<()> {
        let path = self.history_path(&history.name);
        debug!(path = %path.display(), "Saving history");
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, history)?;
        Ok(())
    }

    /// Load a benchmark history.
    #[instrument]
    pub fn load_history(&self, name: &str) -> ReportingResult<BenchmarkHistory> {
        let path = self.history_path(name);
        debug!(path = %path.display(), "Loading history");
        if !path.exists() {
            return Ok(BenchmarkHistory::new(name, "unknown"));
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let history = serde_json::from_reader(reader)?;
        Ok(history)
    }

    /// Record a new benchmark result.
    #[instrument(skip(result))]
    pub fn record_result(&self, result: &BenchmarkResult) -> ReportingResult<TrendAnalysis> {
        let mut history = self.load_history(&result.name)?;
        if history.category.is_empty() || history.category == "unknown" {
            history.category = result.category.clone();
        }

        // Add new point
        history.add_point(HistoryPoint::from(result));

        // Trim to max entries
        history.trim_to_max(self.config.max_history_entries);

        // Save updated history
        self.save_history(&history)?;

        // Analyze trend
        let analysis = TrendAnalysis::analyze(&history, result, &self.config);

        info!(
            name = %result.name,
            is_regression = analysis.is_regression,
            is_improvement = analysis.is_improvement,
            "Recorded benchmark result"
        );

        Ok(analysis)
    }

    /// Record multiple results and return analyses.
    #[instrument(skip(results))]
    pub fn record_results(
        &self,
        results: &[BenchmarkResult],
    ) -> ReportingResult<Vec<TrendAnalysis>> {
        let mut analyses = Vec::new();
        for result in results {
            let analysis = self.record_result(result)?;
            analyses.push(analysis);
        }
        Ok(analyses)
    }

    /// List all benchmark histories.
    pub fn list_histories(&self) -> ReportingResult<Vec<String>> {
        let history_dir = self.base_dir.join("history");
        let mut names = Vec::new();
        for entry in fs::read_dir(&history_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    names.push(name.to_string());
                }
            }
        }
        Ok(names)
    }

    /// Get all histories.
    #[instrument(skip(self))]
    pub fn load_all_histories(&self) -> ReportingResult<Vec<BenchmarkHistory>> {
        let names = self.list_histories()?;
        let mut histories = Vec::new();
        for name in names {
            match self.load_history(&name) {
                Ok(h) => histories.push(h),
                Err(e) => warn!(name, error = %e, "Failed to load history"),
            }
        }
        Ok(histories)
    }

    /// Auto-create baseline from recent history.
    #[instrument]
    pub fn auto_baseline(&self, name: &str) -> ReportingResult<BenchmarkBaseline> {
        let histories = self.load_all_histories()?;
        let mut baseline = BenchmarkBaseline::new(name);

        for history in histories {
            if history.points.is_empty() {
                continue;
            }

            // Use moving average as baseline
            if let Some(avg) = history.moving_average(self.config.moving_average_window) {
                let recent = history.recent_points(1);
                if let Some(latest) = recent.first() {
                    let result = BenchmarkResult {
                        name: history.name.clone(),
                        category: history.category.clone(),
                        timestamp: Utc::now(),
                        git_commit: latest.git_commit.clone(),
                        count: 0,
                        total_nanos: 0,
                        mean_nanos: avg,
                        std_dev_nanos: history.moving_std_dev(self.config.moving_average_window).unwrap_or(0.0),
                        p50_nanos: 0,
                        p95_nanos: latest.p95_nanos,
                        p99_nanos: latest.p99_nanos,
                        throughput_per_sec: latest.throughput_per_sec,
                        metadata: HashMap::new(),
                    };
                    baseline.add_result(result);
                }
            }
        }

        baseline.description = Some(format!(
            "Auto-generated baseline from {} point moving average",
            self.config.moving_average_window
        ));

        Ok(baseline)
    }

    fn history_path(&self, name: &str) -> PathBuf {
        // Sanitize name for filesystem
        let safe_name: String = name
            .chars()
            .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
            .collect();
        self.base_dir.join("history").join(format!("{}.json", safe_name))
    }
}

/// Continuous benchmark runner with automatic regression detection.
#[derive(Debug)]
pub struct ContinuousRunner {
    /// Storage manager.
    storage: ContinuousStorage,
    /// Configuration (used for future extensions).
    #[allow(dead_code)]
    config: ContinuousConfig,
}

impl ContinuousRunner {
    /// Create a new continuous benchmark runner.
    pub fn new(storage: ContinuousStorage, config: ContinuousConfig) -> Self {
        Self { storage, config }
    }

    /// Create with default configuration.
    pub fn with_defaults(base_dir: impl Into<PathBuf>) -> ReportingResult<Self> {
        let config = ContinuousConfig::default();
        let storage = ContinuousStorage::new(base_dir, config.clone())?;
        Ok(Self { storage, config })
    }

    /// Run analysis on a batch of results.
    #[instrument(skip(results))]
    pub fn run_analysis(&self, results: &[BenchmarkResult]) -> ReportingResult<AnalysisReport> {
        let analyses = self.storage.record_results(results)?;

        let regressions: Vec<_> = analyses.iter().filter(|a| a.is_regression).cloned().collect();
        let improvements: Vec<_> = analyses.iter().filter(|a| a.is_improvement).cloned().collect();

        let has_regressions = !regressions.is_empty();

        info!(
            total = analyses.len(),
            regressions = regressions.len(),
            improvements = improvements.len(),
            "Analysis complete"
        );

        Ok(AnalysisReport {
            timestamp: Utc::now(),
            git_commit: get_git_commit(),
            analyses,
            regressions,
            improvements,
            has_regressions,
        })
    }

    /// Get storage reference.
    pub fn storage(&self) -> &ContinuousStorage {
        &self.storage
    }
}

/// Report from continuous benchmark analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisReport {
    /// Timestamp of the analysis.
    pub timestamp: DateTime<Utc>,
    /// Git commit (if available).
    pub git_commit: Option<String>,
    /// All trend analyses.
    pub analyses: Vec<TrendAnalysis>,
    /// Detected regressions.
    pub regressions: Vec<TrendAnalysis>,
    /// Detected improvements.
    pub improvements: Vec<TrendAnalysis>,
    /// Whether any regressions were detected.
    pub has_regressions: bool,
}

impl AnalysisReport {
    /// Generate a summary string.
    pub fn summary(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "Benchmark Analysis Report ({})\n",
            self.timestamp.format("%Y-%m-%d %H:%M:%S UTC")
        ));
        if let Some(ref commit) = self.git_commit {
            s.push_str(&format!("Git Commit: {}\n", commit));
        }
        s.push_str(&format!(
            "\nResults: {} benchmarks, {} regressions, {} improvements\n",
            self.analyses.len(),
            self.regressions.len(),
            self.improvements.len()
        ));

        if !self.regressions.is_empty() {
            s.push_str("\nRegressions:\n");
            for r in &self.regressions {
                s.push_str(&format!("  - {}\n", r));
            }
        }

        if !self.improvements.is_empty() {
            s.push_str("\nImprovements:\n");
            for i in &self.improvements {
                s.push_str(&format!("  - {}\n", i));
            }
        }

        s
    }

    /// Get exit code for CI (0 = success, 1 = regressions found).
    pub fn exit_code(&self) -> i32 {
        if self.has_regressions { 1 } else { 0 }
    }
}

/// Get the current git commit hash.
fn get_git_commit() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout)
                    .ok()
                    .map(|s| s.trim().to_string())
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::harness::BenchmarkStats;
    use chrono::Duration;
    use tempfile::TempDir;

    fn create_test_result(name: &str, mean_nanos: f64) -> BenchmarkResult {
        let stats = BenchmarkStats {
            count: 1000,
            total_nanos: (mean_nanos * 1000.0) as u64,
            min_nanos: (mean_nanos * 0.5) as u64,
            max_nanos: (mean_nanos * 1.5) as u64,
            mean_nanos,
            std_dev_nanos: mean_nanos * 0.1,
            p50_nanos: mean_nanos as u64,
            p95_nanos: (mean_nanos * 1.2) as u64,
            p99_nanos: (mean_nanos * 1.4) as u64,
        };
        BenchmarkResult::from_stats(name, "test", &stats)
    }

    #[test]
    fn test_continuous_config_default() {
        let config = ContinuousConfig::default();
        assert!((config.regression_threshold - 10.0).abs() < f64::EPSILON);
        assert_eq!(config.min_history_for_trend, 5);
    }

    #[test]
    fn test_continuous_config_strict() {
        let config = ContinuousConfig::strict();
        assert!((config.regression_threshold - 5.0).abs() < f64::EPSILON);
        assert_eq!(config.min_history_for_trend, 10);
    }

    #[test]
    fn test_benchmark_history_new() {
        let history = BenchmarkHistory::new("test", "serialization");
        assert_eq!(history.name, "test");
        assert_eq!(history.category, "serialization");
        assert!(history.points.is_empty());
    }

    #[test]
    fn test_benchmark_history_add_point() {
        let mut history = BenchmarkHistory::new("test", "test");
        let point = HistoryPoint {
            timestamp: Utc::now(),
            git_commit: None,
            mean_nanos: 1000.0,
            p95_nanos: 1500,
            p99_nanos: 1800,
            throughput_per_sec: 1_000_000.0,
            std_dev_nanos: 100.0,
        };
        history.add_point(point);
        assert_eq!(history.points.len(), 1);
    }

    #[test]
    fn test_benchmark_history_trim_to_max() {
        let mut history = BenchmarkHistory::new("test", "test");
        for i in 0..20 {
            let point = HistoryPoint {
                timestamp: Utc::now() + Duration::seconds(i as i64),
                git_commit: None,
                mean_nanos: 1000.0 + i as f64,
                p95_nanos: 1500,
                p99_nanos: 1800,
                throughput_per_sec: 1_000_000.0,
                std_dev_nanos: 100.0,
            };
            history.add_point(point);
        }
        history.trim_to_max(10);
        assert_eq!(history.points.len(), 10);
        // Verify oldest points were removed
        assert!((history.points[0].mean_nanos - 1010.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_benchmark_history_moving_average() {
        let mut history = BenchmarkHistory::new("test", "test");
        for i in 0..10 {
            let point = HistoryPoint {
                timestamp: Utc::now() + Duration::seconds(i as i64),
                git_commit: None,
                mean_nanos: (i + 1) as f64 * 100.0, // 100, 200, ..., 1000
                p95_nanos: 1500,
                p99_nanos: 1800,
                throughput_per_sec: 1_000_000.0,
                std_dev_nanos: 10.0,
            };
            history.add_point(point);
        }
        // Moving average of last 5 points (600, 700, 800, 900, 1000) = 800
        let avg = history.moving_average(5).unwrap();
        assert!((avg - 800.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_benchmark_history_moving_std_dev() {
        let mut history = BenchmarkHistory::new("test", "test");
        // Add identical values - std dev should be 0
        for i in 0..5 {
            let point = HistoryPoint {
                timestamp: Utc::now() + Duration::seconds(i as i64),
                git_commit: None,
                mean_nanos: 1000.0,
                p95_nanos: 1500,
                p99_nanos: 1800,
                throughput_per_sec: 1_000_000.0,
                std_dev_nanos: 10.0,
            };
            history.add_point(point);
        }
        let std_dev = history.moving_std_dev(5).unwrap();
        assert!(std_dev.abs() < f64::EPSILON);
    }

    #[test]
    fn test_trend_analysis_stable() {
        let mut history = BenchmarkHistory::new("test", "test");
        for i in 0..10 {
            let point = HistoryPoint {
                timestamp: Utc::now() + Duration::seconds(i as i64),
                git_commit: None,
                mean_nanos: 1000.0 + (i as f64 * 10.0), // Slight variation
                p95_nanos: 1500,
                p99_nanos: 1800,
                throughput_per_sec: 1_000_000.0,
                std_dev_nanos: 50.0,
            };
            history.add_point(point);
        }
        let current = create_test_result("test", 1050.0);
        let config = ContinuousConfig::default();
        let analysis = TrendAnalysis::analyze(&history, &current, &config);

        assert!(!analysis.is_regression);
        assert!(!analysis.is_improvement);
    }

    #[test]
    fn test_trend_analysis_regression() {
        let mut history = BenchmarkHistory::new("test", "test");
        // Add points with some variance so std_dev is non-zero
        for i in 0..10 {
            let point = HistoryPoint {
                timestamp: Utc::now() + chrono::Duration::seconds(i as i64),
                git_commit: None,
                mean_nanos: 1000.0 + (i as f64 % 3.0) * 10.0, // Varies between 1000, 1010, 1020
                p95_nanos: 1500,
                p99_nanos: 1800,
                throughput_per_sec: 1_000_000.0,
                std_dev_nanos: 50.0,
            };
            history.add_point(point);
        }
        // Create a result that is significantly slower (10x the std dev)
        let current = create_test_result("test", 2000.0);
        let config = ContinuousConfig::default();
        let analysis = TrendAnalysis::analyze(&history, &current, &config);

        assert!(analysis.is_regression);
        assert!(!analysis.is_improvement);
    }

    #[test]
    fn test_continuous_storage_save_load_history() {
        let temp_dir = TempDir::new().unwrap();
        let config = ContinuousConfig::default();
        let storage = ContinuousStorage::new(temp_dir.path(), config).unwrap();

        let mut history = BenchmarkHistory::new("test-bench", "serialization");
        history.add_point(HistoryPoint {
            timestamp: Utc::now(),
            git_commit: Some("abc123".to_string()),
            mean_nanos: 1000.0,
            p95_nanos: 1500,
            p99_nanos: 1800,
            throughput_per_sec: 1_000_000.0,
            std_dev_nanos: 50.0,
        });

        storage.save_history(&history).unwrap();
        let loaded = storage.load_history("test-bench").unwrap();

        assert_eq!(loaded.name, "test-bench");
        assert_eq!(loaded.category, "serialization");
        assert_eq!(loaded.points.len(), 1);
    }

    #[test]
    fn test_continuous_storage_record_result() {
        let temp_dir = TempDir::new().unwrap();
        let config = ContinuousConfig::default();
        let storage = ContinuousStorage::new(temp_dir.path(), config).unwrap();

        let result = create_test_result("test-bench", 1000.0);
        let analysis = storage.record_result(&result).unwrap();

        assert_eq!(analysis.name, "test-bench");
        assert!(!analysis.is_regression); // First result, no history

        // Record more results
        for _ in 0..5 {
            let result = create_test_result("test-bench", 1000.0);
            storage.record_result(&result).unwrap();
        }

        // Load and verify history
        let history = storage.load_history("test-bench").unwrap();
        assert_eq!(history.points.len(), 6);
    }

    #[test]
    fn test_continuous_runner_analysis() {
        let temp_dir = TempDir::new().unwrap();
        let runner = ContinuousRunner::with_defaults(temp_dir.path()).unwrap();

        // Build up some history
        for i in 0..10 {
            let result = create_test_result("bench1", 1000.0 + i as f64 * 10.0);
            runner.storage.record_result(&result).unwrap();
        }

        // Run analysis with stable results
        let results = vec![
            create_test_result("bench1", 1050.0),
            create_test_result("bench2", 500.0),
        ];
        let report = runner.run_analysis(&results).unwrap();

        assert_eq!(report.analyses.len(), 2);
        assert!(!report.has_regressions);
        assert_eq!(report.exit_code(), 0);
    }

    #[test]
    fn test_analysis_report_summary() {
        let report = AnalysisReport {
            timestamp: Utc::now(),
            git_commit: Some("abc123".to_string()),
            analyses: vec![],
            regressions: vec![],
            improvements: vec![],
            has_regressions: false,
        };

        let summary = report.summary();
        assert!(summary.contains("abc123"));
        assert!(summary.contains("0 benchmarks"));
    }

    #[test]
    fn test_trend_analysis_display() {
        let analysis = TrendAnalysis {
            name: "test".to_string(),
            current_mean_nanos: 1000.0,
            moving_average_nanos: Some(900.0),
            moving_std_dev_nanos: Some(50.0),
            z_score: Some(2.5),
            trend: Some(0.2),
            is_regression: true,
            is_improvement: false,
            confidence: 0.85,
        };

        let display = analysis.to_string();
        assert!(display.contains("test"));
        assert!(display.contains("REGRESSION"));
        assert!(display.contains("85%"));
    }
}
