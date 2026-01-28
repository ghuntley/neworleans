//! Benchmark result collection and reporting.
//!
//! This module provides tools for collecting, storing, and analyzing
//! benchmark results, including regression detection and trend analysis.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::path::PathBuf;
use tracing::{debug, error, info, instrument, warn};

use crate::harness::BenchmarkStats;

/// Error type for reporting operations.
#[derive(Debug, thiserror::Error)]
pub enum ReportingError {
    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// Benchmark not found.
    #[error("Benchmark not found: {0}")]
    NotFound(String),
    /// Invalid baseline.
    #[error("Invalid baseline: {0}")]
    InvalidBaseline(String),
}

/// Result type for reporting operations.
pub type ReportingResult<T> = Result<T, ReportingError>;

/// A single benchmark result record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    /// Name of the benchmark.
    pub name: String,
    /// Category of the benchmark (e.g., "serialization", "messaging").
    pub category: String,
    /// Timestamp when the benchmark was run.
    pub timestamp: DateTime<Utc>,
    /// Git commit hash (if available).
    pub git_commit: Option<String>,
    /// Number of operations.
    pub count: u64,
    /// Total time in nanoseconds.
    pub total_nanos: u64,
    /// Mean latency in nanoseconds.
    pub mean_nanos: f64,
    /// Standard deviation in nanoseconds.
    pub std_dev_nanos: f64,
    /// p50 latency in nanoseconds.
    pub p50_nanos: u64,
    /// p95 latency in nanoseconds.
    pub p95_nanos: u64,
    /// p99 latency in nanoseconds.
    pub p99_nanos: u64,
    /// Throughput in operations per second.
    pub throughput_per_sec: f64,
    /// Additional metadata.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl BenchmarkResult {
    /// Create a new benchmark result from stats.
    #[instrument(skip(name, category, stats))]
    pub fn from_stats(
        name: impl Into<String>,
        category: impl Into<String>,
        stats: &BenchmarkStats,
    ) -> Self {
        let name = name.into();
        let category = category.into();
        debug!(name = %name, category = %category, "Creating benchmark result");
        Self {
            name,
            category,
            timestamp: Utc::now(),
            git_commit: get_git_commit(),
            count: stats.count,
            total_nanos: stats.total_nanos,
            mean_nanos: stats.mean_nanos,
            std_dev_nanos: stats.std_dev_nanos,
            p50_nanos: stats.p50_nanos,
            p95_nanos: stats.p95_nanos,
            p99_nanos: stats.p99_nanos,
            throughput_per_sec: stats.throughput_per_sec(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the result.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// A collection of benchmark results forming a baseline.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BenchmarkBaseline {
    /// Name of the baseline.
    pub name: String,
    /// When the baseline was created.
    pub created_at: DateTime<Utc>,
    /// Git commit hash for the baseline.
    pub git_commit: Option<String>,
    /// Description of the baseline.
    pub description: Option<String>,
    /// Results indexed by benchmark name.
    pub results: HashMap<String, BenchmarkResult>,
}

impl BenchmarkBaseline {
    /// Create a new baseline.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            created_at: Utc::now(),
            git_commit: get_git_commit(),
            description: None,
            results: HashMap::new(),
        }
    }

    /// Set the description.
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add a result to the baseline.
    pub fn add_result(&mut self, result: BenchmarkResult) {
        self.results.insert(result.name.clone(), result);
    }

    /// Get a result by name.
    pub fn get_result(&self, name: &str) -> Option<&BenchmarkResult> {
        self.results.get(name)
    }
}

/// Comparison result between two benchmark runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResult {
    /// Benchmark name.
    pub name: String,
    /// Baseline mean latency.
    pub baseline_mean_nanos: f64,
    /// Current mean latency.
    pub current_mean_nanos: f64,
    /// Percentage change in mean latency.
    pub mean_change_percent: f64,
    /// Baseline throughput.
    pub baseline_throughput: f64,
    /// Current throughput.
    pub current_throughput: f64,
    /// Percentage change in throughput.
    pub throughput_change_percent: f64,
    /// Whether this is a regression (positive change > threshold).
    pub is_regression: bool,
    /// Whether this is an improvement (negative change > threshold).
    pub is_improvement: bool,
}

impl ComparisonResult {
    /// Create a comparison between baseline and current results.
    #[instrument]
    pub fn compare(
        baseline: &BenchmarkResult,
        current: &BenchmarkResult,
        regression_threshold: f64,
    ) -> Self {
        let mean_change_percent = if baseline.mean_nanos > 0.0 {
            ((current.mean_nanos - baseline.mean_nanos) / baseline.mean_nanos) * 100.0
        } else {
            0.0
        };

        let throughput_change_percent = if baseline.throughput_per_sec > 0.0 {
            ((current.throughput_per_sec - baseline.throughput_per_sec)
                / baseline.throughput_per_sec)
                * 100.0
        } else {
            0.0
        };

        let is_regression = mean_change_percent > regression_threshold;
        let is_improvement = mean_change_percent < -regression_threshold;

        debug!(
            name = %baseline.name,
            mean_change = mean_change_percent,
            is_regression,
            is_improvement,
            "Comparison computed"
        );

        Self {
            name: baseline.name.clone(),
            baseline_mean_nanos: baseline.mean_nanos,
            current_mean_nanos: current.mean_nanos,
            mean_change_percent,
            baseline_throughput: baseline.throughput_per_sec,
            current_throughput: current.throughput_per_sec,
            throughput_change_percent,
            is_regression,
            is_improvement,
        }
    }
}

impl std::fmt::Display for ComparisonResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let status = if self.is_regression {
            "❌ REGRESSION"
        } else if self.is_improvement {
            "✅ IMPROVEMENT"
        } else {
            "➡️  NO CHANGE"
        };

        write!(
            f,
            "{}: {} (mean: {:.2}µs → {:.2}µs, {:+.1}%)",
            self.name,
            status,
            self.baseline_mean_nanos / 1000.0,
            self.current_mean_nanos / 1000.0,
            self.mean_change_percent
        )
    }
}

/// Storage for benchmark results and baselines.
#[derive(Debug)]
pub struct BenchmarkStorage {
    /// Base directory for storing results.
    base_dir: PathBuf,
}

impl BenchmarkStorage {
    /// Create a new benchmark storage.
    #[instrument(skip(base_dir))]
    pub fn new(base_dir: impl Into<PathBuf>) -> ReportingResult<Self> {
        let base_dir = base_dir.into();
        info!(path = %base_dir.display(), "Initializing benchmark storage");
        fs::create_dir_all(&base_dir)?;
        Ok(Self { base_dir })
    }

    /// Get the default storage location.
    pub fn default_location() -> PathBuf {
        PathBuf::from("target/bench-results")
    }

    /// Save a baseline to disk.
    #[instrument(skip(baseline))]
    pub fn save_baseline(&self, baseline: &BenchmarkBaseline) -> ReportingResult<()> {
        let path = self.baseline_path(&baseline.name);
        info!(path = %path.display(), baseline = %baseline.name, "Saving baseline");
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, baseline)?;
        Ok(())
    }

    /// Load a baseline from disk.
    #[instrument]
    pub fn load_baseline(&self, name: &str) -> ReportingResult<BenchmarkBaseline> {
        let path = self.baseline_path(name);
        debug!(path = %path.display(), "Loading baseline");
        if !path.exists() {
            return Err(ReportingError::NotFound(name.to_string()));
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let baseline = serde_json::from_reader(reader)?;
        Ok(baseline)
    }

    /// List available baselines.
    #[instrument(skip(self))]
    pub fn list_baselines(&self) -> ReportingResult<Vec<String>> {
        let mut baselines = Vec::new();
        for entry in fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                if let Some(name) = path.file_stem() {
                    if let Some(name_str) = name.to_str() {
                        if name_str.starts_with("baseline-") {
                            baselines.push(name_str.trim_start_matches("baseline-").to_string());
                        }
                    }
                }
            }
        }
        debug!(count = baselines.len(), "Listed baselines");
        Ok(baselines)
    }

    /// Save a single result.
    #[instrument(skip(result))]
    pub fn save_result(&self, result: &BenchmarkResult) -> ReportingResult<()> {
        let path = self.result_path(&result.name, &result.timestamp);
        debug!(path = %path.display(), "Saving result");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(&path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, result)?;
        Ok(())
    }

    /// Load results history for a benchmark.
    #[instrument]
    pub fn load_result_history(
        &self,
        name: &str,
        limit: Option<usize>,
    ) -> ReportingResult<Vec<BenchmarkResult>> {
        let results_dir = self.base_dir.join("results").join(name);
        if !results_dir.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        let mut entries: Vec<_> = fs::read_dir(&results_dir)?.filter_map(|e| e.ok()).collect();
        entries.sort_by_key(|e| std::cmp::Reverse(e.path()));

        for entry in entries.into_iter().take(limit.unwrap_or(usize::MAX)) {
            let path = entry.path();
            if path.extension().map(|e| e == "json").unwrap_or(false) {
                let file = File::open(&path)?;
                let reader = BufReader::new(file);
                if let Ok(result) = serde_json::from_reader(reader) {
                    results.push(result);
                }
            }
        }

        debug!(name, count = results.len(), "Loaded result history");
        Ok(results)
    }

    fn baseline_path(&self, name: &str) -> PathBuf {
        self.base_dir.join(format!("baseline-{}.json", name))
    }

    fn result_path(&self, name: &str, timestamp: &DateTime<Utc>) -> PathBuf {
        self.base_dir
            .join("results")
            .join(name)
            .join(format!("{}.json", timestamp.format("%Y%m%d-%H%M%S")))
    }
}

/// Report generator for benchmark results.
#[derive(Debug)]
pub struct BenchmarkReporter {
    /// Storage for baselines and results.
    storage: BenchmarkStorage,
    /// Threshold for regression detection (percentage).
    regression_threshold: f64,
}

impl BenchmarkReporter {
    /// Create a new reporter.
    #[instrument]
    pub fn new(storage: BenchmarkStorage) -> Self {
        Self {
            storage,
            regression_threshold: 10.0, // 10% regression threshold
        }
    }

    /// Set the regression threshold.
    pub fn with_regression_threshold(mut self, threshold: f64) -> Self {
        self.regression_threshold = threshold;
        self
    }

    /// Compare current results against a baseline.
    #[instrument(skip(current_results))]
    pub fn compare_to_baseline(
        &self,
        baseline_name: &str,
        current_results: &[BenchmarkResult],
    ) -> ReportingResult<Vec<ComparisonResult>> {
        let baseline = self.storage.load_baseline(baseline_name)?;
        info!(
            baseline = baseline_name,
            current_count = current_results.len(),
            "Comparing to baseline"
        );

        let mut comparisons = Vec::new();
        for current in current_results {
            if let Some(baseline_result) = baseline.get_result(&current.name) {
                let comparison =
                    ComparisonResult::compare(baseline_result, current, self.regression_threshold);
                comparisons.push(comparison);
            } else {
                warn!(name = %current.name, "No baseline found for benchmark");
            }
        }

        Ok(comparisons)
    }

    /// Generate a markdown report.
    #[instrument(skip(results, comparisons))]
    pub fn generate_markdown_report(
        &self,
        title: &str,
        results: &[BenchmarkResult],
        comparisons: Option<&[ComparisonResult]>,
    ) -> String {
        info!(title, result_count = results.len(), "Generating markdown report");

        let mut report = String::new();
        report.push_str(&format!("# {}\n\n", title));
        report.push_str(&format!(
            "Generated: {}\n\n",
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        ));

        if let Some(commit) = get_git_commit() {
            report.push_str(&format!("Git Commit: `{}`\n\n", commit));
        }

        // Summary table
        report.push_str("## Summary\n\n");
        report.push_str("| Benchmark | Mean | p95 | p99 | Throughput |\n");
        report.push_str("|-----------|------|-----|-----|------------|\n");

        for result in results {
            report.push_str(&format!(
                "| {} | {:.2}µs | {:.2}µs | {:.2}µs | {:.0}/s |\n",
                result.name,
                result.mean_nanos / 1000.0,
                result.p95_nanos as f64 / 1000.0,
                result.p99_nanos as f64 / 1000.0,
                result.throughput_per_sec
            ));
        }

        // Comparison section
        if let Some(comparisons) = comparisons {
            report.push_str("\n## Comparison to Baseline\n\n");

            let regressions: Vec<_> = comparisons.iter().filter(|c| c.is_regression).collect();
            let improvements: Vec<_> = comparisons.iter().filter(|c| c.is_improvement).collect();

            if !regressions.is_empty() {
                report.push_str("### ⚠️ Regressions\n\n");
                for r in regressions {
                    report.push_str(&format!("- {}\n", r));
                }
                report.push('\n');
            }

            if !improvements.is_empty() {
                report.push_str("### ✅ Improvements\n\n");
                for i in improvements {
                    report.push_str(&format!("- {}\n", i));
                }
                report.push('\n');
            }

            report.push_str("### Full Comparison\n\n");
            report.push_str("| Benchmark | Baseline | Current | Change |\n");
            report.push_str("|-----------|----------|---------|--------|\n");

            for c in comparisons {
                let status = if c.is_regression {
                    "❌"
                } else if c.is_improvement {
                    "✅"
                } else {
                    "➡️"
                };
                report.push_str(&format!(
                    "| {} | {:.2}µs | {:.2}µs | {} {:+.1}% |\n",
                    c.name,
                    c.baseline_mean_nanos / 1000.0,
                    c.current_mean_nanos / 1000.0,
                    status,
                    c.mean_change_percent
                ));
            }
        }

        report
    }

    /// Check for regressions and return true if any are found.
    #[instrument(skip(comparisons))]
    pub fn has_regressions(&self, comparisons: &[ComparisonResult]) -> bool {
        let has_regression = comparisons.iter().any(|c| c.is_regression);
        if has_regression {
            warn!("Performance regressions detected");
        }
        has_regression
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
    use tempfile::TempDir;

    fn create_test_stats() -> BenchmarkStats {
        BenchmarkStats {
            count: 1000,
            total_nanos: 1_000_000,
            min_nanos: 500,
            max_nanos: 2000,
            mean_nanos: 1000.0,
            std_dev_nanos: 100.0,
            p50_nanos: 950,
            p95_nanos: 1500,
            p99_nanos: 1800,
        }
    }

    #[test]
    fn test_benchmark_result_from_stats() {
        let stats = create_test_stats();
        let result = BenchmarkResult::from_stats("test-bench", "serialization", &stats);

        assert_eq!(result.name, "test-bench");
        assert_eq!(result.category, "serialization");
        assert_eq!(result.count, 1000);
        assert!((result.mean_nanos - 1000.0).abs() < 0.001);
    }

    #[test]
    fn test_benchmark_result_with_metadata() {
        let stats = create_test_stats();
        let result = BenchmarkResult::from_stats("test", "test", &stats)
            .with_metadata("version", "1.0.0")
            .with_metadata("os", "linux");

        assert_eq!(result.metadata.get("version"), Some(&"1.0.0".to_string()));
        assert_eq!(result.metadata.get("os"), Some(&"linux".to_string()));
    }

    #[test]
    fn test_benchmark_baseline_new() {
        let baseline = BenchmarkBaseline::new("main");
        assert_eq!(baseline.name, "main");
        assert!(baseline.results.is_empty());
    }

    #[test]
    fn test_benchmark_baseline_add_result() {
        let mut baseline = BenchmarkBaseline::new("main");
        let stats = create_test_stats();
        let result = BenchmarkResult::from_stats("test", "test", &stats);

        baseline.add_result(result);

        assert_eq!(baseline.results.len(), 1);
        assert!(baseline.get_result("test").is_some());
    }

    #[test]
    fn test_comparison_result_no_change() {
        let stats = create_test_stats();
        let baseline = BenchmarkResult::from_stats("test", "test", &stats);
        let current = BenchmarkResult::from_stats("test", "test", &stats);

        let comparison = ComparisonResult::compare(&baseline, &current, 10.0);

        assert!(!comparison.is_regression);
        assert!(!comparison.is_improvement);
        assert!(comparison.mean_change_percent.abs() < 0.001);
    }

    #[test]
    fn test_comparison_result_regression() {
        let stats = create_test_stats();
        let baseline = BenchmarkResult::from_stats("test", "test", &stats);

        let mut slow_stats = create_test_stats();
        slow_stats.mean_nanos = 1500.0; // 50% slower
        let current = BenchmarkResult::from_stats("test", "test", &slow_stats);

        let comparison = ComparisonResult::compare(&baseline, &current, 10.0);

        assert!(comparison.is_regression);
        assert!(!comparison.is_improvement);
        assert!(comparison.mean_change_percent > 10.0);
    }

    #[test]
    fn test_comparison_result_improvement() {
        let stats = create_test_stats();
        let baseline = BenchmarkResult::from_stats("test", "test", &stats);

        let mut fast_stats = create_test_stats();
        fast_stats.mean_nanos = 500.0; // 50% faster
        let current = BenchmarkResult::from_stats("test", "test", &fast_stats);

        let comparison = ComparisonResult::compare(&baseline, &current, 10.0);

        assert!(!comparison.is_regression);
        assert!(comparison.is_improvement);
        assert!(comparison.mean_change_percent < -10.0);
    }

    #[test]
    fn test_comparison_result_display() {
        let stats = create_test_stats();
        let baseline = BenchmarkResult::from_stats("test", "test", &stats);
        let current = BenchmarkResult::from_stats("test", "test", &stats);

        let comparison = ComparisonResult::compare(&baseline, &current, 10.0);
        let display = comparison.to_string();

        assert!(display.contains("test"));
        assert!(display.contains("NO CHANGE"));
    }

    #[test]
    fn test_benchmark_storage_save_load_baseline() {
        let temp_dir = TempDir::new().unwrap();
        let storage = BenchmarkStorage::new(temp_dir.path()).unwrap();

        let mut baseline = BenchmarkBaseline::new("test-baseline");
        let stats = create_test_stats();
        baseline.add_result(BenchmarkResult::from_stats("bench1", "test", &stats));

        storage.save_baseline(&baseline).unwrap();
        let loaded = storage.load_baseline("test-baseline").unwrap();

        assert_eq!(loaded.name, "test-baseline");
        assert!(loaded.get_result("bench1").is_some());
    }

    #[test]
    fn test_benchmark_storage_list_baselines() {
        let temp_dir = TempDir::new().unwrap();
        let storage = BenchmarkStorage::new(temp_dir.path()).unwrap();

        let baseline1 = BenchmarkBaseline::new("baseline1");
        let baseline2 = BenchmarkBaseline::new("baseline2");

        storage.save_baseline(&baseline1).unwrap();
        storage.save_baseline(&baseline2).unwrap();

        let baselines = storage.list_baselines().unwrap();
        assert_eq!(baselines.len(), 2);
        assert!(baselines.contains(&"baseline1".to_string()));
        assert!(baselines.contains(&"baseline2".to_string()));
    }

    #[test]
    fn test_benchmark_reporter_compare_to_baseline() {
        let temp_dir = TempDir::new().unwrap();
        let storage = BenchmarkStorage::new(temp_dir.path()).unwrap();

        // Create and save baseline
        let mut baseline = BenchmarkBaseline::new("main");
        let stats = create_test_stats();
        baseline.add_result(BenchmarkResult::from_stats("bench1", "test", &stats));
        storage.save_baseline(&baseline).unwrap();

        // Create current results
        let current = vec![BenchmarkResult::from_stats("bench1", "test", &stats)];

        let reporter = BenchmarkReporter::new(storage);
        let comparisons = reporter.compare_to_baseline("main", &current).unwrap();

        assert_eq!(comparisons.len(), 1);
        assert!(!comparisons[0].is_regression);
    }

    #[test]
    fn test_benchmark_reporter_generate_markdown() {
        let temp_dir = TempDir::new().unwrap();
        let storage = BenchmarkStorage::new(temp_dir.path()).unwrap();
        let reporter = BenchmarkReporter::new(storage);

        let stats = create_test_stats();
        let results = vec![BenchmarkResult::from_stats("bench1", "test", &stats)];

        let report = reporter.generate_markdown_report("Test Report", &results, None);

        assert!(report.contains("# Test Report"));
        assert!(report.contains("bench1"));
        assert!(report.contains("Mean"));
    }

    #[test]
    fn test_benchmark_reporter_has_regressions() {
        let stats = create_test_stats();
        let baseline = BenchmarkResult::from_stats("test", "test", &stats);

        let mut slow_stats = create_test_stats();
        slow_stats.mean_nanos = 1500.0;
        let current = BenchmarkResult::from_stats("test", "test", &slow_stats);

        let comparison = ComparisonResult::compare(&baseline, &current, 10.0);

        let temp_dir = TempDir::new().unwrap();
        let storage = BenchmarkStorage::new(temp_dir.path()).unwrap();
        let reporter = BenchmarkReporter::new(storage);

        assert!(reporter.has_regressions(&[comparison]));
    }
}
