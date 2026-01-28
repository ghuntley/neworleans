//! CI/CD integration utilities.
//!
//! This module provides tools for integrating continuous benchmarking with
//! CI/CD pipelines, including exit codes, JUnit XML output, and GitHub Actions
//! compatible annotations.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::{debug, info, instrument};

use crate::continuous::AnalysisReport;
use crate::reporting::{ComparisonResult, ReportingResult};

/// Exit codes for CI integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExitCode {
    /// All benchmarks passed.
    Success = 0,
    /// Performance regressions detected.
    Regression = 1,
    /// Configuration or runtime error.
    Error = 2,
    /// Insufficient data for analysis.
    InsufficientData = 3,
}

impl ExitCode {
    /// Get the numeric exit code.
    pub fn code(&self) -> i32 {
        *self as i32
    }

    /// Determine exit code from analysis report.
    pub fn from_report(report: &AnalysisReport) -> Self {
        if report.has_regressions {
            Self::Regression
        } else {
            Self::Success
        }
    }
}

impl std::fmt::Display for ExitCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Success => write!(f, "SUCCESS"),
            Self::Regression => write!(f, "REGRESSION"),
            Self::Error => write!(f, "ERROR"),
            Self::InsufficientData => write!(f, "INSUFFICIENT_DATA"),
        }
    }
}

/// JUnit XML test suite for CI integration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JUnitTestSuite {
    /// Name of the test suite.
    pub name: String,
    /// Number of tests.
    pub tests: usize,
    /// Number of failures.
    pub failures: usize,
    /// Number of errors.
    pub errors: usize,
    /// Number of skipped tests.
    pub skipped: usize,
    /// Total time in seconds.
    pub time: f64,
    /// Timestamp.
    pub timestamp: DateTime<Utc>,
    /// Test cases.
    pub test_cases: Vec<JUnitTestCase>,
}

/// JUnit XML test case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JUnitTestCase {
    /// Name of the test.
    pub name: String,
    /// Class name (category).
    pub classname: String,
    /// Time in seconds.
    pub time: f64,
    /// Failure message (if any).
    pub failure: Option<JUnitFailure>,
    /// Properties.
    pub properties: Vec<JUnitProperty>,
}

/// JUnit failure information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JUnitFailure {
    /// Failure message.
    pub message: String,
    /// Failure type.
    pub failure_type: String,
    /// Failure details.
    pub details: String,
}

/// JUnit property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JUnitProperty {
    /// Property name.
    pub name: String,
    /// Property value.
    pub value: String,
}

impl JUnitTestSuite {
    /// Create a new test suite.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tests: 0,
            failures: 0,
            errors: 0,
            skipped: 0,
            time: 0.0,
            timestamp: Utc::now(),
            test_cases: Vec::new(),
        }
    }

    /// Create from analysis report.
    pub fn from_analysis(report: &AnalysisReport) -> Self {
        let mut suite = Self::new("Orleans-RS Benchmarks");
        suite.timestamp = report.timestamp;

        for analysis in &report.analyses {
            let mut test_case = JUnitTestCase {
                name: analysis.name.clone(),
                classname: "performance".to_string(),
                time: analysis.current_mean_nanos / 1_000_000_000.0,
                failure: None,
                properties: vec![
                    JUnitProperty {
                        name: "mean_nanos".to_string(),
                        value: format!("{:.2}", analysis.current_mean_nanos),
                    },
                    JUnitProperty {
                        name: "z_score".to_string(),
                        value: analysis.z_score.map(|z| format!("{:.4}", z)).unwrap_or_default(),
                    },
                    JUnitProperty {
                        name: "confidence".to_string(),
                        value: format!("{:.4}", analysis.confidence),
                    },
                ],
            };

            if analysis.is_regression {
                test_case.failure = Some(JUnitFailure {
                    message: format!(
                        "Performance regression detected (z-score: {:.2})",
                        analysis.z_score.unwrap_or(0.0)
                    ),
                    failure_type: "PerformanceRegression".to_string(),
                    details: format!(
                        "Current: {:.2}ns, Baseline: {:.2}ns, Change: {:.2} standard deviations",
                        analysis.current_mean_nanos,
                        analysis.moving_average_nanos.unwrap_or(0.0),
                        analysis.z_score.unwrap_or(0.0)
                    ),
                });
                suite.failures += 1;
            }

            suite.test_cases.push(test_case);
            suite.tests += 1;
        }

        suite
    }

    /// Create from comparison results.
    pub fn from_comparisons(comparisons: &[ComparisonResult]) -> Self {
        let mut suite = Self::new("Orleans-RS Benchmark Comparison");

        for comparison in comparisons {
            let mut test_case = JUnitTestCase {
                name: comparison.name.clone(),
                classname: "performance.comparison".to_string(),
                time: comparison.current_mean_nanos / 1_000_000_000.0,
                failure: None,
                properties: vec![
                    JUnitProperty {
                        name: "baseline_mean_nanos".to_string(),
                        value: format!("{:.2}", comparison.baseline_mean_nanos),
                    },
                    JUnitProperty {
                        name: "current_mean_nanos".to_string(),
                        value: format!("{:.2}", comparison.current_mean_nanos),
                    },
                    JUnitProperty {
                        name: "change_percent".to_string(),
                        value: format!("{:.2}", comparison.mean_change_percent),
                    },
                ],
            };

            if comparison.is_regression {
                test_case.failure = Some(JUnitFailure {
                    message: format!(
                        "Performance regression: {:.1}% slower",
                        comparison.mean_change_percent
                    ),
                    failure_type: "PerformanceRegression".to_string(),
                    details: format!(
                        "Baseline: {:.2}ns, Current: {:.2}ns, Change: {:.2}%",
                        comparison.baseline_mean_nanos,
                        comparison.current_mean_nanos,
                        comparison.mean_change_percent
                    ),
                });
                suite.failures += 1;
            }

            suite.test_cases.push(test_case);
            suite.tests += 1;
        }

        suite
    }

    /// Generate JUnit XML.
    #[instrument(skip(self))]
    pub fn to_xml(&self) -> String {
        let mut xml = String::new();
        xml.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.push_str(&format!(
            "<testsuite name=\"{}\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\" time=\"{:.3}\" timestamp=\"{}\">\n",
            escape_xml(&self.name),
            self.tests,
            self.failures,
            self.errors,
            self.skipped,
            self.time,
            self.timestamp.to_rfc3339()
        ));

        for test_case in &self.test_cases {
            xml.push_str(&format!(
                "  <testcase name=\"{}\" classname=\"{}\" time=\"{:.6}\">\n",
                escape_xml(&test_case.name),
                escape_xml(&test_case.classname),
                test_case.time
            ));

            if !test_case.properties.is_empty() {
                xml.push_str("    <properties>\n");
                for prop in &test_case.properties {
                    xml.push_str(&format!(
                        "      <property name=\"{}\" value=\"{}\"/>\n",
                        escape_xml(&prop.name),
                        escape_xml(&prop.value)
                    ));
                }
                xml.push_str("    </properties>\n");
            }

            if let Some(ref failure) = test_case.failure {
                xml.push_str(&format!(
                    "    <failure message=\"{}\" type=\"{}\">{}</failure>\n",
                    escape_xml(&failure.message),
                    escape_xml(&failure.failure_type),
                    escape_xml(&failure.details)
                ));
            }

            xml.push_str("  </testcase>\n");
        }

        xml.push_str("</testsuite>\n");
        xml
    }

    /// Write JUnit XML to file.
    #[instrument(skip(self, path))]
    pub fn write_to_file(&self, path: impl AsRef<Path>) -> ReportingResult<()> {
        let path = path.as_ref();
        info!(path = %path.display(), "Writing JUnit XML report");
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(self.to_xml().as_bytes())?;
        Ok(())
    }
}

/// GitHub Actions annotations.
#[derive(Debug)]
pub struct GitHubActionsOutput {
    /// Annotations to emit.
    annotations: Vec<GitHubAnnotation>,
    /// Summary lines.
    summary: Vec<String>,
}

#[derive(Debug, Clone)]
struct GitHubAnnotation {
    level: AnnotationLevel,
    message: String,
    title: Option<String>,
    file: Option<String>,
    line: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum AnnotationLevel {
    Notice,
    Warning, // Reserved for future use
    Error,
}

impl GitHubActionsOutput {
    /// Create a new GitHub Actions output handler.
    pub fn new() -> Self {
        Self {
            annotations: Vec::new(),
            summary: Vec::new(),
        }
    }

    /// Add annotations from analysis report.
    pub fn from_analysis(report: &AnalysisReport) -> Self {
        let mut output = Self::new();

        // Add summary
        output.summary.push(format!(
            "## Benchmark Results\n\nAnalyzed {} benchmarks: {} regressions, {} improvements\n",
            report.analyses.len(),
            report.regressions.len(),
            report.improvements.len()
        ));

        // Add regression annotations
        for regression in &report.regressions {
            output.annotations.push(GitHubAnnotation {
                level: AnnotationLevel::Error,
                message: format!(
                    "Performance regression in '{}': current {:.2}µs vs baseline {:.2}µs (z-score: {:.2})",
                    regression.name,
                    regression.current_mean_nanos / 1000.0,
                    regression.moving_average_nanos.unwrap_or(0.0) / 1000.0,
                    regression.z_score.unwrap_or(0.0)
                ),
                title: Some(format!("Regression: {}", regression.name)),
                file: None,
                line: None,
            });
        }

        // Add improvement annotations
        for improvement in &report.improvements {
            output.annotations.push(GitHubAnnotation {
                level: AnnotationLevel::Notice,
                message: format!(
                    "Performance improvement in '{}': current {:.2}µs vs baseline {:.2}µs",
                    improvement.name,
                    improvement.current_mean_nanos / 1000.0,
                    improvement.moving_average_nanos.unwrap_or(0.0) / 1000.0
                ),
                title: Some(format!("Improvement: {}", improvement.name)),
                file: None,
                line: None,
            });
        }

        output
    }

    /// Generate GitHub Actions workflow commands.
    #[instrument(skip(self))]
    pub fn emit_commands(&self) {
        for annotation in &self.annotations {
            let level = match annotation.level {
                AnnotationLevel::Notice => "notice",
                AnnotationLevel::Warning => "warning",
                AnnotationLevel::Error => "error",
            };

            let mut params = Vec::new();
            if let Some(ref title) = annotation.title {
                params.push(format!("title={}", title));
            }
            if let Some(ref file) = annotation.file {
                params.push(format!("file={}", file));
            }
            if let Some(line) = annotation.line {
                params.push(format!("line={}", line));
            }

            let params_str = if params.is_empty() {
                String::new()
            } else {
                params.join(",")
            };

            // GitHub Actions workflow command format
            println!("::{} {}::{}", level, params_str, annotation.message);
        }
    }

    /// Write job summary to GitHub Actions step summary file.
    #[instrument(skip(self))]
    pub fn write_job_summary(&self) -> ReportingResult<()> {
        if let Ok(summary_path) = std::env::var("GITHUB_STEP_SUMMARY") {
            info!(path = %summary_path, "Writing GitHub job summary");
            let mut file = File::options()
                .create(true)
                .append(true)
                .open(&summary_path)?;
            for line in &self.summary {
                writeln!(file, "{}", line)?;
            }
        } else {
            debug!("GITHUB_STEP_SUMMARY not set, skipping job summary");
        }
        Ok(())
    }

    /// Set an output variable for GitHub Actions.
    pub fn set_output(name: &str, value: &str) {
        if let Ok(output_path) = std::env::var("GITHUB_OUTPUT") {
            if let Ok(mut file) = File::options()
                .create(true)
                .append(true)
                .open(&output_path)
            {
                let _ = writeln!(file, "{}={}", name, value);
            }
        } else {
            // Fallback for older GitHub Actions
            println!("::set-output name={}::{}", name, value);
        }
    }

    /// Generate markdown summary.
    pub fn markdown_summary(&self, report: &AnalysisReport) -> String {
        let mut md = String::new();

        md.push_str("# Benchmark Results\n\n");

        // Status badge
        let status = if report.has_regressions {
            "failure"
        } else {
            "success"
        };
        md.push_str(&format!(
            "**Status:** {} | **Regressions:** {} | **Improvements:** {}\n\n",
            status,
            report.regressions.len(),
            report.improvements.len()
        ));

        // Regressions table
        if !report.regressions.is_empty() {
            md.push_str("## Regressions\n\n");
            md.push_str("| Benchmark | Current | Baseline | Z-Score |\n");
            md.push_str("|-----------|---------|----------|---------|\n");
            for r in &report.regressions {
                md.push_str(&format!(
                    "| {} | {:.2}µs | {:.2}µs | {:.2} |\n",
                    r.name,
                    r.current_mean_nanos / 1000.0,
                    r.moving_average_nanos.unwrap_or(0.0) / 1000.0,
                    r.z_score.unwrap_or(0.0)
                ));
            }
            md.push('\n');
        }

        // Improvements table
        if !report.improvements.is_empty() {
            md.push_str("## Improvements\n\n");
            md.push_str("| Benchmark | Current | Baseline | Z-Score |\n");
            md.push_str("|-----------|---------|----------|---------|\n");
            for i in &report.improvements {
                md.push_str(&format!(
                    "| {} | {:.2}µs | {:.2}µs | {:.2} |\n",
                    i.name,
                    i.current_mean_nanos / 1000.0,
                    i.moving_average_nanos.unwrap_or(0.0) / 1000.0,
                    i.z_score.unwrap_or(0.0)
                ));
            }
            md.push('\n');
        }

        // All results summary
        md.push_str("## All Results\n\n");
        md.push_str("| Benchmark | Status | Current | Confidence |\n");
        md.push_str("|-----------|--------|---------|------------|\n");
        for a in &report.analyses {
            let status = if a.is_regression {
                "regression"
            } else if a.is_improvement {
                "improvement"
            } else {
                "stable"
            };
            md.push_str(&format!(
                "| {} | {} | {:.2}µs | {:.0}% |\n",
                a.name,
                status,
                a.current_mean_nanos / 1000.0,
                a.confidence * 100.0
            ));
        }

        md
    }
}

impl Default for GitHubActionsOutput {
    fn default() -> Self {
        Self::new()
    }
}

/// CI runner for benchmark integration.
#[derive(Debug)]
pub struct CiRunner {
    /// Output directory.
    output_dir: std::path::PathBuf,
    /// Enable GitHub Actions output.
    github_actions: bool,
    /// Enable JUnit XML output.
    junit_output: bool,
}

impl CiRunner {
    /// Create a new CI runner.
    pub fn new(output_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            github_actions: std::env::var("GITHUB_ACTIONS").is_ok(),
            junit_output: true,
        }
    }

    /// Enable or disable GitHub Actions integration.
    pub fn with_github_actions(mut self, enabled: bool) -> Self {
        self.github_actions = enabled;
        self
    }

    /// Enable or disable JUnit output.
    pub fn with_junit(mut self, enabled: bool) -> Self {
        self.junit_output = enabled;
        self
    }

    /// Run CI integration with an analysis report.
    #[instrument(skip(self, report))]
    pub fn run(&self, report: &AnalysisReport) -> ReportingResult<ExitCode> {
        std::fs::create_dir_all(&self.output_dir)?;

        // Generate JUnit XML
        if self.junit_output {
            let junit = JUnitTestSuite::from_analysis(report);
            let junit_path = self.output_dir.join("benchmark-results.xml");
            junit.write_to_file(&junit_path)?;
            info!(path = %junit_path.display(), "Generated JUnit XML");
        }

        // GitHub Actions integration
        if self.github_actions {
            let gha = GitHubActionsOutput::from_analysis(report);
            gha.emit_commands();
            gha.write_job_summary()?;

            // Set outputs
            GitHubActionsOutput::set_output("has_regressions", &report.has_regressions.to_string());
            GitHubActionsOutput::set_output("regression_count", &report.regressions.len().to_string());
            GitHubActionsOutput::set_output("improvement_count", &report.improvements.len().to_string());
        }

        // Determine exit code
        let exit_code = ExitCode::from_report(report);
        info!(exit_code = %exit_code, "CI run complete");

        Ok(exit_code)
    }

    /// Print summary to stdout.
    pub fn print_summary(&self, report: &AnalysisReport) {
        println!("\n{}", "=".repeat(60));
        println!("BENCHMARK RESULTS SUMMARY");
        println!("{}\n", "=".repeat(60));

        println!(
            "Total: {} | Regressions: {} | Improvements: {}",
            report.analyses.len(),
            report.regressions.len(),
            report.improvements.len()
        );

        if !report.regressions.is_empty() {
            println!("\nREGRESSIONS:");
            for r in &report.regressions {
                println!(
                    "  - {}: {:.2}µs (z={:.2})",
                    r.name,
                    r.current_mean_nanos / 1000.0,
                    r.z_score.unwrap_or(0.0)
                );
            }
        }

        if !report.improvements.is_empty() {
            println!("\nIMPROVEMENTS:");
            for i in &report.improvements {
                println!(
                    "  + {}: {:.2}µs (z={:.2})",
                    i.name,
                    i.current_mean_nanos / 1000.0,
                    i.z_score.unwrap_or(0.0)
                );
            }
        }

        println!("\n{}", "=".repeat(60));
        if report.has_regressions {
            println!("STATUS: FAILURE (regressions detected)");
        } else {
            println!("STATUS: SUCCESS");
        }
        println!("{}", "=".repeat(60));
    }
}

/// Escape XML special characters.
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuous::TrendAnalysis;

    fn create_test_analysis(name: &str, is_regression: bool, is_improvement: bool) -> TrendAnalysis {
        TrendAnalysis {
            name: name.to_string(),
            current_mean_nanos: 1000.0,
            moving_average_nanos: Some(if is_regression { 500.0 } else if is_improvement { 2000.0 } else { 1000.0 }),
            moving_std_dev_nanos: Some(100.0),
            z_score: Some(if is_regression { 5.0 } else if is_improvement { -10.0 } else { 0.0 }),
            trend: Some(0.0),
            is_regression,
            is_improvement,
            confidence: 0.95,
        }
    }

    fn create_test_report() -> AnalysisReport {
        let stable = create_test_analysis("stable-bench", false, false);
        let regression = create_test_analysis("slow-bench", true, false);
        let improvement = create_test_analysis("fast-bench", false, true);

        AnalysisReport {
            timestamp: Utc::now(),
            git_commit: Some("abc123".to_string()),
            analyses: vec![stable.clone(), regression.clone(), improvement.clone()],
            regressions: vec![regression],
            improvements: vec![improvement],
            has_regressions: true,
        }
    }

    #[test]
    fn test_exit_code_values() {
        assert_eq!(ExitCode::Success.code(), 0);
        assert_eq!(ExitCode::Regression.code(), 1);
        assert_eq!(ExitCode::Error.code(), 2);
        assert_eq!(ExitCode::InsufficientData.code(), 3);
    }

    #[test]
    fn test_exit_code_from_report() {
        let report_with_regression = create_test_report();
        assert_eq!(ExitCode::from_report(&report_with_regression), ExitCode::Regression);

        let report_success = AnalysisReport {
            timestamp: Utc::now(),
            git_commit: None,
            analyses: vec![create_test_analysis("test", false, false)],
            regressions: vec![],
            improvements: vec![],
            has_regressions: false,
        };
        assert_eq!(ExitCode::from_report(&report_success), ExitCode::Success);
    }

    #[test]
    fn test_junit_test_suite_new() {
        let suite = JUnitTestSuite::new("Test Suite");
        assert_eq!(suite.name, "Test Suite");
        assert_eq!(suite.tests, 0);
        assert_eq!(suite.failures, 0);
    }

    #[test]
    fn test_junit_test_suite_from_analysis() {
        let report = create_test_report();
        let suite = JUnitTestSuite::from_analysis(&report);

        assert_eq!(suite.tests, 3);
        assert_eq!(suite.failures, 1); // One regression
        assert_eq!(suite.test_cases.len(), 3);
    }

    #[test]
    fn test_junit_xml_generation() {
        let report = create_test_report();
        let suite = JUnitTestSuite::from_analysis(&report);
        let xml = suite.to_xml();

        assert!(xml.contains("<?xml version=\"1.0\""));
        assert!(xml.contains("<testsuite"));
        assert!(xml.contains("tests=\"3\""));
        assert!(xml.contains("failures=\"1\""));
        assert!(xml.contains("<testcase"));
        assert!(xml.contains("<failure"));
    }

    #[test]
    fn test_github_actions_output_from_analysis() {
        let report = create_test_report();
        let output = GitHubActionsOutput::from_analysis(&report);

        assert!(!output.annotations.is_empty());
        // Should have annotations for regression and improvement
        assert!(output.annotations.iter().any(|a| a.level == AnnotationLevel::Error));
        assert!(output.annotations.iter().any(|a| a.level == AnnotationLevel::Notice));
    }

    #[test]
    fn test_github_actions_markdown_summary() {
        let report = create_test_report();
        let output = GitHubActionsOutput::from_analysis(&report);
        let md = output.markdown_summary(&report);

        assert!(md.contains("# Benchmark Results"));
        assert!(md.contains("## Regressions"));
        assert!(md.contains("## Improvements"));
        assert!(md.contains("slow-bench"));
        assert!(md.contains("fast-bench"));
    }

    #[test]
    fn test_ci_runner_new() {
        let runner = CiRunner::new("/tmp/bench-output");
        assert!(runner.junit_output);
    }

    #[test]
    fn test_escape_xml() {
        assert_eq!(escape_xml("<test>"), "&lt;test&gt;");
        assert_eq!(escape_xml("a & b"), "a &amp; b");
        assert_eq!(escape_xml("\"quoted\""), "&quot;quoted&quot;");
    }

    #[test]
    fn test_exit_code_display() {
        assert_eq!(format!("{}", ExitCode::Success), "SUCCESS");
        assert_eq!(format!("{}", ExitCode::Regression), "REGRESSION");
        assert_eq!(format!("{}", ExitCode::Error), "ERROR");
    }

    #[test]
    fn test_junit_with_properties() {
        let report = create_test_report();
        let suite = JUnitTestSuite::from_analysis(&report);
        let xml = suite.to_xml();

        assert!(xml.contains("<properties>"));
        assert!(xml.contains("mean_nanos"));
        assert!(xml.contains("z_score"));
    }
}
