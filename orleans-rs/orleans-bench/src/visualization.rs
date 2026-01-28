//! Performance trend visualization utilities.
//!
//! This module provides tools for visualizing benchmark trends over time,
//! including ASCII charts, CSV export, and HTML report generation.

use chrono::Utc;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use tracing::{debug, info, instrument};

use crate::continuous::{BenchmarkHistory, TrendAnalysis, AnalysisReport};
use crate::reporting::{BenchmarkResult, ReportingResult};

/// Configuration for visualization output.
#[derive(Debug, Clone)]
pub struct VisualizationConfig {
    /// Width for ASCII charts.
    pub chart_width: usize,
    /// Height for ASCII charts.
    pub chart_height: usize,
    /// Maximum number of points to display in charts.
    pub max_chart_points: usize,
    /// Decimal places for numeric output.
    pub decimal_places: usize,
}

impl Default for VisualizationConfig {
    fn default() -> Self {
        Self {
            chart_width: 60,
            chart_height: 15,
            max_chart_points: 30,
            decimal_places: 2,
        }
    }
}

/// ASCII chart for visualizing trends.
#[derive(Debug)]
pub struct AsciiChart {
    /// Chart title.
    title: String,
    /// Data points (label, value).
    data: Vec<(String, f64)>,
    /// Configuration.
    config: VisualizationConfig,
}

impl AsciiChart {
    /// Create a new ASCII chart.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            data: Vec::new(),
            config: VisualizationConfig::default(),
        }
    }

    /// Set configuration.
    pub fn with_config(mut self, config: VisualizationConfig) -> Self {
        self.config = config;
        self
    }

    /// Add a data point.
    pub fn add_point(&mut self, label: impl Into<String>, value: f64) {
        self.data.push((label.into(), value));
    }

    /// Add points from benchmark history.
    pub fn from_history(history: &BenchmarkHistory, config: &VisualizationConfig) -> Self {
        let mut chart = Self::new(format!("{} (Mean Latency)", history.name))
            .with_config(config.clone());

        let points_to_show = history.points.len().min(config.max_chart_points);
        let start = history.points.len().saturating_sub(points_to_show);

        for point in &history.points[start..] {
            let label = point.timestamp.format("%m/%d").to_string();
            chart.add_point(label, point.mean_nanos / 1000.0); // Convert to microseconds
        }

        chart
    }

    /// Render the chart to a string.
    #[instrument(skip(self))]
    pub fn render(&self) -> String {
        if self.data.is_empty() {
            return format!("{}\n(No data)\n", self.title);
        }

        let values: Vec<f64> = self.data.iter().map(|(_, v)| *v).collect();
        let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = max_val - min_val;

        let effective_range = if range < f64::EPSILON { 1.0 } else { range };

        let mut output = String::new();
        output.push_str(&format!("{}\n", self.title));
        output.push_str(&format!("{}\n", "=".repeat(self.config.chart_width)));

        // Build the chart rows
        for row in 0..self.config.chart_height {
            let row_value = max_val - (row as f64 * effective_range / (self.config.chart_height - 1) as f64);

            // Y-axis label
            output.push_str(&format!("{:>8.1} |", row_value));

            // Plot points
            let points_per_col = (self.data.len() as f64 / self.config.chart_width as f64).max(1.0);
            for col in 0..self.config.chart_width.min(self.data.len()) {
                let data_idx = (col as f64 * points_per_col) as usize;
                if data_idx >= self.data.len() {
                    output.push(' ');
                    continue;
                }

                let (_, value) = &self.data[data_idx];
                let normalized = (value - min_val) / effective_range;
                let value_row = ((1.0 - normalized) * (self.config.chart_height - 1) as f64).round() as usize;

                if value_row == row {
                    output.push('*');
                } else if value_row > row && row == self.config.chart_height - 1 {
                    output.push('_');
                } else {
                    output.push(' ');
                }
            }
            output.push('\n');
        }

        // X-axis
        output.push_str(&format!("{:>8} +", ""));
        output.push_str(&"-".repeat(self.config.chart_width.min(self.data.len())));
        output.push('\n');

        // Legend
        output.push_str(&format!(
            "         Min: {:.2}µs  Max: {:.2}µs  Points: {}\n",
            min_val, max_val, self.data.len()
        ));

        output
    }
}

impl std::fmt::Display for AsciiChart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.render())
    }
}

/// CSV exporter for benchmark data.
#[derive(Debug)]
pub struct CsvExporter {
    /// Column headers.
    headers: Vec<String>,
    /// Rows of data.
    rows: Vec<Vec<String>>,
}

impl CsvExporter {
    /// Create a new CSV exporter.
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Create from benchmark history.
    pub fn from_history(history: &BenchmarkHistory) -> Self {
        let mut exporter = Self::new();
        exporter.headers = vec![
            "timestamp".to_string(),
            "git_commit".to_string(),
            "mean_nanos".to_string(),
            "p95_nanos".to_string(),
            "p99_nanos".to_string(),
            "throughput_per_sec".to_string(),
            "std_dev_nanos".to_string(),
        ];

        for point in &history.points {
            exporter.rows.push(vec![
                point.timestamp.to_rfc3339(),
                point.git_commit.clone().unwrap_or_default(),
                format!("{:.2}", point.mean_nanos),
                point.p95_nanos.to_string(),
                point.p99_nanos.to_string(),
                format!("{:.2}", point.throughput_per_sec),
                format!("{:.2}", point.std_dev_nanos),
            ]);
        }

        exporter
    }

    /// Create from multiple benchmark results.
    pub fn from_results(results: &[BenchmarkResult]) -> Self {
        let mut exporter = Self::new();
        exporter.headers = vec![
            "name".to_string(),
            "category".to_string(),
            "timestamp".to_string(),
            "git_commit".to_string(),
            "count".to_string(),
            "mean_nanos".to_string(),
            "std_dev_nanos".to_string(),
            "p50_nanos".to_string(),
            "p95_nanos".to_string(),
            "p99_nanos".to_string(),
            "throughput_per_sec".to_string(),
        ];

        for result in results {
            exporter.rows.push(vec![
                result.name.clone(),
                result.category.clone(),
                result.timestamp.to_rfc3339(),
                result.git_commit.clone().unwrap_or_default(),
                result.count.to_string(),
                format!("{:.2}", result.mean_nanos),
                format!("{:.2}", result.std_dev_nanos),
                result.p50_nanos.to_string(),
                result.p95_nanos.to_string(),
                result.p99_nanos.to_string(),
                format!("{:.2}", result.throughput_per_sec),
            ]);
        }

        exporter
    }

    /// Create from trend analyses.
    pub fn from_analyses(analyses: &[TrendAnalysis]) -> Self {
        let mut exporter = Self::new();
        exporter.headers = vec![
            "name".to_string(),
            "current_mean_nanos".to_string(),
            "moving_average_nanos".to_string(),
            "moving_std_dev_nanos".to_string(),
            "z_score".to_string(),
            "trend".to_string(),
            "is_regression".to_string(),
            "is_improvement".to_string(),
            "confidence".to_string(),
        ];

        for analysis in analyses {
            exporter.rows.push(vec![
                analysis.name.clone(),
                format!("{:.2}", analysis.current_mean_nanos),
                analysis.moving_average_nanos.map(|v| format!("{:.2}", v)).unwrap_or_default(),
                analysis.moving_std_dev_nanos.map(|v| format!("{:.2}", v)).unwrap_or_default(),
                analysis.z_score.map(|v| format!("{:.4}", v)).unwrap_or_default(),
                analysis.trend.map(|v| format!("{:.4}", v)).unwrap_or_default(),
                analysis.is_regression.to_string(),
                analysis.is_improvement.to_string(),
                format!("{:.4}", analysis.confidence),
            ]);
        }

        exporter
    }

    /// Render to CSV string.
    pub fn to_csv(&self) -> String {
        let mut output = String::new();
        output.push_str(&self.headers.join(","));
        output.push('\n');
        for row in &self.rows {
            output.push_str(&row.join(","));
            output.push('\n');
        }
        output
    }

    /// Write to file.
    #[instrument(skip(self, path))]
    pub fn write_to_file(&self, path: impl AsRef<Path>) -> ReportingResult<()> {
        let path = path.as_ref();
        debug!(path = %path.display(), "Writing CSV file");
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(self.to_csv().as_bytes())?;
        Ok(())
    }
}

impl Default for CsvExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// HTML report generator.
#[derive(Debug)]
pub struct HtmlReportGenerator {
    /// Report title.
    title: String,
    /// Sections of the report.
    sections: Vec<HtmlSection>,
}

#[derive(Debug)]
enum HtmlSection {
    Header(String),
    Paragraph(String),
    Table(HtmlTable),
    Chart(String), // Chart.js config as JSON
}

#[derive(Debug)]
struct HtmlTable {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
    styles: Vec<String>, // CSS classes for rows
}

impl HtmlReportGenerator {
    /// Create a new HTML report generator.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            sections: Vec::new(),
        }
    }

    /// Add a header section.
    pub fn add_header(&mut self, text: impl Into<String>) {
        self.sections.push(HtmlSection::Header(text.into()));
    }

    /// Add a paragraph.
    pub fn add_paragraph(&mut self, text: impl Into<String>) {
        self.sections.push(HtmlSection::Paragraph(text.into()));
    }

    /// Add benchmark results as a table.
    pub fn add_results_table(&mut self, results: &[BenchmarkResult]) {
        let headers = vec![
            "Name".to_string(),
            "Category".to_string(),
            "Mean".to_string(),
            "p95".to_string(),
            "p99".to_string(),
            "Throughput".to_string(),
        ];

        let rows: Vec<Vec<String>> = results
            .iter()
            .map(|r| vec![
                r.name.clone(),
                r.category.clone(),
                format!("{:.2}us", r.mean_nanos / 1000.0),
                format!("{:.2}us", r.p95_nanos as f64 / 1000.0),
                format!("{:.2}us", r.p99_nanos as f64 / 1000.0),
                format!("{:.0}/s", r.throughput_per_sec),
            ])
            .collect();

        let styles = vec!["".to_string(); rows.len()];

        self.sections.push(HtmlSection::Table(HtmlTable { headers, rows, styles }));
    }

    /// Add trend analyses as a table.
    pub fn add_analysis_table(&mut self, analyses: &[TrendAnalysis]) {
        let headers = vec![
            "Name".to_string(),
            "Status".to_string(),
            "Current".to_string(),
            "Baseline".to_string(),
            "Z-Score".to_string(),
            "Confidence".to_string(),
        ];

        let rows: Vec<Vec<String>> = analyses
            .iter()
            .map(|a| {
                let status = if a.is_regression {
                    "REGRESSION"
                } else if a.is_improvement {
                    "IMPROVEMENT"
                } else {
                    "STABLE"
                };
                vec![
                    a.name.clone(),
                    status.to_string(),
                    format!("{:.2}us", a.current_mean_nanos / 1000.0),
                    a.moving_average_nanos
                        .map(|v| format!("{:.2}us", v / 1000.0))
                        .unwrap_or("-".to_string()),
                    a.z_score.map(|v| format!("{:.2}", v)).unwrap_or("-".to_string()),
                    format!("{:.0}%", a.confidence * 100.0),
                ]
            })
            .collect();

        let styles: Vec<String> = analyses
            .iter()
            .map(|a| {
                if a.is_regression {
                    "regression".to_string()
                } else if a.is_improvement {
                    "improvement".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();

        self.sections.push(HtmlSection::Table(HtmlTable { headers, rows, styles }));
    }

    /// Add a Chart.js line chart for history.
    pub fn add_history_chart(&mut self, history: &BenchmarkHistory) {
        let labels: Vec<String> = history
            .points
            .iter()
            .map(|p| p.timestamp.format("%m/%d %H:%M").to_string())
            .collect();
        let data: Vec<f64> = history
            .points
            .iter()
            .map(|p| p.mean_nanos / 1000.0)
            .collect();

        let chart_config = serde_json::json!({
            "type": "line",
            "data": {
                "labels": labels,
                "datasets": [{
                    "label": format!("{} (µs)", history.name),
                    "data": data,
                    "borderColor": "rgb(75, 192, 192)",
                    "tension": 0.1
                }]
            },
            "options": {
                "responsive": true,
                "plugins": {
                    "title": {
                        "display": true,
                        "text": format!("{} - Mean Latency Over Time", history.name)
                    }
                },
                "scales": {
                    "y": {
                        "title": {
                            "display": true,
                            "text": "Latency (µs)"
                        }
                    }
                }
            }
        });

        self.sections.push(HtmlSection::Chart(chart_config.to_string()));
    }

    /// Generate the HTML report.
    #[instrument(skip(self))]
    pub fn generate(&self) -> String {
        let mut html = String::new();

        // HTML header
        html.push_str(&format!(r#"<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 20px; background: #f5f5f5; }}
        .container {{ max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        h1, h2 {{ color: #333; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
        th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f8f9fa; font-weight: 600; }}
        tr:hover {{ background-color: #f5f5f5; }}
        tr.regression {{ background-color: #ffebee; }}
        tr.improvement {{ background-color: #e8f5e9; }}
        .chart-container {{ width: 100%; max-width: 800px; margin: 20px auto; }}
        code {{ background: #f4f4f4; padding: 2px 6px; border-radius: 4px; font-family: 'Menlo', 'Monaco', monospace; }}
        pre {{ background: #f4f4f4; padding: 15px; border-radius: 4px; overflow-x: auto; }}
        .timestamp {{ color: #666; font-size: 0.9em; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{}</h1>
        <p class="timestamp">Generated: {}</p>
"#, self.title, self.title, Utc::now().format("%Y-%m-%d %H:%M:%S UTC")));

        let mut chart_id = 0;
        for section in &self.sections {
            match section {
                HtmlSection::Header(text) => {
                    html.push_str(&format!("        <h2>{}</h2>\n", escape_html(text)));
                }
                HtmlSection::Paragraph(text) => {
                    html.push_str(&format!("        <p>{}</p>\n", escape_html(text)));
                }
                HtmlSection::Table(table) => {
                    html.push_str("        <table>\n");
                    html.push_str("            <thead><tr>\n");
                    for header in &table.headers {
                        html.push_str(&format!("                <th>{}</th>\n", escape_html(header)));
                    }
                    html.push_str("            </tr></thead>\n");
                    html.push_str("            <tbody>\n");
                    for (row, style) in table.rows.iter().zip(table.styles.iter()) {
                        let class_attr = if style.is_empty() {
                            String::new()
                        } else {
                            format!(" class=\"{}\"", style)
                        };
                        html.push_str(&format!("            <tr{}>\n", class_attr));
                        for cell in row {
                            html.push_str(&format!("                <td>{}</td>\n", escape_html(cell)));
                        }
                        html.push_str("            </tr>\n");
                    }
                    html.push_str("            </tbody>\n");
                    html.push_str("        </table>\n");
                }
                HtmlSection::Chart(config) => {
                    html.push_str(&format!(r#"        <div class="chart-container">
            <canvas id="chart{}"></canvas>
        </div>
        <script>
            new Chart(document.getElementById('chart{}'), {});
        </script>
"#, chart_id, chart_id, config));
                    chart_id += 1;
                }
            }
        }

        // HTML footer
        html.push_str(r#"    </div>
</body>
</html>
"#);

        html
    }

    /// Write to file.
    #[instrument(skip(self, path))]
    pub fn write_to_file(&self, path: impl AsRef<Path>) -> ReportingResult<()> {
        let path = path.as_ref();
        info!(path = %path.display(), "Writing HTML report");
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        writer.write_all(self.generate().as_bytes())?;
        Ok(())
    }

    /// Create a complete report from an analysis report.
    pub fn from_analysis_report(report: &AnalysisReport, histories: &[BenchmarkHistory]) -> Self {
        let mut generator = Self::new("Orleans-RS Benchmark Report");

        // Summary
        generator.add_header("Summary");
        generator.add_paragraph(format!(
            "Analyzed {} benchmarks: {} regressions, {} improvements",
            report.analyses.len(),
            report.regressions.len(),
            report.improvements.len()
        ));

        if let Some(ref commit) = report.git_commit {
            generator.add_paragraph(format!("Git commit: {}", commit));
        }

        // Regressions
        if !report.regressions.is_empty() {
            generator.add_header("Regressions");
            generator.add_analysis_table(&report.regressions);
        }

        // Improvements
        if !report.improvements.is_empty() {
            generator.add_header("Improvements");
            generator.add_analysis_table(&report.improvements);
        }

        // All results
        generator.add_header("All Results");
        generator.add_analysis_table(&report.analyses);

        // Charts
        generator.add_header("Historical Trends");
        for history in histories {
            if !history.points.is_empty() {
                generator.add_history_chart(history);
            }
        }

        generator
    }
}

/// Escape HTML special characters.
fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::continuous::HistoryPoint;
    use chrono::Duration;

    fn create_test_history() -> BenchmarkHistory {
        let mut history = BenchmarkHistory::new("test-bench", "serialization");
        for i in 0..10 {
            history.add_point(HistoryPoint {
                timestamp: Utc::now() - Duration::hours(10 - i as i64),
                git_commit: Some(format!("commit{}", i)),
                mean_nanos: 1000.0 + i as f64 * 50.0,
                p95_nanos: 1500 + i * 50,
                p99_nanos: 1800 + i * 50,
                throughput_per_sec: 1_000_000.0 - i as f64 * 10000.0,
                std_dev_nanos: 100.0,
            });
        }
        history
    }

    #[test]
    fn test_ascii_chart_empty() {
        let chart = AsciiChart::new("Empty Chart");
        let output = chart.render();
        assert!(output.contains("No data"));
    }

    #[test]
    fn test_ascii_chart_with_data() {
        let mut chart = AsciiChart::new("Test Chart");
        chart.add_point("A", 10.0);
        chart.add_point("B", 20.0);
        chart.add_point("C", 15.0);
        let output = chart.render();
        assert!(output.contains("Test Chart"));
        assert!(output.contains("Min:"));
        assert!(output.contains("Max:"));
    }

    #[test]
    fn test_ascii_chart_from_history() {
        let history = create_test_history();
        let config = VisualizationConfig::default();
        let chart = AsciiChart::from_history(&history, &config);
        let output = chart.render();
        assert!(output.contains("test-bench"));
        assert!(output.contains("Points: 10"));
    }

    #[test]
    fn test_csv_exporter_new() {
        let exporter = CsvExporter::new();
        assert!(exporter.headers.is_empty());
        assert!(exporter.rows.is_empty());
    }

    #[test]
    fn test_csv_exporter_from_history() {
        let history = create_test_history();
        let exporter = CsvExporter::from_history(&history);
        let csv = exporter.to_csv();
        assert!(csv.contains("timestamp"));
        assert!(csv.contains("mean_nanos"));
        assert_eq!(csv.lines().count(), 11); // Header + 10 data rows
    }

    #[test]
    fn test_csv_exporter_from_analyses() {
        let analyses = vec![
            TrendAnalysis {
                name: "bench1".to_string(),
                current_mean_nanos: 1000.0,
                moving_average_nanos: Some(950.0),
                moving_std_dev_nanos: Some(50.0),
                z_score: Some(1.0),
                trend: Some(0.1),
                is_regression: false,
                is_improvement: false,
                confidence: 0.9,
            },
        ];
        let exporter = CsvExporter::from_analyses(&analyses);
        let csv = exporter.to_csv();
        assert!(csv.contains("bench1"));
        assert!(csv.contains("z_score"));
    }

    #[test]
    fn test_html_report_generator_basic() {
        let mut generator = HtmlReportGenerator::new("Test Report");
        generator.add_header("Section 1");
        generator.add_paragraph("This is a test.");
        let html = generator.generate();
        assert!(html.contains("<title>Test Report</title>"));
        assert!(html.contains("Section 1"));
        assert!(html.contains("This is a test."));
    }

    #[test]
    fn test_html_report_generator_with_chart() {
        let history = create_test_history();
        let mut generator = HtmlReportGenerator::new("Chart Test");
        generator.add_history_chart(&history);
        let html = generator.generate();
        assert!(html.contains("chart.js"));
        assert!(html.contains("canvas"));
    }

    #[test]
    fn test_html_report_generator_analysis_table() {
        let analyses = vec![
            TrendAnalysis {
                name: "fast-bench".to_string(),
                current_mean_nanos: 500.0,
                moving_average_nanos: Some(1000.0),
                moving_std_dev_nanos: Some(100.0),
                z_score: Some(-5.0),
                trend: Some(-0.3),
                is_regression: false,
                is_improvement: true,
                confidence: 0.95,
            },
            TrendAnalysis {
                name: "slow-bench".to_string(),
                current_mean_nanos: 2000.0,
                moving_average_nanos: Some(1000.0),
                moving_std_dev_nanos: Some(100.0),
                z_score: Some(10.0),
                trend: Some(0.5),
                is_regression: true,
                is_improvement: false,
                confidence: 0.95,
            },
        ];
        let mut generator = HtmlReportGenerator::new("Analysis Test");
        generator.add_analysis_table(&analyses);
        let html = generator.generate();
        assert!(html.contains("fast-bench"));
        assert!(html.contains("slow-bench"));
        assert!(html.contains("REGRESSION"));
        assert!(html.contains("IMPROVEMENT"));
    }

    #[test]
    fn test_escape_html() {
        assert_eq!(escape_html("<script>"), "&lt;script&gt;");
        assert_eq!(escape_html("a & b"), "a &amp; b");
        assert_eq!(escape_html("\"quote\""), "&quot;quote&quot;");
    }

    #[test]
    fn test_visualization_config_default() {
        let config = VisualizationConfig::default();
        assert_eq!(config.chart_width, 60);
        assert_eq!(config.chart_height, 15);
        assert_eq!(config.max_chart_points, 30);
    }
}
