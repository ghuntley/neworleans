//! Chaos test reporting.
//!
//! This module provides reporting capabilities for chaos tests, including
//! fault timelines, cluster state snapshots, recovery metrics, and
//! data consistency verification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::ChaosResult;
use crate::injector::{FaultId, FaultState, FaultType};

/// Unique identifier for a chaos test run.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TestRunId(String);

impl TestRunId {
    /// Create a new random test run ID.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Create from a string.
    pub fn from_str(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TestRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TestRunId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Event in the chaos test timeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    /// Timestamp of the event.
    pub timestamp: DateTime<Utc>,
    /// Type of event.
    pub event_type: TimelineEventType,
    /// Event description.
    pub description: String,
    /// Associated fault ID (if any).
    pub fault_id: Option<FaultId>,
    /// Additional metadata.
    pub metadata: HashMap<String, String>,
}

impl TimelineEvent {
    /// Create a new timeline event.
    pub fn new(event_type: TimelineEventType, description: impl Into<String>) -> Self {
        Self {
            timestamp: Utc::now(),
            event_type,
            description: description.into(),
            fault_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the fault ID.
    pub fn with_fault_id(mut self, fault_id: FaultId) -> Self {
        self.fault_id = Some(fault_id);
        self
    }

    /// Add metadata.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Type of timeline event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimelineEventType {
    /// Test started.
    TestStarted,
    /// Test completed.
    TestCompleted,
    /// Fault injected.
    FaultInjected,
    /// Fault healed.
    FaultHealed,
    /// Fault failed to inject.
    FaultFailed,
    /// Cluster state changed.
    ClusterStateChanged,
    /// Silo joined.
    SiloJoined,
    /// Silo left.
    SiloLeft,
    /// Silo failed.
    SiloFailed,
    /// Grain activated.
    GrainActivated,
    /// Grain deactivated.
    GrainDeactivated,
    /// Grain migrated.
    GrainMigrated,
    /// Consistency check passed.
    ConsistencyCheckPassed,
    /// Consistency check failed.
    ConsistencyCheckFailed,
    /// Custom event.
    Custom(String),
}

impl std::fmt::Display for TimelineEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimelineEventType::TestStarted => write!(f, "TestStarted"),
            TimelineEventType::TestCompleted => write!(f, "TestCompleted"),
            TimelineEventType::FaultInjected => write!(f, "FaultInjected"),
            TimelineEventType::FaultHealed => write!(f, "FaultHealed"),
            TimelineEventType::FaultFailed => write!(f, "FaultFailed"),
            TimelineEventType::ClusterStateChanged => write!(f, "ClusterStateChanged"),
            TimelineEventType::SiloJoined => write!(f, "SiloJoined"),
            TimelineEventType::SiloLeft => write!(f, "SiloLeft"),
            TimelineEventType::SiloFailed => write!(f, "SiloFailed"),
            TimelineEventType::GrainActivated => write!(f, "GrainActivated"),
            TimelineEventType::GrainDeactivated => write!(f, "GrainDeactivated"),
            TimelineEventType::GrainMigrated => write!(f, "GrainMigrated"),
            TimelineEventType::ConsistencyCheckPassed => write!(f, "ConsistencyCheckPassed"),
            TimelineEventType::ConsistencyCheckFailed => write!(f, "ConsistencyCheckFailed"),
            TimelineEventType::Custom(name) => write!(f, "Custom({})", name),
        }
    }
}

/// Snapshot of cluster state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    /// Timestamp of the snapshot.
    pub timestamp: DateTime<Utc>,
    /// Active silo addresses.
    pub active_silos: Vec<String>,
    /// Failed silo addresses.
    pub failed_silos: Vec<String>,
    /// Number of active grains.
    pub active_grain_count: u64,
    /// Active faults.
    pub active_faults: Vec<FaultState>,
    /// Additional cluster metrics.
    pub metrics: HashMap<String, f64>,
}

impl ClusterSnapshot {
    /// Create a new cluster snapshot.
    pub fn new() -> Self {
        Self {
            timestamp: Utc::now(),
            active_silos: Vec::new(),
            failed_silos: Vec::new(),
            active_grain_count: 0,
            active_faults: Vec::new(),
            metrics: HashMap::new(),
        }
    }

    /// Add an active silo.
    pub fn with_active_silo(mut self, silo: impl Into<String>) -> Self {
        self.active_silos.push(silo.into());
        self
    }

    /// Add a failed silo.
    pub fn with_failed_silo(mut self, silo: impl Into<String>) -> Self {
        self.failed_silos.push(silo.into());
        self
    }

    /// Set the active grain count.
    pub fn with_grain_count(mut self, count: u64) -> Self {
        self.active_grain_count = count;
        self
    }

    /// Add a metric.
    pub fn with_metric(mut self, name: impl Into<String>, value: f64) -> Self {
        self.metrics.insert(name.into(), value);
        self
    }
}

impl Default for ClusterSnapshot {
    fn default() -> Self {
        Self::new()
    }
}

/// Recovery metrics for a fault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryMetrics {
    /// Fault ID.
    pub fault_id: FaultId,
    /// Fault type.
    pub fault_type: FaultType,
    /// Time when fault was injected.
    pub fault_start: DateTime<Utc>,
    /// Time when fault was healed.
    pub fault_end: Option<DateTime<Utc>>,
    /// Time to detect the fault (e.g., failure detection).
    pub detection_time: Option<Duration>,
    /// Time to recover from the fault.
    pub recovery_time: Option<Duration>,
    /// Whether data consistency was maintained.
    pub data_consistent: Option<bool>,
    /// Number of affected operations.
    pub affected_operations: u64,
    /// Number of failed operations.
    pub failed_operations: u64,
}

impl RecoveryMetrics {
    /// Create new recovery metrics.
    pub fn new(fault_id: FaultId, fault_type: FaultType) -> Self {
        Self {
            fault_id,
            fault_type,
            fault_start: Utc::now(),
            fault_end: None,
            detection_time: None,
            recovery_time: None,
            data_consistent: None,
            affected_operations: 0,
            failed_operations: 0,
        }
    }

    /// Mark the fault as ended.
    pub fn mark_ended(&mut self) {
        self.fault_end = Some(Utc::now());
    }

    /// Set detection time.
    pub fn set_detection_time(&mut self, duration: Duration) {
        self.detection_time = Some(duration);
    }

    /// Set recovery time.
    pub fn set_recovery_time(&mut self, duration: Duration) {
        self.recovery_time = Some(duration);
    }

    /// Set data consistency.
    pub fn set_data_consistent(&mut self, consistent: bool) {
        self.data_consistent = Some(consistent);
    }

    /// Increment affected operations.
    pub fn increment_affected(&mut self) {
        self.affected_operations += 1;
    }

    /// Increment failed operations.
    pub fn increment_failed(&mut self) {
        self.failed_operations += 1;
    }

    /// Get fault duration.
    pub fn fault_duration(&self) -> Option<Duration> {
        self.fault_end.map(|end| {
            (end - self.fault_start)
                .to_std()
                .unwrap_or(Duration::from_secs(0))
        })
    }
}

/// Summary of a chaos test run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestRunSummary {
    /// Test run ID.
    pub run_id: TestRunId,
    /// Test name/description.
    pub test_name: String,
    /// Start time.
    pub start_time: DateTime<Utc>,
    /// End time.
    pub end_time: Option<DateTime<Utc>>,
    /// Whether the test passed.
    pub passed: Option<bool>,
    /// Total faults injected.
    pub total_faults: u64,
    /// Faults by type.
    pub faults_by_type: HashMap<String, u64>,
    /// Total consistency checks.
    pub consistency_checks: u64,
    /// Passed consistency checks.
    pub consistency_checks_passed: u64,
    /// Average recovery time.
    pub avg_recovery_time_ms: Option<f64>,
    /// Error messages (if any).
    pub errors: Vec<String>,
}

impl TestRunSummary {
    /// Create a new test run summary.
    pub fn new(run_id: TestRunId, test_name: impl Into<String>) -> Self {
        Self {
            run_id,
            test_name: test_name.into(),
            start_time: Utc::now(),
            end_time: None,
            passed: None,
            total_faults: 0,
            faults_by_type: HashMap::new(),
            consistency_checks: 0,
            consistency_checks_passed: 0,
            avg_recovery_time_ms: None,
            errors: Vec::new(),
        }
    }

    /// Mark the test as completed.
    pub fn complete(&mut self, passed: bool) {
        self.end_time = Some(Utc::now());
        self.passed = Some(passed);
    }

    /// Add an error.
    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
    }

    /// Get test duration.
    pub fn duration(&self) -> Option<Duration> {
        self.end_time.map(|end| {
            (end - self.start_time)
                .to_std()
                .unwrap_or(Duration::from_secs(0))
        })
    }
}

/// Chaos test reporter.
///
/// Collects and reports on chaos test activities including fault injection,
/// cluster state changes, and recovery metrics.
#[derive(Debug)]
pub struct ChaosReporter {
    /// Current test run ID.
    run_id: TestRunId,
    /// Test name.
    test_name: String,
    /// Timeline of events.
    timeline: RwLock<Vec<TimelineEvent>>,
    /// Cluster snapshots.
    snapshots: RwLock<Vec<ClusterSnapshot>>,
    /// Recovery metrics per fault.
    recovery_metrics: RwLock<HashMap<FaultId, RecoveryMetrics>>,
    /// Test summary.
    summary: RwLock<TestRunSummary>,
    /// Event counter.
    event_counter: AtomicU64,
}

impl ChaosReporter {
    /// Create a new chaos reporter.
    pub fn new(test_name: impl Into<String>) -> Self {
        let run_id = TestRunId::new();
        let test_name = test_name.into();
        let summary = TestRunSummary::new(run_id.clone(), test_name.clone());

        info!(run_id = %run_id, test_name = %test_name, "Starting chaos test reporting");

        Self {
            run_id,
            test_name,
            timeline: RwLock::new(Vec::new()),
            snapshots: RwLock::new(Vec::new()),
            recovery_metrics: RwLock::new(HashMap::new()),
            summary: RwLock::new(summary),
            event_counter: AtomicU64::new(0),
        }
    }

    /// Get the test run ID.
    pub fn run_id(&self) -> &TestRunId {
        &self.run_id
    }

    /// Get the test name.
    pub fn test_name(&self) -> &str {
        &self.test_name
    }

    /// Record a timeline event.
    pub fn record_event(&self, event: TimelineEvent) {
        let count = self.event_counter.fetch_add(1, Ordering::Relaxed);
        debug!(
            event_num = count,
            event_type = %event.event_type,
            description = %event.description,
            "Recording chaos event"
        );
        self.timeline.write().push(event);
    }

    /// Record test started.
    pub fn record_test_started(&self) {
        self.record_event(TimelineEvent::new(
            TimelineEventType::TestStarted,
            format!("Test '{}' started", self.test_name),
        ));
    }

    /// Record test completed.
    pub fn record_test_completed(&self, passed: bool) {
        let status = if passed { "passed" } else { "failed" };
        self.record_event(TimelineEvent::new(
            TimelineEventType::TestCompleted,
            format!("Test '{}' {}", self.test_name, status),
        ));
        self.summary.write().complete(passed);
    }

    /// Record fault injection.
    pub fn record_fault_injected(&self, fault_state: &FaultState) {
        let event = TimelineEvent::new(
            TimelineEventType::FaultInjected,
            format!(
                "Fault '{}' ({}) injected on {}",
                fault_state.descriptor.name,
                fault_state.descriptor.fault_type,
                fault_state.descriptor.target
            ),
        )
        .with_fault_id(fault_state.descriptor.id.clone());
        self.record_event(event);

        // Track in summary
        let mut summary = self.summary.write();
        summary.total_faults += 1;
        *summary
            .faults_by_type
            .entry(fault_state.descriptor.fault_type.to_string())
            .or_insert(0) += 1;

        // Start recovery metrics
        let metrics = RecoveryMetrics::new(
            fault_state.descriptor.id.clone(),
            fault_state.descriptor.fault_type.clone(),
        );
        self.recovery_metrics
            .write()
            .insert(fault_state.descriptor.id.clone(), metrics);
    }

    /// Record fault healed.
    pub fn record_fault_healed(&self, fault_id: &FaultId) {
        self.record_event(
            TimelineEvent::new(
                TimelineEventType::FaultHealed,
                format!("Fault '{}' healed", fault_id),
            )
            .with_fault_id(fault_id.clone()),
        );

        // Update recovery metrics
        if let Some(metrics) = self.recovery_metrics.write().get_mut(fault_id) {
            metrics.mark_ended();
        }
    }

    /// Record fault failure.
    pub fn record_fault_failed(&self, fault_id: &FaultId, error: &str) {
        let event = TimelineEvent::new(
            TimelineEventType::FaultFailed,
            format!("Fault '{}' failed: {}", fault_id, error),
        )
        .with_fault_id(fault_id.clone())
        .with_metadata("error", error);
        self.record_event(event);

        self.summary.write().add_error(error.to_string());
    }

    /// Record cluster state change.
    pub fn record_cluster_state_change(&self, description: impl Into<String>) {
        self.record_event(TimelineEvent::new(
            TimelineEventType::ClusterStateChanged,
            description,
        ));
    }

    /// Record silo joined.
    pub fn record_silo_joined(&self, silo_addr: &str) {
        self.record_event(
            TimelineEvent::new(
                TimelineEventType::SiloJoined,
                format!("Silo '{}' joined cluster", silo_addr),
            )
            .with_metadata("silo", silo_addr),
        );
    }

    /// Record silo left.
    pub fn record_silo_left(&self, silo_addr: &str) {
        self.record_event(
            TimelineEvent::new(
                TimelineEventType::SiloLeft,
                format!("Silo '{}' left cluster", silo_addr),
            )
            .with_metadata("silo", silo_addr),
        );
    }

    /// Record silo failed.
    pub fn record_silo_failed(&self, silo_addr: &str) {
        self.record_event(
            TimelineEvent::new(
                TimelineEventType::SiloFailed,
                format!("Silo '{}' failed", silo_addr),
            )
            .with_metadata("silo", silo_addr),
        );
    }

    /// Record consistency check.
    pub fn record_consistency_check(&self, passed: bool, description: impl Into<String>) {
        let event_type = if passed {
            TimelineEventType::ConsistencyCheckPassed
        } else {
            TimelineEventType::ConsistencyCheckFailed
        };
        self.record_event(TimelineEvent::new(event_type, description));

        let mut summary = self.summary.write();
        summary.consistency_checks += 1;
        if passed {
            summary.consistency_checks_passed += 1;
        }
    }

    /// Take a cluster snapshot.
    pub fn take_snapshot(&self, snapshot: ClusterSnapshot) {
        debug!(
            active_silos = snapshot.active_silos.len(),
            failed_silos = snapshot.failed_silos.len(),
            "Taking cluster snapshot"
        );
        self.snapshots.write().push(snapshot);
    }

    /// Set recovery time for a fault.
    pub fn set_recovery_time(&self, fault_id: &FaultId, duration: Duration) {
        if let Some(metrics) = self.recovery_metrics.write().get_mut(fault_id) {
            metrics.set_recovery_time(duration);
        }
    }

    /// Set data consistency for a fault.
    pub fn set_data_consistency(&self, fault_id: &FaultId, consistent: bool) {
        if let Some(metrics) = self.recovery_metrics.write().get_mut(fault_id) {
            metrics.set_data_consistent(consistent);
        }
    }

    /// Get the event timeline.
    pub fn get_timeline(&self) -> Vec<TimelineEvent> {
        self.timeline.read().clone()
    }

    /// Get cluster snapshots.
    pub fn get_snapshots(&self) -> Vec<ClusterSnapshot> {
        self.snapshots.read().clone()
    }

    /// Get recovery metrics for all faults.
    pub fn get_recovery_metrics(&self) -> HashMap<FaultId, RecoveryMetrics> {
        self.recovery_metrics.read().clone()
    }

    /// Get the test summary.
    pub fn get_summary(&self) -> TestRunSummary {
        let mut summary = self.summary.read().clone();

        // Calculate average recovery time
        let metrics = self.recovery_metrics.read();
        let recovery_times: Vec<f64> = metrics
            .values()
            .filter_map(|m| m.recovery_time)
            .map(|d| d.as_millis() as f64)
            .collect();

        if !recovery_times.is_empty() {
            summary.avg_recovery_time_ms =
                Some(recovery_times.iter().sum::<f64>() / recovery_times.len() as f64);
        }

        summary
    }

    /// Generate a JSON report.
    pub fn generate_json_report(&self) -> ChaosResult<String> {
        let report = ChaosTestReport {
            summary: self.get_summary(),
            timeline: self.get_timeline(),
            snapshots: self.get_snapshots(),
            recovery_metrics: self.get_recovery_metrics().into_values().collect(),
        };

        serde_json::to_string_pretty(&report)
            .map_err(|e| crate::error::ChaosError::reporting_error(format!("JSON serialization failed: {}", e)))
    }
}

/// Complete chaos test report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChaosTestReport {
    /// Test summary.
    pub summary: TestRunSummary,
    /// Event timeline.
    pub timeline: Vec<TimelineEvent>,
    /// Cluster snapshots.
    pub snapshots: Vec<ClusterSnapshot>,
    /// Recovery metrics.
    pub recovery_metrics: Vec<RecoveryMetrics>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_run_id_generation() {
        let id1 = TestRunId::new();
        let id2 = TestRunId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_test_run_id_from_str() {
        let id = TestRunId::from_str("test-run-123");
        assert_eq!(id.as_str(), "test-run-123");
    }

    #[test]
    fn test_timeline_event_creation() {
        let event = TimelineEvent::new(TimelineEventType::FaultInjected, "Test fault injected")
            .with_fault_id(FaultId::from_str("fault-1"))
            .with_metadata("target", "silo-1");

        assert_eq!(event.event_type, TimelineEventType::FaultInjected);
        assert_eq!(event.description, "Test fault injected");
        assert!(event.fault_id.is_some());
        assert_eq!(event.metadata.get("target"), Some(&"silo-1".to_string()));
    }

    #[test]
    fn test_timeline_event_type_display() {
        assert_eq!(TimelineEventType::TestStarted.to_string(), "TestStarted");
        assert_eq!(TimelineEventType::FaultInjected.to_string(), "FaultInjected");
        assert_eq!(
            TimelineEventType::Custom("MyEvent".to_string()).to_string(),
            "Custom(MyEvent)"
        );
    }

    #[test]
    fn test_cluster_snapshot_creation() {
        let snapshot = ClusterSnapshot::new()
            .with_active_silo("silo-1")
            .with_active_silo("silo-2")
            .with_failed_silo("silo-3")
            .with_grain_count(100)
            .with_metric("cpu_usage", 0.5);

        assert_eq!(snapshot.active_silos.len(), 2);
        assert_eq!(snapshot.failed_silos.len(), 1);
        assert_eq!(snapshot.active_grain_count, 100);
        assert_eq!(snapshot.metrics.get("cpu_usage"), Some(&0.5));
    }

    #[test]
    fn test_recovery_metrics() {
        let mut metrics = RecoveryMetrics::new(
            FaultId::from_str("fault-1"),
            FaultType::NetworkDelay,
        );

        metrics.set_detection_time(Duration::from_millis(100));
        metrics.set_recovery_time(Duration::from_secs(5));
        metrics.set_data_consistent(true);
        metrics.increment_affected();
        metrics.increment_affected();
        metrics.increment_failed();

        assert_eq!(metrics.detection_time, Some(Duration::from_millis(100)));
        assert_eq!(metrics.recovery_time, Some(Duration::from_secs(5)));
        assert_eq!(metrics.data_consistent, Some(true));
        assert_eq!(metrics.affected_operations, 2);
        assert_eq!(metrics.failed_operations, 1);
    }

    #[test]
    fn test_recovery_metrics_fault_duration() {
        let mut metrics = RecoveryMetrics::new(
            FaultId::from_str("fault-1"),
            FaultType::NetworkDelay,
        );
        assert!(metrics.fault_duration().is_none());

        metrics.mark_ended();
        assert!(metrics.fault_duration().is_some());
    }

    #[test]
    fn test_test_run_summary() {
        let mut summary = TestRunSummary::new(TestRunId::new(), "test-chaos");
        assert!(summary.end_time.is_none());
        assert!(summary.passed.is_none());

        summary.complete(true);
        assert!(summary.end_time.is_some());
        assert_eq!(summary.passed, Some(true));
        assert!(summary.duration().is_some());
    }

    #[test]
    fn test_test_run_summary_errors() {
        let mut summary = TestRunSummary::new(TestRunId::new(), "test-chaos");
        summary.add_error("Error 1");
        summary.add_error("Error 2");
        assert_eq!(summary.errors.len(), 2);
    }

    #[test]
    fn test_chaos_reporter_creation() {
        let reporter = ChaosReporter::new("test-chaos");
        assert!(!reporter.run_id().as_str().is_empty());
        assert_eq!(reporter.test_name(), "test-chaos");
    }

    #[test]
    fn test_chaos_reporter_record_event() {
        let reporter = ChaosReporter::new("test-chaos");
        reporter.record_event(TimelineEvent::new(
            TimelineEventType::TestStarted,
            "Test started",
        ));
        reporter.record_event(TimelineEvent::new(
            TimelineEventType::FaultInjected,
            "Fault injected",
        ));

        let timeline = reporter.get_timeline();
        assert_eq!(timeline.len(), 2);
    }

    #[test]
    fn test_chaos_reporter_test_lifecycle() {
        let reporter = ChaosReporter::new("test-chaos");
        reporter.record_test_started();
        reporter.record_test_completed(true);

        let summary = reporter.get_summary();
        assert_eq!(summary.passed, Some(true));
    }

    #[test]
    fn test_chaos_reporter_fault_tracking() {
        use crate::injector::{FaultDescriptor, FaultSchedule, FaultParameters, FaultTarget};

        let reporter = ChaosReporter::new("test-chaos");

        let descriptor = FaultDescriptor::new(
            "test-fault",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let fault_state = FaultState::new(descriptor);

        reporter.record_fault_injected(&fault_state);

        let summary = reporter.get_summary();
        assert_eq!(summary.total_faults, 1);
        assert_eq!(
            summary.faults_by_type.get("NetworkDelay"),
            Some(&1)
        );
    }

    #[test]
    fn test_chaos_reporter_consistency_checks() {
        let reporter = ChaosReporter::new("test-chaos");

        reporter.record_consistency_check(true, "Check 1 passed");
        reporter.record_consistency_check(true, "Check 2 passed");
        reporter.record_consistency_check(false, "Check 3 failed");

        let summary = reporter.get_summary();
        assert_eq!(summary.consistency_checks, 3);
        assert_eq!(summary.consistency_checks_passed, 2);
    }

    #[test]
    fn test_chaos_reporter_snapshots() {
        let reporter = ChaosReporter::new("test-chaos");

        reporter.take_snapshot(ClusterSnapshot::new().with_active_silo("silo-1"));
        reporter.take_snapshot(ClusterSnapshot::new().with_active_silo("silo-1").with_active_silo("silo-2"));

        let snapshots = reporter.get_snapshots();
        assert_eq!(snapshots.len(), 2);
    }

    #[test]
    fn test_chaos_reporter_recovery_time() {
        use crate::injector::{FaultDescriptor, FaultSchedule, FaultParameters, FaultTarget};

        let reporter = ChaosReporter::new("test-chaos");

        let descriptor = FaultDescriptor::new(
            "test-fault",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let fault_state = FaultState::new(descriptor.clone());

        reporter.record_fault_injected(&fault_state);
        reporter.set_recovery_time(&descriptor.id, Duration::from_secs(5));

        let metrics = reporter.get_recovery_metrics();
        let fault_metrics = metrics.get(&descriptor.id).unwrap();
        assert_eq!(fault_metrics.recovery_time, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_chaos_reporter_json_report() {
        let reporter = ChaosReporter::new("test-chaos");
        reporter.record_test_started();
        reporter.record_silo_joined("silo-1");
        reporter.record_test_completed(true);

        let json = reporter.generate_json_report().unwrap();
        assert!(json.contains("test-chaos"));
        assert!(json.contains("SiloJoined"));
    }

    #[test]
    fn test_chaos_reporter_silo_events() {
        let reporter = ChaosReporter::new("test-chaos");

        reporter.record_silo_joined("silo-1");
        reporter.record_silo_left("silo-2");
        reporter.record_silo_failed("silo-3");

        let timeline = reporter.get_timeline();
        assert_eq!(timeline.len(), 3);
        assert!(timeline.iter().any(|e| e.event_type == TimelineEventType::SiloJoined));
        assert!(timeline.iter().any(|e| e.event_type == TimelineEventType::SiloLeft));
        assert!(timeline.iter().any(|e| e.event_type == TimelineEventType::SiloFailed));
    }

    #[test]
    fn test_chaos_reporter_avg_recovery_time() {
        use crate::injector::{FaultDescriptor, FaultSchedule, FaultParameters, FaultTarget};

        let reporter = ChaosReporter::new("test-chaos");

        // Inject multiple faults with different recovery times
        for i in 0..3 {
            let descriptor = FaultDescriptor::new(
                format!("fault-{}", i),
                FaultType::NetworkDelay,
                FaultTarget::AllSilos,
                FaultSchedule::immediate(None),
                FaultParameters::new(),
            );
            let fault_state = FaultState::new(descriptor.clone());
            reporter.record_fault_injected(&fault_state);
            reporter.set_recovery_time(&descriptor.id, Duration::from_secs((i + 1) as u64));
        }

        let summary = reporter.get_summary();
        // (1000 + 2000 + 3000) / 3 = 2000
        assert_eq!(summary.avg_recovery_time_ms, Some(2000.0));
    }
}
