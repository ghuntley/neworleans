//! Fault injector trait and types.
//!
//! This module defines the core trait for fault injection and the types
//! that describe faults and their schedules.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{ChaosError, ChaosResult};

/// Unique identifier for a fault.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FaultId(String);

impl FaultId {
    /// Create a new random fault ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create a fault ID from a string.
    pub fn from_str(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for FaultId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FaultId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Type of fault being injected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FaultType {
    /// Network delay injection.
    NetworkDelay,
    /// Network packet loss injection.
    NetworkPacketLoss,
    /// Network partition (isolate nodes).
    NetworkPartition,
    /// Bandwidth throttling.
    NetworkBandwidthThrottle,
    /// Process kill (SIGKILL).
    ProcessKill,
    /// Process pause (SIGSTOP).
    ProcessPause,
    /// Process resume (SIGCONT).
    ProcessResume,
    /// Memory pressure simulation.
    MemoryPressure,
    /// CPU throttling.
    CpuThrottle,
    /// Storage read failure.
    StorageReadFailure,
    /// Storage write failure.
    StorageWriteFailure,
    /// Storage latency injection.
    StorageLatency,
    /// Storage corruption simulation.
    StorageCorruption,
    /// Custom fault type.
    Custom(String),
}

impl std::fmt::Display for FaultType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultType::NetworkDelay => write!(f, "NetworkDelay"),
            FaultType::NetworkPacketLoss => write!(f, "NetworkPacketLoss"),
            FaultType::NetworkPartition => write!(f, "NetworkPartition"),
            FaultType::NetworkBandwidthThrottle => write!(f, "NetworkBandwidthThrottle"),
            FaultType::ProcessKill => write!(f, "ProcessKill"),
            FaultType::ProcessPause => write!(f, "ProcessPause"),
            FaultType::ProcessResume => write!(f, "ProcessResume"),
            FaultType::MemoryPressure => write!(f, "MemoryPressure"),
            FaultType::CpuThrottle => write!(f, "CpuThrottle"),
            FaultType::StorageReadFailure => write!(f, "StorageReadFailure"),
            FaultType::StorageWriteFailure => write!(f, "StorageWriteFailure"),
            FaultType::StorageLatency => write!(f, "StorageLatency"),
            FaultType::StorageCorruption => write!(f, "StorageCorruption"),
            FaultType::Custom(name) => write!(f, "Custom({})", name),
        }
    }
}

/// Target of fault injection.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FaultTarget {
    /// Target a specific silo by address.
    Silo(String),
    /// Target a specific process by PID.
    Process(u32),
    /// Target a network connection between two endpoints.
    Connection { source: String, destination: String },
    /// Target storage operations for a specific grain.
    GrainStorage(String),
    /// Target all silos.
    AllSilos,
    /// Random silo from the cluster.
    RandomSilo,
}

impl std::fmt::Display for FaultTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultTarget::Silo(addr) => write!(f, "Silo({})", addr),
            FaultTarget::Process(pid) => write!(f, "Process({})", pid),
            FaultTarget::Connection { source, destination } => {
                write!(f, "Connection({} -> {})", source, destination)
            }
            FaultTarget::GrainStorage(grain_id) => write!(f, "GrainStorage({})", grain_id),
            FaultTarget::AllSilos => write!(f, "AllSilos"),
            FaultTarget::RandomSilo => write!(f, "RandomSilo"),
        }
    }
}

/// Current status of a fault.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FaultStatus {
    /// Fault is scheduled but not yet active.
    Scheduled,
    /// Fault is currently active.
    Active,
    /// Fault has completed.
    Completed,
    /// Fault was cancelled.
    Cancelled,
    /// Fault failed to inject.
    Failed,
}

impl std::fmt::Display for FaultStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FaultStatus::Scheduled => write!(f, "Scheduled"),
            FaultStatus::Active => write!(f, "Active"),
            FaultStatus::Completed => write!(f, "Completed"),
            FaultStatus::Cancelled => write!(f, "Cancelled"),
            FaultStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// Schedule for fault injection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultSchedule {
    /// When to start the fault injection.
    pub start_time: ScheduleTime,
    /// Duration of the fault (None = permanent until cancelled).
    pub duration: Option<Duration>,
    /// Probability of fault occurring (0.0 to 1.0).
    pub probability: f64,
    /// Repeat configuration for recurring faults.
    pub repeat: Option<RepeatConfig>,
}

impl FaultSchedule {
    /// Create an immediate one-time fault.
    pub fn immediate(duration: Option<Duration>) -> Self {
        Self {
            start_time: ScheduleTime::Immediate,
            duration,
            probability: 1.0,
            repeat: None,
        }
    }

    /// Create a delayed fault.
    pub fn delayed(delay: Duration, duration: Option<Duration>) -> Self {
        Self {
            start_time: ScheduleTime::Delay(delay),
            duration,
            probability: 1.0,
            repeat: None,
        }
    }

    /// Create a fault at a specific time.
    pub fn at(time: DateTime<Utc>, duration: Option<Duration>) -> Self {
        Self {
            start_time: ScheduleTime::At(time),
            duration,
            probability: 1.0,
            repeat: None,
        }
    }

    /// Set the probability of the fault occurring.
    pub fn with_probability(mut self, probability: f64) -> Self {
        self.probability = probability;
        self
    }

    /// Set the repeat configuration.
    pub fn with_repeat(mut self, repeat: RepeatConfig) -> Self {
        self.repeat = Some(repeat);
        self
    }

    /// Validate the schedule.
    pub fn validate(&self) -> ChaosResult<()> {
        if self.probability < 0.0 || self.probability > 1.0 {
            return Err(ChaosError::InvalidProbability {
                probability: self.probability,
            });
        }
        if let Some(repeat) = &self.repeat {
            if repeat.interval < Duration::from_millis(100) {
                return Err(ChaosError::invalid_schedule(
                    "Repeat interval must be at least 100ms",
                ));
            }
        }
        Ok(())
    }
}

/// When to start a fault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScheduleTime {
    /// Start immediately.
    Immediate,
    /// Start after a delay.
    Delay(Duration),
    /// Start at a specific time.
    At(DateTime<Utc>),
}

/// Configuration for repeating faults.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatConfig {
    /// Interval between fault occurrences.
    pub interval: Duration,
    /// Maximum number of repetitions (None = infinite).
    pub max_count: Option<u32>,
}

impl RepeatConfig {
    /// Create a repeat config with fixed interval.
    pub fn every(interval: Duration) -> Self {
        Self {
            interval,
            max_count: None,
        }
    }

    /// Set maximum repetition count.
    pub fn with_max_count(mut self, count: u32) -> Self {
        self.max_count = Some(count);
        self
    }
}

/// Description of a fault to be injected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultDescriptor {
    /// Unique identifier for this fault.
    pub id: FaultId,
    /// Name/label for this fault.
    pub name: String,
    /// Type of fault.
    pub fault_type: FaultType,
    /// Target of the fault.
    pub target: FaultTarget,
    /// Schedule for the fault.
    pub schedule: FaultSchedule,
    /// Fault-specific parameters.
    pub parameters: FaultParameters,
}

impl FaultDescriptor {
    /// Create a new fault descriptor.
    pub fn new(
        name: impl Into<String>,
        fault_type: FaultType,
        target: FaultTarget,
        schedule: FaultSchedule,
        parameters: FaultParameters,
    ) -> Self {
        Self {
            id: FaultId::new(),
            name: name.into(),
            fault_type,
            target,
            schedule,
            parameters,
        }
    }

    /// Create with a specific ID.
    pub fn with_id(mut self, id: FaultId) -> Self {
        self.id = id;
        self
    }
}

/// Fault-specific parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FaultParameters {
    /// Delay to inject (for delay faults).
    pub delay: Option<Duration>,
    /// Loss percentage (for packet loss faults).
    pub loss_percent: Option<f64>,
    /// Bandwidth limit in bytes per second.
    pub bandwidth_limit: Option<u64>,
    /// Memory to allocate in bytes (for memory pressure).
    pub memory_bytes: Option<u64>,
    /// CPU usage percentage (for CPU throttle).
    pub cpu_percent: Option<f64>,
    /// Custom parameters as JSON.
    pub custom: Option<serde_json::Value>,
}

impl FaultParameters {
    /// Create empty parameters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set delay parameter.
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set loss percentage.
    pub fn with_loss_percent(mut self, loss: f64) -> Self {
        self.loss_percent = Some(loss);
        self
    }

    /// Set bandwidth limit.
    pub fn with_bandwidth_limit(mut self, limit: u64) -> Self {
        self.bandwidth_limit = Some(limit);
        self
    }

    /// Set memory bytes.
    pub fn with_memory_bytes(mut self, bytes: u64) -> Self {
        self.memory_bytes = Some(bytes);
        self
    }

    /// Set CPU percentage.
    pub fn with_cpu_percent(mut self, percent: f64) -> Self {
        self.cpu_percent = Some(percent);
        self
    }

    /// Set custom parameters.
    pub fn with_custom(mut self, custom: serde_json::Value) -> Self {
        self.custom = Some(custom);
        self
    }
}

/// State of an active fault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultState {
    /// The fault descriptor.
    pub descriptor: FaultDescriptor,
    /// Current status.
    pub status: FaultStatus,
    /// Time when fault was created.
    pub created_at: DateTime<Utc>,
    /// Time when fault became active.
    pub activated_at: Option<DateTime<Utc>>,
    /// Time when fault ended.
    pub ended_at: Option<DateTime<Utc>>,
    /// Number of times the fault has been applied.
    pub application_count: u32,
    /// Error message if fault failed.
    pub error_message: Option<String>,
}

impl FaultState {
    /// Create a new fault state.
    pub fn new(descriptor: FaultDescriptor) -> Self {
        Self {
            descriptor,
            status: FaultStatus::Scheduled,
            created_at: Utc::now(),
            activated_at: None,
            ended_at: None,
            application_count: 0,
            error_message: None,
        }
    }

    /// Mark the fault as active.
    pub fn activate(&mut self) {
        self.status = FaultStatus::Active;
        self.activated_at = Some(Utc::now());
        self.application_count += 1;
    }

    /// Mark the fault as completed.
    pub fn complete(&mut self) {
        self.status = FaultStatus::Completed;
        self.ended_at = Some(Utc::now());
    }

    /// Mark the fault as cancelled.
    pub fn cancel(&mut self) {
        self.status = FaultStatus::Cancelled;
        self.ended_at = Some(Utc::now());
    }

    /// Mark the fault as failed.
    pub fn fail(&mut self, message: String) {
        self.status = FaultStatus::Failed;
        self.ended_at = Some(Utc::now());
        self.error_message = Some(message);
    }

    /// Get the duration the fault was active.
    pub fn active_duration(&self) -> Option<Duration> {
        match (self.activated_at, self.ended_at) {
            (Some(start), Some(end)) => Some(
                (end - start)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0)),
            ),
            (Some(start), None) => Some(
                (Utc::now() - start)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0)),
            ),
            _ => None,
        }
    }
}

/// Trait for fault injectors.
///
/// Each fault type (network, process, storage) implements this trait
/// to provide the actual fault injection logic.
#[async_trait]
pub trait FaultInjector: Send + Sync + std::fmt::Debug {
    /// Get the name of this injector.
    fn name(&self) -> &str;

    /// Get the fault types this injector can handle.
    fn supported_fault_types(&self) -> Vec<FaultType>;

    /// Check if this injector can handle the given fault type.
    fn can_handle(&self, fault_type: &FaultType) -> bool {
        self.supported_fault_types().contains(fault_type)
    }

    /// Inject a fault.
    async fn inject(&self, descriptor: &FaultDescriptor) -> ChaosResult<()>;

    /// Remove/heal an injected fault.
    async fn heal(&self, fault_id: &FaultId) -> ChaosResult<()>;

    /// Check if a fault is currently active.
    async fn is_active(&self, fault_id: &FaultId) -> bool;

    /// Get all currently active faults for this injector.
    async fn active_faults(&self) -> Vec<FaultId>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fault_id_generation() {
        let id1 = FaultId::new();
        let id2 = FaultId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_fault_id_from_str() {
        let id = FaultId::from_str("test-fault-123");
        assert_eq!(id.as_str(), "test-fault-123");
        assert_eq!(id.to_string(), "test-fault-123");
    }

    #[test]
    fn test_fault_type_display() {
        assert_eq!(FaultType::NetworkDelay.to_string(), "NetworkDelay");
        assert_eq!(FaultType::ProcessKill.to_string(), "ProcessKill");
        assert_eq!(
            FaultType::Custom("MyFault".to_string()).to_string(),
            "Custom(MyFault)"
        );
    }

    #[test]
    fn test_fault_target_display() {
        assert_eq!(FaultTarget::AllSilos.to_string(), "AllSilos");
        assert_eq!(
            FaultTarget::Silo("127.0.0.1:8080".to_string()).to_string(),
            "Silo(127.0.0.1:8080)"
        );
        assert_eq!(FaultTarget::Process(1234).to_string(), "Process(1234)");
        assert_eq!(
            FaultTarget::Connection {
                source: "a".to_string(),
                destination: "b".to_string()
            }
            .to_string(),
            "Connection(a -> b)"
        );
    }

    #[test]
    fn test_fault_status_display() {
        assert_eq!(FaultStatus::Scheduled.to_string(), "Scheduled");
        assert_eq!(FaultStatus::Active.to_string(), "Active");
        assert_eq!(FaultStatus::Completed.to_string(), "Completed");
        assert_eq!(FaultStatus::Cancelled.to_string(), "Cancelled");
        assert_eq!(FaultStatus::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_fault_schedule_immediate() {
        let schedule = FaultSchedule::immediate(Some(Duration::from_secs(10)));
        assert!(matches!(schedule.start_time, ScheduleTime::Immediate));
        assert_eq!(schedule.duration, Some(Duration::from_secs(10)));
        assert_eq!(schedule.probability, 1.0);
    }

    #[test]
    fn test_fault_schedule_delayed() {
        let schedule = FaultSchedule::delayed(Duration::from_secs(5), None);
        assert!(matches!(
            schedule.start_time,
            ScheduleTime::Delay(d) if d == Duration::from_secs(5)
        ));
    }

    #[test]
    fn test_fault_schedule_at() {
        let time = Utc::now();
        let schedule = FaultSchedule::at(time, None);
        assert!(matches!(schedule.start_time, ScheduleTime::At(_)));
    }

    #[test]
    fn test_fault_schedule_validation() {
        let schedule = FaultSchedule::immediate(None).with_probability(0.5);
        assert!(schedule.validate().is_ok());

        let invalid = FaultSchedule::immediate(None).with_probability(1.5);
        assert!(invalid.validate().is_err());

        let invalid_repeat = FaultSchedule::immediate(None).with_repeat(RepeatConfig {
            interval: Duration::from_millis(10), // Too short
            max_count: None,
        });
        assert!(invalid_repeat.validate().is_err());
    }

    #[test]
    fn test_repeat_config() {
        let repeat = RepeatConfig::every(Duration::from_secs(60)).with_max_count(5);
        assert_eq!(repeat.interval, Duration::from_secs(60));
        assert_eq!(repeat.max_count, Some(5));
    }

    #[test]
    fn test_fault_parameters() {
        let params = FaultParameters::new()
            .with_delay(Duration::from_millis(100))
            .with_loss_percent(0.1)
            .with_bandwidth_limit(1024 * 1024)
            .with_memory_bytes(1024 * 1024 * 100)
            .with_cpu_percent(0.5);

        assert_eq!(params.delay, Some(Duration::from_millis(100)));
        assert_eq!(params.loss_percent, Some(0.1));
        assert_eq!(params.bandwidth_limit, Some(1024 * 1024));
        assert_eq!(params.memory_bytes, Some(1024 * 1024 * 100));
        assert_eq!(params.cpu_percent, Some(0.5));
    }

    #[test]
    fn test_fault_state_lifecycle() {
        let descriptor = FaultDescriptor::new(
            "test-fault",
            FaultType::NetworkDelay,
            FaultTarget::RandomSilo,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let mut state = FaultState::new(descriptor);
        assert_eq!(state.status, FaultStatus::Scheduled);
        assert!(state.activated_at.is_none());

        state.activate();
        assert_eq!(state.status, FaultStatus::Active);
        assert!(state.activated_at.is_some());
        assert_eq!(state.application_count, 1);

        state.complete();
        assert_eq!(state.status, FaultStatus::Completed);
        assert!(state.ended_at.is_some());
        assert!(state.active_duration().is_some());
    }

    #[test]
    fn test_fault_state_failure() {
        let descriptor = FaultDescriptor::new(
            "test-fault",
            FaultType::ProcessKill,
            FaultTarget::Process(1234),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let mut state = FaultState::new(descriptor);
        state.fail("Process not found".to_string());
        assert_eq!(state.status, FaultStatus::Failed);
        assert_eq!(state.error_message, Some("Process not found".to_string()));
    }

    #[test]
    fn test_fault_state_cancel() {
        let descriptor = FaultDescriptor::new(
            "test-fault",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let mut state = FaultState::new(descriptor);
        state.activate();
        state.cancel();
        assert_eq!(state.status, FaultStatus::Cancelled);
        assert!(state.ended_at.is_some());
    }

    #[test]
    fn test_fault_descriptor_with_id() {
        let id = FaultId::from_str("my-custom-id");
        let descriptor = FaultDescriptor::new(
            "test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        )
        .with_id(id.clone());
        assert_eq!(descriptor.id, id);
    }
}
