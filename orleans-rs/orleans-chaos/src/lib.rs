//! Orleans-RS Chaos Engineering Framework
//!
//! This crate provides a chaos engineering framework for validating Orleans-RS
//! cluster resilience under adverse conditions. It enables fault injection for
//! network, process, and storage issues, along with comprehensive reporting.
//!
//! # Overview
//!
//! The chaos framework consists of:
//!
//! - **ChaosController**: Orchestrates fault injection across different injectors
//! - **NetworkFaultInjector**: Handles network faults (delay, loss, partition)
//! - **ProcessFaultInjector**: Handles process faults (kill, pause, memory)
//! - **StorageFaultInjector**: Handles storage faults (read/write failure, latency)
//! - **ChaosReporter**: Collects and reports test results
//!
//! # Example
//!
//! ```rust,ignore
//! use orleans_chaos::{
//!     ChaosController, ChaosControllerConfig, ChaosReporter,
//!     FaultDescriptor, FaultType, FaultTarget, FaultSchedule, FaultParameters,
//! };
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a reporter
//!     let reporter = Arc::new(ChaosReporter::new("my-chaos-test"));
//!
//!     // Create and start the controller
//!     let controller = ChaosController::for_testing()
//!         .with_reporter(reporter.clone());
//!     controller.start().await?;
//!
//!     // Inject a network delay fault
//!     let descriptor = FaultDescriptor::new(
//!         "delay-test",
//!         FaultType::NetworkDelay,
//!         FaultTarget::AllSilos,
//!         FaultSchedule::immediate(Some(Duration::from_secs(10))),
//!         FaultParameters::new().with_delay(Duration::from_millis(100)),
//!     );
//!     let fault_id = controller.schedule_fault(descriptor).await?;
//!
//!     // ... run tests while fault is active ...
//!
//!     // Heal the fault
//!     controller.heal_fault(&fault_id).await?;
//!
//!     // Get the report
//!     let report = reporter.generate_json_report()?;
//!     println!("{}", report);
//!
//!     controller.stop().await?;
//!     Ok(())
//! }
//! ```
//!
//! # Fault Types
//!
//! ## Network Faults
//!
//! - `NetworkDelay`: Inject latency into network communication
//! - `NetworkPacketLoss`: Simulate packet loss
//! - `NetworkPartition`: Isolate nodes from each other
//! - `NetworkBandwidthThrottle`: Limit bandwidth
//!
//! ## Process Faults
//!
//! - `ProcessKill`: Terminate a process (SIGKILL)
//! - `ProcessPause`: Pause a process (SIGSTOP)
//! - `ProcessResume`: Resume a paused process (SIGCONT)
//! - `MemoryPressure`: Simulate memory pressure
//! - `CpuThrottle`: Limit CPU usage
//!
//! ## Storage Faults
//!
//! - `StorageReadFailure`: Fail read operations
//! - `StorageWriteFailure`: Fail write operations
//! - `StorageLatency`: Add latency to storage operations
//! - `StorageCorruption`: Corrupt data being read
//!
//! # Scheduling
//!
//! Faults can be scheduled with various timing options:
//!
//! - `FaultSchedule::immediate()`: Inject immediately
//! - `FaultSchedule::delayed(duration)`: Inject after a delay
//! - `FaultSchedule::at(datetime)`: Inject at a specific time
//!
//! Schedules can also include:
//!
//! - Probability (0.0 to 1.0) for probabilistic faults
//! - Repeat configuration for recurring faults
//!
//! # Reporting
//!
//! The `ChaosReporter` collects:
//!
//! - Timeline of all events (fault injection, healing, cluster changes)
//! - Cluster state snapshots
//! - Recovery metrics (detection time, recovery time)
//! - Consistency check results
//!
//! Reports can be generated as JSON for analysis and visualization.

mod controller;
mod error;
mod injector;
mod network;
mod process;
mod reporting;
mod storage;

pub use controller::{ChaosController, ChaosControllerConfig, ControllerStatus};
pub use error::{ChaosError, ChaosResult};
pub use injector::{
    FaultDescriptor, FaultId, FaultInjector, FaultParameters, FaultSchedule, FaultState,
    FaultStatus, FaultTarget, FaultType, RepeatConfig, ScheduleTime,
};
pub use network::{NetworkFaultConfig, NetworkFaultInjector, NetworkFaultState};
pub use process::{ProcessFaultConfig, ProcessFaultInjector, ProcessFaultState, ProcessState};
pub use reporting::{
    ChaosReporter, ChaosTestReport, ClusterSnapshot, RecoveryMetrics, TestRunId, TestRunSummary,
    TimelineEvent, TimelineEventType,
};
pub use storage::{
    corrupt_data, StorageFaultConfig, StorageFaultInjector, StorageFaultState, StorageOperation,
};

/// Prelude module for convenient imports.
pub mod prelude {
    pub use super::{
        ChaosController, ChaosControllerConfig, ChaosError, ChaosReporter, ChaosResult,
        ChaosTestReport, ClusterSnapshot, ControllerStatus, FaultDescriptor, FaultId,
        FaultInjector, FaultParameters, FaultSchedule, FaultState, FaultStatus, FaultTarget,
        FaultType, NetworkFaultConfig, NetworkFaultInjector, ProcessFaultConfig,
        ProcessFaultInjector, RecoveryMetrics, RepeatConfig, ScheduleTime, StorageFaultConfig,
        StorageFaultInjector, TestRunId, TestRunSummary, TimelineEvent, TimelineEventType,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_crate_compiles() {
        // Basic compilation test
        assert!(true);
    }

    #[test]
    fn test_prelude_imports() {
        use crate::prelude::*;

        let _id = FaultId::new();
        let _config = ChaosControllerConfig::for_testing();
        let _reporter = ChaosReporter::new("test");
    }

    #[tokio::test]
    async fn test_full_workflow() {
        use std::sync::Arc;

        // Create reporter
        let reporter = Arc::new(ChaosReporter::new("full-workflow-test"));
        reporter.record_test_started();

        // Create controller
        let controller = ChaosController::for_testing().with_reporter(reporter.clone());
        controller.start().await.unwrap();

        // Inject a network fault
        let net_fault = FaultDescriptor::new(
            "network-delay",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(50)),
        );
        let net_id = controller.schedule_fault(net_fault).await.unwrap();

        // Inject a storage fault
        let storage_fault = FaultDescriptor::new(
            "storage-failure",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-1".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let storage_id = controller.schedule_fault(storage_fault).await.unwrap();

        // Check active faults
        assert_eq!(controller.active_fault_count(), 2);

        // Take a snapshot
        reporter.take_snapshot(
            ClusterSnapshot::new()
                .with_active_silo("silo-1")
                .with_active_silo("silo-2")
                .with_grain_count(100),
        );

        // Record consistency check
        reporter.record_consistency_check(true, "Data consistency verified");

        // Heal faults
        controller.heal_fault(&net_id).await.unwrap();
        controller.heal_fault(&storage_id).await.unwrap();
        assert_eq!(controller.active_fault_count(), 0);

        // Complete test
        reporter.record_test_completed(true);

        // Generate report
        let summary = reporter.get_summary();
        assert_eq!(summary.total_faults, 2);
        assert_eq!(summary.consistency_checks, 1);
        assert_eq!(summary.consistency_checks_passed, 1);
        assert_eq!(summary.passed, Some(true));

        // Generate JSON report
        let json = reporter.generate_json_report().unwrap();
        assert!(json.contains("full-workflow-test"));
        assert!(json.contains("NetworkDelay"));
        assert!(json.contains("StorageReadFailure"));

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_network_partition_scenario() {
        let controller = ChaosController::for_testing();
        controller.start().await.unwrap();

        // Create a partition between silo-1 and silo-2
        let partition = FaultDescriptor::new(
            "partition-1-2",
            FaultType::NetworkPartition,
            FaultTarget::Connection {
                source: "silo-1".to_string(),
                destination: "silo-2".to_string(),
            },
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(partition).await.unwrap();

        // Verify partition is active
        assert!(controller.network_injector().is_partitioned("silo-1", "silo-2"));
        assert!(!controller.network_injector().is_partitioned("silo-1", "silo-3"));

        // Heal the partition
        controller.heal_fault(&fault_id).await.unwrap();
        assert!(!controller.network_injector().is_partitioned("silo-1", "silo-2"));

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_process_pause_resume_scenario() {
        let controller = ChaosController::for_testing();
        controller.start().await.unwrap();

        // Pause a process
        let pause = FaultDescriptor::new(
            "pause-process",
            FaultType::ProcessPause,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(pause).await.unwrap();

        // Verify process is paused
        assert!(controller.process_injector().is_paused(9999));

        // Heal (resume) the process
        controller.heal_fault(&fault_id).await.unwrap();
        assert!(!controller.process_injector().is_paused(9999));

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_storage_multiple_failures() {
        let controller = ChaosController::for_testing();
        controller.start().await.unwrap();

        // Inject read failure
        let read_failure = FaultDescriptor::new(
            "read-failure",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-1".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        // Inject write failure
        let write_failure = FaultDescriptor::new(
            "write-failure",
            FaultType::StorageWriteFailure,
            FaultTarget::GrainStorage("grain-2".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        // Inject latency
        let latency = FaultDescriptor::new(
            "storage-latency",
            FaultType::StorageLatency,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(100)),
        );

        controller.schedule_fault(read_failure).await.unwrap();
        controller.schedule_fault(write_failure).await.unwrap();
        controller.schedule_fault(latency).await.unwrap();

        // Check that faults are active
        assert!(controller.storage_injector().should_fail_read("grain-1").is_some());
        assert!(controller.storage_injector().should_fail_write("grain-2").is_some());
        assert!(controller.storage_injector().get_latency("any-grain").is_some());

        // Heal all
        controller.heal_all().await.unwrap();

        assert!(controller.storage_injector().should_fail_read("grain-1").is_none());

        controller.stop().await.unwrap();
    }

    #[test]
    fn test_corrupt_data_function() {
        let original = vec![0xAA; 10];
        let mut data = original.clone();
        corrupt_data(&mut data, 3);

        // Data should be modified
        assert_ne!(data, original);
    }
}
