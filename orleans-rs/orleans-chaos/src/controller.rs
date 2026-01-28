//! Chaos controller for orchestrating fault injection.
//!
//! This module provides the `ChaosController` which manages fault injectors,
//! schedules fault injection, and coordinates chaos testing activities.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, warn};

use crate::error::{ChaosError, ChaosResult};
use crate::injector::{
    FaultDescriptor, FaultId, FaultInjector, FaultState, FaultStatus, ScheduleTime,
};

#[cfg(test)]
use crate::injector::{FaultParameters, FaultSchedule, FaultTarget, FaultType};
use crate::network::{NetworkFaultConfig, NetworkFaultInjector};
use crate::process::{ProcessFaultConfig, ProcessFaultInjector};
use crate::reporting::ChaosReporter;
use crate::storage::{StorageFaultConfig, StorageFaultInjector};

/// Configuration for the chaos controller.
#[derive(Debug, Clone)]
pub struct ChaosControllerConfig {
    /// Network fault injector configuration.
    pub network_config: NetworkFaultConfig,
    /// Process fault injector configuration.
    pub process_config: ProcessFaultConfig,
    /// Storage fault injector configuration.
    pub storage_config: StorageFaultConfig,
    /// Minimum delay between fault injections.
    pub min_injection_delay: Duration,
    /// Maximum concurrent faults.
    pub max_concurrent_faults: usize,
    /// Fault execution timeout.
    pub fault_timeout: Duration,
    /// Enable automatic fault scheduling.
    pub enable_scheduling: bool,
}

impl Default for ChaosControllerConfig {
    fn default() -> Self {
        Self {
            network_config: NetworkFaultConfig::default(),
            process_config: ProcessFaultConfig::default(),
            storage_config: StorageFaultConfig::default(),
            min_injection_delay: Duration::from_millis(100),
            max_concurrent_faults: 10,
            fault_timeout: Duration::from_secs(300),
            enable_scheduling: true,
        }
    }
}

impl ChaosControllerConfig {
    /// Create a new configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set network fault config.
    pub fn with_network_config(mut self, config: NetworkFaultConfig) -> Self {
        self.network_config = config;
        self
    }

    /// Set process fault config.
    pub fn with_process_config(mut self, config: ProcessFaultConfig) -> Self {
        self.process_config = config;
        self
    }

    /// Set storage fault config.
    pub fn with_storage_config(mut self, config: StorageFaultConfig) -> Self {
        self.storage_config = config;
        self
    }

    /// Set maximum concurrent faults.
    pub fn with_max_concurrent_faults(mut self, max: usize) -> Self {
        self.max_concurrent_faults = max;
        self
    }

    /// Set fault timeout.
    pub fn with_fault_timeout(mut self, timeout: Duration) -> Self {
        self.fault_timeout = timeout;
        self
    }

    /// Create configuration for testing.
    pub fn for_testing() -> Self {
        Self {
            network_config: NetworkFaultConfig::for_testing(),
            process_config: ProcessFaultConfig::for_testing(),
            storage_config: StorageFaultConfig::for_testing(),
            min_injection_delay: Duration::from_millis(10),
            max_concurrent_faults: 5,
            fault_timeout: Duration::from_secs(30),
            enable_scheduling: true,
        }
    }
}

/// Status of the chaos controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControllerStatus {
    /// Controller is not started.
    Stopped,
    /// Controller is starting.
    Starting,
    /// Controller is running.
    Running,
    /// Controller is stopping.
    Stopping,
}

/// Chaos controller for managing fault injection.
///
/// The controller coordinates multiple fault injectors (network, process, storage)
/// and manages the lifecycle of injected faults.
pub struct ChaosController {
    /// Configuration.
    config: ChaosControllerConfig,
    /// Controller status.
    status: RwLock<ControllerStatus>,
    /// Network fault injector.
    network_injector: Arc<NetworkFaultInjector>,
    /// Process fault injector.
    process_injector: Arc<ProcessFaultInjector>,
    /// Storage fault injector.
    storage_injector: Arc<StorageFaultInjector>,
    /// All fault states.
    fault_states: DashMap<FaultId, FaultState>,
    /// Scheduled faults waiting to be injected.
    scheduled_faults: DashMap<FaultId, FaultDescriptor>,
    /// Fault counter.
    fault_counter: AtomicU64,
    /// Is running flag.
    is_running: AtomicBool,
    /// Shutdown signal sender.
    shutdown_tx: RwLock<Option<mpsc::Sender<()>>>,
    /// Reporter for test results.
    reporter: Option<Arc<ChaosReporter>>,
}

impl std::fmt::Debug for ChaosController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChaosController")
            .field("status", &self.status)
            .field("fault_count", &self.fault_counter)
            .field("is_running", &self.is_running)
            .finish()
    }
}

impl ChaosController {
    /// Create a new chaos controller.
    pub fn new(config: ChaosControllerConfig) -> Self {
        info!("Creating chaos controller");

        let network_injector = Arc::new(NetworkFaultInjector::new(config.network_config.clone()));
        let process_injector = Arc::new(ProcessFaultInjector::new(config.process_config.clone()));
        let storage_injector = Arc::new(StorageFaultInjector::new(config.storage_config.clone()));

        Self {
            config,
            status: RwLock::new(ControllerStatus::Stopped),
            network_injector,
            process_injector,
            storage_injector,
            fault_states: DashMap::new(),
            scheduled_faults: DashMap::new(),
            fault_counter: AtomicU64::new(0),
            is_running: AtomicBool::new(false),
            shutdown_tx: RwLock::new(None),
            reporter: None,
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ChaosControllerConfig::default())
    }

    /// Create for testing.
    pub fn for_testing() -> Self {
        Self::new(ChaosControllerConfig::for_testing())
    }

    /// Set the reporter for this controller.
    pub fn with_reporter(mut self, reporter: Arc<ChaosReporter>) -> Self {
        self.reporter = Some(reporter);
        self
    }

    /// Get the controller status.
    pub fn status(&self) -> ControllerStatus {
        *self.status.read()
    }

    /// Check if the controller is running.
    pub fn is_running(&self) -> bool {
        self.is_running.load(Ordering::SeqCst)
    }

    /// Get the network fault injector.
    pub fn network_injector(&self) -> Arc<NetworkFaultInjector> {
        self.network_injector.clone()
    }

    /// Get the process fault injector.
    pub fn process_injector(&self) -> Arc<ProcessFaultInjector> {
        self.process_injector.clone()
    }

    /// Get the storage fault injector.
    pub fn storage_injector(&self) -> Arc<StorageFaultInjector> {
        self.storage_injector.clone()
    }

    /// Start the chaos controller.
    #[instrument(skip(self))]
    pub async fn start(&self) -> ChaosResult<()> {
        let mut status = self.status.write();
        if *status != ControllerStatus::Stopped {
            return Err(ChaosError::ControllerAlreadyStarted);
        }

        info!("Starting chaos controller");
        *status = ControllerStatus::Starting;

        // Set up shutdown channel
        let (tx, mut rx) = mpsc::channel(1);
        *self.shutdown_tx.write() = Some(tx);

        self.is_running.store(true, Ordering::SeqCst);
        *status = ControllerStatus::Running;

        // Start scheduler task if enabled
        if self.config.enable_scheduling {
            let scheduled = self.scheduled_faults.clone();
            let fault_states = self.fault_states.clone();
            let network = self.network_injector.clone();
            let process = self.process_injector.clone();
            let storage = self.storage_injector.clone();
            let reporter = self.reporter.clone();
            let is_running = &self.is_running;

            tokio::spawn({
                let is_running_clone = is_running.load(Ordering::SeqCst);
                async move {
                    loop {
                        tokio::select! {
                            _ = rx.recv() => {
                                debug!("Scheduler received shutdown signal");
                                break;
                            }
                            _ = sleep(Duration::from_millis(100)) => {
                                if !is_running_clone {
                                    break;
                                }
                                // Process scheduled faults
                                Self::process_scheduled_faults(
                                    &scheduled,
                                    &fault_states,
                                    &network,
                                    &process,
                                    &storage,
                                    &reporter,
                                ).await;
                            }
                        }
                    }
                }
            });
        }

        info!("Chaos controller started");
        Ok(())
    }

    /// Stop the chaos controller.
    #[instrument(skip(self))]
    pub async fn stop(&self) -> ChaosResult<()> {
        let mut status = self.status.write();
        if *status == ControllerStatus::Stopped {
            return Ok(());
        }

        info!("Stopping chaos controller");
        *status = ControllerStatus::Stopping;

        // Signal shutdown
        self.is_running.store(false, Ordering::SeqCst);
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(()).await;
        }

        // Heal all active faults
        self.heal_all().await?;

        *status = ControllerStatus::Stopped;
        info!("Chaos controller stopped");
        Ok(())
    }

    /// Schedule a fault for injection.
    #[instrument(skip(self), fields(fault_name = %descriptor.name, fault_type = %descriptor.fault_type))]
    pub async fn schedule_fault(&self, descriptor: FaultDescriptor) -> ChaosResult<FaultId> {
        if !self.is_running() {
            return Err(ChaosError::ControllerNotStarted);
        }

        // Validate schedule
        descriptor.schedule.validate()?;

        // Check concurrent fault limit
        let active_count = self.active_fault_count();
        if active_count >= self.config.max_concurrent_faults {
            return Err(ChaosError::invalid_schedule(format!(
                "Maximum concurrent faults ({}) reached",
                self.config.max_concurrent_faults
            )));
        }

        let fault_id = descriptor.id.clone();
        info!(fault_id = %fault_id, "Scheduling fault");

        // Check if this is an immediate fault
        let is_immediate = matches!(descriptor.schedule.start_time, ScheduleTime::Immediate);

        if is_immediate {
            // Inject immediately
            self.inject_fault(&descriptor).await?;
        } else {
            // Add to scheduled faults
            self.scheduled_faults.insert(fault_id.clone(), descriptor);
        }

        self.fault_counter.fetch_add(1, Ordering::Relaxed);
        Ok(fault_id)
    }

    /// Inject a fault immediately.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    pub async fn inject_fault(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        debug!("Injecting fault");

        // Create fault state
        let mut state = FaultState::new(descriptor.clone());

        // Get the appropriate injector
        let result = if self.network_injector.can_handle(&descriptor.fault_type) {
            self.network_injector.inject(descriptor).await
        } else if self.process_injector.can_handle(&descriptor.fault_type) {
            self.process_injector.inject(descriptor).await
        } else if self.storage_injector.can_handle(&descriptor.fault_type) {
            self.storage_injector.inject(descriptor).await
        } else {
            Err(ChaosError::injection_failed(
                format!("No injector for fault type: {}", descriptor.fault_type),
                descriptor.fault_type.to_string(),
            ))
        };

        match result {
            Ok(()) => {
                state.activate();
                self.fault_states.insert(descriptor.id.clone(), state.clone());

                // Report if we have a reporter
                if let Some(reporter) = &self.reporter {
                    reporter.record_fault_injected(&state);
                }

                info!(fault_id = %descriptor.id, "Fault injected successfully");
                Ok(())
            }
            Err(e) => {
                state.fail(e.to_string());
                self.fault_states.insert(descriptor.id.clone(), state);

                // Report failure
                if let Some(reporter) = &self.reporter {
                    reporter.record_fault_failed(&descriptor.id, &e.to_string());
                }

                error!(fault_id = %descriptor.id, error = %e, "Fault injection failed");
                Err(e)
            }
        }
    }

    /// Heal a specific fault.
    #[instrument(skip(self), fields(fault_id = %fault_id))]
    pub async fn heal_fault(&self, fault_id: &FaultId) -> ChaosResult<()> {
        if let Some(mut state) = self.fault_states.get_mut(fault_id) {
            if state.status != FaultStatus::Active {
                return Ok(());
            }

            debug!("Healing fault");

            // Get the appropriate injector
            let result = if self.network_injector.can_handle(&state.descriptor.fault_type) {
                self.network_injector.heal(fault_id).await
            } else if self.process_injector.can_handle(&state.descriptor.fault_type) {
                self.process_injector.heal(fault_id).await
            } else if self.storage_injector.can_handle(&state.descriptor.fault_type) {
                self.storage_injector.heal(fault_id).await
            } else {
                Err(ChaosError::FaultNotFound {
                    fault_id: fault_id.to_string(),
                })
            };

            match result {
                Ok(()) => {
                    state.complete();

                    // Report if we have a reporter
                    if let Some(reporter) = &self.reporter {
                        reporter.record_fault_healed(fault_id);
                    }

                    info!("Fault healed successfully");
                    Ok(())
                }
                Err(e) => {
                    warn!(error = %e, "Failed to heal fault");
                    Err(e)
                }
            }
        } else {
            Err(ChaosError::FaultNotFound {
                fault_id: fault_id.to_string(),
            })
        }
    }

    /// Cancel a scheduled fault.
    pub fn cancel_fault(&self, fault_id: &FaultId) -> ChaosResult<()> {
        // Remove from scheduled if present
        if self.scheduled_faults.remove(fault_id).is_some() {
            info!(fault_id = %fault_id, "Cancelled scheduled fault");
            return Ok(());
        }

        // Update state if present
        if let Some(mut state) = self.fault_states.get_mut(fault_id) {
            state.cancel();
            info!(fault_id = %fault_id, "Cancelled fault");
            return Ok(());
        }

        Err(ChaosError::FaultNotFound {
            fault_id: fault_id.to_string(),
        })
    }

    /// Heal all active faults.
    #[instrument(skip(self))]
    pub async fn heal_all(&self) -> ChaosResult<()> {
        info!("Healing all active faults");

        let fault_ids: Vec<FaultId> = self
            .fault_states
            .iter()
            .filter(|e| e.status == FaultStatus::Active)
            .map(|e| e.descriptor.id.clone())
            .collect();

        for fault_id in fault_ids {
            if let Err(e) = self.heal_fault(&fault_id).await {
                warn!(fault_id = %fault_id, error = %e, "Failed to heal fault");
            }
        }

        Ok(())
    }

    /// Get fault state by ID.
    pub fn get_fault_state(&self, fault_id: &FaultId) -> Option<FaultState> {
        self.fault_states.get(fault_id).map(|r| r.clone())
    }

    /// Get all fault states.
    pub fn get_all_fault_states(&self) -> Vec<FaultState> {
        self.fault_states.iter().map(|r| r.clone()).collect()
    }

    /// Get active faults.
    pub fn get_active_faults(&self) -> Vec<FaultState> {
        self.fault_states
            .iter()
            .filter(|r| r.status == FaultStatus::Active)
            .map(|r| r.clone())
            .collect()
    }

    /// Get count of active faults.
    pub fn active_fault_count(&self) -> usize {
        self.fault_states
            .iter()
            .filter(|r| r.status == FaultStatus::Active)
            .count()
    }

    /// Get total fault count.
    pub fn total_fault_count(&self) -> u64 {
        self.fault_counter.load(Ordering::Relaxed)
    }

    /// Process scheduled faults.
    async fn process_scheduled_faults(
        scheduled: &DashMap<FaultId, FaultDescriptor>,
        fault_states: &DashMap<FaultId, FaultState>,
        network: &Arc<NetworkFaultInjector>,
        process: &Arc<ProcessFaultInjector>,
        storage: &Arc<StorageFaultInjector>,
        reporter: &Option<Arc<ChaosReporter>>,
    ) {
        let now = Utc::now();
        let mut to_inject = Vec::new();

        // Find faults ready to inject
        for entry in scheduled.iter() {
            let descriptor = entry.value();
            let should_inject = match &descriptor.schedule.start_time {
                ScheduleTime::Immediate => true,
                ScheduleTime::Delay(delay) => {
                    let state = fault_states.get(&descriptor.id);
                    if let Some(state) = state {
                        let elapsed = (now - state.created_at)
                            .to_std()
                            .unwrap_or(Duration::from_secs(0));
                        elapsed >= *delay
                    } else {
                        false
                    }
                }
                ScheduleTime::At(time) => now >= *time,
            };

            if should_inject {
                to_inject.push(descriptor.clone());
            }
        }

        // Inject ready faults
        for descriptor in to_inject {
            scheduled.remove(&descriptor.id);

            let mut state = FaultState::new(descriptor.clone());

            let result = if network.can_handle(&descriptor.fault_type) {
                network.inject(&descriptor).await
            } else if process.can_handle(&descriptor.fault_type) {
                process.inject(&descriptor).await
            } else if storage.can_handle(&descriptor.fault_type) {
                storage.inject(&descriptor).await
            } else {
                continue;
            };

            match result {
                Ok(()) => {
                    state.activate();
                    fault_states.insert(descriptor.id.clone(), state.clone());
                    if let Some(r) = reporter {
                        r.record_fault_injected(&state);
                    }
                }
                Err(e) => {
                    state.fail(e.to_string());
                    fault_states.insert(descriptor.id.clone(), state);
                    if let Some(r) = reporter {
                        r.record_fault_failed(&descriptor.id, &e.to_string());
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::injector::{FaultParameters, FaultTarget};

    fn create_test_controller() -> ChaosController {
        ChaosController::for_testing()
    }

    #[test]
    fn test_config_defaults() {
        let config = ChaosControllerConfig::default();
        assert_eq!(config.max_concurrent_faults, 10);
        assert_eq!(config.fault_timeout, Duration::from_secs(300));
        assert!(config.enable_scheduling);
    }

    #[test]
    fn test_config_builder() {
        let config = ChaosControllerConfig::new()
            .with_max_concurrent_faults(5)
            .with_fault_timeout(Duration::from_secs(60));

        assert_eq!(config.max_concurrent_faults, 5);
        assert_eq!(config.fault_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_controller_creation() {
        let controller = create_test_controller();
        assert_eq!(controller.status(), ControllerStatus::Stopped);
        assert!(!controller.is_running());
    }

    #[tokio::test]
    async fn test_controller_start_stop() {
        let controller = create_test_controller();

        controller.start().await.unwrap();
        assert_eq!(controller.status(), ControllerStatus::Running);
        assert!(controller.is_running());

        controller.stop().await.unwrap();
        assert_eq!(controller.status(), ControllerStatus::Stopped);
        assert!(!controller.is_running());
    }

    #[tokio::test]
    async fn test_controller_double_start() {
        let controller = create_test_controller();

        controller.start().await.unwrap();
        let result = controller.start().await;
        assert!(result.is_err());

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_schedule_fault_not_started() {
        let controller = create_test_controller();
        let descriptor = FaultDescriptor::new(
            "test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let result = controller.schedule_fault(descriptor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_inject_network_fault() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "delay-test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new().with_delay(Duration::from_millis(50)),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        assert_eq!(controller.active_fault_count(), 1);

        let state = controller.get_fault_state(&fault_id).unwrap();
        assert_eq!(state.status, FaultStatus::Active);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_inject_process_fault() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "pause-test",
            FaultType::ProcessPause,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        assert_eq!(controller.active_fault_count(), 1);

        let state = controller.get_fault_state(&fault_id).unwrap();
        assert_eq!(state.status, FaultStatus::Active);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_inject_storage_fault() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "read-failure-test",
            FaultType::StorageReadFailure,
            FaultTarget::GrainStorage("grain-123".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        assert_eq!(controller.active_fault_count(), 1);

        let state = controller.get_fault_state(&fault_id).unwrap();
        assert_eq!(state.status, FaultStatus::Active);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_heal_fault() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "heal-test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        assert_eq!(controller.active_fault_count(), 1);

        controller.heal_fault(&fault_id).await.unwrap();
        assert_eq!(controller.active_fault_count(), 0);

        let state = controller.get_fault_state(&fault_id).unwrap();
        assert_eq!(state.status, FaultStatus::Completed);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_heal_all() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        // Inject multiple faults
        for i in 0..3 {
            let descriptor = FaultDescriptor::new(
                format!("fault-{}", i),
                FaultType::NetworkDelay,
                FaultTarget::AllSilos,
                FaultSchedule::immediate(None),
                FaultParameters::new(),
            );
            controller.schedule_fault(descriptor).await.unwrap();
        }

        assert_eq!(controller.active_fault_count(), 3);

        controller.heal_all().await.unwrap();
        assert_eq!(controller.active_fault_count(), 0);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_cancel_fault() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "cancel-test",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        controller.cancel_fault(&fault_id).unwrap();

        let state = controller.get_fault_state(&fault_id).unwrap();
        assert_eq!(state.status, FaultStatus::Cancelled);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_max_concurrent_faults() {
        let mut config = ChaosControllerConfig::for_testing();
        config.max_concurrent_faults = 2;
        let controller = ChaosController::new(config);
        controller.start().await.unwrap();

        // Inject 2 faults (should succeed)
        for i in 0..2 {
            let descriptor = FaultDescriptor::new(
                format!("fault-{}", i),
                FaultType::NetworkDelay,
                FaultTarget::AllSilos,
                FaultSchedule::immediate(None),
                FaultParameters::new(),
            );
            controller.schedule_fault(descriptor).await.unwrap();
        }

        // Third fault should fail
        let descriptor = FaultDescriptor::new(
            "fault-3",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let result = controller.schedule_fault(descriptor).await;
        assert!(result.is_err());

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_all_fault_states() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        for i in 0..3 {
            let descriptor = FaultDescriptor::new(
                format!("fault-{}", i),
                FaultType::NetworkDelay,
                FaultTarget::AllSilos,
                FaultSchedule::immediate(None),
                FaultParameters::new(),
            );
            controller.schedule_fault(descriptor).await.unwrap();
        }

        let states = controller.get_all_fault_states();
        assert_eq!(states.len(), 3);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_active_faults() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        let d1 = FaultDescriptor::new(
            "fault-1",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let d2 = FaultDescriptor::new(
            "fault-2",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let id1 = controller.schedule_fault(d1).await.unwrap();
        controller.schedule_fault(d2).await.unwrap();

        controller.heal_fault(&id1).await.unwrap();

        let active = controller.get_active_faults();
        assert_eq!(active.len(), 1);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_total_fault_count() {
        let controller = create_test_controller();
        controller.start().await.unwrap();

        for i in 0..5 {
            let descriptor = FaultDescriptor::new(
                format!("fault-{}", i),
                FaultType::NetworkDelay,
                FaultTarget::AllSilos,
                FaultSchedule::immediate(None),
                FaultParameters::new(),
            );
            controller.schedule_fault(descriptor).await.unwrap();
        }

        assert_eq!(controller.total_fault_count(), 5);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_controller_with_reporter() {
        let reporter = Arc::new(ChaosReporter::new("test-chaos"));
        let controller = ChaosController::for_testing().with_reporter(reporter.clone());
        controller.start().await.unwrap();

        let descriptor = FaultDescriptor::new(
            "reported-fault",
            FaultType::NetworkDelay,
            FaultTarget::AllSilos,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let fault_id = controller.schedule_fault(descriptor).await.unwrap();
        controller.heal_fault(&fault_id).await.unwrap();

        let summary = reporter.get_summary();
        assert_eq!(summary.total_faults, 1);

        controller.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_injectors_accessible() {
        let controller = create_test_controller();

        // Register a silo mapping through the process injector
        controller
            .process_injector()
            .register_silo("silo-1", 1234);

        assert_eq!(
            controller
                .process_injector()
                .get_pid_for_silo("silo-1"),
            Some(1234)
        );
    }
}
