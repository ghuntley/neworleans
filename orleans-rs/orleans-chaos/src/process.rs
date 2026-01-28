//! Process fault injection.
//!
//! This module provides fault injection capabilities for process-related issues
//! such as process kill, pause, memory pressure, and CPU throttling.

use std::collections::HashMap;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{debug, info, instrument, warn};

use crate::error::{ChaosError, ChaosResult};
use crate::injector::{
    FaultDescriptor, FaultId, FaultInjector, FaultParameters, FaultTarget, FaultType,
};

/// Configuration for process fault injection.
#[derive(Debug, Clone)]
pub struct ProcessFaultConfig {
    /// Whether to use real process signals (requires appropriate permissions).
    pub use_real_signals: bool,
    /// Timeout for waiting on process operations.
    pub operation_timeout: Duration,
    /// Default memory pressure bytes.
    pub default_memory_bytes: u64,
    /// Default CPU throttle percentage.
    pub default_cpu_percent: f64,
}

impl Default for ProcessFaultConfig {
    fn default() -> Self {
        Self {
            use_real_signals: false,
            operation_timeout: Duration::from_secs(5),
            default_memory_bytes: 100 * 1024 * 1024, // 100 MB
            default_cpu_percent: 0.5,                // 50%
        }
    }
}

impl ProcessFaultConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable real process signals.
    pub fn with_real_signals(mut self) -> Self {
        self.use_real_signals = true;
        self
    }

    /// Set operation timeout.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// Set default memory bytes.
    pub fn with_default_memory_bytes(mut self, bytes: u64) -> Self {
        self.default_memory_bytes = bytes;
        self
    }

    /// Set default CPU throttle percentage.
    pub fn with_default_cpu_percent(mut self, percent: f64) -> Self {
        self.default_cpu_percent = percent;
        self
    }

    /// Create config for testing.
    pub fn for_testing() -> Self {
        Self {
            use_real_signals: false,
            operation_timeout: Duration::from_secs(2),
            default_memory_bytes: 10 * 1024 * 1024, // 10 MB
            default_cpu_percent: 0.25,
        }
    }
}

/// State of an active process fault.
#[derive(Debug, Clone)]
pub struct ProcessFaultState {
    /// The fault ID.
    pub fault_id: FaultId,
    /// The fault type.
    pub fault_type: FaultType,
    /// Target process ID.
    pub target_pid: Option<u32>,
    /// Target silo address.
    pub target_silo: Option<String>,
    /// Parameters.
    pub parameters: FaultParameters,
    /// Whether the fault is currently active.
    pub is_active: bool,
    /// Original process state (for restoration).
    pub original_state: ProcessState,
}

/// Original state of a process for restoration.
#[derive(Debug, Clone, Default)]
pub struct ProcessState {
    /// Whether the process was running.
    pub was_running: bool,
    /// Memory allocation handles (for cleanup).
    pub memory_handles: Vec<String>,
}

/// Process fault injector.
///
/// This injector handles process-related faults such as kill, pause,
/// memory pressure, and CPU throttling.
#[derive(Debug)]
pub struct ProcessFaultInjector {
    /// Configuration.
    config: ProcessFaultConfig,
    /// Active faults.
    active_faults: DashMap<FaultId, ProcessFaultState>,
    /// Mapping of silo addresses to process IDs.
    silo_to_pid: RwLock<HashMap<String, u32>>,
    /// Paused processes (for resumption).
    paused_processes: DashMap<u32, bool>,
}

impl ProcessFaultInjector {
    /// Create a new process fault injector.
    pub fn new(config: ProcessFaultConfig) -> Self {
        info!(
            use_real_signals = config.use_real_signals,
            "Creating process fault injector"
        );
        Self {
            config,
            active_faults: DashMap::new(),
            silo_to_pid: RwLock::new(HashMap::new()),
            paused_processes: DashMap::new(),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(ProcessFaultConfig::default())
    }

    /// Register a silo address to process ID mapping.
    pub fn register_silo(&self, silo_addr: &str, pid: u32) {
        let mut mapping = self.silo_to_pid.write();
        mapping.insert(silo_addr.to_string(), pid);
        debug!(silo = silo_addr, pid = pid, "Registered silo-to-PID mapping");
    }

    /// Unregister a silo.
    pub fn unregister_silo(&self, silo_addr: &str) {
        let mut mapping = self.silo_to_pid.write();
        mapping.remove(silo_addr);
    }

    /// Get process ID for a silo.
    pub fn get_pid_for_silo(&self, silo_addr: &str) -> Option<u32> {
        self.silo_to_pid.read().get(silo_addr).copied()
    }

    /// Check if a process is paused.
    pub fn is_paused(&self, pid: u32) -> bool {
        self.paused_processes.get(&pid).map(|v| *v).unwrap_or(false)
    }

    /// Get the target process ID from a fault target.
    fn resolve_target_pid(&self, target: &FaultTarget) -> ChaosResult<u32> {
        match target {
            FaultTarget::Process(pid) => Ok(*pid),
            FaultTarget::Silo(addr) => self.get_pid_for_silo(addr).ok_or_else(|| {
                ChaosError::target_not_found(format!("No PID registered for silo {}", addr))
            }),
            FaultTarget::RandomSilo => {
                let mapping = self.silo_to_pid.read();
                let pids: Vec<_> = mapping.values().collect();
                if pids.is_empty() {
                    return Err(ChaosError::target_not_found("No silos registered"));
                }
                use rand::Rng;
                let idx = rand::thread_rng().gen_range(0..pids.len());
                Ok(*pids[idx])
            }
            _ => Err(ChaosError::invalid_schedule(
                "Process faults require Process, Silo, or RandomSilo target",
            )),
        }
    }

    /// Inject a process kill fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_kill(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let pid = self.resolve_target_pid(&descriptor.target)?;

        info!(pid = pid, "Injecting process kill fault");

        if self.config.use_real_signals {
            self.send_signal(pid, Signal::Kill).await?;
        } else {
            debug!(pid = pid, "Simulating process kill (real signals disabled)");
        }

        let state = ProcessFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::ProcessKill,
            target_pid: Some(pid),
            target_silo: self.get_silo_for_pid(pid),
            parameters: descriptor.parameters.clone(),
            is_active: true,
            original_state: ProcessState {
                was_running: true,
                ..Default::default()
            },
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a process pause fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_pause(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let pid = self.resolve_target_pid(&descriptor.target)?;

        info!(pid = pid, "Injecting process pause fault");

        if self.config.use_real_signals {
            self.send_signal(pid, Signal::Stop).await?;
        } else {
            debug!(pid = pid, "Simulating process pause (real signals disabled)");
        }

        self.paused_processes.insert(pid, true);

        let state = ProcessFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::ProcessPause,
            target_pid: Some(pid),
            target_silo: self.get_silo_for_pid(pid),
            parameters: descriptor.parameters.clone(),
            is_active: true,
            original_state: ProcessState {
                was_running: true,
                ..Default::default()
            },
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a process resume (undo pause).
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_resume(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let pid = self.resolve_target_pid(&descriptor.target)?;

        info!(pid = pid, "Resuming paused process");

        if self.config.use_real_signals {
            self.send_signal(pid, Signal::Continue).await?;
        } else {
            debug!(pid = pid, "Simulating process resume (real signals disabled)");
        }

        self.paused_processes.remove(&pid);

        let state = ProcessFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::ProcessResume,
            target_pid: Some(pid),
            target_silo: self.get_silo_for_pid(pid),
            parameters: descriptor.parameters.clone(),
            is_active: false, // Resume is instant, not ongoing
            original_state: ProcessState::default(),
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a memory pressure fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_memory_pressure(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let memory_bytes = descriptor
            .parameters
            .memory_bytes
            .unwrap_or(self.config.default_memory_bytes);

        info!(
            target = %descriptor.target,
            memory_bytes = memory_bytes,
            "Injecting memory pressure fault"
        );

        // In simulation mode, we just track the fault
        // Real implementation would allocate memory in the target process
        if self.config.use_real_signals {
            warn!("Real memory pressure injection not implemented");
        }

        let state = ProcessFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::MemoryPressure,
            target_pid: self.resolve_target_pid(&descriptor.target).ok(),
            target_silo: match &descriptor.target {
                FaultTarget::Silo(addr) => Some(addr.clone()),
                _ => None,
            },
            parameters: descriptor.parameters.clone(),
            is_active: true,
            original_state: ProcessState::default(),
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Inject a CPU throttle fault.
    #[instrument(skip(self), fields(fault_id = %descriptor.id))]
    async fn inject_cpu_throttle(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        let cpu_percent = descriptor
            .parameters
            .cpu_percent
            .unwrap_or(self.config.default_cpu_percent);

        info!(
            target = %descriptor.target,
            cpu_percent = cpu_percent,
            "Injecting CPU throttle fault"
        );

        // In simulation mode, we just track the fault
        // Real implementation would use cgroups or similar
        if self.config.use_real_signals {
            warn!("Real CPU throttling not implemented");
        }

        let state = ProcessFaultState {
            fault_id: descriptor.id.clone(),
            fault_type: FaultType::CpuThrottle,
            target_pid: self.resolve_target_pid(&descriptor.target).ok(),
            target_silo: match &descriptor.target {
                FaultTarget::Silo(addr) => Some(addr.clone()),
                _ => None,
            },
            parameters: descriptor.parameters.clone(),
            is_active: true,
            original_state: ProcessState::default(),
        };

        self.active_faults.insert(descriptor.id.clone(), state);
        Ok(())
    }

    /// Get silo address for a PID.
    fn get_silo_for_pid(&self, pid: u32) -> Option<String> {
        let mapping = self.silo_to_pid.read();
        mapping
            .iter()
            .find(|(_, &p)| p == pid)
            .map(|(addr, _)| addr.clone())
    }

    /// Send a signal to a process.
    async fn send_signal(&self, pid: u32, signal: Signal) -> ChaosResult<()> {
        #[cfg(unix)]
        {
            use std::process::Command;
            let signal_name = match signal {
                Signal::Kill => "KILL",
                Signal::Stop => "STOP",
                Signal::Continue => "CONT",
            };

            let output = Command::new("kill")
                .arg(format!("-{}", signal_name))
                .arg(pid.to_string())
                .output()
                .map_err(|e| ChaosError::process_error(format!("Failed to send signal: {}", e)))?;

            if !output.status.success() {
                return Err(ChaosError::process_error(format!(
                    "Signal failed: {}",
                    String::from_utf8_lossy(&output.stderr)
                )));
            }
        }

        #[cfg(not(unix))]
        {
            let _ = (pid, signal);
            warn!("Process signals not supported on this platform");
        }

        Ok(())
    }
}

/// Unix signals for process control.
#[derive(Debug, Clone, Copy)]
enum Signal {
    Kill,
    Stop,
    Continue,
}

#[async_trait]
impl FaultInjector for ProcessFaultInjector {
    fn name(&self) -> &str {
        "ProcessFaultInjector"
    }

    fn supported_fault_types(&self) -> Vec<FaultType> {
        vec![
            FaultType::ProcessKill,
            FaultType::ProcessPause,
            FaultType::ProcessResume,
            FaultType::MemoryPressure,
            FaultType::CpuThrottle,
        ]
    }

    #[instrument(skip(self), fields(fault_id = %descriptor.id, fault_type = %descriptor.fault_type))]
    async fn inject(&self, descriptor: &FaultDescriptor) -> ChaosResult<()> {
        debug!("Injecting process fault");

        match descriptor.fault_type {
            FaultType::ProcessKill => self.inject_kill(descriptor).await,
            FaultType::ProcessPause => self.inject_pause(descriptor).await,
            FaultType::ProcessResume => self.inject_resume(descriptor).await,
            FaultType::MemoryPressure => self.inject_memory_pressure(descriptor).await,
            FaultType::CpuThrottle => self.inject_cpu_throttle(descriptor).await,
            _ => Err(ChaosError::injection_failed(
                format!("Unsupported fault type: {}", descriptor.fault_type),
                descriptor.fault_type.to_string(),
            )),
        }
    }

    #[instrument(skip(self), fields(fault_id = %fault_id))]
    async fn heal(&self, fault_id: &FaultId) -> ChaosResult<()> {
        if let Some(mut state) = self.active_faults.get_mut(fault_id) {
            info!(fault_type = %state.fault_type, "Healing process fault");

            // For pause faults, send SIGCONT
            if state.fault_type == FaultType::ProcessPause {
                if let Some(pid) = state.target_pid {
                    if self.config.use_real_signals {
                        self.send_signal(pid, Signal::Continue).await?;
                    }
                    self.paused_processes.remove(&pid);
                }
            }

            state.is_active = false;
            Ok(())
        } else {
            warn!("Attempted to heal non-existent fault");
            Err(ChaosError::FaultNotFound {
                fault_id: fault_id.to_string(),
            })
        }
    }

    async fn is_active(&self, fault_id: &FaultId) -> bool {
        self.active_faults
            .get(fault_id)
            .map(|s| s.is_active)
            .unwrap_or(false)
    }

    async fn active_faults(&self) -> Vec<FaultId> {
        self.active_faults
            .iter()
            .filter(|e| e.is_active)
            .map(|e| e.fault_id.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::injector::FaultSchedule;

    fn create_test_injector() -> ProcessFaultInjector {
        ProcessFaultInjector::new(ProcessFaultConfig::for_testing())
    }

    #[test]
    fn test_config_defaults() {
        let config = ProcessFaultConfig::default();
        assert!(!config.use_real_signals);
        assert_eq!(config.operation_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_config_builder() {
        let config = ProcessFaultConfig::new()
            .with_real_signals()
            .with_operation_timeout(Duration::from_secs(10))
            .with_default_memory_bytes(500 * 1024 * 1024)
            .with_default_cpu_percent(0.75);

        assert!(config.use_real_signals);
        assert_eq!(config.operation_timeout, Duration::from_secs(10));
        assert_eq!(config.default_memory_bytes, 500 * 1024 * 1024);
        assert_eq!(config.default_cpu_percent, 0.75);
    }

    #[test]
    fn test_supported_fault_types() {
        let injector = create_test_injector();
        let types = injector.supported_fault_types();
        assert!(types.contains(&FaultType::ProcessKill));
        assert!(types.contains(&FaultType::ProcessPause));
        assert!(types.contains(&FaultType::ProcessResume));
        assert!(types.contains(&FaultType::MemoryPressure));
        assert!(types.contains(&FaultType::CpuThrottle));
    }

    #[test]
    fn test_can_handle() {
        let injector = create_test_injector();
        assert!(injector.can_handle(&FaultType::ProcessKill));
        assert!(injector.can_handle(&FaultType::ProcessPause));
        assert!(!injector.can_handle(&FaultType::NetworkDelay));
    }

    #[test]
    fn test_register_silo() {
        let injector = create_test_injector();
        injector.register_silo("127.0.0.1:8080", 1234);
        assert_eq!(injector.get_pid_for_silo("127.0.0.1:8080"), Some(1234));
    }

    #[test]
    fn test_unregister_silo() {
        let injector = create_test_injector();
        injector.register_silo("127.0.0.1:8080", 1234);
        injector.unregister_silo("127.0.0.1:8080");
        assert_eq!(injector.get_pid_for_silo("127.0.0.1:8080"), None);
    }

    #[tokio::test]
    async fn test_inject_kill_by_pid() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "kill-test",
            FaultType::ProcessKill,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_inject_kill_by_silo() {
        let injector = create_test_injector();
        injector.register_silo("silo1", 1234);

        let descriptor = FaultDescriptor::new(
            "kill-silo-test",
            FaultType::ProcessKill,
            FaultTarget::Silo("silo1".to_string()),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_inject_pause() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "pause-test",
            FaultType::ProcessPause,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
        assert!(injector.is_paused(9999));
    }

    #[tokio::test]
    async fn test_inject_resume() {
        let injector = create_test_injector();

        // First pause
        let pause_desc = FaultDescriptor::new(
            "pause-test",
            FaultType::ProcessPause,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        injector.inject(&pause_desc).await.unwrap();
        assert!(injector.is_paused(9999));

        // Then resume
        let resume_desc = FaultDescriptor::new(
            "resume-test",
            FaultType::ProcessResume,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        injector.inject(&resume_desc).await.unwrap();
        assert!(!injector.is_paused(9999));
    }

    #[tokio::test]
    async fn test_inject_memory_pressure() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "memory-test",
            FaultType::MemoryPressure,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new().with_memory_bytes(50 * 1024 * 1024),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_inject_cpu_throttle() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "cpu-test",
            FaultType::CpuThrottle,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new().with_cpu_percent(0.3),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_heal_pause() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "pause-heal-test",
            FaultType::ProcessPause,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_paused(9999));

        injector.heal(&descriptor.id).await.unwrap();
        assert!(!injector.is_active(&descriptor.id).await);
        assert!(!injector.is_paused(9999));
    }

    #[tokio::test]
    async fn test_heal_nonexistent_fault() {
        let injector = create_test_injector();
        let result = injector.heal(&FaultId::from_str("nonexistent")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_active_faults() {
        let injector = create_test_injector();

        let d1 = FaultDescriptor::new(
            "fault1",
            FaultType::ProcessPause,
            FaultTarget::Process(1111),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );
        let d2 = FaultDescriptor::new(
            "fault2",
            FaultType::MemoryPressure,
            FaultTarget::Process(2222),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&d1).await.unwrap();
        injector.inject(&d2).await.unwrap();

        let active = injector.active_faults().await;
        assert_eq!(active.len(), 2);

        injector.heal(&d1.id).await.unwrap();
        let active = injector.active_faults().await;
        assert_eq!(active.len(), 1);
    }

    #[tokio::test]
    async fn test_inject_random_silo() {
        let injector = create_test_injector();
        injector.register_silo("silo1", 1111);
        injector.register_silo("silo2", 2222);

        let descriptor = FaultDescriptor::new(
            "random-test",
            FaultType::ProcessKill,
            FaultTarget::RandomSilo,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        injector.inject(&descriptor).await.unwrap();
        assert!(injector.is_active(&descriptor.id).await);
    }

    #[tokio::test]
    async fn test_inject_random_silo_no_silos() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "random-test",
            FaultType::ProcessKill,
            FaultTarget::RandomSilo,
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let result = injector.inject(&descriptor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_unsupported_fault_type() {
        let injector = create_test_injector();
        let descriptor = FaultDescriptor::new(
            "unsupported",
            FaultType::NetworkDelay,
            FaultTarget::Process(9999),
            FaultSchedule::immediate(None),
            FaultParameters::new(),
        );

        let result = injector.inject(&descriptor).await;
        assert!(result.is_err());
    }
}
