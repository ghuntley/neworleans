//! Process-based silo management for integration testing.
//!
//! This module provides utilities for spawning and managing Orleans silo
//! processes for real network integration testing.

use crate::error::{TestError, TestResult};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

/// Configuration for spawning a silo process.
#[derive(Debug, Clone)]
pub struct SiloProcessConfig {
    /// Port to listen on (0 for auto-assign)
    pub port: u16,
    /// Membership server address
    pub membership_server: String,
    /// Path to silo binary (defaults to cargo build output)
    pub binary_path: Option<PathBuf>,
    /// Additional environment variables
    pub env_vars: Vec<(String, String)>,
    /// Enable test mode (auto-shutdown after idle)
    pub test_mode: bool,
    /// Grain key to create on startup
    pub create_grain: Option<String>,
    /// Grain key to invoke on startup
    pub test_grain: Option<String>,
    /// Wait for N silos before starting
    pub wait_for_cluster: Option<u32>,
    /// Startup timeout
    pub startup_timeout: Duration,
    /// Capture stdout/stderr
    pub capture_output: bool,
}

impl Default for SiloProcessConfig {
    fn default() -> Self {
        Self {
            port: 0,
            membership_server: "127.0.0.1:5000".into(),
            binary_path: None,
            env_vars: vec![],
            test_mode: false,
            create_grain: None,
            test_grain: None,
            wait_for_cluster: None,
            startup_timeout: Duration::from_secs(30),
            capture_output: true,
        }
    }
}

impl SiloProcessConfig {
    /// Create a new config with the specified membership server.
    pub fn new(membership_server: impl Into<String>) -> Self {
        Self {
            membership_server: membership_server.into(),
            ..Default::default()
        }
    }

    /// Set the port to listen on.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Enable test mode.
    pub fn with_test_mode(mut self) -> Self {
        self.test_mode = true;
        self
    }

    /// Create a grain on startup.
    pub fn with_create_grain(mut self, key: impl Into<String>) -> Self {
        self.create_grain = Some(key.into());
        self
    }

    /// Invoke a test grain on startup.
    pub fn with_test_grain(mut self, key: impl Into<String>) -> Self {
        self.test_grain = Some(key.into());
        self
    }

    /// Wait for N silos before starting.
    pub fn with_wait_for_cluster(mut self, count: u32) -> Self {
        self.wait_for_cluster = Some(count);
        self
    }

    /// Set the startup timeout.
    pub fn with_startup_timeout(mut self, timeout: Duration) -> Self {
        self.startup_timeout = timeout;
        self
    }
}

/// Event from a silo process.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum ProcessEvent {
    /// Silo started successfully
    SiloStarted {
        /// Silo address (ip:port)
        address: String,
    },
    /// Silo joined the cluster
    SiloJoined {
        /// Silo address
        address: String,
        /// Cluster ID
        cluster_id: String,
    },
    /// Grain was created
    GrainCreated {
        /// Grain ID
        grain_id: String,
        /// Silo that hosts the grain
        silo: String,
        /// Activation ID
        activation_id: String,
    },
    /// Grain method was invoked
    GrainInvoked {
        /// Whether the invocation succeeded
        success: bool,
        /// Method name
        method: String,
        /// Result value (if applicable)
        result: Option<serde_json::Value>,
        /// Error message (if failed)
        error: Option<String>,
    },
    /// Silo is stopping
    SiloStopping {
        /// Silo address
        address: String,
    },
    /// Silo has stopped
    SiloStopped {
        /// Silo address
        address: String,
    },
    /// Error occurred
    Error {
        /// Error message
        message: String,
    },
    /// Raw log line (not JSON)
    Log {
        /// Log level
        level: String,
        /// Log message
        message: String,
    },
}

/// Captured output from a silo process.
#[derive(Debug, Default)]
pub struct SiloOutput {
    /// Events parsed from stdout
    events: VecDeque<ProcessEvent>,
    /// Raw stdout lines
    stdout_lines: VecDeque<String>,
    /// Raw stderr lines
    stderr_lines: VecDeque<String>,
}

impl SiloOutput {
    /// Get the next event.
    pub fn next_event(&mut self) -> Option<ProcessEvent> {
        self.events.pop_front()
    }

    /// Get all pending events.
    pub fn drain_events(&mut self) -> Vec<ProcessEvent> {
        self.events.drain(..).collect()
    }

    /// Get all stdout lines.
    pub fn stdout_lines(&self) -> &VecDeque<String> {
        &self.stdout_lines
    }

    /// Get all stderr lines.
    pub fn stderr_lines(&self) -> &VecDeque<String> {
        &self.stderr_lines
    }

    /// Add an event.
    fn add_event(&mut self, event: ProcessEvent) {
        self.events.push_back(event);
    }

    /// Add a stdout line.
    fn add_stdout(&mut self, line: String) {
        self.stdout_lines.push_back(line);
    }

    /// Add a stderr line.
    fn add_stderr(&mut self, line: String) {
        self.stderr_lines.push_back(line);
    }
}

/// A managed silo process.
pub struct SiloProcess {
    /// Process handle
    child: Child,
    /// Configuration used to start this process
    config: SiloProcessConfig,
    /// Captured output
    output: Arc<Mutex<SiloOutput>>,
    /// Channel to signal shutdown
    shutdown_tx: mpsc::Sender<()>,
    /// Process ID
    pid: u32,
    /// Assigned port (if auto-assigned)
    port: Option<u16>,
    /// Silo address (once started)
    silo_address: Option<String>,
}

impl SiloProcess {
    /// Spawn a new silo process.
    pub async fn spawn(config: SiloProcessConfig) -> TestResult<Self> {
        let binary_path = config.binary_path.clone().unwrap_or_else(|| {
            // Use cargo's built binary - try environment variable first, then fallback
            std::env::var("CARGO_BIN_EXE_orleans-silo")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("target/debug/orleans-silo"))
        });

        info!(
            binary = %binary_path.display(),
            port = config.port,
            membership_server = %config.membership_server,
            "Spawning silo process"
        );

        let mut cmd = Command::new(&binary_path);

        // Add arguments
        cmd.arg("--port").arg(config.port.to_string());
        cmd.arg("--membership-server").arg(&config.membership_server);

        if config.test_mode {
            cmd.arg("--test");
        }

        if let Some(ref key) = config.create_grain {
            cmd.arg("--create-grain").arg(key);
        }

        if let Some(ref key) = config.test_grain {
            cmd.arg("--test-grain").arg(key);
        }

        if let Some(count) = config.wait_for_cluster {
            cmd.arg("--wait-for-cluster").arg(count.to_string());
        }

        // Set environment
        for (key, value) in &config.env_vars {
            cmd.env(key, value);
        }

        // Configure output capture
        if config.capture_output {
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
        }

        // Spawn the process
        let mut child = cmd.spawn().map_err(TestError::ProcessStart)?;
        let pid = child.id().unwrap_or(0);

        info!(pid = pid, "Silo process spawned");

        let output = Arc::new(Mutex::new(SiloOutput::default()));
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);

        // Spawn output readers
        if config.capture_output {
            let stdout = child.stdout.take().expect("stdout should be captured");
            let stderr = child.stderr.take().expect("stderr should be captured");

            // Stdout reader task
            let output_clone = Arc::clone(&output);
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    let mut output = output_clone.lock();

                    // Try to parse as JSON event
                    if let Ok(event) = serde_json::from_str::<ProcessEvent>(&line) {
                        debug!(event = ?event, "Parsed process event");
                        output.add_event(event);
                    } else {
                        // Store as raw log line
                        output.add_stdout(line.clone());
                        output.add_event(ProcessEvent::Log {
                            level: "info".into(),
                            message: line,
                        });
                    }
                }
            });

            // Stderr reader task
            let output_clone = Arc::clone(&output);
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();

                while let Ok(Some(line)) = lines.next_line().await {
                    let mut output = output_clone.lock();
                    output.add_stderr(line.clone());
                    output.add_event(ProcessEvent::Log {
                        level: "error".into(),
                        message: line,
                    });
                }
            });
        }

        // Spawn shutdown handler
        let pid_clone = pid;
        tokio::spawn(async move {
            let _ = shutdown_rx.recv().await;
            debug!(pid = pid_clone, "Shutdown signal received");
        });

        Ok(Self {
            child,
            config,
            output,
            shutdown_tx,
            pid,
            port: None,
            silo_address: None,
        })
    }

    /// Get the process ID.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Get the configuration.
    pub fn config(&self) -> &SiloProcessConfig {
        &self.config
    }

    /// Get the silo address (once started).
    pub fn silo_address(&self) -> Option<&str> {
        self.silo_address.as_deref()
    }

    /// Get the assigned port.
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// Check if the process is still running.
    pub fn is_running(&mut self) -> bool {
        match self.child.try_wait() {
            Ok(None) => true,
            Ok(Some(_)) => false,
            Err(_) => false,
        }
    }

    /// Wait for the silo to start and join the cluster.
    pub async fn wait_for_startup(&mut self) -> TestResult<()> {
        let timeout = self.config.startup_timeout;
        let start = std::time::Instant::now();

        info!(pid = self.pid, timeout_secs = timeout.as_secs(), "Waiting for silo startup");

        while start.elapsed() < timeout {
            // Check for events
            {
                let mut output = self.output.lock();
                while let Some(event) = output.next_event() {
                    match event {
                        ProcessEvent::SiloStarted { ref address } => {
                            info!(pid = self.pid, address = %address, "Silo started");
                            self.silo_address = Some(address.clone());
                            if let Some(port_str) = address.rsplit(':').next() {
                                if let Ok(port) = port_str.parse() {
                                    self.port = Some(port);
                                }
                            }
                        }
                        ProcessEvent::SiloJoined { ref address, .. } => {
                            info!(pid = self.pid, address = %address, "Silo joined cluster");
                            return Ok(());
                        }
                        ProcessEvent::Error { ref message } => {
                            error!(pid = self.pid, error = %message, "Silo error");
                            return Err(TestError::ClusterFormation(message.clone()));
                        }
                        _ => {}
                    }
                }
            }

            // Check if process is still running
            if !self.is_running() {
                let exit_status = self.child.try_wait().ok().flatten();
                return Err(TestError::ProcessCrashed(format!(
                    "Silo process exited unexpectedly: {:?}",
                    exit_status
                )));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        Err(TestError::timeout("silo startup", timeout.as_secs()))
    }

    /// Wait for a specific event.
    pub async fn wait_for_event<F>(
        &mut self,
        predicate: F,
        timeout: Duration,
    ) -> TestResult<ProcessEvent>
    where
        F: Fn(&ProcessEvent) -> bool,
    {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            {
                let mut output = self.output.lock();
                let events = output.drain_events();
                for event in events {
                    if predicate(&event) {
                        return Ok(event);
                    }
                }
            }

            if !self.is_running() {
                return Err(TestError::ProcessCrashed(
                    "Process exited while waiting for event".into(),
                ));
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Err(TestError::timeout("event", timeout.as_secs()))
    }

    /// Get accumulated output.
    pub fn output(&self) -> impl std::ops::Deref<Target = SiloOutput> + '_ {
        self.output.lock()
    }

    /// Send a graceful shutdown signal.
    pub async fn stop(&mut self) -> TestResult<()> {
        info!(pid = self.pid, "Sending shutdown signal to silo");
        let _ = self.shutdown_tx.send(()).await;

        // Wait for process to exit
        let timeout = Duration::from_secs(10);
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            if let Ok(Some(status)) = self.child.try_wait() {
                info!(pid = self.pid, status = ?status, "Silo process exited");
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Force kill if still running
        warn!(pid = self.pid, "Force killing silo process");
        self.kill().await
    }

    /// Force kill the process.
    pub async fn kill(&mut self) -> TestResult<()> {
        info!(pid = self.pid, "Killing silo process");

        self.child.kill().await.map_err(|e| {
            TestError::ProcessCrashed(format!("Failed to kill process: {}", e))
        })?;

        // Wait for exit
        let _ = self.child.wait().await;

        Ok(())
    }

    /// Wait for the process to exit.
    pub async fn wait(&mut self) -> TestResult<std::process::ExitStatus> {
        self.child.wait().await.map_err(TestError::Io)
    }

    /// Best-effort kill that can be called from synchronous code.
    /// This is used in Drop implementations where we can't await.
    pub fn try_kill_sync(&mut self) {
        if self.is_running() {
            let _ = self.child.start_kill();
        }
    }
}

impl Drop for SiloProcess {
    fn drop(&mut self) {
        // Attempt to kill the process if still running
        if self.is_running() {
            warn!(
                pid = self.pid,
                "SiloProcess dropped while still running, attempting cleanup"
            );
            // Best effort kill - can't await in drop
            let _ = self.child.start_kill();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_silo_process_config_default() {
        let config = SiloProcessConfig::default();
        assert_eq!(config.port, 0);
        assert_eq!(config.membership_server, "127.0.0.1:5000");
        assert!(!config.test_mode);
        assert!(config.create_grain.is_none());
    }

    #[test]
    fn test_silo_process_config_builder() {
        let config = SiloProcessConfig::new("localhost:6000")
            .with_port(8080)
            .with_test_mode()
            .with_create_grain("test-grain")
            .with_wait_for_cluster(3);

        assert_eq!(config.port, 8080);
        assert_eq!(config.membership_server, "localhost:6000");
        assert!(config.test_mode);
        assert_eq!(config.create_grain, Some("test-grain".into()));
        assert_eq!(config.wait_for_cluster, Some(3));
    }

    #[test]
    fn test_process_event_serialization() {
        let event = ProcessEvent::SiloStarted {
            address: "127.0.0.1:8080".into(),
        };

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("silo_started"));
        assert!(json.contains("127.0.0.1:8080"));
    }

    #[test]
    fn test_process_event_deserialization() {
        let json = r#"{"event":"grain_created","grain_id":"test-id","silo":"127.0.0.1:8080","activation_id":"abc123"}"#;
        let event: ProcessEvent = serde_json::from_str(json).unwrap();

        match event {
            ProcessEvent::GrainCreated {
                grain_id,
                silo,
                activation_id,
            } => {
                assert_eq!(grain_id, "test-id");
                assert_eq!(silo, "127.0.0.1:8080");
                assert_eq!(activation_id, "abc123");
            }
            _ => panic!("Expected GrainCreated event"),
        }
    }

    #[test]
    fn test_silo_output() {
        let mut output = SiloOutput::default();

        output.add_event(ProcessEvent::SiloStarted {
            address: "test".into(),
        });
        output.add_stdout("stdout line".into());
        output.add_stderr("stderr line".into());

        assert!(output.next_event().is_some());
        assert_eq!(output.stdout_lines().len(), 1);
        assert_eq!(output.stderr_lines().len(), 1);
    }
}
