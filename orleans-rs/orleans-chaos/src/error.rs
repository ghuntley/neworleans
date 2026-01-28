//! Error types for chaos testing operations.
//!
//! This module defines the error types used throughout the chaos testing framework,
//! including errors for fault injection, chaos controller operations, and reporting.

use std::time::Duration;
use thiserror::Error;

/// Errors that can occur during chaos testing operations.
#[derive(Debug, Error)]
pub enum ChaosError {
    /// Fault injection failed.
    #[error("Fault injection failed: {message}")]
    InjectionFailed {
        /// Description of the failure.
        message: String,
        /// The fault type that failed.
        fault_type: String,
    },

    /// Fault not found.
    #[error("Fault not found: {fault_id}")]
    FaultNotFound {
        /// The fault identifier that was not found.
        fault_id: String,
    },

    /// Fault already exists.
    #[error("Fault already exists: {fault_id}")]
    FaultAlreadyExists {
        /// The fault identifier that already exists.
        fault_id: String,
    },

    /// Target not found for fault injection.
    #[error("Target not found: {target}")]
    TargetNotFound {
        /// The target that was not found.
        target: String,
    },

    /// Target not available (e.g., process already dead).
    #[error("Target unavailable: {target} - {reason}")]
    TargetUnavailable {
        /// The target that is unavailable.
        target: String,
        /// Reason for unavailability.
        reason: String,
    },

    /// Chaos controller not started.
    #[error("Chaos controller not started")]
    ControllerNotStarted,

    /// Chaos controller already started.
    #[error("Chaos controller already started")]
    ControllerAlreadyStarted,

    /// Chaos controller is shutting down.
    #[error("Chaos controller is shutting down")]
    ControllerShuttingDown,

    /// Schedule validation failed.
    #[error("Invalid schedule: {message}")]
    InvalidSchedule {
        /// Description of the schedule validation failure.
        message: String,
    },

    /// Probability out of range (must be 0.0 to 1.0).
    #[error("Invalid probability {probability}: must be between 0.0 and 1.0")]
    InvalidProbability {
        /// The invalid probability value.
        probability: f64,
    },

    /// Duration out of range.
    #[error("Invalid duration: {message}")]
    InvalidDuration {
        /// Description of the duration validation failure.
        message: String,
    },

    /// Process operation failed.
    #[error("Process operation failed: {message}")]
    ProcessError {
        /// Description of the process error.
        message: String,
    },

    /// Network operation failed.
    #[error("Network operation failed: {message}")]
    NetworkError {
        /// Description of the network error.
        message: String,
    },

    /// Storage operation failed.
    #[error("Storage operation failed: {message}")]
    StorageError {
        /// Description of the storage error.
        message: String,
    },

    /// Reporting error.
    #[error("Reporting error: {message}")]
    ReportingError {
        /// Description of the reporting error.
        message: String,
    },

    /// Timeout during chaos operation.
    #[error("Operation timed out after {duration:?}")]
    Timeout {
        /// The timeout duration.
        duration: Duration,
    },

    /// Channel communication error.
    #[error("Channel error: {message}")]
    ChannelError {
        /// Description of the channel error.
        message: String,
    },

    /// Internal error.
    #[error("Internal error: {message}")]
    Internal {
        /// Description of the internal error.
        message: String,
    },
}

impl ChaosError {
    /// Returns true if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ChaosError::TargetUnavailable { .. }
                | ChaosError::Timeout { .. }
                | ChaosError::NetworkError { .. }
        )
    }

    /// Returns true if this is a configuration error.
    pub fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            ChaosError::InvalidSchedule { .. }
                | ChaosError::InvalidProbability { .. }
                | ChaosError::InvalidDuration { .. }
        )
    }

    /// Returns true if this is a target-related error.
    pub fn is_target_error(&self) -> bool {
        matches!(
            self,
            ChaosError::TargetNotFound { .. } | ChaosError::TargetUnavailable { .. }
        )
    }

    /// Create an injection failed error.
    pub fn injection_failed(message: impl Into<String>, fault_type: impl Into<String>) -> Self {
        Self::InjectionFailed {
            message: message.into(),
            fault_type: fault_type.into(),
        }
    }

    /// Create a target not found error.
    pub fn target_not_found(target: impl Into<String>) -> Self {
        Self::TargetNotFound {
            target: target.into(),
        }
    }

    /// Create a target unavailable error.
    pub fn target_unavailable(target: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::TargetUnavailable {
            target: target.into(),
            reason: reason.into(),
        }
    }

    /// Create an invalid schedule error.
    pub fn invalid_schedule(message: impl Into<String>) -> Self {
        Self::InvalidSchedule {
            message: message.into(),
        }
    }

    /// Create a process error.
    pub fn process_error(message: impl Into<String>) -> Self {
        Self::ProcessError {
            message: message.into(),
        }
    }

    /// Create a network error.
    pub fn network_error(message: impl Into<String>) -> Self {
        Self::NetworkError {
            message: message.into(),
        }
    }

    /// Create a storage error.
    pub fn storage_error(message: impl Into<String>) -> Self {
        Self::StorageError {
            message: message.into(),
        }
    }

    /// Create a reporting error.
    pub fn reporting_error(message: impl Into<String>) -> Self {
        Self::ReportingError {
            message: message.into(),
        }
    }

    /// Create an internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

/// A specialized Result type for chaos testing operations.
pub type ChaosResult<T> = Result<T, ChaosError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ChaosError::injection_failed("test failure", "NetworkDelay");
        assert!(err.to_string().contains("Fault injection failed"));
        assert!(err.to_string().contains("test failure"));
    }

    #[test]
    fn test_is_retryable() {
        assert!(ChaosError::target_unavailable("silo1", "process dead").is_retryable());
        assert!(ChaosError::Timeout {
            duration: Duration::from_secs(10)
        }
        .is_retryable());
        assert!(ChaosError::network_error("connection refused").is_retryable());

        assert!(!ChaosError::target_not_found("silo1").is_retryable());
        assert!(!ChaosError::ControllerNotStarted.is_retryable());
    }

    #[test]
    fn test_is_configuration_error() {
        assert!(ChaosError::invalid_schedule("bad cron").is_configuration_error());
        assert!(ChaosError::InvalidProbability { probability: 1.5 }.is_configuration_error());
        assert!(ChaosError::InvalidDuration {
            message: "negative".to_string()
        }
        .is_configuration_error());

        assert!(!ChaosError::target_not_found("silo1").is_configuration_error());
    }

    #[test]
    fn test_is_target_error() {
        assert!(ChaosError::target_not_found("silo1").is_target_error());
        assert!(ChaosError::target_unavailable("silo1", "dead").is_target_error());

        assert!(!ChaosError::ControllerNotStarted.is_target_error());
    }

    #[test]
    fn test_fault_not_found() {
        let err = ChaosError::FaultNotFound {
            fault_id: "fault-123".to_string(),
        };
        assert!(err.to_string().contains("fault-123"));
    }

    #[test]
    fn test_fault_already_exists() {
        let err = ChaosError::FaultAlreadyExists {
            fault_id: "fault-123".to_string(),
        };
        assert!(err.to_string().contains("fault-123"));
    }

    #[test]
    fn test_controller_errors() {
        assert_eq!(
            ChaosError::ControllerNotStarted.to_string(),
            "Chaos controller not started"
        );
        assert_eq!(
            ChaosError::ControllerAlreadyStarted.to_string(),
            "Chaos controller already started"
        );
        assert_eq!(
            ChaosError::ControllerShuttingDown.to_string(),
            "Chaos controller is shutting down"
        );
    }

    #[test]
    fn test_timeout_error() {
        let err = ChaosError::Timeout {
            duration: Duration::from_secs(30),
        };
        assert!(err.to_string().contains("30"));
        assert!(err.is_retryable());
    }

    #[test]
    fn test_channel_error() {
        let err = ChaosError::ChannelError {
            message: "receiver dropped".to_string(),
        };
        assert!(err.to_string().contains("receiver dropped"));
    }
}
