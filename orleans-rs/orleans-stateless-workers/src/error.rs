//! Error types for stateless worker operations.

use thiserror::Error;

/// Result type for stateless worker operations.
pub type StatelessWorkerResult<T> = Result<T, StatelessWorkerError>;

/// Errors that can occur during stateless worker operations.
#[derive(Error, Debug, Clone)]
pub enum StatelessWorkerError {
    /// Worker pool has reached maximum capacity.
    #[error("worker pool at maximum capacity: {max_workers} workers")]
    PoolAtCapacity {
        /// Maximum number of workers allowed.
        max_workers: usize,
    },

    /// No workers available to handle request.
    #[error("no workers available")]
    NoWorkersAvailable,

    /// Worker creation failed.
    #[error("failed to create worker: {reason}")]
    WorkerCreationFailed {
        /// Reason for the failure.
        reason: String,
    },

    /// Worker not found in pool.
    #[error("worker not found: {worker_id}")]
    WorkerNotFound {
        /// The worker ID that was not found.
        worker_id: String,
    },

    /// Invalid configuration.
    #[error("invalid configuration: {message}")]
    InvalidConfiguration {
        /// Description of the configuration error.
        message: String,
    },

    /// Context is shutting down.
    #[error("stateless worker context is shutting down")]
    ShuttingDown,

    /// Message routing failed.
    #[error("failed to route message: {reason}")]
    MessageRoutingFailed {
        /// Reason for the routing failure.
        reason: String,
    },

    /// PID controller error.
    #[error("PID controller error: {message}")]
    PidControllerError {
        /// Description of the error.
        message: String,
    },

    /// Activation failed.
    #[error("worker activation failed: {reason}")]
    ActivationFailed {
        /// Reason for the activation failure.
        reason: String,
    },

    /// Deactivation failed.
    #[error("worker deactivation failed: {reason}")]
    DeactivationFailed {
        /// Reason for the deactivation failure.
        reason: String,
    },

    /// Internal error.
    #[error("internal error: {message}")]
    Internal {
        /// Description of the error.
        message: String,
    },
}

impl StatelessWorkerError {
    /// Creates a new pool at capacity error.
    pub fn pool_at_capacity(max_workers: usize) -> Self {
        Self::PoolAtCapacity { max_workers }
    }

    /// Creates a new worker creation failed error.
    pub fn worker_creation_failed(reason: impl Into<String>) -> Self {
        Self::WorkerCreationFailed {
            reason: reason.into(),
        }
    }

    /// Creates a new worker not found error.
    pub fn worker_not_found(worker_id: impl Into<String>) -> Self {
        Self::WorkerNotFound {
            worker_id: worker_id.into(),
        }
    }

    /// Creates a new invalid configuration error.
    pub fn invalid_configuration(message: impl Into<String>) -> Self {
        Self::InvalidConfiguration {
            message: message.into(),
        }
    }

    /// Creates a new message routing failed error.
    pub fn message_routing_failed(reason: impl Into<String>) -> Self {
        Self::MessageRoutingFailed {
            reason: reason.into(),
        }
    }

    /// Creates a new activation failed error.
    pub fn activation_failed(reason: impl Into<String>) -> Self {
        Self::ActivationFailed {
            reason: reason.into(),
        }
    }

    /// Creates a new deactivation failed error.
    pub fn deactivation_failed(reason: impl Into<String>) -> Self {
        Self::DeactivationFailed {
            reason: reason.into(),
        }
    }

    /// Creates a new internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_at_capacity_error() {
        let err = StatelessWorkerError::pool_at_capacity(8);
        assert!(matches!(err, StatelessWorkerError::PoolAtCapacity { max_workers: 8 }));
        assert!(err.to_string().contains("8 workers"));
    }

    #[test]
    fn test_worker_creation_failed_error() {
        let err = StatelessWorkerError::worker_creation_failed("out of memory");
        assert!(matches!(err, StatelessWorkerError::WorkerCreationFailed { .. }));
        assert!(err.to_string().contains("out of memory"));
    }

    #[test]
    fn test_worker_not_found_error() {
        let err = StatelessWorkerError::worker_not_found("worker-123");
        assert!(matches!(err, StatelessWorkerError::WorkerNotFound { .. }));
        assert!(err.to_string().contains("worker-123"));
    }

    #[test]
    fn test_invalid_configuration_error() {
        let err = StatelessWorkerError::invalid_configuration("max_workers must be > 0");
        assert!(matches!(err, StatelessWorkerError::InvalidConfiguration { .. }));
        assert!(err.to_string().contains("max_workers must be > 0"));
    }

    #[test]
    fn test_shutting_down_error() {
        let err = StatelessWorkerError::ShuttingDown;
        assert!(err.to_string().contains("shutting down"));
    }

    #[test]
    fn test_message_routing_failed_error() {
        let err = StatelessWorkerError::message_routing_failed("no available workers");
        assert!(matches!(err, StatelessWorkerError::MessageRoutingFailed { .. }));
    }

    #[test]
    fn test_activation_failed_error() {
        let err = StatelessWorkerError::activation_failed("timeout");
        assert!(matches!(err, StatelessWorkerError::ActivationFailed { .. }));
    }

    #[test]
    fn test_deactivation_failed_error() {
        let err = StatelessWorkerError::deactivation_failed("still processing");
        assert!(matches!(err, StatelessWorkerError::DeactivationFailed { .. }));
    }

    #[test]
    fn test_internal_error() {
        let err = StatelessWorkerError::internal("unexpected state");
        assert!(matches!(err, StatelessWorkerError::Internal { .. }));
    }

    #[test]
    fn test_error_is_clone() {
        let err = StatelessWorkerError::pool_at_capacity(4);
        let cloned = err.clone();
        assert_eq!(err.to_string(), cloned.to_string());
    }
}
