//! Error types for integration testing framework.

use std::io;
use thiserror::Error;

/// Result type for integration tests.
pub type TestResult<T> = Result<T, TestError>;

/// Errors that can occur during integration testing.
#[derive(Error, Debug)]
pub enum TestError {
    /// Process failed to start
    #[error("Process failed to start: {0}")]
    ProcessStart(#[source] io::Error),

    /// Process exited with non-zero code
    #[error("Process exited with code {code}: {message}")]
    ProcessExited {
        /// Exit code
        code: i32,
        /// Error message
        message: String,
    },

    /// Process was killed or crashed
    #[error("Process crashed or was killed: {0}")]
    ProcessCrashed(String),

    /// Timeout waiting for operation
    #[error("Timeout after {duration_secs}s waiting for: {operation}")]
    Timeout {
        /// Operation that timed out
        operation: String,
        /// Duration waited in seconds
        duration_secs: u64,
    },

    /// Cluster formation failed
    #[error("Cluster formation failed: {0}")]
    ClusterFormation(String),

    /// Membership assertion failed
    #[error("Membership assertion failed: expected {expected}, got {actual}")]
    MembershipAssertion {
        /// Expected value
        expected: String,
        /// Actual value
        actual: String,
    },

    /// Directory assertion failed
    #[error("Directory assertion failed: {0}")]
    DirectoryAssertion(String),

    /// Grain invocation failed
    #[error("Grain invocation failed: {0}")]
    GrainInvocation(String),

    /// Port allocation failed
    #[error("Port allocation failed: {0}")]
    PortAllocation(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Network error
    #[error("Network error: {0}")]
    Network(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
}

impl TestError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            TestError::Timeout { .. }
                | TestError::Network(_)
                | TestError::ProcessStart(_)
        )
    }

    /// Check if this error indicates a permanent failure.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            TestError::Configuration(_) | TestError::ProcessExited { .. }
        )
    }

    /// Create a timeout error.
    pub fn timeout(operation: impl Into<String>, duration_secs: u64) -> Self {
        TestError::Timeout {
            operation: operation.into(),
            duration_secs,
        }
    }

    /// Create a membership assertion error.
    pub fn membership_assertion(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        TestError::MembershipAssertion {
            expected: expected.into(),
            actual: actual.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TestError::timeout("cluster formation", 30);
        assert!(err.to_string().contains("30s"));
        assert!(err.to_string().contains("cluster formation"));
    }

    #[test]
    fn test_is_retryable() {
        assert!(TestError::timeout("test", 10).is_retryable());
        assert!(TestError::Network("connection refused".into()).is_retryable());
        assert!(!TestError::Configuration("invalid".into()).is_retryable());
    }

    #[test]
    fn test_is_permanent() {
        assert!(TestError::Configuration("invalid".into()).is_permanent());
        assert!(TestError::ProcessExited {
            code: 1,
            message: "failed".into()
        }
        .is_permanent());
        assert!(!TestError::timeout("test", 10).is_permanent());
    }

    #[test]
    fn test_from_io_error() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
        let test_err: TestError = io_err.into();
        assert!(matches!(test_err, TestError::Io(_)));
    }

    #[test]
    fn test_membership_assertion_error() {
        let err = TestError::membership_assertion("3 silos", "2 silos");
        assert!(err.to_string().contains("3 silos"));
        assert!(err.to_string().contains("2 silos"));
    }
}
