//! Error types for clustering operations.

use orleans_core::SiloAddress;
use thiserror::Error;

/// Errors that can occur during membership operations.
#[derive(Error, Debug)]
pub enum MembershipError {
    /// The silo was not found in the membership table.
    #[error("Silo not found: {0}")]
    SiloNotFound(SiloAddress),

    /// Version/ETag mismatch during update (optimistic concurrency failure).
    #[error("Version mismatch: expected {expected}, found {actual}")]
    VersionMismatch { expected: i64, actual: i64 },

    /// The silo already exists in the membership table.
    #[error("Silo already exists: {0}")]
    SiloAlreadyExists(SiloAddress),

    /// Invalid state transition attempted.
    #[error("Invalid status transition from {from} to {to}")]
    InvalidStatusTransition {
        from: crate::silo_status::SiloStatus,
        to: crate::silo_status::SiloStatus,
    },

    /// Timeout during membership operation.
    #[error("Membership operation timed out")]
    Timeout,

    /// Failed to join the cluster.
    #[error("Failed to join cluster: {0}")]
    JoinFailed(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Storage backend error.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Network error during probing or gossip.
    #[error("Network error: {0}")]
    Network(String),

    /// The cluster is shutting down.
    #[error("Cluster is shutting down")]
    ClusterShuttingDown,
}

/// Result type for membership operations.
pub type MembershipResult<T> = Result<T, MembershipError>;

impl MembershipError {
    /// Returns true if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::VersionMismatch { .. } | Self::Timeout | Self::Network(_)
        )
    }

    /// Returns true if this error indicates the operation might have partially succeeded.
    pub fn is_uncertain(&self) -> bool {
        matches!(self, Self::Timeout | Self::Network(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::silo_status::SiloStatus;
    use std::net::SocketAddr;

    fn test_address() -> SiloAddress {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    #[test]
    fn test_error_display() {
        let err = MembershipError::SiloNotFound(test_address());
        assert!(err.to_string().contains("Silo not found"));

        let err = MembershipError::VersionMismatch {
            expected: 5,
            actual: 10,
        };
        assert!(err.to_string().contains("expected 5"));
        assert!(err.to_string().contains("found 10"));

        let err = MembershipError::InvalidStatusTransition {
            from: SiloStatus::Active,
            to: SiloStatus::Joining,
        };
        assert!(err.to_string().contains("Active"));
        assert!(err.to_string().contains("Joining"));
    }

    #[test]
    fn test_is_retryable() {
        assert!(MembershipError::VersionMismatch {
            expected: 1,
            actual: 2
        }
        .is_retryable());
        assert!(MembershipError::Timeout.is_retryable());
        assert!(MembershipError::Network("conn refused".to_string()).is_retryable());

        assert!(!MembershipError::SiloNotFound(test_address()).is_retryable());
        assert!(!MembershipError::SiloAlreadyExists(test_address()).is_retryable());
    }

    #[test]
    fn test_is_uncertain() {
        assert!(MembershipError::Timeout.is_uncertain());
        assert!(MembershipError::Network("timeout".to_string()).is_uncertain());

        assert!(!MembershipError::SiloNotFound(test_address()).is_uncertain());
        assert!(!MembershipError::VersionMismatch {
            expected: 1,
            actual: 2
        }
        .is_uncertain());
    }
}
