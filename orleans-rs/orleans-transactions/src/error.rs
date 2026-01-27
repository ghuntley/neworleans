//! Error types for Orleans transactions.
//!
//! Defines the error hierarchy for transaction operations including:
//! - `TransactionalStatus`: Status codes for transaction outcomes
//! - `TransactionError`: Error enum for transaction failures
//! - `AbortedReason`: Specific reasons for transaction aborts

use std::fmt;
use thiserror::Error;

/// Status codes for transaction operations.
///
/// These codes indicate the outcome of transaction phases and help
/// coordinate the two-phase commit protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransactionalStatus {
    /// Transaction completed successfully.
    Ok,
    /// Prepare phase timed out waiting for participants.
    PrepareTimeout,
    /// Transaction was aborted due to cascading abort from another transaction.
    CascadingAbort,
    /// Lock was broken due to lock group timeout.
    BrokenLock,
    /// Lock validation failed during prepare.
    LockValidationFailed,
    /// Participant failed to respond during commit.
    ParticipantResponseTimeout,
    /// Transaction manager failed to respond.
    TMResponseTimeout,
    /// Storage operation failed due to conflict (e.g., ETag mismatch).
    StorageConflict,
    /// Transaction presumed aborted (no record found after timeout).
    PresumedAbort,
    /// An unknown exception occurred.
    UnknownException,
    /// Internal assertion failed.
    AssertionFailed,
    /// Commit phase failed.
    CommitFailure,
}

impl TransactionalStatus {
    /// Returns true if this status definitively indicates the transaction was aborted.
    ///
    /// These statuses mean the transaction definitely did not commit and the
    /// client can safely retry.
    #[inline]
    pub fn definitely_aborted(&self) -> bool {
        matches!(
            self,
            Self::PrepareTimeout
                | Self::CascadingAbort
                | Self::BrokenLock
                | Self::LockValidationFailed
                | Self::StorageConflict
                | Self::CommitFailure
        )
    }

    /// Returns true if the transaction outcome is uncertain.
    ///
    /// For these statuses, the client cannot determine if the transaction
    /// committed or aborted and should handle appropriately.
    #[inline]
    pub fn is_in_doubt(&self) -> bool {
        matches!(
            self,
            Self::ParticipantResponseTimeout
                | Self::TMResponseTimeout
                | Self::UnknownException
                | Self::AssertionFailed
        )
    }

    /// Returns true if the transaction succeeded.
    #[inline]
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

impl fmt::Display for TransactionalStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ok => write!(f, "Ok"),
            Self::PrepareTimeout => write!(f, "PrepareTimeout"),
            Self::CascadingAbort => write!(f, "CascadingAbort"),
            Self::BrokenLock => write!(f, "BrokenLock"),
            Self::LockValidationFailed => write!(f, "LockValidationFailed"),
            Self::ParticipantResponseTimeout => write!(f, "ParticipantResponseTimeout"),
            Self::TMResponseTimeout => write!(f, "TMResponseTimeout"),
            Self::StorageConflict => write!(f, "StorageConflict"),
            Self::PresumedAbort => write!(f, "PresumedAbort"),
            Self::UnknownException => write!(f, "UnknownException"),
            Self::AssertionFailed => write!(f, "AssertionFailed"),
            Self::CommitFailure => write!(f, "CommitFailure"),
        }
    }
}

/// Reasons why a transaction was aborted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AbortedReason {
    /// Transaction was aborted due to cascading abort from a conflicting transaction.
    CascadingAbort(String),
    /// Transaction lock was broken due to timeout.
    BrokenLock(String),
    /// Lock upgrade from read to write was not possible.
    LockUpgrade(String),
    /// Prepare phase timed out.
    PrepareTimeout(String),
    /// Orphan call detected (call outside transaction context).
    OrphanCall(String),
    /// Read-only transaction attempted a write.
    ReadOnlyViolated(String),
    /// Transaction was explicitly aborted by user.
    UserAbort(String),
}

impl fmt::Display for AbortedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CascadingAbort(msg) => write!(f, "CascadingAbort: {}", msg),
            Self::BrokenLock(msg) => write!(f, "BrokenLock: {}", msg),
            Self::LockUpgrade(msg) => write!(f, "LockUpgrade: {}", msg),
            Self::PrepareTimeout(msg) => write!(f, "PrepareTimeout: {}", msg),
            Self::OrphanCall(msg) => write!(f, "OrphanCall: {}", msg),
            Self::ReadOnlyViolated(msg) => write!(f, "ReadOnlyViolated: {}", msg),
            Self::UserAbort(msg) => write!(f, "UserAbort: {}", msg),
        }
    }
}

/// Errors that can occur during transaction operations.
#[derive(Error, Debug)]
pub enum TransactionError {
    /// Transaction was aborted for a specific reason.
    #[error("Transaction aborted: {0}")]
    Aborted(AbortedReason),

    /// Transaction outcome is uncertain (in-doubt).
    #[error("Transaction in doubt: {0}")]
    InDoubt(String),

    /// Failed to start a transaction.
    #[error("Transaction start failed: {0}")]
    StartFailed(String),

    /// Transaction system is overloaded.
    #[error("Transaction system overloaded")]
    Overloaded,

    /// Transactions are disabled.
    #[error("Transactions are disabled")]
    Disabled,

    /// Transaction service is not available.
    #[error("Transaction service not available")]
    ServiceNotAvailable,

    /// Transaction timed out.
    #[error("Transaction timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Invalid transaction state.
    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    /// Transaction not found.
    #[error("Transaction not found: {0}")]
    NotFound(String),

    /// Conflict detected with another transaction.
    #[error("Conflict with transaction: {0}")]
    Conflict(String),

    /// Storage operation failed.
    #[error("Storage error: {0}")]
    Storage(String),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl TransactionError {
    /// Returns true if this error indicates a definite abort.
    pub fn is_definitely_aborted(&self) -> bool {
        matches!(self, Self::Aborted(_) | Self::Conflict(_))
    }

    /// Returns true if retrying the transaction might succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Aborted(AbortedReason::CascadingAbort(_))
                | Self::Overloaded
                | Self::Timeout(_)
                | Self::Conflict(_)
        )
    }
}

/// Result type for transaction operations.
pub type TransactionResult<T> = Result<T, TransactionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transactional_status_definitely_aborted() {
        assert!(TransactionalStatus::PrepareTimeout.definitely_aborted());
        assert!(TransactionalStatus::CascadingAbort.definitely_aborted());
        assert!(TransactionalStatus::BrokenLock.definitely_aborted());
        assert!(TransactionalStatus::LockValidationFailed.definitely_aborted());
        assert!(TransactionalStatus::StorageConflict.definitely_aborted());
        assert!(TransactionalStatus::CommitFailure.definitely_aborted());

        assert!(!TransactionalStatus::Ok.definitely_aborted());
        assert!(!TransactionalStatus::TMResponseTimeout.definitely_aborted());
        assert!(!TransactionalStatus::PresumedAbort.definitely_aborted());
    }

    #[test]
    fn test_transactional_status_is_in_doubt() {
        assert!(TransactionalStatus::ParticipantResponseTimeout.is_in_doubt());
        assert!(TransactionalStatus::TMResponseTimeout.is_in_doubt());
        assert!(TransactionalStatus::UnknownException.is_in_doubt());
        assert!(TransactionalStatus::AssertionFailed.is_in_doubt());

        assert!(!TransactionalStatus::Ok.is_in_doubt());
        assert!(!TransactionalStatus::CascadingAbort.is_in_doubt());
    }

    #[test]
    fn test_transactional_status_is_success() {
        assert!(TransactionalStatus::Ok.is_success());
        assert!(!TransactionalStatus::PrepareTimeout.is_success());
        assert!(!TransactionalStatus::CommitFailure.is_success());
    }

    #[test]
    fn test_transactional_status_display() {
        assert_eq!(TransactionalStatus::Ok.to_string(), "Ok");
        assert_eq!(
            TransactionalStatus::PrepareTimeout.to_string(),
            "PrepareTimeout"
        );
        assert_eq!(
            TransactionalStatus::CascadingAbort.to_string(),
            "CascadingAbort"
        );
    }

    #[test]
    fn test_aborted_reason_display() {
        let reason = AbortedReason::CascadingAbort("tx123".to_string());
        assert!(reason.to_string().contains("CascadingAbort"));
        assert!(reason.to_string().contains("tx123"));
    }

    #[test]
    fn test_transaction_error_is_definitely_aborted() {
        let aborted = TransactionError::Aborted(AbortedReason::BrokenLock("test".to_string()));
        assert!(aborted.is_definitely_aborted());

        let conflict = TransactionError::Conflict("tx123".to_string());
        assert!(conflict.is_definitely_aborted());

        let timeout = TransactionError::Timeout(std::time::Duration::from_secs(30));
        assert!(!timeout.is_definitely_aborted());
    }

    #[test]
    fn test_transaction_error_is_retryable() {
        let cascading =
            TransactionError::Aborted(AbortedReason::CascadingAbort("test".to_string()));
        assert!(cascading.is_retryable());

        let overloaded = TransactionError::Overloaded;
        assert!(overloaded.is_retryable());

        let timeout = TransactionError::Timeout(std::time::Duration::from_secs(30));
        assert!(timeout.is_retryable());

        let broken = TransactionError::Aborted(AbortedReason::BrokenLock("test".to_string()));
        assert!(!broken.is_retryable());
    }

    #[test]
    fn test_transaction_error_display() {
        let err = TransactionError::InDoubt("unknown outcome".to_string());
        assert!(err.to_string().contains("in doubt"));
        assert!(err.to_string().contains("unknown outcome"));
    }
}
