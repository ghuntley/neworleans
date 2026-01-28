//! Error types for event sourcing operations.

use thiserror::Error;

/// Result type for event sourcing operations.
pub type EventSourcingResult<T> = Result<T, EventSourcingError>;

/// Errors that can occur during event sourcing operations.
#[derive(Error, Debug, Clone)]
pub enum EventSourcingError {
    /// Event sequence number is out of order.
    #[error("sequence out of order: expected {expected}, got {actual}")]
    SequenceOutOfOrder {
        /// Expected sequence number.
        expected: u64,
        /// Actual sequence number received.
        actual: u64,
    },

    /// Event not found at the specified sequence.
    #[error("event not found at sequence {0}")]
    EventNotFound(u64),

    /// Snapshot not found at the specified version.
    #[error("snapshot not found at version {0}")]
    SnapshotNotFound(u64),

    /// Event storage is not initialized.
    #[error("event storage not initialized")]
    StorageNotInitialized,

    /// Event log is empty.
    #[error("event log is empty")]
    EmptyEventLog,

    /// Invalid event type encountered.
    #[error("invalid event type: {0}")]
    InvalidEventType(String),

    /// Version conflict during event append.
    #[error("version conflict: expected {expected}, current {current}")]
    VersionConflict {
        /// Expected version.
        expected: u64,
        /// Current version.
        current: u64,
    },

    /// State reconstruction failed.
    #[error("state reconstruction failed: {0}")]
    StateReconstructionFailed(String),

    /// Snapshot creation failed.
    #[error("snapshot creation failed: {0}")]
    SnapshotFailed(String),

    /// Event serialization failed.
    #[error("serialization failed: {0}")]
    SerializationFailed(String),

    /// Event deserialization failed.
    #[error("deserialization failed: {0}")]
    DeserializationFailed(String),

    /// Storage operation failed.
    #[error("storage error: {0}")]
    StorageError(String),

    /// Event log is corrupted.
    #[error("event log corrupted: {0}")]
    CorruptedLog(String),

    /// Operation cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Operation timed out.
    #[error("operation timed out")]
    Timeout,

    /// Grain not configured for event sourcing.
    #[error("grain not configured for event sourcing")]
    NotEventSourced,

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl EventSourcingError {
    /// Returns `true` if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            EventSourcingError::Timeout
                | EventSourcingError::StorageError(_)
                | EventSourcingError::VersionConflict { .. }
        )
    }

    /// Returns `true` if the error indicates corruption.
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            EventSourcingError::CorruptedLog(_)
                | EventSourcingError::SequenceOutOfOrder { .. }
                | EventSourcingError::InvalidEventType(_)
        )
    }

    /// Returns `true` if the error is a serialization error.
    pub fn is_serialization_error(&self) -> bool {
        matches!(
            self,
            EventSourcingError::SerializationFailed(_)
                | EventSourcingError::DeserializationFailed(_)
        )
    }
}

impl From<serde_json::Error> for EventSourcingError {
    fn from(err: serde_json::Error) -> Self {
        EventSourcingError::SerializationFailed(err.to_string())
    }
}

impl From<orleans_persistence::StorageError> for EventSourcingError {
    fn from(err: orleans_persistence::StorageError) -> Self {
        EventSourcingError::StorageError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = EventSourcingError::SequenceOutOfOrder {
            expected: 5,
            actual: 3,
        };
        assert!(err.to_string().contains("expected 5"));
        assert!(err.to_string().contains("got 3"));
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(EventSourcingError::Timeout.is_retryable());
        assert!(EventSourcingError::StorageError("test".into()).is_retryable());
        assert!(EventSourcingError::VersionConflict {
            expected: 1,
            current: 2
        }
        .is_retryable());

        assert!(!EventSourcingError::Cancelled.is_retryable());
        assert!(!EventSourcingError::NotEventSourced.is_retryable());
    }

    #[test]
    fn test_error_is_corruption() {
        assert!(EventSourcingError::CorruptedLog("test".into()).is_corruption());
        assert!(EventSourcingError::SequenceOutOfOrder {
            expected: 1,
            actual: 2
        }
        .is_corruption());
        assert!(EventSourcingError::InvalidEventType("test".into()).is_corruption());

        assert!(!EventSourcingError::Timeout.is_corruption());
    }

    #[test]
    fn test_error_is_serialization_error() {
        assert!(EventSourcingError::SerializationFailed("test".into()).is_serialization_error());
        assert!(EventSourcingError::DeserializationFailed("test".into()).is_serialization_error());

        assert!(!EventSourcingError::Timeout.is_serialization_error());
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_err: Result<(), serde_json::Error> = serde_json::from_str("invalid");
        let es_err: EventSourcingError = json_err.unwrap_err().into();
        assert!(es_err.is_serialization_error());
    }
}
