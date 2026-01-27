//! Error types for grain persistence operations.
//!
//! This module defines the error types that can occur during storage operations
//! such as reading, writing, and clearing grain state.

use thiserror::Error;

/// Errors that can occur during storage operations.
#[derive(Debug, Error)]
pub enum StorageError {
    /// ETag mismatch during concurrent write.
    ///
    /// This indicates an optimistic concurrency conflict - another writer
    /// has modified the state since it was last read.
    #[error("ETag mismatch: stored '{stored}', expected '{expected}'")]
    EtagMismatch {
        /// The ETag currently stored in the backend.
        stored: String,
        /// The ETag that was expected.
        expected: String,
    },

    /// Attempted to insert a record that already exists.
    #[error("Record already exists for this grain")]
    RecordExists,

    /// Attempted to operate on a record that doesn't exist.
    #[error("Record not found for this grain")]
    RecordNotFound,

    /// Payload exceeds the maximum allowed size.
    #[error("Payload too large: {size} bytes exceeds maximum of {max} bytes")]
    PayloadTooLarge {
        /// The actual payload size.
        size: usize,
        /// The maximum allowed size.
        max: usize,
    },

    /// Serialization failed.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization failed.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// State has not been initialized (read_state not called).
    #[error("State not initialized - call read_state first")]
    StateNotInitialized,

    /// Storage provider is not available or configured.
    #[error("Storage provider not available: {0}")]
    ProviderNotAvailable(String),

    /// Generic I/O or infrastructure error.
    #[error("Storage I/O error: {0}")]
    Io(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),
}

/// Error indicating an optimistic concurrency conflict.
///
/// This is thrown when a write operation fails because the stored ETag
/// doesn't match the expected ETag, indicating another writer has modified
/// the state.
#[derive(Debug, Error)]
#[error("Inconsistent state: stored ETag '{stored_etag}' does not match expected '{current_etag}'")]
pub struct InconsistentStateError {
    /// The ETag currently stored in the backend.
    pub stored_etag: String,
    /// The ETag that was expected.
    pub current_etag: String,
    /// Whether this activation is the source of the conflict.
    pub is_source_activation: bool,
}

impl From<InconsistentStateError> for StorageError {
    fn from(err: InconsistentStateError) -> Self {
        StorageError::EtagMismatch {
            stored: err.stored_etag,
            expected: err.current_etag,
        }
    }
}

/// Result type for storage operations.
pub type StorageResult<T> = Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_etag_mismatch_display() {
        let err = StorageError::EtagMismatch {
            stored: "abc123".to_string(),
            expected: "def456".to_string(),
        };
        assert!(err.to_string().contains("abc123"));
        assert!(err.to_string().contains("def456"));
    }

    #[test]
    fn test_inconsistent_state_error_display() {
        let err = InconsistentStateError {
            stored_etag: "stored".to_string(),
            current_etag: "expected".to_string(),
            is_source_activation: true,
        };
        assert!(err.to_string().contains("stored"));
        assert!(err.to_string().contains("expected"));
    }

    #[test]
    fn test_inconsistent_state_converts_to_storage_error() {
        let err = InconsistentStateError {
            stored_etag: "stored".to_string(),
            current_etag: "expected".to_string(),
            is_source_activation: false,
        };
        let storage_err: StorageError = err.into();
        match storage_err {
            StorageError::EtagMismatch { stored, expected } => {
                assert_eq!(stored, "stored");
                assert_eq!(expected, "expected");
            }
            _ => panic!("Expected EtagMismatch variant"),
        }
    }

    #[test]
    fn test_payload_too_large_display() {
        let err = StorageError::PayloadTooLarge {
            size: 500_000,
            max: 400_000,
        };
        assert!(err.to_string().contains("500000"));
        assert!(err.to_string().contains("400000"));
    }

    #[test]
    fn test_all_error_variants_have_display() {
        let errors: Vec<StorageError> = vec![
            StorageError::EtagMismatch {
                stored: "a".to_string(),
                expected: "b".to_string(),
            },
            StorageError::RecordExists,
            StorageError::RecordNotFound,
            StorageError::PayloadTooLarge { size: 100, max: 50 },
            StorageError::Serialization("test".to_string()),
            StorageError::Deserialization("test".to_string()),
            StorageError::StateNotInitialized,
            StorageError::ProviderNotAvailable("test".to_string()),
            StorageError::Io("test".to_string()),
            StorageError::Configuration("test".to_string()),
        ];

        for err in errors {
            let msg = err.to_string();
            assert!(!msg.is_empty(), "Error variant should have a message");
        }
    }
}
