//! Error types for PostgreSQL storage operations.

use thiserror::Error;

/// Errors that can occur during PostgreSQL storage operations.
#[derive(Error, Debug)]
pub enum PostgresError {
    /// Database connection failed.
    #[error("database connection failed: {0}")]
    ConnectionFailed(String),

    /// Database pool exhausted.
    #[error("database pool exhausted: all connections in use")]
    PoolExhausted,

    /// Query execution failed.
    #[error("query execution failed: {0}")]
    QueryFailed(String),

    /// ETag mismatch during optimistic concurrency control.
    #[error("ETag mismatch: expected '{expected}', found '{actual}'")]
    EtagMismatch { expected: String, actual: String },

    /// Version mismatch during membership table operations.
    #[error("version mismatch: expected {expected}, found {actual}")]
    VersionMismatch { expected: i64, actual: i64 },

    /// Record already exists.
    #[error("record already exists: {0}")]
    RecordExists(String),

    /// Record not found.
    #[error("record not found: {0}")]
    RecordNotFound(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Invalid configuration.
    #[error("invalid configuration: {0}")]
    Configuration(String),

    /// Schema migration failed.
    #[error("schema migration failed: {0}")]
    Migration(String),

    /// Transaction error.
    #[error("transaction error: {0}")]
    Transaction(String),

    /// Timeout error.
    #[error("operation timed out: {0}")]
    Timeout(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl PostgresError {
    /// Returns true if the error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            PostgresError::ConnectionFailed(_)
                | PostgresError::PoolExhausted
                | PostgresError::Timeout(_)
        )
    }

    /// Returns true if this is a concurrency conflict error.
    pub fn is_concurrency_error(&self) -> bool {
        matches!(
            self,
            PostgresError::EtagMismatch { .. } | PostgresError::VersionMismatch { .. }
        )
    }
}

impl From<sqlx::Error> for PostgresError {
    fn from(err: sqlx::Error) -> Self {
        match err {
            sqlx::Error::PoolTimedOut => PostgresError::PoolExhausted,
            sqlx::Error::PoolClosed => PostgresError::ConnectionFailed("pool closed".into()),
            sqlx::Error::RowNotFound => PostgresError::RecordNotFound("row not found".into()),
            _ => PostgresError::QueryFailed(err.to_string()),
        }
    }
}

/// Result type for PostgreSQL operations.
pub type PostgresResult<T> = Result<T, PostgresError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retryable_errors() {
        assert!(PostgresError::ConnectionFailed("test".into()).is_retryable());
        assert!(PostgresError::PoolExhausted.is_retryable());
        assert!(PostgresError::Timeout("test".into()).is_retryable());
        assert!(!PostgresError::RecordNotFound("test".into()).is_retryable());
        assert!(!PostgresError::EtagMismatch {
            expected: "a".into(),
            actual: "b".into()
        }
        .is_retryable());
    }

    #[test]
    fn test_concurrency_errors() {
        assert!(PostgresError::EtagMismatch {
            expected: "a".into(),
            actual: "b".into()
        }
        .is_concurrency_error());
        assert!(PostgresError::VersionMismatch {
            expected: 1,
            actual: 2
        }
        .is_concurrency_error());
        assert!(!PostgresError::RecordNotFound("test".into()).is_concurrency_error());
    }

    #[test]
    fn test_error_display() {
        let err = PostgresError::EtagMismatch {
            expected: "abc".into(),
            actual: "xyz".into(),
        };
        assert!(err.to_string().contains("abc"));
        assert!(err.to_string().contains("xyz"));
    }

    #[test]
    fn test_from_sqlx_error() {
        // Test PoolTimedOut conversion
        let err = PostgresError::from(sqlx::Error::PoolTimedOut);
        assert!(matches!(err, PostgresError::PoolExhausted));
    }
}
