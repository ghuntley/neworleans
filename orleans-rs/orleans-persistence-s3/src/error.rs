//! Error types for S3 storage operations.
//!
//! This module defines the error types returned by S3 storage operations,
//! including errors from the AWS SDK, serialization, and validation.

/// Result type for S3 storage operations.
pub type S3Result<T> = Result<T, S3Error>;

/// Errors that can occur during S3 storage operations.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    /// The specified bucket does not exist.
    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    /// The specified object does not exist.
    #[error("object not found: {key} in bucket {bucket}")]
    ObjectNotFound { bucket: String, key: String },

    /// The bucket or object already exists.
    #[error("object already exists: {key} in bucket {bucket}")]
    ObjectAlreadyExists { bucket: String, key: String },

    /// ETag mismatch during conditional operation.
    #[error("ETag mismatch: expected {expected}, got {actual}")]
    EtagMismatch { expected: String, actual: String },

    /// Precondition failed (e.g., If-Match, If-None-Match).
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),

    /// Access denied to the bucket or object.
    #[error("access denied: {0}")]
    AccessDenied(String),

    /// Invalid bucket name.
    #[error("invalid bucket name: {0}")]
    InvalidBucketName(String),

    /// Invalid object key.
    #[error("invalid object key: {0}")]
    InvalidObjectKey(String),

    /// Object is too large.
    #[error("object too large: {size} bytes exceeds maximum {max_size} bytes")]
    ObjectTooLarge { size: u64, max_size: u64 },

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Compression error.
    #[error("compression error: {0}")]
    Compression(String),

    /// Decompression error.
    #[error("decompression error: {0}")]
    Decompression(String),

    /// AWS SDK error.
    #[error("AWS SDK error: {0}")]
    AwsSdk(String),

    /// Network or connectivity error.
    #[error("network error: {0}")]
    Network(String),

    /// Request timeout.
    #[error("request timeout after {0:?}")]
    Timeout(std::time::Duration),

    /// Rate limit exceeded.
    #[error("rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Credentials error.
    #[error("credentials error: {0}")]
    Credentials(String),

    /// Region error.
    #[error("region error: {0}")]
    Region(String),

    /// The operation was cancelled.
    #[error("operation cancelled")]
    Cancelled,

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl S3Error {
    /// Check if this error is retryable.
    ///
    /// Retryable errors are those that may succeed if the operation is retried,
    /// such as network errors, rate limiting, and timeouts.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            S3Error::Network(_)
                | S3Error::Timeout(_)
                | S3Error::RateLimitExceeded(_)
                | S3Error::Internal(_)
        )
    }

    /// Check if this error is a concurrency error.
    ///
    /// Concurrency errors indicate that another client modified the object
    /// between read and write operations.
    pub fn is_concurrency_error(&self) -> bool {
        matches!(
            self,
            S3Error::EtagMismatch { .. } | S3Error::PreconditionFailed(_)
        )
    }

    /// Check if this error is a not-found error.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            S3Error::BucketNotFound(_) | S3Error::ObjectNotFound { .. }
        )
    }

    /// Check if this error is an access error.
    pub fn is_access_error(&self) -> bool {
        matches!(
            self,
            S3Error::AccessDenied(_) | S3Error::Credentials(_)
        )
    }

    /// Check if this error is a configuration error.
    pub fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            S3Error::Configuration(_)
                | S3Error::InvalidBucketName(_)
                | S3Error::InvalidObjectKey(_)
                | S3Error::Region(_)
        )
    }

    /// Check if this error is a serialization error.
    pub fn is_serialization_error(&self) -> bool {
        matches!(
            self,
            S3Error::Serialization(_)
                | S3Error::Deserialization(_)
                | S3Error::Compression(_)
                | S3Error::Decompression(_)
        )
    }
}

impl From<serde_json::Error> for S3Error {
    fn from(err: serde_json::Error) -> Self {
        S3Error::Serialization(err.to_string())
    }
}

impl From<std::io::Error> for S3Error {
    fn from(err: std::io::Error) -> Self {
        if err.kind() == std::io::ErrorKind::TimedOut {
            S3Error::Timeout(std::time::Duration::from_secs(0))
        } else {
            S3Error::Network(err.to_string())
        }
    }
}

/// Extension trait for converting Orleans persistence errors to S3 errors.
impl From<orleans_persistence::StorageError> for S3Error {
    fn from(err: orleans_persistence::StorageError) -> Self {
        match err {
            orleans_persistence::StorageError::EtagMismatch { stored, expected } => {
                S3Error::EtagMismatch {
                    expected,
                    actual: stored,
                }
            }
            orleans_persistence::StorageError::RecordNotFound => S3Error::ObjectNotFound {
                bucket: "unknown".to_string(),
                key: "unknown".to_string(),
            },
            orleans_persistence::StorageError::RecordExists => S3Error::ObjectAlreadyExists {
                bucket: "unknown".to_string(),
                key: "unknown".to_string(),
            },
            orleans_persistence::StorageError::Serialization(msg) => S3Error::Serialization(msg),
            orleans_persistence::StorageError::Deserialization(msg) => {
                S3Error::Deserialization(msg)
            }
            _ => S3Error::Internal(err.to_string()),
        }
    }
}

impl From<S3Error> for orleans_persistence::StorageError {
    fn from(err: S3Error) -> Self {
        match err {
            S3Error::EtagMismatch { expected, actual } => {
                orleans_persistence::StorageError::EtagMismatch {
                    stored: actual,
                    expected,
                }
            }
            S3Error::ObjectNotFound { .. } => orleans_persistence::StorageError::RecordNotFound,
            S3Error::ObjectAlreadyExists { .. } => orleans_persistence::StorageError::RecordExists,
            S3Error::Serialization(msg) => orleans_persistence::StorageError::Serialization(msg),
            S3Error::Deserialization(msg) => {
                orleans_persistence::StorageError::Deserialization(msg)
            }
            _ => orleans_persistence::StorageError::Io(err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_is_retryable() {
        assert!(S3Error::Network("connection reset".into()).is_retryable());
        assert!(S3Error::Timeout(std::time::Duration::from_secs(30)).is_retryable());
        assert!(S3Error::RateLimitExceeded("too many requests".into()).is_retryable());
        assert!(S3Error::Internal("temporary failure".into()).is_retryable());

        assert!(!S3Error::BucketNotFound("bucket".into()).is_retryable());
        assert!(!S3Error::AccessDenied("forbidden".into()).is_retryable());
        assert!(!S3Error::Configuration("invalid".into()).is_retryable());
    }

    #[test]
    fn test_error_is_concurrency_error() {
        assert!(S3Error::EtagMismatch {
            expected: "a".into(),
            actual: "b".into()
        }
        .is_concurrency_error());
        assert!(S3Error::PreconditionFailed("If-Match".into()).is_concurrency_error());

        assert!(!S3Error::ObjectNotFound {
            bucket: "bucket".into(),
            key: "key".into()
        }
        .is_concurrency_error());
    }

    #[test]
    fn test_error_is_not_found() {
        assert!(S3Error::BucketNotFound("bucket".into()).is_not_found());
        assert!(S3Error::ObjectNotFound {
            bucket: "bucket".into(),
            key: "key".into()
        }
        .is_not_found());

        assert!(!S3Error::AccessDenied("forbidden".into()).is_not_found());
    }

    #[test]
    fn test_error_is_access_error() {
        assert!(S3Error::AccessDenied("forbidden".into()).is_access_error());
        assert!(S3Error::Credentials("invalid".into()).is_access_error());

        assert!(!S3Error::BucketNotFound("bucket".into()).is_access_error());
    }

    #[test]
    fn test_error_is_configuration_error() {
        assert!(S3Error::Configuration("invalid".into()).is_configuration_error());
        assert!(S3Error::InvalidBucketName("bad-bucket".into()).is_configuration_error());
        assert!(S3Error::InvalidObjectKey("bad-key".into()).is_configuration_error());
        assert!(S3Error::Region("invalid region".into()).is_configuration_error());

        assert!(!S3Error::Network("timeout".into()).is_configuration_error());
    }

    #[test]
    fn test_error_is_serialization_error() {
        assert!(S3Error::Serialization("invalid json".into()).is_serialization_error());
        assert!(S3Error::Deserialization("parse error".into()).is_serialization_error());
        assert!(S3Error::Compression("gzip error".into()).is_serialization_error());
        assert!(S3Error::Decompression("inflate error".into()).is_serialization_error());

        assert!(!S3Error::Network("timeout".into()).is_serialization_error());
    }

    #[test]
    fn test_error_display() {
        let err = S3Error::ObjectNotFound {
            bucket: "my-bucket".into(),
            key: "my-key".into(),
        };
        assert!(err.to_string().contains("my-bucket"));
        assert!(err.to_string().contains("my-key"));

        let err = S3Error::EtagMismatch {
            expected: "abc".into(),
            actual: "def".into(),
        };
        assert!(err.to_string().contains("abc"));
        assert!(err.to_string().contains("def"));
    }

    #[test]
    fn test_from_json_error() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let s3_err: S3Error = json_err.into();
        assert!(matches!(s3_err, S3Error::Serialization(_)));
    }

    #[test]
    fn test_from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "connection reset");
        let s3_err: S3Error = io_err.into();
        assert!(matches!(s3_err, S3Error::Network(_)));

        let timeout_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        let s3_err: S3Error = timeout_err.into();
        assert!(matches!(s3_err, S3Error::Timeout(_)));
    }
}
