//! Error types for Orleans streaming operations.

use std::fmt;
use thiserror::Error;

/// Error type for streaming operations.
#[derive(Debug, Clone, Error)]
pub enum StreamError {
    /// Stream subscription not found.
    #[error("Subscription not found: {subscription_id}")]
    SubscriptionNotFound { subscription_id: String },

    /// Stream already has an active subscription with this ID.
    #[error("Subscription already exists: {subscription_id}")]
    SubscriptionAlreadyExists { subscription_id: String },

    /// Stream provider not found.
    #[error("Stream provider not found: {provider_name}")]
    ProviderNotFound { provider_name: String },

    /// Stream provider already registered.
    #[error("Stream provider already registered: {provider_name}")]
    ProviderAlreadyRegistered { provider_name: String },

    /// Invalid stream ID format.
    #[error("Invalid stream ID: {message}")]
    InvalidStreamId { message: String },

    /// Invalid namespace format.
    #[error("Invalid namespace: {namespace}")]
    InvalidNamespace { namespace: String },

    /// Stream completed (no more events).
    #[error("Stream completed")]
    StreamCompleted,

    /// Stream errored.
    #[error("Stream error: {message}")]
    StreamErrored { message: String },

    /// Queue adapter error.
    #[error("Queue adapter error: {message}")]
    QueueAdapterError { message: String },

    /// Serialization error.
    #[error("Serialization error: {message}")]
    Serialization { message: String },

    /// Deserialization error.
    #[error("Deserialization error: {message}")]
    Deserialization { message: String },

    /// Observer delivery failed.
    #[error("Failed to deliver to observer: {message}")]
    DeliveryFailed { message: String },

    /// Filter evaluation failed.
    #[error("Filter evaluation failed: {message}")]
    FilterError { message: String },

    /// Timeout waiting for operation.
    #[error("Operation timed out: {operation}")]
    Timeout { operation: String },

    /// Channel closed.
    #[error("Channel closed")]
    ChannelClosed,

    /// Stream provider is shutting down.
    #[error("Stream provider is shutting down")]
    ShuttingDown,

    /// Cache miss - requested token not in cache.
    #[error("Cache miss: sequence token {token:?} not found in cache")]
    CacheMiss { token: String },

    /// Internal error.
    #[error("Internal error: {message}")]
    Internal { message: String },
}

impl StreamError {
    /// Create a subscription not found error.
    pub fn subscription_not_found(subscription_id: impl Into<String>) -> Self {
        Self::SubscriptionNotFound {
            subscription_id: subscription_id.into(),
        }
    }

    /// Create a provider not found error.
    pub fn provider_not_found(provider_name: impl Into<String>) -> Self {
        Self::ProviderNotFound {
            provider_name: provider_name.into(),
        }
    }

    /// Create an invalid stream ID error.
    pub fn invalid_stream_id(message: impl Into<String>) -> Self {
        Self::InvalidStreamId {
            message: message.into(),
        }
    }

    /// Create a serialization error.
    pub fn serialization(message: impl Into<String>) -> Self {
        Self::Serialization {
            message: message.into(),
        }
    }

    /// Create a deserialization error.
    pub fn deserialization(message: impl Into<String>) -> Self {
        Self::Deserialization {
            message: message.into(),
        }
    }

    /// Create a delivery failed error.
    pub fn delivery_failed(message: impl Into<String>) -> Self {
        Self::DeliveryFailed {
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

/// Result type for streaming operations.
pub type StreamResult<T> = Result<T, StreamError>;

/// Status of a stream delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    /// Delivery succeeded.
    Delivered,
    /// Delivery failed but can be retried.
    Retry,
    /// Delivery failed permanently.
    Failed,
    /// Observer was garbage collected.
    ObserverGone,
}

impl fmt::Display for DeliveryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delivered => write!(f, "delivered"),
            Self::Retry => write!(f, "retry"),
            Self::Failed => write!(f, "failed"),
            Self::ObserverGone => write!(f, "observer_gone"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = StreamError::subscription_not_found("sub-123");
        assert!(err.to_string().contains("sub-123"));

        let err = StreamError::provider_not_found("MemoryProvider");
        assert!(err.to_string().contains("MemoryProvider"));

        let err = StreamError::StreamCompleted;
        assert_eq!(err.to_string(), "Stream completed");
    }

    #[test]
    fn test_error_constructors() {
        let err = StreamError::invalid_stream_id("bad format");
        assert!(matches!(err, StreamError::InvalidStreamId { .. }));

        let err = StreamError::serialization("failed to serialize");
        assert!(matches!(err, StreamError::Serialization { .. }));

        let err = StreamError::internal("unexpected state");
        assert!(matches!(err, StreamError::Internal { .. }));
    }

    #[test]
    fn test_delivery_status_display() {
        assert_eq!(DeliveryStatus::Delivered.to_string(), "delivered");
        assert_eq!(DeliveryStatus::Retry.to_string(), "retry");
        assert_eq!(DeliveryStatus::Failed.to_string(), "failed");
        assert_eq!(DeliveryStatus::ObserverGone.to_string(), "observer_gone");
    }
}
