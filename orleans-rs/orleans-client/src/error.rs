//! Error types for the Orleans client.

use std::fmt;
use thiserror::Error;

/// Result type for client operations.
pub type ClientResult<T> = Result<T, ClientError>;

/// Errors that can occur during client operations.
#[derive(Debug, Error)]
pub enum ClientError {
    /// Client is not connected to the cluster.
    #[error("client is not connected to the cluster")]
    NotConnected,

    /// Client is already connected.
    #[error("client is already connected")]
    AlreadyConnected,

    /// Client is currently connecting.
    #[error("client is currently connecting")]
    Connecting,

    /// Connection to gateway failed.
    #[error("failed to connect to gateway: {0}")]
    GatewayConnectionFailed(String),

    /// No gateways available.
    #[error("no gateways available in the cluster")]
    NoGatewaysAvailable,

    /// Gateway disconnected unexpectedly.
    #[error("gateway disconnected: {0}")]
    GatewayDisconnected(String),

    /// Request timed out.
    #[error("request timed out after {0:?}")]
    RequestTimeout(std::time::Duration),

    /// Request was rejected.
    #[error("request rejected: {0}")]
    RequestRejected(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Network error.
    #[error("network error: {0}")]
    Network(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// Internal client error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Client is shutting down.
    #[error("client is shutting down")]
    ShuttingDown,

    /// Callback not found for correlation ID.
    #[error("callback not found for correlation ID: {0}")]
    CallbackNotFound(String),

    /// Client lifecycle error.
    #[error("lifecycle error: {0}")]
    Lifecycle(String),

    /// Messaging error from orleans-messaging.
    #[error("messaging error: {0}")]
    Messaging(#[from] orleans_messaging::MessagingError),

    /// Membership error from orleans-clustering.
    #[error("membership error: {0}")]
    Membership(#[from] orleans_clustering::MembershipError),
}

impl ClientError {
    /// Check if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ClientError::GatewayConnectionFailed(_)
                | ClientError::GatewayDisconnected(_)
                | ClientError::RequestTimeout(_)
                | ClientError::Network(_)
                | ClientError::NoGatewaysAvailable
        )
    }

    /// Check if this error indicates the client should reconnect.
    pub fn should_reconnect(&self) -> bool {
        matches!(
            self,
            ClientError::GatewayDisconnected(_) | ClientError::NotConnected
        )
    }
}

/// Status of the cluster client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientStatus {
    /// Client is created but not connected.
    Created,
    /// Client is currently connecting.
    Connecting,
    /// Client is connected and ready.
    Connected,
    /// Client is disconnecting.
    Disconnecting,
    /// Client is disconnected.
    Disconnected,
}

impl fmt::Display for ClientStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientStatus::Created => write!(f, "Created"),
            ClientStatus::Connecting => write!(f, "Connecting"),
            ClientStatus::Connected => write!(f, "Connected"),
            ClientStatus::Disconnecting => write!(f, "Disconnecting"),
            ClientStatus::Disconnected => write!(f, "Disconnected"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = ClientError::NotConnected;
        assert_eq!(err.to_string(), "client is not connected to the cluster");

        let err = ClientError::RequestTimeout(std::time::Duration::from_secs(30));
        assert!(err.to_string().contains("30s"));
    }

    #[test]
    fn test_is_retryable() {
        assert!(ClientError::GatewayConnectionFailed("test".into()).is_retryable());
        assert!(ClientError::GatewayDisconnected("test".into()).is_retryable());
        assert!(ClientError::RequestTimeout(std::time::Duration::from_secs(1)).is_retryable());
        assert!(ClientError::Network("test".into()).is_retryable());
        assert!(ClientError::NoGatewaysAvailable.is_retryable());

        assert!(!ClientError::NotConnected.is_retryable());
        assert!(!ClientError::Configuration("test".into()).is_retryable());
        assert!(!ClientError::Serialization("test".into()).is_retryable());
    }

    #[test]
    fn test_should_reconnect() {
        assert!(ClientError::GatewayDisconnected("test".into()).should_reconnect());
        assert!(ClientError::NotConnected.should_reconnect());

        assert!(!ClientError::RequestTimeout(std::time::Duration::from_secs(1)).should_reconnect());
        assert!(!ClientError::Configuration("test".into()).should_reconnect());
    }

    #[test]
    fn test_client_status_display() {
        assert_eq!(ClientStatus::Created.to_string(), "Created");
        assert_eq!(ClientStatus::Connecting.to_string(), "Connecting");
        assert_eq!(ClientStatus::Connected.to_string(), "Connected");
        assert_eq!(ClientStatus::Disconnecting.to_string(), "Disconnecting");
        assert_eq!(ClientStatus::Disconnected.to_string(), "Disconnected");
    }
}
