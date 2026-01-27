//! Error types for observer operations.

use thiserror::Error;

/// Errors that can occur during observer operations.
#[derive(Debug, Error)]
pub enum ObserverError {
    /// The observer was garbage collected (weak reference no longer valid).
    #[error("Observer was garbage collected: {observer_id}")]
    ObserverGarbageCollected { observer_id: String },

    /// The observer is not registered.
    #[error("Observer not registered: {observer_id}")]
    NotRegistered { observer_id: String },

    /// The observer is already registered.
    #[error("Observer already registered: {observer_id}")]
    AlreadyRegistered { observer_id: String },

    /// The observer reference is invalid.
    #[error("Invalid observer reference: {reason}")]
    InvalidReference { reason: String },

    /// The grain ID is not an observer grain ID.
    #[error("Not an observer grain ID: {grain_id}")]
    NotObserverGrainId { grain_id: String },

    /// The observer subscription expired.
    #[error("Observer subscription expired: {observer_id}")]
    SubscriptionExpired { observer_id: String },

    /// Failed to deliver notification to observer.
    #[error("Notification delivery failed: {reason}")]
    NotificationFailed { reason: String },

    /// The observer manager is shutting down.
    #[error("Observer manager is shutting down")]
    ShuttingDown,

    /// Channel communication error.
    #[error("Channel error: {reason}")]
    ChannelError { reason: String },

    /// Internal error.
    #[error("Internal observer error: {0}")]
    Internal(String),
}

/// Result type for observer operations.
pub type ObserverResult<T> = Result<T, ObserverError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observer_garbage_collected_display() {
        let err = ObserverError::ObserverGarbageCollected {
            observer_id: "obs-123".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("garbage collected"));
        assert!(msg.contains("obs-123"));
    }

    #[test]
    fn test_not_registered_display() {
        let err = ObserverError::NotRegistered {
            observer_id: "obs-456".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("not registered"));
        assert!(msg.contains("obs-456"));
    }

    #[test]
    fn test_already_registered_display() {
        let err = ObserverError::AlreadyRegistered {
            observer_id: "obs-789".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("already registered"));
        assert!(msg.contains("obs-789"));
    }

    #[test]
    fn test_invalid_reference_display() {
        let err = ObserverError::InvalidReference {
            reason: "null pointer".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid observer reference"));
        assert!(msg.contains("null pointer"));
    }

    #[test]
    fn test_not_observer_grain_id_display() {
        let err = ObserverError::NotObserverGrainId {
            grain_id: "MyGrain/key".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Not an observer grain ID"));
        assert!(msg.contains("MyGrain/key"));
    }

    #[test]
    fn test_subscription_expired_display() {
        let err = ObserverError::SubscriptionExpired {
            observer_id: "obs-expired".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("expired"));
        assert!(msg.contains("obs-expired"));
    }

    #[test]
    fn test_notification_failed_display() {
        let err = ObserverError::NotificationFailed {
            reason: "connection closed".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("delivery failed"));
        assert!(msg.contains("connection closed"));
    }

    #[test]
    fn test_shutting_down_display() {
        let err = ObserverError::ShuttingDown;
        let msg = format!("{}", err);
        assert!(msg.contains("shutting down"));
    }

    #[test]
    fn test_channel_error_display() {
        let err = ObserverError::ChannelError {
            reason: "receiver dropped".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Channel error"));
        assert!(msg.contains("receiver dropped"));
    }

    #[test]
    fn test_internal_error_display() {
        let err = ObserverError::Internal("unexpected state".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Internal"));
        assert!(msg.contains("unexpected state"));
    }
}
