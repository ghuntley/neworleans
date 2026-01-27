//! Error types for reminder operations.

use thiserror::Error;

/// Errors that can occur during reminder operations.
#[derive(Debug, Error)]
pub enum ReminderError {
    /// Reminder was not found.
    #[error("reminder not found: {0}")]
    NotFound(String),

    /// Reminder already exists.
    #[error("reminder already exists: {0}")]
    AlreadyExists(String),

    /// ETag mismatch during optimistic concurrency check.
    #[error("etag mismatch: expected {expected}, found {actual}")]
    EtagMismatch { expected: String, actual: String },

    /// Reminder period is too short.
    #[error("reminder period {period_secs}s is below minimum {min_secs}s")]
    PeriodTooShort { period_secs: f64, min_secs: f64 },

    /// Invalid reminder name.
    #[error("invalid reminder name: {0}")]
    InvalidName(String),

    /// Grain does not implement IRemindable.
    #[error("grain does not implement IRemindable trait")]
    NotRemindable,

    /// Reminder service is not initialized.
    #[error("reminder service not initialized")]
    NotInitialized,

    /// Reminder service is shutting down.
    #[error("reminder service is shutting down")]
    ShuttingDown,

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Result type for reminder operations.
pub type ReminderResult<T> = Result<T, ReminderError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_not_found() {
        let err = ReminderError::NotFound("my-reminder".to_string());
        assert_eq!(format!("{}", err), "reminder not found: my-reminder");
    }

    #[test]
    fn test_error_display_already_exists() {
        let err = ReminderError::AlreadyExists("my-reminder".to_string());
        assert_eq!(format!("{}", err), "reminder already exists: my-reminder");
    }

    #[test]
    fn test_error_display_etag_mismatch() {
        let err = ReminderError::EtagMismatch {
            expected: "123".to_string(),
            actual: "456".to_string(),
        };
        assert_eq!(format!("{}", err), "etag mismatch: expected 123, found 456");
    }

    #[test]
    fn test_error_display_period_too_short() {
        let err = ReminderError::PeriodTooShort {
            period_secs: 30.0,
            min_secs: 60.0,
        };
        assert_eq!(
            format!("{}", err),
            "reminder period 30s is below minimum 60s"
        );
    }

    #[test]
    fn test_error_display_invalid_name() {
        let err = ReminderError::InvalidName("".to_string());
        assert_eq!(format!("{}", err), "invalid reminder name: ");
    }
}
