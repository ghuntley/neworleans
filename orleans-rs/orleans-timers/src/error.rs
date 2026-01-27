//! Timer error types.

use thiserror::Error;

use crate::TimerId;

/// Errors that can occur during timer operations.
#[derive(Debug, Error)]
pub enum TimerError {
    /// The timer was already disposed.
    #[error("Timer {timer_id:?} has already been disposed")]
    AlreadyDisposed { timer_id: TimerId },

    /// The timer was not found.
    #[error("Timer {timer_id:?} not found")]
    NotFound { timer_id: TimerId },

    /// The timer period is too short.
    #[error("Timer period {period_ms}ms is below minimum {min_period_ms}ms")]
    PeriodTooShort { period_ms: u64, min_period_ms: u64 },

    /// The callback channel was closed.
    #[error("Timer callback channel closed")]
    ChannelClosed,

    /// Internal timer error.
    #[error("Internal timer error: {0}")]
    Internal(String),
}

/// Result type for timer operations.
pub type TimerResult<T> = Result<T, TimerError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_already_disposed_error() {
        let err = TimerError::AlreadyDisposed {
            timer_id: TimerId::new(42),
        };
        assert!(err.to_string().contains("42"));
        assert!(err.to_string().contains("disposed"));
    }

    #[test]
    fn test_not_found_error() {
        let err = TimerError::NotFound {
            timer_id: TimerId::new(99),
        };
        assert!(err.to_string().contains("99"));
        assert!(err.to_string().contains("not found"));
    }

    #[test]
    fn test_period_too_short_error() {
        let err = TimerError::PeriodTooShort {
            period_ms: 5,
            min_period_ms: 10,
        };
        assert!(err.to_string().contains("5ms"));
        assert!(err.to_string().contains("10ms"));
    }

    #[test]
    fn test_channel_closed_error() {
        let err = TimerError::ChannelClosed;
        assert!(err.to_string().contains("channel closed"));
    }

    #[test]
    fn test_internal_error() {
        let err = TimerError::Internal("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }
}
