//! Error types for call filters.
//!
//! This module defines the error types that can occur during filter
//! pipeline execution.

use thiserror::Error;

/// Errors that can occur during filter pipeline execution.
#[derive(Debug, Error)]
pub enum FilterError {
    /// A filter in the chain did not call `context.invoke()`.
    #[error("Broken filter chain at stage {stage}: filter '{filter_name}' did not continue the chain")]
    BrokenFilterChain {
        /// The stage index where the chain was broken.
        stage: usize,
        /// The name/type of the filter that broke the chain.
        filter_name: String,
    },

    /// A filter in the chain did not set a response after invoking.
    #[error("Filter '{filter_name}' invoked but did not set a response")]
    NoResponseSet {
        /// The name/type of the filter that failed to set a response.
        filter_name: String,
    },

    /// The filter pipeline is not properly configured.
    #[error("Filter pipeline configuration error: {0}")]
    Configuration(String),

    /// An error occurred during method invocation.
    #[error("Method invocation error: {0}")]
    Invocation(String),

    /// An internal error occurred.
    #[error("Internal filter error: {0}")]
    Internal(String),

    /// The request context key was not found.
    #[error("Request context key not found: {key}")]
    ContextKeyNotFound {
        /// The key that was not found.
        key: String,
    },

    /// The request context value had an unexpected type.
    #[error("Request context type mismatch for key '{key}': expected {expected}, got {actual}")]
    ContextTypeMismatch {
        /// The key being accessed.
        key: String,
        /// The expected type name.
        expected: String,
        /// The actual type name.
        actual: String,
    },

    /// Access denied by a filter.
    #[error("Access denied: {reason}")]
    AccessDenied {
        /// The reason for denial.
        reason: String,
    },

    /// The filter timed out.
    #[error("Filter timeout: {filter_name} exceeded {timeout_ms}ms")]
    Timeout {
        /// The filter that timed out.
        filter_name: String,
        /// The timeout in milliseconds.
        timeout_ms: u64,
    },
}

/// Result type for filter operations.
pub type FilterResult<T> = Result<T, FilterError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broken_filter_chain_error() {
        let err = FilterError::BrokenFilterChain {
            stage: 2,
            filter_name: "LoggingFilter".to_string(),
        };
        assert!(err.to_string().contains("stage 2"));
        assert!(err.to_string().contains("LoggingFilter"));
    }

    #[test]
    fn test_no_response_set_error() {
        let err = FilterError::NoResponseSet {
            filter_name: "AuthFilter".to_string(),
        };
        assert!(err.to_string().contains("AuthFilter"));
    }

    #[test]
    fn test_configuration_error() {
        let err = FilterError::Configuration("missing filter".to_string());
        assert!(err.to_string().contains("missing filter"));
    }

    #[test]
    fn test_invocation_error() {
        let err = FilterError::Invocation("method not found".to_string());
        assert!(err.to_string().contains("method not found"));
    }

    #[test]
    fn test_access_denied_error() {
        let err = FilterError::AccessDenied {
            reason: "admin only".to_string(),
        };
        assert!(err.to_string().contains("admin only"));
    }

    #[test]
    fn test_timeout_error() {
        let err = FilterError::Timeout {
            filter_name: "SlowFilter".to_string(),
            timeout_ms: 5000,
        };
        assert!(err.to_string().contains("SlowFilter"));
        assert!(err.to_string().contains("5000ms"));
    }

    #[test]
    fn test_context_key_not_found() {
        let err = FilterError::ContextKeyNotFound {
            key: "user_id".to_string(),
        };
        assert!(err.to_string().contains("user_id"));
    }

    #[test]
    fn test_context_type_mismatch() {
        let err = FilterError::ContextTypeMismatch {
            key: "user_id".to_string(),
            expected: "String".to_string(),
            actual: "i32".to_string(),
        };
        assert!(err.to_string().contains("user_id"));
        assert!(err.to_string().contains("String"));
        assert!(err.to_string().contains("i32"));
    }
}
