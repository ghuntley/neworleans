//! Configuration options for Orleans transactions.
//!
//! Provides configuration for transaction timeouts, lock behavior,
//! and other tunable parameters.

use std::time::Duration;

/// Configuration options for transactional state.
#[derive(Clone, Debug)]
pub struct TransactionalStateOptions {
    /// Maximum time a lock group can hold the lock before being broken.
    /// Default: 8 seconds.
    pub lock_timeout: Duration,

    /// Maximum time a transaction waits to acquire a lock.
    /// Default: 10 seconds.
    pub lock_acquire_timeout: Duration,

    /// Maximum time the TM waits for prepare responses from participants.
    /// Default: 20 seconds.
    pub prepare_timeout: Duration,

    /// Retry interval for confirmation messages to participants.
    /// Default: 30 seconds.
    pub confirmation_retry_delay: Duration,

    /// Maximum number of concurrent non-conflicting transactions in a lock group.
    /// Default: 20.
    pub max_lock_group_size: usize,

    /// Interval for cleaning up completed transactions.
    /// Default: 60 seconds.
    pub cleanup_interval: Duration,

    /// How long to keep commit records after completion.
    /// Default: 5 minutes.
    pub commit_record_retention: Duration,
}

impl Default for TransactionalStateOptions {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(8),
            lock_acquire_timeout: Duration::from_secs(10),
            prepare_timeout: Duration::from_secs(20),
            confirmation_retry_delay: Duration::from_secs(30),
            max_lock_group_size: 20,
            cleanup_interval: Duration::from_secs(60),
            commit_record_retention: Duration::from_secs(300),
        }
    }
}

impl TransactionalStateOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options suitable for testing (shorter timeouts).
    pub fn for_testing() -> Self {
        Self {
            lock_timeout: Duration::from_millis(500),
            lock_acquire_timeout: Duration::from_millis(500),
            prepare_timeout: Duration::from_secs(2),
            confirmation_retry_delay: Duration::from_millis(100),
            max_lock_group_size: 5,
            cleanup_interval: Duration::from_secs(1),
            commit_record_retention: Duration::from_secs(5),
        }
    }

    /// Sets the lock timeout.
    pub fn with_lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// Sets the lock acquire timeout.
    pub fn with_lock_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.lock_acquire_timeout = timeout;
        self
    }

    /// Sets the prepare timeout.
    pub fn with_prepare_timeout(mut self, timeout: Duration) -> Self {
        self.prepare_timeout = timeout;
        self
    }

    /// Sets the confirmation retry delay.
    pub fn with_confirmation_retry_delay(mut self, delay: Duration) -> Self {
        self.confirmation_retry_delay = delay;
        self
    }

    /// Sets the maximum lock group size.
    pub fn with_max_lock_group_size(mut self, size: usize) -> Self {
        self.max_lock_group_size = size;
        self
    }

    /// Sets the cleanup interval.
    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Sets the commit record retention duration.
    pub fn with_commit_record_retention(mut self, retention: Duration) -> Self {
        self.commit_record_retention = retention;
        self
    }
}

/// Configuration options for the Transaction Agent.
#[derive(Clone, Debug)]
pub struct TransactionAgentOptions {
    /// Default timeout for transactions if not specified.
    /// Default: 30 seconds.
    pub default_timeout: Duration,

    /// Maximum concurrent transactions per agent.
    /// Default: 100.
    pub max_concurrent_transactions: usize,

    /// Enable transaction overload detection.
    /// Default: true.
    pub enable_overload_detection: bool,

    /// Threshold for overload detection (pending transactions).
    /// Default: 50.
    pub overload_threshold: usize,

    /// Retry attempts for transient failures.
    /// Default: 3.
    pub retry_attempts: usize,

    /// Delay between retry attempts.
    /// Default: 100ms.
    pub retry_delay: Duration,
}

impl Default for TransactionAgentOptions {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            max_concurrent_transactions: 100,
            enable_overload_detection: true,
            overload_threshold: 50,
            retry_attempts: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl TransactionAgentOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options suitable for testing.
    pub fn for_testing() -> Self {
        Self {
            default_timeout: Duration::from_secs(5),
            max_concurrent_transactions: 10,
            enable_overload_detection: false,
            overload_threshold: 5,
            retry_attempts: 1,
            retry_delay: Duration::from_millis(10),
        }
    }

    /// Sets the default timeout.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.default_timeout = timeout;
        self
    }

    /// Sets the maximum concurrent transactions.
    pub fn with_max_concurrent_transactions(mut self, max: usize) -> Self {
        self.max_concurrent_transactions = max;
        self
    }

    /// Sets whether to enable overload detection.
    pub fn with_overload_detection(mut self, enabled: bool) -> Self {
        self.enable_overload_detection = enabled;
        self
    }

    /// Sets the overload threshold.
    pub fn with_overload_threshold(mut self, threshold: usize) -> Self {
        self.overload_threshold = threshold;
        self
    }

    /// Sets the number of retry attempts.
    pub fn with_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry_attempts = attempts;
        self
    }

    /// Sets the retry delay.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transactional_state_options_default() {
        let opts = TransactionalStateOptions::default();
        assert_eq!(opts.lock_timeout, Duration::from_secs(8));
        assert_eq!(opts.lock_acquire_timeout, Duration::from_secs(10));
        assert_eq!(opts.prepare_timeout, Duration::from_secs(20));
        assert_eq!(opts.max_lock_group_size, 20);
    }

    #[test]
    fn test_transactional_state_options_for_testing() {
        let opts = TransactionalStateOptions::for_testing();
        assert!(opts.lock_timeout < Duration::from_secs(1));
        assert!(opts.prepare_timeout < Duration::from_secs(5));
    }

    #[test]
    fn test_transactional_state_options_builder() {
        let opts = TransactionalStateOptions::new()
            .with_lock_timeout(Duration::from_secs(15))
            .with_max_lock_group_size(50);
        assert_eq!(opts.lock_timeout, Duration::from_secs(15));
        assert_eq!(opts.max_lock_group_size, 50);
    }

    #[test]
    fn test_transaction_agent_options_default() {
        let opts = TransactionAgentOptions::default();
        assert_eq!(opts.default_timeout, Duration::from_secs(30));
        assert_eq!(opts.max_concurrent_transactions, 100);
        assert!(opts.enable_overload_detection);
    }

    #[test]
    fn test_transaction_agent_options_for_testing() {
        let opts = TransactionAgentOptions::for_testing();
        assert!(!opts.enable_overload_detection);
        assert!(opts.default_timeout < Duration::from_secs(10));
    }

    #[test]
    fn test_transaction_agent_options_builder() {
        let opts = TransactionAgentOptions::new()
            .with_default_timeout(Duration::from_secs(60))
            .with_overload_detection(false);
        assert_eq!(opts.default_timeout, Duration::from_secs(60));
        assert!(!opts.enable_overload_detection);
    }
}
