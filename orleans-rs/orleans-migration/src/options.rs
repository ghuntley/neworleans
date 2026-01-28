//! Configuration options for grain migration.
//!
//! This module provides configuration for controlling migration behavior,
//! including timeouts, retry policies, and batch sizes.

use std::time::Duration;

/// Configuration options for the migration manager.
#[derive(Debug, Clone)]
pub struct MigrationOptions {
    /// Timeout for the entire migration operation.
    pub migration_timeout: Duration,

    /// Timeout for the dehydration phase.
    pub dehydration_timeout: Duration,

    /// Timeout for the rehydration phase.
    pub rehydration_timeout: Duration,

    /// Timeout for state transfer between silos.
    pub state_transfer_timeout: Duration,

    /// Maximum number of concurrent migrations per silo.
    pub max_concurrent_migrations: usize,

    /// Maximum number of retry attempts for failed migrations.
    pub max_retry_attempts: u32,

    /// Delay between retry attempts.
    pub retry_delay: Duration,

    /// Whether to allow migration of grains with pending requests.
    pub allow_migration_with_pending_requests: bool,

    /// Maximum size (in bytes) of migration context data.
    pub max_context_size: usize,

    /// Whether to enable migration metrics collection.
    pub enable_metrics: bool,

    /// Maximum time to wait for in-flight requests to complete before migration.
    pub drain_timeout: Duration,

    /// Whether to forward messages to the new location after migration.
    pub enable_message_forwarding: bool,

    /// Maximum number of times a message can be forwarded.
    pub max_forward_count: u32,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            migration_timeout: Duration::from_secs(60),
            dehydration_timeout: Duration::from_secs(10),
            rehydration_timeout: Duration::from_secs(10),
            state_transfer_timeout: Duration::from_secs(30),
            max_concurrent_migrations: 10,
            max_retry_attempts: 3,
            retry_delay: Duration::from_millis(500),
            allow_migration_with_pending_requests: false,
            max_context_size: 10 * 1024 * 1024, // 10 MB
            enable_metrics: true,
            drain_timeout: Duration::from_secs(5),
            enable_message_forwarding: true,
            max_forward_count: 2,
        }
    }
}

impl MigrationOptions {
    /// Create a new MigrationOptions with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create options suitable for testing with shorter timeouts.
    pub fn for_testing() -> Self {
        Self {
            migration_timeout: Duration::from_secs(5),
            dehydration_timeout: Duration::from_secs(2),
            rehydration_timeout: Duration::from_secs(2),
            state_transfer_timeout: Duration::from_secs(3),
            max_concurrent_migrations: 100,
            max_retry_attempts: 1,
            retry_delay: Duration::from_millis(50),
            allow_migration_with_pending_requests: true,
            max_context_size: 1024 * 1024, // 1 MB
            enable_metrics: false,
            drain_timeout: Duration::from_millis(500),
            enable_message_forwarding: true,
            max_forward_count: 2,
        }
    }

    /// Create options for aggressive migration (silo shutdown).
    pub fn for_shutdown() -> Self {
        Self {
            migration_timeout: Duration::from_secs(30),
            dehydration_timeout: Duration::from_secs(5),
            rehydration_timeout: Duration::from_secs(5),
            state_transfer_timeout: Duration::from_secs(15),
            max_concurrent_migrations: 50,
            max_retry_attempts: 2,
            retry_delay: Duration::from_millis(100),
            allow_migration_with_pending_requests: true, // Must migrate during shutdown
            max_context_size: 10 * 1024 * 1024,
            enable_metrics: true,
            drain_timeout: Duration::from_secs(2),
            enable_message_forwarding: true,
            max_forward_count: 3,
        }
    }

    /// Set the migration timeout.
    pub fn with_migration_timeout(mut self, timeout: Duration) -> Self {
        self.migration_timeout = timeout;
        self
    }

    /// Set the dehydration timeout.
    pub fn with_dehydration_timeout(mut self, timeout: Duration) -> Self {
        self.dehydration_timeout = timeout;
        self
    }

    /// Set the rehydration timeout.
    pub fn with_rehydration_timeout(mut self, timeout: Duration) -> Self {
        self.rehydration_timeout = timeout;
        self
    }

    /// Set the state transfer timeout.
    pub fn with_state_transfer_timeout(mut self, timeout: Duration) -> Self {
        self.state_transfer_timeout = timeout;
        self
    }

    /// Set the maximum concurrent migrations.
    pub fn with_max_concurrent_migrations(mut self, max: usize) -> Self {
        self.max_concurrent_migrations = max;
        self
    }

    /// Set the maximum retry attempts.
    pub fn with_max_retry_attempts(mut self, max: u32) -> Self {
        self.max_retry_attempts = max;
        self
    }

    /// Set the retry delay.
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Set whether to allow migration with pending requests.
    pub fn with_allow_pending_requests(mut self, allow: bool) -> Self {
        self.allow_migration_with_pending_requests = allow;
        self
    }

    /// Set the maximum context size.
    pub fn with_max_context_size(mut self, size: usize) -> Self {
        self.max_context_size = size;
        self
    }

    /// Set whether to enable metrics.
    pub fn with_metrics(mut self, enable: bool) -> Self {
        self.enable_metrics = enable;
        self
    }

    /// Set the drain timeout.
    pub fn with_drain_timeout(mut self, timeout: Duration) -> Self {
        self.drain_timeout = timeout;
        self
    }

    /// Set whether to enable message forwarding.
    pub fn with_message_forwarding(mut self, enable: bool) -> Self {
        self.enable_message_forwarding = enable;
        self
    }

    /// Set the maximum forward count.
    pub fn with_max_forward_count(mut self, count: u32) -> Self {
        self.max_forward_count = count;
        self
    }
}

/// Grain-level migration configuration attributes.
#[derive(Debug, Clone, Default)]
pub struct GrainMigrationConfig {
    /// Whether this grain type can be migrated.
    pub is_migratable: bool,

    /// Whether to persist state before migration (false = transfer in-memory only).
    pub persist_before_migration: bool,

    /// Custom timeout override for this grain type.
    pub custom_timeout: Option<Duration>,

    /// Priority for migration order (higher = migrated first during shutdown).
    pub migration_priority: i32,
}

impl GrainMigrationConfig {
    /// Create a new migratable grain configuration.
    pub fn migratable() -> Self {
        Self {
            is_migratable: true,
            persist_before_migration: false,
            custom_timeout: None,
            migration_priority: 0,
        }
    }

    /// Create a non-migratable (immovable) grain configuration.
    pub fn immovable() -> Self {
        Self {
            is_migratable: false,
            persist_before_migration: false,
            custom_timeout: None,
            migration_priority: 0,
        }
    }

    /// Set whether to persist state before migration.
    pub fn with_persist_before_migration(mut self, persist: bool) -> Self {
        self.persist_before_migration = persist;
        self
    }

    /// Set a custom timeout for this grain type.
    pub fn with_custom_timeout(mut self, timeout: Duration) -> Self {
        self.custom_timeout = Some(timeout);
        self
    }

    /// Set the migration priority.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.migration_priority = priority;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = MigrationOptions::default();
        assert_eq!(options.migration_timeout, Duration::from_secs(60));
        assert_eq!(options.max_concurrent_migrations, 10);
        assert_eq!(options.max_retry_attempts, 3);
        assert!(!options.allow_migration_with_pending_requests);
    }

    #[test]
    fn test_for_testing_options() {
        let options = MigrationOptions::for_testing();
        assert_eq!(options.migration_timeout, Duration::from_secs(5));
        assert_eq!(options.max_concurrent_migrations, 100);
        assert_eq!(options.max_retry_attempts, 1);
        assert!(options.allow_migration_with_pending_requests);
    }

    #[test]
    fn test_for_shutdown_options() {
        let options = MigrationOptions::for_shutdown();
        assert_eq!(options.migration_timeout, Duration::from_secs(30));
        assert!(options.allow_migration_with_pending_requests);
        assert_eq!(options.max_concurrent_migrations, 50);
    }

    #[test]
    fn test_builder_pattern() {
        let options = MigrationOptions::new()
            .with_migration_timeout(Duration::from_secs(120))
            .with_max_retry_attempts(5)
            .with_allow_pending_requests(true)
            .with_max_context_size(5 * 1024 * 1024);

        assert_eq!(options.migration_timeout, Duration::from_secs(120));
        assert_eq!(options.max_retry_attempts, 5);
        assert!(options.allow_migration_with_pending_requests);
        assert_eq!(options.max_context_size, 5 * 1024 * 1024);
    }

    #[test]
    fn test_grain_migration_config_migratable() {
        let config = GrainMigrationConfig::migratable();
        assert!(config.is_migratable);
        assert!(!config.persist_before_migration);
        assert!(config.custom_timeout.is_none());
    }

    #[test]
    fn test_grain_migration_config_immovable() {
        let config = GrainMigrationConfig::immovable();
        assert!(!config.is_migratable);
    }

    #[test]
    fn test_grain_migration_config_builder() {
        let config = GrainMigrationConfig::migratable()
            .with_persist_before_migration(true)
            .with_custom_timeout(Duration::from_secs(30))
            .with_priority(10);

        assert!(config.is_migratable);
        assert!(config.persist_before_migration);
        assert_eq!(config.custom_timeout, Some(Duration::from_secs(30)));
        assert_eq!(config.migration_priority, 10);
    }

    #[test]
    fn test_options_clone() {
        let options = MigrationOptions::default();
        let cloned = options.clone();
        assert_eq!(options.migration_timeout, cloned.migration_timeout);
        assert_eq!(
            options.max_concurrent_migrations,
            cloned.max_concurrent_migrations
        );
    }

    #[test]
    fn test_drain_timeout() {
        let options = MigrationOptions::default().with_drain_timeout(Duration::from_secs(10));
        assert_eq!(options.drain_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_message_forwarding_options() {
        let options = MigrationOptions::default()
            .with_message_forwarding(false)
            .with_max_forward_count(5);

        assert!(!options.enable_message_forwarding);
        assert_eq!(options.max_forward_count, 5);
    }
}
