//! PostgreSQL storage providers for Orleans-RS.
//!
//! This crate provides PostgreSQL implementations of the core storage interfaces:
//!
//! - [`PostgresMembershipTable`] - Cluster membership table storage
//! - [`PostgresGrainStorage`] - Grain state persistence
//! - [`PostgresReminderTable`] - Reminder table storage
//!
//! # Features
//!
//! - Full PostgreSQL support via `sqlx` with async/await
//! - Optimistic concurrency control via ETags
//! - Automatic schema migrations
//! - Configurable connection pooling
//! - Structured logging via `tracing`
//!
//! # Example
//!
//! ```ignore
//! use orleans_postgres::{PostgresOptions, PostgresMembershipTable, PostgresGrainStorage, PostgresReminderTable};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure PostgreSQL connection
//!     let options = PostgresOptions::new("postgres://user:pass@localhost/orleans")
//!         .with_schema("my_cluster")
//!         .with_max_connections(20);
//!
//!     // Create storage providers
//!     let membership = PostgresMembershipTable::new(&options).await?;
//!     let storage = PostgresGrainStorage::new(&options).await?;
//!     let reminders = PostgresReminderTable::new(&options).await?;
//!
//!     // Use with Orleans silo/client...
//!     Ok(())
//! }
//! ```
//!
//! # Database Schema
//!
//! The crate automatically creates the following tables (in the configured schema):
//!
//! ## Membership Tables
//!
//! - `membership` - Silo entries with status, heartbeat, and suspect votes
//! - `membership_version` - Table version for optimistic concurrency
//!
//! ## Grain Storage Tables
//!
//! - `grain_state` - Grain state with type, key, and serialized data
//!
//! ## Reminder Tables
//!
//! - `reminders` - Reminder entries with scheduling information

mod error;
mod membership;
mod options;
mod reminder;
mod storage;

pub use error::{PostgresError, PostgresResult};
pub use membership::PostgresMembershipTable;
pub use options::PostgresOptions;
pub use reminder::PostgresReminderTable;
pub use storage::PostgresGrainStorage;

// Re-export sqlx pool for advanced usage
pub use sqlx::PgPool;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_validation() {
        let opts = PostgresOptions::new("postgres://localhost/test");
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_options_for_testing() {
        let opts = PostgresOptions::for_testing("postgres://localhost/test");
        assert_eq!(opts.schema, "orleans_test");
        assert_eq!(opts.cluster_id, "test");
    }

    #[test]
    fn test_error_retryable() {
        assert!(PostgresError::ConnectionFailed("test".into()).is_retryable());
        assert!(PostgresError::PoolExhausted.is_retryable());
        assert!(!PostgresError::RecordNotFound("test".into()).is_retryable());
    }

    #[test]
    fn test_error_concurrency() {
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
    }
}
