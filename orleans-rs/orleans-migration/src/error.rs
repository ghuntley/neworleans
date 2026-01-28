//! Error types for grain migration operations.
//!
//! This module defines the error hierarchy for migration operations,
//! including dehydration, rehydration, and state transfer failures.

use std::fmt;
use thiserror::Error;

use orleans_core::{ActivationId, GrainId, SiloAddress};

/// Result type alias for migration operations.
pub type MigrationResult<T> = Result<T, MigrationError>;

/// Errors that can occur during grain migration.
#[derive(Debug, Error)]
pub enum MigrationError {
    /// The grain is marked as immovable and cannot be migrated.
    #[error("grain {0} is marked as immovable and cannot be migrated")]
    GrainImmovable(GrainId),

    /// The grain is currently executing a request and cannot be migrated.
    #[error("grain {0} is currently executing and cannot be migrated")]
    GrainBusy(GrainId),

    /// The grain activation was not found on this silo.
    #[error("activation {activation_id} for grain {grain_id} not found")]
    ActivationNotFound {
        grain_id: GrainId,
        activation_id: ActivationId,
    },

    /// The target silo is not available for migration.
    #[error("target silo is not available")]
    TargetSiloUnavailable(SiloAddress),

    /// The target silo rejected the migration.
    #[error("target silo {silo} rejected migration: {reason}")]
    MigrationRejected { silo: SiloAddress, reason: String },

    /// Dehydration failed - could not serialize grain state.
    #[error("dehydration failed for grain {grain_id}: {reason}")]
    DehydrationFailed { grain_id: GrainId, reason: String },

    /// Rehydration failed - could not deserialize grain state.
    #[error("rehydration failed for grain {grain_id}: {reason}")]
    RehydrationFailed { grain_id: GrainId, reason: String },

    /// A required migration context key was not found.
    #[error("migration context key '{key}' not found")]
    ContextKeyNotFound { key: String },

    /// Type mismatch when retrieving a value from migration context.
    #[error("type mismatch for key '{key}': expected {expected}, got {actual}")]
    ContextTypeMismatch {
        key: String,
        expected: String,
        actual: String,
    },

    /// State transfer between silos failed.
    #[error("state transfer failed from {source_silo} to {target_silo}: {reason}")]
    StateTransferFailed {
        source_silo: SiloAddress,
        target_silo: SiloAddress,
        reason: String,
    },

    /// Migration timed out.
    #[error("migration timed out after {duration_ms}ms")]
    Timeout { duration_ms: u64 },

    /// The migration was cancelled.
    #[error("migration was cancelled")]
    Cancelled,

    /// The migration manager is shutting down.
    #[error("migration manager is shutting down")]
    ShuttingDown,

    /// The grain is already being migrated.
    #[error("grain {0} is already being migrated")]
    AlreadyMigrating(GrainId),

    /// Directory update failed during migration.
    #[error("directory update failed: {0}")]
    DirectoryUpdateFailed(String),

    /// Serialization error during migration.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error during migration.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl MigrationError {
    /// Returns true if this error indicates a transient failure that may succeed on retry.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            MigrationError::GrainBusy(_)
                | MigrationError::TargetSiloUnavailable(_)
                | MigrationError::StateTransferFailed { .. }
                | MigrationError::Timeout { .. }
        )
    }

    /// Returns true if this error indicates the grain cannot be migrated.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            MigrationError::GrainImmovable(_)
                | MigrationError::ActivationNotFound { .. }
                | MigrationError::ContextTypeMismatch { .. }
        )
    }

    /// Returns true if this error is related to serialization.
    pub fn is_serialization_error(&self) -> bool {
        matches!(
            self,
            MigrationError::DehydrationFailed { .. }
                | MigrationError::RehydrationFailed { .. }
                | MigrationError::Serialization(_)
                | MigrationError::Deserialization(_)
        )
    }
}

/// Reason why a migration was initiated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MigrationReason {
    /// Silo is shutting down gracefully.
    SiloShutdown,
    /// Cluster rebalancing due to membership change.
    Rebalancing,
    /// Manual migration request.
    Manual,
    /// Resource optimization (memory pressure, CPU load).
    ResourceOptimization,
    /// Version upgrade - grain needs to move to silo with newer version.
    VersionUpgrade,
}

impl fmt::Display for MigrationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationReason::SiloShutdown => write!(f, "silo_shutdown"),
            MigrationReason::Rebalancing => write!(f, "rebalancing"),
            MigrationReason::Manual => write!(f, "manual"),
            MigrationReason::ResourceOptimization => write!(f, "resource_optimization"),
            MigrationReason::VersionUpgrade => write!(f, "version_upgrade"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_grain_id(key: &str) -> GrainId {
        use orleans_core::{GrainType, IdSpan};
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(key))
    }

    fn make_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    #[test]
    fn test_error_display() {
        let grain_id = make_grain_id("test");
        let error = MigrationError::GrainImmovable(grain_id.clone());
        assert!(error.to_string().contains("immovable"));

        let error = MigrationError::GrainBusy(grain_id);
        assert!(error.to_string().contains("executing"));
    }

    #[test]
    fn test_is_retryable() {
        let grain_id = make_grain_id("test");
        let silo = make_silo_address(11111);

        assert!(MigrationError::GrainBusy(grain_id.clone()).is_retryable());
        assert!(MigrationError::TargetSiloUnavailable(silo.clone()).is_retryable());
        assert!(MigrationError::Timeout { duration_ms: 1000 }.is_retryable());

        assert!(!MigrationError::GrainImmovable(grain_id).is_retryable());
        assert!(!MigrationError::Cancelled.is_retryable());
    }

    #[test]
    fn test_is_permanent() {
        let grain_id = make_grain_id("test");
        let activation_id = ActivationId::new();

        assert!(MigrationError::GrainImmovable(grain_id.clone()).is_permanent());
        assert!(MigrationError::ActivationNotFound {
            grain_id: grain_id.clone(),
            activation_id
        }
        .is_permanent());

        assert!(!MigrationError::GrainBusy(grain_id).is_permanent());
        assert!(!MigrationError::Timeout { duration_ms: 1000 }.is_permanent());
    }

    #[test]
    fn test_is_serialization_error() {
        let grain_id = make_grain_id("test");

        assert!(MigrationError::DehydrationFailed {
            grain_id: grain_id.clone(),
            reason: "test".to_string()
        }
        .is_serialization_error());
        assert!(MigrationError::RehydrationFailed {
            grain_id,
            reason: "test".to_string()
        }
        .is_serialization_error());
        assert!(MigrationError::Serialization("test".to_string()).is_serialization_error());

        assert!(!MigrationError::Cancelled.is_serialization_error());
    }

    #[test]
    fn test_migration_reason_display() {
        assert_eq!(MigrationReason::SiloShutdown.to_string(), "silo_shutdown");
        assert_eq!(MigrationReason::Rebalancing.to_string(), "rebalancing");
        assert_eq!(MigrationReason::Manual.to_string(), "manual");
        assert_eq!(
            MigrationReason::ResourceOptimization.to_string(),
            "resource_optimization"
        );
        assert_eq!(MigrationReason::VersionUpgrade.to_string(), "version_upgrade");
    }

    #[test]
    fn test_state_transfer_failed_error() {
        let source = make_silo_address(11111);
        let target = make_silo_address(22222);

        let error = MigrationError::StateTransferFailed {
            source_silo: source.clone(),
            target_silo: target.clone(),
            reason: "network timeout".to_string(),
        };

        assert!(error.is_retryable());
        assert!(!error.is_permanent());
        let display = error.to_string();
        assert!(display.contains("state transfer failed"));
        assert!(display.contains("network timeout"));
    }

    #[test]
    fn test_context_key_not_found() {
        let error = MigrationError::ContextKeyNotFound {
            key: "my_state".to_string(),
        };

        assert!(!error.is_retryable());
        assert!(!error.is_permanent());
        assert!(error.to_string().contains("my_state"));
    }

    #[test]
    fn test_context_type_mismatch() {
        let error = MigrationError::ContextTypeMismatch {
            key: "counter".to_string(),
            expected: "i32".to_string(),
            actual: "String".to_string(),
        };

        assert!(error.is_permanent());
        let display = error.to_string();
        assert!(display.contains("counter"));
        assert!(display.contains("i32"));
        assert!(display.contains("String"));
    }
}
