//! Error types for the grain directory.

use orleans_core::{GrainAddress, GrainId, SiloAddress};
use thiserror::Error;

/// Result type for grain directory operations.
pub type DirectoryResult<T> = Result<T, DirectoryError>;

/// Errors that can occur during grain directory operations.
#[derive(Debug, Error)]
pub enum DirectoryError {
    /// No silos are available in the cluster.
    #[error("no silos available in the cluster")]
    NoSilosAvailable,

    /// The target silo is not reachable.
    #[error("silo {0} is not reachable")]
    SiloNotReachable(SiloAddress),

    /// A conflicting registration already exists.
    #[error("registration conflict for grain {grain_id}: existing {existing:?}, requested {requested:?}")]
    RegistrationConflict {
        grain_id: GrainId,
        existing: GrainAddress,
        requested: GrainAddress,
    },

    /// The grain was not found in the directory.
    #[error("grain {0} not found in directory")]
    GrainNotFound(GrainId),

    /// The operation was interrupted by a membership change.
    #[error("membership change during operation, retry required")]
    MembershipChanged,

    /// The hash ring is empty (no silos registered).
    #[error("consistent hash ring is empty")]
    EmptyRing,

    /// Internal error.
    #[error("internal directory error: {0}")]
    Internal(String),
}
