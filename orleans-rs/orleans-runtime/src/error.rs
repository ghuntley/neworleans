//! Error types for the grain runtime.

use orleans_core::{ActivationId, GrainId, SiloAddress};
use thiserror::Error;

/// Result type for grain runtime operations.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Errors that can occur during grain runtime operations.
#[derive(Error, Debug)]
pub enum RuntimeError {
    /// The grain activation was not found in the catalog.
    #[error("activation not found for grain {grain_id}")]
    ActivationNotFound { grain_id: GrainId },

    /// The grain activation is in an invalid state for the requested operation.
    #[error("activation {activation_id} is in invalid state: {state}")]
    InvalidActivationState {
        activation_id: ActivationId,
        state: String,
    },

    /// Failed to create a new activation.
    #[error("failed to create activation for grain {grain_id}: {reason}")]
    ActivationCreationFailed { grain_id: GrainId, reason: String },

    /// The grain method was not found.
    #[error("method {method_id} not found on interface {interface_type}")]
    MethodNotFound { interface_type: String, method_id: u32 },

    /// The grain activation is being deactivated.
    #[error("activation {activation_id} is being deactivated")]
    ActivationDeactivating { activation_id: ActivationId },

    /// The grain type is not registered.
    #[error("grain type {grain_type} is not registered")]
    GrainTypeNotRegistered { grain_type: String },

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("deserialization error: {0}")]
    Deserialization(String),

    /// The request timed out.
    #[error("request timed out after {duration_ms}ms")]
    Timeout { duration_ms: u64 },

    /// The target silo is unavailable.
    #[error("silo {silo_address} is unavailable")]
    SiloUnavailable { silo_address: SiloAddress },

    /// Messaging error.
    #[error("messaging error: {0}")]
    Messaging(#[from] orleans_messaging::MessagingError),

    /// Directory error.
    #[error("directory error: {0}")]
    Directory(#[from] orleans_directory::DirectoryError),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}
