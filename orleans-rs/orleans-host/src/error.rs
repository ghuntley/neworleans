//! Error types for the silo host.

use thiserror::Error;

/// Errors that can occur in the silo host.
#[derive(Error, Debug)]
pub enum SiloError {
    /// Silo is not in the expected state.
    #[error("Silo is not in the expected state: expected {expected}, actual {actual}")]
    InvalidState { expected: String, actual: String },

    /// Silo startup failed.
    #[error("Silo startup failed: {0}")]
    StartupFailed(String),

    /// Silo shutdown failed.
    #[error("Silo shutdown failed: {0}")]
    ShutdownFailed(String),

    /// No grain types registered.
    #[error("No grain types registered - at least one grain type must be registered")]
    NoGrainTypesRegistered,

    /// Messaging error.
    #[error("Messaging error: {0}")]
    Messaging(#[from] orleans_messaging::MessagingError),

    /// Membership error.
    #[error("Membership error: {0}")]
    Membership(#[from] orleans_clustering::MembershipError),

    /// Directory error.
    #[error("Directory error: {0}")]
    Directory(#[from] orleans_directory::DirectoryError),

    /// Runtime error.
    #[error("Runtime error: {0}")]
    Runtime(#[from] orleans_runtime::RuntimeError),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for silo operations.
pub type SiloResult<T> = Result<T, SiloError>;
