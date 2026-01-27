//! Error types for Orleans messaging.

use thiserror::Error;

use crate::message::RejectionType;

/// Errors that can occur during messaging operations.
#[derive(Error, Debug)]
pub enum MessagingError {
    /// The message is too large.
    #[error("Message too large: {size} bytes (max: {max_size})")]
    MessageTooLarge { size: usize, max_size: usize },

    /// The frame is incomplete.
    #[error("Incomplete frame: need {needed} bytes, have {available}")]
    IncompleteFrame { needed: usize, available: usize },

    /// Failed to bind to the specified address.
    #[error("Failed to bind: {0}")]
    BindFailed(String),

    /// Failed to connect to the remote silo.
    #[error("Failed to connect: {0}")]
    ConnectionFailed(String),

    /// Connection timed out.
    #[error("Connection timed out")]
    ConnectionTimeout,

    /// The connection was closed.
    #[error("Connection closed")]
    ConnectionClosed,

    /// No target silo specified in the message.
    #[error("No target silo specified")]
    NoTargetSilo,

    /// Invalid message type for the operation.
    #[error("Invalid message type: {0}")]
    InvalidMessageType(String),

    /// The request timed out.
    #[error("Request timed out")]
    RequestTimeout,

    /// The response channel was closed.
    #[error("Response channel closed")]
    ResponseChannelClosed,

    /// The request was rejected by the remote silo.
    #[error("Request rejected ({rejection_type:?}): {message}")]
    RequestRejected {
        rejection_type: RejectionType,
        message: String,
    },

    /// A required field is missing.
    #[error("Missing field: {0}")]
    MissingField(String),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] orleans_serialization::SerializationError),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}
