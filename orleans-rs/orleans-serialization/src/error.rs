//! Serialization error types.

use thiserror::Error;

/// Errors that can occur during serialization or deserialization.
#[derive(Debug, Error)]
pub enum SerializationError {
    /// End of input reached unexpectedly.
    #[error("Unexpected end of input")]
    UnexpectedEndOfInput,

    /// Invalid wire type encountered.
    #[error("Invalid wire type: {0}")]
    InvalidWireType(u8),

    /// Invalid schema type encountered.
    #[error("Invalid schema type: {0}")]
    InvalidSchemaType(u8),

    /// Invalid VarInt encoding.
    #[error("Invalid VarInt encoding: too many bytes")]
    InvalidVarInt,

    /// Invalid UTF-8 string.
    #[error("Invalid UTF-8 string: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),

    /// Buffer overflow.
    #[error("Buffer overflow: needed {needed} bytes but only {available} available")]
    BufferOverflow { needed: usize, available: usize },

    /// Unknown field type.
    #[error("Unknown field type: {0}")]
    UnknownFieldType(String),

    /// Missing required field.
    #[error("Missing required field: {0}")]
    MissingField(u32),

    /// Invalid field ID.
    #[error("Invalid field ID: {0}")]
    InvalidFieldId(u32),

    /// Type mismatch.
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

/// Result type for serialization operations.
pub type Result<T> = std::result::Result<T, SerializationError>;
