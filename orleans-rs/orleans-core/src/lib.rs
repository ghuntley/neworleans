//! Orleans Core - Identity Types
//!
//! This crate provides the fundamental identity types for the Orleans Rust port:
//! - `IdSpan` - UTF-8 byte array with pre-computed XxHash32
//! - `GrainType` - Type identifier for grain classes
//! - `GrainId` - Composite identifier (GrainType + IdSpan)
//! - `SiloAddress` - Silo endpoint with generation number
//! - `ActivationId` - Unique identifier for grain activations
//! - `GrainAddress` - Complete grain location

mod id_span;
mod grain_type;
mod grain_id;
mod silo_address;
mod activation_id;
mod grain_address;

pub use id_span::IdSpan;
pub use grain_type::GrainType;
pub use grain_id::GrainId;
pub use silo_address::SiloAddress;
pub use activation_id::ActivationId;
pub use grain_address::GrainAddress;

/// Error types for Orleans core operations
#[derive(Debug, thiserror::Error)]
pub enum OrleansError {
    #[error("Invalid grain ID format: {0}")]
    InvalidGrainId(String),

    #[error("Invalid silo address format: {0}")]
    InvalidSiloAddress(String),

    #[error("Parse error: {0}")]
    ParseError(String),
}
