//! Codecs for serializing and deserializing types.
//!
//! This module provides traits and implementations for:
//! - Primitive types (integers, floats, bool, String)
//! - Identity types (IdSpan, GrainType, GrainId, SiloAddress, ActivationId, GrainAddress)

mod primitives;
mod identity;

pub use primitives::*;
pub use identity::*;

use crate::error::Result;
use crate::reader::Reader;
use crate::writer::Writer;

/// Trait for types that can be serialized to the Orleans binary format.
pub trait Serialize {
    /// Write this value to the writer.
    fn serialize(&self, writer: &mut Writer);
}

/// Trait for types that can be deserialized from the Orleans binary format.
pub trait Deserialize: Sized {
    /// Read a value from the reader.
    fn deserialize(reader: &mut Reader) -> Result<Self>;
}

/// Trait for types that can be serialized as a field.
pub trait FieldSerialize {
    /// Write this value as a field with the given field ID.
    fn serialize_field(&self, writer: &mut Writer, field_id: u32);
}

/// Trait for types that can be deserialized from a field.
pub trait FieldDeserialize: Sized {
    /// Read this value from the current field.
    /// The field header has already been read.
    fn deserialize_field(reader: &mut Reader) -> Result<Self>;
}
