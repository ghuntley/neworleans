//! Orleans Serialization - Binary Wire Protocol
//!
//! This crate implements the Orleans binary serialization format for network communication.
//! It provides:
//! - VarInt encoding/decoding with ZigZag for signed integers
//! - Wire types and field headers
//! - Writer and Reader abstractions
//! - Primitive type codecs
//! - Identity type codecs (IdSpan, GrainType, GrainId, etc.)

mod varint;
mod wire_type;
mod field;
mod writer;
mod reader;
pub mod codecs;
mod error;

pub use varint::{read_varint, write_varint, zigzag_decode, zigzag_encode};
pub use wire_type::{ExtendedWireType, SchemaType, WireType};
pub use field::Field;
pub use writer::Writer;
pub use reader::Reader;
pub use error::{SerializationError, Result};

pub mod prelude {
    pub use super::codecs::*;
    pub use super::{
        read_varint, write_varint, zigzag_decode, zigzag_encode, ExtendedWireType, Field, Reader,
        SchemaType, SerializationError, WireType, Writer,
    };
}
