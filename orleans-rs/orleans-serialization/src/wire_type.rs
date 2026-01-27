//! Wire type definitions for the Orleans binary protocol.
//!
//! The wire type determines how a field value is encoded on the wire.

use crate::error::{Result, SerializationError};

/// Wire type indicating how a field value is encoded.
///
/// The wire type occupies the high 3 bits of the field header byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    /// Variable-length integer (1-10 bytes depending on value).
    VarInt = 0,
    /// Tag-delimited compound object (fields until end tag).
    TagDelimited = 1,
    /// Length-prefixed byte sequence.
    LengthPrefixed = 2,
    /// Fixed 4-byte value.
    Fixed32 = 3,
    /// Fixed 8-byte value.
    Fixed64 = 4,
    /// Reference to a previously serialized object.
    Reference = 6,
    /// Extended/control tag (end markers).
    Extended = 7,
}

impl WireType {
    /// The bit mask for extracting wire type from field header.
    pub const MASK: u8 = 0b111;

    /// The bit position of wire type in field header (bits 5-7).
    pub const SHIFT: u8 = 5;

    /// Create a WireType from the raw header byte.
    pub fn from_header(header: u8) -> Result<Self> {
        let bits = (header >> Self::SHIFT) & Self::MASK;
        Self::from_bits(bits)
    }

    /// Create a WireType from its 3-bit representation.
    pub fn from_bits(bits: u8) -> Result<Self> {
        match bits {
            0 => Ok(WireType::VarInt),
            1 => Ok(WireType::TagDelimited),
            2 => Ok(WireType::LengthPrefixed),
            3 => Ok(WireType::Fixed32),
            4 => Ok(WireType::Fixed64),
            6 => Ok(WireType::Reference),
            7 => Ok(WireType::Extended),
            n => Err(SerializationError::InvalidWireType(n)),
        }
    }

    /// Convert to bits for encoding in header.
    pub fn to_bits(self) -> u8 {
        self as u8
    }

    /// Encode into the header byte position.
    pub fn to_header_bits(self) -> u8 {
        (self as u8) << Self::SHIFT
    }
}

/// Schema type indicating how the field's type information is encoded.
///
/// The schema type occupies bits 3-4 of the field header byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SchemaType {
    /// Type matches expected/default type (no type info encoded).
    Expected = 0,
    /// Type is in well-known collection (1 byte ID follows).
    WellKnown = 1,
    /// Type string is encoded inline.
    Encoded = 2,
    /// Type reference ID (VarInt follows).
    Referenced = 3,
}

impl SchemaType {
    /// The bit mask for extracting schema type.
    pub const MASK: u8 = 0b11;

    /// The bit position of schema type in field header (bits 3-4).
    pub const SHIFT: u8 = 3;

    /// Create a SchemaType from the raw header byte.
    pub fn from_header(header: u8) -> Result<Self> {
        let bits = (header >> Self::SHIFT) & Self::MASK;
        Self::from_bits(bits)
    }

    /// Create a SchemaType from its 2-bit representation.
    pub fn from_bits(bits: u8) -> Result<Self> {
        match bits {
            0 => Ok(SchemaType::Expected),
            1 => Ok(SchemaType::WellKnown),
            2 => Ok(SchemaType::Encoded),
            3 => Ok(SchemaType::Referenced),
            n => Err(SerializationError::InvalidSchemaType(n)),
        }
    }

    /// Convert to bits for encoding in header.
    pub fn to_bits(self) -> u8 {
        self as u8
    }

    /// Encode into the header byte position.
    pub fn to_header_bits(self) -> u8 {
        (self as u8) << Self::SHIFT
    }
}

/// Extended wire type markers for control tags.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExtendedWireType {
    /// End of tag-delimited structure.
    EndTagDelimited = 0,
    /// End of base class fields (before derived class fields).
    EndBaseFields = 1,
}

impl ExtendedWireType {
    /// Create an ExtendedWireType from the field ID delta.
    pub fn from_field_id_delta(delta: u8) -> Option<Self> {
        match delta {
            0 => Some(ExtendedWireType::EndTagDelimited),
            1 => Some(ExtendedWireType::EndBaseFields),
            _ => None,
        }
    }

    /// Get the field ID delta value for this extended type.
    pub fn to_field_id_delta(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wire_type_roundtrip() {
        for wire_type in [
            WireType::VarInt,
            WireType::TagDelimited,
            WireType::LengthPrefixed,
            WireType::Fixed32,
            WireType::Fixed64,
            WireType::Reference,
            WireType::Extended,
        ] {
            let bits = wire_type.to_bits();
            let recovered = WireType::from_bits(bits).unwrap();
            assert_eq!(wire_type, recovered);
        }
    }

    #[test]
    fn test_wire_type_header_encoding() {
        // VarInt at position 5-7 should produce 0x00
        assert_eq!(WireType::VarInt.to_header_bits(), 0b00000000);
        // TagDelimited should produce 0x20
        assert_eq!(WireType::TagDelimited.to_header_bits(), 0b00100000);
        // Extended should produce 0xE0
        assert_eq!(WireType::Extended.to_header_bits(), 0b11100000);
    }

    #[test]
    fn test_schema_type_roundtrip() {
        for schema_type in [
            SchemaType::Expected,
            SchemaType::WellKnown,
            SchemaType::Encoded,
            SchemaType::Referenced,
        ] {
            let bits = schema_type.to_bits();
            let recovered = SchemaType::from_bits(bits).unwrap();
            assert_eq!(schema_type, recovered);
        }
    }

    #[test]
    fn test_schema_type_header_encoding() {
        // Expected at bits 3-4 should produce 0x00
        assert_eq!(SchemaType::Expected.to_header_bits(), 0b00000000);
        // Referenced should produce 0x18
        assert_eq!(SchemaType::Referenced.to_header_bits(), 0b00011000);
    }

    #[test]
    fn test_invalid_wire_type() {
        assert!(WireType::from_bits(5).is_err());
    }

    #[test]
    fn test_extended_wire_type() {
        assert_eq!(
            ExtendedWireType::from_field_id_delta(0),
            Some(ExtendedWireType::EndTagDelimited)
        );
        assert_eq!(
            ExtendedWireType::from_field_id_delta(1),
            Some(ExtendedWireType::EndBaseFields)
        );
        assert_eq!(ExtendedWireType::from_field_id_delta(2), None);
    }
}
