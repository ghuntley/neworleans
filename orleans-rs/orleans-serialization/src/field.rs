//! Field header structure.
//!
//! The field header is a single byte that encodes:
//! - Wire type (bits 5-7): How the value is encoded
//! - Schema type (bits 3-4): How type information is encoded
//! - Field ID delta (bits 0-2): Delta from previous field ID
//!
//! ```text
//! Field Header (1 byte):
//! ┌─────────────┬─────────────┬───────────────┐
//! │ WireType(3) │ Schema(2)   │ FieldIdDelta(3)│
//! │ bits 7-5    │ bits 4-3    │ bits 2-0      │
//! └─────────────┴─────────────┴───────────────┘
//! ```
//!
//! If FieldIdDelta is 7, an extended VarInt field ID follows.

use crate::error::Result;
use crate::wire_type::{SchemaType, WireType};

/// Maximum field ID delta that fits in the 3-bit inline field.
pub const MAX_INLINE_FIELD_ID_DELTA: u32 = 6;

/// Sentinel value indicating extended field ID follows.
pub const EXTENDED_FIELD_ID_DELTA: u8 = 7;

/// Parsed field header information.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    /// How the field value is encoded.
    pub wire_type: WireType,
    /// How type information is encoded.
    pub schema_type: SchemaType,
    /// Field ID delta from previous field.
    pub field_id_delta: u32,
}

impl Field {
    /// Create a new field header.
    pub fn new(wire_type: WireType, schema_type: SchemaType, field_id_delta: u32) -> Self {
        Self {
            wire_type,
            schema_type,
            field_id_delta,
        }
    }

    /// Parse a field header from a byte, with optional extended field ID.
    pub fn from_header(header: u8, extended_delta: Option<u32>) -> Result<Self> {
        let wire_type = WireType::from_header(header)?;
        let schema_type = SchemaType::from_header(header)?;

        let inline_delta = header & 0b111;
        let field_id_delta = if inline_delta == EXTENDED_FIELD_ID_DELTA {
            extended_delta.unwrap_or(0)
        } else {
            inline_delta as u32
        };

        Ok(Self {
            wire_type,
            schema_type,
            field_id_delta,
        })
    }

    /// Encode this field header to bytes.
    ///
    /// Returns (header_byte, extended_delta) where extended_delta is Some
    /// if the field ID delta doesn't fit in the inline field.
    pub fn encode(&self) -> (u8, Option<u32>) {
        let wire_bits = self.wire_type.to_header_bits();
        let schema_bits = self.schema_type.to_header_bits();

        let (delta_bits, extended) = if self.field_id_delta <= MAX_INLINE_FIELD_ID_DELTA {
            (self.field_id_delta as u8, None)
        } else {
            (EXTENDED_FIELD_ID_DELTA, Some(self.field_id_delta))
        };

        let header = wire_bits | schema_bits | delta_bits;
        (header, extended)
    }

    /// Check if this field marks the end of a tag-delimited structure.
    pub fn is_end_tag(&self) -> bool {
        self.wire_type == WireType::Extended && self.field_id_delta == 0
    }

    /// Check if this field marks the end of base class fields.
    pub fn is_end_base_fields(&self) -> bool {
        self.wire_type == WireType::Extended && self.field_id_delta == 1
    }

    /// Create an end tag field.
    pub fn end_tag() -> Self {
        Self {
            wire_type: WireType::Extended,
            schema_type: SchemaType::Expected,
            field_id_delta: 0,
        }
    }

    /// Create an end base fields marker.
    pub fn end_base_fields() -> Self {
        Self {
            wire_type: WireType::Extended,
            schema_type: SchemaType::Expected,
            field_id_delta: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_encode_inline_delta() {
        let field = Field::new(WireType::VarInt, SchemaType::Expected, 3);
        let (header, extended) = field.encode();

        assert_eq!(header & 0b111, 3); // Delta in low 3 bits
        assert!(extended.is_none());
    }

    #[test]
    fn test_field_encode_extended_delta() {
        let field = Field::new(WireType::VarInt, SchemaType::Expected, 100);
        let (header, extended) = field.encode();

        assert_eq!(header & 0b111, 7); // Sentinel value
        assert_eq!(extended, Some(100));
    }

    #[test]
    fn test_field_roundtrip() {
        let test_cases = [
            Field::new(WireType::VarInt, SchemaType::Expected, 0),
            Field::new(WireType::TagDelimited, SchemaType::WellKnown, 5),
            Field::new(WireType::LengthPrefixed, SchemaType::Encoded, 6),
            Field::new(WireType::Fixed64, SchemaType::Referenced, 100),
            Field::new(WireType::Extended, SchemaType::Expected, 0),
        ];

        for original in test_cases {
            let (header, extended) = original.encode();
            let recovered = Field::from_header(header, extended).unwrap();
            assert_eq!(original, recovered, "Roundtrip failed for {:?}", original);
        }
    }

    #[test]
    fn test_end_tag() {
        let end_tag = Field::end_tag();
        assert!(end_tag.is_end_tag());
        assert!(!end_tag.is_end_base_fields());

        let (header, extended) = end_tag.encode();
        assert_eq!(header, 0b11100000); // Extended wire type, delta 0
        assert!(extended.is_none());
    }

    #[test]
    fn test_end_base_fields() {
        let end_base = Field::end_base_fields();
        assert!(!end_base.is_end_tag());
        assert!(end_base.is_end_base_fields());

        let (header, extended) = end_base.encode();
        assert_eq!(header, 0b11100001); // Extended wire type, delta 1
        assert!(extended.is_none());
    }

    #[test]
    fn test_wire_type_bits() {
        // Verify wire type is in bits 5-7
        let field = Field::new(WireType::Fixed64, SchemaType::Expected, 0);
        let (header, _) = field.encode();
        assert_eq!((header >> 5) & 0b111, WireType::Fixed64 as u8);
    }

    #[test]
    fn test_schema_type_bits() {
        // Verify schema type is in bits 3-4
        let field = Field::new(WireType::VarInt, SchemaType::Referenced, 0);
        let (header, _) = field.encode();
        assert_eq!((header >> 3) & 0b11, SchemaType::Referenced as u8);
    }
}
