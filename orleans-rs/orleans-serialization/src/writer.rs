//! Writer for encoding Orleans binary format.
//!
//! The Writer provides methods for writing field headers, VarInt values,
//! fixed-size values, and length-prefixed data.

use crate::field::Field;
use crate::varint::{write_varint, write_varint32, zigzag_encode, zigzag_encode32, MAX_VARINT_BYTES};
use crate::wire_type::{SchemaType, WireType};
use bytes::{BufMut, BytesMut};

/// Writer for Orleans binary serialization.
///
/// Wraps a growable buffer and provides methods for writing the Orleans wire format.
pub struct Writer {
    buffer: BytesMut,
    /// Current field ID for delta encoding.
    current_field_id: u32,
}

impl Writer {
    /// Create a new writer with default capacity.
    pub fn new() -> Self {
        Self::with_capacity(1024)
    }

    /// Create a new writer with specified initial capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            current_field_id: 0,
        }
    }

    /// Get the current position (number of bytes written).
    pub fn position(&self) -> usize {
        self.buffer.len()
    }

    /// Get a reference to the written bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Consume the writer and return the buffer.
    pub fn into_bytes(self) -> BytesMut {
        self.buffer
    }

    /// Reset the writer for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.current_field_id = 0;
    }

    /// Reset just the field ID tracking (for nested structures).
    pub fn reset_field_id(&mut self) {
        self.current_field_id = 0;
    }

    // ========================================================================
    // Low-level write methods
    // ========================================================================

    /// Write a single byte.
    #[inline]
    pub fn write_u8(&mut self, value: u8) {
        self.buffer.put_u8(value);
    }

    /// Write a u16 in little-endian format.
    #[inline]
    pub fn write_u16_le(&mut self, value: u16) {
        self.buffer.put_u16_le(value);
    }

    /// Write a u32 in little-endian format.
    #[inline]
    pub fn write_u32_le(&mut self, value: u32) {
        self.buffer.put_u32_le(value);
    }

    /// Write a u64 in little-endian format.
    #[inline]
    pub fn write_u64_le(&mut self, value: u64) {
        self.buffer.put_u64_le(value);
    }

    /// Write an i32 in little-endian format.
    #[inline]
    pub fn write_i32_le(&mut self, value: i32) {
        self.buffer.put_i32_le(value);
    }

    /// Write an i64 in little-endian format.
    #[inline]
    pub fn write_i64_le(&mut self, value: i64) {
        self.buffer.put_i64_le(value);
    }

    /// Write a f32 in little-endian format.
    #[inline]
    pub fn write_f32_le(&mut self, value: f32) {
        self.buffer.put_f32_le(value);
    }

    /// Write a f64 in little-endian format.
    #[inline]
    pub fn write_f64_le(&mut self, value: f64) {
        self.buffer.put_f64_le(value);
    }

    /// Write a byte slice.
    #[inline]
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buffer.put_slice(bytes);
    }

    // ========================================================================
    // VarInt methods
    // ========================================================================

    /// Write a VarInt-encoded u64.
    pub fn write_varint(&mut self, value: u64) {
        let mut buf = [0u8; MAX_VARINT_BYTES];
        let len = write_varint(&mut buf, value);
        self.buffer.put_slice(&buf[..len]);
    }

    /// Write a VarInt-encoded u32.
    pub fn write_varint32(&mut self, value: u32) {
        let mut buf = [0u8; 5];
        let len = write_varint32(&mut buf, value);
        self.buffer.put_slice(&buf[..len]);
    }

    /// Write a ZigZag-encoded signed VarInt.
    pub fn write_signed_varint(&mut self, value: i64) {
        self.write_varint(zigzag_encode(value));
    }

    /// Write a ZigZag-encoded signed VarInt32.
    pub fn write_signed_varint32(&mut self, value: i32) {
        self.write_varint32(zigzag_encode32(value));
    }

    // ========================================================================
    // Field header methods
    // ========================================================================

    /// Write a field header.
    pub fn write_field_header(&mut self, field: &Field) {
        let (header, extended) = field.encode();
        self.write_u8(header);
        if let Some(delta) = extended {
            self.write_varint32(delta);
        }
    }

    /// Write a field header with automatic delta calculation.
    ///
    /// This method tracks the current field ID and automatically calculates
    /// the delta from the previous field.
    pub fn write_field_header_with_id(
        &mut self,
        field_id: u32,
        wire_type: WireType,
        schema_type: SchemaType,
    ) {
        let delta = field_id - self.current_field_id;
        self.current_field_id = field_id;

        let field = Field::new(wire_type, schema_type, delta);
        self.write_field_header(&field);
    }

    /// Write an expected-type VarInt field.
    pub fn write_varint_field(&mut self, field_id: u32, value: u64) {
        self.write_field_header_with_id(field_id, WireType::VarInt, SchemaType::Expected);
        self.write_varint(value);
    }

    /// Write an expected-type signed VarInt field.
    pub fn write_signed_varint_field(&mut self, field_id: u32, value: i64) {
        self.write_field_header_with_id(field_id, WireType::VarInt, SchemaType::Expected);
        self.write_signed_varint(value);
    }

    /// Write an expected-type Fixed32 field.
    pub fn write_fixed32_field(&mut self, field_id: u32, value: u32) {
        self.write_field_header_with_id(field_id, WireType::Fixed32, SchemaType::Expected);
        self.write_u32_le(value);
    }

    /// Write an expected-type Fixed64 field.
    pub fn write_fixed64_field(&mut self, field_id: u32, value: u64) {
        self.write_field_header_with_id(field_id, WireType::Fixed64, SchemaType::Expected);
        self.write_u64_le(value);
    }

    /// Write a length-prefixed bytes field.
    pub fn write_length_prefixed_field(&mut self, field_id: u32, bytes: &[u8]) {
        self.write_field_header_with_id(field_id, WireType::LengthPrefixed, SchemaType::Expected);
        self.write_varint(bytes.len() as u64);
        self.write_bytes(bytes);
    }

    /// Write a length-prefixed string field.
    pub fn write_string_field(&mut self, field_id: u32, value: &str) {
        self.write_length_prefixed_field(field_id, value.as_bytes());
    }

    /// Begin a tag-delimited (nested object) field.
    ///
    /// After calling this, write the nested fields, then call `write_end_tag()`.
    pub fn begin_tag_delimited_field(&mut self, field_id: u32) {
        self.write_field_header_with_id(field_id, WireType::TagDelimited, SchemaType::Expected);
        // Reset field ID for nested object
        self.current_field_id = 0;
    }

    /// Write an end tag to close a tag-delimited structure.
    pub fn write_end_tag(&mut self) {
        self.write_field_header(&Field::end_tag());
    }

    /// Write an end base fields marker.
    pub fn write_end_base_fields(&mut self) {
        self.write_field_header(&Field::end_base_fields());
    }

    /// Write a reference to a previously serialized object.
    pub fn write_reference(&mut self, reference_id: u32) {
        let field = Field::new(WireType::Reference, SchemaType::Expected, 0);
        self.write_field_header(&field);
        self.write_varint32(reference_id);
    }
}

impl Default for Writer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_u8() {
        let mut writer = Writer::new();
        writer.write_u8(0x42);
        assert_eq!(writer.as_bytes(), &[0x42]);
    }

    #[test]
    fn test_write_u32_le() {
        let mut writer = Writer::new();
        writer.write_u32_le(0x12345678);
        assert_eq!(writer.as_bytes(), &[0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_write_varint_single_byte() {
        let mut writer = Writer::new();
        writer.write_varint(127);
        assert_eq!(writer.as_bytes(), &[127]);
    }

    #[test]
    fn test_write_varint_multi_byte() {
        let mut writer = Writer::new();
        writer.write_varint(300);
        // 300 = 0b100101100 = [0xAC, 0x02]
        assert_eq!(writer.as_bytes(), &[0xAC, 0x02]);
    }

    #[test]
    fn test_write_signed_varint() {
        let mut writer = Writer::new();
        writer.write_signed_varint(-1);
        // -1 zigzag encodes to 1
        assert_eq!(writer.as_bytes(), &[1]);

        writer.reset();
        writer.write_signed_varint(-2);
        // -2 zigzag encodes to 3
        assert_eq!(writer.as_bytes(), &[3]);
    }

    #[test]
    fn test_write_field_header_inline_delta() {
        let mut writer = Writer::new();
        writer.write_field_header_with_id(3, WireType::VarInt, SchemaType::Expected);

        let header = writer.as_bytes()[0];
        assert_eq!(header & 0b111, 3); // Delta
        assert_eq!((header >> 3) & 0b11, 0); // SchemaType::Expected
        assert_eq!((header >> 5) & 0b111, 0); // WireType::VarInt
    }

    #[test]
    fn test_write_field_header_extended_delta() {
        let mut writer = Writer::new();
        writer.write_field_header_with_id(100, WireType::VarInt, SchemaType::Expected);

        assert_eq!(writer.as_bytes()[0] & 0b111, 7); // Extended delta marker
        // Delta 100 follows as VarInt
        assert_eq!(writer.as_bytes()[1], 100);
    }

    #[test]
    fn test_write_field_header_delta_tracking() {
        let mut writer = Writer::new();

        // First field at ID 5
        writer.write_field_header_with_id(5, WireType::VarInt, SchemaType::Expected);
        assert_eq!(writer.as_bytes()[0] & 0b111, 5);

        // Second field at ID 7 (delta 2)
        writer.write_field_header_with_id(7, WireType::VarInt, SchemaType::Expected);
        assert_eq!(writer.as_bytes()[1] & 0b111, 2);
    }

    #[test]
    fn test_write_varint_field() {
        let mut writer = Writer::new();
        writer.write_varint_field(1, 42);

        // Header: VarInt, Expected, delta 1
        assert_eq!(writer.as_bytes()[0], 0b00000001);
        // Value: 42
        assert_eq!(writer.as_bytes()[1], 42);
    }

    #[test]
    fn test_write_length_prefixed_field() {
        let mut writer = Writer::new();
        writer.write_length_prefixed_field(1, b"hello");

        // Header: LengthPrefixed, Expected, delta 1
        assert_eq!(writer.as_bytes()[0], 0b01000001);
        // Length: 5
        assert_eq!(writer.as_bytes()[1], 5);
        // Data
        assert_eq!(&writer.as_bytes()[2..7], b"hello");
    }

    #[test]
    fn test_write_end_tag() {
        let mut writer = Writer::new();
        writer.write_end_tag();

        // Extended wire type (111), Expected schema (00), delta 0 (000)
        assert_eq!(writer.as_bytes()[0], 0b11100000);
    }

    #[test]
    fn test_nested_tag_delimited() {
        let mut writer = Writer::new();

        // Outer field 1
        writer.begin_tag_delimited_field(1);

        // Inner field 1 (note: field ID resets)
        writer.write_varint_field(1, 100);

        // Inner field 2
        writer.write_varint_field(2, 200);

        // Close
        writer.write_end_tag();

        // 1 (tag delimited header) + 1+1 (field 1 header + 100) + 1+2 (field 2 header + 200) + 1 (end tag)
        // 200 encodes as 2 bytes in VarInt: 0xC8, 0x01
        assert_eq!(writer.as_bytes().len(), 7);
    }

    #[test]
    fn test_reset() {
        let mut writer = Writer::new();
        writer.write_varint(42);
        writer.write_field_header_with_id(5, WireType::VarInt, SchemaType::Expected);

        writer.reset();

        assert_eq!(writer.position(), 0);
        assert_eq!(writer.as_bytes().len(), 0);

        // Field ID should be reset too
        writer.write_field_header_with_id(5, WireType::VarInt, SchemaType::Expected);
        assert_eq!(writer.as_bytes()[0] & 0b111, 5);
    }
}
