//! Codecs for primitive types.
//!
//! This module provides serialization for:
//! - Integer types (i8, i16, i32, i64, u8, u16, u32, u64)
//! - Floating point (f32, f64)
//! - Boolean
//! - String
//! - Vec<u8> (raw bytes)

use super::{Deserialize, FieldDeserialize, FieldSerialize, Serialize};
use crate::error::Result;
use crate::reader::Reader;
use crate::writer::Writer;

// ============================================================================
// Boolean
// ============================================================================

impl Serialize for bool {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u8(if *self { 1 } else { 0 });
    }
}

impl Deserialize for bool {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_u8()? != 0)
    }
}

impl FieldSerialize for bool {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_varint_field(field_id, if *self { 1 } else { 0 });
    }
}

impl FieldDeserialize for bool {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_varint()? != 0)
    }
}

// ============================================================================
// Unsigned Integers
// ============================================================================

impl Serialize for u8 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u8(*self);
    }
}

impl Deserialize for u8 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_u8()
    }
}

impl FieldSerialize for u8 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_varint_field(field_id, *self as u64);
    }
}

impl FieldDeserialize for u8 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_varint()? as u8)
    }
}

impl Serialize for u16 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u16_le(*self);
    }
}

impl Deserialize for u16 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_u16_le()
    }
}

impl FieldSerialize for u16 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_varint_field(field_id, *self as u64);
    }
}

impl FieldDeserialize for u16 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_varint()? as u16)
    }
}

impl Serialize for u32 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u32_le(*self);
    }
}

impl Deserialize for u32 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_u32_le()
    }
}

impl FieldSerialize for u32 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_varint_field(field_id, *self as u64);
    }
}

impl FieldDeserialize for u32 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_varint()? as u32)
    }
}

impl Serialize for u64 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u64_le(*self);
    }
}

impl Deserialize for u64 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_u64_le()
    }
}

impl FieldSerialize for u64 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_varint_field(field_id, *self);
    }
}

impl FieldDeserialize for u64 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        reader.read_varint()
    }
}

// ============================================================================
// Signed Integers (using ZigZag encoding)
// ============================================================================

impl Serialize for i8 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u8(*self as u8);
    }
}

impl Deserialize for i8 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_u8()? as i8)
    }
}

impl FieldSerialize for i8 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_signed_varint_field(field_id, *self as i64);
    }
}

impl FieldDeserialize for i8 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_signed_varint()? as i8)
    }
}

impl Serialize for i16 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_i32_le(*self as i32);
    }
}

impl Deserialize for i16 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_i32_le()? as i16)
    }
}

impl FieldSerialize for i16 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_signed_varint_field(field_id, *self as i64);
    }
}

impl FieldDeserialize for i16 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_signed_varint()? as i16)
    }
}

impl Serialize for i32 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_i32_le(*self);
    }
}

impl Deserialize for i32 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_i32_le()
    }
}

impl FieldSerialize for i32 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_signed_varint_field(field_id, *self as i64);
    }
}

impl FieldDeserialize for i32 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(reader.read_signed_varint()? as i32)
    }
}

impl Serialize for i64 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_i64_le(*self);
    }
}

impl Deserialize for i64 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_i64_le()
    }
}

impl FieldSerialize for i64 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_signed_varint_field(field_id, *self);
    }
}

impl FieldDeserialize for i64 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        reader.read_signed_varint()
    }
}

// ============================================================================
// Floating Point (Fixed encoding)
// ============================================================================

impl Serialize for f32 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_f32_le(*self);
    }
}

impl Deserialize for f32 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_f32_le()
    }
}

impl FieldSerialize for f32 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_fixed32_field(field_id, self.to_bits());
    }
}

impl FieldDeserialize for f32 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(f32::from_bits(reader.read_u32_le()?))
    }
}

impl Serialize for f64 {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_f64_le(*self);
    }
}

impl Deserialize for f64 {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_f64_le()
    }
}

impl FieldSerialize for f64 {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_fixed64_field(field_id, self.to_bits());
    }
}

impl FieldDeserialize for f64 {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        Ok(f64::from_bits(reader.read_u64_le()?))
    }
}

// ============================================================================
// String
// ============================================================================

impl Serialize for String {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_varint(self.len() as u64);
        writer.write_bytes(self.as_bytes());
    }
}

impl Deserialize for String {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        reader.read_string()
    }
}

impl FieldSerialize for String {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_string_field(field_id, self);
    }
}

impl FieldDeserialize for String {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        reader.read_string()
    }
}

impl FieldSerialize for str {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_string_field(field_id, self);
    }
}

// ============================================================================
// Vec<u8> (raw bytes)
// ============================================================================

impl Serialize for Vec<u8> {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_varint(self.len() as u64);
        writer.write_bytes(self);
    }
}

impl Deserialize for Vec<u8> {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let bytes = reader.read_length_prefixed()?;
        Ok(bytes.to_vec())
    }
}

impl FieldSerialize for Vec<u8> {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_length_prefixed_field(field_id, self);
    }
}

impl FieldDeserialize for Vec<u8> {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let bytes = reader.read_length_prefixed()?;
        Ok(bytes.to_vec())
    }
}

impl FieldSerialize for [u8] {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_length_prefixed_field(field_id, self);
    }
}

// ============================================================================
// Option<T>
// ============================================================================

impl<T: FieldSerialize> FieldSerialize for Option<T> {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        if let Some(value) = self {
            value.serialize_field(writer, field_id);
        }
        // None values are simply not written
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! test_field_roundtrip {
        ($type:ty, $value:expr) => {{
            let value: $type = $value;
            let mut writer = Writer::new();
            value.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = <$type>::deserialize_field(&mut reader).unwrap();
            assert_eq!(decoded, value);
        }};
    }

    #[test]
    fn test_bool_roundtrip() {
        test_field_roundtrip!(bool, true);
        test_field_roundtrip!(bool, false);
    }

    #[test]
    fn test_u8_roundtrip() {
        test_field_roundtrip!(u8, 0);
        test_field_roundtrip!(u8, 127);
        test_field_roundtrip!(u8, 255);
    }

    #[test]
    fn test_u16_roundtrip() {
        test_field_roundtrip!(u16, 0);
        test_field_roundtrip!(u16, 256);
        test_field_roundtrip!(u16, u16::MAX);
    }

    #[test]
    fn test_u32_roundtrip() {
        test_field_roundtrip!(u32, 0);
        test_field_roundtrip!(u32, 12345);
        test_field_roundtrip!(u32, u32::MAX);
    }

    #[test]
    fn test_u64_roundtrip() {
        test_field_roundtrip!(u64, 0);
        test_field_roundtrip!(u64, 12345678901234);
        test_field_roundtrip!(u64, u64::MAX);
    }

    #[test]
    fn test_i8_roundtrip() {
        test_field_roundtrip!(i8, 0);
        test_field_roundtrip!(i8, -1);
        test_field_roundtrip!(i8, i8::MIN);
        test_field_roundtrip!(i8, i8::MAX);
    }

    #[test]
    fn test_i16_roundtrip() {
        test_field_roundtrip!(i16, 0);
        test_field_roundtrip!(i16, -1);
        test_field_roundtrip!(i16, i16::MIN);
        test_field_roundtrip!(i16, i16::MAX);
    }

    #[test]
    fn test_i32_roundtrip() {
        test_field_roundtrip!(i32, 0);
        test_field_roundtrip!(i32, -1);
        test_field_roundtrip!(i32, -1000);
        test_field_roundtrip!(i32, i32::MIN);
        test_field_roundtrip!(i32, i32::MAX);
    }

    #[test]
    fn test_i64_roundtrip() {
        test_field_roundtrip!(i64, 0);
        test_field_roundtrip!(i64, -1);
        test_field_roundtrip!(i64, i64::MIN);
        test_field_roundtrip!(i64, i64::MAX);
    }

    #[test]
    fn test_f32_roundtrip() {
        test_field_roundtrip!(f32, 0.0);
        test_field_roundtrip!(f32, 1.5);
        test_field_roundtrip!(f32, -1.5);
        test_field_roundtrip!(f32, f32::MIN);
        test_field_roundtrip!(f32, f32::MAX);
    }

    #[test]
    fn test_f64_roundtrip() {
        test_field_roundtrip!(f64, 0.0);
        test_field_roundtrip!(f64, 1.5);
        test_field_roundtrip!(f64, -1.5);
        test_field_roundtrip!(f64, std::f64::consts::PI);
    }

    #[test]
    fn test_string_roundtrip() {
        test_field_roundtrip!(String, String::new());
        test_field_roundtrip!(String, "hello".to_string());
        test_field_roundtrip!(String, "hello world with spaces".to_string());
        test_field_roundtrip!(String, "unicode: \u{1F600}".to_string());
    }

    #[test]
    fn test_bytes_roundtrip() {
        test_field_roundtrip!(Vec<u8>, vec![]);
        test_field_roundtrip!(Vec<u8>, vec![1, 2, 3, 4, 5]);
        test_field_roundtrip!(Vec<u8>, vec![0xFF; 100]);
    }

    #[test]
    fn test_option_some() {
        let value: Option<u32> = Some(42);
        let mut writer = Writer::new();
        value.serialize_field(&mut writer, 1);

        let mut reader = Reader::new(writer.as_bytes());
        let _field = reader.read_field_header().unwrap();
        let decoded = u32::deserialize_field(&mut reader).unwrap();
        assert_eq!(decoded, 42);
    }

    #[test]
    fn test_option_none() {
        let value: Option<u32> = None;
        let mut writer = Writer::new();
        value.serialize_field(&mut writer, 1);

        // None should not write anything
        assert_eq!(writer.as_bytes().len(), 0);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    macro_rules! proptest_field_roundtrip {
        ($type:ty) => {
            paste::paste! {
                proptest! {
                    #[test]
                    fn [<prop_ $type:lower _roundtrip>](value: $type) {
                        let mut writer = Writer::new();
                        value.serialize_field(&mut writer, 1);

                        let mut reader = Reader::new(writer.as_bytes());
                        let _field = reader.read_field_header().unwrap();
                        let decoded = <$type>::deserialize_field(&mut reader).unwrap();
                        prop_assert_eq!(decoded, value);
                    }
                }
            }
        };
    }

    // Note: These tests require the paste crate, but we can do them manually
    proptest! {
        #[test]
        fn prop_u32_roundtrip(value: u32) {
            let mut writer = Writer::new();
            value.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = u32::deserialize_field(&mut reader).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_i64_roundtrip(value: i64) {
            let mut writer = Writer::new();
            value.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = i64::deserialize_field(&mut reader).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_string_roundtrip(value: String) {
            let mut writer = Writer::new();
            value.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = String::deserialize_field(&mut reader).unwrap();
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_bytes_roundtrip(value: Vec<u8>) {
            let mut writer = Writer::new();
            value.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = Vec::<u8>::deserialize_field(&mut reader).unwrap();
            prop_assert_eq!(decoded, value);
        }
    }
}
