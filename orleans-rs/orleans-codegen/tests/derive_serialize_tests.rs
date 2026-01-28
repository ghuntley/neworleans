//! Tests for OrleansSerialize and OrleansDeserialize derive macros.

use orleans_codegen::{OrleansDeserialize, OrleansSerialize};
use orleans_serialization::codecs::{FieldDeserialize, FieldSerialize};
use orleans_serialization::{Reader, Writer};

// ============================================================================
// Basic struct tests
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct SimpleStruct {
    #[id(0)]
    pub value: u32,
}

#[test]
fn test_simple_struct_roundtrip() {
    let original = SimpleStruct { value: 42 };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = SimpleStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Multiple fields test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct MultiFieldStruct {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub age: u32,
    #[id(2)]
    pub score: i64,
}

#[test]
fn test_multi_field_struct_roundtrip() {
    let original = MultiFieldStruct {
        name: "Alice".to_string(),
        age: 30,
        score: -100,
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = MultiFieldStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Auto-assigned field IDs test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct AutoIdStruct {
    pub a: u32,
    pub b: String,
    pub c: bool,
}

#[test]
fn test_auto_id_struct_roundtrip() {
    let original = AutoIdStruct {
        a: 123,
        b: "test".to_string(),
        c: true,
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = AutoIdStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Nested struct test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct InnerStruct {
    #[id(0)]
    pub value: i32,
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct OuterStruct {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub inner: InnerStruct,
}

#[test]
fn test_nested_struct_roundtrip() {
    let original = OuterStruct {
        name: "outer".to_string(),
        inner: InnerStruct { value: -42 },
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = OuterStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// All primitive types test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct AllPrimitivesStruct {
    #[id(0)]
    pub bool_val: bool,
    #[id(1)]
    pub u8_val: u8,
    #[id(2)]
    pub u16_val: u16,
    #[id(3)]
    pub u32_val: u32,
    #[id(4)]
    pub u64_val: u64,
    #[id(5)]
    pub i8_val: i8,
    #[id(6)]
    pub i16_val: i16,
    #[id(7)]
    pub i32_val: i32,
    #[id(8)]
    pub i64_val: i64,
    #[id(9)]
    pub string_val: String,
}

#[test]
fn test_all_primitives_roundtrip() {
    let original = AllPrimitivesStruct {
        bool_val: true,
        u8_val: 255,
        u16_val: 65535,
        u32_val: u32::MAX,
        u64_val: u64::MAX,
        i8_val: i8::MIN,
        i16_val: i16::MIN,
        i32_val: i32::MIN,
        i64_val: i64::MIN,
        string_val: "hello world".to_string(),
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = AllPrimitivesStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Empty struct test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct EmptyStruct {}

#[test]
fn test_empty_struct_roundtrip() {
    let original = EmptyStruct {};

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = EmptyStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Unit struct test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct UnitStruct;

#[test]
fn test_unit_struct_roundtrip() {
    let original = UnitStruct;

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = UnitStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Bytes field test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct BytesStruct {
    #[id(0)]
    pub data: Vec<u8>,
}

#[test]
fn test_bytes_roundtrip() {
    let original = BytesStruct {
        data: vec![1, 2, 3, 4, 5, 255, 0],
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = BytesStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Non-sequential field IDs test
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct SparseIdStruct {
    #[id(0)]
    pub first: u32,
    #[id(10)]
    pub second: String,
    #[id(100)]
    pub third: i64,
}

#[test]
fn test_sparse_ids_roundtrip() {
    let original = SparseIdStruct {
        first: 1,
        second: "sparse".to_string(),
        third: -999,
    };

    let mut writer = Writer::new();
    original.serialize_field(&mut writer, 1);

    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = SparseIdStruct::deserialize_field(&mut reader).unwrap();

    assert_eq!(original, decoded);
}

// ============================================================================
// Forward compatibility test (unknown fields are skipped)
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct VersionedStructV2 {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub value: u32,
    #[id(2)]
    pub new_field: String, // New field added in V2
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct VersionedStructV1 {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub value: u32,
}

#[test]
fn test_forward_compatibility_skips_unknown_fields() {
    // Serialize V2
    let v2 = VersionedStructV2 {
        name: "test".to_string(),
        value: 42,
        new_field: "new data".to_string(),
    };

    let mut writer = Writer::new();
    v2.serialize_field(&mut writer, 1);

    // Deserialize as V1 (should skip the unknown field)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = VersionedStructV1::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.name, "test");
    assert_eq!(decoded.value, 42);
}

// ============================================================================
// Property-based tests
// ============================================================================

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_simple_struct_roundtrip(value: u32) {
            let original = SimpleStruct { value };

            let mut writer = Writer::new();
            original.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = SimpleStruct::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(original, decoded);
        }

        #[test]
        fn prop_multi_field_struct_roundtrip(
            name in "\\PC{0,50}",
            age: u32,
            score: i64,
        ) {
            let original = MultiFieldStruct { name, age, score };

            let mut writer = Writer::new();
            original.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = MultiFieldStruct::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(original, decoded);
        }

        #[test]
        fn prop_all_primitives_roundtrip(
            bool_val: bool,
            u8_val: u8,
            u16_val: u16,
            u32_val: u32,
            u64_val: u64,
            i8_val: i8,
            i16_val: i16,
            i32_val: i32,
            i64_val: i64,
            string_val in "\\PC{0,100}",
        ) {
            let original = AllPrimitivesStruct {
                bool_val,
                u8_val,
                u16_val,
                u32_val,
                u64_val,
                i8_val,
                i16_val,
                i32_val,
                i64_val,
                string_val,
            };

            let mut writer = Writer::new();
            original.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = AllPrimitivesStruct::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(original, decoded);
        }

        #[test]
        fn prop_bytes_roundtrip(data: Vec<u8>) {
            let original = BytesStruct { data };

            let mut writer = Writer::new();
            original.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = BytesStruct::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(original, decoded);
        }
    }
}
