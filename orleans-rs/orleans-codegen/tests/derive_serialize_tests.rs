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
// Version Tolerance Tests - Forward Compatibility (unknown fields are skipped)
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
// Version Tolerance Tests - Backward Compatibility (missing fields get defaults)
// ============================================================================

#[test]
fn test_backward_compatibility_missing_fields_get_defaults() {
    // Serialize V1 (older version)
    let v1 = VersionedStructV1 {
        name: "old data".to_string(),
        value: 123,
    };

    let mut writer = Writer::new();
    v1.serialize_field(&mut writer, 1);

    // Deserialize as V2 (newer version - should get default for missing field)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = VersionedStructV2::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.name, "old data");
    assert_eq!(decoded.value, 123);
    assert_eq!(decoded.new_field, String::default()); // Should be empty string (default)
}

// ============================================================================
// Version Tolerance Tests - Multiple unknown fields
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct VersionedStructV3 {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub value: u32,
    #[id(2)]
    pub field_a: String,
    #[id(3)]
    pub field_b: i64,
    #[id(4)]
    pub field_c: bool,
    #[id(5)]
    pub field_d: Vec<u8>,
}

#[test]
fn test_forward_compatibility_skips_multiple_unknown_fields() {
    // Serialize V3 with many fields
    let v3 = VersionedStructV3 {
        name: "complex".to_string(),
        value: 999,
        field_a: "extra a".to_string(),
        field_b: -12345,
        field_c: true,
        field_d: vec![1, 2, 3, 4, 5],
    };

    let mut writer = Writer::new();
    v3.serialize_field(&mut writer, 1);

    // Deserialize as V1 (should skip all extra fields)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = VersionedStructV1::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.name, "complex");
    assert_eq!(decoded.value, 999);
}

// ============================================================================
// Version Tolerance Tests - Sparse field IDs with gaps
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct SparseFieldsOld {
    #[id(0)]
    pub a: u32,
    #[id(5)]
    pub b: String,
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct SparseFieldsNew {
    #[id(0)]
    pub a: u32,
    #[id(2)]
    pub inserted: i64,  // New field inserted in gap
    #[id(5)]
    pub b: String,
    #[id(10)]
    pub appended: bool, // New field appended
}

#[test]
fn test_forward_compatibility_with_sparse_field_ids() {
    // Serialize new version with fields in gaps
    let new_struct = SparseFieldsNew {
        a: 100,
        inserted: -500,
        b: "sparse".to_string(),
        appended: true,
    };

    let mut writer = Writer::new();
    new_struct.serialize_field(&mut writer, 1);

    // Deserialize as old version (should skip inserted and appended fields)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = SparseFieldsOld::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.a, 100);
    assert_eq!(decoded.b, "sparse");
}

#[test]
fn test_backward_compatibility_with_sparse_field_ids() {
    // Serialize old version
    let old_struct = SparseFieldsOld {
        a: 200,
        b: "old sparse".to_string(),
    };

    let mut writer = Writer::new();
    old_struct.serialize_field(&mut writer, 1);

    // Deserialize as new version (should get defaults for missing fields)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = SparseFieldsNew::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.a, 200);
    assert_eq!(decoded.inserted, i64::default()); // Default: 0
    assert_eq!(decoded.b, "old sparse");
    assert_eq!(decoded.appended, bool::default()); // Default: false
}

// ============================================================================
// Version Tolerance Tests - Nested structs with version differences
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct InnerV1 {
    #[id(0)]
    pub x: u32,
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct InnerV2 {
    #[id(0)]
    pub x: u32,
    #[id(1)]
    pub y: String, // New field in V2
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct OuterWithInnerV1 {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub inner: InnerV1,
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct OuterWithInnerV2 {
    #[id(0)]
    pub name: String,
    #[id(1)]
    pub inner: InnerV2,
}

#[test]
fn test_nested_forward_compatibility() {
    // Serialize outer with InnerV2
    let outer_v2 = OuterWithInnerV2 {
        name: "nested test".to_string(),
        inner: InnerV2 {
            x: 42,
            y: "inner new".to_string(),
        },
    };

    let mut writer = Writer::new();
    outer_v2.serialize_field(&mut writer, 1);

    // Deserialize as outer with InnerV1 (should skip inner's new field)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = OuterWithInnerV1::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.name, "nested test");
    assert_eq!(decoded.inner.x, 42);
}

#[test]
fn test_nested_backward_compatibility() {
    // Serialize outer with InnerV1
    let outer_v1 = OuterWithInnerV1 {
        name: "old nested".to_string(),
        inner: InnerV1 { x: 99 },
    };

    let mut writer = Writer::new();
    outer_v1.serialize_field(&mut writer, 1);

    // Deserialize as outer with InnerV2 (should get default for inner's new field)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = OuterWithInnerV2::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.name, "old nested");
    assert_eq!(decoded.inner.x, 99);
    assert_eq!(decoded.inner.y, String::default());
}

// ============================================================================
// Version Tolerance Tests - Field reordering (same IDs, different order in code)
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct OrderedFieldsA {
    #[id(0)]
    pub first: u32,
    #[id(1)]
    pub second: String,
    #[id(2)]
    pub third: bool,
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct OrderedFieldsB {
    // Same IDs but different declaration order
    #[id(2)]
    pub third: bool,
    #[id(0)]
    pub first: u32,
    #[id(1)]
    pub second: String,
}

#[test]
fn test_field_reordering_same_ids() {
    // Serialize with OrderedFieldsA
    let a = OrderedFieldsA {
        first: 1,
        second: "two".to_string(),
        third: true,
    };

    let mut writer = Writer::new();
    a.serialize_field(&mut writer, 1);

    // Deserialize as OrderedFieldsB (same IDs, different order)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = OrderedFieldsB::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.first, 1);
    assert_eq!(decoded.second, "two");
    assert_eq!(decoded.third, true);
}

// ============================================================================
// Version Tolerance Tests - Empty to non-empty struct evolution
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct EvolvingStructEmpty {}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct EvolvingStructWithFields {
    #[id(0)]
    pub new_field: String,
    #[id(1)]
    pub another: u32,
}

#[test]
fn test_empty_struct_forward_compatibility() {
    // Serialize struct with fields
    let with_fields = EvolvingStructWithFields {
        new_field: "added".to_string(),
        another: 42,
    };

    let mut writer = Writer::new();
    with_fields.serialize_field(&mut writer, 1);

    // Deserialize as empty struct (should skip all fields)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = EvolvingStructEmpty::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded, EvolvingStructEmpty {});
}

#[test]
fn test_empty_struct_backward_compatibility() {
    // Serialize empty struct
    let empty = EvolvingStructEmpty {};

    let mut writer = Writer::new();
    empty.serialize_field(&mut writer, 1);

    // Deserialize as struct with fields (should get defaults)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = EvolvingStructWithFields::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.new_field, String::default());
    assert_eq!(decoded.another, u32::default());
}

// ============================================================================
// Version Tolerance Tests - Different wire types for unknown fields
// ============================================================================

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct MixedWireTypesNew {
    #[id(0)]
    pub kept: u32,
    #[id(1)]
    pub varint_field: i64,      // VarInt wire type
    #[id(2)]
    pub string_field: String,    // LengthPrefixed wire type
    #[id(3)]
    pub bytes_field: Vec<u8>,    // LengthPrefixed wire type
    #[id(4)]
    pub nested: InnerV1,         // TagDelimited wire type
}

#[derive(Debug, Default, Clone, PartialEq, OrleansSerialize, OrleansDeserialize)]
struct MixedWireTypesOld {
    #[id(0)]
    pub kept: u32,
}

#[test]
fn test_skip_different_wire_types() {
    // Serialize with various wire types
    let new_struct = MixedWireTypesNew {
        kept: 777,
        varint_field: -999999,
        string_field: "to be skipped".to_string(),
        bytes_field: vec![0xFF, 0xFE, 0xFD],
        nested: InnerV1 { x: 12345 },
    };

    let mut writer = Writer::new();
    new_struct.serialize_field(&mut writer, 1);

    // Deserialize as old version (should correctly skip all different wire types)
    let mut reader = Reader::new(writer.as_bytes());
    let _field = reader.read_field_header().unwrap();
    let decoded = MixedWireTypesOld::deserialize_field(&mut reader).unwrap();

    assert_eq!(decoded.kept, 777);
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

        // ====================================================================
        // Version Tolerance Property Tests
        // ====================================================================

        /// Property: Forward compatibility - V2 data can always be read as V1
        /// The common fields should always match.
        #[test]
        fn prop_forward_compatibility_preserves_common_fields(
            name in "\\PC{0,50}",
            value: u32,
            new_field in "\\PC{0,50}",
        ) {
            let v2 = VersionedStructV2 {
                name: name.clone(),
                value,
                new_field,
            };

            let mut writer = Writer::new();
            v2.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let v1 = VersionedStructV1::deserialize_field(&mut reader).unwrap();

            // Common fields must be preserved
            prop_assert_eq!(v1.name, name);
            prop_assert_eq!(v1.value, value);
        }

        /// Property: Backward compatibility - V1 data can always be read as V2
        /// The common fields should match, new fields should have defaults.
        #[test]
        fn prop_backward_compatibility_preserves_common_fields(
            name in "\\PC{0,50}",
            value: u32,
        ) {
            let v1 = VersionedStructV1 {
                name: name.clone(),
                value,
            };

            let mut writer = Writer::new();
            v1.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let v2 = VersionedStructV2::deserialize_field(&mut reader).unwrap();

            // Common fields must be preserved
            prop_assert_eq!(v2.name, name);
            prop_assert_eq!(v2.value, value);
            // New field should be default
            prop_assert_eq!(v2.new_field, String::default());
        }

        /// Property: Field ID determines identity, not declaration order
        /// Structs with same field IDs but different declaration order
        /// should serialize/deserialize correctly.
        #[test]
        fn prop_field_id_determines_identity(
            first: u32,
            second in "\\PC{0,30}",
            third: bool,
        ) {
            let a = OrderedFieldsA {
                first,
                second: second.clone(),
                third,
            };

            let mut writer = Writer::new();
            a.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let b = OrderedFieldsB::deserialize_field(&mut reader).unwrap();

            // All fields must match regardless of declaration order
            prop_assert_eq!(b.first, first);
            prop_assert_eq!(b.second, second);
            prop_assert_eq!(b.third, third);
        }

        /// Property: Nested version tolerance works correctly
        /// Inner struct version differences should be handled properly.
        #[test]
        fn prop_nested_version_tolerance(
            name in "\\PC{0,30}",
            x: u32,
            y in "\\PC{0,30}",
        ) {
            // Serialize with InnerV2
            let outer_v2 = OuterWithInnerV2 {
                name: name.clone(),
                inner: InnerV2 { x, y },
            };

            let mut writer = Writer::new();
            outer_v2.serialize_field(&mut writer, 1);

            // Deserialize as OuterWithInnerV1
            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let outer_v1 = OuterWithInnerV1::deserialize_field(&mut reader).unwrap();

            // Outer name and inner x should be preserved
            prop_assert_eq!(outer_v1.name, name);
            prop_assert_eq!(outer_v1.inner.x, x);
        }
    }
}
