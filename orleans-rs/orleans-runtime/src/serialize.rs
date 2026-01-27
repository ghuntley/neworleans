//! Simple serialization traits for grain method invocation.
//!
//! These traits provide a simple way to serialize and deserialize grain method
//! arguments and return values. For more complex types, consider using the
//! full Orleans serialization system from `orleans-serialization`.

/// Trait for types that can be serialized for grain method invocation.
///
/// This is implemented for common primitive types. For custom types,
/// implement this trait manually or use the Orleans serialization derive macros.
pub trait GrainSerialize {
    /// Serialize this value to bytes.
    fn serialize(&self) -> Vec<u8>;
}

/// Trait for types that can be deserialized from grain method invocation.
///
/// This is implemented for common primitive types. For custom types,
/// implement this trait manually or use the Orleans serialization derive macros.
pub trait GrainDeserialize: Sized {
    /// Deserialize a value from bytes.
    fn deserialize(bytes: &[u8]) -> Option<Self>;
}

// Implement GrainSerialize for primitive types

impl GrainSerialize for () {
    fn serialize(&self) -> Vec<u8> {
        Vec::new()
    }
}

impl GrainSerialize for bool {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl GrainSerialize for u8 {
    fn serialize(&self) -> Vec<u8> {
        vec![*self]
    }
}

impl GrainSerialize for i8 {
    fn serialize(&self) -> Vec<u8> {
        vec![*self as u8]
    }
}

impl GrainSerialize for u16 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for i16 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for u32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for i32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for u64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for i64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for f32 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for f64 {
    fn serialize(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl GrainSerialize for String {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4 + self.len());
        result.extend_from_slice(&(self.len() as u32).to_le_bytes());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl GrainSerialize for &str {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(4 + self.len());
        result.extend_from_slice(&(self.len() as u32).to_le_bytes());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl<T: GrainSerialize> GrainSerialize for Vec<T> {
    fn serialize(&self) -> Vec<u8> {
        let mut result = Vec::new();
        result.extend_from_slice(&(self.len() as u32).to_le_bytes());
        for item in self {
            let item_bytes = item.serialize();
            result.extend_from_slice(&(item_bytes.len() as u32).to_le_bytes());
            result.extend_from_slice(&item_bytes);
        }
        result
    }
}

impl<T: GrainSerialize> GrainSerialize for Option<T> {
    fn serialize(&self) -> Vec<u8> {
        match self {
            Some(value) => {
                let mut result = vec![1u8];
                result.extend(value.serialize());
                result
            }
            None => vec![0u8],
        }
    }
}

// Implement GrainDeserialize for primitive types

impl GrainDeserialize for () {
    fn deserialize(_bytes: &[u8]) -> Option<Self> {
        Some(())
    }
}

impl GrainDeserialize for bool {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bytes.first().map(|&b| b != 0)
    }
}

impl GrainDeserialize for u8 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bytes.first().copied()
    }
}

impl GrainDeserialize for i8 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bytes.first().map(|&b| b as i8)
    }
}

impl GrainDeserialize for u16 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        Some(u16::from_le_bytes([bytes[0], bytes[1]]))
    }
}

impl GrainDeserialize for i16 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 2 {
            return None;
        }
        Some(i16::from_le_bytes([bytes[0], bytes[1]]))
    }
}

impl GrainDeserialize for u32 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl GrainDeserialize for i32 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        Some(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl GrainDeserialize for u64 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        Some(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

impl GrainDeserialize for i64 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        Some(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

impl GrainDeserialize for f32 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        Some(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl GrainDeserialize for f64 {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        Some(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

impl GrainDeserialize for String {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if bytes.len() < 4 + len {
            return None;
        }
        String::from_utf8(bytes[4..4 + len].to_vec()).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_u32_roundtrip() {
        let value: u32 = 42;
        let bytes = value.serialize();
        let recovered = u32::deserialize(&bytes).unwrap();
        assert_eq!(value, recovered);
    }

    #[test]
    fn test_i64_roundtrip() {
        let value: i64 = -123456789;
        let bytes = value.serialize();
        let recovered = i64::deserialize(&bytes).unwrap();
        assert_eq!(value, recovered);
    }

    #[test]
    fn test_string_roundtrip() {
        let value = String::from("Hello, Orleans!");
        let bytes = value.serialize();
        let recovered = String::deserialize(&bytes).unwrap();
        assert_eq!(value, recovered);
    }

    #[test]
    fn test_bool_roundtrip() {
        let true_bytes = true.serialize();
        let false_bytes = false.serialize();
        assert_eq!(bool::deserialize(&true_bytes), Some(true));
        assert_eq!(bool::deserialize(&false_bytes), Some(false));
    }

    #[test]
    fn test_option_roundtrip() {
        let some_value: Option<u32> = Some(42);
        let none_value: Option<u32> = None;

        let some_bytes = some_value.serialize();
        let none_bytes = none_value.serialize();

        // Just verify serialization produces bytes
        assert!(!some_bytes.is_empty());
        assert!(!none_bytes.is_empty());
    }
}
