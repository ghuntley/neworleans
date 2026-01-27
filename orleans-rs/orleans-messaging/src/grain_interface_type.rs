//! GrainInterfaceType - Identifier for grain interface types.
//!
//! This type identifies the specific interface being invoked on a grain.
//! Each grain can implement multiple interfaces, and this type distinguishes between them.

use std::fmt;

use orleans_core::IdSpan;
use orleans_serialization::codecs::{Deserialize, FieldDeserialize, FieldSerialize, Serialize};
use orleans_serialization::{Reader, SerializationError, WireType, Writer};

type Result<T> = std::result::Result<T, SerializationError>;

/// Identifies a grain interface type.
///
/// This is used in messages to specify which interface and method is being invoked.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrainInterfaceType {
    value: IdSpan,
}

impl GrainInterfaceType {
    /// Creates a new GrainInterfaceType from a string identifier.
    pub fn create(name: &str) -> Self {
        Self {
            value: IdSpan::from_str(name),
        }
    }

    /// Creates a GrainInterfaceType from an IdSpan.
    pub fn from_id_span(value: IdSpan) -> Self {
        Self { value }
    }

    /// Returns the underlying IdSpan.
    pub fn value(&self) -> &IdSpan {
        &self.value
    }

    /// Returns the interface type as a string.
    pub fn as_str(&self) -> Option<&str> {
        self.value.as_str()
    }

    /// Returns a hash code for this interface type.
    pub fn get_hash_code(&self) -> u32 {
        self.value.get_hash_code()
    }

    /// Returns a default/empty interface type.
    pub fn default_type() -> Self {
        Self {
            value: IdSpan::empty(),
        }
    }

    /// Returns true if this is the default/empty interface type.
    pub fn is_default(&self) -> bool {
        self.value.is_empty()
    }
}

impl Default for GrainInterfaceType {
    fn default() -> Self {
        Self::default_type()
    }
}

impl fmt::Display for GrainInterfaceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = self.value.as_str() {
            write!(f, "{}", s)
        } else {
            write!(f, "<binary:{} bytes>", self.value.len())
        }
    }
}

impl From<&str> for GrainInterfaceType {
    fn from(s: &str) -> Self {
        Self::create(s)
    }
}

impl From<String> for GrainInterfaceType {
    fn from(s: String) -> Self {
        Self::create(&s)
    }
}

// Serialization implementations
impl Serialize for GrainInterfaceType {
    fn serialize(&self, writer: &mut Writer) {
        self.value.serialize(writer);
    }
}

impl Deserialize for GrainInterfaceType {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let value = IdSpan::deserialize(reader)?;
        Ok(Self { value })
    }
}

impl FieldSerialize for GrainInterfaceType {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        let bytes = self.value.as_bytes();
        writer.write_length_prefixed_field(field_id, bytes);
    }
}

impl FieldDeserialize for GrainInterfaceType {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let field = reader.read_field_header()?;
        match field.wire_type {
            WireType::LengthPrefixed => {
                let bytes = reader.read_length_prefixed()?;
                Ok(Self {
                    value: IdSpan::new(bytes),
                })
            }
            _ => Err(orleans_serialization::SerializationError::TypeMismatch {
                expected: "LengthPrefixed".to_string(),
                actual: format!("{:?}", field.wire_type),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let iface = GrainInterfaceType::create("IHelloGrain");
        assert_eq!(iface.as_str(), Some("IHelloGrain"));
    }

    #[test]
    fn test_hash_code_stability() {
        let iface = GrainInterfaceType::create("IHelloGrain");
        let hash1 = iface.get_hash_code();
        let hash2 = iface.get_hash_code();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_equality() {
        let iface1 = GrainInterfaceType::create("IHelloGrain");
        let iface2 = GrainInterfaceType::create("IHelloGrain");
        let iface3 = GrainInterfaceType::create("IGoodbyeGrain");
        assert_eq!(iface1, iface2);
        assert_ne!(iface1, iface3);
    }

    #[test]
    fn test_default() {
        let iface = GrainInterfaceType::default();
        assert!(iface.is_default());
    }

    #[test]
    fn test_display() {
        let iface = GrainInterfaceType::create("IHelloGrain");
        assert_eq!(iface.to_string(), "IHelloGrain");
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let iface = GrainInterfaceType::create("MyApp.Grains.IPlayerGrain");
        let mut writer = Writer::new();
        iface.serialize(&mut writer);
        let bytes = writer.into_bytes();

        let mut reader = Reader::new(&bytes);
        let deserialized = GrainInterfaceType::deserialize(&mut reader).unwrap();
        assert_eq!(iface, deserialized);
    }
}
