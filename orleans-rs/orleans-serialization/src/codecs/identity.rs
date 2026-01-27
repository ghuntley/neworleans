//! Codecs for Orleans identity types.
//!
//! This module provides serialization for:
//! - IdSpan
//! - GrainType
//! - GrainId
//! - SiloAddress
//! - ActivationId
//! - GrainAddress

use super::{Deserialize, FieldDeserialize, FieldSerialize, Serialize};
use crate::error::{Result, SerializationError};
use crate::reader::Reader;
use crate::writer::Writer;
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

// ============================================================================
// IdSpan - Length-prefixed bytes
// ============================================================================

impl Serialize for IdSpan {
    fn serialize(&self, writer: &mut Writer) {
        let bytes = self.as_bytes();
        writer.write_varint(bytes.len() as u64);
        writer.write_bytes(bytes);
    }
}

impl Deserialize for IdSpan {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let len = reader.read_varint()? as usize;
        if len == 0 {
            return Ok(IdSpan::empty());
        }
        let bytes = reader.read_bytes(len)?;
        Ok(IdSpan::new(bytes))
    }
}

impl FieldSerialize for IdSpan {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.write_length_prefixed_field(field_id, self.as_bytes());
    }
}

impl FieldDeserialize for IdSpan {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let bytes = reader.read_length_prefixed()?;
        if bytes.is_empty() {
            Ok(IdSpan::empty())
        } else {
            Ok(IdSpan::new(bytes))
        }
    }
}

// ============================================================================
// GrainType - Wraps IdSpan
// ============================================================================

impl Serialize for GrainType {
    fn serialize(&self, writer: &mut Writer) {
        self.as_id_span().serialize(writer);
    }
}

impl Deserialize for GrainType {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let id_span = IdSpan::deserialize(reader)?;
        Ok(GrainType::from_id_span(id_span))
    }
}

impl FieldSerialize for GrainType {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        self.as_id_span().serialize_field(writer, field_id);
    }
}

impl FieldDeserialize for GrainType {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let id_span = IdSpan::deserialize_field(reader)?;
        Ok(GrainType::from_id_span(id_span))
    }
}

// ============================================================================
// GrainId - Tag-delimited (grain_type + key)
// ============================================================================

impl Serialize for GrainId {
    fn serialize(&self, writer: &mut Writer) {
        // Field 0: grain_type
        self.grain_type().serialize_field(writer, 0);
        // Field 1: key
        self.key().serialize_field(writer, 1);
    }
}

impl Deserialize for GrainId {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let mut grain_type = GrainType::default();
        let mut key = IdSpan::empty();

        loop {
            let field = reader.read_field_header()?;
            if field.is_end_tag() {
                break;
            }

            match reader.current_field_id() {
                0 => grain_type = GrainType::deserialize_field(reader)?,
                1 => key = IdSpan::deserialize_field(reader)?,
                _ => reader.skip_field(&field)?,
            }
        }

        Ok(GrainId::new(grain_type, key))
    }
}

impl FieldSerialize for GrainId {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.begin_tag_delimited_field(field_id);
        // begin_tag_delimited_field already resets field ID for nested content
        self.serialize(writer);
        writer.write_end_tag();
        // Restore field ID to the one we just wrote (for correct delta calculation)
        writer.set_field_id(field_id);
    }
}

impl FieldDeserialize for GrainId {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        // The field header (TagDelimited) has already been read
        let saved_field_id = reader.current_field_id();
        reader.reset_field_id();
        let result = Self::deserialize(reader)?;
        reader.set_field_id(saved_field_id);
        Ok(result)
    }
}

// ============================================================================
// SiloAddress - Tag-delimited (endpoint + generation)
// ============================================================================

impl Serialize for SiloAddress {
    fn serialize(&self, writer: &mut Writer) {
        // Field 0: IP address bytes
        match self.ip() {
            IpAddr::V4(ip) => {
                writer.write_length_prefixed_field(0, &ip.octets());
            }
            IpAddr::V6(ip) => {
                writer.write_length_prefixed_field(0, &ip.octets());
            }
        }
        // Field 1: port
        writer.write_varint_field(1, self.port() as u64);
        // Field 2: generation
        writer.write_signed_varint_field(2, self.generation());
    }
}

impl Deserialize for SiloAddress {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let mut ip_bytes: Vec<u8> = Vec::new();
        let mut port: u16 = 0;
        let mut generation: i64 = 0;

        loop {
            let field = reader.read_field_header()?;
            if field.is_end_tag() {
                break;
            }

            match reader.current_field_id() {
                0 => {
                    let bytes = reader.read_length_prefixed()?;
                    ip_bytes = bytes.to_vec();
                }
                1 => port = reader.read_varint()? as u16,
                2 => generation = reader.read_signed_varint()?,
                _ => reader.skip_field(&field)?,
            }
        }

        let ip_addr = if ip_bytes.len() == 4 {
            IpAddr::V4(Ipv4Addr::new(
                ip_bytes[0],
                ip_bytes[1],
                ip_bytes[2],
                ip_bytes[3],
            ))
        } else if ip_bytes.len() == 16 {
            let bytes: [u8; 16] = ip_bytes.try_into().map_err(|_| {
                SerializationError::TypeMismatch {
                    expected: "16-byte IPv6".to_string(),
                    actual: "invalid length".to_string(),
                }
            })?;
            IpAddr::V6(Ipv6Addr::from(bytes))
        } else {
            return Err(SerializationError::TypeMismatch {
                expected: "4 or 16 byte IP".to_string(),
                actual: format!("{} bytes", ip_bytes.len()),
            });
        };

        Ok(SiloAddress::new(SocketAddr::new(ip_addr, port), generation))
    }
}

impl FieldSerialize for SiloAddress {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.begin_tag_delimited_field(field_id);
        // begin_tag_delimited_field already resets field ID for nested content
        self.serialize(writer);
        writer.write_end_tag();
        // Restore field ID to the one we just wrote (for correct delta calculation)
        writer.set_field_id(field_id);
    }
}

impl FieldDeserialize for SiloAddress {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let saved_field_id = reader.current_field_id();
        reader.reset_field_id();
        let result = Self::deserialize(reader)?;
        reader.set_field_id(saved_field_id);
        Ok(result)
    }
}

// ============================================================================
// ActivationId - Fixed 16 bytes (UUID)
// ============================================================================

impl Serialize for ActivationId {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_bytes(self.as_bytes());
    }
}

impl Deserialize for ActivationId {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let bytes = reader.read_bytes(16)?;
        let bytes_arr: [u8; 16] = bytes.try_into().map_err(|_| {
            SerializationError::TypeMismatch {
                expected: "16 bytes".to_string(),
                actual: format!("{} bytes", bytes.len()),
            }
        })?;
        Ok(ActivationId::from_uuid(uuid::Uuid::from_bytes(bytes_arr)))
    }
}

impl FieldSerialize for ActivationId {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        // Use length-prefixed for activation ID (16 bytes)
        writer.write_length_prefixed_field(field_id, self.as_bytes());
    }
}

impl FieldDeserialize for ActivationId {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let bytes = reader.read_length_prefixed()?;
        if bytes.len() != 16 {
            return Err(SerializationError::TypeMismatch {
                expected: "16 bytes".to_string(),
                actual: format!("{} bytes", bytes.len()),
            });
        }
        let bytes_arr: [u8; 16] = bytes.try_into().unwrap();
        Ok(ActivationId::from_uuid(uuid::Uuid::from_bytes(bytes_arr)))
    }
}

// ============================================================================
// GrainAddress - Tag-delimited (grain_id + activation_id + silo_address?)
// ============================================================================

impl Serialize for GrainAddress {
    fn serialize(&self, writer: &mut Writer) {
        // Field 0: grain_id
        self.grain_id().serialize_field(writer, 0);
        // Field 1: activation_id
        self.activation_id().serialize_field(writer, 1);
        // Field 2: silo_address (optional)
        if let Some(silo) = self.silo_address() {
            silo.serialize_field(writer, 2);
        }
    }
}

impl Deserialize for GrainAddress {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let mut grain_id = GrainId::default();
        let mut activation_id = ActivationId::default();
        let mut silo_address: Option<SiloAddress> = None;

        loop {
            let field = reader.read_field_header()?;
            if field.is_end_tag() {
                break;
            }

            match reader.current_field_id() {
                0 => grain_id = GrainId::deserialize_field(reader)?,
                1 => activation_id = ActivationId::deserialize_field(reader)?,
                2 => silo_address = Some(SiloAddress::deserialize_field(reader)?),
                _ => reader.skip_field(&field)?,
            }
        }

        Ok(GrainAddress::new(grain_id, activation_id, silo_address))
    }
}

impl FieldSerialize for GrainAddress {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        writer.begin_tag_delimited_field(field_id);
        // begin_tag_delimited_field already resets field ID for nested content
        self.serialize(writer);
        writer.write_end_tag();
        // Restore field ID to the one we just wrote (for correct delta calculation)
        writer.set_field_id(field_id);
    }
}

impl FieldDeserialize for GrainAddress {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        let saved_field_id = reader.current_field_id();
        reader.reset_field_id();
        let result = Self::deserialize(reader)?;
        reader.set_field_id(saved_field_id);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    // Helper to roundtrip via field serialization
    fn roundtrip_field<T: FieldSerialize + FieldDeserialize + PartialEq + std::fmt::Debug>(
        value: &T,
    ) {
        let mut writer = Writer::new();
        value.serialize_field(&mut writer, 1);

        let mut reader = Reader::new(writer.as_bytes());
        let field = reader.read_field_header().unwrap();
        assert!(!field.is_end_tag());

        let decoded = T::deserialize_field(&mut reader).unwrap();
        assert_eq!(*value, decoded);
    }

    // ========================================================================
    // IdSpan tests
    // ========================================================================

    #[test]
    fn test_id_span_empty() {
        roundtrip_field(&IdSpan::empty());
    }

    #[test]
    fn test_id_span_string() {
        roundtrip_field(&IdSpan::from_str("hello-world"));
    }

    #[test]
    fn test_id_span_bytes() {
        roundtrip_field(&IdSpan::new(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn test_id_span_unicode() {
        roundtrip_field(&IdSpan::from_str("héllo wörld 🌍"));
    }

    // ========================================================================
    // GrainType tests
    // ========================================================================

    #[test]
    fn test_grain_type_default() {
        roundtrip_field(&GrainType::default());
    }

    #[test]
    fn test_grain_type_simple() {
        roundtrip_field(&GrainType::create("MyApp.HelloGrain"));
    }

    #[test]
    fn test_grain_type_system() {
        roundtrip_field(&GrainType::system_type("membership"));
    }

    // ========================================================================
    // GrainId tests
    // ========================================================================

    #[test]
    fn test_grain_id_default() {
        roundtrip_field(&GrainId::default());
    }

    #[test]
    fn test_grain_id_simple() {
        roundtrip_field(&GrainId::create("MyApp.UserGrain", "user-123"));
    }

    #[test]
    fn test_grain_id_integer_key() {
        roundtrip_field(&GrainId::with_integer_key("CounterGrain", 42));
    }

    #[test]
    fn test_grain_id_compound_key() {
        roundtrip_field(&GrainId::with_compound_key("CompoundGrain", "key", "ext"));
    }

    // ========================================================================
    // SiloAddress tests
    // ========================================================================

    #[test]
    fn test_silo_address_ipv4() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo = SiloAddress::new(addr, 12345678);
        roundtrip_field(&silo);
    }

    #[test]
    fn test_silo_address_ipv6() {
        let addr = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 22222);
        let silo = SiloAddress::new(addr, 99999);
        roundtrip_field(&silo);
    }

    #[test]
    fn test_silo_address_zero() {
        let silo = SiloAddress::zero();
        roundtrip_field(&silo);
    }

    #[test]
    fn test_silo_address_negative_generation() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 11111);
        let silo = SiloAddress::new(addr, -12345);
        roundtrip_field(&silo);
    }

    // ========================================================================
    // ActivationId tests
    // ========================================================================

    #[test]
    fn test_activation_id_default() {
        roundtrip_field(&ActivationId::default());
    }

    #[test]
    fn test_activation_id_random() {
        roundtrip_field(&ActivationId::new());
    }

    #[test]
    fn test_activation_id_deterministic() {
        let grain_id = GrainId::create("TestGrain", "key");
        roundtrip_field(&ActivationId::get_deterministic(&grain_id));
    }

    // ========================================================================
    // GrainAddress tests
    // ========================================================================

    #[test]
    fn test_grain_address_default() {
        roundtrip_field(&GrainAddress::default());
    }

    #[test]
    fn test_grain_address_for_grain() {
        let grain_id = GrainId::create("TestGrain", "key");
        roundtrip_field(&GrainAddress::for_grain(grain_id));
    }

    #[test]
    fn test_grain_address_complete() {
        let grain_id = GrainId::create("TestGrain", "key");
        let activation_id = ActivationId::new();
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 10)), 11111);
        let silo = SiloAddress::new(addr, 12345678);
        let grain_addr = GrainAddress::complete(grain_id, activation_id, silo);
        roundtrip_field(&grain_addr);
    }

    #[test]
    fn test_grain_address_without_silo() {
        let grain_id = GrainId::create("TestGrain", "key");
        let activation_id = ActivationId::new();
        let grain_addr = GrainAddress::new(grain_id, activation_id, None);
        roundtrip_field(&grain_addr);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    proptest! {
        #[test]
        fn prop_id_span_roundtrip(s in "\\PC{0,100}") {
            let span = IdSpan::from_str(&s);

            let mut writer = Writer::new();
            span.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = IdSpan::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(span, decoded);
        }

        #[test]
        fn prop_grain_type_roundtrip(name in "[a-zA-Z][a-zA-Z0-9.]{0,50}") {
            let gt = GrainType::create(&name);

            let mut writer = Writer::new();
            gt.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = GrainType::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(gt, decoded);
        }

        #[test]
        fn prop_grain_id_roundtrip(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
            key in "[a-zA-Z0-9_-]{1,30}"
        ) {
            let grain_id = GrainId::create(&grain_type, &key);

            let mut writer = Writer::new();
            grain_id.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = GrainId::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(grain_id, decoded);
        }

        #[test]
        fn prop_silo_address_roundtrip(
            ip0 in 0u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            generation in any::<i64>()
        ) {
            let addr = SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                port
            );
            let silo = SiloAddress::new(addr, generation);

            let mut writer = Writer::new();
            silo.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = SiloAddress::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(silo, decoded);
        }

        #[test]
        fn prop_activation_id_roundtrip(bytes in prop::array::uniform16(any::<u8>())) {
            let uuid = uuid::Uuid::from_bytes(bytes);
            let act_id = ActivationId::from_uuid(uuid);

            let mut writer = Writer::new();
            act_id.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = ActivationId::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(act_id, decoded);
        }

        #[test]
        fn prop_grain_address_roundtrip(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,20}",
            key in "[a-zA-Z0-9_-]{1,20}",
            has_silo in any::<bool>(),
            ip0 in 1u8..255,
            ip1 in 0u8..255,
            ip2 in 0u8..255,
            ip3 in 0u8..255,
            port in 1u16..65535,
            generation in 1i64..i64::MAX
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let activation_id = ActivationId::new();

            let silo_address = if has_silo {
                let addr = SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(ip0, ip1, ip2, ip3)),
                    port
                );
                Some(SiloAddress::new(addr, generation))
            } else {
                None
            };

            let grain_addr = GrainAddress::new(grain_id, activation_id, silo_address);

            let mut writer = Writer::new();
            grain_addr.serialize_field(&mut writer, 1);

            let mut reader = Reader::new(writer.as_bytes());
            let _field = reader.read_field_header().unwrap();
            let decoded = GrainAddress::deserialize_field(&mut reader).unwrap();

            prop_assert_eq!(grain_addr, decoded);
        }
    }
}
