//! Message serialization for wire format.
//!
//! This module handles serializing and deserializing messages for network transmission.
//! The wire format is: [header_len: i32][body_len: i32][header][body]

use bytes::{BufMut, Bytes, BytesMut};
use orleans_core::{ActivationId, GrainId, SiloAddress};
use orleans_serialization::codecs::{FieldDeserialize, FieldSerialize};
use orleans_serialization::{Reader, Writer};

use crate::correlation_id::CorrelationId;
use crate::direction::Direction;
use crate::grain_interface_type::GrainInterfaceType;
use crate::message::{Message, RejectionInfo, RejectionType};
use crate::MessagingError;

/// Frame header size (header_len + body_len, each 4 bytes).
pub const FRAME_HEADER_SIZE: usize = 8;

/// Maximum message size (16 MB).
pub const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Encodes a message into a framed byte buffer.
pub fn encode_message(message: &Message) -> Result<Bytes, MessagingError> {
    // Serialize header
    let mut header_writer = Writer::new();
    serialize_message_header(message, &mut header_writer);
    let header_bytes = header_writer.into_bytes();

    // The body is already serialized
    let body_bytes = &message.body;

    // Create the framed message
    let total_size = FRAME_HEADER_SIZE + header_bytes.len() + body_bytes.len();
    if total_size > MAX_MESSAGE_SIZE {
        return Err(MessagingError::MessageTooLarge {
            size: total_size,
            max_size: MAX_MESSAGE_SIZE,
        });
    }

    let mut buf = BytesMut::with_capacity(total_size);
    buf.put_i32_le(header_bytes.len() as i32);
    buf.put_i32_le(body_bytes.len() as i32);
    buf.extend_from_slice(&header_bytes);
    buf.extend_from_slice(body_bytes);

    Ok(buf.freeze())
}

/// Decodes a message from a framed byte buffer.
///
/// The buffer must contain a complete frame (header_len + body_len + header + body).
pub fn decode_message(data: &[u8]) -> Result<Message, MessagingError> {
    if data.len() < FRAME_HEADER_SIZE {
        return Err(MessagingError::IncompleteFrame {
            needed: FRAME_HEADER_SIZE,
            available: data.len(),
        });
    }

    let header_len = i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let body_len = i32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    let expected_size = FRAME_HEADER_SIZE + header_len + body_len;
    if data.len() < expected_size {
        return Err(MessagingError::IncompleteFrame {
            needed: expected_size,
            available: data.len(),
        });
    }

    let header_start = FRAME_HEADER_SIZE;
    let header_end = header_start + header_len;
    let body_start = header_end;
    let body_end = body_start + body_len;

    let header_bytes = &data[header_start..header_end];
    let body_bytes = Bytes::copy_from_slice(&data[body_start..body_end]);

    let mut reader = Reader::new(header_bytes);
    deserialize_message_header(&mut reader, body_bytes)
}

/// Returns the frame size if the buffer contains a complete frame, or the number of bytes needed.
pub fn frame_size(data: &[u8]) -> Result<usize, usize> {
    if data.len() < FRAME_HEADER_SIZE {
        return Err(FRAME_HEADER_SIZE);
    }

    let header_len = i32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let body_len = i32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;

    let total = FRAME_HEADER_SIZE + header_len + body_len;
    if data.len() >= total {
        Ok(total)
    } else {
        Err(total)
    }
}

// Field IDs for message header serialization
// CorrelationId uses fields 0 and 1 (nonce and counter)
const FIELD_ID_NONCE: u32 = 0;
const FIELD_ID_COUNTER: u32 = 1;
const FIELD_DIRECTION: u32 = 2;
const FIELD_TARGET_GRAIN: u32 = 3;
const FIELD_TARGET_SILO: u32 = 4;
const FIELD_TARGET_ACTIVATION: u32 = 5;
const FIELD_SENDING_GRAIN: u32 = 6;
const FIELD_SENDING_SILO: u32 = 7;
const FIELD_SENDING_ACTIVATION: u32 = 8;
const FIELD_INTERFACE_TYPE: u32 = 9;
const FIELD_METHOD_ID: u32 = 10;
const FIELD_REJECTION_TYPE: u32 = 11;
const FIELD_REJECTION_MESSAGE: u32 = 12;
const FIELD_TIMEOUT_MS: u32 = 13;

fn serialize_message_header(message: &Message, writer: &mut Writer) {
    // Fields 0-1: CorrelationId (two fixed64 values)
    writer.write_fixed64_field(FIELD_ID_NONCE, message.id.nonce());
    writer.write_fixed64_field(FIELD_ID_COUNTER, message.id.counter());

    // Field 1: Direction (varint)
    (message.direction as u8).serialize_field(writer, FIELD_DIRECTION);

    // Field 2: Target grain (tag-delimited)
    message.target_grain.serialize_field(writer, FIELD_TARGET_GRAIN);

    // Field 3: Target silo (optional)
    if let Some(ref silo) = message.target_silo {
        silo.serialize_field(writer, FIELD_TARGET_SILO);
    }

    // Field 4: Target activation (optional)
    if let Some(ref activation) = message.target_activation {
        activation.serialize_field(writer, FIELD_TARGET_ACTIVATION);
    }

    // Field 5: Sending grain (optional)
    if let Some(ref grain) = message.sending_grain {
        grain.serialize_field(writer, FIELD_SENDING_GRAIN);
    }

    // Field 6: Sending silo
    message.sending_silo.serialize_field(writer, FIELD_SENDING_SILO);

    // Field 7: Sending activation (optional)
    if let Some(ref activation) = message.sending_activation {
        activation.serialize_field(writer, FIELD_SENDING_ACTIVATION);
    }

    // Field 8: Interface type
    message.interface_type.serialize_field(writer, FIELD_INTERFACE_TYPE);

    // Field 9: Method ID
    message.method_id.serialize_field(writer, FIELD_METHOD_ID);

    // Field 10: Rejection type (optional)
    if let Some(ref rejection) = message.rejection_info {
        (rejection.rejection_type as u8).serialize_field(writer, FIELD_REJECTION_TYPE);
        // Field 11: Rejection message
        rejection.message.serialize_field(writer, FIELD_REJECTION_MESSAGE);
    }

    // Field 12: Timeout in milliseconds (optional)
    if let Some(timeout) = message.timeout {
        (timeout.as_millis() as u64).serialize_field(writer, FIELD_TIMEOUT_MS);
    }
}

fn deserialize_message_header(reader: &mut Reader, body: Bytes) -> Result<Message, MessagingError> {
    let mut id_nonce: Option<u64> = None;
    let mut id_counter: Option<u64> = None;
    let mut direction: Option<Direction> = None;
    let mut target_grain: Option<GrainId> = None;
    let mut target_silo: Option<SiloAddress> = None;
    let mut target_activation: Option<ActivationId> = None;
    let mut sending_grain: Option<GrainId> = None;
    let mut sending_silo: Option<SiloAddress> = None;
    let mut sending_activation: Option<ActivationId> = None;
    let mut interface_type: Option<GrainInterfaceType> = None;
    let mut method_id: Option<u32> = None;
    let mut rejection_type: Option<RejectionType> = None;
    let mut rejection_message: Option<String> = None;
    let mut timeout_ms: Option<u64> = None;

    while !reader.is_at_end() {
        match reader.peek_u8() {
            Ok(_) => {}
            Err(_) => break,
        }

        // Read field and determine its ID
        let field = reader.read_field_header().map_err(MessagingError::from)?;
        let current_field_id = reader.current_field_id();

        match current_field_id {
            FIELD_ID_NONCE => {
                id_nonce = Some(reader.read_u64_le().map_err(MessagingError::from)?);
            }
            FIELD_ID_COUNTER => {
                id_counter = Some(reader.read_u64_le().map_err(MessagingError::from)?);
            }
            FIELD_DIRECTION => {
                let value = reader.read_varint().map_err(MessagingError::from)?;
                direction = Direction::from_u8(value as u8);
            }
            FIELD_TARGET_GRAIN => {
                target_grain = Some(GrainId::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_TARGET_SILO => {
                target_silo = Some(SiloAddress::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_TARGET_ACTIVATION => {
                target_activation = Some(ActivationId::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_SENDING_GRAIN => {
                sending_grain = Some(GrainId::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_SENDING_SILO => {
                sending_silo = Some(SiloAddress::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_SENDING_ACTIVATION => {
                sending_activation = Some(ActivationId::deserialize_field(reader).map_err(MessagingError::from)?);
            }
            FIELD_INTERFACE_TYPE => {
                let bytes = reader.read_length_prefixed().map_err(MessagingError::from)?;
                interface_type = Some(GrainInterfaceType::create(
                    std::str::from_utf8(bytes).unwrap_or(""),
                ));
            }
            FIELD_METHOD_ID => {
                method_id = Some(reader.read_varint().map_err(MessagingError::from)? as u32);
            }
            FIELD_REJECTION_TYPE => {
                let value = reader.read_varint().map_err(MessagingError::from)?;
                rejection_type = RejectionType::from_u8(value as u8);
            }
            FIELD_REJECTION_MESSAGE => {
                let bytes = reader.read_length_prefixed().map_err(MessagingError::from)?;
                rejection_message = Some(String::from_utf8_lossy(bytes).to_string());
            }
            FIELD_TIMEOUT_MS => {
                timeout_ms = Some(reader.read_varint().map_err(MessagingError::from)?);
            }
            _ => {
                reader.skip_field(&field).map_err(MessagingError::from)?;
            }
        }
    }

    let id = match (id_nonce, id_counter) {
        (Some(nonce), Some(counter)) => CorrelationId::from_parts(nonce, counter),
        _ => return Err(MessagingError::MissingField("id".to_string())),
    };

    let direction = direction.ok_or_else(|| MessagingError::MissingField("direction".to_string()))?;
    let target_grain = target_grain.ok_or_else(|| MessagingError::MissingField("target_grain".to_string()))?;
    let sending_silo = sending_silo.ok_or_else(|| MessagingError::MissingField("sending_silo".to_string()))?;
    let interface_type = interface_type.unwrap_or_default();
    let method_id = method_id.unwrap_or(0);

    let rejection_info = match (rejection_type, rejection_message) {
        (Some(rt), Some(msg)) => Some(RejectionInfo {
            rejection_type: rt,
            message: msg,
        }),
        (Some(rt), None) => Some(RejectionInfo {
            rejection_type: rt,
            message: String::new(),
        }),
        _ => None,
    };

    let timeout = timeout_ms.map(|ms| std::time::Duration::from_millis(ms));

    Ok(Message {
        id,
        direction,
        target_grain,
        target_silo,
        target_activation,
        sending_grain,
        sending_silo,
        sending_activation,
        interface_type,
        method_id,
        body,
        rejection_info,
        created_at: std::time::Instant::now(),
        timeout,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::GrainType;
    use std::net::SocketAddr;

    fn test_silo_address() -> SiloAddress {
        SiloAddress::new(
            "127.0.0.1:11111".parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    fn test_grain_id() -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), "key1".into())
    }

    #[test]
    fn test_encode_decode_request() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            42,
            Bytes::from_static(b"hello world"),
            test_silo_address(),
        );

        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.id, msg.id);
        assert_eq!(decoded.direction, msg.direction);
        assert_eq!(decoded.target_grain, msg.target_grain);
        assert_eq!(decoded.sending_silo, msg.sending_silo);
        assert_eq!(decoded.interface_type, msg.interface_type);
        assert_eq!(decoded.method_id, msg.method_id);
        assert_eq!(decoded.body, msg.body);
    }

    #[test]
    fn test_encode_decode_response() {
        let request = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::from_static(b"request"),
            test_silo_address(),
        )
        .with_sending_grain(test_grain_id(), Some(ActivationId::new()));

        let response = request.create_response(Bytes::from_static(b"response"));

        let encoded = encode_message(&response).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.id, response.id);
        assert!(decoded.is_response());
        assert_eq!(decoded.body, response.body);
    }

    #[test]
    fn test_encode_decode_rejection() {
        let request = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        );

        let rejection = request.create_rejection(
            RejectionType::GrainNotFound,
            "Test rejection".to_string(),
        );

        let encoded = encode_message(&rejection).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        assert!(decoded.is_rejection());
        let info = decoded.rejection_info.unwrap();
        assert_eq!(info.rejection_type, RejectionType::GrainNotFound);
        assert_eq!(info.message, "Test rejection");
    }

    #[test]
    fn test_frame_size() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        );

        let encoded = encode_message(&msg).unwrap();

        // Complete frame
        assert_eq!(frame_size(&encoded), Ok(encoded.len()));

        // Incomplete frame header
        assert_eq!(frame_size(&encoded[..4]), Err(FRAME_HEADER_SIZE));

        // Incomplete frame
        let size = frame_size(&encoded[..10]);
        assert!(size.is_err());
    }

    #[test]
    fn test_message_with_timeout() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        )
        .with_timeout(std::time::Duration::from_secs(60));

        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.timeout, Some(std::time::Duration::from_secs(60)));
    }
}
