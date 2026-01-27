//! VarInt encoding and decoding.
//!
//! Orleans uses a standard 7-bit continuation-based VarInt encoding where:
//! - Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag
//! - Values 0-127 use 1 byte
//! - Values 128-16383 use 2 bytes
//! - Up to 10 bytes for full 64-bit values
//!
//! Signed integers use ZigZag encoding to efficiently encode small negative numbers.

use crate::error::{Result, SerializationError};

/// Maximum number of bytes needed to encode a u64 as VarInt.
pub const MAX_VARINT_BYTES: usize = 10;

/// Maximum number of bytes needed to encode a u32 as VarInt.
pub const MAX_VARINT32_BYTES: usize = 5;

/// Write a VarInt-encoded unsigned integer to a byte slice.
///
/// Returns the number of bytes written.
///
/// # Panics
///
/// Panics if the buffer is too small (should have at least 10 bytes).
pub fn write_varint(buf: &mut [u8], mut value: u64) -> usize {
    let mut pos = 0;

    while value >= 0x80 {
        buf[pos] = (value as u8) | 0x80;
        value >>= 7;
        pos += 1;
    }
    buf[pos] = value as u8;
    pos + 1
}

/// Write a VarInt-encoded u32 to a byte slice.
///
/// Returns the number of bytes written.
pub fn write_varint32(buf: &mut [u8], mut value: u32) -> usize {
    let mut pos = 0;

    while value >= 0x80 {
        buf[pos] = (value as u8) | 0x80;
        value >>= 7;
        pos += 1;
    }
    buf[pos] = value as u8;
    pos + 1
}

/// Read a VarInt-encoded unsigned integer from a byte slice.
///
/// Returns the value and the number of bytes consumed.
pub fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut result = 0u64;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= buf.len() {
            return Err(SerializationError::UnexpectedEndOfInput);
        }

        let byte = buf[pos];
        pos += 1;

        result |= ((byte & 0x7F) as u64) << shift;

        if byte & 0x80 == 0 {
            break;
        }

        shift += 7;

        // Prevent overflow: maximum 10 bytes for 64-bit value
        if shift >= 70 {
            return Err(SerializationError::InvalidVarInt);
        }
    }

    Ok((result, pos))
}

/// Read a VarInt-encoded u32 from a byte slice.
///
/// Returns the value and the number of bytes consumed.
pub fn read_varint32(buf: &[u8]) -> Result<(u32, usize)> {
    let (value, len) = read_varint(buf)?;
    Ok((value as u32, len))
}

/// ZigZag encode a signed integer to unsigned.
///
/// This encoding moves the sign bit to the LSB, making small negative numbers
/// small positive numbers: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, etc.
#[inline]
pub fn zigzag_encode(n: i64) -> u64 {
    ((n << 1) ^ (n >> 63)) as u64
}

/// ZigZag encode a signed 32-bit integer.
#[inline]
pub fn zigzag_encode32(n: i32) -> u32 {
    ((n << 1) ^ (n >> 31)) as u32
}

/// ZigZag decode an unsigned integer to signed.
#[inline]
pub fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}

/// ZigZag decode an unsigned 32-bit integer.
#[inline]
pub fn zigzag_decode32(n: u32) -> i32 {
    ((n >> 1) as i32) ^ (-((n & 1) as i32))
}

/// Calculate the number of bytes needed to encode a u64 as VarInt.
#[inline]
pub fn varint_size(value: u64) -> usize {
    if value == 0 {
        return 1;
    }

    // Number of bits needed to represent the value
    let bits = 64 - value.leading_zeros() as usize;
    // Each byte encodes 7 bits
    (bits + 6) / 7
}

/// Calculate the number of bytes needed to encode a u32 as VarInt.
#[inline]
pub fn varint32_size(value: u32) -> usize {
    if value == 0 {
        return 1;
    }

    let bits = 32 - value.leading_zeros() as usize;
    (bits + 6) / 7
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varint_single_byte() {
        let mut buf = [0u8; 10];

        for value in 0u64..128 {
            let len = write_varint(&mut buf, value);
            assert_eq!(len, 1, "Value {value} should encode to 1 byte");
            assert_eq!(buf[0], value as u8);

            let (decoded, read_len) = read_varint(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(read_len, 1);
        }
    }

    #[test]
    fn test_varint_two_bytes() {
        let mut buf = [0u8; 10];

        for value in [128u64, 255, 256, 16383] {
            let len = write_varint(&mut buf, value);
            assert_eq!(len, 2, "Value {value} should encode to 2 bytes");

            let (decoded, read_len) = read_varint(&buf).unwrap();
            assert_eq!(decoded, value);
            assert_eq!(read_len, 2);
        }
    }

    #[test]
    fn test_varint_large_values() {
        let mut buf = [0u8; 10];

        let test_values = [
            16384u64,              // 3 bytes
            2097151,               // 3 bytes (max)
            2097152,               // 4 bytes
            268435455,             // 4 bytes (max)
            268435456,             // 5 bytes
            u32::MAX as u64,       // 5 bytes
            u64::MAX / 2,          // 9 bytes
            u64::MAX,              // 10 bytes
        ];

        for value in test_values {
            let len = write_varint(&mut buf, value);
            let (decoded, read_len) = read_varint(&buf).unwrap();
            assert_eq!(decoded, value, "Roundtrip failed for {value}");
            assert_eq!(read_len, len);
        }
    }

    #[test]
    fn test_varint_size() {
        assert_eq!(varint_size(0), 1);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16383), 2);
        assert_eq!(varint_size(16384), 3);
        assert_eq!(varint_size(u32::MAX as u64), 5);
        assert_eq!(varint_size(u64::MAX), 10);
    }

    #[test]
    fn test_zigzag_encode_decode() {
        // Test specific values
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);
        assert_eq!(zigzag_encode(2), 4);

        // Roundtrip
        for value in [-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX] {
            let encoded = zigzag_encode(value);
            let decoded = zigzag_decode(encoded);
            assert_eq!(decoded, value, "ZigZag roundtrip failed for {value}");
        }
    }

    #[test]
    fn test_zigzag32_encode_decode() {
        for value in [-1000i32, -1, 0, 1, 1000, i32::MIN, i32::MAX] {
            let encoded = zigzag_encode32(value);
            let decoded = zigzag_decode32(encoded);
            assert_eq!(decoded, value, "ZigZag32 roundtrip failed for {value}");
        }
    }

    #[test]
    fn test_read_varint_truncated() {
        // Continuation bit set but no more bytes
        let buf = [0x80u8];
        assert!(matches!(
            read_varint(&buf),
            Err(SerializationError::UnexpectedEndOfInput)
        ));
    }

    #[test]
    fn test_read_varint_empty() {
        let buf: [u8; 0] = [];
        assert!(matches!(
            read_varint(&buf),
            Err(SerializationError::UnexpectedEndOfInput)
        ));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_varint_roundtrip(value: u64) {
            let mut buf = [0u8; 10];
            let written = write_varint(&mut buf, value);
            let (decoded, read) = read_varint(&buf).unwrap();
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(written, read);
        }

        #[test]
        fn prop_varint32_roundtrip(value: u32) {
            let mut buf = [0u8; 5];
            let written = write_varint32(&mut buf, value);
            let (decoded, read) = read_varint32(&buf).unwrap();
            prop_assert_eq!(decoded, value);
            prop_assert_eq!(written, read);
        }

        #[test]
        fn prop_zigzag_roundtrip(value: i64) {
            let encoded = zigzag_encode(value);
            let decoded = zigzag_decode(encoded);
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_zigzag32_roundtrip(value: i32) {
            let encoded = zigzag_encode32(value);
            let decoded = zigzag_decode32(encoded);
            prop_assert_eq!(decoded, value);
        }

        #[test]
        fn prop_varint_size_matches_written(value: u64) {
            let mut buf = [0u8; 10];
            let written = write_varint(&mut buf, value);
            prop_assert_eq!(varint_size(value), written);
        }

        #[test]
        fn prop_small_negatives_encode_small(value in -64i64..64i64) {
            let encoded = zigzag_encode(value);
            // Small values should encode to small unsigned values
            prop_assert!(encoded < 128, "Small signed {} should encode to small unsigned", value);
        }
    }
}
