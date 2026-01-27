//! CorrelationId - Unique identifier for request/response matching.
//!
//! Each message in Orleans is assigned a CorrelationId that uniquely identifies
//! the request. Responses carry the same CorrelationId to match with the original request.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use orleans_serialization::codecs::{Deserialize, FieldDeserialize, FieldSerialize, Serialize};
use orleans_serialization::{Reader, SerializationError, Writer};

type Result<T> = std::result::Result<T, SerializationError>;

/// Counter for generating unique correlation IDs within this process.
static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique identifier for correlating requests with responses.
///
/// CorrelationId consists of:
/// - `nonce`: A random value unique to this process (generated at startup)
/// - `counter`: A monotonically increasing counter
///
/// Together, these ensure uniqueness across processes and time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CorrelationId {
    /// Random nonce unique to this process instance.
    nonce: u64,
    /// Monotonically increasing counter.
    counter: u64,
}

impl CorrelationId {
    /// Generate the process nonce lazily.
    fn get_nonce() -> u64 {
        use std::sync::OnceLock;
        static NONCE: OnceLock<u64> = OnceLock::new();
        *NONCE.get_or_init(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            let seed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0);
            // Mix in process id for additional uniqueness
            seed ^ (std::process::id() as u64)
        })
    }

    /// Creates a new unique CorrelationId.
    pub fn new() -> Self {
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        Self {
            nonce: Self::get_nonce(),
            counter,
        }
    }

    /// Creates a CorrelationId from explicit nonce and counter values.
    pub fn from_parts(nonce: u64, counter: u64) -> Self {
        Self { nonce, counter }
    }

    /// Returns the nonce component.
    pub fn nonce(&self) -> u64 {
        self.nonce
    }

    /// Returns the counter component.
    pub fn counter(&self) -> u64 {
        self.counter
    }

    /// Returns a hash code for this CorrelationId.
    pub fn get_hash_code(&self) -> u32 {
        let combined = self.nonce.wrapping_mul(31).wrapping_add(self.counter);
        (combined >> 32) as u32 ^ combined as u32
    }
}

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}:{:016x}", self.nonce, self.counter)
    }
}

impl std::str::FromStr for CorrelationId {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 2 {
            return Err("Expected format: nonce:counter");
        }
        let nonce = u64::from_str_radix(parts[0], 16).map_err(|_| "Invalid nonce hex")?;
        let counter = u64::from_str_radix(parts[1], 16).map_err(|_| "Invalid counter hex")?;
        Ok(Self { nonce, counter })
    }
}

// Serialization implementations
impl Serialize for CorrelationId {
    fn serialize(&self, writer: &mut Writer) {
        writer.write_u64_le(self.nonce);
        writer.write_u64_le(self.counter);
    }
}

impl Deserialize for CorrelationId {
    fn deserialize(reader: &mut Reader) -> Result<Self> {
        let nonce = reader.read_u64_le()?;
        let counter = reader.read_u64_le()?;
        Ok(Self { nonce, counter })
    }
}

impl FieldSerialize for CorrelationId {
    fn serialize_field(&self, writer: &mut Writer, field_id: u32) {
        // Serialize as two fixed64 fields using the existing helpers
        writer.write_fixed64_field(field_id, self.nonce);
        writer.write_fixed64_field(field_id + 1, self.counter);
    }
}

impl FieldDeserialize for CorrelationId {
    fn deserialize_field(reader: &mut Reader) -> Result<Self> {
        // Read the nonce field
        let _field = reader.read_field_header()?;
        let nonce = reader.read_u64_le()?;
        // Read the counter field
        let _field = reader.read_field_header()?;
        let counter = reader.read_u64_le()?;
        Ok(Self { nonce, counter })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_creates_unique_ids() {
        let id1 = CorrelationId::new();
        let id2 = CorrelationId::new();
        assert_ne!(id1, id2);
        assert_eq!(id1.nonce(), id2.nonce()); // Same process = same nonce
        assert_ne!(id1.counter(), id2.counter()); // Different counters
    }

    #[test]
    fn test_from_parts() {
        let id = CorrelationId::from_parts(123, 456);
        assert_eq!(id.nonce(), 123);
        assert_eq!(id.counter(), 456);
    }

    #[test]
    fn test_display_parse_roundtrip() {
        let id = CorrelationId::from_parts(0x123456789ABCDEF0, 0xFEDCBA9876543210);
        let s = id.to_string();
        let parsed: CorrelationId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let id = CorrelationId::from_parts(0x123456789ABCDEF0, 0xFEDCBA9876543210);
        let mut writer = Writer::new();
        id.serialize(&mut writer);
        let bytes = writer.into_bytes();

        let mut reader = Reader::new(&bytes);
        let deserialized = CorrelationId::deserialize(&mut reader).unwrap();
        assert_eq!(id, deserialized);
    }

    #[test]
    fn test_hash_code_stability() {
        let id = CorrelationId::from_parts(123, 456);
        let hash1 = id.get_hash_code();
        let hash2 = id.get_hash_code();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_counter_is_monotonic() {
        let mut last_counter = 0u64;
        for _ in 0..100 {
            let id = CorrelationId::new();
            assert!(id.counter() > last_counter || last_counter == 0);
            last_counter = id.counter();
        }
    }
}
