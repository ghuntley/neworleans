//! Stream identity types.
//!
//! Streams in Orleans are identified by a namespace and key combination.
//! The namespace groups related streams, while the key uniquely identifies
//! a specific stream within that namespace.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;
use uuid::Uuid;
use xxhash_rust::xxh32::xxh32;

/// Key portion of a stream identifier.
///
/// Stream keys can be GUIDs, strings, or integers, providing flexibility
/// in how streams are identified.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamKey {
    /// GUID-based key.
    Guid(Uuid),
    /// String-based key.
    String(String),
    /// Integer-based key.
    Integer(i64),
}

impl StreamKey {
    /// Create a new GUID-based stream key.
    pub fn guid(id: Uuid) -> Self {
        Self::Guid(id)
    }

    /// Create a new random GUID-based stream key.
    pub fn new_guid() -> Self {
        Self::Guid(Uuid::new_v4())
    }

    /// Create a new string-based stream key.
    pub fn string(s: impl Into<String>) -> Self {
        Self::String(s.into())
    }

    /// Create a new integer-based stream key.
    pub fn integer(n: i64) -> Self {
        Self::Integer(n)
    }

    /// Get the hash code for this key.
    pub fn get_hash_code(&self) -> u32 {
        match self {
            Self::Guid(id) => xxh32(id.as_bytes(), 0),
            Self::String(s) => xxh32(s.as_bytes(), 0),
            Self::Integer(n) => xxh32(&n.to_le_bytes(), 0),
        }
    }
}

impl fmt::Display for StreamKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Guid(id) => write!(f, "{}", id),
            Self::String(s) => write!(f, "{}", s),
            Self::Integer(n) => write!(f, "{}", n),
        }
    }
}

impl From<Uuid> for StreamKey {
    fn from(id: Uuid) -> Self {
        Self::Guid(id)
    }
}

impl From<String> for StreamKey {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<&str> for StreamKey {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<i64> for StreamKey {
    fn from(n: i64) -> Self {
        Self::Integer(n)
    }
}

impl From<i32> for StreamKey {
    fn from(n: i32) -> Self {
        Self::Integer(n as i64)
    }
}

/// Unique identifier for a stream.
///
/// Combines a namespace (for grouping related streams) with a key
/// (for identifying a specific stream within the namespace).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamId {
    /// The namespace that groups related streams.
    pub namespace: String,
    /// The key that uniquely identifies the stream within the namespace.
    pub key: StreamKey,
}

impl StreamId {
    /// Create a new stream ID.
    pub fn create(namespace: impl Into<String>, key: impl Into<StreamKey>) -> Self {
        Self {
            namespace: namespace.into(),
            key: key.into(),
        }
    }

    /// Create a stream ID with a GUID key.
    pub fn with_guid(namespace: impl Into<String>, key: Uuid) -> Self {
        Self::create(namespace, StreamKey::Guid(key))
    }

    /// Create a stream ID with a string key.
    pub fn with_string(namespace: impl Into<String>, key: impl Into<String>) -> Self {
        Self::create(namespace, StreamKey::String(key.into()))
    }

    /// Create a stream ID with an integer key.
    pub fn with_integer(namespace: impl Into<String>, key: i64) -> Self {
        Self::create(namespace, StreamKey::Integer(key))
    }

    /// Get the hash code for this stream ID.
    ///
    /// Combines the namespace and key hashes for uniform distribution.
    pub fn get_hash_code(&self) -> u32 {
        let namespace_hash = xxh32(self.namespace.as_bytes(), 0);
        let key_hash = self.key.get_hash_code();
        // Combine hashes using a simple mixing function
        namespace_hash.wrapping_add(key_hash.wrapping_mul(31))
    }

    /// Get the namespace.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the key.
    pub fn key(&self) -> &StreamKey {
        &self.key
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.namespace, self.key)
    }
}

/// Position marker within a stream.
///
/// Sequence tokens allow consumers to checkpoint their position
/// and resume from where they left off, enabling reliable delivery.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamSequenceToken {
    /// The sequence number (monotonically increasing).
    pub sequence_number: i64,
    /// The event index within a batch at this sequence number.
    pub event_index: i32,
}

impl StreamSequenceToken {
    /// Create a new sequence token.
    pub fn new(sequence_number: i64, event_index: i32) -> Self {
        Self {
            sequence_number,
            event_index,
        }
    }

    /// Create the first token (sequence 0, index 0).
    pub fn first() -> Self {
        Self::new(0, 0)
    }

    /// Create a token from just a sequence number (event index 0).
    pub fn from_sequence(sequence_number: i64) -> Self {
        Self::new(sequence_number, 0)
    }

    /// Check if this token is newer than another.
    pub fn newer_than(&self, other: &Self) -> bool {
        self > other
    }

    /// Check if this token is older than another.
    pub fn older_than(&self, other: &Self) -> bool {
        self < other
    }

    /// Get the next token (increments event index).
    pub fn next(&self) -> Self {
        Self::new(self.sequence_number, self.event_index + 1)
    }

    /// Get the next sequence token (increments sequence number, resets event index).
    pub fn next_sequence(&self) -> Self {
        Self::new(self.sequence_number + 1, 0)
    }
}

impl Default for StreamSequenceToken {
    fn default() -> Self {
        Self::first()
    }
}

impl fmt::Display for StreamSequenceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.sequence_number, self.event_index)
    }
}

/// Qualified stream ID that includes the provider name.
///
/// Used internally to route stream operations to the correct provider.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedStreamId {
    /// The stream provider name.
    pub provider_name: String,
    /// The stream identity.
    pub stream_id: StreamId,
}

impl QualifiedStreamId {
    /// Create a new qualified stream ID.
    pub fn new(provider_name: impl Into<String>, stream_id: StreamId) -> Self {
        Self {
            provider_name: provider_name.into(),
            stream_id,
        }
    }

    /// Get the hash code.
    pub fn get_hash_code(&self) -> u32 {
        let provider_hash = xxh32(self.provider_name.as_bytes(), 0);
        let stream_hash = self.stream_id.get_hash_code();
        provider_hash.wrapping_add(stream_hash.wrapping_mul(37))
    }
}

impl fmt::Display for QualifiedStreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.provider_name, self.stream_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_key_guid() {
        let id = Uuid::new_v4();
        let key = StreamKey::guid(id);
        assert!(matches!(key, StreamKey::Guid(_)));
        assert_eq!(key.to_string(), id.to_string());
    }

    #[test]
    fn test_stream_key_string() {
        let key = StreamKey::string("my-key");
        assert!(matches!(key, StreamKey::String(_)));
        assert_eq!(key.to_string(), "my-key");
    }

    #[test]
    fn test_stream_key_integer() {
        let key = StreamKey::integer(42);
        assert!(matches!(key, StreamKey::Integer(42)));
        assert_eq!(key.to_string(), "42");
    }

    #[test]
    fn test_stream_key_conversions() {
        let key: StreamKey = "test".into();
        assert!(matches!(key, StreamKey::String(_)));

        let key: StreamKey = 123i64.into();
        assert!(matches!(key, StreamKey::Integer(123)));

        let key: StreamKey = 456i32.into();
        assert!(matches!(key, StreamKey::Integer(456)));
    }

    #[test]
    fn test_stream_key_hash_consistency() {
        let key1 = StreamKey::string("test");
        let key2 = StreamKey::string("test");
        assert_eq!(key1.get_hash_code(), key2.get_hash_code());

        let key3 = StreamKey::string("different");
        assert_ne!(key1.get_hash_code(), key3.get_hash_code());
    }

    #[test]
    fn test_stream_id_creation() {
        let stream_id = StreamId::create("my-namespace", "my-key");
        assert_eq!(stream_id.namespace(), "my-namespace");
        assert!(matches!(stream_id.key(), StreamKey::String(_)));
    }

    #[test]
    fn test_stream_id_with_guid() {
        let guid = Uuid::new_v4();
        let stream_id = StreamId::with_guid("ns", guid);
        assert!(matches!(stream_id.key(), StreamKey::Guid(_)));
    }

    #[test]
    fn test_stream_id_with_integer() {
        let stream_id = StreamId::with_integer("ns", 42);
        assert!(matches!(stream_id.key(), StreamKey::Integer(42)));
    }

    #[test]
    fn test_stream_id_display() {
        let stream_id = StreamId::create("orders", "order-123");
        assert_eq!(stream_id.to_string(), "orders/order-123");
    }

    #[test]
    fn test_stream_id_hash_consistency() {
        let id1 = StreamId::create("ns", "key");
        let id2 = StreamId::create("ns", "key");
        assert_eq!(id1.get_hash_code(), id2.get_hash_code());

        let id3 = StreamId::create("other", "key");
        assert_ne!(id1.get_hash_code(), id3.get_hash_code());
    }

    #[test]
    fn test_sequence_token_ordering() {
        let t1 = StreamSequenceToken::new(0, 0);
        let t2 = StreamSequenceToken::new(0, 1);
        let t3 = StreamSequenceToken::new(1, 0);

        assert!(t2.newer_than(&t1));
        assert!(t3.newer_than(&t2));
        assert!(t1.older_than(&t2));
    }

    #[test]
    fn test_sequence_token_next() {
        let t1 = StreamSequenceToken::new(5, 3);
        let t2 = t1.next();
        assert_eq!(t2.sequence_number, 5);
        assert_eq!(t2.event_index, 4);

        let t3 = t1.next_sequence();
        assert_eq!(t3.sequence_number, 6);
        assert_eq!(t3.event_index, 0);
    }

    #[test]
    fn test_sequence_token_display() {
        let token = StreamSequenceToken::new(10, 5);
        assert_eq!(token.to_string(), "10:5");
    }

    #[test]
    fn test_sequence_token_default() {
        let token = StreamSequenceToken::default();
        assert_eq!(token.sequence_number, 0);
        assert_eq!(token.event_index, 0);
    }

    #[test]
    fn test_qualified_stream_id() {
        let stream_id = StreamId::create("ns", "key");
        let qualified = QualifiedStreamId::new("MemoryProvider", stream_id);
        assert_eq!(qualified.provider_name, "MemoryProvider");
        assert_eq!(qualified.to_string(), "MemoryProvider:ns/key");
    }

    #[test]
    fn test_stream_id_serialization() {
        let stream_id = StreamId::create("orders", "order-123");
        let json = serde_json::to_string(&stream_id).unwrap();
        let deserialized: StreamId = serde_json::from_str(&json).unwrap();
        assert_eq!(stream_id, deserialized);
    }

    #[test]
    fn test_sequence_token_serialization() {
        let token = StreamSequenceToken::new(42, 7);
        let json = serde_json::to_string(&token).unwrap();
        let deserialized: StreamSequenceToken = serde_json::from_str(&json).unwrap();
        assert_eq!(token, deserialized);
    }
}
