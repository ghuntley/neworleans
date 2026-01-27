//! ActivationId - Unique identifier for grain activations
//!
//! `ActivationId` identifies a specific activation (instance) of a grain.
//! While a `GrainId` identifies a logical grain, an `ActivationId` identifies
//! a particular physical instance of that grain on a silo.

use crate::GrainId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;
use xxhash_rust::xxh32;

/// Unique identifier for a grain activation.
///
/// An `ActivationId` distinguishes different physical instances of the same
/// logical grain. This is needed because:
/// - A grain may be deactivated and later reactivated (new activation)
/// - During failures, there may briefly be duplicate activations
/// - The directory tracks activations, not just grain IDs
///
/// # Creation Methods
///
/// - `new()` - Creates a random UUID-based activation ID
/// - `get_deterministic(grain_id)` - Creates a reproducible ID from a grain ID
///
/// # Examples
///
/// ```
/// use orleans_core::{ActivationId, GrainId};
///
/// // Create random activation ID
/// let act_id = ActivationId::new();
/// assert!(!act_id.is_default());
///
/// // Create deterministic activation ID
/// let grain_id = GrainId::create("TestGrain", "key-1");
/// let det_id = ActivationId::get_deterministic(&grain_id);
/// let det_id2 = ActivationId::get_deterministic(&grain_id);
/// assert_eq!(det_id, det_id2);
/// ```
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ActivationId {
    /// The underlying UUID
    key: Uuid,
}

impl ActivationId {
    /// Creates a new random `ActivationId`.
    ///
    /// Uses UUID v4 (random) for uniqueness.
    pub fn new() -> Self {
        Self { key: Uuid::new_v4() }
    }

    /// Creates an `ActivationId` from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self { key: uuid }
    }

    /// Creates the default (nil) activation ID.
    pub fn default_id() -> Self {
        Self { key: Uuid::nil() }
    }

    /// Creates a deterministic `ActivationId` from a `GrainId`.
    ///
    /// This is useful for scenarios where you need a reproducible activation ID
    /// based on the grain identity (e.g., for testing or deterministic placement).
    ///
    /// The ID is derived by hashing the grain ID's string representation
    /// and using it to seed a UUID v5 (name-based).
    pub fn get_deterministic(grain_id: &GrainId) -> Self {
        // Use UUID v5 (name-based, SHA-1) with a fixed namespace
        // Namespace: Orleans activation IDs
        const NAMESPACE: Uuid = Uuid::from_bytes([
            0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4,
            0x30, 0xc8,
        ]);

        let name = format!("{}", grain_id);
        let uuid = Uuid::new_v5(&NAMESPACE, name.as_bytes());
        Self { key: uuid }
    }

    /// Returns true if this is the default (nil) activation ID.
    pub fn is_default(&self) -> bool {
        self.key.is_nil()
    }

    /// Returns the underlying UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.key
    }

    /// Returns the UUID bytes.
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.key.as_bytes()
    }

    /// Returns the hash code for this activation ID.
    pub fn get_hash_code(&self) -> u32 {
        xxh32::xxh32(self.key.as_bytes(), 0)
    }

    /// Returns the uniform hash code (same as `get_hash_code`).
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.get_hash_code()
    }

    /// Parses an `ActivationId` from a UUID string.
    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        let uuid = Uuid::parse_str(s)?;
        Ok(Self { key: uuid })
    }
}

impl Default for ActivationId {
    fn default() -> Self {
        Self::default_id()
    }
}

impl FromStr for ActivationId {
    type Err = uuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Debug for ActivationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActivationId({})", self.key)
    }
}

impl fmt::Display for ActivationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.key)
    }
}

impl From<Uuid> for ActivationId {
    fn from(uuid: Uuid) -> Self {
        Self::from_uuid(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let act_id = ActivationId::new();
        assert!(!act_id.is_default());
        // Each new() should be unique
        let act_id2 = ActivationId::new();
        assert_ne!(act_id, act_id2);
    }

    #[test]
    fn test_from_uuid() {
        let uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let act_id = ActivationId::from_uuid(uuid);
        assert_eq!(act_id.as_uuid(), &uuid);
    }

    #[test]
    fn test_default() {
        let act_id = ActivationId::default();
        assert!(act_id.is_default());
        assert!(act_id.as_uuid().is_nil());
    }

    #[test]
    fn test_get_deterministic() {
        let grain_id = GrainId::create("TestGrain", "test-key");

        let act_id1 = ActivationId::get_deterministic(&grain_id);
        let act_id2 = ActivationId::get_deterministic(&grain_id);

        // Same grain ID should produce same activation ID
        assert_eq!(act_id1, act_id2);
        assert!(!act_id1.is_default());
    }

    #[test]
    fn test_get_deterministic_different_grains() {
        let grain_id1 = GrainId::create("TestGrain", "key-1");
        let grain_id2 = GrainId::create("TestGrain", "key-2");

        let act_id1 = ActivationId::get_deterministic(&grain_id1);
        let act_id2 = ActivationId::get_deterministic(&grain_id2);

        // Different grain IDs should produce different activation IDs
        assert_ne!(act_id1, act_id2);
    }

    #[test]
    fn test_hash_code_stable() {
        let uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let act_id1 = ActivationId::from_uuid(uuid);
        let act_id2 = ActivationId::from_uuid(uuid);

        assert_eq!(act_id1.get_hash_code(), act_id2.get_hash_code());
        assert_eq!(
            act_id1.get_uniform_hash_code(),
            act_id2.get_uniform_hash_code()
        );
    }

    #[test]
    fn test_equality() {
        let uuid = Uuid::new_v4();
        let act_id1 = ActivationId::from_uuid(uuid);
        let act_id2 = ActivationId::from_uuid(uuid);
        let act_id3 = ActivationId::new();

        assert_eq!(act_id1, act_id2);
        assert_ne!(act_id1, act_id3);
    }

    #[test]
    fn test_parse() {
        let act_id = ActivationId::parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        assert!(!act_id.is_default());

        let uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        assert_eq!(act_id.as_uuid(), &uuid);
    }

    #[test]
    fn test_parse_invalid() {
        let result = ActivationId::parse("not-a-uuid");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_str() {
        let act_id: ActivationId = "a1b2c3d4-e5f6-7890-abcd-ef1234567890".parse().unwrap();
        assert!(!act_id.is_default());
    }

    #[test]
    fn test_display_roundtrip() {
        let act_id = ActivationId::new();
        let display = format!("{}", act_id);
        let parsed: ActivationId = display.parse().unwrap();
        assert_eq!(act_id, parsed);
    }

    #[test]
    fn test_debug() {
        let uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let act_id = ActivationId::from_uuid(uuid);
        let debug = format!("{:?}", act_id);
        assert!(debug.contains("a1b2c3d4-e5f6-7890-abcd-ef1234567890"));
    }

    #[test]
    fn test_as_bytes() {
        let uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let act_id = ActivationId::from_uuid(uuid);
        assert_eq!(act_id.as_bytes(), uuid.as_bytes());
    }

    #[test]
    fn test_from_uuid_conversion() {
        let uuid = Uuid::new_v4();
        let act_id: ActivationId = uuid.into();
        assert_eq!(act_id.as_uuid(), &uuid);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_deterministic_is_deterministic(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,50}",
            key in "[a-zA-Z0-9_-]{1,50}"
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let act_id1 = ActivationId::get_deterministic(&grain_id);
            let act_id2 = ActivationId::get_deterministic(&grain_id);
            prop_assert_eq!(act_id1, act_id2);
        }

        #[test]
        fn prop_parse_display_roundtrip(bytes in prop::array::uniform16(any::<u8>())) {
            let uuid = Uuid::from_bytes(bytes);
            let act_id = ActivationId::from_uuid(uuid);
            let display = format!("{}", act_id);
            let parsed: Result<ActivationId, _> = display.parse();
            prop_assert!(parsed.is_ok());
            prop_assert_eq!(act_id, parsed.unwrap());
        }

        #[test]
        fn prop_hash_stable(bytes in prop::array::uniform16(any::<u8>())) {
            let uuid = Uuid::from_bytes(bytes);
            let act_id1 = ActivationId::from_uuid(uuid);
            let act_id2 = ActivationId::from_uuid(uuid);
            prop_assert_eq!(act_id1.get_hash_code(), act_id2.get_hash_code());
        }

        #[test]
        fn prop_new_always_unique(_seed in 0u64..1000) {
            // Each new() call should produce a unique ID
            let act_id1 = ActivationId::new();
            let act_id2 = ActivationId::new();
            prop_assert_ne!(act_id1, act_id2);
        }
    }
}
