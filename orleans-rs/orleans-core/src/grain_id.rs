//! GrainId - Composite identifier for grains
//!
//! `GrainId` is the primary identifier for a grain, consisting of:
//! - `GrainType`: The type of the grain (e.g., "MyApp.HelloGrain")
//! - `IdSpan` key: The unique key within that type (e.g., "user-123")

use crate::{GrainType, IdSpan, OrleansError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Separator between type and key in string representation
const TYPE_KEY_SEPARATOR: char = '/';

/// Compound key separator
const COMPOUND_KEY_SEPARATOR: char = '+';

/// Composite identifier for a grain.
///
/// A `GrainId` uniquely identifies a grain across the entire cluster by combining:
/// - The grain's type (e.g., "MyApp.UserGrain")
/// - A unique key within that type (e.g., "user-12345")
///
/// # Key Types
///
/// Orleans supports several key types:
/// - **String**: Any UTF-8 string (e.g., "my-key")
/// - **Long**: 64-bit integer encoded as hex (e.g., "0000000000000001")
/// - **Guid**: 128-bit GUID in N-format (e.g., "a1b2c3d4e5f6...")
/// - **Compound**: Key + extension separated by '+' (e.g., "key+extension")
///
/// # String Format
///
/// `GrainId` can be serialized to/from a string in the format:
/// `{type}/{key}` (e.g., "MyApp.UserGrain/user-123")
///
/// # Examples
///
/// ```
/// use orleans_core::{GrainId, GrainType, IdSpan};
///
/// // Create with string key
/// let grain_id = GrainId::new(
///     GrainType::create("MyApp.UserGrain"),
///     IdSpan::from_str("user-123")
/// );
///
/// // Create from components
/// let grain_id = GrainId::create("MyApp.UserGrain", "user-456");
///
/// // Create with integer key
/// let grain_id = GrainId::with_integer_key("MyApp.CounterGrain", 42);
/// ```
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct GrainId {
    /// The type of the grain
    grain_type: GrainType,
    /// The unique key within the grain type
    key: IdSpan,
}

impl GrainId {
    /// Creates a new `GrainId` from a grain type and key.
    ///
    /// # Arguments
    /// * `grain_type` - The grain's type
    /// * `key` - The unique key within that type
    pub fn new(grain_type: GrainType, key: IdSpan) -> Self {
        Self { grain_type, key }
    }

    /// Creates a `GrainId` from string type and key.
    ///
    /// # Arguments
    /// * `grain_type` - The grain type name (e.g., "MyApp.UserGrain")
    /// * `key` - The string key (e.g., "user-123")
    pub fn create(grain_type: &str, key: &str) -> Self {
        Self {
            grain_type: GrainType::create(grain_type),
            key: IdSpan::from_str(key),
        }
    }

    /// Creates a `GrainId` with an integer key.
    ///
    /// The integer is encoded as a 16-character hexadecimal string.
    pub fn with_integer_key(grain_type: &str, key: i64) -> Self {
        // Encode as 16-character hex string (like Orleans)
        let hex_key = format!("{:016x}", key as u64);
        Self {
            grain_type: GrainType::create(grain_type),
            key: IdSpan::from_str(&hex_key),
        }
    }

    /// Creates a `GrainId` with a GUID key.
    ///
    /// The GUID is encoded as a 32-character hexadecimal string (N-format).
    pub fn with_guid_key(grain_type: &str, guid: uuid::Uuid) -> Self {
        let guid_key = guid.as_simple().to_string();
        Self {
            grain_type: GrainType::create(grain_type),
            key: IdSpan::from_str(&guid_key),
        }
    }

    /// Creates a `GrainId` with a compound key (key + extension).
    pub fn with_compound_key(grain_type: &str, key: &str, extension: &str) -> Self {
        let compound = format!("{}{}{}", key, COMPOUND_KEY_SEPARATOR, extension);
        Self {
            grain_type: GrainType::create(grain_type),
            key: IdSpan::from_str(&compound),
        }
    }

    /// Returns the default (empty) grain ID.
    pub fn default_id() -> Self {
        Self {
            grain_type: GrainType::default(),
            key: IdSpan::empty(),
        }
    }

    /// Returns true if this is the default (empty) grain ID.
    pub fn is_default(&self) -> bool {
        self.grain_type.is_default() && self.key.is_empty()
    }

    /// Returns the grain type.
    pub fn grain_type(&self) -> &GrainType {
        &self.grain_type
    }

    /// Returns the grain key.
    pub fn key(&self) -> &IdSpan {
        &self.key
    }

    /// Returns the key as a string, if valid UTF-8.
    pub fn key_as_str(&self) -> Option<&str> {
        self.key.as_str()
    }

    /// Attempts to parse the key as an integer.
    ///
    /// Returns `Some(i64)` if the key is a valid 16-character hex string.
    pub fn key_as_integer(&self) -> Option<i64> {
        self.key.as_str().and_then(|s| {
            if s.len() == 16 {
                u64::from_str_radix(s, 16).ok().map(|v| v as i64)
            } else {
                None
            }
        })
    }

    /// Attempts to parse the key as a GUID.
    ///
    /// Returns `Some(Uuid)` if the key is a valid 32-character hex string.
    pub fn key_as_guid(&self) -> Option<uuid::Uuid> {
        self.key.as_str().and_then(|s| {
            if s.len() == 32 {
                uuid::Uuid::parse_str(s).ok()
            } else {
                None
            }
        })
    }

    /// Checks if the key is a compound key (contains '+').
    pub fn is_compound_key(&self) -> bool {
        self.key
            .as_str()
            .map(|s| s.contains(COMPOUND_KEY_SEPARATOR))
            .unwrap_or(false)
    }

    /// Splits a compound key into (primary_key, extension).
    ///
    /// Returns `None` if the key is not compound.
    pub fn split_compound_key(&self) -> Option<(&str, &str)> {
        self.key.as_str().and_then(|s| {
            s.find(COMPOUND_KEY_SEPARATOR)
                .map(|pos| (&s[..pos], &s[pos + 1..]))
        })
    }

    /// Returns the uniform hash code for consistent hashing.
    ///
    /// This hash is used for:
    /// - Grain directory placement
    /// - Consistent routing
    /// - Load balancing
    ///
    /// The hash combines the grain type and key hashes using the formula:
    /// `type_hash * 31 + key_hash`
    pub fn get_uniform_hash_code(&self) -> u32 {
        // Combine type and key hashes (matches Orleans algorithm)
        self.grain_type
            .get_uniform_hash_code()
            .wrapping_mul(31)
            .wrapping_add(self.key.get_uniform_hash_code())
    }

    /// Returns true if this is a system grain.
    pub fn is_system(&self) -> bool {
        self.grain_type.is_system_type()
    }

    /// Returns true if this is a system target (service).
    pub fn is_system_target(&self) -> bool {
        self.grain_type.is_system_target()
    }

    /// Returns true if this is a client grain.
    pub fn is_client(&self) -> bool {
        self.grain_type.is_client()
    }

    /// Parses a `GrainId` from its string representation.
    ///
    /// Format: `{type}/{key}`
    pub fn parse(s: &str) -> Result<Self, OrleansError> {
        let pos = s
            .find(TYPE_KEY_SEPARATOR)
            .ok_or_else(|| OrleansError::InvalidGrainId(format!("missing separator in: {}", s)))?;

        let type_str = &s[..pos];
        let key_str = &s[pos + 1..];

        Ok(Self {
            grain_type: GrainType::create(type_str),
            key: IdSpan::from_str(key_str),
        })
    }
}

impl Default for GrainId {
    fn default() -> Self {
        Self::default_id()
    }
}

impl FromStr for GrainId {
    type Err = OrleansError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl fmt::Debug for GrainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "GrainId({}/{})",
            self.grain_type,
            self.key.as_str().unwrap_or("<binary>")
        )
    }
}

impl fmt::Display for GrainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.grain_type,
            TYPE_KEY_SEPARATOR,
            self.key.as_str().unwrap_or("<binary>")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let grain_id = GrainId::create("MyApp.UserGrain", "user-123");
        assert_eq!(grain_id.grain_type().as_str(), Some("MyApp.UserGrain"));
        assert_eq!(grain_id.key_as_str(), Some("user-123"));
    }

    #[test]
    fn test_new() {
        let grain_type = GrainType::create("TestGrain");
        let key = IdSpan::from_str("test-key");
        let grain_id = GrainId::new(grain_type.clone(), key.clone());

        assert_eq!(grain_id.grain_type(), &grain_type);
        assert_eq!(grain_id.key(), &key);
    }

    #[test]
    fn test_default() {
        let grain_id = GrainId::default();
        assert!(grain_id.is_default());
        assert!(grain_id.grain_type().is_default());
        assert!(grain_id.key().is_empty());
    }

    #[test]
    fn test_with_integer_key() {
        let grain_id = GrainId::with_integer_key("CounterGrain", 42);
        assert_eq!(grain_id.key_as_str(), Some("000000000000002a"));
        assert_eq!(grain_id.key_as_integer(), Some(42));

        let grain_id_neg = GrainId::with_integer_key("CounterGrain", -1);
        // -1 as u64 is max u64
        assert_eq!(grain_id_neg.key_as_integer(), Some(-1));
    }

    #[test]
    fn test_with_guid_key() {
        let guid = uuid::Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let grain_id = GrainId::with_guid_key("GuidGrain", guid);

        // N-format (no dashes)
        assert_eq!(
            grain_id.key_as_str(),
            Some("a1b2c3d4e5f67890abcdef1234567890")
        );
        assert_eq!(grain_id.key_as_guid(), Some(guid));
    }

    #[test]
    fn test_with_compound_key() {
        let grain_id = GrainId::with_compound_key("CompoundGrain", "primary", "extension");
        assert_eq!(grain_id.key_as_str(), Some("primary+extension"));
        assert!(grain_id.is_compound_key());
        assert_eq!(
            grain_id.split_compound_key(),
            Some(("primary", "extension"))
        );
    }

    #[test]
    fn test_not_compound_key() {
        let grain_id = GrainId::create("SimpleGrain", "simple-key");
        assert!(!grain_id.is_compound_key());
        assert_eq!(grain_id.split_compound_key(), None);
    }

    #[test]
    fn test_hash_code() {
        let grain_id1 = GrainId::create("TestGrain", "test-key");
        let grain_id2 = GrainId::create("TestGrain", "test-key");
        let grain_id3 = GrainId::create("TestGrain", "other-key");

        // Same IDs should have same hash
        assert_eq!(
            grain_id1.get_uniform_hash_code(),
            grain_id2.get_uniform_hash_code()
        );

        // Different keys should (usually) have different hashes
        assert_ne!(
            grain_id1.get_uniform_hash_code(),
            grain_id3.get_uniform_hash_code()
        );
    }

    #[test]
    fn test_equality() {
        let grain_id1 = GrainId::create("TestGrain", "test-key");
        let grain_id2 = GrainId::create("TestGrain", "test-key");
        let grain_id3 = GrainId::create("TestGrain", "other-key");
        let grain_id4 = GrainId::create("OtherGrain", "test-key");

        assert_eq!(grain_id1, grain_id2);
        assert_ne!(grain_id1, grain_id3);
        assert_ne!(grain_id1, grain_id4);
    }

    #[test]
    fn test_is_system() {
        let sys_grain = GrainId::create("sys.membership", "silo1");
        assert!(sys_grain.is_system());

        let user_grain = GrainId::create("MyApp.UserGrain", "user-1");
        assert!(!user_grain.is_system());
    }

    #[test]
    fn test_is_system_target() {
        let sys_target = GrainId::create("sys.svc.directory", "partition-1");
        assert!(sys_target.is_system_target());

        let user_grain = GrainId::create("MyApp.UserGrain", "user-1");
        assert!(!user_grain.is_system_target());
    }

    #[test]
    fn test_is_client() {
        let client = GrainId::create("sys.client", "client-1");
        assert!(client.is_client());

        let user_grain = GrainId::create("MyApp.UserGrain", "user-1");
        assert!(!user_grain.is_client());
    }

    #[test]
    fn test_parse() {
        let grain_id = GrainId::parse("MyApp.UserGrain/user-123").unwrap();
        assert_eq!(grain_id.grain_type().as_str(), Some("MyApp.UserGrain"));
        assert_eq!(grain_id.key_as_str(), Some("user-123"));
    }

    #[test]
    fn test_parse_empty_key() {
        let grain_id = GrainId::parse("MyApp.Grain/").unwrap();
        assert_eq!(grain_id.grain_type().as_str(), Some("MyApp.Grain"));
        assert!(grain_id.key().is_empty());
    }

    #[test]
    fn test_parse_invalid() {
        let result = GrainId::parse("no-separator");
        assert!(result.is_err());
    }

    #[test]
    fn test_from_str() {
        let grain_id: GrainId = "TestGrain/test-key".parse().unwrap();
        assert_eq!(grain_id.grain_type().as_str(), Some("TestGrain"));
        assert_eq!(grain_id.key_as_str(), Some("test-key"));
    }

    #[test]
    fn test_display_roundtrip() {
        let grain_id = GrainId::create("MyApp.TestGrain", "my-key");
        let display = format!("{}", grain_id);
        let parsed: GrainId = display.parse().unwrap();
        assert_eq!(grain_id, parsed);
    }

    #[test]
    fn test_debug() {
        let grain_id = GrainId::create("TestGrain", "test-key");
        let debug = format!("{:?}", grain_id);
        assert!(debug.contains("TestGrain"));
        assert!(debug.contains("test-key"));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_parse_display_roundtrip(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,50}",
            key in "[a-zA-Z0-9_-]{1,50}"
        ) {
            let grain_id = GrainId::create(&grain_type, &key);
            let display = format!("{}", grain_id);
            let parsed: Result<GrainId, _> = display.parse();
            prop_assert!(parsed.is_ok());
            prop_assert_eq!(grain_id, parsed.unwrap());
        }

        #[test]
        fn prop_hash_consistent(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,50}",
            key in "[a-zA-Z0-9_-]{1,50}"
        ) {
            let grain_id1 = GrainId::create(&grain_type, &key);
            let grain_id2 = GrainId::create(&grain_type, &key);
            prop_assert_eq!(grain_id1.get_uniform_hash_code(), grain_id2.get_uniform_hash_code());
        }

        #[test]
        fn prop_equality_consistent_with_hash(
            grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,50}",
            key in "[a-zA-Z0-9_-]{1,50}"
        ) {
            let grain_id1 = GrainId::create(&grain_type, &key);
            let grain_id2 = GrainId::create(&grain_type, &key);
            let hash1 = grain_id1.get_uniform_hash_code();
            let hash2 = grain_id2.get_uniform_hash_code();
            prop_assert_eq!(grain_id1, grain_id2);
            prop_assert_eq!(hash1, hash2);
        }

        #[test]
        fn prop_integer_key_roundtrip(key in any::<i64>()) {
            let grain_id = GrainId::with_integer_key("TestGrain", key);
            let recovered = grain_id.key_as_integer();
            prop_assert_eq!(recovered, Some(key));
        }

        #[test]
        fn prop_compound_key_split(
            primary in "[a-zA-Z0-9_-]{1,20}",
            extension in "[a-zA-Z0-9_-]{1,20}"
        ) {
            let grain_id = GrainId::with_compound_key("TestGrain", &primary, &extension);
            prop_assert!(grain_id.is_compound_key());
            let (p, e) = grain_id.split_compound_key().unwrap();
            prop_assert_eq!(p, primary.as_str());
            prop_assert_eq!(e, extension.as_str());
        }
    }
}
