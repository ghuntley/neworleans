//! Observer Grain ID - Identity for observers.
//!
//! Observer grain IDs have a special format that includes a client ID and
//! an observer-scoped ID separated by a '+' character:
//! `[ClientId]+[ObserverScopedId]`

use orleans_core::{GrainId, GrainType, IdSpan};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Separator between client ID and observer-scoped ID in the key.
const SEGMENT_SEPARATOR: char = '+';

/// Client grain type prefix.
const CLIENT_PREFIX: &str = "sys.client";

/// Observer grain type.
const OBSERVER_TYPE: &str = "sys.observer";

/// Observer grain ID - identifies an observer registration.
///
/// Format: `sys.observer/[ClientId]+[ObserverScopedId]`
///
/// # Examples
///
/// ```
/// use orleans_observers::ObserverGrainId;
///
/// // Create a new observer ID
/// let observer_id = ObserverGrainId::create("client-123");
/// assert!(ObserverGrainId::is_observer_grain_id(&observer_id.grain_id()));
/// ```
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ObserverGrainId {
    grain_id: GrainId,
}

impl ObserverGrainId {
    /// Creates a new observer grain ID for a client.
    ///
    /// # Arguments
    /// * `client_id` - The client identifier
    ///
    /// # Returns
    /// A new `ObserverGrainId` with a randomly generated observer-scoped ID.
    pub fn create(client_id: &str) -> Self {
        let scoped_id = Uuid::new_v4();
        Self::create_with_scoped_id(client_id, scoped_id)
    }

    /// Creates an observer grain ID with a specific scoped ID.
    ///
    /// # Arguments
    /// * `client_id` - The client identifier
    /// * `scoped_id` - The observer-scoped UUID
    pub fn create_with_scoped_id(client_id: &str, scoped_id: Uuid) -> Self {
        let key = format!(
            "{}{}{}",
            client_id,
            SEGMENT_SEPARATOR,
            scoped_id.as_simple()
        );
        Self {
            grain_id: GrainId::new(
                GrainType::create(OBSERVER_TYPE),
                IdSpan::from_str(&key),
            ),
        }
    }

    /// Checks if a grain ID is an observer grain ID.
    ///
    /// Observer grain IDs have:
    /// - A client-type grain type (starts with "sys.client" or "sys.observer")
    /// - A key containing the '+' separator
    pub fn is_observer_grain_id(grain_id: &GrainId) -> bool {
        let type_str = grain_id.grain_type().as_str().unwrap_or("");
        let has_observer_type = type_str == OBSERVER_TYPE || type_str.starts_with(CLIENT_PREFIX);

        let key_str = grain_id.key_as_str().unwrap_or("");
        let has_separator = key_str.contains(SEGMENT_SEPARATOR);

        has_observer_type && has_separator
    }

    /// Attempts to parse a grain ID as an observer grain ID.
    ///
    /// # Returns
    /// `Some(ObserverGrainId)` if the grain ID is a valid observer ID, `None` otherwise.
    pub fn try_parse(grain_id: &GrainId) -> Option<Self> {
        if Self::is_observer_grain_id(grain_id) {
            Some(Self {
                grain_id: grain_id.clone(),
            })
        } else {
            None
        }
    }

    /// Returns the underlying grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Consumes this observer ID and returns the underlying grain ID.
    pub fn into_grain_id(self) -> GrainId {
        self.grain_id
    }

    /// Returns the client ID portion of this observer ID.
    pub fn client_id(&self) -> Option<&str> {
        self.grain_id.key_as_str().and_then(|key| {
            key.find(SEGMENT_SEPARATOR).map(|pos| &key[..pos])
        })
    }

    /// Returns the observer-scoped ID portion (as a string).
    pub fn scoped_id(&self) -> Option<&str> {
        self.grain_id.key_as_str().and_then(|key| {
            key.find(SEGMENT_SEPARATOR).map(|pos| &key[pos + 1..])
        })
    }

    /// Returns the observer-scoped ID as a UUID, if valid.
    pub fn scoped_id_as_uuid(&self) -> Option<Uuid> {
        self.scoped_id().and_then(|s| Uuid::parse_str(s).ok())
    }

    /// Returns the uniform hash code for consistent hashing.
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.grain_id.get_uniform_hash_code()
    }
}

impl fmt::Debug for ObserverGrainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ObserverGrainId(client={:?}, scoped={:?})",
            self.client_id(),
            self.scoped_id()
        )
    }
}

impl fmt::Display for ObserverGrainId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.grain_id)
    }
}

impl From<ObserverGrainId> for GrainId {
    fn from(observer_id: ObserverGrainId) -> Self {
        observer_id.grain_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let observer_id = ObserverGrainId::create("client-123");
        assert!(ObserverGrainId::is_observer_grain_id(&observer_id.grain_id));
        assert_eq!(observer_id.client_id(), Some("client-123"));
        assert!(observer_id.scoped_id().is_some());
    }

    #[test]
    fn test_create_with_scoped_id() {
        let scoped_uuid = Uuid::parse_str("a1b2c3d4-e5f6-7890-abcd-ef1234567890").unwrap();
        let observer_id = ObserverGrainId::create_with_scoped_id("client-456", scoped_uuid);

        assert_eq!(observer_id.client_id(), Some("client-456"));
        assert_eq!(observer_id.scoped_id_as_uuid(), Some(scoped_uuid));
    }

    #[test]
    fn test_is_observer_grain_id_true() {
        let observer_id = ObserverGrainId::create("test-client");
        assert!(ObserverGrainId::is_observer_grain_id(&observer_id.grain_id));
    }

    #[test]
    fn test_is_observer_grain_id_false_no_separator() {
        let grain_id = GrainId::create("sys.observer", "no-separator");
        assert!(!ObserverGrainId::is_observer_grain_id(&grain_id));
    }

    #[test]
    fn test_is_observer_grain_id_false_wrong_type() {
        let grain_id = GrainId::create("MyGrain", "client+scoped");
        assert!(!ObserverGrainId::is_observer_grain_id(&grain_id));
    }

    #[test]
    fn test_try_parse_valid() {
        let observer_id = ObserverGrainId::create("client-789");
        let parsed = ObserverGrainId::try_parse(&observer_id.grain_id);
        assert!(parsed.is_some());
        assert_eq!(parsed.unwrap().client_id(), Some("client-789"));
    }

    #[test]
    fn test_try_parse_invalid() {
        let grain_id = GrainId::create("MyGrain", "not-an-observer");
        let parsed = ObserverGrainId::try_parse(&grain_id);
        assert!(parsed.is_none());
    }

    #[test]
    fn test_client_id() {
        let observer_id = ObserverGrainId::create("my-client");
        assert_eq!(observer_id.client_id(), Some("my-client"));
    }

    #[test]
    fn test_scoped_id() {
        let scoped_uuid = Uuid::new_v4();
        let observer_id = ObserverGrainId::create_with_scoped_id("client", scoped_uuid);

        let scoped_str = observer_id.scoped_id();
        assert!(scoped_str.is_some());
        assert_eq!(scoped_str.unwrap(), scoped_uuid.as_simple().to_string());
    }

    #[test]
    fn test_uniform_hash_code_consistent() {
        let scoped_uuid = Uuid::new_v4();
        let observer_id1 = ObserverGrainId::create_with_scoped_id("client", scoped_uuid);
        let observer_id2 = ObserverGrainId::create_with_scoped_id("client", scoped_uuid);

        assert_eq!(
            observer_id1.get_uniform_hash_code(),
            observer_id2.get_uniform_hash_code()
        );
    }

    #[test]
    fn test_uniform_hash_code_different() {
        let observer_id1 = ObserverGrainId::create("client-1");
        let observer_id2 = ObserverGrainId::create("client-2");

        // Different clients should (usually) have different hashes
        // Note: This could theoretically fail with extremely low probability
        assert_ne!(
            observer_id1.get_uniform_hash_code(),
            observer_id2.get_uniform_hash_code()
        );
    }

    #[test]
    fn test_debug_format() {
        let observer_id = ObserverGrainId::create("debug-client");
        let debug = format!("{:?}", observer_id);
        assert!(debug.contains("ObserverGrainId"));
        assert!(debug.contains("debug-client"));
    }

    #[test]
    fn test_display_format() {
        let observer_id = ObserverGrainId::create("display-client");
        let display = format!("{}", observer_id);
        assert!(display.contains("sys.observer"));
        assert!(display.contains("display-client"));
    }

    #[test]
    fn test_into_grain_id() {
        let observer_id = ObserverGrainId::create("convert-client");
        let grain_id_ref = observer_id.grain_id().clone();
        let grain_id: GrainId = observer_id.into_grain_id();
        assert_eq!(grain_id, grain_id_ref);
    }

    #[test]
    fn test_from_observer_grain_id() {
        let observer_id = ObserverGrainId::create("from-client");
        let grain_id_ref = observer_id.grain_id().clone();
        let grain_id: GrainId = observer_id.into();
        assert_eq!(grain_id, grain_id_ref);
    }

    #[test]
    fn test_equality() {
        let scoped_uuid = Uuid::new_v4();
        let observer_id1 = ObserverGrainId::create_with_scoped_id("eq-client", scoped_uuid);
        let observer_id2 = ObserverGrainId::create_with_scoped_id("eq-client", scoped_uuid);
        let observer_id3 = ObserverGrainId::create("eq-client"); // Different scoped ID

        assert_eq!(observer_id1, observer_id2);
        assert_ne!(observer_id1, observer_id3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;

        let scoped_uuid = Uuid::new_v4();
        let observer_id1 = ObserverGrainId::create_with_scoped_id("hash-client", scoped_uuid);
        let observer_id2 = ObserverGrainId::create_with_scoped_id("hash-client", scoped_uuid);

        let mut set = HashSet::new();
        set.insert(observer_id1.clone());

        assert!(set.contains(&observer_id2));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_create_always_valid_observer_id(client_id in "[a-zA-Z0-9_-]{1,50}") {
            let observer_id = ObserverGrainId::create(&client_id);
            prop_assert!(ObserverGrainId::is_observer_grain_id(&observer_id.grain_id));
            prop_assert_eq!(observer_id.client_id(), Some(client_id.as_str()));
            prop_assert!(observer_id.scoped_id().is_some());
        }

        #[test]
        fn prop_hash_consistent(client_id in "[a-zA-Z0-9_-]{1,50}") {
            let scoped_uuid = Uuid::new_v4();
            let observer_id1 = ObserverGrainId::create_with_scoped_id(&client_id, scoped_uuid);
            let observer_id2 = ObserverGrainId::create_with_scoped_id(&client_id, scoped_uuid);
            prop_assert_eq!(
                observer_id1.get_uniform_hash_code(),
                observer_id2.get_uniform_hash_code()
            );
        }

        #[test]
        fn prop_try_parse_roundtrip(client_id in "[a-zA-Z0-9_-]{1,50}") {
            let original = ObserverGrainId::create(&client_id);
            let grain_id = original.grain_id().clone();
            let parsed = ObserverGrainId::try_parse(&grain_id);
            prop_assert!(parsed.is_some());
            prop_assert_eq!(parsed.unwrap(), original);
        }
    }
}
