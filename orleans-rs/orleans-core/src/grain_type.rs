//! GrainType - Type identifier for grain classes
//!
//! `GrainType` identifies the type of a grain (e.g., "HelloGrain", "UserGrain").
//! It wraps an `IdSpan` and provides methods for identifying system types.

use crate::IdSpan;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Prefix for system grain types
const SYSTEM_PREFIX: &str = "sys.";
/// Prefix for system target types (services)
const SYSTEM_TARGET_PREFIX: &str = "sys.svc.";
/// Prefix for user grain services
const GRAIN_SERVICE_PREFIX: &str = "sys.svc.user.";
/// Client type identifier
const CLIENT_PREFIX: &str = "sys.client";
/// Legacy grain prefix
const LEGACY_GRAIN_PREFIX: &str = "sys.grain.v1.";

/// Type identifier for grain classes.
///
/// `GrainType` wraps an `IdSpan` containing the UTF-8 encoded type name
/// (e.g., "MyApp.HelloGrain"). It provides utilities for:
/// - Identifying system vs user grain types
/// - Computing hash codes for consistent hashing
/// - Parsing and formatting type names
///
/// # Type Prefixes
///
/// | Prefix | Description |
/// |--------|-------------|
/// | `sys.` | System types |
/// | `sys.svc.` | System targets (services) |
/// | `sys.svc.user.` | User grain services |
/// | `sys.client` | Client connections |
/// | `sys.grain.v1.` | Legacy grains |
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct GrainType {
    value: IdSpan,
}

impl GrainType {
    /// Creates a new `GrainType` from a type name.
    ///
    /// # Arguments
    /// * `name` - The grain type name (e.g., "MyApp.HelloGrain")
    ///
    /// # Examples
    /// ```
    /// use orleans_core::GrainType;
    /// let grain_type = GrainType::create("MyApp.HelloGrain");
    /// assert_eq!(grain_type.as_str(), Some("MyApp.HelloGrain"));
    /// ```
    pub fn create(name: &str) -> Self {
        Self {
            value: IdSpan::from_str(name),
        }
    }

    /// Creates a `GrainType` from an `IdSpan`.
    pub fn from_id_span(span: IdSpan) -> Self {
        Self { value: span }
    }

    /// Returns the default (empty) grain type.
    pub fn default_type() -> Self {
        Self {
            value: IdSpan::empty(),
        }
    }

    /// Returns true if this is the default (empty) grain type.
    pub fn is_default(&self) -> bool {
        self.value.is_empty()
    }

    /// Returns the type name as a string slice.
    pub fn as_str(&self) -> Option<&str> {
        self.value.as_str()
    }

    /// Returns the raw bytes of the type name.
    pub fn as_bytes(&self) -> &[u8] {
        self.value.as_bytes()
    }

    /// Returns the underlying `IdSpan`.
    pub fn as_id_span(&self) -> &IdSpan {
        &self.value
    }

    /// Returns the pre-computed hash code.
    pub fn get_hash_code(&self) -> u32 {
        self.value.get_hash_code()
    }

    /// Returns the uniform hash code for consistent hashing.
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.value.get_uniform_hash_code()
    }

    /// Returns true if this is a system grain type (prefixed with "sys.").
    ///
    /// # Examples
    /// ```
    /// use orleans_core::GrainType;
    /// assert!(GrainType::create("sys.membership").is_system_type());
    /// assert!(!GrainType::create("MyApp.HelloGrain").is_system_type());
    /// ```
    pub fn is_system_type(&self) -> bool {
        self.as_str()
            .map(|s| s.starts_with(SYSTEM_PREFIX))
            .unwrap_or(false)
    }

    /// Returns true if this is a system target (service) type.
    ///
    /// # Examples
    /// ```
    /// use orleans_core::GrainType;
    /// assert!(GrainType::create("sys.svc.membership").is_system_target());
    /// assert!(!GrainType::create("sys.other").is_system_target());
    /// ```
    pub fn is_system_target(&self) -> bool {
        self.as_str()
            .map(|s| s.starts_with(SYSTEM_TARGET_PREFIX))
            .unwrap_or(false)
    }

    /// Returns true if this is a grain service type.
    pub fn is_grain_service(&self) -> bool {
        self.as_str()
            .map(|s| s.starts_with(GRAIN_SERVICE_PREFIX))
            .unwrap_or(false)
    }

    /// Returns true if this is a client type.
    ///
    /// # Examples
    /// ```
    /// use orleans_core::GrainType;
    /// assert!(GrainType::create("sys.client").is_client());
    /// assert!(GrainType::create("sys.client.gateway").is_client());
    /// assert!(!GrainType::create("MyApp.HelloGrain").is_client());
    /// ```
    pub fn is_client(&self) -> bool {
        self.as_str()
            .map(|s| s.starts_with(CLIENT_PREFIX))
            .unwrap_or(false)
    }

    /// Returns true if this is a legacy grain type.
    pub fn is_legacy_grain(&self) -> bool {
        self.as_str()
            .map(|s| s.starts_with(LEGACY_GRAIN_PREFIX))
            .unwrap_or(false)
    }

    /// Creates a system type with the given suffix.
    ///
    /// # Examples
    /// ```
    /// use orleans_core::GrainType;
    /// let sys_type = GrainType::system_type("membership");
    /// assert_eq!(sys_type.as_str(), Some("sys.membership"));
    /// ```
    pub fn system_type(name: &str) -> Self {
        Self::create(&format!("{}{}", SYSTEM_PREFIX, name))
    }

    /// Creates a system target (service) type with the given suffix.
    pub fn system_target(name: &str) -> Self {
        Self::create(&format!("{}{}", SYSTEM_TARGET_PREFIX, name))
    }

    /// Creates a grain service type with the given suffix.
    pub fn grain_service(name: &str) -> Self {
        Self::create(&format!("{}{}", GRAIN_SERVICE_PREFIX, name))
    }

    /// Creates a client type.
    pub fn client() -> Self {
        Self::create(CLIENT_PREFIX)
    }
}

impl Default for GrainType {
    fn default() -> Self {
        Self::default_type()
    }
}

impl fmt::Debug for GrainType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            Some(s) => write!(f, "GrainType({:?})", s),
            None if self.is_default() => write!(f, "GrainType(default)"),
            None => write!(f, "GrainType({:?})", self.as_bytes()),
        }
    }
}

impl fmt::Display for GrainType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            Some(s) => write!(f, "{}", s),
            None => write!(f, ""),
        }
    }
}

impl From<&str> for GrainType {
    fn from(s: &str) -> Self {
        Self::create(s)
    }
}

impl From<String> for GrainType {
    fn from(s: String) -> Self {
        Self::create(&s)
    }
}

impl From<IdSpan> for GrainType {
    fn from(span: IdSpan) -> Self {
        Self::from_id_span(span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let gt = GrainType::create("MyApp.HelloGrain");
        assert_eq!(gt.as_str(), Some("MyApp.HelloGrain"));
        assert!(!gt.is_default());
    }

    #[test]
    fn test_default() {
        let gt = GrainType::default();
        assert!(gt.is_default());
        assert!(gt.value.is_empty());
    }

    #[test]
    fn test_hash_code() {
        let gt1 = GrainType::create("TestGrain");
        let gt2 = GrainType::create("TestGrain");
        assert_eq!(gt1.get_hash_code(), gt2.get_hash_code());
        assert_eq!(gt1.get_uniform_hash_code(), gt2.get_uniform_hash_code());
    }

    #[test]
    fn test_equality() {
        let gt1 = GrainType::create("SameGrain");
        let gt2 = GrainType::create("SameGrain");
        let gt3 = GrainType::create("DifferentGrain");

        assert_eq!(gt1, gt2);
        assert_ne!(gt1, gt3);
    }

    #[test]
    fn test_is_system_type() {
        assert!(GrainType::create("sys.membership").is_system_type());
        assert!(GrainType::create("sys.").is_system_type());
        assert!(!GrainType::create("MyApp.Grain").is_system_type());
        assert!(!GrainType::create("system.NotReally").is_system_type());
    }

    #[test]
    fn test_is_system_target() {
        assert!(GrainType::create("sys.svc.membership").is_system_target());
        assert!(GrainType::create("sys.svc.").is_system_target());
        assert!(!GrainType::create("sys.other").is_system_target());
        assert!(!GrainType::create("MyApp.Grain").is_system_target());
    }

    #[test]
    fn test_is_grain_service() {
        assert!(GrainType::create("sys.svc.user.myservice").is_grain_service());
        assert!(!GrainType::create("sys.svc.other").is_grain_service());
    }

    #[test]
    fn test_is_client() {
        assert!(GrainType::create("sys.client").is_client());
        assert!(GrainType::create("sys.client.gateway").is_client());
        assert!(!GrainType::create("sys.other").is_client());
        assert!(!GrainType::create("MyApp.Client").is_client());
    }

    #[test]
    fn test_is_legacy_grain() {
        assert!(GrainType::create("sys.grain.v1.MyGrain").is_legacy_grain());
        assert!(!GrainType::create("sys.grain.v2.MyGrain").is_legacy_grain());
        assert!(!GrainType::create("MyApp.Grain").is_legacy_grain());
    }

    #[test]
    fn test_factory_methods() {
        let sys = GrainType::system_type("test");
        assert_eq!(sys.as_str(), Some("sys.test"));
        assert!(sys.is_system_type());

        let svc = GrainType::system_target("test");
        assert_eq!(svc.as_str(), Some("sys.svc.test"));
        assert!(svc.is_system_target());

        let grain_svc = GrainType::grain_service("test");
        assert_eq!(grain_svc.as_str(), Some("sys.svc.user.test"));
        assert!(grain_svc.is_grain_service());

        let client = GrainType::client();
        assert_eq!(client.as_str(), Some("sys.client"));
        assert!(client.is_client());
    }

    #[test]
    fn test_display() {
        let gt = GrainType::create("MyApp.HelloGrain");
        assert_eq!(format!("{}", gt), "MyApp.HelloGrain");

        let default = GrainType::default();
        assert_eq!(format!("{}", default), "");
    }

    #[test]
    fn test_debug() {
        let gt = GrainType::create("MyApp.HelloGrain");
        let debug = format!("{:?}", gt);
        assert!(debug.contains("MyApp.HelloGrain"));
    }

    #[test]
    fn test_from_conversions() {
        let from_str: GrainType = "FromStr".into();
        assert_eq!(from_str.as_str(), Some("FromStr"));

        let from_string: GrainType = String::from("FromString").into();
        assert_eq!(from_string.as_str(), Some("FromString"));

        let span = IdSpan::from_str("FromIdSpan");
        let from_span: GrainType = span.into();
        assert_eq!(from_span.as_str(), Some("FromIdSpan"));
    }

    #[test]
    fn test_as_id_span() {
        let gt = GrainType::create("TestGrain");
        let span = gt.as_id_span();
        assert_eq!(span.as_str(), Some("TestGrain"));
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_hash_stable(name in "[a-zA-Z][a-zA-Z0-9.]*") {
            let gt1 = GrainType::create(&name);
            let gt2 = GrainType::create(&name);
            prop_assert_eq!(gt1.get_hash_code(), gt2.get_hash_code());
        }

        #[test]
        fn prop_equality_consistent_with_hash(name in "[a-zA-Z][a-zA-Z0-9.]*") {
            let gt1 = GrainType::create(&name);
            let gt2 = GrainType::create(&name);
            let hash1 = gt1.get_hash_code();
            let hash2 = gt2.get_hash_code();
            prop_assert_eq!(gt1, gt2);
            prop_assert_eq!(hash1, hash2);
        }

        #[test]
        fn prop_system_prefix_detection(suffix in "[a-zA-Z0-9]+") {
            let sys_type = GrainType::create(&format!("sys.{}", suffix));
            prop_assert!(sys_type.is_system_type());

            let non_sys = GrainType::create(&format!("app.{}", suffix));
            prop_assert!(!non_sys.is_system_type());
        }
    }
}
