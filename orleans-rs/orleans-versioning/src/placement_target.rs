//! Placement target with version information.

use std::collections::HashMap;
use orleans_core::{GrainId, GrainType};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A placement target with version information for version-aware grain placement.
///
/// This structure carries the information needed to place a grain activation,
/// including optional version information for heterogeneous deployments.
///
/// # Version-Aware vs Version-Unaware
///
/// - **Version-Aware** (`interface_version > 0`): The placement service filters
///   silos by version compatibility before selecting one.
///
/// - **Version-Unaware** (`interface_version == 0`): All silos supporting the
///   grain type are eligible, regardless of version. This is the backward
///   compatibility mode for grains without version attributes.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::PlacementTarget;
/// use orleans_core::{GrainId, GrainType, IdSpan};
///
/// let grain_type = GrainType::create("my.grain");
/// let grain_id = GrainId::new(grain_type, IdSpan::from_str("key1"));
/// let interface_type = GrainType::create("IMyGrain");
///
/// // Create a version-aware placement target
/// let target = PlacementTarget::new(grain_id, interface_type)
///     .with_version(2);
///
/// assert!(target.is_version_aware());
/// assert_eq!(target.interface_version(), 2);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementTarget {
    /// The grain identity to place.
    grain_id: GrainId,

    /// The interface type being invoked.
    interface_type: GrainType,

    /// The interface version requested (0 = unspecified/version-unaware).
    interface_version: u16,

    /// Additional request context data for placement decisions.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    request_context: HashMap<String, Value>,
}

impl PlacementTarget {
    /// Creates a new placement target without version information.
    ///
    /// This creates a version-unaware target (version = 0).
    pub fn new(grain_id: GrainId, interface_type: GrainType) -> Self {
        Self {
            grain_id,
            interface_type,
            interface_version: 0,
            request_context: HashMap::new(),
        }
    }

    /// Creates a new placement target with version information.
    pub fn new_with_version(
        grain_id: GrainId,
        interface_type: GrainType,
        interface_version: u16,
    ) -> Self {
        Self {
            grain_id,
            interface_type,
            interface_version,
            request_context: HashMap::new(),
        }
    }

    /// Sets the interface version.
    pub fn with_version(mut self, version: u16) -> Self {
        self.interface_version = version;
        self
    }

    /// Adds a request context value.
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.request_context.insert(key.into(), value.into());
        self
    }

    /// Adds multiple request context values.
    pub fn with_context_data(mut self, data: HashMap<String, Value>) -> Self {
        self.request_context.extend(data);
        self
    }

    /// Gets the grain identity.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Gets the interface type.
    pub fn interface_type(&self) -> &GrainType {
        &self.interface_type
    }

    /// Gets the interface version.
    ///
    /// Returns 0 if no version was specified (version-unaware mode).
    pub fn interface_version(&self) -> u16 {
        self.interface_version
    }

    /// Gets the request context data.
    pub fn request_context(&self) -> &HashMap<String, Value> {
        &self.request_context
    }

    /// Gets a mutable reference to the request context data.
    pub fn request_context_mut(&mut self) -> &mut HashMap<String, Value> {
        &mut self.request_context
    }

    /// Gets a value from the request context.
    pub fn get_context(&self, key: &str) -> Option<&Value> {
        self.request_context.get(key)
    }

    /// Checks if this target is version-aware.
    ///
    /// Returns `true` if a non-zero version was specified.
    pub fn is_version_aware(&self) -> bool {
        self.interface_version > 0
    }

    /// Gets the grain type from the grain identity.
    pub fn grain_type(&self) -> &GrainType {
        self.grain_id.grain_type()
    }
}

impl From<GrainId> for PlacementTarget {
    fn from(grain_id: GrainId) -> Self {
        let interface_type = grain_id.grain_type().clone();
        Self::new(grain_id, interface_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::IdSpan;

    fn create_grain_id(name: &str, key: &str) -> GrainId {
        GrainId::new(GrainType::create(name), IdSpan::from_str(key))
    }

    fn create_interface(name: &str) -> GrainType {
        GrainType::create(name)
    }

    #[test]
    fn test_new_is_version_unaware() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id.clone(), interface.clone());

        assert_eq!(target.grain_id(), &grain_id);
        assert_eq!(target.interface_type(), &interface);
        assert_eq!(target.interface_version(), 0);
        assert!(!target.is_version_aware());
    }

    #[test]
    fn test_with_version() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id, interface)
            .with_version(2);

        assert_eq!(target.interface_version(), 2);
        assert!(target.is_version_aware());
    }

    #[test]
    fn test_new_with_version() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new_with_version(grain_id.clone(), interface.clone(), 3);

        assert_eq!(target.interface_version(), 3);
        assert!(target.is_version_aware());
    }

    #[test]
    fn test_request_context() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id, interface)
            .with_context("user_id", "user123")
            .with_context("tenant", "tenant1");

        assert_eq!(target.request_context().len(), 2);
        assert_eq!(target.get_context("user_id"), Some(&Value::String("user123".to_string())));
        assert_eq!(target.get_context("tenant"), Some(&Value::String("tenant1".to_string())));
    }

    #[test]
    fn test_with_context_data() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let mut data = HashMap::new();
        data.insert("key1".to_string(), Value::String("value1".to_string()));
        data.insert("key2".to_string(), Value::Number(42.into()));

        let target = PlacementTarget::new(grain_id, interface)
            .with_context_data(data);

        assert_eq!(target.request_context().len(), 2);
        assert_eq!(target.get_context("key1"), Some(&Value::String("value1".to_string())));
        assert_eq!(target.get_context("key2"), Some(&Value::Number(42.into())));
    }

    #[test]
    fn test_grain_type() {
        let grain_type = GrainType::create("MyGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id, interface);
        assert_eq!(target.grain_type(), &grain_type);
    }

    #[test]
    fn test_from_grain_id() {
        let grain_type = GrainType::create("MyGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));

        let target: PlacementTarget = grain_id.clone().into();

        assert_eq!(target.grain_id(), &grain_id);
        assert_eq!(target.interface_type(), &grain_type);
        assert!(!target.is_version_aware());
    }

    #[test]
    fn test_request_context_mut() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let mut target = PlacementTarget::new(grain_id, interface);
        target.request_context_mut().insert("key".to_string(), Value::Bool(true));

        assert_eq!(target.get_context("key"), Some(&Value::Bool(true)));
    }

    #[test]
    fn test_serialization() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id, interface)
            .with_version(2)
            .with_context("test", "value");

        let json = serde_json::to_string(&target).unwrap();
        let deserialized: PlacementTarget = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.interface_version(), 2);
        assert_eq!(deserialized.get_context("test"), Some(&Value::String("value".to_string())));
    }

    #[test]
    fn test_version_zero_is_unaware() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        // Even if explicitly set to 0, it should be version-unaware
        let target = PlacementTarget::new(grain_id, interface)
            .with_version(0);

        assert!(!target.is_version_aware());
    }

    #[test]
    fn test_builder_chain() {
        let grain_id = create_grain_id("MyGrain", "key1");
        let interface = create_interface("IMyGrain");

        let target = PlacementTarget::new(grain_id.clone(), interface.clone())
            .with_version(3)
            .with_context("a", "1")
            .with_context("b", "2");

        assert_eq!(target.interface_version(), 3);
        assert_eq!(target.request_context().len(), 2);
    }

    // Property-based tests
    #[cfg(test)]
    mod property_tests {
        use super::*;
        use ::proptest::prelude::*;

        ::proptest::proptest! {
            #[test]
            fn version_aware_iff_nonzero(version in 0u16..=u16::MAX) {
                let grain_id = create_grain_id("TestGrain", "key1");
                let interface = create_interface("ITestGrain");

                let target = PlacementTarget::new(grain_id, interface)
                    .with_version(version);

                prop_assert_eq!(target.is_version_aware(), version > 0);
            }

            #[test]
            fn context_preserved(
                key in "[a-z]{1,10}",
                value in "[a-z0-9]{1,20}"
            ) {
                let grain_id = create_grain_id("TestGrain", "key1");
                let interface = create_interface("ITestGrain");

                let target = PlacementTarget::new(grain_id, interface)
                    .with_context(key.clone(), value.clone());

                prop_assert_eq!(
                    target.get_context(&key),
                    Some(&Value::String(value))
                );
            }
        }
    }
}
