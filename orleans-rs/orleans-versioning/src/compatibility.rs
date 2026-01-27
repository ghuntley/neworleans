//! Compatibility strategies for grain interface versioning.
//!
//! Compatibility strategies determine whether a grain activation of one version
//! can serve requests from callers expecting a different version.

use std::fmt;

/// Trait for version compatibility checking.
///
/// Implementations determine whether a request for one version can be served
/// by an activation of another version.
pub trait CompatibilityDirector: Send + Sync + fmt::Debug {
    /// Checks if a grain at `current_version` can handle a request
    /// expecting `requested_version`.
    ///
    /// # Arguments
    ///
    /// * `requested_version` - The version the caller expects
    /// * `current_version` - The version of the available activation
    ///
    /// # Returns
    ///
    /// `true` if the current version can handle the requested version
    fn is_compatible(&self, requested_version: u16, current_version: u16) -> bool;

    /// Returns the name of this compatibility strategy.
    fn name(&self) -> &'static str;
}

/// Marker trait for compatibility strategies that can be used in placement.
pub trait CompatibilityStrategy: CompatibilityDirector {}

// Blanket implementation
impl<T: CompatibilityDirector> CompatibilityStrategy for T {}

/// Backward compatible strategy (default).
///
/// Newer versions can handle requests from older versions.
/// This is the most common strategy for additive API changes.
///
/// # Compatibility Rules
///
/// - `requested <= current` → compatible
/// - `requested > current` → not compatible
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{BackwardCompatible, CompatibilityDirector};
///
/// let strategy = BackwardCompatible;
///
/// // v2 can serve v1 requests (additive methods)
/// assert!(strategy.is_compatible(1, 2));
///
/// // v1 cannot serve v2 requests (missing methods)
/// assert!(!strategy.is_compatible(2, 1));
///
/// // Same version is always compatible
/// assert!(strategy.is_compatible(2, 2));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct BackwardCompatible;

impl CompatibilityDirector for BackwardCompatible {
    fn is_compatible(&self, requested_version: u16, current_version: u16) -> bool {
        requested_version <= current_version
    }

    fn name(&self) -> &'static str {
        "BackwardCompatible"
    }
}

/// Strict version compatible strategy.
///
/// Only exact version matches are compatible.
/// Use this for breaking API changes between versions.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{StrictVersionCompatible, CompatibilityDirector};
///
/// let strategy = StrictVersionCompatible;
///
/// // Only exact matches work
/// assert!(strategy.is_compatible(1, 1));
/// assert!(strategy.is_compatible(2, 2));
///
/// // Different versions are not compatible
/// assert!(!strategy.is_compatible(1, 2));
/// assert!(!strategy.is_compatible(2, 1));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct StrictVersionCompatible;

impl CompatibilityDirector for StrictVersionCompatible {
    fn is_compatible(&self, requested_version: u16, current_version: u16) -> bool {
        requested_version == current_version
    }

    fn name(&self) -> &'static str {
        "StrictVersionCompatible"
    }
}

/// All versions compatible strategy.
///
/// All versions are compatible with all other versions.
/// Use this for maximum flexibility during gradual upgrades.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{AllVersionsCompatible, CompatibilityDirector};
///
/// let strategy = AllVersionsCompatible;
///
/// // All versions work together
/// assert!(strategy.is_compatible(1, 2));
/// assert!(strategy.is_compatible(2, 1));
/// assert!(strategy.is_compatible(100, 1));
/// assert!(strategy.is_compatible(1, 100));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct AllVersionsCompatible;

impl CompatibilityDirector for AllVersionsCompatible {
    fn is_compatible(&self, _requested_version: u16, _current_version: u16) -> bool {
        true
    }

    fn name(&self) -> &'static str {
        "AllVersionsCompatible"
    }
}

/// Creates a compatibility director from a strategy name.
pub fn create_compatibility_director(name: &str) -> Option<Box<dyn CompatibilityDirector>> {
    match name {
        "BackwardCompatible" => Some(Box::new(BackwardCompatible)),
        "StrictVersionCompatible" => Some(Box::new(StrictVersionCompatible)),
        "AllVersionsCompatible" => Some(Box::new(AllVersionsCompatible)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backward_compatible_same_version() {
        let strategy = BackwardCompatible;
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(2, 2));
        assert!(strategy.is_compatible(100, 100));
    }

    #[test]
    fn test_backward_compatible_older_requests() {
        let strategy = BackwardCompatible;
        // Older versions can call newer versions
        assert!(strategy.is_compatible(1, 2));
        assert!(strategy.is_compatible(1, 3));
        assert!(strategy.is_compatible(2, 5));
    }

    #[test]
    fn test_backward_compatible_newer_requests() {
        let strategy = BackwardCompatible;
        // Newer versions cannot call older versions
        assert!(!strategy.is_compatible(2, 1));
        assert!(!strategy.is_compatible(3, 1));
        assert!(!strategy.is_compatible(5, 2));
    }

    #[test]
    fn test_strict_compatible_same_version() {
        let strategy = StrictVersionCompatible;
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(2, 2));
        assert!(strategy.is_compatible(100, 100));
    }

    #[test]
    fn test_strict_compatible_different_versions() {
        let strategy = StrictVersionCompatible;
        assert!(!strategy.is_compatible(1, 2));
        assert!(!strategy.is_compatible(2, 1));
        assert!(!strategy.is_compatible(1, 100));
        assert!(!strategy.is_compatible(100, 1));
    }

    #[test]
    fn test_all_versions_compatible() {
        let strategy = AllVersionsCompatible;
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(1, 2));
        assert!(strategy.is_compatible(2, 1));
        assert!(strategy.is_compatible(1, 100));
        assert!(strategy.is_compatible(100, 1));
    }

    #[test]
    fn test_strategy_names() {
        assert_eq!(BackwardCompatible.name(), "BackwardCompatible");
        assert_eq!(StrictVersionCompatible.name(), "StrictVersionCompatible");
        assert_eq!(AllVersionsCompatible.name(), "AllVersionsCompatible");
    }

    #[test]
    fn test_create_compatibility_director() {
        let backward = create_compatibility_director("BackwardCompatible").unwrap();
        assert_eq!(backward.name(), "BackwardCompatible");
        assert!(backward.is_compatible(1, 2));

        let strict = create_compatibility_director("StrictVersionCompatible").unwrap();
        assert_eq!(strict.name(), "StrictVersionCompatible");
        assert!(!strict.is_compatible(1, 2));

        let all = create_compatibility_director("AllVersionsCompatible").unwrap();
        assert_eq!(all.name(), "AllVersionsCompatible");
        assert!(all.is_compatible(2, 1));

        assert!(create_compatibility_director("Unknown").is_none());
    }

    #[test]
    fn test_version_zero() {
        // Version 0 is special (unspecified)
        let backward = BackwardCompatible;
        // 0 requesting 1 → 0 <= 1, compatible
        assert!(backward.is_compatible(0, 1));
        // 1 requesting 0 → 1 <= 0 is false
        assert!(!backward.is_compatible(1, 0));
    }

    // Property-based tests
    #[cfg(test)]
    mod property_tests {
        use super::*;
        use ::proptest::prelude::*;

        ::proptest::proptest! {
            #[test]
            fn backward_compatible_same_version_always_works(v in 0u16..=u16::MAX) {
                let strategy = BackwardCompatible;
                prop_assert!(strategy.is_compatible(v, v));
            }

            #[test]
            fn strict_compatible_same_version_always_works(v in 0u16..=u16::MAX) {
                let strategy = StrictVersionCompatible;
                prop_assert!(strategy.is_compatible(v, v));
            }

            #[test]
            fn all_versions_always_compatible(req in 0u16..=u16::MAX, cur in 0u16..=u16::MAX) {
                let strategy = AllVersionsCompatible;
                prop_assert!(strategy.is_compatible(req, cur));
            }

            #[test]
            fn backward_transitive(v1 in 1u16..1000, v2 in 1u16..1000, v3 in 1u16..1000) {
                let strategy = BackwardCompatible;
                // If v1 compatible with v2, and v2 compatible with v3
                // then v1 should be compatible with v3 (transitivity)
                if strategy.is_compatible(v1, v2) && strategy.is_compatible(v2, v3) {
                    prop_assert!(strategy.is_compatible(v1, v3),
                        "Transitivity failed: v1={}, v2={}, v3={}", v1, v2, v3);
                }
            }
        }
    }
}
