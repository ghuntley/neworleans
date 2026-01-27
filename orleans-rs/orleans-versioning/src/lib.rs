//! # Orleans Versioning
//!
//! Interface versioning and compatibility system for heterogeneous Orleans cluster deployments.
//!
//! This crate enables rolling upgrades without stopping the cluster by combining:
//! - **Interface versions**: Declare versions on grain interfaces
//! - **Compatibility strategies**: Control which versions can communicate
//! - **Version selectors**: Choose which version to activate
//!
//! ## Example
//!
//! ```rust
//! use orleans_versioning::{
//!     CompatibilityDirector, VersionSelector,
//!     BackwardCompatible, AllVersionsCompatible,
//!     GrainVersioningOptions,
//! };
//!
//! // Create versioning options with default strategies
//! let options = GrainVersioningOptions::default();
//! assert_eq!(options.default_compatibility_strategy(), "BackwardCompatible");
//! assert_eq!(options.default_version_selector_strategy(), "AllCompatibleVersions");
//!
//! // Check compatibility with backward compatible strategy
//! let strategy = BackwardCompatible;
//! assert!(strategy.is_compatible(1, 2)); // v1 can call v2
//! assert!(!strategy.is_compatible(2, 1)); // v2 cannot call v1
//! ```

mod error;
mod options;
mod compatibility;
mod selector;
mod manifest;
mod manager;
mod placement_target;

pub use error::{VersionError, VersionResult};
pub use options::GrainVersioningOptions;
pub use compatibility::{
    CompatibilityStrategy, CompatibilityDirector,
    BackwardCompatible, StrictVersionCompatible, AllVersionsCompatible,
};
pub use selector::{
    VersionSelector, VersionSelectorStrategy,
    MinimumVersionSelector, LatestVersionSelector, AllCompatibleVersionsSelector,
};
pub use manifest::GrainVersionManifest;
pub use manager::{
    CompatibilityDirectorManager, VersionSelectorManager, CachedVersionSelectorManager,
    CachedEntry, SuitableSilosResult,
};
pub use placement_target::PlacementTarget;

/// Interface version type (16-bit unsigned integer).
///
/// Version 0 indicates "no version specified" for backward compatibility.
pub type InterfaceVersion = u16;

/// Well-known grain interface properties related to versioning.
pub mod properties {
    /// Property key for interface version in grain manifest.
    pub const VERSION: &str = "version";

    /// Property key for type hash in grain manifest.
    pub const TYPE_HASH: &str = "type-hash";
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};

    #[test]
    fn test_interface_version_is_u16() {
        let version: InterfaceVersion = 42;
        assert_eq!(std::mem::size_of_val(&version), 2);
    }

    #[test]
    fn test_version_zero_means_unspecified() {
        let unspecified: InterfaceVersion = 0;
        let specified: InterfaceVersion = 1;
        assert_eq!(unspecified, 0);
        assert_ne!(specified, 0);
    }

    #[test]
    fn test_backward_compatible_default() {
        let strategy = BackwardCompatible;
        // v1 can call v2 (older calling newer)
        assert!(strategy.is_compatible(1, 2));
        // v2 cannot call v1 (newer calling older)
        assert!(!strategy.is_compatible(2, 1));
        // Same version is compatible
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(2, 2));
    }

    #[test]
    fn test_strict_compatible() {
        let strategy = StrictVersionCompatible;
        // Only exact matches are compatible
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(2, 2));
        assert!(!strategy.is_compatible(1, 2));
        assert!(!strategy.is_compatible(2, 1));
    }

    #[test]
    fn test_all_versions_compatible() {
        let strategy = AllVersionsCompatible;
        // All versions are compatible with all others
        assert!(strategy.is_compatible(1, 1));
        assert!(strategy.is_compatible(1, 2));
        assert!(strategy.is_compatible(2, 1));
        assert!(strategy.is_compatible(100, 1));
    }

    #[test]
    fn test_minimum_version_selector() {
        let selector = MinimumVersionSelector;
        let available = vec![1, 2, 3, 4, 5];
        let compatibility = BackwardCompatible;

        // Requesting v2 with backward compatibility: v2, v3, v4, v5 are compatible
        // Minimum selector returns the smallest
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![2]);
    }

    #[test]
    fn test_latest_version_selector() {
        let selector = LatestVersionSelector;
        let available = vec![1, 2, 3, 4, 5];
        let compatibility = BackwardCompatible;

        // Requesting v2 with backward compatibility: v2, v3, v4, v5 are compatible
        // Latest selector returns the largest
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_all_compatible_versions_selector() {
        let selector = AllCompatibleVersionsSelector;
        let available = vec![1, 2, 3, 4, 5];
        let compatibility = BackwardCompatible;

        // Requesting v2 with backward compatibility: v2, v3, v4, v5 are compatible
        // All compatible selector returns all matching versions
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![2, 3, 4, 5]);
    }

    #[test]
    fn test_options_defaults() {
        let options = GrainVersioningOptions::default();
        assert_eq!(options.default_compatibility_strategy(), "BackwardCompatible");
        assert_eq!(options.default_version_selector_strategy(), "AllCompatibleVersions");
    }

    #[test]
    fn test_placement_target_creation() {
        let grain_type = GrainType::create("test.grain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainType::create("test.interface");

        let target = PlacementTarget::new(grain_id.clone(), interface_type.clone())
            .with_version(2);

        assert_eq!(target.grain_id(), &grain_id);
        assert_eq!(target.interface_type(), &interface_type);
        assert_eq!(target.interface_version(), 2);
    }

    #[test]
    fn test_placement_target_version_aware() {
        let grain_type = GrainType::create("test.grain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainType::create("test.interface");

        let no_version = PlacementTarget::new(grain_id.clone(), interface_type.clone());
        assert!(!no_version.is_version_aware());

        let with_version = no_version.with_version(1);
        assert!(with_version.is_version_aware());
    }
}
