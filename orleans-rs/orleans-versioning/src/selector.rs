//! Version selector strategies for grain activation.
//!
//! Version selectors determine which compatible version(s) to use when
//! multiple versions are available in the cluster.

use std::fmt;
use crate::compatibility::CompatibilityDirector;

/// Trait for version selection.
///
/// Implementations determine which version(s) to use when multiple
/// compatible versions are available.
pub trait VersionSelector: Send + Sync + fmt::Debug {
    /// Gets suitable versions from the available versions.
    ///
    /// # Arguments
    ///
    /// * `requested_version` - The version the caller expects
    /// * `available_versions` - Versions available in the cluster
    /// * `compatibility` - The compatibility strategy to use
    ///
    /// # Returns
    ///
    /// A list of suitable versions (may be empty if none are compatible)
    fn get_suitable_versions(
        &self,
        requested_version: u16,
        available_versions: &[u16],
        compatibility: &dyn CompatibilityDirector,
    ) -> Vec<u16>;

    /// Returns the name of this selector strategy.
    fn name(&self) -> &'static str;
}

/// Marker trait for version selector strategies.
pub trait VersionSelectorStrategy: VersionSelector {}

// Blanket implementation
impl<T: VersionSelector> VersionSelectorStrategy for T {}

/// Minimum version selector.
///
/// Always selects the lowest compatible version.
/// Use this for conservative rollouts where you want to maximize
/// compatibility with older silos.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{
///     MinimumVersionSelector, VersionSelector,
///     BackwardCompatible, CompatibilityDirector,
/// };
///
/// let selector = MinimumVersionSelector;
/// let compatibility = BackwardCompatible;
/// let available = vec![1, 2, 3, 4, 5];
///
/// // Requesting v2 with backward compatibility:
/// // Compatible versions are 2, 3, 4, 5
/// // Minimum selector returns 2
/// let result = selector.get_suitable_versions(2, &available, &compatibility);
/// assert_eq!(result, vec![2]);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct MinimumVersionSelector;

impl VersionSelector for MinimumVersionSelector {
    fn get_suitable_versions(
        &self,
        requested_version: u16,
        available_versions: &[u16],
        compatibility: &dyn CompatibilityDirector,
    ) -> Vec<u16> {
        available_versions
            .iter()
            .copied()
            .filter(|&v| compatibility.is_compatible(requested_version, v))
            .min()
            .map(|v| vec![v])
            .unwrap_or_default()
    }

    fn name(&self) -> &'static str {
        "MinimumVersion"
    }
}

/// Latest version selector.
///
/// Always selects the highest compatible version.
/// Use this to encourage quick adoption of new versions.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{
///     LatestVersionSelector, VersionSelector,
///     BackwardCompatible, CompatibilityDirector,
/// };
///
/// let selector = LatestVersionSelector;
/// let compatibility = BackwardCompatible;
/// let available = vec![1, 2, 3, 4, 5];
///
/// // Requesting v2 with backward compatibility:
/// // Compatible versions are 2, 3, 4, 5
/// // Latest selector returns 5
/// let result = selector.get_suitable_versions(2, &available, &compatibility);
/// assert_eq!(result, vec![5]);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct LatestVersionSelector;

impl VersionSelector for LatestVersionSelector {
    fn get_suitable_versions(
        &self,
        requested_version: u16,
        available_versions: &[u16],
        compatibility: &dyn CompatibilityDirector,
    ) -> Vec<u16> {
        available_versions
            .iter()
            .copied()
            .filter(|&v| compatibility.is_compatible(requested_version, v))
            .max()
            .map(|v| vec![v])
            .unwrap_or_default()
    }

    fn name(&self) -> &'static str {
        "LatestVersion"
    }
}

/// All compatible versions selector (default).
///
/// Returns all compatible versions.
/// Use this for load distribution across versions during gradual upgrades.
///
/// # Example
///
/// ```rust
/// use orleans_versioning::{
///     AllCompatibleVersionsSelector, VersionSelector,
///     BackwardCompatible, CompatibilityDirector,
/// };
///
/// let selector = AllCompatibleVersionsSelector;
/// let compatibility = BackwardCompatible;
/// let available = vec![1, 2, 3, 4, 5];
///
/// // Requesting v2 with backward compatibility:
/// // Compatible versions are 2, 3, 4, 5
/// // All compatible selector returns all of them
/// let result = selector.get_suitable_versions(2, &available, &compatibility);
/// assert_eq!(result, vec![2, 3, 4, 5]);
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct AllCompatibleVersionsSelector;

impl VersionSelector for AllCompatibleVersionsSelector {
    fn get_suitable_versions(
        &self,
        requested_version: u16,
        available_versions: &[u16],
        compatibility: &dyn CompatibilityDirector,
    ) -> Vec<u16> {
        available_versions
            .iter()
            .copied()
            .filter(|&v| compatibility.is_compatible(requested_version, v))
            .collect()
    }

    fn name(&self) -> &'static str {
        "AllCompatibleVersions"
    }
}

/// Creates a version selector from a strategy name.
pub fn create_version_selector(name: &str) -> Option<Box<dyn VersionSelector>> {
    match name {
        "MinimumVersion" => Some(Box::new(MinimumVersionSelector)),
        "LatestVersion" => Some(Box::new(LatestVersionSelector)),
        "AllCompatibleVersions" => Some(Box::new(AllCompatibleVersionsSelector)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compatibility::{BackwardCompatible, StrictVersionCompatible, AllVersionsCompatible};

    #[test]
    fn test_minimum_selector_backward_compatible() {
        let selector = MinimumVersionSelector;
        let compatibility = BackwardCompatible;
        let available = vec![1, 2, 3, 4, 5];

        // Request v2: compatible with 2, 3, 4, 5. Min = 2
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![2]);

        // Request v1: compatible with 1, 2, 3, 4, 5. Min = 1
        let result = selector.get_suitable_versions(1, &available, &compatibility);
        assert_eq!(result, vec![1]);

        // Request v5: compatible with 5. Min = 5
        let result = selector.get_suitable_versions(5, &available, &compatibility);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_minimum_selector_no_compatible() {
        let selector = MinimumVersionSelector;
        let compatibility = BackwardCompatible;
        let available = vec![1, 2, 3];

        // Request v4: none compatible
        let result = selector.get_suitable_versions(4, &available, &compatibility);
        assert!(result.is_empty());
    }

    #[test]
    fn test_latest_selector_backward_compatible() {
        let selector = LatestVersionSelector;
        let compatibility = BackwardCompatible;
        let available = vec![1, 2, 3, 4, 5];

        // Request v2: compatible with 2, 3, 4, 5. Max = 5
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![5]);

        // Request v1: compatible with 1, 2, 3, 4, 5. Max = 5
        let result = selector.get_suitable_versions(1, &available, &compatibility);
        assert_eq!(result, vec![5]);

        // Request v5: compatible with 5. Max = 5
        let result = selector.get_suitable_versions(5, &available, &compatibility);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_latest_selector_no_compatible() {
        let selector = LatestVersionSelector;
        let compatibility = BackwardCompatible;
        let available = vec![1, 2, 3];

        // Request v4: none compatible
        let result = selector.get_suitable_versions(4, &available, &compatibility);
        assert!(result.is_empty());
    }

    #[test]
    fn test_all_compatible_selector_backward_compatible() {
        let selector = AllCompatibleVersionsSelector;
        let compatibility = BackwardCompatible;
        let available = vec![1, 2, 3, 4, 5];

        // Request v2: compatible with 2, 3, 4, 5
        let result = selector.get_suitable_versions(2, &available, &compatibility);
        assert_eq!(result, vec![2, 3, 4, 5]);

        // Request v1: compatible with 1, 2, 3, 4, 5
        let result = selector.get_suitable_versions(1, &available, &compatibility);
        assert_eq!(result, vec![1, 2, 3, 4, 5]);

        // Request v5: compatible with 5
        let result = selector.get_suitable_versions(5, &available, &compatibility);
        assert_eq!(result, vec![5]);
    }

    #[test]
    fn test_selectors_with_strict_compatible() {
        let min_selector = MinimumVersionSelector;
        let max_selector = LatestVersionSelector;
        let all_selector = AllCompatibleVersionsSelector;
        let compatibility = StrictVersionCompatible;
        let available = vec![1, 2, 3, 4, 5];

        // Request v3: only 3 is compatible
        assert_eq!(min_selector.get_suitable_versions(3, &available, &compatibility), vec![3]);
        assert_eq!(max_selector.get_suitable_versions(3, &available, &compatibility), vec![3]);
        assert_eq!(all_selector.get_suitable_versions(3, &available, &compatibility), vec![3]);

        // Request v6: none compatible
        assert!(min_selector.get_suitable_versions(6, &available, &compatibility).is_empty());
        assert!(max_selector.get_suitable_versions(6, &available, &compatibility).is_empty());
        assert!(all_selector.get_suitable_versions(6, &available, &compatibility).is_empty());
    }

    #[test]
    fn test_selectors_with_all_versions_compatible() {
        let min_selector = MinimumVersionSelector;
        let max_selector = LatestVersionSelector;
        let all_selector = AllCompatibleVersionsSelector;
        let compatibility = AllVersionsCompatible;
        let available = vec![1, 2, 3, 4, 5];

        // Request any version: all are compatible
        assert_eq!(min_selector.get_suitable_versions(100, &available, &compatibility), vec![1]);
        assert_eq!(max_selector.get_suitable_versions(100, &available, &compatibility), vec![5]);
        assert_eq!(all_selector.get_suitable_versions(100, &available, &compatibility), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_selector_names() {
        assert_eq!(MinimumVersionSelector.name(), "MinimumVersion");
        assert_eq!(LatestVersionSelector.name(), "LatestVersion");
        assert_eq!(AllCompatibleVersionsSelector.name(), "AllCompatibleVersions");
    }

    #[test]
    fn test_create_version_selector() {
        let min = create_version_selector("MinimumVersion").unwrap();
        assert_eq!(min.name(), "MinimumVersion");

        let max = create_version_selector("LatestVersion").unwrap();
        assert_eq!(max.name(), "LatestVersion");

        let all = create_version_selector("AllCompatibleVersions").unwrap();
        assert_eq!(all.name(), "AllCompatibleVersions");

        assert!(create_version_selector("Unknown").is_none());
    }

    #[test]
    fn test_empty_available_versions() {
        let min_selector = MinimumVersionSelector;
        let max_selector = LatestVersionSelector;
        let all_selector = AllCompatibleVersionsSelector;
        let compatibility = BackwardCompatible;
        let available: Vec<u16> = vec![];

        assert!(min_selector.get_suitable_versions(1, &available, &compatibility).is_empty());
        assert!(max_selector.get_suitable_versions(1, &available, &compatibility).is_empty());
        assert!(all_selector.get_suitable_versions(1, &available, &compatibility).is_empty());
    }

    #[test]
    fn test_single_available_version() {
        let min_selector = MinimumVersionSelector;
        let max_selector = LatestVersionSelector;
        let all_selector = AllCompatibleVersionsSelector;
        let compatibility = BackwardCompatible;
        let available = vec![3];

        // Request v1: 3 is compatible
        assert_eq!(min_selector.get_suitable_versions(1, &available, &compatibility), vec![3]);
        assert_eq!(max_selector.get_suitable_versions(1, &available, &compatibility), vec![3]);
        assert_eq!(all_selector.get_suitable_versions(1, &available, &compatibility), vec![3]);

        // Request v5: none compatible
        assert!(min_selector.get_suitable_versions(5, &available, &compatibility).is_empty());
    }

    // Property-based tests
    #[cfg(test)]
    mod property_tests {
        use super::*;
        use ::proptest::prelude::*;

        ::proptest::proptest! {
            #[test]
            fn minimum_is_smallest_compatible(
                requested in 1u16..100,
                available in proptest::collection::vec(1u16..100, 1..10)
            ) {
                let selector = MinimumVersionSelector;
                let compatibility = BackwardCompatible;

                let result = selector.get_suitable_versions(requested, &available, &compatibility);

                if !result.is_empty() {
                    let selected = result[0];
                    // The selected version should be compatible
                    prop_assert!(compatibility.is_compatible(requested, selected));
                    // No smaller compatible version should exist
                    for &v in &available {
                        if v < selected && compatibility.is_compatible(requested, v) {
                            prop_assert!(false, "Found smaller compatible version {} < {}", v, selected);
                        }
                    }
                }
            }

            #[test]
            fn latest_is_largest_compatible(
                requested in 1u16..100,
                available in proptest::collection::vec(1u16..100, 1..10)
            ) {
                let selector = LatestVersionSelector;
                let compatibility = BackwardCompatible;

                let result = selector.get_suitable_versions(requested, &available, &compatibility);

                if !result.is_empty() {
                    let selected = result[0];
                    // The selected version should be compatible
                    prop_assert!(compatibility.is_compatible(requested, selected));
                    // No larger compatible version should exist
                    for &v in &available {
                        if v > selected && compatibility.is_compatible(requested, v) {
                            prop_assert!(false, "Found larger compatible version {} > {}", v, selected);
                        }
                    }
                }
            }

            #[test]
            fn all_compatible_returns_all_compatible_versions(
                requested in 1u16..100,
                available in proptest::collection::vec(1u16..100, 1..10)
            ) {
                let selector = AllCompatibleVersionsSelector;
                let compatibility = BackwardCompatible;

                let result = selector.get_suitable_versions(requested, &available, &compatibility);

                // All returned versions should be compatible
                for &v in &result {
                    prop_assert!(compatibility.is_compatible(requested, v));
                }

                // All compatible versions should be in the result
                for &v in &available {
                    if compatibility.is_compatible(requested, v) {
                        prop_assert!(result.contains(&v));
                    }
                }
            }
        }
    }
}
