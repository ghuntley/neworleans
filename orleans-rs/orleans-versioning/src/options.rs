//! Configuration options for grain versioning.

use serde::{Deserialize, Serialize};

/// Configuration options for grain interface versioning.
///
/// These options control the default behavior for version compatibility
/// checking and version selection during grain placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrainVersioningOptions {
    /// The default compatibility strategy name.
    /// Default: "BackwardCompatible"
    default_compatibility_strategy: String,

    /// The default version selector strategy name.
    /// Default: "AllCompatibleVersions"
    default_version_selector_strategy: String,

    /// Whether version-aware placement is enabled.
    /// When false, all versions are treated as compatible.
    /// Default: true
    enabled: bool,
}

impl Default for GrainVersioningOptions {
    fn default() -> Self {
        Self {
            default_compatibility_strategy: "BackwardCompatible".to_string(),
            default_version_selector_strategy: "AllCompatibleVersions".to_string(),
            enabled: true,
        }
    }
}

impl GrainVersioningOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the default compatibility strategy name.
    pub fn default_compatibility_strategy(&self) -> &str {
        &self.default_compatibility_strategy
    }

    /// Sets the default compatibility strategy name.
    ///
    /// Available strategies:
    /// - "BackwardCompatible" (default): Newer versions handle older requests
    /// - "StrictVersionCompatible": Only exact version matches
    /// - "AllVersionsCompatible": All versions work together
    pub fn with_compatibility_strategy(mut self, strategy: impl Into<String>) -> Self {
        self.default_compatibility_strategy = strategy.into();
        self
    }

    /// Gets the default version selector strategy name.
    pub fn default_version_selector_strategy(&self) -> &str {
        &self.default_version_selector_strategy
    }

    /// Sets the default version selector strategy name.
    ///
    /// Available strategies:
    /// - "AllCompatibleVersions" (default): Use any compatible version
    /// - "LatestVersion": Use the newest compatible version
    /// - "MinimumVersion": Use the oldest compatible version
    pub fn with_version_selector_strategy(mut self, strategy: impl Into<String>) -> Self {
        self.default_version_selector_strategy = strategy.into();
        self
    }

    /// Whether version-aware placement is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enables or disables version-aware placement.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Creates options for strict version enforcement.
    ///
    /// Uses strict compatibility (exact version match required)
    /// and latest version selection.
    pub fn strict() -> Self {
        Self {
            default_compatibility_strategy: "StrictVersionCompatible".to_string(),
            default_version_selector_strategy: "LatestVersion".to_string(),
            enabled: true,
        }
    }

    /// Creates options for permissive version handling.
    ///
    /// Uses all versions compatible strategy and all compatible versions selector.
    /// This maximizes load distribution across different version silos.
    pub fn permissive() -> Self {
        Self {
            default_compatibility_strategy: "AllVersionsCompatible".to_string(),
            default_version_selector_strategy: "AllCompatibleVersions".to_string(),
            enabled: true,
        }
    }

    /// Creates options for conservative upgrades.
    ///
    /// Uses backward compatibility with minimum version selection.
    /// This ensures new code isn't used until explicitly requested.
    pub fn conservative() -> Self {
        Self {
            default_compatibility_strategy: "BackwardCompatible".to_string(),
            default_version_selector_strategy: "MinimumVersion".to_string(),
            enabled: true,
        }
    }

    /// Creates options for aggressive upgrades.
    ///
    /// Uses backward compatibility with latest version selection.
    /// This encourages adoption of new versions.
    pub fn aggressive() -> Self {
        Self {
            default_compatibility_strategy: "BackwardCompatible".to_string(),
            default_version_selector_strategy: "LatestVersion".to_string(),
            enabled: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let options = GrainVersioningOptions::default();
        assert_eq!(options.default_compatibility_strategy(), "BackwardCompatible");
        assert_eq!(options.default_version_selector_strategy(), "AllCompatibleVersions");
        assert!(options.is_enabled());
    }

    #[test]
    fn test_strict_options() {
        let options = GrainVersioningOptions::strict();
        assert_eq!(options.default_compatibility_strategy(), "StrictVersionCompatible");
        assert_eq!(options.default_version_selector_strategy(), "LatestVersion");
        assert!(options.is_enabled());
    }

    #[test]
    fn test_permissive_options() {
        let options = GrainVersioningOptions::permissive();
        assert_eq!(options.default_compatibility_strategy(), "AllVersionsCompatible");
        assert_eq!(options.default_version_selector_strategy(), "AllCompatibleVersions");
    }

    #[test]
    fn test_conservative_options() {
        let options = GrainVersioningOptions::conservative();
        assert_eq!(options.default_compatibility_strategy(), "BackwardCompatible");
        assert_eq!(options.default_version_selector_strategy(), "MinimumVersion");
    }

    #[test]
    fn test_aggressive_options() {
        let options = GrainVersioningOptions::aggressive();
        assert_eq!(options.default_compatibility_strategy(), "BackwardCompatible");
        assert_eq!(options.default_version_selector_strategy(), "LatestVersion");
    }

    #[test]
    fn test_builder_pattern() {
        let options = GrainVersioningOptions::new()
            .with_compatibility_strategy("StrictVersionCompatible")
            .with_version_selector_strategy("MinimumVersion")
            .with_enabled(false);

        assert_eq!(options.default_compatibility_strategy(), "StrictVersionCompatible");
        assert_eq!(options.default_version_selector_strategy(), "MinimumVersion");
        assert!(!options.is_enabled());
    }

    #[test]
    fn test_serialization() {
        let options = GrainVersioningOptions::default();
        let json = serde_json::to_string(&options).unwrap();
        let deserialized: GrainVersioningOptions = serde_json::from_str(&json).unwrap();

        assert_eq!(options.default_compatibility_strategy(), deserialized.default_compatibility_strategy());
        assert_eq!(options.default_version_selector_strategy(), deserialized.default_version_selector_strategy());
    }
}
