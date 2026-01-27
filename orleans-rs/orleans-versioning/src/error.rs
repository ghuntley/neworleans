//! Error types for versioning operations.

use thiserror::Error;

/// Errors that can occur during version operations.
#[derive(Debug, Error)]
pub enum VersionError {
    /// No compatible version found for the requested version.
    #[error("No compatible version found for version {requested}. Available: {available:?}")]
    NoCompatibleVersion {
        /// The version that was requested.
        requested: u16,
        /// The versions that are available in the cluster.
        available: Vec<u16>,
    },

    /// Interface type not found in the manifest.
    #[error("Interface type not found: {interface_type}")]
    InterfaceNotFound {
        /// The interface type that was not found.
        interface_type: String,
    },

    /// Grain type not found in the manifest.
    #[error("Grain type not found: {grain_type}")]
    GrainTypeNotFound {
        /// The grain type that was not found.
        grain_type: String,
    },

    /// No silos support the requested version.
    #[error("No silos support version {version} of interface {interface_type}")]
    NoSilosForVersion {
        /// The interface type.
        interface_type: String,
        /// The version that no silos support.
        version: u16,
    },

    /// Manifest is out of date and needs to be refreshed.
    #[error("Manifest is stale (version {current}, expected {expected})")]
    StaleManifest {
        /// The current manifest version.
        current: u64,
        /// The expected manifest version.
        expected: u64,
    },

    /// Invalid version number (version 0 is reserved for unspecified).
    #[error("Invalid version number: {version}. Version 0 is reserved for unspecified.")]
    InvalidVersion {
        /// The invalid version number.
        version: u16,
    },

    /// Invalid strategy name.
    #[error("Unknown strategy: {name}")]
    UnknownStrategy {
        /// The strategy name that was not recognized.
        name: String,
    },

    /// Configuration error.
    #[error("Configuration error: {message}")]
    Configuration {
        /// Description of the configuration error.
        message: String,
    },

    /// Internal error.
    #[error("Internal error: {message}")]
    Internal {
        /// Description of the internal error.
        message: String,
    },
}

impl VersionError {
    /// Creates a new no compatible version error.
    pub fn no_compatible_version(requested: u16, available: Vec<u16>) -> Self {
        Self::NoCompatibleVersion { requested, available }
    }

    /// Creates a new interface not found error.
    pub fn interface_not_found(interface_type: impl Into<String>) -> Self {
        Self::InterfaceNotFound {
            interface_type: interface_type.into(),
        }
    }

    /// Creates a new grain type not found error.
    pub fn grain_type_not_found(grain_type: impl Into<String>) -> Self {
        Self::GrainTypeNotFound {
            grain_type: grain_type.into(),
        }
    }

    /// Creates a new no silos for version error.
    pub fn no_silos_for_version(interface_type: impl Into<String>, version: u16) -> Self {
        Self::NoSilosForVersion {
            interface_type: interface_type.into(),
            version,
        }
    }

    /// Creates a new stale manifest error.
    pub fn stale_manifest(current: u64, expected: u64) -> Self {
        Self::StaleManifest { current, expected }
    }

    /// Creates a new invalid version error.
    pub fn invalid_version(version: u16) -> Self {
        Self::InvalidVersion { version }
    }

    /// Creates a new unknown strategy error.
    pub fn unknown_strategy(name: impl Into<String>) -> Self {
        Self::UnknownStrategy { name: name.into() }
    }

    /// Creates a new configuration error.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration {
            message: message.into(),
        }
    }

    /// Creates a new internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }
}

/// Result type for versioning operations.
pub type VersionResult<T> = Result<T, VersionError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_compatible_version_error() {
        let err = VersionError::no_compatible_version(2, vec![1]);
        assert!(err.to_string().contains("No compatible version"));
        assert!(err.to_string().contains("version 2"));
    }

    #[test]
    fn test_interface_not_found_error() {
        let err = VersionError::interface_not_found("IMyGrain");
        assert!(err.to_string().contains("Interface type not found"));
        assert!(err.to_string().contains("IMyGrain"));
    }

    #[test]
    fn test_grain_type_not_found_error() {
        let err = VersionError::grain_type_not_found("MyGrain");
        assert!(err.to_string().contains("Grain type not found"));
        assert!(err.to_string().contains("MyGrain"));
    }

    #[test]
    fn test_no_silos_for_version_error() {
        let err = VersionError::no_silos_for_version("IMyGrain", 3);
        assert!(err.to_string().contains("No silos support version 3"));
    }

    #[test]
    fn test_stale_manifest_error() {
        let err = VersionError::stale_manifest(1, 2);
        assert!(err.to_string().contains("stale"));
        assert!(err.to_string().contains("1"));
        assert!(err.to_string().contains("2"));
    }

    #[test]
    fn test_invalid_version_error() {
        let err = VersionError::invalid_version(0);
        assert!(err.to_string().contains("Invalid version"));
    }

    #[test]
    fn test_unknown_strategy_error() {
        let err = VersionError::unknown_strategy("UnknownStrategy");
        assert!(err.to_string().contains("Unknown strategy"));
    }

    #[test]
    fn test_configuration_error() {
        let err = VersionError::configuration("Invalid config");
        assert!(err.to_string().contains("Configuration error"));
    }

    #[test]
    fn test_internal_error() {
        let err = VersionError::internal("Unexpected state");
        assert!(err.to_string().contains("Internal error"));
    }
}
