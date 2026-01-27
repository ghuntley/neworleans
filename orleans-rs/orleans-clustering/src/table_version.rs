//! Table version for optimistic concurrency control.

use serde::{Deserialize, Serialize};

/// Represents the version of the membership table.
///
/// Used for optimistic concurrency control - updates must provide
/// the expected version and will fail if the table has changed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableVersion {
    /// Monotonically increasing version number.
    pub version: i64,
    /// ETag for storage backends that use them.
    pub version_etag: String,
}

impl TableVersion {
    /// Create a new table version with version 0.
    pub fn new() -> Self {
        Self {
            version: 0,
            version_etag: String::new(),
        }
    }

    /// Create a table version with a specific version number.
    pub fn with_version(version: i64) -> Self {
        Self {
            version,
            version_etag: String::new(),
        }
    }

    /// Create a table version with version and etag.
    pub fn with_etag(version: i64, etag: impl Into<String>) -> Self {
        Self {
            version,
            version_etag: etag.into(),
        }
    }

    /// Get the next version (incremented by 1).
    pub fn next(&self) -> Self {
        Self {
            version: self.version + 1,
            version_etag: String::new(), // Backend generates new ETag
        }
    }

    /// Check if this version is newer than another.
    pub fn is_newer_than(&self, other: &TableVersion) -> bool {
        self.version > other.version
    }
}

impl Default for TableVersion {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialOrd for TableVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.version.cmp(&other.version)
    }
}

impl std::fmt::Display for TableVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.version_etag.is_empty() {
            write!(f, "v{}", self.version)
        } else {
            write!(f, "v{}@{}", self.version, self.version_etag)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let v = TableVersion::new();
        assert_eq!(v.version, 0);
        assert!(v.version_etag.is_empty());
    }

    #[test]
    fn test_with_version() {
        let v = TableVersion::with_version(42);
        assert_eq!(v.version, 42);
        assert!(v.version_etag.is_empty());
    }

    #[test]
    fn test_with_etag() {
        let v = TableVersion::with_etag(42, "abc123");
        assert_eq!(v.version, 42);
        assert_eq!(v.version_etag, "abc123");
    }

    #[test]
    fn test_next() {
        let v1 = TableVersion::with_etag(5, "old");
        let v2 = v1.next();

        assert_eq!(v2.version, 6);
        assert!(v2.version_etag.is_empty()); // ETag cleared
    }

    #[test]
    fn test_is_newer_than() {
        let v1 = TableVersion::with_version(5);
        let v2 = TableVersion::with_version(10);

        assert!(v2.is_newer_than(&v1));
        assert!(!v1.is_newer_than(&v2));
        assert!(!v1.is_newer_than(&v1));
    }

    #[test]
    fn test_ordering() {
        let v1 = TableVersion::with_version(5);
        let v2 = TableVersion::with_version(10);
        let v3 = TableVersion::with_version(5);

        assert!(v2 > v1);
        assert!(v1 < v2);
        assert_eq!(v1, v3);
    }

    #[test]
    fn test_display() {
        let v1 = TableVersion::with_version(42);
        assert_eq!(format!("{}", v1), "v42");

        let v2 = TableVersion::with_etag(42, "abc");
        assert_eq!(format!("{}", v2), "v42@abc");
    }

    #[test]
    fn test_serialization() {
        let v = TableVersion::with_etag(42, "abc");
        let json = serde_json::to_string(&v).unwrap();
        let deserialized: TableVersion = serde_json::from_str(&json).unwrap();

        assert_eq!(v, deserialized);
    }
}
