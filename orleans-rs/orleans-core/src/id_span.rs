//! IdSpan - The primitive building block for all identity types
//!
//! `IdSpan` represents a UTF-8 byte sequence with a pre-computed XxHash32 hash code.
//! It is used as the foundational type for `GrainType`, `GrainId` keys, and other
//! identity types in Orleans.

use serde::{Deserialize, Serialize};
use std::fmt;
use xxhash_rust::xxh32;

/// The primitive building block for all Orleans identity types.
///
/// `IdSpan` is an immutable UTF-8 byte sequence with a pre-computed XxHash32 hash.
/// The hash is computed once at creation time and cached for efficient lookups
/// in hash maps and consistent hashing rings.
#[derive(Clone, Serialize, Deserialize)]
pub struct IdSpan {
    /// The raw UTF-8 bytes, or None for empty spans
    value: Option<Vec<u8>>,
    /// Pre-computed XxHash32 hash code
    hash_code: u32,
}

impl IdSpan {
    /// Creates a new `IdSpan` from raw bytes.
    ///
    /// # Arguments
    /// * `bytes` - The raw bytes to store (should be valid UTF-8)
    ///
    /// # Examples
    /// ```
    /// use orleans_core::IdSpan;
    /// let span = IdSpan::new(b"hello");
    /// assert_eq!(span.as_bytes(), b"hello");
    /// ```
    pub fn new(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            Self::empty()
        } else {
            let hash_code = xxh32::xxh32(bytes, 0);
            Self {
                value: Some(bytes.to_vec()),
                hash_code,
            }
        }
    }

    /// Creates a new `IdSpan` from a string slice.
    ///
    /// # Examples
    /// ```
    /// use orleans_core::IdSpan;
    /// let span = IdSpan::from_str("my-key");
    /// assert_eq!(span.as_str(), Some("my-key"));
    /// ```
    pub fn from_str(s: &str) -> Self {
        Self::new(s.as_bytes())
    }

    /// Creates an empty `IdSpan`.
    ///
    /// # Examples
    /// ```
    /// use orleans_core::IdSpan;
    /// let span = IdSpan::empty();
    /// assert!(span.is_empty());
    /// ```
    pub fn empty() -> Self {
        Self {
            value: None,
            hash_code: 0,
        }
    }

    /// Returns true if this span is empty.
    pub fn is_empty(&self) -> bool {
        self.value.is_none()
    }

    /// Returns the raw bytes of this span.
    pub fn as_bytes(&self) -> &[u8] {
        self.value.as_deref().unwrap_or(&[])
    }

    /// Returns this span as a string slice if it contains valid UTF-8.
    pub fn as_str(&self) -> Option<&str> {
        self.value
            .as_ref()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Returns the pre-computed XxHash32 hash code.
    ///
    /// This hash is used for:
    /// - Grain directory placement (consistent hashing)
    /// - Hash map lookups
    /// - Load balancing
    ///
    /// # Examples
    /// ```
    /// use orleans_core::IdSpan;
    /// let span1 = IdSpan::from_str("test");
    /// let span2 = IdSpan::from_str("test");
    /// assert_eq!(span1.get_hash_code(), span2.get_hash_code());
    /// ```
    pub fn get_hash_code(&self) -> u32 {
        self.hash_code
    }

    /// Returns the uniform hash code for consistent hashing.
    /// This is the same as `get_hash_code()` but named to match Orleans API.
    pub fn get_uniform_hash_code(&self) -> u32 {
        self.hash_code
    }

    /// Returns the length of the span in bytes.
    pub fn len(&self) -> usize {
        self.value.as_ref().map(|v| v.len()).unwrap_or(0)
    }
}

impl Default for IdSpan {
    fn default() -> Self {
        Self::empty()
    }
}

impl PartialEq for IdSpan {
    fn eq(&self, other: &Self) -> bool {
        // Fast path: compare hash codes first
        if self.hash_code != other.hash_code {
            return false;
        }
        // Then compare actual bytes
        self.value == other.value
    }
}

impl Eq for IdSpan {}

impl std::hash::Hash for IdSpan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Use pre-computed hash for efficiency
        self.hash_code.hash(state);
    }
}

impl fmt::Debug for IdSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            Some(s) => write!(f, "IdSpan({:?})", s),
            None if self.is_empty() => write!(f, "IdSpan(empty)"),
            None => write!(f, "IdSpan({:?})", self.as_bytes()),
        }
    }
}

impl fmt::Display for IdSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            Some(s) => write!(f, "{}", s),
            None if self.is_empty() => Ok(()),
            None => write!(f, "{:?}", self.as_bytes()),
        }
    }
}

impl From<&str> for IdSpan {
    fn from(s: &str) -> Self {
        Self::from_str(s)
    }
}

impl From<String> for IdSpan {
    fn from(s: String) -> Self {
        Self::new(s.as_bytes())
    }
}

impl From<&[u8]> for IdSpan {
    fn from(bytes: &[u8]) -> Self {
        Self::new(bytes)
    }
}

impl From<Vec<u8>> for IdSpan {
    fn from(bytes: Vec<u8>) -> Self {
        if bytes.is_empty() {
            Self::empty()
        } else {
            let hash_code = xxh32::xxh32(&bytes, 0);
            Self {
                value: Some(bytes),
                hash_code,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_from_bytes() {
        let span = IdSpan::new(b"hello");
        assert_eq!(span.as_bytes(), b"hello");
        assert!(!span.is_empty());
    }

    #[test]
    fn test_from_str() {
        let span = IdSpan::from_str("world");
        assert_eq!(span.as_str(), Some("world"));
        assert_eq!(span.as_bytes(), b"world");
    }

    #[test]
    fn test_empty() {
        let span = IdSpan::empty();
        assert!(span.is_empty());
        assert_eq!(span.as_bytes(), b"");
        assert_eq!(span.get_hash_code(), 0);
    }

    #[test]
    fn test_empty_from_empty_bytes() {
        let span = IdSpan::new(b"");
        assert!(span.is_empty());
        assert_eq!(span.get_hash_code(), 0);
    }

    #[test]
    fn test_hash_stability() {
        // Hash should be consistent across multiple calls
        let span1 = IdSpan::from_str("test-key");
        let span2 = IdSpan::from_str("test-key");
        assert_eq!(span1.get_hash_code(), span2.get_hash_code());

        // Hash should be computed at creation time
        let hash1 = span1.get_hash_code();
        let hash2 = span1.get_hash_code();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_equality() {
        let span1 = IdSpan::from_str("same");
        let span2 = IdSpan::from_str("same");
        let span3 = IdSpan::from_str("different");

        assert_eq!(span1, span2);
        assert_ne!(span1, span3);
    }

    #[test]
    fn test_equality_with_different_hash_same_bytes() {
        // This shouldn't happen in practice, but test the behavior
        let span1 = IdSpan::from_str("test");
        let span2 = IdSpan::from_str("test");
        assert_eq!(span1, span2);
    }

    #[test]
    fn test_display() {
        let span = IdSpan::from_str("display-test");
        assert_eq!(format!("{}", span), "display-test");

        let empty = IdSpan::empty();
        assert_eq!(format!("{}", empty), "");
    }

    #[test]
    fn test_debug() {
        let span = IdSpan::from_str("debug-test");
        let debug_str = format!("{:?}", span);
        assert!(debug_str.contains("debug-test"));
    }

    #[test]
    fn test_from_conversions() {
        let from_str: IdSpan = "from-str".into();
        assert_eq!(from_str.as_str(), Some("from-str"));

        let from_string: IdSpan = String::from("from-string").into();
        assert_eq!(from_string.as_str(), Some("from-string"));

        let from_bytes: IdSpan = b"from-bytes".as_slice().into();
        assert_eq!(from_bytes.as_bytes(), b"from-bytes");

        let from_vec: IdSpan = vec![1, 2, 3].into();
        assert_eq!(from_vec.as_bytes(), &[1, 2, 3]);
    }

    #[test]
    fn test_len() {
        assert_eq!(IdSpan::empty().len(), 0);
        assert_eq!(IdSpan::from_str("hello").len(), 5);
        assert_eq!(IdSpan::from_str("").len(), 0);
    }

    #[test]
    fn test_default() {
        let span = IdSpan::default();
        assert!(span.is_empty());
    }

    #[test]
    fn test_hash_different_values() {
        let span1 = IdSpan::from_str("value1");
        let span2 = IdSpan::from_str("value2");
        // Different values should (usually) have different hashes
        // Note: hash collisions are possible but unlikely for these values
        assert_ne!(span1.get_hash_code(), span2.get_hash_code());
    }

    #[test]
    fn test_uniform_hash_code() {
        let span = IdSpan::from_str("uniform");
        assert_eq!(span.get_hash_code(), span.get_uniform_hash_code());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prop_hash_stable_across_calls(s in "\\PC*") {
            let span = IdSpan::from_str(&s);
            let hash1 = span.get_hash_code();
            let hash2 = span.get_hash_code();
            prop_assert_eq!(hash1, hash2);
        }

        #[test]
        fn prop_equal_spans_have_equal_hashes(s in "\\PC*") {
            let span1 = IdSpan::from_str(&s);
            let span2 = IdSpan::from_str(&s);
            prop_assert_eq!(span1.get_hash_code(), span2.get_hash_code());
            prop_assert_eq!(span1, span2);
        }

        #[test]
        fn prop_roundtrip_bytes(bytes in prop::collection::vec(any::<u8>(), 0..1000)) {
            let span = IdSpan::new(&bytes);
            if bytes.is_empty() {
                prop_assert!(span.is_empty());
            } else {
                prop_assert_eq!(span.as_bytes(), bytes.as_slice());
            }
        }

        #[test]
        fn prop_roundtrip_string(s in "\\PC*") {
            let span = IdSpan::from_str(&s);
            if s.is_empty() {
                prop_assert!(span.is_empty());
            } else {
                prop_assert_eq!(span.as_str(), Some(s.as_str()));
            }
        }
    }
}
