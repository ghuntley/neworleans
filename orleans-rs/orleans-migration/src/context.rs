//! Migration context types for dehydration and rehydration.
//!
//! This module provides the `MigrationContext` for transferring grain state between silos.
//! Unlike typical trait-based designs, we use a concrete `MigrationContext` type to avoid
//! issues with generic methods and dyn-safety.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, trace, warn};

use crate::error::{MigrationError, MigrationResult};

/// Entry in the migration context storing serialized data.
#[derive(Debug, Clone)]
struct ContextEntry {
    /// Serialized bytes.
    data: Bytes,
    /// Type name for debugging.
    type_name: String,
}

/// Migration context for transferring grain state between silos.
///
/// This struct serves as both the dehydration and rehydration context.
/// During dehydration, grain components serialize their state into this context.
/// During rehydration, components retrieve and deserialize their state.
///
/// # Example
///
/// ```
/// use orleans_migration::MigrationContext;
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct MyState {
///     counter: i32,
/// }
///
/// // Dehydration (source silo)
/// let mut ctx = MigrationContext::new();
/// let state = MyState { counter: 42 };
/// ctx.try_add_value("my_state", &state);
///
/// // Transfer (serialize for network)
/// let bytes = ctx.to_bytes().unwrap();
///
/// // Rehydration (target silo)
/// let ctx2 = MigrationContext::from_bytes(&bytes).unwrap();
/// let restored: MyState = ctx2.try_get_value("my_state").unwrap();
/// assert_eq!(restored.counter, 42);
/// ```
#[derive(Debug, Clone)]
pub struct MigrationContext {
    /// Stored entries keyed by string.
    entries: HashMap<String, ContextEntry>,
    /// Maximum allowed size in bytes.
    max_size: usize,
    /// Current total size.
    current_size: usize,
}

impl MigrationContext {
    /// Create a new empty migration context.
    pub fn new() -> Self {
        Self::with_max_size(10 * 1024 * 1024) // 10 MB default
    }

    /// Create a new migration context with a specific max size.
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            current_size: 0,
        }
    }

    /// Create from serialized bytes.
    pub fn from_bytes(data: &[u8]) -> MigrationResult<Self> {
        let serializable: SerializableMigrationContext =
            serde_json::from_slice(data).map_err(|e| MigrationError::Deserialization(e.to_string()))?;
        Ok(serializable.into())
    }

    /// Serialize the context to bytes for transfer.
    pub fn to_bytes(&self) -> MigrationResult<Vec<u8>> {
        serde_json::to_vec(&SerializableMigrationContext::from(self))
            .map_err(|e| MigrationError::Serialization(e.to_string()))
    }

    /// Get the number of entries in the context.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the context is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clear all entries from the context.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.current_size = 0;
    }

    /// Merge another context into this one.
    pub fn merge(&mut self, other: &MigrationContext) -> MigrationResult<()> {
        for (key, entry) in &other.entries {
            if self.current_size + entry.data.len() > self.max_size {
                return Err(MigrationError::Internal(format!(
                    "merged context exceeds max size of {} bytes",
                    self.max_size
                )));
            }
            if !self.entries.contains_key(key) {
                self.current_size += entry.data.len();
                self.entries.insert(key.clone(), entry.clone());
            }
        }
        Ok(())
    }

    /// Add raw bytes to the context with the given key.
    pub fn add_bytes(&mut self, key: &str, value: &[u8]) {
        if self.current_size + value.len() > self.max_size {
            warn!(
                key = key,
                size = value.len(),
                max_size = self.max_size,
                "migration context size limit exceeded, ignoring key"
            );
            return;
        }

        let entry = ContextEntry {
            data: Bytes::copy_from_slice(value),
            type_name: "bytes".to_string(),
        };

        if let Some(old) = self.entries.insert(key.to_string(), entry) {
            self.current_size -= old.data.len();
        }
        self.current_size += value.len();

        trace!(key = key, size = value.len(), "added bytes to migration context");
    }

    /// Try to add a serializable value to the context.
    /// Returns false if a value with this key already exists or if serialization fails.
    pub fn try_add_value<T: Serialize>(&mut self, key: &str, value: &T) -> bool {
        if self.entries.contains_key(key) {
            debug!(key = key, "key already exists in migration context");
            return false;
        }

        match serde_json::to_vec(value) {
            Ok(data) => {
                if self.current_size + data.len() > self.max_size {
                    warn!(
                        key = key,
                        size = data.len(),
                        max_size = self.max_size,
                        "migration context size limit exceeded"
                    );
                    return false;
                }

                let entry = ContextEntry {
                    data: Bytes::from(data.clone()),
                    type_name: std::any::type_name::<T>().to_string(),
                };

                self.current_size += data.len();
                self.entries.insert(key.to_string(), entry);

                trace!(
                    key = key,
                    type_name = std::any::type_name::<T>(),
                    size = data.len(),
                    "added value to migration context"
                );
                true
            }
            Err(e) => {
                warn!(
                    key = key,
                    error = %e,
                    "failed to serialize value for migration context"
                );
                false
            }
        }
    }

    /// Check if a key exists in the context.
    pub fn has_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Get all keys in the context.
    pub fn keys(&self) -> Vec<String> {
        self.entries.keys().cloned().collect()
    }

    /// Get the total size of the context in bytes.
    pub fn total_size(&self) -> usize {
        self.current_size
    }

    /// Try to get raw bytes from the context.
    pub fn try_get_bytes(&self, key: &str) -> Option<Bytes> {
        self.entries.get(key).map(|e| e.data.clone())
    }

    /// Try to get a deserializable value from the context.
    pub fn try_get_value<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let entry = self.entries.get(key)?;

        match serde_json::from_slice(&entry.data) {
            Ok(value) => {
                trace!(
                    key = key,
                    type_name = std::any::type_name::<T>(),
                    "retrieved value from migration context"
                );
                Some(value)
            }
            Err(e) => {
                warn!(
                    key = key,
                    error = %e,
                    expected_type = std::any::type_name::<T>(),
                    stored_type = %entry.type_name,
                    "failed to deserialize value from migration context"
                );
                None
            }
        }
    }

    /// Get a value or return a default.
    pub fn get_value_or_default<T: DeserializeOwned + Default>(&self, key: &str) -> T {
        self.try_get_value(key).unwrap_or_default()
    }
}

impl Default for MigrationContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Serializable representation of MigrationContext for wire transfer.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SerializableMigrationContext {
    entries: Vec<SerializableEntry>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct SerializableEntry {
    key: String,
    data: Vec<u8>,
    type_name: String,
}

impl From<&MigrationContext> for SerializableMigrationContext {
    fn from(ctx: &MigrationContext) -> Self {
        Self {
            entries: ctx
                .entries
                .iter()
                .map(|(k, v)| SerializableEntry {
                    key: k.clone(),
                    data: v.data.to_vec(),
                    type_name: v.type_name.clone(),
                })
                .collect(),
        }
    }
}

impl From<SerializableMigrationContext> for MigrationContext {
    fn from(ctx: SerializableMigrationContext) -> Self {
        let mut result = MigrationContext::new();
        for entry in ctx.entries {
            result.entries.insert(
                entry.key,
                ContextEntry {
                    data: Bytes::from(entry.data.clone()),
                    type_name: entry.type_name,
                },
            );
            result.current_size += entry.data.len();
        }
        result
    }
}

/// Thread-safe wrapper around MigrationContext.
#[derive(Debug, Clone)]
pub struct SharedMigrationContext {
    inner: Arc<RwLock<MigrationContext>>,
}

impl SharedMigrationContext {
    /// Create a new shared context.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MigrationContext::new())),
        }
    }

    /// Create from an existing context.
    pub fn from_context(context: MigrationContext) -> Self {
        Self {
            inner: Arc::new(RwLock::new(context)),
        }
    }

    /// Get a reference to the inner context for reading.
    pub fn read(&self) -> parking_lot::RwLockReadGuard<'_, MigrationContext> {
        self.inner.read()
    }

    /// Get a mutable reference to the inner context.
    pub fn write(&self) -> parking_lot::RwLockWriteGuard<'_, MigrationContext> {
        self.inner.write()
    }

    /// Extract the inner context.
    pub fn into_inner(self) -> MigrationContext {
        Arc::try_unwrap(self.inner)
            .map(|rw| rw.into_inner())
            .unwrap_or_else(|arc| arc.read().clone())
    }
}

impl Default for SharedMigrationContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestState {
        counter: i32,
        name: String,
    }

    #[test]
    fn test_empty_context() {
        let ctx = MigrationContext::new();
        assert!(ctx.is_empty());
        assert_eq!(ctx.len(), 0);
        assert_eq!(ctx.total_size(), 0);
    }

    #[test]
    fn test_add_and_get_value() {
        let mut ctx = MigrationContext::new();
        let state = TestState {
            counter: 42,
            name: "test".to_string(),
        };

        assert!(ctx.try_add_value("my_state", &state));
        assert!(ctx.has_key("my_state"));
        assert!(!ctx.has_key("other"));

        let retrieved: Option<TestState> = ctx.try_get_value("my_state");
        assert_eq!(retrieved, Some(state));
    }

    #[test]
    fn test_add_duplicate_key_fails() {
        let mut ctx = MigrationContext::new();
        let state1 = TestState {
            counter: 1,
            name: "first".to_string(),
        };
        let state2 = TestState {
            counter: 2,
            name: "second".to_string(),
        };

        assert!(ctx.try_add_value("key", &state1));
        assert!(!ctx.try_add_value("key", &state2)); // Should fail

        // Original value preserved
        let retrieved: TestState = ctx.try_get_value("key").unwrap();
        assert_eq!(retrieved.counter, 1);
    }

    #[test]
    fn test_add_bytes() {
        let mut ctx = MigrationContext::new();
        let data = vec![1, 2, 3, 4, 5];

        ctx.add_bytes("raw_data", &data);
        assert!(ctx.has_key("raw_data"));

        let retrieved = ctx.try_get_bytes("raw_data").unwrap();
        assert_eq!(&retrieved[..], &data[..]);
    }

    #[test]
    fn test_keys() {
        let mut ctx = MigrationContext::new();
        ctx.try_add_value("key1", &1i32);
        ctx.try_add_value("key2", &2i32);
        ctx.try_add_value("key3", &3i32);

        let keys = ctx.keys();
        assert_eq!(keys.len(), 3);
        assert!(keys.contains(&"key1".to_string()));
        assert!(keys.contains(&"key2".to_string()));
        assert!(keys.contains(&"key3".to_string()));
    }

    #[test]
    fn test_get_value_or_default() {
        let ctx = MigrationContext::new();

        let value: i32 = ctx.get_value_or_default("missing");
        assert_eq!(value, 0);

        let value: String = ctx.get_value_or_default("missing");
        assert_eq!(value, "");
    }

    #[test]
    fn test_clear() {
        let mut ctx = MigrationContext::new();
        ctx.try_add_value("key1", &1i32);
        ctx.try_add_value("key2", &2i32);

        assert_eq!(ctx.len(), 2);

        ctx.clear();
        assert!(ctx.is_empty());
        assert_eq!(ctx.total_size(), 0);
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut ctx = MigrationContext::new();
        let state = TestState {
            counter: 42,
            name: "test".to_string(),
        };
        ctx.try_add_value("state", &state);
        ctx.add_bytes("raw", &[1, 2, 3]);

        // Serialize
        let bytes = ctx.to_bytes().unwrap();

        // Deserialize
        let ctx2 = MigrationContext::from_bytes(&bytes).unwrap();

        // Verify
        let retrieved: TestState = ctx2.try_get_value("state").unwrap();
        assert_eq!(retrieved, state);

        let raw = ctx2.try_get_bytes("raw").unwrap();
        assert_eq!(&raw[..], &[1, 2, 3]);
    }

    #[test]
    fn test_max_size_enforcement() {
        let mut ctx = MigrationContext::with_max_size(100);

        // Should succeed - small value
        assert!(ctx.try_add_value("small", &1i32));

        // Should fail - value too large
        let large_data: Vec<u8> = vec![0; 200];
        assert!(!ctx.try_add_value("large", &large_data));
    }

    #[test]
    fn test_merge_contexts() {
        let mut ctx1 = MigrationContext::new();
        ctx1.try_add_value("key1", &1i32);

        let mut ctx2 = MigrationContext::new();
        ctx2.try_add_value("key2", &2i32);
        ctx2.try_add_value("key3", &3i32);

        ctx1.merge(&ctx2).unwrap();

        assert_eq!(ctx1.len(), 3);
        assert!(ctx1.has_key("key1"));
        assert!(ctx1.has_key("key2"));
        assert!(ctx1.has_key("key3"));
    }

    #[test]
    fn test_merge_preserves_existing() {
        let mut ctx1 = MigrationContext::new();
        ctx1.try_add_value("key", &1i32);

        let mut ctx2 = MigrationContext::new();
        ctx2.try_add_value("key", &2i32); // Same key

        ctx1.merge(&ctx2).unwrap();

        // Original value preserved
        let value: i32 = ctx1.try_get_value("key").unwrap();
        assert_eq!(value, 1);
    }

    #[test]
    fn test_shared_context() {
        let shared = SharedMigrationContext::new();

        {
            let mut ctx = shared.write();
            ctx.try_add_value("key", &42i32);
        }

        {
            let ctx = shared.read();
            let value: i32 = ctx.try_get_value("key").unwrap();
            assert_eq!(value, 42);
        }
    }

    #[test]
    fn test_total_size_tracking() {
        let mut ctx = MigrationContext::new();

        let initial_size = ctx.total_size();
        assert_eq!(initial_size, 0);

        ctx.try_add_value("number", &42i32);
        let size_after_number = ctx.total_size();
        assert!(size_after_number > 0);

        ctx.try_add_value("string", &"hello world".to_string());
        let size_after_string = ctx.total_size();
        assert!(size_after_string > size_after_number);
    }

    #[test]
    fn test_type_mismatch_returns_none() {
        let mut ctx = MigrationContext::new();
        ctx.try_add_value("number", &42i32);

        // Try to get as wrong type
        let result: Option<String> = ctx.try_get_value("number");
        assert!(result.is_none());
    }

    #[test]
    fn test_missing_key_returns_none() {
        let ctx = MigrationContext::new();

        let result: Option<i32> = ctx.try_get_value("missing");
        assert!(result.is_none());

        let bytes = ctx.try_get_bytes("missing");
        assert!(bytes.is_none());
    }

    #[test]
    fn test_context_clone() {
        let mut ctx = MigrationContext::new();
        ctx.try_add_value("key", &42i32);

        let cloned = ctx.clone();
        assert_eq!(cloned.len(), ctx.len());

        let value: i32 = cloned.try_get_value("key").unwrap();
        assert_eq!(value, 42);
    }
}
