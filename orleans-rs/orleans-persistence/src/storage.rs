//! Grain storage provider interface.
//!
//! This module defines the `IGrainStorage` trait that all storage providers
//! must implement to persist grain state.

use async_trait::async_trait;
use orleans_core::GrainId;

use crate::error::StorageResult;

/// Raw state data as stored in the backend.
///
/// This struct holds the serialized state bytes along with metadata.
#[derive(Debug, Clone, Default)]
pub struct RawGrainState {
    /// Serialized state data (empty if no record exists).
    pub data: Vec<u8>,
    /// The ETag for optimistic concurrency control.
    /// None if the state hasn't been read from or written to storage.
    pub etag: Option<String>,
    /// Whether this state has a corresponding record in storage.
    pub record_exists: bool,
}

impl RawGrainState {
    /// Create a new empty raw state (no record exists).
    pub fn empty() -> Self {
        Self {
            data: Vec::new(),
            etag: None,
            record_exists: false,
        }
    }

    /// Create a raw state with existing data.
    pub fn with_data(data: Vec<u8>, etag: String) -> Self {
        Self {
            data,
            etag: Some(etag),
            record_exists: true,
        }
    }
}

/// Primary interface for grain storage providers.
///
/// Storage providers implement this trait to enable grains to persist their state.
/// The trait works with raw bytes - serialization/deserialization is handled
/// by the `StateStorageBridge` layer.
///
/// # Optimistic Concurrency
///
/// All write operations use optimistic concurrency control via ETags:
///
/// - On read, the provider returns the current ETag in `RawGrainState`
/// - On write, the provider checks that the stored ETag matches the expected ETag
/// - If there's a mismatch, `StorageError::EtagMismatch` is returned
///
/// # Example
///
/// ```ignore
/// use orleans_persistence::{IGrainStorage, RawGrainState, StorageResult};
/// use orleans_core::GrainId;
///
/// struct MyStorageProvider { /* ... */ }
///
/// #[async_trait]
/// impl IGrainStorage for MyStorageProvider {
///     async fn read_state(&self, state_name: &str, grain_id: &GrainId) -> StorageResult<RawGrainState> {
///         // Read from storage...
///     }
///
///     async fn write_state(&self, state_name: &str, grain_id: &GrainId, state: &RawGrainState) -> StorageResult<String> {
///         // Write to storage with ETag check, return new ETag...
///     }
///
///     async fn clear_state(&self, state_name: &str, grain_id: &GrainId, expected_etag: Option<&str>) -> StorageResult<()> {
///         // Clear from storage...
///     }
/// }
/// ```
#[async_trait]
pub trait IGrainStorage: Send + Sync {
    /// Read grain state from storage.
    ///
    /// If a record exists, returns `RawGrainState` with:
    /// - The serialized state data
    /// - The current ETag
    /// - `record_exists = true`
    ///
    /// If no record exists, returns `RawGrainState::empty()`.
    ///
    /// # Arguments
    ///
    /// * `state_name` - The name/type of the state (used as part of the storage key)
    /// * `grain_id` - The grain's identity
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails (e.g., storage unavailable).
    async fn read_state(&self, state_name: &str, grain_id: &GrainId) -> StorageResult<RawGrainState>;

    /// Write grain state to storage.
    ///
    /// This performs an optimistic concurrency check:
    /// - If `state.etag` is `None`, this is an insert (fails if record exists)
    /// - If `state.etag` is `Some`, this is an update (fails if ETag doesn't match)
    ///
    /// # Arguments
    ///
    /// * `state_name` - The name/type of the state
    /// * `grain_id` - The grain's identity
    /// * `state` - The raw state to write
    ///
    /// # Returns
    ///
    /// The new ETag on success.
    ///
    /// # Errors
    ///
    /// - `StorageError::EtagMismatch` - Another writer modified the state
    /// - `StorageError::RecordExists` - Insert attempted but record already exists
    /// - Other errors for storage failures
    async fn write_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        state: &RawGrainState,
    ) -> StorageResult<String>;

    /// Clear grain state from storage.
    ///
    /// This removes the state record from storage.
    ///
    /// # Arguments
    ///
    /// * `state_name` - The name/type of the state
    /// * `grain_id` - The grain's identity
    /// * `expected_etag` - The expected ETag (for optimistic concurrency)
    ///
    /// # Errors
    ///
    /// - `StorageError::EtagMismatch` - Another writer modified the state
    /// - Other errors for storage failures
    async fn clear_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        expected_etag: Option<&str>,
    ) -> StorageResult<()>;
}

/// High-level storage interface for grains.
///
/// This is the interface that grains use to interact with their persistent state.
/// It wraps the lower-level `IGrainStorage` trait and provides a simpler API.
#[async_trait]
pub trait IStorage: Send + Sync {
    /// Get the current ETag.
    fn etag(&self) -> Option<&str>;

    /// Check if a record exists in storage.
    fn record_exists(&self) -> bool;

    /// Read state from storage.
    ///
    /// This should be called during grain activation to load persisted state.
    async fn read_state(&mut self) -> StorageResult<()>;

    /// Write state to storage.
    ///
    /// Call this after modifying state to persist changes.
    async fn write_state(&mut self) -> StorageResult<()>;

    /// Clear state from storage.
    ///
    /// This removes the persisted state and resets to defaults.
    async fn clear_state(&mut self) -> StorageResult<()>;
}

/// Typed storage interface with access to the state value.
pub trait IStorageTyped<TState>: IStorage {
    /// Get a reference to the state.
    fn state(&self) -> &TState;

    /// Get a mutable reference to the state.
    fn state_mut(&mut self) -> &mut TState;
}

/// Serializer for grain storage.
///
/// Provides methods to serialize and deserialize grain state.
/// This is a concrete type rather than a trait to avoid dyn compatibility issues.
#[derive(Debug, Clone, Default)]
pub struct GrainStorageSerializer;

impl GrainStorageSerializer {
    /// Create a new serializer.
    pub fn new() -> Self {
        Self
    }

    /// Serialize a value to bytes using JSON.
    pub fn serialize<T: serde::Serialize>(&self, value: &T) -> StorageResult<Vec<u8>> {
        serde_json::to_vec(value)
            .map_err(|e| crate::error::StorageError::Serialization(e.to_string()))
    }

    /// Deserialize a value from bytes.
    pub fn deserialize<T: serde::de::DeserializeOwned>(&self, data: &[u8]) -> StorageResult<T> {
        serde_json::from_slice(data)
            .map_err(|e| crate::error::StorageError::Deserialization(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct TestState {
        counter: i32,
        name: String,
    }

    #[test]
    fn test_raw_grain_state_empty() {
        let state = RawGrainState::empty();
        assert!(state.data.is_empty());
        assert!(state.etag.is_none());
        assert!(!state.record_exists);
    }

    #[test]
    fn test_raw_grain_state_with_data() {
        let data = vec![1, 2, 3, 4];
        let state = RawGrainState::with_data(data.clone(), "etag-1".to_string());
        assert_eq!(state.data, data);
        assert_eq!(state.etag.as_deref(), Some("etag-1"));
        assert!(state.record_exists);
    }

    #[test]
    fn test_serializer_roundtrip() {
        let serializer = GrainStorageSerializer::new();
        let original = TestState {
            counter: 42,
            name: "test".to_string(),
        };

        let bytes = serializer.serialize(&original).unwrap();
        let deserialized: TestState = serializer.deserialize(&bytes).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_serializer_produces_valid_json() {
        let serializer = GrainStorageSerializer::new();
        let state = TestState {
            counter: 42,
            name: "hello".to_string(),
        };

        let bytes = serializer.serialize(&state).unwrap();
        let json_str = String::from_utf8(bytes).unwrap();

        assert!(json_str.contains("\"counter\":42"));
        assert!(json_str.contains("\"name\":\"hello\""));
    }

    #[test]
    fn test_serializer_error_on_invalid_data() {
        let serializer = GrainStorageSerializer::new();
        let invalid_bytes = b"not valid json";

        let result: StorageResult<TestState> = serializer.deserialize(invalid_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_serializer_default() {
        let serializer = GrainStorageSerializer::default();
        let state = TestState::default();

        let bytes = serializer.serialize(&state).unwrap();
        assert!(!bytes.is_empty());
    }
}
