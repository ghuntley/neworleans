//! In-memory grain storage provider.
//!
//! This module provides a memory-based storage provider for testing and development.
//! State is stored in memory and will be lost when the process terminates.

use std::collections::HashMap;

use async_trait::async_trait;
use orleans_core::GrainId;
use parking_lot::RwLock;
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::error::{StorageError, StorageResult};
use crate::storage::{IGrainStorage, RawGrainState};

/// In-memory grain storage provider.
///
/// This provider stores grain state in memory using a concurrent hash map.
/// It's useful for testing and development but not suitable for production
/// as state is lost on process restart.
///
/// # Features
///
/// - Thread-safe concurrent access
/// - Full optimistic concurrency control with ETags
///
/// # Example
///
/// ```
/// use orleans_persistence::{MemoryGrainStorage, MemoryGrainStorageOptions};
///
/// let storage = MemoryGrainStorage::new();
///
/// // Or with custom options
/// let storage = MemoryGrainStorage::with_options(MemoryGrainStorageOptions {
///     num_storage_partitions: 16,
///     ..Default::default()
/// });
/// ```
#[derive(Debug)]
pub struct MemoryGrainStorage {
    /// Storage partitions (for reduced lock contention).
    partitions: Vec<RwLock<HashMap<String, StoredEntry>>>,
    /// Number of partitions.
    num_partitions: usize,
}

/// A stored entry in memory storage.
#[derive(Debug, Clone)]
struct StoredEntry {
    /// Serialized state data.
    data: Vec<u8>,
    /// Current ETag/version.
    etag: String,
}

/// Configuration options for memory storage.
#[derive(Debug, Clone)]
pub struct MemoryGrainStorageOptions {
    /// Number of storage partitions (for reduced lock contention).
    /// Default: 16
    pub num_storage_partitions: usize,
}

impl Default for MemoryGrainStorageOptions {
    fn default() -> Self {
        Self {
            num_storage_partitions: 16,
        }
    }
}

impl Default for MemoryGrainStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryGrainStorage {
    /// Create a new memory storage provider with default options.
    pub fn new() -> Self {
        Self::with_options(MemoryGrainStorageOptions::default())
    }

    /// Create a new memory storage provider with custom options.
    pub fn with_options(options: MemoryGrainStorageOptions) -> Self {
        let num_partitions = options.num_storage_partitions.max(1);
        let partitions = (0..num_partitions)
            .map(|_| RwLock::new(HashMap::new()))
            .collect();

        Self {
            partitions,
            num_partitions,
        }
    }

    /// Generate a storage key from state name and grain ID.
    fn make_key(&self, state_name: &str, grain_id: &GrainId) -> String {
        format!("{}:{}", state_name, grain_id)
    }

    /// Get the partition index for a given key.
    fn partition_index(&self, key: &str) -> usize {
        // Simple hash-based partitioning
        let hash = key.bytes().fold(0u32, |acc, b| acc.wrapping_add(b as u32));
        (hash as usize) % self.num_partitions
    }

    /// Generate a new ETag.
    fn generate_etag() -> String {
        Uuid::new_v4().to_string()
    }

    /// Get the number of stored entries (for testing).
    #[cfg(test)]
    pub fn entry_count(&self) -> usize {
        self.partitions.iter().map(|p| p.read().len()).sum()
    }

    /// Clear all stored entries (for testing).
    #[cfg(test)]
    pub fn clear_all(&self) {
        for partition in &self.partitions {
            partition.write().clear();
        }
    }
}

#[async_trait]
impl IGrainStorage for MemoryGrainStorage {
    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn read_state(&self, state_name: &str, grain_id: &GrainId) -> StorageResult<RawGrainState> {
        let key = self.make_key(state_name, grain_id);
        let partition_idx = self.partition_index(&key);

        debug!(
            state_name = %state_name,
            partition = partition_idx,
            "Reading grain state from memory storage"
        );

        let partition = self.partitions[partition_idx].read();

        match partition.get(&key) {
            Some(entry) => {
                debug!(etag = %entry.etag, data_len = entry.data.len(), "State found and loaded");
                Ok(RawGrainState::with_data(entry.data.clone(), entry.etag.clone()))
            }
            None => {
                debug!("No state found, returning empty");
                Ok(RawGrainState::empty())
            }
        }
    }

    #[instrument(skip(self, state), fields(grain_id = %grain_id))]
    async fn write_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        state: &RawGrainState,
    ) -> StorageResult<String> {
        let key = self.make_key(state_name, grain_id);
        let partition_idx = self.partition_index(&key);
        let current_etag = state.etag.as_deref();

        debug!(
            state_name = %state_name,
            partition = partition_idx,
            current_etag = ?current_etag,
            data_len = state.data.len(),
            "Writing grain state to memory storage"
        );

        let new_etag = Self::generate_etag();

        let mut partition = self.partitions[partition_idx].write();

        match current_etag {
            None => {
                // Insert new record - fail if exists
                if partition.contains_key(&key) {
                    warn!("Attempted to insert duplicate record");
                    return Err(StorageError::RecordExists);
                }

                partition.insert(
                    key,
                    StoredEntry {
                        data: state.data.clone(),
                        etag: new_etag.clone(),
                    },
                );

                debug!(etag = %new_etag, "New state record created");
            }
            Some(expected_etag) => {
                // Update existing record - check ETag
                match partition.get(&key) {
                    Some(entry) if entry.etag == expected_etag || expected_etag == "*" => {
                        partition.insert(
                            key,
                            StoredEntry {
                                data: state.data.clone(),
                                etag: new_etag.clone(),
                            },
                        );
                        debug!(
                            old_etag = %expected_etag,
                            new_etag = %new_etag,
                            "State record updated"
                        );
                    }
                    Some(entry) => {
                        warn!(
                            stored_etag = %entry.etag,
                            expected_etag = %expected_etag,
                            "ETag mismatch during write"
                        );
                        return Err(StorageError::EtagMismatch {
                            stored: entry.etag.clone(),
                            expected: expected_etag.to_string(),
                        });
                    }
                    None => {
                        // Record doesn't exist but we have an ETag - allow upsert
                        partition.insert(
                            key,
                            StoredEntry {
                                data: state.data.clone(),
                                etag: new_etag.clone(),
                            },
                        );
                        debug!(etag = %new_etag, "State record created (upsert)");
                    }
                }
            }
        }

        Ok(new_etag)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn clear_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        expected_etag: Option<&str>,
    ) -> StorageResult<()> {
        let key = self.make_key(state_name, grain_id);
        let partition_idx = self.partition_index(&key);

        debug!(
            state_name = %state_name,
            partition = partition_idx,
            expected_etag = ?expected_etag,
            "Clearing grain state from memory storage"
        );

        let mut partition = self.partitions[partition_idx].write();

        // Check ETag if provided
        if let Some(expected) = expected_etag {
            if let Some(entry) = partition.get(&key) {
                if entry.etag != expected && expected != "*" {
                    warn!(
                        stored_etag = %entry.etag,
                        expected_etag = %expected,
                        "ETag mismatch during clear"
                    );
                    return Err(StorageError::EtagMismatch {
                        stored: entry.etag.clone(),
                        expected: expected.to_string(),
                    });
                }
            }
        }

        partition.remove(&key);
        debug!("State record cleared");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::GrainStorageSerializer;
    use orleans_core::{GrainType, IdSpan};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct TestState {
        counter: i32,
        name: String,
    }

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(name))
    }

    fn serialize_state(state: &TestState) -> Vec<u8> {
        GrainStorageSerializer::new().serialize(state).unwrap()
    }

    fn deserialize_state(data: &[u8]) -> TestState {
        GrainStorageSerializer::new().deserialize(data).unwrap()
    }

    #[tokio::test]
    async fn test_read_nonexistent_state_returns_empty() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test1");

        let result = storage.read_state("State", &grain_id).await.unwrap();

        assert!(!result.record_exists);
        assert!(result.etag.is_none());
        assert!(result.data.is_empty());
    }

    #[tokio::test]
    async fn test_write_and_read_state() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test2");
        let state = TestState {
            counter: 42,
            name: "hello".to_string(),
        };
        let data = serialize_state(&state);

        // Write state
        let raw_state = RawGrainState {
            data: data.clone(),
            etag: None,
            record_exists: false,
        };
        let etag = storage.write_state("State", &grain_id, &raw_state).await.unwrap();
        assert!(!etag.is_empty());

        // Read it back
        let result = storage.read_state("State", &grain_id).await.unwrap();

        assert!(result.record_exists);
        assert_eq!(result.etag.as_deref(), Some(etag.as_str()));

        let read_state: TestState = deserialize_state(&result.data);
        assert_eq!(read_state.counter, 42);
        assert_eq!(read_state.name, "hello");
    }

    #[tokio::test]
    async fn test_update_state_with_correct_etag() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test3");
        let state1 = TestState {
            counter: 1,
            name: "first".to_string(),
        };

        // Initial write
        let raw_state1 = RawGrainState {
            data: serialize_state(&state1),
            etag: None,
            record_exists: false,
        };
        let first_etag = storage.write_state("State", &grain_id, &raw_state1).await.unwrap();

        // Update with correct ETag
        let state2 = TestState {
            counter: 2,
            name: "second".to_string(),
        };
        let raw_state2 = RawGrainState {
            data: serialize_state(&state2),
            etag: Some(first_etag.clone()),
            record_exists: true,
        };
        let second_etag = storage.write_state("State", &grain_id, &raw_state2).await.unwrap();

        // ETag should change
        assert_ne!(first_etag, second_etag);

        // Verify updated value
        let result = storage.read_state("State", &grain_id).await.unwrap();
        let read_state: TestState = deserialize_state(&result.data);
        assert_eq!(read_state.counter, 2);
        assert_eq!(read_state.name, "second");
    }

    #[tokio::test]
    async fn test_update_state_with_wrong_etag_fails() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test4");
        let state = TestState {
            counter: 1,
            name: "first".to_string(),
        };

        // Initial write
        let raw_state1 = RawGrainState {
            data: serialize_state(&state),
            etag: None,
            record_exists: false,
        };
        let _first_etag = storage.write_state("State", &grain_id, &raw_state1).await.unwrap();

        // Update with wrong ETag
        let raw_state2 = RawGrainState {
            data: serialize_state(&state),
            etag: Some("wrong-etag".to_string()),
            record_exists: true,
        };
        let result = storage.write_state("State", &grain_id, &raw_state2).await;
        assert!(matches!(result, Err(StorageError::EtagMismatch { .. })));
    }

    #[tokio::test]
    async fn test_insert_duplicate_fails() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test5");

        // First insert
        let raw_state1 = RawGrainState {
            data: vec![1, 2, 3],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state1).await.unwrap();

        // Second insert (no ETag = insert)
        let raw_state2 = RawGrainState {
            data: vec![4, 5, 6],
            etag: None,
            record_exists: false,
        };
        let result = storage.write_state("State", &grain_id, &raw_state2).await;
        assert!(matches!(result, Err(StorageError::RecordExists)));
    }

    #[tokio::test]
    async fn test_clear_state() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test6");

        // Write state
        let raw_state = RawGrainState {
            data: vec![1, 2, 3],
            etag: None,
            record_exists: false,
        };
        let etag = storage.write_state("State", &grain_id, &raw_state).await.unwrap();

        // Clear with correct ETag
        storage
            .clear_state("State", &grain_id, Some(&etag))
            .await
            .unwrap();

        // Should not be readable
        let result = storage.read_state("State", &grain_id).await.unwrap();
        assert!(!result.record_exists);
    }

    #[tokio::test]
    async fn test_clear_state_with_wrong_etag_fails() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test7");

        // Write state
        let raw_state = RawGrainState {
            data: vec![1, 2, 3],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state).await.unwrap();

        // Try to clear with wrong ETag
        let result = storage
            .clear_state("State", &grain_id, Some("wrong-etag"))
            .await;
        assert!(matches!(result, Err(StorageError::EtagMismatch { .. })));
    }

    #[tokio::test]
    async fn test_wildcard_etag_always_succeeds() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test8");

        // Write state
        let raw_state1 = RawGrainState {
            data: vec![1, 2, 3],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state1).await.unwrap();

        // Update with wildcard ETag
        let raw_state2 = RawGrainState {
            data: vec![4, 5, 6],
            etag: Some("*".to_string()),
            record_exists: true,
        };
        storage.write_state("State", &grain_id, &raw_state2).await.unwrap();

        // Verify updated
        let result = storage.read_state("State", &grain_id).await.unwrap();
        assert_eq!(result.data, vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_different_state_names_are_independent() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test9");

        // Write to "State1"
        let raw_state1 = RawGrainState {
            data: vec![1],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State1", &grain_id, &raw_state1).await.unwrap();

        // Write to "State2"
        let raw_state2 = RawGrainState {
            data: vec![2],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State2", &grain_id, &raw_state2).await.unwrap();

        // Read State1
        let result1 = storage.read_state("State1", &grain_id).await.unwrap();
        assert_eq!(result1.data, vec![1]);

        // Read State2
        let result2 = storage.read_state("State2", &grain_id).await.unwrap();
        assert_eq!(result2.data, vec![2]);
    }

    #[tokio::test]
    async fn test_different_grain_ids_are_independent() {
        let storage = MemoryGrainStorage::new();
        let grain_id1 = make_grain_id("grain1");
        let grain_id2 = make_grain_id("grain2");

        // Write to grain1
        let raw_state1 = RawGrainState {
            data: vec![1],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id1, &raw_state1).await.unwrap();

        // Write to grain2
        let raw_state2 = RawGrainState {
            data: vec![2],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id2, &raw_state2).await.unwrap();

        // Verify independence
        let result1 = storage.read_state("State", &grain_id1).await.unwrap();
        assert_eq!(result1.data, vec![1]);

        let result2 = storage.read_state("State", &grain_id2).await.unwrap();
        assert_eq!(result2.data, vec![2]);
    }

    #[tokio::test]
    async fn test_entry_count_and_clear_all() {
        let storage = MemoryGrainStorage::new();

        // Initially empty
        assert_eq!(storage.entry_count(), 0);

        // Add some entries
        for i in 0..5 {
            let grain_id = make_grain_id(&format!("grain{}", i));
            let raw_state = RawGrainState {
                data: vec![i as u8],
                etag: None,
                record_exists: false,
            };
            storage.write_state("State", &grain_id, &raw_state).await.unwrap();
        }

        assert_eq!(storage.entry_count(), 5);

        // Clear all
        storage.clear_all();
        assert_eq!(storage.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_custom_options() {
        let storage = MemoryGrainStorage::with_options(MemoryGrainStorageOptions {
            num_storage_partitions: 4,
        });

        let grain_id = make_grain_id("test");
        let raw_state = RawGrainState {
            data: vec![42],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state).await.unwrap();

        let result = storage.read_state("State", &grain_id).await.unwrap();
        assert_eq!(result.data, vec![42]);
    }

    #[tokio::test]
    async fn test_upsert_with_etag_but_no_record() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test_upsert");

        // Try to update a non-existent record with an ETag (upsert)
        let raw_state = RawGrainState {
            data: vec![42],
            etag: Some("some-etag".to_string()),
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state).await.unwrap();

        // Verify it was written
        let result = storage.read_state("State", &grain_id).await.unwrap();
        assert!(result.record_exists);
        assert_eq!(result.data, vec![42]);
    }

    #[tokio::test]
    async fn test_clear_without_etag() {
        let storage = MemoryGrainStorage::new();
        let grain_id = make_grain_id("test_clear_no_etag");

        // Write state
        let raw_state = RawGrainState {
            data: vec![1, 2, 3],
            etag: None,
            record_exists: false,
        };
        storage.write_state("State", &grain_id, &raw_state).await.unwrap();

        // Clear without ETag check
        storage.clear_state("State", &grain_id, None).await.unwrap();

        // Should be cleared
        let result = storage.read_state("State", &grain_id).await.unwrap();
        assert!(!result.record_exists);
    }
}
