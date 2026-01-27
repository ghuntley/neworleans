//! State storage bridge connecting grains to storage providers.
//!
//! The `StateStorageBridge` acts as an adapter between the grain and its
//! storage provider, handling state lifecycle and providing a convenient API.

use std::sync::Arc;

use async_trait::async_trait;
use orleans_core::GrainId;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, instrument, warn};

use crate::error::{StorageError, StorageResult};
use crate::grain_state::GrainState;
use crate::storage::{GrainStorageSerializer, IGrainStorage, IStorage, IStorageTyped, RawGrainState};

/// Bridge connecting a grain's persistent state to a storage provider.
///
/// This struct wraps a `GrainState<T>` and provides a convenient API for
/// reading, writing, and clearing state. It handles initialization and
/// tracks whether state has been read.
///
/// # Usage in Grains
///
/// ```ignore
/// struct MyGrain {
///     state: StateStorageBridge<MyState>,
/// }
///
/// impl MyGrain {
///     async fn on_activate(&mut self, context: &dyn IGrainContext) -> Result<()> {
///         self.state.read_state().await?;
///         Ok(())
///     }
///
///     async fn update_something(&mut self, value: i32) -> Result<()> {
///         self.state.state_mut().value = value;
///         self.state.write_state().await?;
///         Ok(())
///     }
/// }
/// ```
pub struct StateStorageBridge<TState> {
    /// The grain ID this bridge is for.
    grain_id: GrainId,
    /// The state name (used as part of storage key).
    state_name: String,
    /// The storage provider.
    storage: Arc<dyn IGrainStorage>,
    /// The serializer for state data.
    serializer: GrainStorageSerializer,
    /// The current grain state.
    grain_state: GrainState<TState>,
    /// Whether state has been initialized (read_state called).
    is_initialized: bool,
}

impl<TState: Default + Clone> StateStorageBridge<TState> {
    /// Create a new state storage bridge.
    ///
    /// # Arguments
    ///
    /// * `grain_id` - The grain's identity
    /// * `state_name` - The name of this state (used in storage key)
    /// * `storage` - The storage provider to use
    pub fn new(
        grain_id: GrainId,
        state_name: impl Into<String>,
        storage: Arc<dyn IGrainStorage>,
    ) -> Self {
        Self {
            grain_id,
            state_name: state_name.into(),
            storage,
            serializer: GrainStorageSerializer::new(),
            grain_state: GrainState::new(),
            is_initialized: false,
        }
    }

    /// Create a new state storage bridge with an initial state value.
    pub fn with_state(
        grain_id: GrainId,
        state_name: impl Into<String>,
        storage: Arc<dyn IGrainStorage>,
        initial_state: TState,
    ) -> Self {
        Self {
            grain_id,
            state_name: state_name.into(),
            storage,
            serializer: GrainStorageSerializer::new(),
            grain_state: GrainState::with_state(initial_state),
            is_initialized: false,
        }
    }

    /// Check if the state has been initialized.
    pub fn is_initialized(&self) -> bool {
        self.is_initialized
    }

    /// Get the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Get the state name.
    pub fn state_name(&self) -> &str {
        &self.state_name
    }
}

#[async_trait]
impl<TState> IStorage for StateStorageBridge<TState>
where
    TState: Default + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn etag(&self) -> Option<&str> {
        self.grain_state.etag()
    }

    fn record_exists(&self) -> bool {
        self.grain_state.record_exists()
    }

    #[instrument(skip(self), fields(grain_id = %self.grain_id, state_name = %self.state_name))]
    async fn read_state(&mut self) -> StorageResult<()> {
        debug!("Reading state");

        let raw_state = self
            .storage
            .read_state(&self.state_name, &self.grain_id)
            .await?;

        if raw_state.record_exists {
            let state: TState = self.serializer.deserialize(&raw_state.data)?;
            self.grain_state.mark_read(state, raw_state.etag, true);
        } else {
            self.grain_state
                .mark_read(TState::default(), None, false);
        }

        self.is_initialized = true;

        debug!(
            record_exists = self.grain_state.record_exists(),
            etag = ?self.grain_state.etag(),
            "State read complete"
        );

        Ok(())
    }

    #[instrument(skip(self), fields(grain_id = %self.grain_id, state_name = %self.state_name))]
    async fn write_state(&mut self) -> StorageResult<()> {
        if !self.is_initialized {
            warn!("Attempting to write state before initialization");
            return Err(StorageError::StateNotInitialized);
        }

        debug!(
            current_etag = ?self.grain_state.etag(),
            "Writing state"
        );

        let data = self.serializer.serialize(self.grain_state.state())?;
        let raw_state = RawGrainState {
            data,
            etag: self.grain_state.etag().map(|s| s.to_string()),
            record_exists: self.grain_state.record_exists(),
        };

        let new_etag = self
            .storage
            .write_state(&self.state_name, &self.grain_id, &raw_state)
            .await?;

        self.grain_state.mark_written(new_etag);

        debug!(
            new_etag = ?self.grain_state.etag(),
            "State write complete"
        );

        Ok(())
    }

    #[instrument(skip(self), fields(grain_id = %self.grain_id, state_name = %self.state_name))]
    async fn clear_state(&mut self) -> StorageResult<()> {
        if !self.is_initialized {
            warn!("Attempting to clear state before initialization");
            return Err(StorageError::StateNotInitialized);
        }

        debug!("Clearing state");

        self.storage
            .clear_state(
                &self.state_name,
                &self.grain_id,
                self.grain_state.etag(),
            )
            .await?;

        self.grain_state.mark_cleared(TState::default());

        debug!("State cleared");

        Ok(())
    }
}

impl<TState> IStorageTyped<TState> for StateStorageBridge<TState>
where
    TState: Default + Clone + Serialize + DeserializeOwned + Send + Sync,
{
    fn state(&self) -> &TState {
        self.grain_state.state()
    }

    fn state_mut(&mut self) -> &mut TState {
        self.grain_state.state_mut()
    }
}

/// Extension trait for persistent grains.
///
/// This trait provides convenient methods for grains that use persistent state.
pub trait PersistentGrain {
    /// The type of state this grain persists.
    type State: Default + Clone + Serialize + DeserializeOwned + Send + Sync;

    /// Get the state storage bridge.
    fn storage(&self) -> &StateStorageBridge<Self::State>;

    /// Get a mutable reference to the state storage bridge.
    fn storage_mut(&mut self) -> &mut StateStorageBridge<Self::State>;

    /// Get a reference to the persisted state.
    fn state(&self) -> &Self::State {
        self.storage().state()
    }

    /// Get a mutable reference to the persisted state.
    fn state_mut(&mut self) -> &mut Self::State {
        self.storage_mut().state_mut()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_storage::MemoryGrainStorage;
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

    #[tokio::test]
    async fn test_bridge_initialization() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test1");
        let bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id.clone(), "State", storage);

        assert!(!bridge.is_initialized());
        assert_eq!(bridge.grain_id(), &grain_id);
        assert_eq!(bridge.state_name(), "State");
    }

    #[tokio::test]
    async fn test_bridge_with_initial_state() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test2");
        let initial = TestState {
            counter: 42,
            name: "initial".to_string(),
        };
        let bridge: StateStorageBridge<TestState> =
            StateStorageBridge::with_state(grain_id, "State", storage, initial.clone());

        assert_eq!(*bridge.state(), initial);
        assert!(!bridge.is_initialized());
    }

    #[tokio::test]
    async fn test_read_state_initializes() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test3");
        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);

        assert!(!bridge.is_initialized());

        bridge.read_state().await.unwrap();

        assert!(bridge.is_initialized());
        assert!(!bridge.record_exists());
    }

    #[tokio::test]
    async fn test_write_before_read_fails() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test4");
        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);

        let result = bridge.write_state().await;
        assert!(matches!(result, Err(StorageError::StateNotInitialized)));
    }

    #[tokio::test]
    async fn test_clear_before_read_fails() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test5");
        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);

        let result = bridge.clear_state().await;
        assert!(matches!(result, Err(StorageError::StateNotInitialized)));
    }

    #[tokio::test]
    async fn test_full_lifecycle() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test6");

        // Create and initialize
        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id.clone(), "State", storage.clone());

        bridge.read_state().await.unwrap();
        assert!(!bridge.record_exists());

        // Modify and write
        bridge.state_mut().counter = 42;
        bridge.state_mut().name = "modified".to_string();
        bridge.write_state().await.unwrap();
        assert!(bridge.record_exists());

        // Create new bridge to same grain and verify persistence
        let mut bridge2: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);

        bridge2.read_state().await.unwrap();
        assert!(bridge2.record_exists());
        assert_eq!(bridge2.state().counter, 42);
        assert_eq!(bridge2.state().name, "modified");
    }

    #[tokio::test]
    async fn test_clear_removes_state() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test7");

        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id.clone(), "State", storage.clone());

        // Write some state
        bridge.read_state().await.unwrap();
        bridge.state_mut().counter = 100;
        bridge.write_state().await.unwrap();

        // Clear it
        bridge.clear_state().await.unwrap();
        assert!(!bridge.record_exists());
        assert!(bridge.etag().is_none());

        // Verify it's gone
        let mut bridge2: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);
        bridge2.read_state().await.unwrap();
        assert!(!bridge2.record_exists());
    }

    #[tokio::test]
    async fn test_etag_tracking() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test8");

        let mut bridge: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);

        bridge.read_state().await.unwrap();
        assert!(bridge.etag().is_none());

        bridge.state_mut().counter = 1;
        bridge.write_state().await.unwrap();
        let etag1 = bridge.etag().unwrap().to_string();

        bridge.state_mut().counter = 2;
        bridge.write_state().await.unwrap();
        let etag2 = bridge.etag().unwrap().to_string();

        // ETags should change on each write
        assert_ne!(etag1, etag2);
    }

    #[tokio::test]
    async fn test_concurrent_modification_detection() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("test9");

        // First bridge
        let mut bridge1: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id.clone(), "State", storage.clone());
        bridge1.read_state().await.unwrap();
        bridge1.state_mut().counter = 1;
        bridge1.write_state().await.unwrap();
        let etag1 = bridge1.etag().unwrap().to_string();

        // Second bridge reads same state
        let mut bridge2: StateStorageBridge<TestState> =
            StateStorageBridge::new(grain_id, "State", storage);
        bridge2.read_state().await.unwrap();
        assert_eq!(bridge2.etag().unwrap(), etag1);

        // First bridge writes again
        bridge1.state_mut().counter = 2;
        bridge1.write_state().await.unwrap();

        // Second bridge now has stale ETag - write should fail
        bridge2.state_mut().counter = 3;
        let result = bridge2.write_state().await;
        assert!(matches!(result, Err(StorageError::EtagMismatch { .. })));
    }
}
