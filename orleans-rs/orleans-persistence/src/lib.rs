//! Orleans Persistence - Grain state storage framework.
//!
//! This crate provides the persistence layer for Orleans grains, enabling them
//! to store state durably across activations. It implements an optimistic
//! concurrency model using ETags for conflict detection.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                           Grain                                  │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │               StateStorageBridge<TState>                 │   │
//! │  │  ┌───────────────────────────────────────────────────┐  │   │
//! │  │  │            GrainState<TState>                      │  │   │
//! │  │  │  - state: TState                                   │  │   │
//! │  │  │  - etag: Option<String>                           │  │   │
//! │  │  │  - record_exists: bool                            │  │   │
//! │  │  └───────────────────────────────────────────────────┘  │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                            │                                     │
//! └────────────────────────────│─────────────────────────────────────┘
//!                              │ IGrainStorage
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Storage Provider                            │
//! │  ┌───────────────┐  ┌───────────────┐  ┌───────────────────┐   │
//! │  │MemoryStorage  │  │  SQL Storage  │  │ DynamoDB Storage  │   │
//! │  │  (testing)    │  │  (ADO.NET)    │  │     (AWS)         │   │
//! │  └───────────────┘  └───────────────┘  └───────────────────┘   │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Core Concepts
//!
//! ## GrainState<T>
//!
//! Wraps the grain's state with metadata for persistence:
//! - `state`: The actual state data
//! - `etag`: Version token for optimistic concurrency
//! - `record_exists`: Whether state has been persisted
//!
//! ## IGrainStorage
//!
//! The trait that storage providers implement. Provides:
//! - `read_state`: Load state from storage
//! - `write_state`: Save state with ETag verification
//! - `clear_state`: Remove state from storage
//!
//! ## StateStorageBridge<T>
//!
//! Adapter that connects a grain to its storage provider, handling
//! initialization and providing a convenient API.
//!
//! # Example Usage
//!
//! ```ignore
//! use orleans_persistence::{
//!     GrainState, IGrainStorage, MemoryGrainStorage, StateStorageBridge,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Default, Clone, Serialize, Deserialize)]
//! struct CounterState {
//!     count: i32,
//! }
//!
//! struct CounterGrain {
//!     state: StateStorageBridge<CounterState>,
//! }
//!
//! impl CounterGrain {
//!     async fn on_activate(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//!         // Load state on activation
//!         self.state.read_state().await?;
//!         Ok(())
//!     }
//!
//!     async fn increment(&mut self) -> Result<i32, Box<dyn std::error::Error>> {
//!         self.state.state_mut().count += 1;
//!         self.state.write_state().await?;
//!         Ok(self.state.state().count)
//!     }
//!
//!     async fn reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//!         self.state.clear_state().await?;
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Optimistic Concurrency
//!
//! All write operations use optimistic concurrency control:
//!
//! 1. When state is read, the current ETag is stored
//! 2. When state is written, the stored ETag is compared to the expected ETag
//! 3. If they don't match, `StorageError::EtagMismatch` is returned
//! 4. On success, a new ETag is generated
//!
//! This allows multiple activations (in case of grain migration or directory
//! inconsistency) to safely detect conflicts.
//!
//! # Storage Providers
//!
//! ## MemoryGrainStorage
//!
//! In-memory storage for testing and development. State is lost on restart.
//!
//! ```
//! use orleans_persistence::MemoryGrainStorage;
//!
//! let storage = MemoryGrainStorage::new();
//! ```
//!
//! Additional storage providers (SQL, DynamoDB, etc.) can be implemented
//! by implementing the `IGrainStorage` trait.

pub mod error;
pub mod grain_state;
pub mod memory_storage;
pub mod state_bridge;
pub mod storage;

// Re-exports for convenience
pub use error::{InconsistentStateError, StorageError, StorageResult};
pub use grain_state::{GrainState, StoredGrainState};
pub use memory_storage::{MemoryGrainStorage, MemoryGrainStorageOptions};
pub use state_bridge::{PersistentGrain, StateStorageBridge};
pub use storage::{
    GrainStorageSerializer, IGrainStorage, IStorage, IStorageTyped, RawGrainState,
};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
    struct TestState {
        value: i32,
    }

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(key))
    }

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<GrainState<TestState>>();
        let _ = std::any::type_name::<MemoryGrainStorage>();
        let _ = std::any::type_name::<StateStorageBridge<TestState>>();
        let _ = std::any::type_name::<StorageError>();
    }

    #[tokio::test]
    async fn test_end_to_end_persistence() {
        let storage = Arc::new(MemoryGrainStorage::new());
        let grain_id = make_grain_id("e2e-test");

        // First activation - create state
        {
            let mut bridge: StateStorageBridge<TestState> =
                StateStorageBridge::new(grain_id.clone(), "State", storage.clone());

            bridge.read_state().await.unwrap();
            assert!(!bridge.record_exists());

            bridge.state_mut().value = 42;
            bridge.write_state().await.unwrap();
            assert!(bridge.record_exists());
        }

        // Second activation - read existing state
        {
            let mut bridge: StateStorageBridge<TestState> =
                StateStorageBridge::new(grain_id.clone(), "State", storage.clone());

            bridge.read_state().await.unwrap();
            assert!(bridge.record_exists());
            assert_eq!(bridge.state().value, 42);

            // Modify
            bridge.state_mut().value = 100;
            bridge.write_state().await.unwrap();
        }

        // Third activation - verify modifications
        {
            let mut bridge: StateStorageBridge<TestState> =
                StateStorageBridge::new(grain_id.clone(), "State", storage.clone());

            bridge.read_state().await.unwrap();
            assert_eq!(bridge.state().value, 100);

            // Clear
            bridge.clear_state().await.unwrap();
            assert!(!bridge.record_exists());
        }

        // Fourth activation - verify cleared
        {
            let mut bridge: StateStorageBridge<TestState> =
                StateStorageBridge::new(grain_id, "State", storage);

            bridge.read_state().await.unwrap();
            assert!(!bridge.record_exists());
            assert_eq!(bridge.state().value, 0); // default
        }
    }
}
