//! # Orleans Event Sourcing
//!
//! Event sourcing infrastructure for Orleans grains, enabling audit trails,
//! temporal queries, and complex state reconstruction.
//!
//! ## Overview
//!
//! Event sourcing is an architectural pattern where state changes are captured
//! as a sequence of immutable events rather than directly mutating state. This
//! provides:
//!
//! - **Audit trail**: Complete history of all changes
//! - **Temporal queries**: Query state at any point in time
//! - **Debugging**: Replay events to reproduce issues
//! - **Event-driven integration**: Events can be published to other systems
//!
//! ## Core Concepts
//!
//! ### Event Entries
//!
//! Events are stored as `EventEntry<E>` with:
//! - Sequence number (monotonically increasing)
//! - Timestamp
//! - Event payload
//! - Optional metadata (correlation ID, user ID, etc.)
//!
//! ### State Reconstruction
//!
//! State is reconstructed by applying events in sequence using an `EventApplier`:
//!
//! ```ignore
//! struct CounterApplier;
//!
//! impl EventApplier<CounterState, CounterEvent> for CounterApplier {
//!     fn apply(state: &mut CounterState, event: &CounterEvent) {
//!         match event {
//!             CounterEvent::Incremented(n) => state.value += n,
//!             CounterEvent::Decremented(n) => state.value -= n,
//!         }
//!     }
//! }
//! ```
//!
//! ### Snapshots
//!
//! For grains with many events, snapshots speed up activation:
//! - State is periodically snapshotted
//! - Activation loads snapshot + subsequent events
//! - Configurable snapshot interval
//!
//! ## Usage Example
//!
//! ```ignore
//! use orleans_event_sourcing::{
//!     JournaledGrain, EventApplier, InMemoryEventStorage,
//! };
//!
//! #[derive(Clone, Default, Serialize, Deserialize)]
//! struct BankAccountState {
//!     balance: i64,
//! }
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! enum BankAccountEvent {
//!     Deposited(i64),
//!     Withdrawn(i64),
//! }
//!
//! struct BankAccountApplier;
//!
//! impl EventApplier<BankAccountState, BankAccountEvent> for BankAccountApplier {
//!     fn apply(state: &mut BankAccountState, event: &BankAccountEvent) {
//!         match event {
//!             BankAccountEvent::Deposited(amount) => state.balance += amount,
//!             BankAccountEvent::Withdrawn(amount) => state.balance -= amount,
//!         }
//!     }
//! }
//!
//! async fn example() {
//!     let storage = Arc::new(InMemoryEventStorage::new());
//!     let mut grain = JournaledGrain::<_, _, BankAccountApplier>::new(grain_id, storage);
//!
//!     grain.on_activate().await?;
//!
//!     // Raise events (applied to tentative state)
//!     grain.raise_event(BankAccountEvent::Deposited(100));
//!     grain.raise_event(BankAccountEvent::Withdrawn(30));
//!
//!     // Confirm (persists to storage)
//!     grain.confirm_events().await?;
//!
//!     // Query state at previous version
//!     let old_state = grain.get_state_at_version(1).await?;
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                       JournaledGrain<S, E, A>                    │
//! │  ┌─────────────────────────────────────────────────────────┐   │
//! │  │              LogViewAdaptor<S, E, A>                     │   │
//! │  │  ┌───────────────────┐  ┌───────────────────────────┐  │   │
//! │  │  │  Confirmed State   │  │     Tentative State       │  │   │
//! │  │  │  (from storage)    │  │  (includes pending events)│  │   │
//! │  │  └───────────────────┘  └───────────────────────────┘  │   │
//! │  └─────────────────────────────────────────────────────────┘   │
//! │                           │                                     │
//! └───────────────────────────│─────────────────────────────────────┘
//!                             │
//!            ┌────────────────┴────────────────┐
//!            │                                  │
//!            ▼                                  ▼
//! ┌─────────────────────┐           ┌─────────────────────┐
//! │   IEventStorage     │           │  ISnapshotStorage   │
//! │  (append/read)      │           │  (save/load)        │
//! └─────────────────────┘           └─────────────────────┘
//! ```
//!
//! ## Storage Implementations
//!
//! - `InMemoryEventStorage` - For testing and development
//! - `InMemorySnapshotStorage` - For testing and development
//! - PostgreSQL and S3 implementations available in separate crates

pub mod adaptor;
pub mod error;
pub mod event_entry;
pub mod journaled_grain;
pub mod snapshot;
pub mod storage;
pub mod traits;

// Re-exports for convenience
pub use adaptor::{LogViewAdaptor, LogViewAdaptorFactory, LogViewAdaptorOptions};
pub use error::{EventSourcingError, EventSourcingResult};
pub use event_entry::{EventEntry, EventMetadata, LogViewState};
pub use journaled_grain::{JournaledGrain, JournaledGrainBuilder};
pub use snapshot::{SnapshotConfig, SnapshotMetadata, SnapshotState};
pub use storage::{InMemoryEventStorage, InMemoryLogStorage, InMemorySnapshotStorage};
pub use traits::{EventApplier, IEventStorage, ILogConsistentGrain, ILogViewAdaptor, ISnapshotStorage};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use std::sync::Arc;

    // Test types for integration tests

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
    struct ShoppingCartState {
        items: Vec<CartItem>,
        total: i64,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    struct CartItem {
        product_id: String,
        name: String,
        price: i64,
        quantity: u32,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum ShoppingCartEvent {
        ItemAdded {
            product_id: String,
            name: String,
            price: i64,
        },
        ItemQuantityChanged {
            product_id: String,
            new_quantity: u32,
        },
        ItemRemoved {
            product_id: String,
        },
        CartCleared,
    }

    struct ShoppingCartApplier;

    impl EventApplier<ShoppingCartState, ShoppingCartEvent> for ShoppingCartApplier {
        fn apply(state: &mut ShoppingCartState, event: &ShoppingCartEvent) {
            match event {
                ShoppingCartEvent::ItemAdded {
                    product_id,
                    name,
                    price,
                } => {
                    state.items.push(CartItem {
                        product_id: product_id.clone(),
                        name: name.clone(),
                        price: *price,
                        quantity: 1,
                    });
                    state.total += price;
                }
                ShoppingCartEvent::ItemQuantityChanged {
                    product_id,
                    new_quantity,
                } => {
                    if let Some(item) = state.items.iter_mut().find(|i| &i.product_id == product_id)
                    {
                        let old_quantity = item.quantity as i64;
                        let new_quantity = *new_quantity as i64;
                        state.total += (new_quantity - old_quantity) * item.price;
                        item.quantity = new_quantity as u32;
                    }
                }
                ShoppingCartEvent::ItemRemoved { product_id } => {
                    if let Some(idx) = state.items.iter().position(|i| &i.product_id == product_id)
                    {
                        let item = state.items.remove(idx);
                        state.total -= item.price * item.quantity as i64;
                    }
                }
                ShoppingCartEvent::CartCleared => {
                    state.items.clear();
                    state.total = 0;
                }
            }
        }
    }

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("ShoppingCart"), IdSpan::from_str(key))
    }

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<EventEntry<()>>();
        let _ = std::any::type_name::<EventMetadata>();
        let _ = std::any::type_name::<LogViewState<(), ()>>();
        let _ = std::any::type_name::<EventSourcingError>();
        let _ = std::any::type_name::<InMemoryEventStorage<()>>();
        let _ = std::any::type_name::<InMemorySnapshotStorage<()>>();
        let _ = std::any::type_name::<SnapshotConfig>();
        let _ = std::any::type_name::<SnapshotMetadata>();
    }

    #[tokio::test]
    async fn test_full_event_sourcing_flow() {
        let event_storage = Arc::new(InMemoryEventStorage::<ShoppingCartEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<ShoppingCartState>::new());
        let grain_id = make_grain_id("cart-1");

        // Create and activate grain
        let mut grain: JournaledGrain<ShoppingCartState, ShoppingCartEvent, ShoppingCartApplier> =
            JournaledGrain::with_snapshots(
                grain_id.clone(),
                event_storage.clone(),
                snapshot_storage.clone(),
            );

        grain.on_activate().await.unwrap();
        assert_eq!(grain.confirmed_version(), 0);
        assert_eq!(grain.state().total, 0);

        // Add items
        grain.raise_event(ShoppingCartEvent::ItemAdded {
            product_id: "P1".into(),
            name: "Widget".into(),
            price: 1000,
        });
        grain.raise_event(ShoppingCartEvent::ItemAdded {
            product_id: "P2".into(),
            name: "Gadget".into(),
            price: 2500,
        });

        // Tentative state should be updated
        assert_eq!(grain.state().items.len(), 2);
        assert_eq!(grain.state().total, 3500);
        assert_eq!(grain.tentative_version(), 2);

        // Confirmed state should not be updated
        assert_eq!(grain.confirmed_state().items.len(), 0);
        assert_eq!(grain.confirmed_version(), 0);

        // Confirm events
        grain.confirm_events().await.unwrap();
        assert_eq!(grain.confirmed_version(), 2);
        assert_eq!(grain.confirmed_state().total, 3500);

        // Modify quantity
        grain.raise_event(ShoppingCartEvent::ItemQuantityChanged {
            product_id: "P1".into(),
            new_quantity: 3,
        });
        grain.confirm_events().await.unwrap();

        assert_eq!(grain.state().items[0].quantity, 3);
        assert_eq!(grain.state().total, 5500); // 3*1000 + 2500

        // Remove item
        grain.raise_event(ShoppingCartEvent::ItemRemoved {
            product_id: "P1".into(),
        });
        grain.confirm_events().await.unwrap();

        assert_eq!(grain.state().items.len(), 1);
        assert_eq!(grain.state().total, 2500);

        // Take snapshot
        grain.take_snapshot().await.unwrap();

        // Deactivate
        grain.on_deactivate().await.unwrap();

        // Verify storage state
        let version = event_storage.get_version(&grain_id).await.unwrap();
        assert_eq!(version, 4);

        let snapshot = snapshot_storage.load_snapshot(&grain_id).await.unwrap();
        assert!(snapshot.is_some());
        let (state, snap_version) = snapshot.unwrap();
        assert_eq!(snap_version, 4);
        assert_eq!(state.total, 2500);
    }

    #[tokio::test]
    async fn test_temporal_queries() {
        let event_storage = Arc::new(InMemoryEventStorage::<ShoppingCartEvent>::new());
        let grain_id = make_grain_id("cart-2");

        let mut grain: JournaledGrain<ShoppingCartState, ShoppingCartEvent, ShoppingCartApplier> =
            JournaledGrain::new(grain_id, event_storage);

        grain.on_activate().await.unwrap();

        // Create history
        for i in 1..=5 {
            grain.raise_event(ShoppingCartEvent::ItemAdded {
                product_id: format!("P{}", i),
                name: format!("Product {}", i),
                price: i as i64 * 100,
            });
            grain.confirm_events().await.unwrap();
        }

        // Query state at different points
        let state_v1 = grain.get_state_at_version(1).await.unwrap();
        assert_eq!(state_v1.items.len(), 1);
        assert_eq!(state_v1.total, 100);

        let state_v3 = grain.get_state_at_version(3).await.unwrap();
        assert_eq!(state_v3.items.len(), 3);
        assert_eq!(state_v3.total, 600); // 100 + 200 + 300

        let state_v5 = grain.get_state_at_version(5).await.unwrap();
        assert_eq!(state_v5.items.len(), 5);
        assert_eq!(state_v5.total, 1500); // 100 + 200 + 300 + 400 + 500

        // Get events since version 2
        let events = grain.get_events_since(2).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence(), 3);
    }

    #[tokio::test]
    async fn test_abort_and_refresh() {
        let event_storage = Arc::new(InMemoryEventStorage::<ShoppingCartEvent>::new());
        let grain_id = make_grain_id("cart-3");

        let mut grain: JournaledGrain<ShoppingCartState, ShoppingCartEvent, ShoppingCartApplier> =
            JournaledGrain::new(grain_id, event_storage);

        grain.on_activate().await.unwrap();

        // Add and confirm an item
        grain.raise_event(ShoppingCartEvent::ItemAdded {
            product_id: "P1".into(),
            name: "Keep".into(),
            price: 1000,
        });
        grain.confirm_events().await.unwrap();

        // Add another item but abort
        grain.raise_event(ShoppingCartEvent::ItemAdded {
            product_id: "P2".into(),
            name: "Discard".into(),
            price: 2000,
        });
        assert_eq!(grain.state().items.len(), 2);

        grain.abort_pending_events();
        assert_eq!(grain.state().items.len(), 1);
        assert_eq!(grain.state().total, 1000);
    }

    #[tokio::test]
    async fn test_reactivation_with_snapshot() {
        let event_storage = Arc::new(InMemoryEventStorage::<ShoppingCartEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<ShoppingCartState>::new());
        let grain_id = make_grain_id("cart-4");

        // First session
        {
            let mut grain: JournaledGrain<ShoppingCartState, ShoppingCartEvent, ShoppingCartApplier> =
                JournaledGrain::with_snapshots(
                    grain_id.clone(),
                    event_storage.clone(),
                    snapshot_storage.clone(),
                );

            grain.on_activate().await.unwrap();

            // Add 3 items
            for i in 1..=3 {
                grain.raise_event(ShoppingCartEvent::ItemAdded {
                    product_id: format!("P{}", i),
                    name: format!("Product {}", i),
                    price: i as i64 * 100,
                });
            }
            grain.confirm_events().await.unwrap();

            // Take snapshot at version 3
            grain.take_snapshot().await.unwrap();

            // Add 2 more items after snapshot
            for i in 4..=5 {
                grain.raise_event(ShoppingCartEvent::ItemAdded {
                    product_id: format!("P{}", i),
                    name: format!("Product {}", i),
                    price: i as i64 * 100,
                });
            }
            grain.confirm_events().await.unwrap();

            grain.on_deactivate().await.unwrap();
        }

        // Second session - should load from snapshot + replay events
        {
            let mut grain: JournaledGrain<ShoppingCartState, ShoppingCartEvent, ShoppingCartApplier> =
                JournaledGrain::with_snapshots(grain_id, event_storage, snapshot_storage);

            grain.on_activate().await.unwrap();

            // Should have all 5 items
            assert_eq!(grain.state().items.len(), 5);
            assert_eq!(grain.confirmed_version(), 5);
            assert_eq!(grain.state().total, 1500); // 100+200+300+400+500
        }
    }

    #[test]
    fn test_event_metadata() {
        let metadata = EventMetadata::new()
            .with_correlation_id("corr-123")
            .with_user_id("user-456")
            .with_session_id("sess-789")
            .with_custom("key", "value");

        assert_eq!(metadata.correlation_id.as_deref(), Some("corr-123"));
        assert_eq!(metadata.user_id.as_deref(), Some("user-456"));
        assert_eq!(metadata.session_id.as_deref(), Some("sess-789"));
        assert_eq!(metadata.custom.get("key").map(String::as_str), Some("value"));
    }
}
