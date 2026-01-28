//! Traits for event sourcing grains and storage.

use crate::error::EventSourcingResult;
use crate::event_entry::EventEntry;
use async_trait::async_trait;
use orleans_core::GrainId;
use serde::{de::DeserializeOwned, Serialize};
use std::fmt::Debug;

/// Marker trait for grains that use event sourcing.
///
/// Grains implementing this trait maintain their state by applying
/// a sequence of events rather than directly mutating state.
pub trait ILogConsistentGrain: Send + Sync + Debug {
    /// Returns `true` if this grain is configured for event sourcing.
    fn is_event_sourced(&self) -> bool {
        true
    }
}

/// Event application function type.
///
/// Takes current state and an event, returns the new state.
pub trait EventApplier<S, E> {
    /// Applies an event to the state, returning the modified state.
    fn apply(state: &mut S, event: &E);
}

/// Trait for accessing and manipulating the event log.
#[async_trait]
pub trait ILogViewAdaptor<S, E>: Send + Sync
where
    S: Clone + Default + Send + Sync,
    E: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    /// Returns the confirmed version (number of persisted events).
    fn confirmed_version(&self) -> u64;

    /// Returns the tentative version (confirmed + pending events).
    fn tentative_version(&self) -> u64;

    /// Returns a reference to the confirmed state.
    fn confirmed_state(&self) -> &S;

    /// Returns a reference to the tentative state (including pending events).
    fn tentative_state(&self) -> &S;

    /// Raises a new event that will be applied to the tentative state.
    fn raise_event(&mut self, event: E);

    /// Confirms all pending events (writes to storage).
    async fn confirm_events(&mut self) -> EventSourcingResult<()>;

    /// Aborts all pending events (discards uncommitted changes).
    fn abort_pending_events(&mut self);

    /// Refreshes state from storage.
    async fn refresh(&mut self) -> EventSourcingResult<()>;
}

/// Storage trait for event persistence.
#[async_trait]
pub trait IEventStorage<E>: Send + Sync
where
    E: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    /// Appends events to the event log for the given grain.
    ///
    /// Returns the new confirmed version after append.
    async fn append_events(
        &self,
        grain_id: &GrainId,
        events: Vec<EventEntry<E>>,
        expected_version: u64,
    ) -> EventSourcingResult<u64>;

    /// Reads events from the event log starting at the given sequence.
    async fn read_events(
        &self,
        grain_id: &GrainId,
        from_sequence: u64,
        max_count: Option<usize>,
    ) -> EventSourcingResult<Vec<EventEntry<E>>>;

    /// Reads all events for the grain.
    async fn read_all_events(&self, grain_id: &GrainId) -> EventSourcingResult<Vec<EventEntry<E>>>;

    /// Returns the current version (latest event sequence number).
    async fn get_version(&self, grain_id: &GrainId) -> EventSourcingResult<u64>;

    /// Clears all events for the grain (for testing).
    async fn clear(&self, grain_id: &GrainId) -> EventSourcingResult<()>;
}

/// Storage trait for state snapshots.
#[async_trait]
pub trait ISnapshotStorage<S>: Send + Sync
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned,
{
    /// Saves a snapshot at the given version.
    async fn save_snapshot(
        &self,
        grain_id: &GrainId,
        state: &S,
        version: u64,
    ) -> EventSourcingResult<()>;

    /// Loads the latest snapshot.
    ///
    /// Returns `None` if no snapshot exists.
    async fn load_snapshot(&self, grain_id: &GrainId) -> EventSourcingResult<Option<(S, u64)>>;

    /// Loads a snapshot at or before the given version.
    async fn load_snapshot_at_or_before(
        &self,
        grain_id: &GrainId,
        version: u64,
    ) -> EventSourcingResult<Option<(S, u64)>>;

    /// Deletes old snapshots, keeping only the most recent `keep_count`.
    async fn cleanup_old_snapshots(
        &self,
        grain_id: &GrainId,
        keep_count: usize,
    ) -> EventSourcingResult<()>;
}

/// Combined event and snapshot storage.
#[async_trait]
pub trait ILogStorage<S, E>: IEventStorage<E> + ISnapshotStorage<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned,
    E: Clone + Send + Sync + Serialize + DeserializeOwned,
{
}

// Blanket implementation for types that implement both traits
impl<T, S, E> ILogStorage<S, E> for T
where
    T: IEventStorage<E> + ISnapshotStorage<S>,
    S: Clone + Send + Sync + Serialize + DeserializeOwned,
    E: Clone + Send + Sync + Serialize + DeserializeOwned,
{
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestGrain;

    impl ILogConsistentGrain for TestGrain {}

    #[test]
    fn test_log_consistent_grain_default() {
        let grain = TestGrain;
        assert!(grain.is_event_sourced());
    }

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
    struct CounterState {
        value: i32,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum CounterEvent {
        Incremented(i32),
        Decremented(i32),
    }

    struct CounterApplier;

    impl EventApplier<CounterState, CounterEvent> for CounterApplier {
        fn apply(state: &mut CounterState, event: &CounterEvent) {
            match event {
                CounterEvent::Incremented(n) => state.value += n,
                CounterEvent::Decremented(n) => state.value -= n,
            }
        }
    }

    #[test]
    fn test_event_applier() {
        let mut state = CounterState::default();

        CounterApplier::apply(&mut state, &CounterEvent::Incremented(5));
        assert_eq!(state.value, 5);

        CounterApplier::apply(&mut state, &CounterEvent::Decremented(3));
        assert_eq!(state.value, 2);
    }
}
