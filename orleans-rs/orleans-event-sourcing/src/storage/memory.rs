//! In-memory storage implementations for event sourcing.

use crate::error::{EventSourcingError, EventSourcingResult};
use crate::event_entry::EventEntry;
use crate::traits::{IEventStorage, ISnapshotStorage};
use async_trait::async_trait;
use dashmap::DashMap;
use orleans_core::GrainId;
use parking_lot::RwLock;
use serde::{de::DeserializeOwned, Serialize};
use std::collections::BTreeMap;
use std::marker::PhantomData;
use tracing::{debug, instrument, trace};

/// In-memory event storage for testing and development.
///
/// Events are stored in a concurrent hash map keyed by grain ID.
/// Each grain's events are stored in a sorted map by sequence number.
#[derive(Debug)]
pub struct InMemoryEventStorage<E> {
    /// Events stored per grain ID.
    events: DashMap<GrainId, RwLock<BTreeMap<u64, EventEntry<E>>>>,
    _marker: PhantomData<E>,
}

impl<E> Default for InMemoryEventStorage<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> InMemoryEventStorage<E> {
    /// Creates a new in-memory event storage.
    pub fn new() -> Self {
        Self {
            events: DashMap::new(),
            _marker: PhantomData,
        }
    }

    /// Returns the number of grains with events.
    pub fn grain_count(&self) -> usize {
        self.events.len()
    }

    /// Returns the total number of events across all grains.
    pub fn total_event_count(&self) -> usize {
        self.events
            .iter()
            .map(|entry| entry.value().read().len())
            .sum()
    }
}

#[async_trait]
impl<E> IEventStorage<E> for InMemoryEventStorage<E>
where
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    #[instrument(skip(self, events), fields(grain_id = %grain_id, event_count = events.len(), expected_version))]
    async fn append_events(
        &self,
        grain_id: &GrainId,
        events: Vec<EventEntry<E>>,
        expected_version: u64,
    ) -> EventSourcingResult<u64> {
        let entry = self
            .events
            .entry(grain_id.clone())
            .or_insert_with(|| RwLock::new(BTreeMap::new()));

        let mut log = entry.write();

        // Check expected version
        let current_version = log.keys().last().copied().unwrap_or(0);
        if current_version != expected_version {
            debug!(
                current_version,
                expected_version, "version conflict during append"
            );
            return Err(EventSourcingError::VersionConflict {
                expected: expected_version,
                current: current_version,
            });
        }

        // Append events
        let mut new_version = current_version;
        for event in events {
            let seq = event.sequence();
            if seq != new_version + 1 {
                return Err(EventSourcingError::SequenceOutOfOrder {
                    expected: new_version + 1,
                    actual: seq,
                });
            }
            trace!(sequence = seq, "appending event");
            log.insert(seq, event);
            new_version = seq;
        }

        debug!(new_version, "events appended successfully");
        Ok(new_version)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id, from_sequence, max_count))]
    async fn read_events(
        &self,
        grain_id: &GrainId,
        from_sequence: u64,
        max_count: Option<usize>,
    ) -> EventSourcingResult<Vec<EventEntry<E>>> {
        let entry = match self.events.get(grain_id) {
            Some(e) => e,
            None => {
                debug!("no events found for grain");
                return Ok(Vec::new());
            }
        };

        let log = entry.read();
        let events: Vec<_> = log
            .range(from_sequence..)
            .take(max_count.unwrap_or(usize::MAX))
            .map(|(_, e)| e.clone())
            .collect();

        debug!(event_count = events.len(), "read events");
        Ok(events)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn read_all_events(&self, grain_id: &GrainId) -> EventSourcingResult<Vec<EventEntry<E>>> {
        self.read_events(grain_id, 1, None).await
    }

    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn get_version(&self, grain_id: &GrainId) -> EventSourcingResult<u64> {
        let version = self
            .events
            .get(grain_id)
            .map(|entry| entry.read().keys().last().copied().unwrap_or(0))
            .unwrap_or(0);

        trace!(version, "got version");
        Ok(version)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn clear(&self, grain_id: &GrainId) -> EventSourcingResult<()> {
        self.events.remove(grain_id);
        debug!("cleared events for grain");
        Ok(())
    }
}

/// In-memory snapshot storage for testing and development.
#[derive(Debug)]
pub struct InMemorySnapshotStorage<S> {
    /// Snapshots stored per grain ID (version -> state).
    snapshots: DashMap<GrainId, RwLock<BTreeMap<u64, S>>>,
}

impl<S> Default for InMemorySnapshotStorage<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> InMemorySnapshotStorage<S> {
    /// Creates a new in-memory snapshot storage.
    pub fn new() -> Self {
        Self {
            snapshots: DashMap::new(),
        }
    }

    /// Returns the number of grains with snapshots.
    pub fn grain_count(&self) -> usize {
        self.snapshots.len()
    }
}

#[async_trait]
impl<S> ISnapshotStorage<S> for InMemorySnapshotStorage<S>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    #[instrument(skip(self, state), fields(grain_id = %grain_id, version))]
    async fn save_snapshot(
        &self,
        grain_id: &GrainId,
        state: &S,
        version: u64,
    ) -> EventSourcingResult<()> {
        let entry = self
            .snapshots
            .entry(grain_id.clone())
            .or_insert_with(|| RwLock::new(BTreeMap::new()));

        entry.write().insert(version, state.clone());
        debug!("saved snapshot");
        Ok(())
    }

    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn load_snapshot(&self, grain_id: &GrainId) -> EventSourcingResult<Option<(S, u64)>> {
        let result = self.snapshots.get(grain_id).and_then(|entry| {
            let snapshots = entry.read();
            snapshots
                .iter()
                .next_back()
                .map(|(version, state)| (state.clone(), *version))
        });

        debug!(found = result.is_some(), "loaded snapshot");
        Ok(result)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id, version))]
    async fn load_snapshot_at_or_before(
        &self,
        grain_id: &GrainId,
        version: u64,
    ) -> EventSourcingResult<Option<(S, u64)>> {
        let result = self.snapshots.get(grain_id).and_then(|entry| {
            let snapshots = entry.read();
            snapshots
                .range(..=version)
                .next_back()
                .map(|(v, state)| (state.clone(), *v))
        });

        debug!(found = result.is_some(), "loaded snapshot at or before version");
        Ok(result)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id, keep_count))]
    async fn cleanup_old_snapshots(
        &self,
        grain_id: &GrainId,
        keep_count: usize,
    ) -> EventSourcingResult<()> {
        if let Some(entry) = self.snapshots.get(grain_id) {
            let mut snapshots = entry.write();
            let total = snapshots.len();
            if total > keep_count {
                let to_remove: Vec<_> = snapshots
                    .keys()
                    .take(total - keep_count)
                    .copied()
                    .collect();
                for version in to_remove {
                    snapshots.remove(&version);
                }
                debug!(removed = total - keep_count, "cleaned up old snapshots");
            }
        }
        Ok(())
    }
}

/// Combined in-memory event and snapshot storage.
#[derive(Debug)]
pub struct InMemoryLogStorage<S, E> {
    /// Event storage.
    pub events: InMemoryEventStorage<E>,
    /// Snapshot storage.
    pub snapshots: InMemorySnapshotStorage<S>,
}

impl<S, E> Default for InMemoryLogStorage<S, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S, E> InMemoryLogStorage<S, E> {
    /// Creates a new combined in-memory storage.
    pub fn new() -> Self {
        Self {
            events: InMemoryEventStorage::new(),
            snapshots: InMemorySnapshotStorage::new(),
        }
    }

    /// Creates from separate event and snapshot storage.
    pub fn from_parts(
        events: InMemoryEventStorage<E>,
        snapshots: InMemorySnapshotStorage<S>,
    ) -> Self {
        Self { events, snapshots }
    }
}

#[async_trait]
impl<E> IEventStorage<E> for InMemoryLogStorage<(), E>
where
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn append_events(
        &self,
        grain_id: &GrainId,
        events: Vec<EventEntry<E>>,
        expected_version: u64,
    ) -> EventSourcingResult<u64> {
        self.events.append_events(grain_id, events, expected_version).await
    }

    async fn read_events(
        &self,
        grain_id: &GrainId,
        from_sequence: u64,
        max_count: Option<usize>,
    ) -> EventSourcingResult<Vec<EventEntry<E>>> {
        self.events.read_events(grain_id, from_sequence, max_count).await
    }

    async fn read_all_events(&self, grain_id: &GrainId) -> EventSourcingResult<Vec<EventEntry<E>>> {
        self.events.read_all_events(grain_id).await
    }

    async fn get_version(&self, grain_id: &GrainId) -> EventSourcingResult<u64> {
        self.events.get_version(grain_id).await
    }

    async fn clear(&self, grain_id: &GrainId) -> EventSourcingResult<()> {
        self.events.clear(grain_id).await
    }
}

#[async_trait]
impl<S> ISnapshotStorage<S> for InMemoryLogStorage<S, ()>
where
    S: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
{
    async fn save_snapshot(
        &self,
        grain_id: &GrainId,
        state: &S,
        version: u64,
    ) -> EventSourcingResult<()> {
        self.snapshots.save_snapshot(grain_id, state, version).await
    }

    async fn load_snapshot(&self, grain_id: &GrainId) -> EventSourcingResult<Option<(S, u64)>> {
        self.snapshots.load_snapshot(grain_id).await
    }

    async fn load_snapshot_at_or_before(
        &self,
        grain_id: &GrainId,
        version: u64,
    ) -> EventSourcingResult<Option<(S, u64)>> {
        self.snapshots.load_snapshot_at_or_before(grain_id, version).await
    }

    async fn cleanup_old_snapshots(
        &self,
        grain_id: &GrainId,
        keep_count: usize,
    ) -> EventSourcingResult<()> {
        self.snapshots.cleanup_old_snapshots(grain_id, keep_count).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainType, IdSpan};

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    enum TestEvent {
        Created { name: String },
        Updated { value: i32 },
        Deleted,
    }

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TestState {
        name: String,
        value: i32,
        deleted: bool,
    }

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(key))
    }

    #[tokio::test]
    async fn test_append_and_read_events() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-1");

        // Append events
        let events = vec![
            EventEntry::new(1, TestEvent::Created { name: "Test".into() }),
            EventEntry::new(2, TestEvent::Updated { value: 42 }),
        ];

        let version = storage.append_events(&grain_id, events, 0).await.unwrap();
        assert_eq!(version, 2);

        // Read all events
        let read_events = storage.read_all_events(&grain_id).await.unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].sequence(), 1);
        assert_eq!(read_events[1].sequence(), 2);
    }

    #[tokio::test]
    async fn test_version_conflict() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-2");

        // First append succeeds
        let events = vec![EventEntry::new(1, TestEvent::Created { name: "Test".into() })];
        storage.append_events(&grain_id, events, 0).await.unwrap();

        // Second append with wrong expected version fails
        let events = vec![EventEntry::new(2, TestEvent::Updated { value: 1 })];
        let result = storage.append_events(&grain_id, events, 0).await;
        assert!(matches!(
            result,
            Err(EventSourcingError::VersionConflict { expected: 0, current: 1 })
        ));
    }

    #[tokio::test]
    async fn test_sequence_out_of_order() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-3");

        // Try to append event with wrong sequence
        let events = vec![EventEntry::new(5, TestEvent::Created { name: "Test".into() })];
        let result = storage.append_events(&grain_id, events, 0).await;
        assert!(matches!(
            result,
            Err(EventSourcingError::SequenceOutOfOrder { expected: 1, actual: 5 })
        ));
    }

    #[tokio::test]
    async fn test_read_events_from_sequence() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-4");

        // Append 5 events
        for i in 1..=5 {
            let events = vec![EventEntry::new(i, TestEvent::Updated { value: i as i32 })];
            storage.append_events(&grain_id, events, i - 1).await.unwrap();
        }

        // Read from sequence 3
        let events = storage.read_events(&grain_id, 3, None).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence(), 3);
        assert_eq!(events[2].sequence(), 5);

        // Read with max count
        let events = storage.read_events(&grain_id, 1, Some(2)).await.unwrap();
        assert_eq!(events.len(), 2);
    }

    #[tokio::test]
    async fn test_get_version() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-5");

        // Initially version is 0
        assert_eq!(storage.get_version(&grain_id).await.unwrap(), 0);

        // After appending events
        let events = vec![
            EventEntry::new(1, TestEvent::Created { name: "Test".into() }),
            EventEntry::new(2, TestEvent::Updated { value: 1 }),
        ];
        storage.append_events(&grain_id, events, 0).await.unwrap();

        assert_eq!(storage.get_version(&grain_id).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_clear_events() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        let grain_id = make_grain_id("test-6");

        // Append some events
        let events = vec![EventEntry::new(1, TestEvent::Created { name: "Test".into() })];
        storage.append_events(&grain_id, events, 0).await.unwrap();
        assert_eq!(storage.get_version(&grain_id).await.unwrap(), 1);

        // Clear
        storage.clear(&grain_id).await.unwrap();
        assert_eq!(storage.get_version(&grain_id).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_snapshot_save_and_load() {
        let storage: InMemorySnapshotStorage<TestState> = InMemorySnapshotStorage::new();
        let grain_id = make_grain_id("test-7");

        // Save snapshot
        let state = TestState {
            name: "Test".into(),
            value: 42,
            deleted: false,
        };
        storage.save_snapshot(&grain_id, &state, 10).await.unwrap();

        // Load latest snapshot
        let (loaded_state, version) = storage.load_snapshot(&grain_id).await.unwrap().unwrap();
        assert_eq!(version, 10);
        assert_eq!(loaded_state, state);
    }

    #[tokio::test]
    async fn test_snapshot_at_or_before() {
        let storage: InMemorySnapshotStorage<TestState> = InMemorySnapshotStorage::new();
        let grain_id = make_grain_id("test-8");

        // Save multiple snapshots
        for i in [10, 20, 30] {
            let state = TestState {
                name: format!("v{}", i),
                value: i,
                deleted: false,
            };
            storage.save_snapshot(&grain_id, &state, i as u64).await.unwrap();
        }

        // Load at or before version 25 should get version 20
        let (state, version) = storage
            .load_snapshot_at_or_before(&grain_id, 25)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version, 20);
        assert_eq!(state.value, 20);

        // Load at or before version 30 should get version 30
        let (state, version) = storage
            .load_snapshot_at_or_before(&grain_id, 30)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(version, 30);
        assert_eq!(state.value, 30);
    }

    #[tokio::test]
    async fn test_cleanup_old_snapshots() {
        let storage: InMemorySnapshotStorage<TestState> = InMemorySnapshotStorage::new();
        let grain_id = make_grain_id("test-9");

        // Save 5 snapshots
        for i in 1..=5 {
            let state = TestState {
                name: format!("v{}", i),
                value: i,
                deleted: false,
            };
            storage.save_snapshot(&grain_id, &state, i as u64).await.unwrap();
        }

        // Keep only 2 snapshots
        storage.cleanup_old_snapshots(&grain_id, 2).await.unwrap();

        // Verify only versions 4 and 5 remain
        let (_, version) = storage.load_snapshot(&grain_id).await.unwrap().unwrap();
        assert_eq!(version, 5);

        let result = storage.load_snapshot_at_or_before(&grain_id, 3).await.unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_storage_counts() {
        let storage: InMemoryEventStorage<TestEvent> = InMemoryEventStorage::new();
        assert_eq!(storage.grain_count(), 0);
        assert_eq!(storage.total_event_count(), 0);
    }
}
