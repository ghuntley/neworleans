//! Journaled grain base implementation for event-sourced grains.

use crate::adaptor::{LogViewAdaptor, LogViewAdaptorFactory, LogViewAdaptorOptions};
use crate::error::EventSourcingResult;
use crate::event_entry::{EventEntry, EventMetadata};
use crate::traits::{EventApplier, IEventStorage, ILogConsistentGrain, ILogViewAdaptor, ISnapshotStorage};
use orleans_core::GrainId;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{debug, instrument};

/// Base struct for event-sourced grains.
///
/// This provides the core event sourcing functionality:
/// - State management through events
/// - Event raising and confirmation
/// - Snapshot support for fast recovery
/// - Temporal queries
///
/// # Type Parameters
///
/// - `S`: The state type (must be Clone + Default + Serialize + DeserializeOwned)
/// - `E`: The event type (must be Clone + Serialize + DeserializeOwned)
/// - `A`: The event applier (implements EventApplier<S, E>)
///
/// # Example
///
/// ```ignore
/// use orleans_event_sourcing::{JournaledGrain, EventApplier};
///
/// #[derive(Clone, Default, Serialize, Deserialize)]
/// struct BankAccountState {
///     balance: i64,
/// }
///
/// #[derive(Clone, Serialize, Deserialize)]
/// enum BankAccountEvent {
///     Deposited(i64),
///     Withdrawn(i64),
/// }
///
/// struct BankAccountApplier;
///
/// impl EventApplier<BankAccountState, BankAccountEvent> for BankAccountApplier {
///     fn apply(state: &mut BankAccountState, event: &BankAccountEvent) {
///         match event {
///             BankAccountEvent::Deposited(amount) => state.balance += amount,
///             BankAccountEvent::Withdrawn(amount) => state.balance -= amount,
///         }
///     }
/// }
///
/// struct BankAccountGrain {
///     inner: JournaledGrain<BankAccountState, BankAccountEvent, BankAccountApplier>,
/// }
///
/// impl BankAccountGrain {
///     async fn deposit(&mut self, amount: i64) -> EventSourcingResult<i64> {
///         self.inner.raise_event(BankAccountEvent::Deposited(amount));
///         self.inner.confirm_events().await?;
///         Ok(self.inner.state().balance)
///     }
/// }
/// ```
pub struct JournaledGrain<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    adaptor: LogViewAdaptor<S, E, A>,
}

impl<S, E, A> std::fmt::Debug for JournaledGrain<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + std::fmt::Debug + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + std::fmt::Debug + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournaledGrain")
            .field("adaptor", &self.adaptor)
            .finish()
    }
}

impl<S, E, A> JournaledGrain<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    /// Creates a new journaled grain with event storage only.
    pub fn new(grain_id: GrainId, event_storage: Arc<dyn IEventStorage<E>>) -> Self {
        Self {
            adaptor: LogViewAdaptor::new(
                grain_id,
                event_storage,
                None,
                LogViewAdaptorOptions::default(),
            ),
        }
    }

    /// Creates a new journaled grain with event and snapshot storage.
    pub fn with_snapshots(
        grain_id: GrainId,
        event_storage: Arc<dyn IEventStorage<E>>,
        snapshot_storage: Arc<dyn ISnapshotStorage<S>>,
    ) -> Self {
        Self {
            adaptor: LogViewAdaptor::new(
                grain_id,
                event_storage,
                Some(snapshot_storage),
                LogViewAdaptorOptions::default(),
            ),
        }
    }

    /// Creates a new journaled grain with custom options.
    pub fn with_options(
        grain_id: GrainId,
        event_storage: Arc<dyn IEventStorage<E>>,
        snapshot_storage: Option<Arc<dyn ISnapshotStorage<S>>>,
        options: LogViewAdaptorOptions,
    ) -> Self {
        Self {
            adaptor: LogViewAdaptor::new(grain_id, event_storage, snapshot_storage, options),
        }
    }

    /// Creates a journaled grain from an adaptor factory.
    pub fn from_factory(
        grain_id: GrainId,
        factory: &LogViewAdaptorFactory<S, E, A>,
    ) -> Self {
        Self {
            adaptor: factory.create(grain_id),
        }
    }

    /// Activates the grain by loading state from storage.
    #[instrument(skip(self))]
    pub async fn on_activate(&mut self) -> EventSourcingResult<()> {
        debug!("activating journaled grain");
        self.adaptor.activate().await
    }

    /// Deactivates the grain.
    #[instrument(skip(self))]
    pub async fn on_deactivate(&mut self) -> EventSourcingResult<()> {
        debug!("deactivating journaled grain");
        self.adaptor.deactivate().await
    }

    /// Returns the confirmed version (number of persisted events).
    pub fn confirmed_version(&self) -> u64 {
        self.adaptor.confirmed_version()
    }

    /// Returns the tentative version (confirmed + pending events).
    pub fn tentative_version(&self) -> u64 {
        self.adaptor.tentative_version()
    }

    /// Returns a reference to the confirmed state.
    pub fn confirmed_state(&self) -> &S {
        self.adaptor.confirmed_state()
    }

    /// Returns a reference to the tentative state (including pending events).
    pub fn state(&self) -> &S {
        self.adaptor.tentative_state()
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        self.adaptor.grain_id()
    }

    /// Raises a new event that will be applied to the tentative state.
    ///
    /// The event is not persisted until `confirm_events()` is called.
    pub fn raise_event(&mut self, event: E) {
        self.adaptor.raise_event(event);
    }

    /// Raises a new event with metadata.
    pub fn raise_event_with_metadata(&mut self, event: E, metadata: EventMetadata) {
        self.adaptor.raise_event_with_metadata(event, metadata);
    }

    /// Confirms all pending events (writes to storage).
    pub async fn confirm_events(&mut self) -> EventSourcingResult<()> {
        self.adaptor.confirm_events().await
    }

    /// Aborts all pending events (discards uncommitted changes).
    pub fn abort_pending_events(&mut self) {
        self.adaptor.abort_pending_events();
    }

    /// Refreshes state from storage.
    pub async fn refresh(&mut self) -> EventSourcingResult<()> {
        self.adaptor.refresh().await
    }

    /// Manually takes a snapshot.
    pub async fn take_snapshot(&mut self) -> EventSourcingResult<()> {
        self.adaptor.take_snapshot().await
    }

    /// Gets the state at a specific version (for temporal queries).
    pub async fn get_state_at_version(&self, version: u64) -> EventSourcingResult<S> {
        self.adaptor.get_state_at_version(version).await
    }

    /// Gets events in a version range.
    pub async fn get_events_in_range(
        &self,
        from_version: u64,
        to_version: u64,
    ) -> EventSourcingResult<Vec<EventEntry<E>>> {
        self.adaptor.get_events_in_range(from_version, to_version).await
    }

    /// Gets all events since a given version.
    pub async fn get_events_since(&self, since_version: u64) -> EventSourcingResult<Vec<EventEntry<E>>> {
        self.adaptor
            .get_events_in_range(since_version + 1, self.confirmed_version())
            .await
    }
}

impl<S, E, A> ILogConsistentGrain for JournaledGrain<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + std::fmt::Debug + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + std::fmt::Debug + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
}

/// Builder for creating journaled grains.
pub struct JournaledGrainBuilder<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    grain_id: GrainId,
    event_storage: Arc<dyn IEventStorage<E>>,
    snapshot_storage: Option<Arc<dyn ISnapshotStorage<S>>>,
    options: LogViewAdaptorOptions,
    _marker: PhantomData<A>,
}

impl<S, E, A> JournaledGrainBuilder<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    /// Creates a new builder with required parameters.
    pub fn new(grain_id: GrainId, event_storage: Arc<dyn IEventStorage<E>>) -> Self {
        Self {
            grain_id,
            event_storage,
            snapshot_storage: None,
            options: LogViewAdaptorOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Adds snapshot storage.
    pub fn with_snapshot_storage(mut self, storage: Arc<dyn ISnapshotStorage<S>>) -> Self {
        self.snapshot_storage = Some(storage);
        self
    }

    /// Sets the snapshot interval.
    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.options.snapshot_interval = interval;
        self
    }

    /// Sets the maximum number of snapshots to keep.
    pub fn with_max_snapshots(mut self, count: usize) -> Self {
        self.options.max_snapshots = count;
        self
    }

    /// Disables automatic snapshots.
    pub fn without_auto_snapshot(mut self) -> Self {
        self.options.auto_snapshot = false;
        self
    }

    /// Sets custom options.
    pub fn with_options(mut self, options: LogViewAdaptorOptions) -> Self {
        self.options = options;
        self
    }

    /// Builds the journaled grain.
    pub fn build(self) -> JournaledGrain<S, E, A> {
        JournaledGrain::with_options(
            self.grain_id,
            self.event_storage,
            self.snapshot_storage,
            self.options,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{InMemoryEventStorage, InMemorySnapshotStorage};
    use orleans_core::{GrainType, IdSpan};

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
    struct TodoState {
        items: Vec<String>,
        completed: Vec<bool>,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum TodoEvent {
        ItemAdded(String),
        ItemCompleted(usize),
        ItemRemoved(usize),
        AllCleared,
    }

    struct TodoApplier;

    impl EventApplier<TodoState, TodoEvent> for TodoApplier {
        fn apply(state: &mut TodoState, event: &TodoEvent) {
            match event {
                TodoEvent::ItemAdded(item) => {
                    state.items.push(item.clone());
                    state.completed.push(false);
                }
                TodoEvent::ItemCompleted(idx) => {
                    if *idx < state.completed.len() {
                        state.completed[*idx] = true;
                    }
                }
                TodoEvent::ItemRemoved(idx) => {
                    if *idx < state.items.len() {
                        state.items.remove(*idx);
                        state.completed.remove(*idx);
                    }
                }
                TodoEvent::AllCleared => {
                    state.items.clear();
                    state.completed.clear();
                }
            }
        }
    }

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("TodoGrain"), IdSpan::from_str(key))
    }

    #[tokio::test]
    async fn test_journaled_grain_basic() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let grain_id = make_grain_id("test-1");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrain::new(grain_id, storage);

        grain.on_activate().await.unwrap();

        // Add items
        grain.raise_event(TodoEvent::ItemAdded("Buy milk".into()));
        grain.raise_event(TodoEvent::ItemAdded("Walk dog".into()));

        // State should be updated tentatively
        assert_eq!(grain.state().items.len(), 2);
        assert_eq!(grain.confirmed_version(), 0);
        assert_eq!(grain.tentative_version(), 2);

        // Confirm
        grain.confirm_events().await.unwrap();
        assert_eq!(grain.confirmed_version(), 2);
    }

    #[tokio::test]
    async fn test_journaled_grain_persistence() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<TodoState>::new());
        let grain_id = make_grain_id("test-2");

        // First activation
        {
            let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
                JournaledGrain::with_snapshots(
                    grain_id.clone(),
                    storage.clone(),
                    snapshot_storage.clone(),
                );

            grain.on_activate().await.unwrap();
            grain.raise_event(TodoEvent::ItemAdded("Task 1".into()));
            grain.raise_event(TodoEvent::ItemAdded("Task 2".into()));
            grain.confirm_events().await.unwrap();
            grain.on_deactivate().await.unwrap();
        }

        // Second activation - state should be preserved
        {
            let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
                JournaledGrain::with_snapshots(grain_id, storage, snapshot_storage);

            grain.on_activate().await.unwrap();
            assert_eq!(grain.state().items.len(), 2);
            assert_eq!(grain.state().items[0], "Task 1");
            assert_eq!(grain.confirmed_version(), 2);
        }
    }

    #[tokio::test]
    async fn test_journaled_grain_abort() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let grain_id = make_grain_id("test-3");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrain::new(grain_id, storage);

        grain.on_activate().await.unwrap();

        // Add and confirm one item
        grain.raise_event(TodoEvent::ItemAdded("Keep this".into()));
        grain.confirm_events().await.unwrap();

        // Add another item but abort
        grain.raise_event(TodoEvent::ItemAdded("Discard this".into()));
        assert_eq!(grain.state().items.len(), 2);

        grain.abort_pending_events();
        assert_eq!(grain.state().items.len(), 1);
        assert_eq!(grain.state().items[0], "Keep this");
    }

    #[tokio::test]
    async fn test_journaled_grain_temporal_query() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let grain_id = make_grain_id("test-4");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrain::new(grain_id, storage);

        grain.on_activate().await.unwrap();

        // Create history
        for i in 1..=5 {
            grain.raise_event(TodoEvent::ItemAdded(format!("Task {}", i)));
            grain.confirm_events().await.unwrap();
        }

        // Query historical state
        let state_v2 = grain.get_state_at_version(2).await.unwrap();
        assert_eq!(state_v2.items.len(), 2);
        assert_eq!(state_v2.items[1], "Task 2");

        let state_v4 = grain.get_state_at_version(4).await.unwrap();
        assert_eq!(state_v4.items.len(), 4);
    }

    #[tokio::test]
    async fn test_journaled_grain_builder() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<TodoState>::new());
        let grain_id = make_grain_id("test-5");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrainBuilder::new(grain_id, storage)
                .with_snapshot_storage(snapshot_storage)
                .with_snapshot_interval(10)
                .with_max_snapshots(5)
                .build();

        grain.on_activate().await.unwrap();
        grain.raise_event(TodoEvent::ItemAdded("Test".into()));
        grain.confirm_events().await.unwrap();

        assert_eq!(grain.state().items.len(), 1);
    }

    #[tokio::test]
    async fn test_journaled_grain_events_since() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let grain_id = make_grain_id("test-6");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrain::new(grain_id, storage);

        grain.on_activate().await.unwrap();

        // Create events
        for i in 1..=5 {
            grain.raise_event(TodoEvent::ItemAdded(format!("Task {}", i)));
            grain.confirm_events().await.unwrap();
        }

        // Get events since version 2
        let events = grain.get_events_since(2).await.unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].sequence(), 3);
        assert_eq!(events[2].sequence(), 5);
    }

    #[tokio::test]
    async fn test_journaled_grain_with_metadata() {
        let storage = Arc::new(InMemoryEventStorage::<TodoEvent>::new());
        let grain_id = make_grain_id("test-7");

        let mut grain: JournaledGrain<TodoState, TodoEvent, TodoApplier> =
            JournaledGrain::new(grain_id, storage);

        grain.on_activate().await.unwrap();

        let metadata = EventMetadata::new()
            .with_user_id("user-123")
            .with_correlation_id("corr-456");

        grain.raise_event_with_metadata(TodoEvent::ItemAdded("With metadata".into()), metadata);
        grain.confirm_events().await.unwrap();

        assert_eq!(grain.state().items.len(), 1);
    }
}
