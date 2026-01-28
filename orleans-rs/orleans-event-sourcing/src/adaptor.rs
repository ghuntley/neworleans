//! Log view adaptor implementation.

use crate::error::EventSourcingResult;
use crate::event_entry::{EventEntry, EventMetadata, LogViewState};
use crate::traits::{EventApplier, IEventStorage, ILogViewAdaptor, ISnapshotStorage};
use async_trait::async_trait;
use orleans_core::GrainId;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tracing::{debug, instrument, trace, warn};

/// Configuration options for the log view adaptor.
#[derive(Debug, Clone)]
pub struct LogViewAdaptorOptions {
    /// Number of events between snapshots.
    pub snapshot_interval: u64,

    /// Maximum number of snapshots to keep.
    pub max_snapshots: usize,

    /// Maximum number of events to read at once.
    pub max_events_per_read: usize,

    /// Whether to automatically take snapshots.
    pub auto_snapshot: bool,
}

impl Default for LogViewAdaptorOptions {
    fn default() -> Self {
        Self {
            snapshot_interval: 100,
            max_snapshots: 10,
            max_events_per_read: 1000,
            auto_snapshot: true,
        }
    }
}

impl LogViewAdaptorOptions {
    /// Creates options for testing with more frequent snapshots.
    pub fn for_testing() -> Self {
        Self {
            snapshot_interval: 10,
            max_snapshots: 5,
            max_events_per_read: 100,
            auto_snapshot: true,
        }
    }

    /// Builder method to set snapshot interval.
    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Builder method to set max snapshots.
    pub fn with_max_snapshots(mut self, count: usize) -> Self {
        self.max_snapshots = count;
        self
    }

    /// Builder method to disable auto snapshots.
    pub fn without_auto_snapshot(mut self) -> Self {
        self.auto_snapshot = false;
        self
    }
}

/// Factory for creating log view adaptors.
pub struct LogViewAdaptorFactory<S, E, A>
where
    S: Clone + Default + Send + Sync,
    E: Clone + Send + Sync,
    A: EventApplier<S, E> + Send + Sync,
{
    event_storage: Arc<dyn IEventStorage<E>>,
    snapshot_storage: Option<Arc<dyn ISnapshotStorage<S>>>,
    options: LogViewAdaptorOptions,
    _marker: PhantomData<(S, E, A)>,
}

impl<S, E, A> std::fmt::Debug for LogViewAdaptorFactory<S, E, A>
where
    S: Clone + Default + Send + Sync,
    E: Clone + Send + Sync,
    A: EventApplier<S, E> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogViewAdaptorFactory")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<S, E, A> LogViewAdaptorFactory<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    /// Creates a new factory with only event storage.
    pub fn new(event_storage: Arc<dyn IEventStorage<E>>) -> Self {
        Self {
            event_storage,
            snapshot_storage: None,
            options: LogViewAdaptorOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Creates a new factory with both event and snapshot storage.
    pub fn with_snapshots(
        event_storage: Arc<dyn IEventStorage<E>>,
        snapshot_storage: Arc<dyn ISnapshotStorage<S>>,
    ) -> Self {
        Self {
            event_storage,
            snapshot_storage: Some(snapshot_storage),
            options: LogViewAdaptorOptions::default(),
            _marker: PhantomData,
        }
    }

    /// Sets the options for created adaptors.
    pub fn with_options(mut self, options: LogViewAdaptorOptions) -> Self {
        self.options = options;
        self
    }

    /// Creates a new adaptor for the given grain.
    pub fn create(&self, grain_id: GrainId) -> LogViewAdaptor<S, E, A> {
        LogViewAdaptor::new(
            grain_id,
            self.event_storage.clone(),
            self.snapshot_storage.clone(),
            self.options.clone(),
        )
    }
}

/// The log view adaptor coordinates event storage and state reconstruction.
pub struct LogViewAdaptor<S, E, A>
where
    S: Clone + Default + Send + Sync,
    E: Clone + Send + Sync,
    A: EventApplier<S, E> + Send + Sync,
{
    grain_id: GrainId,
    event_storage: Arc<dyn IEventStorage<E>>,
    snapshot_storage: Option<Arc<dyn ISnapshotStorage<S>>>,
    options: LogViewAdaptorOptions,

    /// The confirmed state (from storage).
    confirmed_state: S,
    /// The tentative state (including pending events).
    tentative_state: S,
    /// The log view state tracking versions and pending events.
    log_state: LogViewState<S, E>,

    _applier: PhantomData<A>,
}

impl<S, E, A> std::fmt::Debug for LogViewAdaptor<S, E, A>
where
    S: Clone + Default + Send + Sync + std::fmt::Debug,
    E: Clone + Send + Sync + std::fmt::Debug,
    A: EventApplier<S, E> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogViewAdaptor")
            .field("grain_id", &self.grain_id)
            .field("confirmed_version", &self.log_state.confirmed_version())
            .field("pending_count", &self.log_state.pending_event_count())
            .finish()
    }
}

impl<S, E, A> LogViewAdaptor<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    /// Creates a new log view adaptor.
    pub fn new(
        grain_id: GrainId,
        event_storage: Arc<dyn IEventStorage<E>>,
        snapshot_storage: Option<Arc<dyn ISnapshotStorage<S>>>,
        options: LogViewAdaptorOptions,
    ) -> Self {
        Self {
            grain_id,
            event_storage,
            snapshot_storage,
            options,
            confirmed_state: S::default(),
            tentative_state: S::default(),
            log_state: LogViewState::new(),
            _applier: PhantomData,
        }
    }

    /// Activates the adaptor by loading state from storage.
    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    pub async fn activate(&mut self) -> EventSourcingResult<()> {
        debug!("activating log view adaptor");

        // Try to load from snapshot first
        let (mut state, from_version) = if let Some(ref snapshot_storage) = self.snapshot_storage {
            if let Some((snapshot, version)) = snapshot_storage.load_snapshot(&self.grain_id).await?
            {
                debug!(version, "loaded state from snapshot");
                (snapshot, version)
            } else {
                debug!("no snapshot found, starting from default state");
                (S::default(), 0)
            }
        } else {
            (S::default(), 0)
        };

        // Replay events from the snapshot version
        let events = self
            .event_storage
            .read_events(&self.grain_id, from_version + 1, Some(self.options.max_events_per_read))
            .await?;

        debug!(
            event_count = events.len(),
            from_version, "replaying events from snapshot"
        );

        for event in &events {
            A::apply(&mut state, event.event());
        }

        let confirmed_version = if events.is_empty() {
            from_version
        } else {
            events.last().unwrap().sequence()
        };

        self.confirmed_state = state.clone();
        self.tentative_state = state.clone();
        self.log_state = LogViewState::from_snapshot(state, confirmed_version);
        self.log_state.set_last_snapshot_version(from_version);

        debug!(
            confirmed_version = self.log_state.confirmed_version(),
            "activation complete"
        );
        Ok(())
    }

    /// Deactivates the adaptor.
    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    pub async fn deactivate(&mut self) -> EventSourcingResult<()> {
        debug!("deactivating log view adaptor");

        // Confirm any pending events
        if self.log_state.has_pending_events() {
            warn!(
                pending_count = self.log_state.pending_event_count(),
                "deactivating with pending events - confirming"
            );
            self.confirm_events().await?;
        }

        Ok(())
    }

    /// Raises a new event with metadata.
    pub fn raise_event_with_metadata(&mut self, event: E, metadata: EventMetadata) {
        // Apply to tentative state
        A::apply(&mut self.tentative_state, &event);

        // Track in log state
        self.log_state.add_pending_event_with_metadata(event, metadata);

        trace!(
            pending_count = self.log_state.pending_event_count(),
            "raised event with metadata"
        );
    }

    /// Gets events in a version range.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, from_version, to_version))]
    pub async fn get_events_in_range(
        &self,
        from_version: u64,
        to_version: u64,
    ) -> EventSourcingResult<Vec<EventEntry<E>>> {
        let count = (to_version - from_version + 1) as usize;
        self.event_storage
            .read_events(&self.grain_id, from_version, Some(count))
            .await
    }

    /// Gets the state at a specific version (for temporal queries).
    #[instrument(skip(self), fields(grain_id = %self.grain_id, version))]
    pub async fn get_state_at_version(&self, version: u64) -> EventSourcingResult<S> {
        // Start from snapshot if possible
        let (mut state, from_version) = if let Some(ref snapshot_storage) = self.snapshot_storage {
            if let Some((snapshot, snap_version)) = snapshot_storage
                .load_snapshot_at_or_before(&self.grain_id, version)
                .await?
            {
                (snapshot, snap_version)
            } else {
                (S::default(), 0)
            }
        } else {
            (S::default(), 0)
        };

        // Replay events up to the target version
        if from_version < version {
            let events = self
                .event_storage
                .read_events(
                    &self.grain_id,
                    from_version + 1,
                    Some((version - from_version) as usize),
                )
                .await?;

            for event in &events {
                if event.sequence() <= version {
                    A::apply(&mut state, event.event());
                }
            }
        }

        Ok(state)
    }

    /// Manually takes a snapshot.
    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    pub async fn take_snapshot(&mut self) -> EventSourcingResult<()> {
        if let Some(ref snapshot_storage) = self.snapshot_storage {
            let version = self.log_state.confirmed_version();
            snapshot_storage
                .save_snapshot(&self.grain_id, &self.confirmed_state, version)
                .await?;

            self.log_state.set_last_snapshot_version(version);
            debug!(version, "took snapshot");

            // Cleanup old snapshots
            snapshot_storage
                .cleanup_old_snapshots(&self.grain_id, self.options.max_snapshots)
                .await?;
        }
        Ok(())
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }
}

#[async_trait]
impl<S, E, A> ILogViewAdaptor<S, E> for LogViewAdaptor<S, E, A>
where
    S: Clone + Default + Send + Sync + Serialize + DeserializeOwned + 'static,
    E: Clone + Send + Sync + Serialize + DeserializeOwned + 'static,
    A: EventApplier<S, E> + Send + Sync,
{
    fn confirmed_version(&self) -> u64 {
        self.log_state.confirmed_version()
    }

    fn tentative_version(&self) -> u64 {
        self.log_state.pending_version()
    }

    fn confirmed_state(&self) -> &S {
        &self.confirmed_state
    }

    fn tentative_state(&self) -> &S {
        &self.tentative_state
    }

    fn raise_event(&mut self, event: E) {
        // Apply to tentative state
        A::apply(&mut self.tentative_state, &event);

        // Track in log state
        self.log_state.add_pending_event(event);

        trace!(
            pending_count = self.log_state.pending_event_count(),
            "raised event"
        );
    }

    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    async fn confirm_events(&mut self) -> EventSourcingResult<()> {
        if !self.log_state.has_pending_events() {
            debug!("no pending events to confirm");
            return Ok(());
        }

        let pending = self.log_state.take_pending_events();
        let event_count = pending.len();
        let expected_version = self.log_state.confirmed_version();

        debug!(
            event_count,
            expected_version, "confirming pending events"
        );

        // Append to storage
        let new_version = self
            .event_storage
            .append_events(&self.grain_id, pending, expected_version)
            .await?;

        // Update confirmed state
        self.log_state.set_confirmed_version(new_version);
        self.confirmed_state = self.tentative_state.clone();

        debug!(new_version, "events confirmed");

        // Auto-snapshot if needed
        if self.options.auto_snapshot
            && self.log_state.needs_snapshot(self.options.snapshot_interval)
        {
            debug!("taking automatic snapshot");
            self.take_snapshot().await?;
        }

        Ok(())
    }

    fn abort_pending_events(&mut self) {
        let count = self.log_state.pending_event_count();
        self.log_state.clear_pending_events();
        self.tentative_state = self.confirmed_state.clone();
        debug!(aborted_count = count, "aborted pending events");
    }

    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    async fn refresh(&mut self) -> EventSourcingResult<()> {
        debug!("refreshing state from storage");

        // Discard pending events
        self.abort_pending_events();

        // Re-read from storage
        let current_version = self.log_state.confirmed_version();
        let events = self
            .event_storage
            .read_events(
                &self.grain_id,
                current_version + 1,
                Some(self.options.max_events_per_read),
            )
            .await?;

        if !events.is_empty() {
            debug!(event_count = events.len(), "applying new events from storage");
            for event in &events {
                A::apply(&mut self.confirmed_state, event.event());
            }
            let new_version = events.last().unwrap().sequence();
            self.log_state.set_confirmed_version(new_version);
            self.tentative_state = self.confirmed_state.clone();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryEventStorage;
    use crate::storage::InMemorySnapshotStorage;
    use orleans_core::{GrainType, IdSpan};

    #[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, PartialEq)]
    struct CounterState {
        value: i32,
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    enum CounterEvent {
        Incremented(i32),
        Decremented(i32),
        Reset,
    }

    struct CounterApplier;

    impl EventApplier<CounterState, CounterEvent> for CounterApplier {
        fn apply(state: &mut CounterState, event: &CounterEvent) {
            match event {
                CounterEvent::Incremented(n) => state.value += n,
                CounterEvent::Decremented(n) => state.value -= n,
                CounterEvent::Reset => state.value = 0,
            }
        }
    }

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("CounterGrain"), IdSpan::from_str(key))
    }

    #[tokio::test]
    async fn test_adaptor_activate_empty() {
        let storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let grain_id = make_grain_id("test-1");

        let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptor::new(grain_id, storage, None, LogViewAdaptorOptions::for_testing());

        adaptor.activate().await.unwrap();

        assert_eq!(adaptor.confirmed_version(), 0);
        assert_eq!(adaptor.tentative_version(), 0);
        assert_eq!(adaptor.confirmed_state().value, 0);
    }

    #[tokio::test]
    async fn test_adaptor_raise_and_confirm() {
        let storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let grain_id = make_grain_id("test-2");

        let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptor::new(grain_id, storage, None, LogViewAdaptorOptions::for_testing());

        adaptor.activate().await.unwrap();

        // Raise events
        adaptor.raise_event(CounterEvent::Incremented(5));
        adaptor.raise_event(CounterEvent::Incremented(3));

        // Tentative state should be updated
        assert_eq!(adaptor.tentative_state().value, 8);
        assert_eq!(adaptor.tentative_version(), 2);

        // Confirmed state should not be updated yet
        assert_eq!(adaptor.confirmed_state().value, 0);
        assert_eq!(adaptor.confirmed_version(), 0);

        // Confirm events
        adaptor.confirm_events().await.unwrap();

        // Now both should be updated
        assert_eq!(adaptor.confirmed_state().value, 8);
        assert_eq!(adaptor.confirmed_version(), 2);
    }

    #[tokio::test]
    async fn test_adaptor_abort_pending() {
        let storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let grain_id = make_grain_id("test-3");

        let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptor::new(grain_id, storage, None, LogViewAdaptorOptions::for_testing());

        adaptor.activate().await.unwrap();

        // Raise and confirm some events
        adaptor.raise_event(CounterEvent::Incremented(10));
        adaptor.confirm_events().await.unwrap();

        // Raise more events
        adaptor.raise_event(CounterEvent::Incremented(5));
        assert_eq!(adaptor.tentative_state().value, 15);

        // Abort
        adaptor.abort_pending_events();

        // Tentative should revert to confirmed
        assert_eq!(adaptor.tentative_state().value, 10);
        assert_eq!(adaptor.tentative_version(), 1);
    }

    #[tokio::test]
    async fn test_adaptor_with_snapshot() {
        let event_storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<CounterState>::new());
        let grain_id = make_grain_id("test-4");

        let options = LogViewAdaptorOptions::for_testing().with_snapshot_interval(5);

        let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptor::new(
                grain_id.clone(),
                event_storage.clone(),
                Some(snapshot_storage.clone()),
                options,
            );

        adaptor.activate().await.unwrap();

        // Raise and confirm more than snapshot_interval events
        for _i in 1..=10 {
            adaptor.raise_event(CounterEvent::Incremented(1));
            adaptor.confirm_events().await.unwrap();
        }

        assert_eq!(adaptor.confirmed_state().value, 10);

        // Verify snapshot was taken
        let snapshot = snapshot_storage.load_snapshot(&grain_id).await.unwrap();
        assert!(snapshot.is_some());
    }

    #[tokio::test]
    async fn test_adaptor_reload_from_storage() {
        let event_storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<CounterState>::new());
        let grain_id = make_grain_id("test-5");

        // First activation - write some events
        {
            let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
                LogViewAdaptor::new(
                    grain_id.clone(),
                    event_storage.clone(),
                    Some(snapshot_storage.clone()),
                    LogViewAdaptorOptions::for_testing(),
                );

            adaptor.activate().await.unwrap();
            adaptor.raise_event(CounterEvent::Incremented(42));
            adaptor.confirm_events().await.unwrap();
            adaptor.take_snapshot().await.unwrap();
        }

        // Second activation - should load from snapshot
        {
            let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
                LogViewAdaptor::new(
                    grain_id,
                    event_storage,
                    Some(snapshot_storage),
                    LogViewAdaptorOptions::for_testing(),
                );

            adaptor.activate().await.unwrap();
            assert_eq!(adaptor.confirmed_state().value, 42);
            assert_eq!(adaptor.confirmed_version(), 1);
        }
    }

    #[tokio::test]
    async fn test_adaptor_factory() {
        let event_storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let snapshot_storage = Arc::new(InMemorySnapshotStorage::<CounterState>::new());

        let factory: LogViewAdaptorFactory<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptorFactory::with_snapshots(event_storage, snapshot_storage)
                .with_options(LogViewAdaptorOptions::for_testing());

        let grain_id = make_grain_id("test-6");
        let mut adaptor = factory.create(grain_id);

        adaptor.activate().await.unwrap();
        adaptor.raise_event(CounterEvent::Incremented(100));
        adaptor.confirm_events().await.unwrap();

        assert_eq!(adaptor.confirmed_state().value, 100);
    }

    #[tokio::test]
    async fn test_temporal_query() {
        let event_storage = Arc::new(InMemoryEventStorage::<CounterEvent>::new());
        let grain_id = make_grain_id("test-7");

        let mut adaptor: LogViewAdaptor<CounterState, CounterEvent, CounterApplier> =
            LogViewAdaptor::new(
                grain_id.clone(),
                event_storage.clone(),
                None,
                LogViewAdaptorOptions::for_testing(),
            );

        adaptor.activate().await.unwrap();

        // Create event history
        for i in 1..=5 {
            adaptor.raise_event(CounterEvent::Incremented(i));
            adaptor.confirm_events().await.unwrap();
        }

        // Query state at different versions
        let state_v2 = adaptor.get_state_at_version(2).await.unwrap();
        assert_eq!(state_v2.value, 3); // 1 + 2

        let state_v4 = adaptor.get_state_at_version(4).await.unwrap();
        assert_eq!(state_v4.value, 10); // 1 + 2 + 3 + 4
    }

    #[test]
    fn test_options_builder() {
        let options = LogViewAdaptorOptions::default()
            .with_snapshot_interval(50)
            .with_max_snapshots(3)
            .without_auto_snapshot();

        assert_eq!(options.snapshot_interval, 50);
        assert_eq!(options.max_snapshots, 3);
        assert!(!options.auto_snapshot);
    }
}
