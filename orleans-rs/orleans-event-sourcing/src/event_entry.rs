//! Event entry types for event sourcing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// A single event in the event log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEntry<E> {
    /// Sequence number of this event (1-based, monotonically increasing).
    sequence: u64,

    /// Timestamp when the event was created.
    timestamp: DateTime<Utc>,

    /// The event payload.
    event: E,

    /// Optional metadata for the event (correlation ID, user ID, etc.).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata: Option<EventMetadata>,
}

impl<E> EventEntry<E> {
    /// Creates a new event entry with the given sequence number and event.
    pub fn new(sequence: u64, event: E) -> Self {
        Self {
            sequence,
            timestamp: Utc::now(),
            event,
            metadata: None,
        }
    }

    /// Creates a new event entry with a specific timestamp.
    pub fn with_timestamp(sequence: u64, event: E, timestamp: DateTime<Utc>) -> Self {
        Self {
            sequence,
            timestamp,
            event,
            metadata: None,
        }
    }

    /// Adds metadata to this event entry.
    pub fn with_metadata(mut self, metadata: EventMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Returns the sequence number of this event.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the timestamp when this event was created.
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    /// Returns a reference to the event payload.
    pub fn event(&self) -> &E {
        &self.event
    }

    /// Consumes the entry and returns the event payload.
    pub fn into_event(self) -> E {
        self.event
    }

    /// Returns a reference to the metadata, if any.
    pub fn metadata(&self) -> Option<&EventMetadata> {
        self.metadata.as_ref()
    }
}

/// Metadata associated with an event.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Correlation ID for tracing events across operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// User ID who initiated the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,

    /// Session ID associated with the event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    /// Additional custom data.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub custom: std::collections::HashMap<String, String>,
}

impl EventMetadata {
    /// Creates new empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the correlation ID.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Sets the user ID.
    pub fn with_user_id(mut self, id: impl Into<String>) -> Self {
        self.user_id = Some(id.into());
        self
    }

    /// Sets the session ID.
    pub fn with_session_id(mut self, id: impl Into<String>) -> Self {
        self.session_id = Some(id.into());
        self
    }

    /// Adds a custom key-value pair.
    pub fn with_custom(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

/// Combined state and event log for a log-consistent grain.
#[derive(Debug, Clone)]
pub struct LogViewState<S, E> {
    /// The current state (reconstructed from events).
    state: S,

    /// The confirmed version (number of committed events).
    confirmed_version: u64,

    /// Pending events that have been raised but not yet confirmed.
    pending_events: Vec<EventEntry<E>>,

    /// The version at which the last snapshot was taken.
    last_snapshot_version: u64,
}

impl<S: Default, E> Default for LogViewState<S, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Default, E> LogViewState<S, E> {
    /// Creates a new log view state with default state.
    pub fn new() -> Self {
        Self {
            state: S::default(),
            confirmed_version: 0,
            pending_events: Vec::new(),
            last_snapshot_version: 0,
        }
    }
}

impl<S, E> LogViewState<S, E> {
    /// Creates a log view state with the given initial state.
    pub fn with_state(state: S) -> Self {
        Self {
            state,
            confirmed_version: 0,
            pending_events: Vec::new(),
            last_snapshot_version: 0,
        }
    }

    /// Creates a log view state from a snapshot.
    pub fn from_snapshot(state: S, version: u64) -> Self {
        Self {
            state,
            confirmed_version: version,
            pending_events: Vec::new(),
            last_snapshot_version: version,
        }
    }

    /// Returns a reference to the current state.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Returns a mutable reference to the current state.
    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    /// Returns the confirmed version (number of committed events).
    pub fn confirmed_version(&self) -> u64 {
        self.confirmed_version
    }

    /// Sets the confirmed version.
    pub fn set_confirmed_version(&mut self, version: u64) {
        self.confirmed_version = version;
    }

    /// Returns the pending version (confirmed + pending events).
    pub fn pending_version(&self) -> u64 {
        self.confirmed_version + self.pending_events.len() as u64
    }

    /// Returns whether there are pending events.
    pub fn has_pending_events(&self) -> bool {
        !self.pending_events.is_empty()
    }

    /// Returns the number of pending events.
    pub fn pending_event_count(&self) -> usize {
        self.pending_events.len()
    }

    /// Returns a reference to the pending events.
    pub fn pending_events(&self) -> &[EventEntry<E>] {
        &self.pending_events
    }

    /// Adds a pending event.
    pub fn add_pending_event(&mut self, event: E) {
        let sequence = self.pending_version() + 1;
        self.pending_events.push(EventEntry::new(sequence, event));
    }

    /// Adds a pending event with metadata.
    pub fn add_pending_event_with_metadata(&mut self, event: E, metadata: EventMetadata) {
        let sequence = self.pending_version() + 1;
        self.pending_events
            .push(EventEntry::new(sequence, event).with_metadata(metadata));
    }

    /// Clears all pending events (called on abort).
    pub fn clear_pending_events(&mut self) {
        self.pending_events.clear();
    }

    /// Confirms pending events up to the given version.
    pub fn confirm_events(&mut self, up_to_version: u64) {
        let to_confirm = (up_to_version - self.confirmed_version) as usize;
        if to_confirm <= self.pending_events.len() {
            self.pending_events.drain(..to_confirm);
            self.confirmed_version = up_to_version;
        }
    }

    /// Confirms all pending events.
    pub fn confirm_all_pending(&mut self) {
        self.confirmed_version += self.pending_events.len() as u64;
        self.pending_events.clear();
    }

    /// Returns the version at which the last snapshot was taken.
    pub fn last_snapshot_version(&self) -> u64 {
        self.last_snapshot_version
    }

    /// Sets the last snapshot version.
    pub fn set_last_snapshot_version(&mut self, version: u64) {
        self.last_snapshot_version = version;
    }

    /// Returns `true` if a new snapshot should be taken.
    pub fn needs_snapshot(&self, snapshot_interval: u64) -> bool {
        self.confirmed_version - self.last_snapshot_version >= snapshot_interval
    }

    /// Takes the pending events, returning them and clearing the internal list.
    pub fn take_pending_events(&mut self) -> Vec<EventEntry<E>> {
        std::mem::take(&mut self.pending_events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Incremented(i32),
        Decremented(i32),
        Reset,
    }

    #[derive(Debug, Clone, Default)]
    struct CounterState {
        value: i32,
    }

    #[test]
    fn test_event_entry_creation() {
        let entry = EventEntry::new(1, TestEvent::Incremented(5));
        assert_eq!(entry.sequence(), 1);
        assert!(entry.metadata().is_none());
        assert!(matches!(entry.event(), TestEvent::Incremented(5)));
    }

    #[test]
    fn test_event_entry_with_metadata() {
        let metadata = EventMetadata::new()
            .with_correlation_id("corr-123")
            .with_user_id("user-456");

        let entry = EventEntry::new(1, TestEvent::Reset).with_metadata(metadata);

        let meta = entry.metadata().unwrap();
        assert_eq!(meta.correlation_id.as_deref(), Some("corr-123"));
        assert_eq!(meta.user_id.as_deref(), Some("user-456"));
    }

    #[test]
    fn test_event_metadata_builder() {
        let metadata = EventMetadata::new()
            .with_correlation_id("corr")
            .with_user_id("user")
            .with_session_id("sess")
            .with_custom("key1", "value1");

        assert_eq!(metadata.correlation_id.as_deref(), Some("corr"));
        assert_eq!(metadata.user_id.as_deref(), Some("user"));
        assert_eq!(metadata.session_id.as_deref(), Some("sess"));
        assert_eq!(metadata.custom.get("key1").map(String::as_str), Some("value1"));
    }

    #[test]
    fn test_log_view_state_creation() {
        let log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();
        assert_eq!(log_state.confirmed_version(), 0);
        assert_eq!(log_state.pending_version(), 0);
        assert!(!log_state.has_pending_events());
    }

    #[test]
    fn test_log_view_state_from_snapshot() {
        let state = CounterState { value: 42 };
        let log_state: LogViewState<CounterState, TestEvent> = LogViewState::from_snapshot(state, 10);

        assert_eq!(log_state.confirmed_version(), 10);
        assert_eq!(log_state.state().value, 42);
        assert_eq!(log_state.last_snapshot_version(), 10);
    }

    #[test]
    fn test_log_view_state_pending_events() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();

        log_state.add_pending_event(TestEvent::Incremented(1));
        log_state.add_pending_event(TestEvent::Incremented(2));

        assert_eq!(log_state.pending_event_count(), 2);
        assert_eq!(log_state.pending_version(), 2);
        assert_eq!(log_state.confirmed_version(), 0);
        assert!(log_state.has_pending_events());

        // Check sequence numbers
        assert_eq!(log_state.pending_events()[0].sequence(), 1);
        assert_eq!(log_state.pending_events()[1].sequence(), 2);
    }

    #[test]
    fn test_log_view_state_confirm_events() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();

        log_state.add_pending_event(TestEvent::Incremented(1));
        log_state.add_pending_event(TestEvent::Incremented(2));
        log_state.add_pending_event(TestEvent::Incremented(3));

        // Confirm first 2 events
        log_state.confirm_events(2);

        assert_eq!(log_state.confirmed_version(), 2);
        assert_eq!(log_state.pending_event_count(), 1);
        assert_eq!(log_state.pending_events()[0].sequence(), 3);
    }

    #[test]
    fn test_log_view_state_confirm_all() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();

        log_state.add_pending_event(TestEvent::Incremented(1));
        log_state.add_pending_event(TestEvent::Incremented(2));

        log_state.confirm_all_pending();

        assert_eq!(log_state.confirmed_version(), 2);
        assert!(!log_state.has_pending_events());
    }

    #[test]
    fn test_log_view_state_clear_pending() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();

        log_state.add_pending_event(TestEvent::Incremented(1));
        log_state.add_pending_event(TestEvent::Incremented(2));

        log_state.clear_pending_events();

        assert!(!log_state.has_pending_events());
        assert_eq!(log_state.confirmed_version(), 0);
    }

    #[test]
    fn test_log_view_state_needs_snapshot() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();
        log_state.set_confirmed_version(50);
        log_state.set_last_snapshot_version(40);

        assert!(log_state.needs_snapshot(10));
        assert!(!log_state.needs_snapshot(20));
    }

    #[test]
    fn test_log_view_state_take_pending() {
        let mut log_state: LogViewState<CounterState, TestEvent> = LogViewState::new();

        log_state.add_pending_event(TestEvent::Incremented(1));
        log_state.add_pending_event(TestEvent::Incremented(2));

        let taken = log_state.take_pending_events();
        assert_eq!(taken.len(), 2);
        assert!(!log_state.has_pending_events());
    }
}
