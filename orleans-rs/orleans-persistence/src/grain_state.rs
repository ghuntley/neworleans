//! Grain state wrapper with ETag tracking.
//!
//! This module provides the `GrainState<T>` wrapper that tracks metadata
//! about persisted grain state, including the ETag for optimistic concurrency
//! and whether a record exists in storage.

use serde::{Deserialize, Serialize};

/// Wrapper around grain state that tracks persistence metadata.
///
/// `GrainState<T>` wraps the actual grain state `T` and adds metadata needed
/// for persistence operations:
///
/// - `etag`: Version token for optimistic concurrency control
/// - `record_exists`: Whether this state has been persisted to storage
///
/// # Example
///
/// ```
/// use orleans_persistence::GrainState;
///
/// #[derive(Default, Clone)]
/// struct MyState {
///     counter: i32,
/// }
///
/// let mut state = GrainState::<MyState>::new();
/// assert!(!state.record_exists());
/// assert!(state.etag().is_none());
///
/// // After reading from storage, etag and record_exists would be set
/// state.state_mut().counter = 42;
/// ```
#[derive(Debug, Clone)]
pub struct GrainState<T> {
    /// The actual grain state.
    state: T,
    /// The ETag for optimistic concurrency control.
    /// None if the state hasn't been read from or written to storage.
    etag: Option<String>,
    /// Whether this state has a corresponding record in storage.
    record_exists: bool,
}

impl<T: Default> Default for GrainState<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Default> GrainState<T> {
    /// Create a new grain state with default values.
    ///
    /// The state will be initialized with `T::default()`, no ETag,
    /// and `record_exists` set to false.
    pub fn new() -> Self {
        Self {
            state: T::default(),
            etag: None,
            record_exists: false,
        }
    }
}

impl<T> GrainState<T> {
    /// Create a grain state with a specific initial value.
    ///
    /// The state will be initialized with the provided value, no ETag,
    /// and `record_exists` set to false.
    pub fn with_state(state: T) -> Self {
        Self {
            state,
            etag: None,
            record_exists: false,
        }
    }

    /// Get a reference to the state.
    pub fn state(&self) -> &T {
        &self.state
    }

    /// Get a mutable reference to the state.
    pub fn state_mut(&mut self) -> &mut T {
        &mut self.state
    }

    /// Get the current ETag.
    ///
    /// Returns `None` if the state hasn't been read from or written to storage.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// Check if a record exists in storage for this state.
    pub fn record_exists(&self) -> bool {
        self.record_exists
    }

    /// Set the ETag (for use by storage providers).
    pub fn set_etag(&mut self, etag: Option<String>) {
        self.etag = etag;
    }

    /// Set whether the record exists (for use by storage providers).
    pub fn set_record_exists(&mut self, exists: bool) {
        self.record_exists = exists;
    }

    /// Replace the entire state value.
    pub fn set_state(&mut self, state: T) {
        self.state = state;
    }

    /// Clear the state back to defaults (for use by storage providers).
    ///
    /// This clears the ETag, sets `record_exists` to false,
    /// and resets the state to the provided default value.
    pub fn clear(&mut self, default_state: T) {
        self.state = default_state;
        self.etag = None;
        self.record_exists = false;
    }

    /// Update metadata after a successful read from storage.
    pub fn mark_read(&mut self, state: T, etag: Option<String>, exists: bool) {
        self.state = state;
        self.etag = etag;
        self.record_exists = exists;
    }

    /// Update metadata after a successful write to storage.
    pub fn mark_written(&mut self, new_etag: String) {
        self.etag = Some(new_etag);
        self.record_exists = true;
    }

    /// Update metadata after a successful clear operation.
    pub fn mark_cleared(&mut self, default_state: T) {
        self.state = default_state;
        self.etag = None;
        self.record_exists = false;
    }
}

/// Serializable representation of grain state for storage.
///
/// This is used internally by storage providers to serialize the state
/// along with its metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredGrainState<T> {
    /// The serialized state data.
    pub state: T,
    /// The version/ETag when this was stored.
    pub version: String,
}

impl<T> StoredGrainState<T> {
    /// Create a new stored state record.
    pub fn new(state: T, version: String) -> Self {
        Self { state, version }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Default, Clone, PartialEq)]
    struct TestState {
        counter: i32,
        name: String,
    }

    #[test]
    fn test_new_grain_state_has_defaults() {
        let state = GrainState::<TestState>::new();
        assert_eq!(state.state().counter, 0);
        assert_eq!(state.state().name, "");
        assert!(state.etag().is_none());
        assert!(!state.record_exists());
    }

    #[test]
    fn test_with_state_initializes_correctly() {
        let initial = TestState {
            counter: 42,
            name: "test".to_string(),
        };
        let state = GrainState::with_state(initial.clone());
        assert_eq!(*state.state(), initial);
        assert!(state.etag().is_none());
        assert!(!state.record_exists());
    }

    #[test]
    fn test_state_mutation() {
        let mut state = GrainState::<TestState>::new();
        state.state_mut().counter = 100;
        state.state_mut().name = "mutated".to_string();
        assert_eq!(state.state().counter, 100);
        assert_eq!(state.state().name, "mutated");
    }

    #[test]
    fn test_set_state_replaces_entire_state() {
        let mut state = GrainState::<TestState>::new();
        state.set_state(TestState {
            counter: 999,
            name: "replaced".to_string(),
        });
        assert_eq!(state.state().counter, 999);
        assert_eq!(state.state().name, "replaced");
    }

    #[test]
    fn test_etag_management() {
        let mut state = GrainState::<TestState>::new();
        assert!(state.etag().is_none());

        state.set_etag(Some("etag-1".to_string()));
        assert_eq!(state.etag(), Some("etag-1"));

        state.set_etag(Some("etag-2".to_string()));
        assert_eq!(state.etag(), Some("etag-2"));

        state.set_etag(None);
        assert!(state.etag().is_none());
    }

    #[test]
    fn test_record_exists_flag() {
        let mut state = GrainState::<TestState>::new();
        assert!(!state.record_exists());

        state.set_record_exists(true);
        assert!(state.record_exists());

        state.set_record_exists(false);
        assert!(!state.record_exists());
    }

    #[test]
    fn test_mark_read_updates_all_fields() {
        let mut state = GrainState::<TestState>::new();
        let new_data = TestState {
            counter: 42,
            name: "read".to_string(),
        };

        state.mark_read(new_data.clone(), Some("read-etag".to_string()), true);

        assert_eq!(*state.state(), new_data);
        assert_eq!(state.etag(), Some("read-etag"));
        assert!(state.record_exists());
    }

    #[test]
    fn test_mark_read_with_no_record() {
        let mut state = GrainState::<TestState>::new();
        state.mark_read(TestState::default(), None, false);

        assert!(!state.record_exists());
        assert!(state.etag().is_none());
    }

    #[test]
    fn test_mark_written_updates_metadata() {
        let mut state = GrainState::<TestState>::new();
        state.state_mut().counter = 100;

        state.mark_written("write-etag".to_string());

        assert_eq!(state.etag(), Some("write-etag"));
        assert!(state.record_exists());
        assert_eq!(state.state().counter, 100); // State unchanged
    }

    #[test]
    fn test_mark_cleared_resets_state() {
        let mut state = GrainState::<TestState>::new();
        state.state_mut().counter = 100;
        state.set_etag(Some("some-etag".to_string()));
        state.set_record_exists(true);

        state.mark_cleared(TestState::default());

        assert_eq!(state.state().counter, 0);
        assert!(state.etag().is_none());
        assert!(!state.record_exists());
    }

    #[test]
    fn test_clear_resets_all() {
        let mut state = GrainState::<TestState>::new();
        state.state_mut().counter = 100;
        state.set_etag(Some("etag".to_string()));
        state.set_record_exists(true);

        state.clear(TestState::default());

        assert_eq!(state.state().counter, 0);
        assert!(state.etag().is_none());
        assert!(!state.record_exists());
    }

    #[test]
    fn test_stored_grain_state_creation() {
        let stored = StoredGrainState::new(
            TestState {
                counter: 42,
                name: "stored".to_string(),
            },
            "v1".to_string(),
        );
        assert_eq!(stored.state.counter, 42);
        assert_eq!(stored.version, "v1");
    }

    #[test]
    fn test_default_impl() {
        let state: GrainState<TestState> = Default::default();
        assert!(!state.record_exists());
        assert!(state.etag().is_none());
    }
}
