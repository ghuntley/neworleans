//! Invokable Object Manager - Registry and dispatch for local observers.
//!
//! This module provides the central registry for locally registered observers
//! and handles dispatching incoming messages to the appropriate observer.

use dashmap::DashMap;
use std::sync::Arc;
use tracing::{debug, trace, warn};

use crate::error::{ObserverError, ObserverResult};
use crate::local_object_data::{LocalObjectData, ObserverMessage};
use crate::traits::IGrainObserver;
use crate::ObserverGrainId;
use orleans_core::GrainId;

/// Manager for locally registered invokable objects (observers).
///
/// This manager:
/// - Registers and deregisters observer objects
/// - Dispatches incoming messages to the appropriate observer
/// - Handles garbage collection of defunct observers
///
/// # Example
///
/// ```ignore
/// use orleans_observers::{InvokableObjectManager, IGrainObserver, ObserverGrainId};
/// use std::sync::Arc;
///
/// let manager = InvokableObjectManager::new("client-1".to_string());
///
/// // Register an observer
/// let observer: Arc<dyn IGrainObserver> = Arc::new(MyObserver::new());
/// let observer_id = manager.register(observer).unwrap();
///
/// // Dispatch a message
/// manager.dispatch(observer_id.grain_id(), 1, &[]).await.unwrap();
///
/// // Deregister
/// manager.deregister(&observer_id).unwrap();
/// ```
pub struct InvokableObjectManager {
    /// Registered local objects.
    local_objects: DashMap<ObserverGrainId, Arc<LocalObjectData>>,
    /// The client ID for this manager.
    client_id: String,
}

impl InvokableObjectManager {
    /// Creates a new invokable object manager.
    ///
    /// # Arguments
    /// * `client_id` - The client identifier for observer ID generation
    pub fn new(client_id: String) -> Self {
        debug!(client_id = %client_id, "Creating InvokableObjectManager");
        Self {
            local_objects: DashMap::new(),
            client_id,
        }
    }

    /// Returns the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the number of registered observers.
    pub fn count(&self) -> usize {
        self.local_objects.len()
    }

    /// Returns true if there are no registered observers.
    pub fn is_empty(&self) -> bool {
        self.local_objects.is_empty()
    }

    /// Registers an observer object and returns its observer ID.
    ///
    /// # Arguments
    /// * `observer` - The observer object to register
    ///
    /// # Returns
    /// The generated `ObserverGrainId` for this registration.
    pub fn register(&self, observer: Arc<dyn IGrainObserver>) -> ObserverResult<ObserverGrainId> {
        let observer_id = ObserverGrainId::create(&self.client_id);
        self.try_register(observer, observer_id.clone())?;
        Ok(observer_id)
    }

    /// Attempts to register an observer with a specific ID.
    ///
    /// # Arguments
    /// * `observer` - The observer object to register
    /// * `observer_id` - The specific observer ID to use
    ///
    /// # Returns
    /// `Ok(())` if registration succeeded, or an error if the ID is already registered.
    pub fn try_register(
        &self,
        observer: Arc<dyn IGrainObserver>,
        observer_id: ObserverGrainId,
    ) -> ObserverResult<()> {
        let data = Arc::new(LocalObjectData::new(observer, observer_id.clone()));

        if self.local_objects.insert(observer_id.clone(), data).is_some() {
            warn!(observer_id = %observer_id, "Observer ID already registered");
            return Err(ObserverError::AlreadyRegistered {
                observer_id: observer_id.to_string(),
            });
        }

        debug!(observer_id = %observer_id, "Registered observer");
        Ok(())
    }

    /// Deregisters an observer.
    ///
    /// # Arguments
    /// * `observer_id` - The observer ID to deregister
    ///
    /// # Returns
    /// `Ok(())` if deregistration succeeded, or an error if not found.
    pub fn deregister(&self, observer_id: &ObserverGrainId) -> ObserverResult<()> {
        if let Some((_, data)) = self.local_objects.remove(observer_id) {
            data.mark_deregistered();
            debug!(observer_id = %observer_id, "Deregistered observer");
            Ok(())
        } else {
            Err(ObserverError::NotRegistered {
                observer_id: observer_id.to_string(),
            })
        }
    }

    /// Checks if an observer is registered.
    pub fn is_registered(&self, observer_id: &ObserverGrainId) -> bool {
        self.local_objects.contains_key(observer_id)
    }

    /// Gets the local object data for an observer.
    pub fn get(&self, observer_id: &ObserverGrainId) -> Option<Arc<LocalObjectData>> {
        self.local_objects.get(observer_id).map(|r| r.clone())
    }

    /// Dispatches a message to an observer by grain ID.
    ///
    /// # Arguments
    /// * `grain_id` - The target grain ID (must be an observer ID)
    /// * `method_id` - The method to invoke
    /// * `body` - The serialized method arguments
    ///
    /// # Returns
    /// `Ok(true)` if the message pump was started, `Ok(false)` if already running,
    /// or an error if the observer is not found or was garbage collected.
    pub fn dispatch(
        &self,
        grain_id: &GrainId,
        method_id: u32,
        body: &[u8],
    ) -> ObserverResult<bool> {
        // Parse the grain ID as an observer ID
        let observer_id = ObserverGrainId::try_parse(grain_id).ok_or_else(|| {
            warn!(grain_id = %grain_id, "Message not addressed to an observer");
            ObserverError::NotObserverGrainId {
                grain_id: grain_id.to_string(),
            }
        })?;

        self.dispatch_to_observer(&observer_id, method_id, body, false)
    }

    /// Dispatches a message to an observer by observer ID.
    ///
    /// # Arguments
    /// * `observer_id` - The target observer ID
    /// * `method_id` - The method to invoke
    /// * `body` - The serialized method arguments
    /// * `always_interleave` - Whether to process immediately
    ///
    /// # Returns
    /// `Ok(true)` if the message pump was started, `Ok(false)` if already running
    /// or if always_interleave is true.
    pub fn dispatch_to_observer(
        &self,
        observer_id: &ObserverGrainId,
        method_id: u32,
        body: &[u8],
        always_interleave: bool,
    ) -> ObserverResult<bool> {
        let data = self.local_objects.get(observer_id).ok_or_else(|| {
            trace!(observer_id = %observer_id, "Observer not found");
            ObserverError::NotRegistered {
                observer_id: observer_id.to_string(),
            }
        })?;

        let message = ObserverMessage {
            method_id,
            body: body.to_vec(),
            always_interleave,
        };

        let result = data.receive_message(message);

        // If the observer was garbage collected, deregister it
        if matches!(result, Err(ObserverError::ObserverGarbageCollected { .. })) {
            drop(data);
            let _ = self.local_objects.remove(observer_id);
        }

        result
    }

    /// Cleans up garbage-collected observers.
    ///
    /// Removes all observers whose weak references are no longer valid.
    ///
    /// # Returns
    /// The number of observers that were removed.
    pub fn cleanup_garbage_collected(&self) -> usize {
        let to_remove: Vec<ObserverGrainId> = self
            .local_objects
            .iter()
            .filter(|entry| !entry.value().is_alive())
            .map(|entry| entry.key().clone())
            .collect();

        let count = to_remove.len();
        for observer_id in to_remove {
            self.local_objects.remove(&observer_id);
        }

        if count > 0 {
            debug!(count, "Cleaned up garbage-collected observers");
        }

        count
    }

    /// Clears all registered observers.
    pub fn clear(&self) {
        let count = self.local_objects.len();
        for entry in self.local_objects.iter() {
            entry.value().mark_deregistered();
        }
        self.local_objects.clear();
        debug!(count, "Cleared all observers");
    }

    /// Returns all registered observer IDs.
    pub fn observer_ids(&self) -> Vec<ObserverGrainId> {
        self.local_objects.iter().map(|e| e.key().clone()).collect()
    }
}

impl std::fmt::Debug for InvokableObjectManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvokableObjectManager")
            .field("client_id", &self.client_id)
            .field("count", &self.count())
            .finish()
    }
}

impl Default for InvokableObjectManager {
    fn default() -> Self {
        Self::new("default".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;

    #[derive(Debug)]
    struct TestObserver {
        id: String,
    }

    impl IGrainObserver for TestObserver {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    fn create_test_observer(id: &str) -> Arc<dyn IGrainObserver> {
        Arc::new(TestObserver { id: id.to_string() })
    }

    #[test]
    fn test_new() {
        let manager = InvokableObjectManager::new("test-client".to_string());
        assert_eq!(manager.client_id(), "test-client");
        assert!(manager.is_empty());
        assert_eq!(manager.count(), 0);
    }

    #[test]
    fn test_default() {
        let manager = InvokableObjectManager::default();
        assert_eq!(manager.client_id(), "default");
    }

    #[test]
    fn test_register() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        let observer_id = manager.register(observer).unwrap();

        assert!(!manager.is_empty());
        assert_eq!(manager.count(), 1);
        assert!(manager.is_registered(&observer_id));
    }

    #[test]
    fn test_register_multiple() {
        let manager = InvokableObjectManager::new("client-1".to_string());

        for i in 0..5 {
            let observer = create_test_observer(&format!("obs-{}", i));
            manager.register(observer).unwrap();
        }

        assert_eq!(manager.count(), 5);
    }

    #[test]
    fn test_try_register_duplicate() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer_id = ObserverGrainId::create("client-1");

        let observer1 = create_test_observer("obs-1");
        manager.try_register(observer1, observer_id.clone()).unwrap();

        let observer2 = create_test_observer("obs-2");
        let result = manager.try_register(observer2, observer_id);
        assert!(matches!(result, Err(ObserverError::AlreadyRegistered { .. })));
    }

    #[test]
    fn test_deregister() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        let observer_id = manager.register(observer).unwrap();
        assert!(manager.is_registered(&observer_id));

        manager.deregister(&observer_id).unwrap();
        assert!(!manager.is_registered(&observer_id));
        assert!(manager.is_empty());
    }

    #[test]
    fn test_deregister_not_found() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer_id = ObserverGrainId::create("client-1");

        let result = manager.deregister(&observer_id);
        assert!(matches!(result, Err(ObserverError::NotRegistered { .. })));
    }

    #[test]
    fn test_get() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        // Keep strong reference alive - manager stores weak reference
        let _keep_alive = observer.clone();
        let observer_id = manager.register(observer).unwrap();

        let data = manager.get(&observer_id);
        assert!(data.is_some());
        assert!(data.unwrap().is_alive());
    }

    #[test]
    fn test_get_not_found() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer_id = ObserverGrainId::create("client-1");

        let data = manager.get(&observer_id);
        assert!(data.is_none());
    }

    #[test]
    fn test_dispatch() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        // Keep strong reference alive - manager stores weak reference
        let _keep_alive = observer.clone();
        let observer_id = manager.register(observer).unwrap();
        let grain_id = observer_id.grain_id().clone();

        // Dispatch should succeed
        let started = manager.dispatch(&grain_id, 1, &[1, 2, 3]).unwrap();
        assert!(started);

        // Second dispatch should return false (pump already running)
        let started2 = manager.dispatch(&grain_id, 2, &[4, 5, 6]).unwrap();
        assert!(!started2);
    }

    #[test]
    fn test_dispatch_not_observer_id() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let grain_id = GrainId::create("MyGrain", "key-1");

        let result = manager.dispatch(&grain_id, 1, &[]);
        assert!(matches!(result, Err(ObserverError::NotObserverGrainId { .. })));
    }

    #[test]
    fn test_dispatch_not_registered() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer_id = ObserverGrainId::create("client-1");
        let grain_id = observer_id.grain_id().clone();

        let result = manager.dispatch(&grain_id, 1, &[]);
        assert!(matches!(result, Err(ObserverError::NotRegistered { .. })));
    }

    #[test]
    fn test_dispatch_to_observer() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        // Keep strong reference alive - manager stores weak reference
        let _keep_alive = observer.clone();
        let observer_id = manager.register(observer).unwrap();

        let started = manager
            .dispatch_to_observer(&observer_id, 1, &[1, 2, 3], false)
            .unwrap();
        assert!(started);
    }

    #[test]
    fn test_dispatch_always_interleave() {
        let manager = InvokableObjectManager::new("client-1".to_string());
        let observer = create_test_observer("obs-1");

        // Keep strong reference alive - manager stores weak reference
        let _keep_alive = observer.clone();
        let observer_id = manager.register(observer).unwrap();

        // Always interleave should not start pump
        let started = manager
            .dispatch_to_observer(&observer_id, 1, &[1, 2, 3], true)
            .unwrap();
        assert!(!started);
    }

    #[test]
    fn test_cleanup_garbage_collected() {
        let manager = InvokableObjectManager::new("client-1".to_string());

        // Register an observer that will be dropped
        let observer_id = {
            let observer = create_test_observer("gc-obs");
            manager.register(observer).unwrap()
        };

        // Observer should still be registered but not alive
        assert!(manager.is_registered(&observer_id));
        let data = manager.get(&observer_id).unwrap();
        assert!(!data.is_alive());
        drop(data);

        // Cleanup should remove it
        let removed = manager.cleanup_garbage_collected();
        assert_eq!(removed, 1);
        assert!(!manager.is_registered(&observer_id));
    }

    #[test]
    fn test_clear() {
        let manager = InvokableObjectManager::new("client-1".to_string());

        for i in 0..5 {
            let observer = create_test_observer(&format!("obs-{}", i));
            manager.register(observer).unwrap();
        }

        assert_eq!(manager.count(), 5);
        manager.clear();
        assert!(manager.is_empty());
    }

    #[test]
    fn test_observer_ids() {
        let manager = InvokableObjectManager::new("client-1".to_string());

        let mut registered_ids = Vec::new();
        for i in 0..3 {
            let observer = create_test_observer(&format!("obs-{}", i));
            let id = manager.register(observer).unwrap();
            registered_ids.push(id);
        }

        let ids = manager.observer_ids();
        assert_eq!(ids.len(), 3);

        for id in &registered_ids {
            assert!(ids.contains(id));
        }
    }

    #[test]
    fn test_debug_format() {
        let manager = InvokableObjectManager::new("debug-client".to_string());
        let observer = create_test_observer("obs-1");
        manager.register(observer).unwrap();

        let debug = format!("{:?}", manager);
        assert!(debug.contains("InvokableObjectManager"));
        assert!(debug.contains("debug-client"));
        assert!(debug.contains("1")); // count
    }

    #[test]
    fn test_concurrent_register_deregister() {
        use std::thread;

        let manager = Arc::new(InvokableObjectManager::new("concurrent-client".to_string()));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let manager = manager.clone();
                thread::spawn(move || {
                    for j in 0..10 {
                        let observer = create_test_observer(&format!("obs-{}-{}", i, j));
                        let id = manager.register(observer).unwrap();

                        // Small delay
                        thread::yield_now();

                        manager.deregister(&id).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(manager.is_empty());
    }
}
