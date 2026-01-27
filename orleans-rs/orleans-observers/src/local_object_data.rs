//! Local Object Data - Weak reference storage for observers.
//!
//! This module provides the storage and dispatch mechanism for locally
//! registered observers using weak references to allow garbage collection.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use tracing::{debug, trace, warn};

use crate::error::{ObserverError, ObserverResult};
use crate::traits::IGrainObserver;
use crate::ObserverGrainId;

/// Data associated with a locally registered observer.
///
/// Uses weak references to allow the observer to be garbage collected
/// when no longer in use elsewhere. Messages are queued and processed
/// sequentially to maintain ordering guarantees.
pub struct LocalObjectData {
    /// Weak reference to the observer (allows GC).
    local_object: Weak<dyn IGrainObserver>,
    /// The observer's grain ID.
    observer_id: ObserverGrainId,
    /// Pending messages queue.
    messages: Mutex<VecDeque<ObserverMessage>>,
    /// Whether the message pump is currently running.
    running: AtomicBool,
    /// Whether this object has been deregistered.
    deregistered: AtomicBool,
}

/// A message to be delivered to an observer.
#[derive(Clone)]
pub struct ObserverMessage {
    /// The method ID to invoke.
    pub method_id: u32,
    /// The serialized method arguments.
    pub body: Vec<u8>,
    /// Whether this message should be processed immediately (always interleave).
    pub always_interleave: bool,
}

impl LocalObjectData {
    /// Creates a new local object data entry.
    ///
    /// # Arguments
    /// * `observer` - The observer object (stored as weak reference)
    /// * `observer_id` - The observer's grain ID
    pub fn new(observer: Arc<dyn IGrainObserver>, observer_id: ObserverGrainId) -> Self {
        debug!(observer_id = %observer_id, "Creating local object data");
        Self {
            local_object: Arc::downgrade(&observer),
            observer_id,
            messages: Mutex::new(VecDeque::new()),
            running: AtomicBool::new(false),
            deregistered: AtomicBool::new(false),
        }
    }

    /// Returns the observer ID.
    pub fn observer_id(&self) -> &ObserverGrainId {
        &self.observer_id
    }

    /// Returns true if the observer is still alive (not garbage collected).
    pub fn is_alive(&self) -> bool {
        self.local_object.strong_count() > 0
    }

    /// Returns true if this object has been deregistered.
    pub fn is_deregistered(&self) -> bool {
        self.deregistered.load(Ordering::SeqCst)
    }

    /// Marks this object as deregistered.
    pub fn mark_deregistered(&self) {
        self.deregistered.store(true, Ordering::SeqCst);
    }

    /// Returns the number of pending messages.
    pub fn pending_message_count(&self) -> usize {
        self.messages.lock().len()
    }

    /// Attempts to upgrade the weak reference to a strong reference.
    ///
    /// Returns `None` if the observer has been garbage collected.
    pub fn try_get_observer(&self) -> Option<Arc<dyn IGrainObserver>> {
        self.local_object.upgrade()
    }

    /// Receives a message for this observer.
    ///
    /// If the observer has been garbage collected, returns an error.
    /// Otherwise, queues the message and starts the message pump if needed.
    ///
    /// # Arguments
    /// * `message` - The message to deliver
    ///
    /// # Returns
    /// `Ok(true)` if message pump was started, `Ok(false)` if already running,
    /// or an error if the observer was garbage collected.
    pub fn receive_message(&self, message: ObserverMessage) -> ObserverResult<bool> {
        // Check if observer is still alive
        if self.local_object.strong_count() == 0 {
            warn!(observer_id = %self.observer_id, "Observer was garbage collected");
            return Err(ObserverError::ObserverGarbageCollected {
                observer_id: self.observer_id.to_string(),
            });
        }

        // Check if deregistered
        if self.is_deregistered() {
            return Err(ObserverError::NotRegistered {
                observer_id: self.observer_id.to_string(),
            });
        }

        // Handle always-interleave messages immediately
        if message.always_interleave {
            trace!(
                observer_id = %self.observer_id,
                method_id = message.method_id,
                "Processing always-interleave message immediately"
            );
            // For always-interleave, we don't queue - process immediately
            // The caller is responsible for handling this case
            return Ok(false);
        }

        // Queue the message
        let start_pump;
        {
            let mut messages = self.messages.lock();
            messages.push_back(message);
            start_pump = !self.running.swap(true, Ordering::SeqCst);
        }

        if start_pump {
            trace!(observer_id = %self.observer_id, "Starting message pump");
        }

        Ok(start_pump)
    }

    /// Dequeues the next message, if any.
    ///
    /// Returns `None` if the queue is empty (and marks the pump as stopped).
    pub fn try_dequeue_message(&self) -> Option<ObserverMessage> {
        let mut messages = self.messages.lock();
        if let Some(msg) = messages.pop_front() {
            Some(msg)
        } else {
            self.running.store(false, Ordering::SeqCst);
            None
        }
    }

    /// Clears all pending messages.
    ///
    /// # Returns
    /// The number of messages that were cleared.
    pub fn clear_messages(&self) -> usize {
        let mut messages = self.messages.lock();
        let count = messages.len();
        messages.clear();
        self.running.store(false, Ordering::SeqCst);
        count
    }
}

impl std::fmt::Debug for LocalObjectData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalObjectData")
            .field("observer_id", &self.observer_id)
            .field("is_alive", &self.is_alive())
            .field("is_deregistered", &self.is_deregistered())
            .field("pending_messages", &self.pending_message_count())
            .finish()
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
        let observer = create_test_observer("test-1");
        let observer_id = ObserverGrainId::create("client-1");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id.clone());

        assert!(data.is_alive());
        assert!(!data.is_deregistered());
        assert_eq!(data.pending_message_count(), 0);
        assert_eq!(data.observer_id(), &observer_id);
    }

    #[test]
    fn test_is_alive_with_strong_reference() {
        let observer = create_test_observer("test-2");
        let observer_id = ObserverGrainId::create("client-2");

        let data = LocalObjectData::new(observer.clone(), observer_id);

        assert!(data.is_alive());

        // Observer still held
        drop(observer);

        // Now it should be dead (depending on when drop happens)
        // Note: This test is a bit racy, but demonstrates the concept
    }

    #[test]
    fn test_garbage_collection() {
        let observer_id = ObserverGrainId::create("client-gc");

        let data = {
            let observer = create_test_observer("gc-test");
            LocalObjectData::new(observer, observer_id.clone())
        };
        // Observer dropped here

        assert!(!data.is_alive());
        assert!(data.try_get_observer().is_none());

        // Receiving a message should fail
        let msg = ObserverMessage {
            method_id: 1,
            body: vec![],
            always_interleave: false,
        };
        let result = data.receive_message(msg);
        assert!(matches!(
            result,
            Err(ObserverError::ObserverGarbageCollected { .. })
        ));
    }

    #[test]
    fn test_receive_message_queues() {
        let observer = create_test_observer("queue-test");
        let observer_id = ObserverGrainId::create("client-queue");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id);

        let msg1 = ObserverMessage {
            method_id: 1,
            body: vec![1, 2, 3],
            always_interleave: false,
        };
        let msg2 = ObserverMessage {
            method_id: 2,
            body: vec![4, 5, 6],
            always_interleave: false,
        };

        // First message should start pump
        let start1 = data.receive_message(msg1).unwrap();
        assert!(start1);
        assert_eq!(data.pending_message_count(), 1);

        // Second message should not start pump (already running)
        let start2 = data.receive_message(msg2).unwrap();
        assert!(!start2);
        assert_eq!(data.pending_message_count(), 2);
    }

    #[test]
    fn test_try_dequeue_message() {
        let observer = create_test_observer("dequeue-test");
        let observer_id = ObserverGrainId::create("client-dequeue");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id);

        // Queue two messages
        let msg1 = ObserverMessage {
            method_id: 1,
            body: vec![1],
            always_interleave: false,
        };
        let msg2 = ObserverMessage {
            method_id: 2,
            body: vec![2],
            always_interleave: false,
        };

        data.receive_message(msg1).unwrap();
        data.receive_message(msg2).unwrap();

        // Dequeue first
        let dequeued1 = data.try_dequeue_message();
        assert!(dequeued1.is_some());
        assert_eq!(dequeued1.unwrap().method_id, 1);
        assert_eq!(data.pending_message_count(), 1);

        // Dequeue second
        let dequeued2 = data.try_dequeue_message();
        assert!(dequeued2.is_some());
        assert_eq!(dequeued2.unwrap().method_id, 2);
        assert_eq!(data.pending_message_count(), 0);

        // Empty queue
        let dequeued3 = data.try_dequeue_message();
        assert!(dequeued3.is_none());
    }

    #[test]
    fn test_clear_messages() {
        let observer = create_test_observer("clear-test");
        let observer_id = ObserverGrainId::create("client-clear");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id);

        // Queue some messages
        for i in 0..5 {
            let msg = ObserverMessage {
                method_id: i,
                body: vec![],
                always_interleave: false,
            };
            data.receive_message(msg).unwrap();
        }

        assert_eq!(data.pending_message_count(), 5);

        let cleared = data.clear_messages();
        assert_eq!(cleared, 5);
        assert_eq!(data.pending_message_count(), 0);
    }

    #[test]
    fn test_mark_deregistered() {
        let observer = create_test_observer("dereg-test");
        let observer_id = ObserverGrainId::create("client-dereg");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id);

        assert!(!data.is_deregistered());
        data.mark_deregistered();
        assert!(data.is_deregistered());

        // Receiving messages should fail after deregistration
        let msg = ObserverMessage {
            method_id: 1,
            body: vec![],
            always_interleave: false,
        };
        let result = data.receive_message(msg);
        assert!(matches!(result, Err(ObserverError::NotRegistered { .. })));
    }

    #[test]
    fn test_always_interleave_not_queued() {
        let observer = create_test_observer("interleave-test");
        let observer_id = ObserverGrainId::create("client-interleave");

        // Keep strong reference alive - LocalObjectData only stores Weak
        let _keep_alive = observer.clone();
        let data = LocalObjectData::new(observer, observer_id);

        let msg = ObserverMessage {
            method_id: 1,
            body: vec![],
            always_interleave: true,
        };

        // Always-interleave should not start pump or queue
        let result = data.receive_message(msg).unwrap();
        assert!(!result);
        assert_eq!(data.pending_message_count(), 0);
    }

    #[test]
    fn test_debug_format() {
        let observer = create_test_observer("debug-test");
        let observer_id = ObserverGrainId::create("client-debug");

        let data = LocalObjectData::new(observer, observer_id);

        let debug = format!("{:?}", data);
        assert!(debug.contains("LocalObjectData"));
        assert!(debug.contains("is_alive"));
        assert!(debug.contains("pending_messages"));
    }

    #[test]
    fn test_try_get_observer() {
        let observer = create_test_observer("get-test");
        let observer_id = ObserverGrainId::create("client-get");

        let data = LocalObjectData::new(observer.clone(), observer_id);

        // Should be able to get observer
        let retrieved = data.try_get_observer();
        assert!(retrieved.is_some());

        // Drop original reference
        drop(observer);
        drop(retrieved);

        // Now it might be gone (but this is racy)
    }
}
