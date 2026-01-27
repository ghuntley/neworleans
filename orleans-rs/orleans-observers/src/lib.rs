//! Orleans Observers and Callbacks
//!
//! This crate provides a publish-subscribe communication pattern for grains
//! where grains can send notifications to clients or other grains without polling.
//!
//! # Overview
//!
//! Observers enable:
//! - **Pub/Sub communication**: Grains can notify multiple subscribers
//! - **Weak reference storage**: Observers are stored with weak references to allow GC
//! - **One-way semantics**: Notifications are fire-and-forget
//! - **Subscription management**: Automatic expiration and cleanup
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────┐     subscribe      ┌──────────────────┐
//! │     Client       │ ─────────────────► │      Grain       │
//! │   (Observer)     │                    │  (Observable)    │
//! └──────────────────┘ ◄───────────────── └──────────────────┘
//!         │                  notify              │
//!         │                                      │
//!         ▼                                      ▼
//! ┌──────────────────┐              ┌──────────────────────┐
//! │  LocalObjectData │              │   ObserverManager    │
//! │  (Weak Ref)      │              │  (Subscriptions)     │
//! └──────────────────┘              └──────────────────────┘
//! ```
//!
//! # Components
//!
//! - **`ObserverGrainId`**: Special grain ID format for observers (`[ClientId]+[ScopedId]`)
//! - **`IGrainObserver`**: Marker trait for observer interfaces
//! - **`ObserverManager`**: Subscription management with copy-on-write and expiration
//! - **`LocalObjectData`**: Weak reference storage for registered observers
//! - **`InvokableObjectManager`**: Registry and dispatch for local observers
//!
//! # Example: Observable Grain
//!
//! ```ignore
//! use orleans_observers::{ObserverManager, IGrainObserver};
//! use async_trait::async_trait;
//! use std::time::Duration;
//!
//! // Define observer interface
//! #[async_trait]
//! pub trait IChatObserver: IGrainObserver {
//!     async fn on_message(&self, from: String, message: String);
//! }
//!
//! // Observable grain
//! pub struct ChatRoomGrain {
//!     observers: ObserverManager<String, Box<dyn IChatObserver>>,
//! }
//!
//! impl ChatRoomGrain {
//!     pub fn new() -> Self {
//!         Self {
//!             observers: ObserverManager::new(Duration::from_secs(300)),
//!         }
//!     }
//!
//!     pub fn subscribe(&self, user_id: String, observer: Box<dyn IChatObserver>) {
//!         self.observers.subscribe(user_id, observer);
//!     }
//!
//!     pub fn unsubscribe(&self, user_id: &str) {
//!         self.observers.unsubscribe(&user_id.to_string());
//!     }
//!
//!     pub async fn send_message(&self, from: String, message: String) {
//!         self.observers.notify_async(|observer| {
//!             let from = from.clone();
//!             let message = message.clone();
//!             async move {
//!                 observer.on_message(from, message).await;
//!                 Ok(())
//!             }
//!         }).await;
//!     }
//! }
//! ```
//!
//! # Example: Client Registration
//!
//! ```ignore
//! use orleans_observers::{InvokableObjectManager, IGrainObserver};
//! use std::sync::Arc;
//!
//! // Create manager for client
//! let manager = InvokableObjectManager::new("client-123".to_string());
//!
//! // Register observer
//! let observer: Arc<dyn IGrainObserver> = Arc::new(MyChatObserver::new());
//! let observer_id = manager.register(observer).unwrap();
//!
//! // Pass observer_id to grain for subscription
//! // grain.subscribe(observer_id).await;
//!
//! // Later, deregister
//! manager.deregister(&observer_id).unwrap();
//! ```
//!
//! # One-Way (Fire-and-Forget) Calls
//!
//! Observer notifications are typically one-way calls that don't expect a response.
//! Use `InvokeMethodOptions::one_way()` when invoking observer methods.
//!
//! # Expiration
//!
//! Subscriptions can be configured with an expiration duration. Observers that
//! haven't been renewed within this duration are automatically removed during
//! notification or explicit cleanup.

pub mod error;
pub mod invokable_object_manager;
pub mod local_object_data;
pub mod observer_grain_id;
pub mod observer_manager;
pub mod traits;

// Re-exports for convenience
pub use error::{ObserverError, ObserverResult};
pub use invokable_object_manager::InvokableObjectManager;
pub use local_object_data::{LocalObjectData, ObserverMessage};
pub use observer_grain_id::ObserverGrainId;
pub use observer_manager::{ObserverManager, SimpleObserverManager};
pub use traits::{IAddressable, IGrainObserver, IInvokable, IObserverFactory, InvokeMethodOptions};

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Debug)]
    struct TestObserver {
        name: String,
    }

    impl IGrainObserver for TestObserver {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<ObserverGrainId>();
        let _ = std::any::type_name::<ObserverManager<String, i32>>();
        let _ = std::any::type_name::<InvokableObjectManager>();
        let _ = std::any::type_name::<LocalObjectData>();
        let _ = std::any::type_name::<ObserverMessage>();
        let _ = std::any::type_name::<InvokeMethodOptions>();
    }

    #[test]
    fn test_observer_grain_id_creation() {
        let observer_id = ObserverGrainId::create("test-client");
        assert!(ObserverGrainId::is_observer_grain_id(observer_id.grain_id()));
        assert_eq!(observer_id.client_id(), Some("test-client"));
    }

    #[test]
    fn test_observer_manager_basic() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 100);
        assert_eq!(manager.count(), 1);
        assert!(manager.contains(&"key-1".to_string()));

        manager.unsubscribe(&"key-1".to_string());
        assert!(manager.is_empty());
    }

    #[test]
    fn test_invokable_object_manager_basic() {
        let manager = InvokableObjectManager::new("client-1".to_string());

        let observer: Arc<dyn IGrainObserver> = Arc::new(TestObserver {
            name: "test".to_string(),
        });

        let observer_id = manager.register(observer).unwrap();
        assert!(manager.is_registered(&observer_id));

        manager.deregister(&observer_id).unwrap();
        assert!(!manager.is_registered(&observer_id));
    }

    #[test]
    fn test_invoke_method_options() {
        let default = InvokeMethodOptions::default();
        assert!(!default.one_way);
        assert!(!default.read_only);

        let one_way = InvokeMethodOptions::one_way();
        assert!(one_way.one_way);

        let read_only = InvokeMethodOptions::read_only();
        assert!(read_only.read_only);

        let interleave = InvokeMethodOptions::always_interleave();
        assert!(interleave.always_interleave);
    }

    #[test]
    fn test_error_types() {
        let err = ObserverError::NotRegistered {
            observer_id: "test".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("not registered"));

        let err2 = ObserverError::ObserverGarbageCollected {
            observer_id: "gc-test".to_string(),
        };
        let msg2 = format!("{}", err2);
        assert!(msg2.contains("garbage collected"));
    }

    #[tokio::test]
    async fn test_observer_manager_async_notify() {
        let manager: ObserverManager<String, i32> = ObserverManager::new(Duration::from_secs(60));

        manager.subscribe("key-1".to_string(), 10);
        manager.subscribe("key-2".to_string(), 20);

        let sum = std::sync::Arc::new(std::sync::atomic::AtomicI32::new(0));
        let sum_clone = sum.clone();

        manager
            .notify_async(move |value| {
                let sum = sum_clone.clone();
                async move {
                    sum.fetch_add(value, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;

        assert_eq!(sum.load(std::sync::atomic::Ordering::SeqCst), 30);
    }

    #[test]
    fn test_full_observer_flow() {
        // 1. Create invokable object manager (on client)
        let manager = InvokableObjectManager::new("client-1".to_string());

        // 2. Create and register observer
        let observer: Arc<dyn IGrainObserver> = Arc::new(TestObserver {
            name: "my-observer".to_string(),
        });
        // Keep strong reference alive - manager stores weak reference
        let _keep_alive = observer.clone();
        let observer_id = manager.register(observer).unwrap();

        // 3. Verify observer ID format
        assert!(ObserverGrainId::is_observer_grain_id(observer_id.grain_id()));
        assert_eq!(observer_id.client_id(), Some("client-1"));

        // 4. Dispatch a message (simulating grain notification)
        let grain_id = observer_id.grain_id().clone();
        let result = manager.dispatch(&grain_id, 1, &[1, 2, 3]);
        assert!(result.is_ok());

        // 5. Get local object data and process message
        let data = manager.get(&observer_id).unwrap();
        let msg = data.try_dequeue_message();
        assert!(msg.is_some());
        assert_eq!(msg.unwrap().method_id, 1);

        // 6. Cleanup
        manager.deregister(&observer_id).unwrap();
        assert!(!manager.is_registered(&observer_id));
    }
}
