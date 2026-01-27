//! Observer traits and interfaces.
//!
//! This module defines the core traits for the observer pattern in Orleans.

use async_trait::async_trait;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::ObserverResult;
use crate::ObserverGrainId;

/// Marker trait for grain observers.
///
/// All observer interfaces must extend this trait. Observer methods should
/// typically be one-way (fire-and-forget) since observers don't return values
/// to the notifying grain.
///
/// # Example
///
/// ```ignore
/// use orleans_observers::IGrainObserver;
/// use async_trait::async_trait;
///
/// // Define an observer interface
/// #[async_trait]
/// pub trait IMessageObserver: IGrainObserver {
///     async fn on_message_received(&self, message: String);
/// }
///
/// // Implement the observer
/// struct MessageHandler {
///     messages: Vec<String>,
/// }
///
/// #[async_trait]
/// impl IGrainObserver for MessageHandler {}
///
/// #[async_trait]
/// impl IMessageObserver for MessageHandler {
///     async fn on_message_received(&self, message: String) {
///         println!("Received: {}", message);
///     }
/// }
/// ```
pub trait IGrainObserver: Send + Sync + Debug + Any {
    /// Returns this observer as an Any reference for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Returns this observer as a mutable Any reference for downcasting.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Trait for addressable endpoints (grains, observers, system targets).
///
/// This is the common base for anything that can receive messages.
pub trait IAddressable: Send + Sync {
    /// Returns the grain ID for this addressable.
    fn get_grain_id(&self) -> Option<&orleans_core::GrainId>;
}

/// Trait for objects that can be invoked as observers.
///
/// This trait is implemented by observer registration handlers to dispatch
/// incoming notifications.
#[async_trait]
pub trait IInvokable: Send + Sync {
    /// Invokes a method on this object.
    ///
    /// # Arguments
    /// * `method_id` - The method identifier
    /// * `body` - The serialized method arguments
    ///
    /// # Returns
    /// An optional serialized result (for one-way calls, this is `None`).
    async fn invoke(&self, method_id: u32, body: &[u8]) -> ObserverResult<Option<Vec<u8>>>;
}

/// Options for method invocation.
#[derive(Debug, Clone, Copy, Default)]
pub struct InvokeMethodOptions {
    /// Fire-and-forget, no response expected.
    pub one_way: bool,
    /// Can interleave with other read-only calls.
    pub read_only: bool,
    /// Always interleave (process immediately).
    pub always_interleave: bool,
    /// Unordered execution allowed.
    pub unordered: bool,
}

impl InvokeMethodOptions {
    /// Creates options for a one-way call.
    pub fn one_way() -> Self {
        Self {
            one_way: true,
            ..Default::default()
        }
    }

    /// Creates options for a read-only call.
    pub fn read_only() -> Self {
        Self {
            read_only: true,
            ..Default::default()
        }
    }

    /// Creates options for an always-interleave call.
    pub fn always_interleave() -> Self {
        Self {
            always_interleave: true,
            ..Default::default()
        }
    }
}

/// Trait for observer reference creation.
///
/// Implemented by grain factories to create observer references.
#[async_trait]
pub trait IObserverFactory: Send + Sync {
    /// Creates an observer reference for the given object.
    ///
    /// # Arguments
    /// * `observer` - The observer object to register
    ///
    /// # Returns
    /// The observer grain ID for the registered observer.
    async fn create_object_reference(
        &self,
        observer: Arc<dyn IGrainObserver>,
    ) -> ObserverResult<ObserverGrainId>;

    /// Deletes an observer reference.
    ///
    /// # Arguments
    /// * `observer_id` - The observer ID to deregister
    async fn delete_object_reference(&self, observer_id: &ObserverGrainId) -> ObserverResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_observer_trait_implementation() {
        let observer = TestObserver {
            name: "test".to_string(),
        };

        // Can downcast to concrete type
        let any_ref = observer.as_any();
        let concrete = any_ref.downcast_ref::<TestObserver>();
        assert!(concrete.is_some());
        assert_eq!(concrete.unwrap().name, "test");
    }

    #[test]
    fn test_invoke_method_options_default() {
        let options = InvokeMethodOptions::default();
        assert!(!options.one_way);
        assert!(!options.read_only);
        assert!(!options.always_interleave);
        assert!(!options.unordered);
    }

    #[test]
    fn test_invoke_method_options_one_way() {
        let options = InvokeMethodOptions::one_way();
        assert!(options.one_way);
        assert!(!options.read_only);
    }

    #[test]
    fn test_invoke_method_options_read_only() {
        let options = InvokeMethodOptions::read_only();
        assert!(!options.one_way);
        assert!(options.read_only);
    }

    #[test]
    fn test_invoke_method_options_always_interleave() {
        let options = InvokeMethodOptions::always_interleave();
        assert!(options.always_interleave);
    }
}
