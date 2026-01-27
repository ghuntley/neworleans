//! Core streaming traits.
//!
//! Defines the interfaces for stream consumers (observers) and producers.

use async_trait::async_trait;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use crate::error::{StreamError, StreamResult};
use crate::stream_id::{StreamId, StreamSequenceToken};
use crate::subscription::StreamSubscriptionHandle;

/// Observer interface for receiving stream events one at a time.
///
/// Implement this trait to handle individual events from a stream.
#[async_trait]
pub trait IAsyncObserver<T>: Send + Sync + Debug {
    /// Called when a new event is received.
    ///
    /// # Arguments
    /// * `item` - The event data
    /// * `token` - Optional sequence token for checkpointing
    async fn on_next(&self, item: T, token: Option<StreamSequenceToken>) -> StreamResult<()>;

    /// Called when the stream completes normally.
    async fn on_completed(&self) -> StreamResult<()>;

    /// Called when the stream encounters an error.
    async fn on_error(&self, error: StreamError) -> StreamResult<()>;
}

/// Observer interface for receiving stream events in batches.
///
/// Implement this trait for higher throughput event processing.
#[async_trait]
pub trait IAsyncBatchObserver<T>: Send + Sync + Debug {
    /// Called when a batch of events is received.
    ///
    /// # Arguments
    /// * `items` - The batch of events with their sequence tokens
    async fn on_next_batch(&self, items: Vec<(T, StreamSequenceToken)>) -> StreamResult<()>;

    /// Called when the stream completes normally.
    async fn on_completed(&self) -> StreamResult<()>;

    /// Called when the stream encounters an error.
    async fn on_error(&self, error: StreamError) -> StreamResult<()>;
}

/// Stream filter for selective event delivery.
#[derive(Clone, Debug)]
pub struct StreamFilter {
    /// Filter data serialized for transmission.
    pub filter_data: Vec<u8>,
}

impl StreamFilter {
    /// Create a new stream filter.
    pub fn new(filter_data: Vec<u8>) -> Self {
        Self { filter_data }
    }

    /// Create an empty (pass-all) filter.
    pub fn empty() -> Self {
        Self {
            filter_data: Vec::new(),
        }
    }

    /// Check if this filter is empty.
    pub fn is_empty(&self) -> bool {
        self.filter_data.is_empty()
    }
}

/// Main stream interface for both producing and consuming events.
///
/// Provides methods for subscribing to receive events and publishing events.
#[async_trait]
pub trait IAsyncStream<T: Clone + Send + Sync + 'static>: Send + Sync {
    /// Get the stream identifier.
    fn stream_id(&self) -> &StreamId;

    /// Get the provider name.
    fn provider_name(&self) -> &str;

    /// Check if this stream supports rewinding to past events.
    fn is_rewindable(&self) -> bool;

    // ========== Consumer operations ==========

    /// Subscribe to receive events from this stream.
    async fn subscribe(
        &self,
        observer: Arc<dyn IAsyncObserver<T>>,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Subscribe starting from a specific sequence token.
    ///
    /// Only works if the stream is rewindable.
    async fn subscribe_from(
        &self,
        observer: Arc<dyn IAsyncObserver<T>>,
        token: StreamSequenceToken,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Subscribe with a filter for selective event delivery.
    async fn subscribe_with_filter(
        &self,
        observer: Arc<dyn IAsyncObserver<T>>,
        filter: StreamFilter,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Get all active subscription handles for this stream.
    async fn get_all_subscription_handles(&self) -> StreamResult<Vec<StreamSubscriptionHandle>>;

    // ========== Producer operations ==========

    /// Publish a single event to this stream.
    async fn on_next(&self, item: T) -> StreamResult<()>;

    /// Publish a batch of events to this stream.
    async fn on_next_batch(&self, items: Vec<T>) -> StreamResult<()>;

    /// Signal that the stream has completed.
    async fn on_completed(&self) -> StreamResult<()>;

    /// Signal that the stream has encountered an error.
    async fn on_error(&self, error: StreamError) -> StreamResult<()>;
}

/// Internal interface for stream observables (consumer side).
#[async_trait]
pub trait IInternalAsyncObservable<T>: Send + Sync {
    /// Subscribe an observer.
    async fn subscribe(
        &self,
        observer: Arc<dyn IAsyncObserver<T>>,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Subscribe from a specific token.
    async fn subscribe_from(
        &self,
        observer: Arc<dyn IAsyncObserver<T>>,
        token: StreamSequenceToken,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Unsubscribe.
    async fn unsubscribe(&self, subscription_id: &uuid::Uuid) -> StreamResult<()>;

    /// Get all handles.
    async fn get_all_subscription_handles(&self) -> StreamResult<Vec<StreamSubscriptionHandle>>;
}

/// Internal interface for stream batch observers (producer side).
#[async_trait]
pub trait IInternalAsyncBatchObserver<T>: Send + Sync {
    /// Publish a single event.
    async fn on_next(&self, item: T) -> StreamResult<()>;

    /// Publish a batch.
    async fn on_next_batch(&self, items: Vec<T>) -> StreamResult<()>;

    /// Signal completion.
    async fn on_completed(&self) -> StreamResult<()>;

    /// Signal error.
    async fn on_error(&self, error: StreamError) -> StreamResult<()>;
}

/// Marker trait for types that can be streamed.
pub trait StreamItem: Clone + Send + Sync + 'static {}

impl<T: Clone + Send + Sync + 'static> StreamItem for T {}

/// Trait for type-erased stream items.
pub trait AnyStreamItem: Send + Sync + Any + Debug {
    /// Get as Any for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Clone into a boxed trait object.
    fn clone_box(&self) -> Box<dyn AnyStreamItem>;
}

impl<T: Clone + Send + Sync + Any + Debug + 'static> AnyStreamItem for T {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn AnyStreamItem> {
        Box::new(self.clone())
    }
}

// Note: We intentionally do NOT implement Clone for Box<dyn AnyStreamItem>
// because Box<dyn AnyStreamItem> itself satisfies the bounds for the blanket
// AnyStreamItem impl, which would cause infinite recursion when cloning.
// Use clone_box() directly instead.

/// Generic async observer that wraps a callback function.
pub struct GenericAsyncObserver<T, F>
where
    F: Fn(T, Option<StreamSequenceToken>) -> futures::future::BoxFuture<'static, StreamResult<()>>
        + Send
        + Sync,
{
    callback: F,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, F> GenericAsyncObserver<T, F>
where
    F: Fn(T, Option<StreamSequenceToken>) -> futures::future::BoxFuture<'static, StreamResult<()>>
        + Send
        + Sync,
{
    /// Create a new generic observer from a callback.
    pub fn new(callback: F) -> Self {
        Self {
            callback,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, F> Debug for GenericAsyncObserver<T, F>
where
    F: Fn(T, Option<StreamSequenceToken>) -> futures::future::BoxFuture<'static, StreamResult<()>>
        + Send
        + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GenericAsyncObserver").finish()
    }
}

#[async_trait]
impl<T, F> IAsyncObserver<T> for GenericAsyncObserver<T, F>
where
    T: Send + Sync + 'static,
    F: Fn(T, Option<StreamSequenceToken>) -> futures::future::BoxFuture<'static, StreamResult<()>>
        + Send
        + Sync,
{
    async fn on_next(&self, item: T, token: Option<StreamSequenceToken>) -> StreamResult<()> {
        (self.callback)(item, token).await
    }

    async fn on_completed(&self) -> StreamResult<()> {
        Ok(())
    }

    async fn on_error(&self, _error: StreamError) -> StreamResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestObserver {
        received: Arc<parking_lot::Mutex<Vec<String>>>,
    }

    impl TestObserver {
        fn new() -> Self {
            Self {
                received: Arc::new(parking_lot::Mutex::new(Vec::new())),
            }
        }

        fn received(&self) -> Vec<String> {
            self.received.lock().clone()
        }
    }

    #[async_trait]
    impl IAsyncObserver<String> for TestObserver {
        async fn on_next(
            &self,
            item: String,
            _token: Option<StreamSequenceToken>,
        ) -> StreamResult<()> {
            self.received.lock().push(item);
            Ok(())
        }

        async fn on_completed(&self) -> StreamResult<()> {
            Ok(())
        }

        async fn on_error(&self, _error: StreamError) -> StreamResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_async_observer() {
        let observer = TestObserver::new();
        observer
            .on_next("event1".to_string(), None)
            .await
            .unwrap();
        observer
            .on_next("event2".to_string(), Some(StreamSequenceToken::new(1, 0)))
            .await
            .unwrap();

        assert_eq!(observer.received(), vec!["event1", "event2"]);
    }

    #[test]
    fn test_stream_filter() {
        let filter = StreamFilter::new(vec![1, 2, 3]);
        assert!(!filter.is_empty());
        assert_eq!(filter.filter_data, vec![1, 2, 3]);

        let empty_filter = StreamFilter::empty();
        assert!(empty_filter.is_empty());
    }

    #[test]
    fn test_any_stream_item() {
        let item: Box<dyn AnyStreamItem> = Box::new("test".to_string());
        let cloned = item.clone_box();

        let downcasted = item.as_any().downcast_ref::<String>();
        assert!(downcasted.is_some());
        assert_eq!(downcasted.unwrap(), "test");

        let cloned_downcasted = cloned.as_any().downcast_ref::<String>();
        assert!(cloned_downcasted.is_some());
    }
}
