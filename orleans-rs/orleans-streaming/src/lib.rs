//! # Orleans Streaming
//!
//! Reactive pub/sub messaging for distributed systems.
//!
//! Orleans Streaming provides a programming model for processing sequences of events.
//! Streams are virtual (always exist conceptually), support both implicit and explicit
//! subscriptions, and integrate with various queue backends for persistence.
//!
//! ## Core Concepts
//!
//! - **Stream**: A virtual sequence of events identified by namespace + key
//! - **Stream Provider**: Backend that manages stream transport/storage
//! - **Observer**: Consumer that receives stream events
//! - **Subscription**: Active link between a stream and an observer
//!
//! ## Example
//!
//! ```rust,no_run
//! use orleans_streaming::{
//!     MemoryStreamProvider, StreamId, IAsyncObserver, StreamSequenceToken,
//!     StreamError, StreamResult, IStreamProvider, StreamHandle,
//! };
//! use std::sync::Arc;
//! use async_trait::async_trait;
//!
//! #[derive(Debug)]
//! struct MyObserver;
//!
//! #[async_trait]
//! impl IAsyncObserver<serde_json::Value> for MyObserver {
//!     async fn on_next(
//!         &self,
//!         item: serde_json::Value,
//!         token: Option<StreamSequenceToken>,
//!     ) -> StreamResult<()> {
//!         println!("Received: {:?} at {:?}", item, token);
//!         Ok(())
//!     }
//!
//!     async fn on_completed(&self) -> StreamResult<()> {
//!         println!("Stream completed");
//!         Ok(())
//!     }
//!
//!     async fn on_error(&self, error: StreamError) -> StreamResult<()> {
//!         println!("Stream error: {}", error);
//!         Ok(())
//!     }
//! }
//!
//! async fn example() -> StreamResult<()> {
//!     // Create a memory stream provider
//!     let provider = Arc::new(MemoryStreamProvider::new("MyProvider"));
//!     provider.start().await?;
//!
//!     // Get a stream
//!     let stream_id = StreamId::create("orders", "customer-123");
//!     let stream = provider.get_stream(stream_id);
//!
//!     // Subscribe to events
//!     let observer = Arc::new(MyObserver);
//!     let handle = stream.subscribe_any(observer).await?;
//!
//!     // Publish events
//!     stream.on_next_any(serde_json::json!({"order_id": 1, "amount": 99.99})).await?;
//!     stream.on_next_any(serde_json::json!({"order_id": 2, "amount": 45.00})).await?;
//!
//!     // Unsubscribe when done
//!     handle.unsubscribe().await?;
//!
//!     provider.stop().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Stream Providers
//!
//! Orleans Streaming supports pluggable stream providers:
//!
//! - **MemoryStreamProvider**: In-memory streams for testing/development
//! - (Future) Persistent providers for Kafka, Redis Streams, etc.
//!
//! ## Subscription Types
//!
//! - **Explicit**: Programmatically subscribe using `stream.subscribe()`
//! - **Implicit**: Attribute-based subscriptions (grains auto-subscribe)

pub mod error;
pub mod memory_provider;
pub mod options;
pub mod provider;
pub mod stream_id;
pub mod subscription;
pub mod traits;

// Re-export main types
pub use error::{DeliveryStatus, StreamError, StreamResult};
pub use memory_provider::{MemoryStreamOptions, MemoryStreamProvider};
pub use options::{
    CacheEvictionStrategy, HashRingStreamQueueMapperOptions, StreamCacheEvictionOptions,
    StreamLifecycleOptions, StreamPubSubOptions, StreamPubSubType, StreamPullingAgentOptions,
};
pub use provider::{IStreamProvider, StreamHandle, StreamProviderDirection, StreamProviderRegistry};
pub use stream_id::{QualifiedStreamId, StreamId, StreamKey, StreamSequenceToken};
pub use subscription::{
    PubSubSubscriptionState, StreamSubscriptionHandle, SubscriptionLifecycle, SubscriptionMarker,
    SubscriptionState, SubscriptionType,
};
pub use traits::{
    AnyStreamItem, GenericAsyncObserver, IAsyncBatchObserver, IAsyncObserver, IAsyncStream,
    IInternalAsyncBatchObserver, IInternalAsyncObservable, StreamFilter, StreamItem,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug)]
    struct TestObserver {
        count: AtomicUsize,
    }

    impl TestObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl IAsyncObserver<serde_json::Value> for TestObserver {
        async fn on_next(
            &self,
            _item: serde_json::Value,
            _token: Option<StreamSequenceToken>,
        ) -> StreamResult<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
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
    async fn test_end_to_end_streaming() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer = Arc::new(TestObserver::new());
        let handle = stream.subscribe_any(observer.clone()).await.unwrap();

        // Publish some events
        for i in 0..5 {
            stream
                .on_next_any(serde_json::json!({"event": i}))
                .await
                .unwrap();
        }

        // Allow async delivery
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(observer.count.load(Ordering::SeqCst), 5);

        // Unsubscribe
        handle.unsubscribe().await.unwrap();

        // Stop provider
        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_provider_registry() {
        let registry = StreamProviderRegistry::new();
        assert!(registry.is_empty());

        let provider1 = Arc::new(MemoryStreamProvider::new("Provider1"));
        let provider2 = Arc::new(MemoryStreamProvider::new("Provider2"));

        registry.register(provider1.clone()).unwrap();
        registry.register(provider2.clone()).unwrap();

        assert_eq!(registry.len(), 2);
        assert!(registry.get("Provider1").is_some());
        assert!(registry.get("Provider2").is_some());
        assert!(registry.get("Provider3").is_none());

        // Can't register duplicate
        let dup = Arc::new(MemoryStreamProvider::new("Provider1"));
        assert!(registry.register(dup).is_err());

        // Remove provider
        registry.remove("Provider1");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn test_stream_id_creation() {
        let id = StreamId::create("orders", "order-123");
        assert_eq!(id.namespace(), "orders");
        assert_eq!(id.to_string(), "orders/order-123");

        let id2 = StreamId::with_integer("metrics", 42);
        assert!(matches!(id2.key(), StreamKey::Integer(42)));
    }

    #[test]
    fn test_sequence_token_ordering() {
        let t1 = StreamSequenceToken::new(0, 0);
        let t2 = StreamSequenceToken::new(0, 1);
        let t3 = StreamSequenceToken::new(1, 0);

        assert!(t1 < t2);
        assert!(t2 < t3);
        assert!(t1.older_than(&t2));
        assert!(t3.newer_than(&t1));
    }
}
