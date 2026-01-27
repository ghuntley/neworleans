//! In-memory stream provider.
//!
//! Provides an in-memory implementation of streams for testing and development.
//! Events are stored in memory and delivered immediately to subscribers.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

use crate::error::{StreamError, StreamResult};
use crate::provider::{IStreamProvider, StreamHandle, StreamProviderDirection};
use crate::stream_id::{StreamId, StreamSequenceToken};
use crate::subscription::{StreamSubscriptionHandle, SubscriptionLifecycle, SubscriptionMarker};
use crate::traits::IAsyncObserver;

/// Configuration for the memory stream provider.
#[derive(Clone, Debug)]
pub struct MemoryStreamOptions {
    /// Maximum messages to cache per stream.
    pub max_cache_size: usize,
    /// Maximum age of cached messages.
    pub message_retention: std::time::Duration,
}

impl Default for MemoryStreamOptions {
    fn default() -> Self {
        Self {
            max_cache_size: 1000,
            message_retention: std::time::Duration::from_secs(300), // 5 minutes
        }
    }
}

/// In-memory stream provider implementation.
pub struct MemoryStreamProvider {
    name: String,
    options: MemoryStreamOptions,
    /// Queues per stream.
    queues: Arc<DashMap<StreamId, MemoryStreamQueue>>,
    /// Subscriptions per stream.
    subscriptions: Arc<DashMap<StreamId, Vec<SubscriptionEntry>>>,
    /// Global subscription lookup.
    subscription_map: Arc<DashMap<Uuid, StreamId>>,
    /// Running state.
    is_running: AtomicBool,
}

/// Entry for a subscription.
struct SubscriptionEntry {
    subscription_id: Uuid,
    observer: Arc<dyn IAsyncObserver<serde_json::Value>>,
}

/// Queue for storing messages in a stream.
struct MemoryStreamQueue {
    messages: RwLock<VecDeque<MemoryMessage>>,
    sequence_counter: AtomicI64,
    completed: AtomicBool,
    error: RwLock<Option<String>>,
}

/// A message stored in the queue.
#[derive(Clone, Debug)]
struct MemoryMessage {
    data: serde_json::Value,
    sequence_token: StreamSequenceToken,
    timestamp: DateTime<Utc>,
}

impl MemoryStreamProvider {
    /// Create a new memory stream provider.
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_options(name, MemoryStreamOptions::default())
    }

    /// Create with custom options.
    pub fn with_options(name: impl Into<String>, options: MemoryStreamOptions) -> Self {
        Self {
            name: name.into(),
            options,
            queues: Arc::new(DashMap::new()),
            subscriptions: Arc::new(DashMap::new()),
            subscription_map: Arc::new(DashMap::new()),
            is_running: AtomicBool::new(false),
        }
    }

    /// Get or create a queue for a stream.
    fn get_or_create_queue(&self, stream_id: &StreamId) -> dashmap::mapref::one::Ref<'_, StreamId, MemoryStreamQueue> {
        if !self.queues.contains_key(stream_id) {
            self.queues.insert(
                stream_id.clone(),
                MemoryStreamQueue {
                    messages: RwLock::new(VecDeque::new()),
                    sequence_counter: AtomicI64::new(0),
                    completed: AtomicBool::new(false),
                    error: RwLock::new(None),
                },
            );
        }
        self.queues.get(stream_id).unwrap()
    }

    /// Publish an event to a stream.
    async fn publish_event(
        &self,
        stream_id: &StreamId,
        data: serde_json::Value,
    ) -> StreamResult<()> {
        if !self.is_running.load(Ordering::SeqCst) {
            return Err(StreamError::ShuttingDown);
        }

        let queue = self.get_or_create_queue(stream_id);

        if queue.completed.load(Ordering::SeqCst) {
            return Err(StreamError::StreamCompleted);
        }

        if let Some(err) = queue.error.read().as_ref() {
            return Err(StreamError::StreamErrored {
                message: err.clone(),
            });
        }

        let seq = queue.sequence_counter.fetch_add(1, Ordering::SeqCst);
        let token = StreamSequenceToken::new(seq, 0);

        let message = MemoryMessage {
            data: data.clone(),
            sequence_token: token.clone(),
            timestamp: Utc::now(),
        };

        // Cache the message
        {
            let mut messages = queue.messages.write();
            messages.push_back(message);

            // Trim if over max size
            while messages.len() > self.options.max_cache_size {
                messages.pop_front();
            }
        }

        // Drop the queue reference before notifying observers
        drop(queue);

        // Notify all subscribers
        self.notify_subscribers(stream_id, data, token).await;

        Ok(())
    }

    /// Notify all subscribers of a new event.
    async fn notify_subscribers(
        &self,
        stream_id: &StreamId,
        data: serde_json::Value,
        token: StreamSequenceToken,
    ) {
        if let Some(subs) = self.subscriptions.get(stream_id) {
            for entry in subs.iter() {
                if let Err(e) = entry.observer.on_next(data.clone(), Some(token.clone())).await {
                    tracing::warn!(
                        subscription_id = %entry.subscription_id,
                        stream_id = %stream_id,
                        error = %e,
                        "Failed to deliver event to observer"
                    );
                }
            }
        }
    }

    /// Notify all subscribers of completion.
    async fn notify_completion(&self, stream_id: &StreamId) {
        if let Some(subs) = self.subscriptions.get(stream_id) {
            for entry in subs.iter() {
                if let Err(e) = entry.observer.on_completed().await {
                    tracing::warn!(
                        subscription_id = %entry.subscription_id,
                        stream_id = %stream_id,
                        error = %e,
                        "Failed to deliver completion to observer"
                    );
                }
            }
        }
    }

    /// Notify all subscribers of an error.
    async fn notify_error(&self, stream_id: &StreamId, error: StreamError) {
        if let Some(subs) = self.subscriptions.get(stream_id) {
            for entry in subs.iter() {
                if let Err(e) = entry.observer.on_error(error.clone()).await {
                    tracing::warn!(
                        subscription_id = %entry.subscription_id,
                        stream_id = %stream_id,
                        error = %e,
                        "Failed to deliver error to observer"
                    );
                }
            }
        }
    }

    /// Add a subscription.
    fn add_subscription(
        &self,
        stream_id: StreamId,
        observer: Arc<dyn IAsyncObserver<serde_json::Value>>,
    ) -> StreamResult<Uuid> {
        if !self.is_running.load(Ordering::SeqCst) {
            return Err(StreamError::ShuttingDown);
        }

        let subscription_id = SubscriptionMarker::create_explicit();

        let entry = SubscriptionEntry {
            subscription_id,
            observer,
        };

        self.subscriptions
            .entry(stream_id.clone())
            .or_default()
            .push(entry);

        self.subscription_map.insert(subscription_id, stream_id);

        tracing::debug!(
            subscription_id = %subscription_id,
            "Added stream subscription"
        );

        Ok(subscription_id)
    }

    /// Remove a subscription.
    fn remove_subscription(&self, subscription_id: &Uuid) -> StreamResult<()> {
        if let Some((_, stream_id)) = self.subscription_map.remove(subscription_id) {
            if let Some(mut subs) = self.subscriptions.get_mut(&stream_id) {
                subs.retain(|e| e.subscription_id != *subscription_id);
            }

            tracing::debug!(
                subscription_id = %subscription_id,
                "Removed stream subscription"
            );

            Ok(())
        } else {
            Err(StreamError::subscription_not_found(subscription_id.to_string()))
        }
    }
}

#[async_trait]
impl IStreamProvider for MemoryStreamProvider {
    fn name(&self) -> &str {
        &self.name
    }

    fn is_rewindable(&self) -> bool {
        true // Memory provider caches messages
    }

    fn direction(&self) -> StreamProviderDirection {
        StreamProviderDirection::ReadWrite
    }

    fn get_stream(&self, stream_id: StreamId) -> Arc<dyn StreamHandle> {
        Arc::new(MemoryStream {
            stream_id,
            provider: Arc::new(self.clone()),
        })
    }

    async fn unsubscribe(&self, subscription_id: &Uuid) -> StreamResult<()> {
        self.remove_subscription(subscription_id)
    }

    async fn start(&self) -> StreamResult<()> {
        tracing::info!(provider = %self.name, "Starting memory stream provider");
        self.is_running.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn stop(&self) -> StreamResult<()> {
        tracing::info!(provider = %self.name, "Stopping memory stream provider");
        self.is_running.store(false, Ordering::SeqCst);

        // Clear all subscriptions
        self.subscriptions.clear();
        self.subscription_map.clear();

        Ok(())
    }
}

impl Clone for MemoryStreamProvider {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            options: self.options.clone(),
            queues: self.queues.clone(),
            subscriptions: self.subscriptions.clone(),
            subscription_map: self.subscription_map.clone(),
            is_running: AtomicBool::new(self.is_running.load(Ordering::SeqCst)),
        }
    }
}

/// Stream handle for memory provider.
struct MemoryStream {
    stream_id: StreamId,
    provider: Arc<MemoryStreamProvider>,
}

#[async_trait]
impl StreamHandle for MemoryStream {
    fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    fn provider_name(&self) -> &str {
        self.provider.name()
    }

    fn is_rewindable(&self) -> bool {
        true
    }

    async fn subscribe_any(
        &self,
        observer: Arc<dyn IAsyncObserver<serde_json::Value>>,
    ) -> StreamResult<StreamSubscriptionHandle> {
        let subscription_id = self.provider.add_subscription(self.stream_id.clone(), observer)?;

        let unsubscriber = Arc::new(MemoryUnsubscriber {
            provider: self.provider.clone(),
        });

        Ok(StreamSubscriptionHandle::new(
            subscription_id,
            self.stream_id.clone(),
            self.provider.name(),
            unsubscriber,
        ))
    }

    async fn on_next_any(&self, item: serde_json::Value) -> StreamResult<()> {
        self.provider.publish_event(&self.stream_id, item).await
    }

    async fn on_next_batch_any(&self, items: Vec<serde_json::Value>) -> StreamResult<()> {
        for item in items {
            self.provider.publish_event(&self.stream_id, item).await?;
        }
        Ok(())
    }

    async fn on_completed(&self) -> StreamResult<()> {
        // Get or create queue to ensure completed flag is set
        let queue = self.provider.get_or_create_queue(&self.stream_id);
        queue.completed.store(true, Ordering::SeqCst);
        drop(queue);
        self.provider.notify_completion(&self.stream_id).await;
        Ok(())
    }

    async fn on_error(&self, error: StreamError) -> StreamResult<()> {
        // Get or create queue to ensure error is set
        let queue = self.provider.get_or_create_queue(&self.stream_id);
        *queue.error.write() = Some(error.to_string());
        drop(queue);
        self.provider.notify_error(&self.stream_id, error).await;
        Ok(())
    }

    async fn get_all_subscription_handles(&self) -> StreamResult<Vec<StreamSubscriptionHandle>> {
        let unsubscriber = Arc::new(MemoryUnsubscriber {
            provider: self.provider.clone(),
        });

        let mut handles = Vec::new();
        if let Some(subs) = self.provider.subscriptions.get(&self.stream_id) {
            for entry in subs.iter() {
                handles.push(StreamSubscriptionHandle::new(
                    entry.subscription_id,
                    self.stream_id.clone(),
                    self.provider.name(),
                    unsubscriber.clone(),
                ));
            }
        }
        Ok(handles)
    }
}

/// Unsubscriber for memory streams.
struct MemoryUnsubscriber {
    provider: Arc<MemoryStreamProvider>,
}

#[async_trait]
impl SubscriptionLifecycle for MemoryUnsubscriber {
    async fn unsubscribe(&self, subscription_id: &Uuid) -> StreamResult<()> {
        self.provider.remove_subscription(subscription_id)
    }
}

// Make provider thread-safe for Debug
impl std::fmt::Debug for MemoryStreamProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStreamProvider")
            .field("name", &self.name)
            .field("options", &self.options)
            .field("is_running", &self.is_running.load(Ordering::SeqCst))
            .field("stream_count", &self.queues.len())
            .field("subscription_count", &self.subscription_map.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[derive(Debug)]
    struct CountingObserver {
        count: AtomicUsize,
        events: parking_lot::Mutex<Vec<serde_json::Value>>,
    }

    impl CountingObserver {
        fn new() -> Self {
            Self {
                count: AtomicUsize::new(0),
                events: parking_lot::Mutex::new(Vec::new()),
            }
        }

        fn event_count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }

        fn events(&self) -> Vec<serde_json::Value> {
            self.events.lock().clone()
        }
    }

    #[async_trait]
    impl IAsyncObserver<serde_json::Value> for CountingObserver {
        async fn on_next(
            &self,
            item: serde_json::Value,
            _token: Option<StreamSequenceToken>,
        ) -> StreamResult<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            self.events.lock().push(item);
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
    async fn test_memory_provider_lifecycle() {
        let provider = MemoryStreamProvider::new("TestProvider");

        assert_eq!(provider.name(), "TestProvider");
        assert!(provider.is_rewindable());
        assert_eq!(provider.direction(), StreamProviderDirection::ReadWrite);

        provider.start().await.unwrap();
        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_publish_subscribe() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer = Arc::new(CountingObserver::new());
        let _handle = stream.subscribe_any(observer.clone()).await.unwrap();

        stream
            .on_next_any(serde_json::json!({"event": 1}))
            .await
            .unwrap();
        stream
            .on_next_any(serde_json::json!({"event": 2}))
            .await
            .unwrap();

        // Give async delivery time to complete
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(observer.event_count(), 2);

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer1 = Arc::new(CountingObserver::new());
        let observer2 = Arc::new(CountingObserver::new());

        let _handle1 = stream.subscribe_any(observer1.clone()).await.unwrap();
        let _handle2 = stream.subscribe_any(observer2.clone()).await.unwrap();

        stream
            .on_next_any(serde_json::json!({"event": "test"}))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(observer1.event_count(), 1);
        assert_eq!(observer2.event_count(), 1);

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_unsubscribe() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer = Arc::new(CountingObserver::new());
        let handle = stream.subscribe_any(observer.clone()).await.unwrap();

        stream
            .on_next_any(serde_json::json!({"event": 1}))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert_eq!(observer.event_count(), 1);

        handle.unsubscribe().await.unwrap();

        stream
            .on_next_any(serde_json::json!({"event": 2}))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Should still be 1 since we unsubscribed
        assert_eq!(observer.event_count(), 1);

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_completion() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        stream.on_completed().await.unwrap();

        // Should fail to publish after completion
        let result = stream.on_next_any(serde_json::json!({"event": 1})).await;
        assert!(matches!(result, Err(StreamError::StreamCompleted)));

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_publish() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer = Arc::new(CountingObserver::new());
        let _handle = stream.subscribe_any(observer.clone()).await.unwrap();

        let batch = vec![
            serde_json::json!({"event": 1}),
            serde_json::json!({"event": 2}),
            serde_json::json!({"event": 3}),
        ];

        stream.on_next_batch_any(batch).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        assert_eq!(observer.event_count(), 3);

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_subscription_handles() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        let observer1 = Arc::new(CountingObserver::new());
        let observer2 = Arc::new(CountingObserver::new());

        let _handle1 = stream.subscribe_any(observer1).await.unwrap();
        let _handle2 = stream.subscribe_any(observer2).await.unwrap();

        let handles = stream.get_all_subscription_handles().await.unwrap();
        assert_eq!(handles.len(), 2);

        provider.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_provider_shutdown_blocks_operations() {
        let provider = Arc::new(MemoryStreamProvider::new("TestProvider"));
        provider.start().await.unwrap();
        provider.stop().await.unwrap();

        let stream_id = StreamId::create("test-ns", "test-key");
        let stream = provider.get_stream(stream_id.clone());

        // Should fail since provider is stopped
        let result = stream.on_next_any(serde_json::json!({"event": 1})).await;
        assert!(matches!(result, Err(StreamError::ShuttingDown)));

        let observer = Arc::new(CountingObserver::new());
        let result = stream.subscribe_any(observer).await;
        assert!(matches!(result, Err(StreamError::ShuttingDown)));
    }
}
