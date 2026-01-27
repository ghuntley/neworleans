//! Stream provider infrastructure.
//!
//! Stream providers are responsible for managing the underlying transport
//! and storage for streams. Multiple providers can be registered in a silo,
//! each with a unique name.

use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

use crate::error::StreamResult;
use crate::stream_id::StreamId;
use crate::subscription::StreamSubscriptionHandle;
use crate::traits::IAsyncObserver;

/// Direction of a stream provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum StreamProviderDirection {
    /// Provider only supports reading (consuming).
    ReadOnly,
    /// Provider only supports writing (producing).
    WriteOnly,
    /// Provider supports both reading and writing.
    ReadWrite,
}

impl StreamProviderDirection {
    /// Check if this direction supports reading.
    pub fn can_read(&self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite)
    }

    /// Check if this direction supports writing.
    pub fn can_write(&self) -> bool {
        matches!(self, Self::WriteOnly | Self::ReadWrite)
    }
}

impl fmt::Display for StreamProviderDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly => write!(f, "ReadOnly"),
            Self::WriteOnly => write!(f, "WriteOnly"),
            Self::ReadWrite => write!(f, "ReadWrite"),
        }
    }
}

/// Core stream provider interface.
///
/// Stream providers manage the lifecycle and routing of streams.
/// Each provider has a unique name and can create streams.
#[async_trait]
pub trait IStreamProvider: Send + Sync {
    /// Get the provider name.
    fn name(&self) -> &str;

    /// Check if this provider supports rewinding to past events.
    fn is_rewindable(&self) -> bool;

    /// Get the provider direction.
    fn direction(&self) -> StreamProviderDirection;

    /// Get a stream by ID.
    ///
    /// This returns a stream handle for the given ID. The stream is virtual
    /// and always "exists" - calling this doesn't necessarily create any
    /// resources until events are published or subscriptions are made.
    fn get_stream(&self, stream_id: StreamId) -> Arc<dyn StreamHandle>;

    /// Unsubscribe a subscription.
    async fn unsubscribe(&self, subscription_id: &uuid::Uuid) -> StreamResult<()>;

    /// Start the provider.
    async fn start(&self) -> StreamResult<()>;

    /// Stop the provider.
    async fn stop(&self) -> StreamResult<()>;
}

/// Type-erased stream handle for provider-independent access.
#[async_trait]
pub trait StreamHandle: Send + Sync {
    /// Get the stream ID.
    fn stream_id(&self) -> &StreamId;

    /// Get the provider name.
    fn provider_name(&self) -> &str;

    /// Check if rewindable.
    fn is_rewindable(&self) -> bool;

    /// Subscribe with a type-erased observer.
    async fn subscribe_any(
        &self,
        observer: Arc<dyn IAsyncObserver<serde_json::Value>>,
    ) -> StreamResult<StreamSubscriptionHandle>;

    /// Publish a type-erased event.
    async fn on_next_any(&self, item: serde_json::Value) -> StreamResult<()>;

    /// Publish a batch of type-erased events.
    async fn on_next_batch_any(&self, items: Vec<serde_json::Value>) -> StreamResult<()>;

    /// Signal completion.
    async fn on_completed(&self) -> StreamResult<()>;

    /// Signal error.
    async fn on_error(&self, error: crate::error::StreamError) -> StreamResult<()>;

    /// Get all subscription handles.
    async fn get_all_subscription_handles(&self) -> StreamResult<Vec<StreamSubscriptionHandle>>;
}

/// Registry of stream providers.
pub struct StreamProviderRegistry {
    providers: dashmap::DashMap<String, Arc<dyn IStreamProvider>>,
}

impl StreamProviderRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            providers: dashmap::DashMap::new(),
        }
    }

    /// Register a stream provider.
    pub fn register(&self, provider: Arc<dyn IStreamProvider>) -> StreamResult<()> {
        let name = provider.name().to_string();
        if self.providers.contains_key(&name) {
            return Err(crate::error::StreamError::ProviderAlreadyRegistered {
                provider_name: name,
            });
        }
        self.providers.insert(name, provider);
        Ok(())
    }

    /// Get a provider by name.
    pub fn get(&self, name: &str) -> Option<Arc<dyn IStreamProvider>> {
        self.providers.get(name).map(|r| r.clone())
    }

    /// Get all provider names.
    pub fn provider_names(&self) -> Vec<String> {
        self.providers.iter().map(|r| r.key().clone()).collect()
    }

    /// Get the number of registered providers.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// Check if the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }

    /// Remove a provider by name.
    pub fn remove(&self, name: &str) -> Option<Arc<dyn IStreamProvider>> {
        self.providers.remove(name).map(|(_, v)| v)
    }

    /// Start all providers.
    pub async fn start_all(&self) -> StreamResult<()> {
        for provider in self.providers.iter() {
            tracing::info!(provider = %provider.key(), "Starting stream provider");
            provider.value().start().await?;
        }
        Ok(())
    }

    /// Stop all providers.
    pub async fn stop_all(&self) -> StreamResult<()> {
        for provider in self.providers.iter() {
            tracing::info!(provider = %provider.key(), "Stopping stream provider");
            if let Err(e) = provider.value().stop().await {
                tracing::error!(provider = %provider.key(), error = %e, "Failed to stop provider");
            }
        }
        Ok(())
    }
}

impl Default for StreamProviderRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for StreamProviderRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamProviderRegistry")
            .field("providers", &self.provider_names())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_provider_direction() {
        let read_only = StreamProviderDirection::ReadOnly;
        assert!(read_only.can_read());
        assert!(!read_only.can_write());

        let write_only = StreamProviderDirection::WriteOnly;
        assert!(!write_only.can_read());
        assert!(write_only.can_write());

        let read_write = StreamProviderDirection::ReadWrite;
        assert!(read_write.can_read());
        assert!(read_write.can_write());
    }

    #[test]
    fn test_stream_provider_direction_display() {
        assert_eq!(StreamProviderDirection::ReadOnly.to_string(), "ReadOnly");
        assert_eq!(StreamProviderDirection::WriteOnly.to_string(), "WriteOnly");
        assert_eq!(StreamProviderDirection::ReadWrite.to_string(), "ReadWrite");
    }

    #[test]
    fn test_registry_empty() {
        let registry = StreamProviderRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.get("test").is_none());
    }
}
