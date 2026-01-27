//! Stream subscription management.
//!
//! Subscription handles allow consumers to manage their stream subscriptions,
//! including unsubscribing and resuming from checkpoints.

use std::fmt;
use std::sync::Arc;

use uuid::Uuid;

use crate::error::StreamResult;
use crate::stream_id::{StreamId, StreamSequenceToken};

/// Handle to an active stream subscription.
///
/// Use this handle to unsubscribe from a stream or resume a subscription
/// from a checkpoint.
#[derive(Clone)]
pub struct StreamSubscriptionHandle {
    /// Unique subscription identifier.
    subscription_id: Uuid,
    /// The stream this subscription is for.
    stream_id: StreamId,
    /// Provider name for routing.
    provider_name: String,
    /// Internal unsubscribe callback.
    unsubscriber: Arc<dyn SubscriptionLifecycle>,
}

impl StreamSubscriptionHandle {
    /// Create a new subscription handle.
    pub fn new(
        subscription_id: Uuid,
        stream_id: StreamId,
        provider_name: impl Into<String>,
        unsubscriber: Arc<dyn SubscriptionLifecycle>,
    ) -> Self {
        Self {
            subscription_id,
            stream_id,
            provider_name: provider_name.into(),
            unsubscriber,
        }
    }

    /// Get the subscription ID.
    pub fn subscription_id(&self) -> &Uuid {
        &self.subscription_id
    }

    /// Get the stream ID.
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    /// Get the provider name.
    pub fn provider_name(&self) -> &str {
        &self.provider_name
    }

    /// Unsubscribe from the stream.
    ///
    /// After unsubscribing, the observer will no longer receive events.
    pub async fn unsubscribe(&self) -> StreamResult<()> {
        self.unsubscriber.unsubscribe(&self.subscription_id).await
    }
}

impl fmt::Debug for StreamSubscriptionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamSubscriptionHandle")
            .field("subscription_id", &self.subscription_id)
            .field("stream_id", &self.stream_id)
            .field("provider_name", &self.provider_name)
            .finish()
    }
}

impl PartialEq for StreamSubscriptionHandle {
    fn eq(&self, other: &Self) -> bool {
        self.subscription_id == other.subscription_id
    }
}

impl Eq for StreamSubscriptionHandle {}

impl std::hash::Hash for StreamSubscriptionHandle {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.subscription_id.hash(state);
    }
}

/// Internal trait for subscription lifecycle management.
#[async_trait::async_trait]
pub trait SubscriptionLifecycle: Send + Sync {
    /// Unsubscribe a subscription.
    async fn unsubscribe(&self, subscription_id: &Uuid) -> StreamResult<()>;
}

/// Subscription state for internal tracking.
#[derive(Clone, Debug)]
pub struct SubscriptionState {
    /// Unique subscription ID.
    pub subscription_id: Uuid,
    /// Stream identifier.
    pub stream_id: StreamId,
    /// Last delivered sequence token.
    pub last_token: Option<StreamSequenceToken>,
    /// Filter data (if any).
    pub filter_data: Option<Vec<u8>>,
    /// When the subscription was created.
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Whether the subscription is active.
    pub is_active: bool,
}

impl SubscriptionState {
    /// Create new subscription state.
    pub fn new(subscription_id: Uuid, stream_id: StreamId) -> Self {
        Self {
            subscription_id,
            stream_id,
            last_token: None,
            filter_data: None,
            created_at: chrono::Utc::now(),
            is_active: true,
        }
    }

    /// Update the last delivered token.
    pub fn update_token(&mut self, token: StreamSequenceToken) {
        self.last_token = Some(token);
    }

    /// Mark the subscription as inactive.
    pub fn deactivate(&mut self) {
        self.is_active = false;
    }
}

/// Marker for identifying implicit vs explicit subscriptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionType {
    /// Explicit subscription (programmatically created).
    Explicit,
    /// Implicit subscription (attribute-based).
    Implicit,
}

impl SubscriptionType {
    /// Check if this is an implicit subscription based on the subscription ID.
    ///
    /// Implicit subscription IDs have a specific bit pattern.
    pub fn from_subscription_id(id: &Uuid) -> Self {
        // Check the version bits to determine subscription type
        // Version 4 = explicit, Version 5 = implicit
        let version = (id.as_bytes()[6] >> 4) & 0x0F;
        if version == 5 {
            Self::Implicit
        } else {
            Self::Explicit
        }
    }
}

/// Marker utility for creating implicit subscription IDs.
pub struct SubscriptionMarker;

impl SubscriptionMarker {
    /// Create an implicit subscription ID from stream and grain IDs.
    pub fn create_implicit(stream_id: &StreamId, grain_id: &orleans_core::GrainId) -> Uuid {
        // Use UUID v5 (namespace-based) for implicit subscriptions
        let namespace = Uuid::NAMESPACE_OID;
        let name = format!("{}-{}", stream_id, grain_id);
        Uuid::new_v5(&namespace, name.as_bytes())
    }

    /// Check if a subscription ID is implicit.
    pub fn is_implicit(id: &Uuid) -> bool {
        SubscriptionType::from_subscription_id(id) == SubscriptionType::Implicit
    }

    /// Create an explicit subscription ID (random UUID v4).
    pub fn create_explicit() -> Uuid {
        Uuid::new_v4()
    }
}

/// Pub/Sub subscription state for tracking consumers.
#[derive(Clone, Debug)]
pub struct PubSubSubscriptionState {
    /// The subscription ID.
    pub subscription_id: Uuid,
    /// The consumer grain ID.
    pub consumer: orleans_core::GrainId,
    /// Optional filter.
    pub filter: Option<Vec<u8>>,
}

impl PubSubSubscriptionState {
    /// Create new pub/sub subscription state.
    pub fn new(subscription_id: Uuid, consumer: orleans_core::GrainId) -> Self {
        Self {
            subscription_id,
            consumer,
            filter: None,
        }
    }

    /// Create with a filter.
    pub fn with_filter(
        subscription_id: Uuid,
        consumer: orleans_core::GrainId,
        filter: Vec<u8>,
    ) -> Self {
        Self {
            subscription_id,
            consumer,
            filter: Some(filter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockUnsubscriber;

    #[async_trait::async_trait]
    impl SubscriptionLifecycle for MockUnsubscriber {
        async fn unsubscribe(&self, _subscription_id: &Uuid) -> StreamResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_subscription_handle_creation() {
        let stream_id = StreamId::create("ns", "key");
        let handle = StreamSubscriptionHandle::new(
            Uuid::new_v4(),
            stream_id.clone(),
            "MemoryProvider",
            Arc::new(MockUnsubscriber),
        );

        assert_eq!(handle.stream_id(), &stream_id);
        assert_eq!(handle.provider_name(), "MemoryProvider");
    }

    #[test]
    fn test_subscription_handle_equality() {
        let id = Uuid::new_v4();
        let stream_id = StreamId::create("ns", "key");

        let handle1 = StreamSubscriptionHandle::new(
            id,
            stream_id.clone(),
            "Provider",
            Arc::new(MockUnsubscriber),
        );

        let handle2 = StreamSubscriptionHandle::new(
            id,
            stream_id.clone(),
            "Provider",
            Arc::new(MockUnsubscriber),
        );

        assert_eq!(handle1, handle2);

        let handle3 = StreamSubscriptionHandle::new(
            Uuid::new_v4(),
            stream_id,
            "Provider",
            Arc::new(MockUnsubscriber),
        );

        assert_ne!(handle1, handle3);
    }

    #[test]
    fn test_subscription_state() {
        let stream_id = StreamId::create("ns", "key");
        let mut state = SubscriptionState::new(Uuid::new_v4(), stream_id);

        assert!(state.is_active);
        assert!(state.last_token.is_none());

        state.update_token(StreamSequenceToken::new(5, 2));
        assert_eq!(state.last_token, Some(StreamSequenceToken::new(5, 2)));

        state.deactivate();
        assert!(!state.is_active);
    }

    #[test]
    fn test_subscription_marker_explicit() {
        let id = SubscriptionMarker::create_explicit();
        assert!(!SubscriptionMarker::is_implicit(&id));
        assert_eq!(
            SubscriptionType::from_subscription_id(&id),
            SubscriptionType::Explicit
        );
    }

    #[test]
    fn test_subscription_marker_implicit() {
        let stream_id = StreamId::create("ns", "key");
        let grain_id =
            orleans_core::GrainId::new(orleans_core::GrainType::create("TestGrain"), "test-key".into());

        let id = SubscriptionMarker::create_implicit(&stream_id, &grain_id);
        assert!(SubscriptionMarker::is_implicit(&id));
        assert_eq!(
            SubscriptionType::from_subscription_id(&id),
            SubscriptionType::Implicit
        );

        // Same inputs produce same ID (deterministic)
        let id2 = SubscriptionMarker::create_implicit(&stream_id, &grain_id);
        assert_eq!(id, id2);
    }

    #[test]
    fn test_pubsub_subscription_state() {
        let grain_id =
            orleans_core::GrainId::new(orleans_core::GrainType::create("TestGrain"), "test-key".into());

        let state = PubSubSubscriptionState::new(Uuid::new_v4(), grain_id.clone());
        assert!(state.filter.is_none());

        let state_with_filter =
            PubSubSubscriptionState::with_filter(Uuid::new_v4(), grain_id, vec![1, 2, 3]);
        assert_eq!(state_with_filter.filter, Some(vec![1, 2, 3]));
    }
}
