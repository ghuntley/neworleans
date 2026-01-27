//! Activation data structures.
//!
//! An activation represents a live instance of a grain on a specific silo.
//! This module provides the data structures for managing activations,
//! including state management, message queuing, and lifecycle.

use chrono::{DateTime, Utc};
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, SiloAddress};
use orleans_messaging::Message;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};

use crate::activation_state::{ActivationState, DeactivationReason};
use crate::error::{RuntimeError, RuntimeResult};
use crate::grain::{GrainTypeData, IGrainMethodInvoker};
use crate::grain_context::GrainContext;

/// A pending message waiting to be processed by the grain.
pub struct PendingMessage {
    /// The incoming message.
    pub message: Message,

    /// Channel to send the response.
    pub response_tx: Option<oneshot::Sender<Message>>,

    /// When the message was received.
    pub received_at: Instant,
}

impl PendingMessage {
    /// Create a new pending message.
    pub fn new(message: Message, response_tx: Option<oneshot::Sender<Message>>) -> Self {
        Self {
            message,
            response_tx,
            received_at: Instant::now(),
        }
    }

    /// Check if the message has timed out.
    pub fn is_expired(&self) -> bool {
        if let Some(timeout) = self.message.timeout() {
            self.received_at.elapsed() > timeout
        } else {
            false
        }
    }
}

/// Statistics about an activation.
#[derive(Debug, Default, Clone)]
pub struct ActivationStats {
    /// Number of messages received.
    pub messages_received: u64,

    /// Number of messages successfully processed.
    pub messages_processed: u64,

    /// Number of messages that failed.
    pub messages_failed: u64,

    /// Total processing time in microseconds.
    pub total_processing_time_us: u64,
}

/// An activation of a grain on this silo.
pub struct ActivationData {
    /// The grain's identity.
    grain_id: GrainId,

    /// The grain type.
    grain_type: GrainType,

    /// The unique activation ID.
    activation_id: ActivationId,

    /// The silo hosting this activation.
    silo_address: SiloAddress,

    /// The complete grain address.
    address: GrainAddress,

    /// The grain instance (type-erased).
    grain: RwLock<Option<Box<dyn std::any::Any + Send + Sync>>>,

    /// The grain context.
    context: Arc<GrainContext>,

    /// The grain type data (invokers, etc.).
    grain_type_data: Arc<GrainTypeData>,

    /// Current activation state.
    state: RwLock<ActivationState>,

    /// Reason for deactivation (if deactivating).
    deactivation_reason: RwLock<Option<DeactivationReason>>,

    /// Queue of pending messages.
    message_queue: Mutex<VecDeque<PendingMessage>>,

    /// Channel to send messages to the activation's worker.
    message_tx: mpsc::UnboundedSender<PendingMessage>,

    /// Whether the worker is currently processing.
    is_processing: AtomicBool,

    /// Activation creation time.
    created_at: DateTime<Utc>,

    /// Last message processing time.
    last_activity: RwLock<Instant>,

    /// Statistics.
    stats: RwLock<ActivationStats>,

    /// Reference count for tracking outstanding calls.
    outstanding_calls: AtomicU64,
}

impl ActivationData {
    /// Create a new activation.
    pub fn new(
        grain_id: GrainId,
        grain_type: GrainType,
        activation_id: ActivationId,
        silo_address: SiloAddress,
        context: Arc<GrainContext>,
        grain_type_data: Arc<GrainTypeData>,
        message_tx: mpsc::UnboundedSender<PendingMessage>,
    ) -> Self {
        let address = GrainAddress::new(
            grain_id.clone(),
            activation_id,
            Some(silo_address.clone()),
        );

        Self {
            grain_id,
            grain_type,
            activation_id,
            silo_address,
            address,
            grain: RwLock::new(None),
            context,
            grain_type_data,
            state: RwLock::new(ActivationState::Creating),
            deactivation_reason: RwLock::new(None),
            message_queue: Mutex::new(VecDeque::new()),
            message_tx,
            is_processing: AtomicBool::new(false),
            created_at: Utc::now(),
            last_activity: RwLock::new(Instant::now()),
            stats: RwLock::new(ActivationStats::default()),
            outstanding_calls: AtomicU64::new(0),
        }
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Returns the grain type.
    pub fn grain_type(&self) -> &GrainType {
        &self.grain_type
    }

    /// Returns the activation ID.
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    /// Returns the silo address.
    pub fn silo_address(&self) -> &SiloAddress {
        &self.silo_address
    }

    /// Returns the complete grain address.
    pub fn address(&self) -> &GrainAddress {
        &self.address
    }

    /// Returns the current state.
    pub fn state(&self) -> ActivationState {
        *self.state.read()
    }

    /// Returns the deactivation reason if deactivating.
    pub fn deactivation_reason(&self) -> Option<DeactivationReason> {
        self.deactivation_reason.read().clone()
    }

    /// Returns the context.
    pub fn context(&self) -> &Arc<GrainContext> {
        &self.context
    }

    /// Returns the creation time.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// Returns the last activity time.
    pub fn last_activity(&self) -> Instant {
        *self.last_activity.read()
    }

    /// Returns the statistics.
    pub fn stats(&self) -> ActivationStats {
        self.stats.read().clone()
    }

    /// Returns the number of outstanding calls.
    pub fn outstanding_calls(&self) -> u64 {
        self.outstanding_calls.load(Ordering::Acquire)
    }

    /// Returns the number of pending messages.
    pub fn pending_message_count(&self) -> usize {
        self.message_queue.lock().len()
    }

    /// Check if the activation can receive messages.
    pub fn can_receive_messages(&self) -> bool {
        self.state().can_receive_messages()
    }

    /// Transition to a new state.
    pub fn transition_to(&self, target: ActivationState) -> RuntimeResult<()> {
        let mut state = self.state.write();
        if let Some(new_state) = state.transition_to(target) {
            *state = new_state;
            Ok(())
        } else {
            Err(RuntimeError::InvalidActivationState {
                activation_id: self.activation_id.clone(),
                state: format!("Cannot transition from {} to {}", *state, target),
            })
        }
    }

    /// Set the deactivation reason.
    pub fn set_deactivation_reason(&self, reason: DeactivationReason) {
        *self.deactivation_reason.write() = Some(reason);
    }

    /// Set the grain instance.
    pub fn set_grain(&self, grain: Box<dyn std::any::Any + Send + Sync>) {
        *self.grain.write() = Some(grain);
    }

    /// Take the grain instance (for deactivation).
    pub fn take_grain(&self) -> Option<Box<dyn std::any::Any + Send + Sync>> {
        self.grain.write().take()
    }

    /// Get write access to the grain instance.
    pub fn grain_mut(&self) -> parking_lot::RwLockWriteGuard<'_, Option<Box<dyn std::any::Any + Send + Sync>>> {
        self.grain.write()
    }

    /// Enqueue a message for processing.
    ///
    /// Messages can be enqueued while the activation is in Creating, Activating, or Valid state.
    /// They will be queued and processed once the activation worker starts processing.
    /// Messages are rejected if the activation is deactivating or already invalid.
    pub fn enqueue_message(&self, message: PendingMessage) -> RuntimeResult<()> {
        let state = self.state();
        // Reject if deactivating or terminal - the activation can't process messages anymore
        if state.is_deactivating() || state.is_terminal() {
            return Err(RuntimeError::ActivationDeactivating {
                activation_id: self.activation_id.clone(),
            });
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.messages_received += 1;
        }

        // Send to the worker
        self.message_tx
            .send(message)
            .map_err(|_| RuntimeError::Internal("Message channel closed".to_string()))
    }

    /// Update last activity time.
    pub fn touch(&self) {
        *self.last_activity.write() = Instant::now();
    }

    /// Check if the activation is idle.
    pub fn is_idle(&self, idle_timeout: Duration) -> bool {
        self.last_activity().elapsed() > idle_timeout && self.outstanding_calls() == 0
    }

    /// Increment outstanding calls.
    pub fn increment_outstanding_calls(&self) {
        self.outstanding_calls.fetch_add(1, Ordering::AcqRel);
    }

    /// Decrement outstanding calls.
    pub fn decrement_outstanding_calls(&self) {
        self.outstanding_calls.fetch_sub(1, Ordering::AcqRel);
    }

    /// Record that a message was processed.
    pub fn record_message_processed(&self, processing_time: Duration) {
        let mut stats = self.stats.write();
        stats.messages_processed += 1;
        stats.total_processing_time_us += processing_time.as_micros() as u64;
    }

    /// Record that a message failed.
    pub fn record_message_failed(&self) {
        let mut stats = self.stats.write();
        stats.messages_failed += 1;
    }

    /// Get an invoker for the given interface type.
    pub fn get_invoker(&self, interface_type: &str) -> Option<Arc<dyn IGrainMethodInvoker>> {
        self.grain_type_data.invokers.get(interface_type).cloned()
    }
}

/// Handle to an activation for external use.
#[derive(Clone)]
pub struct ActivationHandle {
    inner: Arc<ActivationData>,
}

impl std::fmt::Debug for ActivationHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActivationHandle")
            .field("grain_id", self.inner.grain_id())
            .field("activation_id", self.inner.activation_id())
            .field("state", &self.inner.state())
            .finish()
    }
}

impl ActivationHandle {
    /// Create a new handle.
    pub fn new(data: Arc<ActivationData>) -> Self {
        Self { inner: data }
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        self.inner.grain_id()
    }

    /// Returns the activation ID.
    pub fn activation_id(&self) -> &ActivationId {
        self.inner.activation_id()
    }

    /// Returns the address.
    pub fn address(&self) -> &GrainAddress {
        self.inner.address()
    }

    /// Returns the current state.
    pub fn state(&self) -> ActivationState {
        self.inner.state()
    }

    /// Enqueue a message.
    pub fn enqueue_message(&self, message: PendingMessage) -> RuntimeResult<()> {
        self.inner.enqueue_message(message)
    }

    /// Returns the inner data.
    pub fn inner(&self) -> &Arc<ActivationData> {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::grain::{IGrainActivator, IGrain};
    use crate::grain_factory::IGrainFactory;
    use async_trait::async_trait;
    use orleans_messaging::GrainInterfaceType;

    struct TestGrain {
        value: i32,
    }

    #[async_trait]
    impl IGrain for TestGrain {
        fn grain_type() -> GrainType {
            GrainType::create("TestGrain")
        }
    }

    struct TestActivator;

    impl IGrainActivator for TestActivator {
        fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
            Box::new(TestGrain { value: 42 })
        }

        fn grain_type(&self) -> GrainType {
            TestGrain::grain_type()
        }
    }

    struct MockGrainFactory;

    impl IGrainFactory for MockGrainFactory {
        fn get_grain_reference(
            &self,
            _grain_type: GrainType,
            _key: IdSpan,
        ) -> Arc<dyn crate::grain_reference::IGrainReference> {
            unimplemented!()
        }
    }

    use orleans_core::IdSpan;

    fn create_test_activation() -> (Arc<ActivationData>, mpsc::UnboundedReceiver<PendingMessage>) {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("test-key"));
        let activation_id = ActivationId::new();
        let silo_address =
            SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1234);

        let grain_factory = Arc::new(MockGrainFactory);
        let context = Arc::new(GrainContext::new(
            grain_id.clone(),
            grain_type.clone(),
            activation_id.clone(),
            silo_address.clone(),
            grain_factory,
        ));

        let activator = Arc::new(TestActivator);
        let grain_type_data = Arc::new(GrainTypeData::new(grain_type.clone(), activator));

        let (tx, rx) = mpsc::unbounded_channel();

        let activation = Arc::new(ActivationData::new(
            grain_id,
            grain_type,
            activation_id,
            silo_address,
            context,
            grain_type_data,
            tx,
        ));

        (activation, rx)
    }

    #[test]
    fn test_activation_creation() {
        let (activation, _rx) = create_test_activation();

        assert_eq!(activation.grain_type().as_str(), Some("TestGrain"));
        assert_eq!(activation.state(), ActivationState::Creating);
        assert!(!activation.activation_id().is_default());
    }

    #[test]
    fn test_activation_state_transitions() {
        let (activation, _rx) = create_test_activation();

        // Creating -> Activating
        assert!(activation.transition_to(ActivationState::Activating).is_ok());
        assert_eq!(activation.state(), ActivationState::Activating);

        // Activating -> Valid
        assert!(activation.transition_to(ActivationState::Valid).is_ok());
        assert_eq!(activation.state(), ActivationState::Valid);

        // Valid -> Deactivating
        assert!(activation.transition_to(ActivationState::Deactivating).is_ok());
        assert_eq!(activation.state(), ActivationState::Deactivating);

        // Deactivating -> Invalid
        assert!(activation.transition_to(ActivationState::Invalid).is_ok());
        assert_eq!(activation.state(), ActivationState::Invalid);
    }

    #[test]
    fn test_invalid_state_transition() {
        let (activation, _rx) = create_test_activation();

        // Creating cannot go directly to Valid
        assert!(activation.transition_to(ActivationState::Valid).is_err());
    }

    #[test]
    fn test_activation_can_receive_messages() {
        let (activation, _rx) = create_test_activation();

        // Creating cannot receive
        assert!(!activation.can_receive_messages());

        activation.transition_to(ActivationState::Activating).unwrap();
        assert!(!activation.can_receive_messages());

        activation.transition_to(ActivationState::Valid).unwrap();
        assert!(activation.can_receive_messages());

        activation.transition_to(ActivationState::Deactivating).unwrap();
        assert!(!activation.can_receive_messages());
    }

    #[test]
    fn test_activation_grain_storage() {
        let (activation, _rx) = create_test_activation();

        assert!(activation.grain.read().is_none());

        let grain = Box::new(TestGrain { value: 99 });
        activation.set_grain(grain);

        assert!(activation.grain.read().is_some());

        let taken = activation.take_grain();
        assert!(taken.is_some());
        assert!(activation.grain.read().is_none());
    }

    #[test]
    fn test_activation_touch() {
        let (activation, _rx) = create_test_activation();

        let initial = activation.last_activity();
        std::thread::sleep(Duration::from_millis(10));
        activation.touch();

        assert!(activation.last_activity() > initial);
    }

    #[test]
    fn test_activation_is_idle() {
        let (activation, _rx) = create_test_activation();

        // Just created, not idle
        assert!(!activation.is_idle(Duration::from_millis(100)));

        // Wait for idle timeout
        std::thread::sleep(Duration::from_millis(50));
        assert!(activation.is_idle(Duration::from_millis(10)));
    }

    #[test]
    fn test_activation_outstanding_calls() {
        let (activation, _rx) = create_test_activation();

        assert_eq!(activation.outstanding_calls(), 0);

        activation.increment_outstanding_calls();
        assert_eq!(activation.outstanding_calls(), 1);

        activation.increment_outstanding_calls();
        assert_eq!(activation.outstanding_calls(), 2);

        activation.decrement_outstanding_calls();
        assert_eq!(activation.outstanding_calls(), 1);
    }

    #[test]
    fn test_activation_stats() {
        let (activation, _rx) = create_test_activation();

        let stats = activation.stats();
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.messages_processed, 0);

        activation.record_message_processed(Duration::from_micros(100));
        let stats = activation.stats();
        assert_eq!(stats.messages_processed, 1);
        assert_eq!(stats.total_processing_time_us, 100);
    }

    #[test]
    fn test_activation_handle() {
        let (activation, _rx) = create_test_activation();
        let handle = ActivationHandle::new(activation.clone());

        assert_eq!(handle.grain_id(), activation.grain_id());
        assert_eq!(handle.activation_id(), activation.activation_id());
        assert_eq!(handle.state(), ActivationState::Creating);
    }

    #[test]
    fn test_pending_message_expiry() {
        let silo = SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1);
        let message = Message::new_request(
            GrainId::new(
                GrainType::create("Test"),
                IdSpan::from_str("key"),
            ),
            GrainInterfaceType::create("ITest"),
            1,
            Bytes::new(),
            silo,
        )
        .with_timeout(Some(Duration::from_millis(10)));

        let pending = PendingMessage::new(message, None);

        // Not expired yet
        assert!(!pending.is_expired());

        // Wait for expiry
        std::thread::sleep(Duration::from_millis(15));
        assert!(pending.is_expired());
    }

    #[test]
    fn test_deactivation_reason() {
        let (activation, _rx) = create_test_activation();

        assert!(activation.deactivation_reason().is_none());

        activation.set_deactivation_reason(DeactivationReason::IdleTimeout);
        assert_eq!(
            activation.deactivation_reason(),
            Some(DeactivationReason::IdleTimeout)
        );
    }
}
