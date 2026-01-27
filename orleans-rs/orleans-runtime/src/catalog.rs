//! Activation catalog - registry of active grains on this silo.
//!
//! The catalog is responsible for:
//! - Managing the lifecycle of grain activations
//! - Looking up activations by GrainId
//! - Creating new activations when needed
//! - Garbage collecting idle activations

use dashmap::DashMap;
use orleans_core::{ActivationId, GrainId, GrainType, SiloAddress};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::activation_data::{ActivationData, ActivationHandle, PendingMessage};
use crate::activation_state::{ActivationState, DeactivationReason};
use crate::error::{RuntimeError, RuntimeResult};
use crate::grain::GrainTypeData;
use crate::grain_context::GrainContext;
use crate::grain_factory::IGrainFactory;

/// Configuration options for the catalog.
#[derive(Debug, Clone)]
pub struct CatalogOptions {
    /// Maximum number of activations on this silo.
    pub max_activations: usize,

    /// Time before an idle activation is collected.
    pub idle_timeout: Duration,

    /// Interval for running the activation collection.
    pub collection_interval: Duration,

    /// Number of shards for the activation map (for concurrency).
    pub shard_count: usize,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        Self {
            max_activations: 100_000,
            idle_timeout: Duration::from_secs(120),
            collection_interval: Duration::from_secs(30),
            shard_count: 64,
        }
    }
}

/// Statistics about the catalog.
#[derive(Debug, Default, Clone)]
pub struct CatalogStats {
    /// Total activations created.
    pub activations_created: u64,

    /// Total activations destroyed.
    pub activations_destroyed: u64,

    /// Current number of active activations.
    pub active_activations: u64,

    /// Number of activations collected due to idle timeout.
    pub idle_collections: u64,
}

/// The activation catalog - registry of active grains on this silo.
pub struct Catalog {
    /// The silo hosting this catalog.
    silo_address: SiloAddress,

    /// Active activations indexed by GrainId.
    activations: DashMap<GrainId, Arc<ActivationData>>,

    /// Activations indexed by ActivationId (for fast lookup).
    activations_by_id: DashMap<ActivationId, Arc<ActivationData>>,

    /// Registered grain types.
    grain_types: RwLock<HashMap<String, Arc<GrainTypeData>>>,

    /// The grain factory.
    grain_factory: Arc<dyn IGrainFactory>,

    /// Configuration options.
    options: CatalogOptions,

    /// Statistics.
    stats: CatalogStats,

    /// Counter for statistics.
    activations_created: AtomicU64,
    activations_destroyed: AtomicU64,
    idle_collections: AtomicU64,
}

impl Catalog {
    /// Create a new catalog.
    pub fn new(
        silo_address: SiloAddress,
        grain_factory: Arc<dyn IGrainFactory>,
        options: CatalogOptions,
    ) -> Self {
        Self {
            silo_address,
            activations: DashMap::with_shard_amount(options.shard_count),
            activations_by_id: DashMap::with_shard_amount(options.shard_count),
            grain_types: RwLock::new(HashMap::new()),
            grain_factory,
            options,
            stats: CatalogStats::default(),
            activations_created: AtomicU64::new(0),
            activations_destroyed: AtomicU64::new(0),
            idle_collections: AtomicU64::new(0),
        }
    }

    /// Register a grain type with its activator and invokers.
    pub fn register_grain_type(&self, grain_type_data: Arc<GrainTypeData>) {
        let type_name = grain_type_data.grain_type.as_str().unwrap_or("Unknown").to_string();
        self.grain_types.write().insert(type_name.clone(), grain_type_data);
        debug!(grain_type = %type_name, "Registered grain type");
    }

    /// Get the grain type data for a grain type.
    pub fn get_grain_type_data(&self, grain_type: &GrainType) -> Option<Arc<GrainTypeData>> {
        self.grain_types.read().get(grain_type.as_str().unwrap_or("Unknown")).cloned()
    }

    /// Look up an activation by grain ID.
    pub fn lookup(&self, grain_id: &GrainId) -> Option<ActivationHandle> {
        self.activations
            .get(grain_id)
            .map(|entry| ActivationHandle::new(entry.value().clone()))
    }

    /// Look up an activation by activation ID.
    pub fn lookup_by_activation_id(&self, activation_id: &ActivationId) -> Option<ActivationHandle> {
        self.activations_by_id
            .get(activation_id)
            .map(|entry| ActivationHandle::new(entry.value().clone()))
    }

    /// Get or create an activation for a grain.
    ///
    /// If an activation already exists, return it.
    /// Otherwise, create a new one.
    pub fn get_or_create_activation(
        &self,
        grain_id: &GrainId,
    ) -> RuntimeResult<ActivationHandle> {
        // Fast path: check if activation exists
        if let Some(handle) = self.lookup(grain_id) {
            return Ok(handle);
        }

        // Slow path: create new activation
        self.create_activation(grain_id)
    }

    /// Create a new activation for a grain.
    fn create_activation(&self, grain_id: &GrainId) -> RuntimeResult<ActivationHandle> {
        let grain_type = grain_id.grain_type();

        // Check if we have the grain type registered
        let grain_type_data = self
            .get_grain_type_data(grain_type)
            .ok_or_else(|| RuntimeError::GrainTypeNotRegistered {
                grain_type: grain_type.as_str().unwrap_or("Unknown").to_string(),
            })?;

        // Check capacity
        let current_count = self.activations.len();
        if current_count >= self.options.max_activations {
            return Err(RuntimeError::Internal(format!(
                "Maximum activation count reached: {}",
                self.options.max_activations
            )));
        }

        // Create activation ID
        let activation_id = ActivationId::new();

        // Create the message channel for this activation
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        // Create the grain context
        let context = Arc::new(GrainContext::new(
            grain_id.clone(),
            grain_type.clone(),
            activation_id.clone(),
            self.silo_address.clone(),
            self.grain_factory.clone(),
        ));

        // Create the activation data
        let activation = Arc::new(ActivationData::new(
            grain_id.clone(),
            grain_type.clone(),
            activation_id.clone(),
            self.silo_address.clone(),
            context,
            grain_type_data.clone(),
            message_tx,
        ));

        // Try to insert (handle race condition)
        match self.activations.entry(grain_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(existing) => {
                // Another thread created the activation first
                Ok(ActivationHandle::new(existing.get().clone()))
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                // We won the race, insert the activation
                vacant.insert(activation.clone());
                self.activations_by_id
                    .insert(activation_id.clone(), activation.clone());

                self.activations_created.fetch_add(1, Ordering::Relaxed);

                debug!(
                    grain_id = %grain_id,
                    activation_id = %activation_id,
                    "Created activation"
                );

                // Spawn the activation worker
                self.spawn_activation_worker(activation.clone(), message_rx, grain_type_data);

                Ok(ActivationHandle::new(activation))
            }
        }
    }

    /// Spawn the worker task for an activation.
    fn spawn_activation_worker(
        &self,
        activation: Arc<ActivationData>,
        mut message_rx: mpsc::UnboundedReceiver<PendingMessage>,
        grain_type_data: Arc<GrainTypeData>,
    ) {
        let silo_address = self.silo_address.clone();
        let _catalog = self.clone_weak();

        tokio::spawn(async move {
            // Create the grain instance
            let grain = grain_type_data.activator.create(activation.grain_id());
            activation.set_grain(grain);

            // Transition to Activating
            if activation.transition_to(ActivationState::Activating).is_err() {
                warn!(
                    grain_id = %activation.grain_id(),
                    "Failed to transition to Activating"
                );
                return;
            }

            // Call OnActivate
            // Note: In a full implementation, we would call the grain's on_activate method here
            // For now, we just transition to Valid

            // Transition to Valid
            if activation.transition_to(ActivationState::Valid).is_err() {
                warn!(
                    grain_id = %activation.grain_id(),
                    "Failed to transition to Valid"
                );
                return;
            }

            debug!(
                grain_id = %activation.grain_id(),
                activation_id = %activation.activation_id(),
                "Activation is now valid and processing messages"
            );

            // Process messages
            while let Some(pending) = message_rx.recv().await {
                // Check if we're still valid
                if !activation.can_receive_messages() {
                    // Reject the message
                    if let Some(response_tx) = pending.response_tx {
                        let rejection = orleans_messaging::Message::create_rejection(
                            &pending.message,
                            orleans_messaging::RejectionType::Transient,
                            "Activation is deactivating".to_string(),
                            silo_address.clone(),
                        );
                        let _ = response_tx.send(rejection);
                    }
                    continue;
                }

                // Update activity
                activation.touch();
                activation.increment_outstanding_calls();

                let start = Instant::now();

                // Process the message
                let result = Self::process_message(&activation, &pending, &grain_type_data).await;

                let processing_time = start.elapsed();

                // Send response
                match result {
                    Ok(response_body) => {
                        if let Some(response_tx) = pending.response_tx {
                            let response = pending.message.create_response(
                                response_body,
                                silo_address.clone(),
                            );
                            let _ = response_tx.send(response);
                        }
                        activation.record_message_processed(processing_time);
                    }
                    Err(e) => {
                        if let Some(response_tx) = pending.response_tx {
                            let rejection = orleans_messaging::Message::create_rejection(
                                &pending.message,
                                orleans_messaging::RejectionType::Unrecoverable,
                                e.to_string(),
                                silo_address.clone(),
                            );
                            let _ = response_tx.send(rejection);
                        }
                        activation.record_message_failed();
                    }
                }

                activation.decrement_outstanding_calls();
            }

            debug!(
                grain_id = %activation.grain_id(),
                "Activation worker shutting down"
            );
        });
    }

    /// Process a single message.
    async fn process_message(
        activation: &ActivationData,
        pending: &PendingMessage,
        _grain_type_data: &GrainTypeData,
    ) -> RuntimeResult<bytes::Bytes> {
        let message = &pending.message;
        let interface_type = message.interface_type().as_str().unwrap_or("Unknown");
        let method_id = message.method_id();

        // Get the invoker for this interface
        let invoker = activation.get_invoker(interface_type).ok_or_else(|| {
            RuntimeError::MethodNotFound {
                interface_type: interface_type.to_string(),
                method_id,
            }
        })?;

        // Take the grain instance temporarily to avoid holding lock across await
        let mut grain = activation.take_grain().ok_or_else(|| {
            RuntimeError::Internal("Grain instance not found".to_string())
        })?;

        // Invoke the method
        let result = invoker
            .invoke(
                grain.as_mut(),
                activation.context().as_ref(),
                method_id,
                message.body(),
            )
            .await;

        // Put the grain back
        activation.set_grain(grain);

        Ok(bytes::Bytes::from(result?))
    }

    /// Remove an activation from the catalog.
    pub fn remove_activation(&self, grain_id: &GrainId) -> Option<Arc<ActivationData>> {
        if let Some((_, activation)) = self.activations.remove(grain_id) {
            self.activations_by_id.remove(activation.activation_id());
            self.activations_destroyed.fetch_add(1, Ordering::Relaxed);

            debug!(
                grain_id = %grain_id,
                activation_id = %activation.activation_id(),
                "Removed activation"
            );

            Some(activation)
        } else {
            None
        }
    }

    /// Deactivate a grain.
    pub async fn deactivate_grain(
        &self,
        grain_id: &GrainId,
        reason: DeactivationReason,
    ) -> RuntimeResult<()> {
        if let Some(activation) = self.lookup(grain_id) {
            let inner = activation.inner();

            // Set deactivation reason
            inner.set_deactivation_reason(reason);

            // Transition to Deactivating
            inner.transition_to(ActivationState::Deactivating)?;

            // Wait for outstanding calls to complete
            while inner.outstanding_calls() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Call OnDeactivate
            // Note: In a full implementation, we would call the grain's on_deactivate method here

            // Transition to Invalid
            inner.transition_to(ActivationState::Invalid)?;

            // Remove from catalog
            self.remove_activation(grain_id);
        }

        Ok(())
    }

    /// Collect idle activations.
    pub fn collect_idle_activations(&self) -> Vec<GrainId> {
        let idle_timeout = self.options.idle_timeout;
        let mut collected = Vec::new();

        for entry in self.activations.iter() {
            let activation = entry.value();
            if activation.state() == ActivationState::Valid
                && activation.is_idle(idle_timeout)
                && !activation.context().is_deactivate_requested()
            {
                collected.push(entry.key().clone());
            }
        }

        if !collected.is_empty() {
            self.idle_collections
                .fetch_add(collected.len() as u64, Ordering::Relaxed);
            info!(
                count = collected.len(),
                "Collecting idle activations"
            );
        }

        collected
    }

    /// Get the current statistics.
    pub fn stats(&self) -> CatalogStats {
        CatalogStats {
            activations_created: self.activations_created.load(Ordering::Relaxed),
            activations_destroyed: self.activations_destroyed.load(Ordering::Relaxed),
            active_activations: self.activations.len() as u64,
            idle_collections: self.idle_collections.load(Ordering::Relaxed),
        }
    }

    /// Get the number of active activations.
    pub fn activation_count(&self) -> usize {
        self.activations.len()
    }

    /// Get all active grain IDs.
    pub fn all_grain_ids(&self) -> Vec<GrainId> {
        self.activations.iter().map(|e| e.key().clone()).collect()
    }

    /// Clone a weak reference to the catalog (for spawned tasks).
    fn clone_weak(&self) -> CatalogRef {
        CatalogRef {
            silo_address: self.silo_address.clone(),
        }
    }
}

/// A weak reference to the catalog for use in spawned tasks.
struct CatalogRef {
    silo_address: SiloAddress,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grain::{IGrain, IGrainActivator};
    use async_trait::async_trait;
    use orleans_core::IdSpan;

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

    fn create_test_catalog() -> Catalog {
        let silo_address = SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1234);
        let grain_factory = Arc::new(MockGrainFactory);
        let options = CatalogOptions::default();

        let catalog = Catalog::new(silo_address, grain_factory, options);

        // Register the test grain type
        let activator = Arc::new(TestActivator);
        let grain_type_data = Arc::new(GrainTypeData::new(TestGrain::grain_type(), activator));
        catalog.register_grain_type(grain_type_data);

        catalog
    }

    #[test]
    fn test_catalog_creation() {
        let catalog = create_test_catalog();
        assert_eq!(catalog.activation_count(), 0);
    }

    #[test]
    fn test_register_grain_type() {
        let catalog = create_test_catalog();

        let data = catalog.get_grain_type_data(&TestGrain::grain_type());
        assert!(data.is_some());
        assert_eq!(data.unwrap().grain_type.as_str(), Some("TestGrain"));
    }

    #[tokio::test]
    async fn test_get_or_create_activation() {
        let catalog = create_test_catalog();

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));

        let handle = catalog.get_or_create_activation(&grain_id).unwrap();

        assert_eq!(handle.grain_id(), &grain_id);
        assert_eq!(catalog.activation_count(), 1);

        // Getting the same grain should return the same activation
        let handle2 = catalog.get_or_create_activation(&grain_id).unwrap();
        assert_eq!(handle.activation_id(), handle2.activation_id());
        assert_eq!(catalog.activation_count(), 1);
    }

    #[tokio::test]
    async fn test_lookup() {
        let catalog = create_test_catalog();

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));

        // Initially not found
        assert!(catalog.lookup(&grain_id).is_none());

        // Create activation
        catalog.get_or_create_activation(&grain_id).unwrap();

        // Now should be found
        let handle = catalog.lookup(&grain_id);
        assert!(handle.is_some());
        assert_eq!(handle.unwrap().grain_id(), &grain_id);
    }

    #[tokio::test]
    async fn test_lookup_by_activation_id() {
        let catalog = create_test_catalog();

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));
        let handle = catalog.get_or_create_activation(&grain_id).unwrap();

        let activation_id = handle.activation_id().clone();

        let found = catalog.lookup_by_activation_id(&activation_id);
        assert!(found.is_some());
        assert_eq!(found.unwrap().grain_id(), &grain_id);
    }

    #[tokio::test]
    async fn test_remove_activation() {
        let catalog = create_test_catalog();

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));
        catalog.get_or_create_activation(&grain_id).unwrap();

        assert_eq!(catalog.activation_count(), 1);

        catalog.remove_activation(&grain_id);

        assert_eq!(catalog.activation_count(), 0);
        assert!(catalog.lookup(&grain_id).is_none());
    }

    #[tokio::test]
    async fn test_unregistered_grain_type() {
        let silo_address = SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1234);
        let grain_factory = Arc::new(MockGrainFactory);
        let options = CatalogOptions::default();

        let catalog = Catalog::new(silo_address, grain_factory, options);
        // Note: NOT registering the grain type

        let grain_id = GrainId::new(
            GrainType::create("UnknownGrain"),
            IdSpan::from_str("key1"),
        );

        let result = catalog.get_or_create_activation(&grain_id);
        assert!(result.is_err());
        match result.unwrap_err() {
            RuntimeError::GrainTypeNotRegistered { grain_type } => {
                assert_eq!(grain_type, "UnknownGrain");
            }
            _ => panic!("Expected GrainTypeNotRegistered error"),
        }
    }

    #[tokio::test]
    async fn test_all_grain_ids() {
        let catalog = create_test_catalog();

        let grain_id1 = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));
        let grain_id2 = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key2"));

        catalog.get_or_create_activation(&grain_id1).unwrap();
        catalog.get_or_create_activation(&grain_id2).unwrap();

        let all_ids = catalog.all_grain_ids();
        assert_eq!(all_ids.len(), 2);
        assert!(all_ids.contains(&grain_id1));
        assert!(all_ids.contains(&grain_id2));
    }

    #[tokio::test]
    async fn test_stats() {
        let catalog = create_test_catalog();

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));
        catalog.get_or_create_activation(&grain_id).unwrap();

        let stats = catalog.stats();
        assert_eq!(stats.activations_created, 1);
        assert_eq!(stats.active_activations, 1);

        catalog.remove_activation(&grain_id);

        let stats = catalog.stats();
        assert_eq!(stats.activations_destroyed, 1);
        assert_eq!(stats.active_activations, 0);
    }

    #[tokio::test]
    async fn test_collect_idle_activations() {
        let silo_address = SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1234);
        let grain_factory = Arc::new(MockGrainFactory);
        let options = CatalogOptions {
            idle_timeout: Duration::from_millis(10),
            ..Default::default()
        };

        let catalog = Catalog::new(silo_address, grain_factory, options);

        let activator = Arc::new(TestActivator);
        let grain_type_data = Arc::new(GrainTypeData::new(TestGrain::grain_type(), activator));
        catalog.register_grain_type(grain_type_data);

        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("key1"));
        let handle = catalog.get_or_create_activation(&grain_id).unwrap();

        // Wait for activation to become valid
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Only collect if valid and idle
        if handle.state() == ActivationState::Valid {
            let collected = catalog.collect_idle_activations();
            assert_eq!(collected.len(), 1);
            assert_eq!(collected[0], grain_id);
        }
    }
}
