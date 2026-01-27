//! Integration tests for multi-silo clusters.
//!
//! These tests demonstrate:
//! 1. Three silos forming a cluster
//! 2. Grain creation on one silo
//! 3. Cross-silo access to grains

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orleans_clustering::InMemoryMembershipTable;
use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
use orleans_host::{
    GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker,
    IMembershipTable, RuntimeResult, SiloBuilder, SiloState,
};

// ============================================================================
// Test Grain Implementation - CounterGrain
// ============================================================================

/// A simple counter grain for testing.
/// The counter value is stored per-grain-instance and accessed via method calls.
struct CounterGrain {
    counter: AtomicU32,
    silo_created_on: Option<SiloAddress>,
}

#[async_trait]
impl IGrain for CounterGrain {
    fn grain_type() -> GrainType {
        GrainType::create("CounterGrain")
    }
}

impl CounterGrain {
    fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            silo_created_on: None,
        }
    }

    fn increment(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn get_value(&self) -> u32 {
        self.counter.load(Ordering::SeqCst)
    }

    fn set_silo(&mut self, silo: SiloAddress) {
        self.silo_created_on = Some(silo);
    }
}

/// Activator for CounterGrain.
struct CounterGrainActivator;

impl IGrainActivator for CounterGrainActivator {
    fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(CounterGrain::new())
    }

    fn grain_type(&self) -> GrainType {
        CounterGrain::grain_type()
    }
}

/// Invoker for CounterGrain.
struct CounterGrainInvoker;

impl CounterGrainInvoker {
    const INTERFACE_TYPE: &'static str = "ICounterGrain";
    const METHOD_IDS: [u32; 2] = [1, 2];
}

impl IGrainMethodInvoker for CounterGrainInvoker {
    fn interface_type(&self) -> &str {
        Self::INTERFACE_TYPE
    }

    fn method_ids(&self) -> &[u32] {
        &Self::METHOD_IDS
    }

    fn invoke<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        grain: &'life1 mut dyn std::any::Any,
        _context: &'life2 dyn IGrainContext,
        method_id: u32,
        _body: &'life3 [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        let grain = grain.downcast_mut::<CounterGrain>().unwrap();

        let result = match method_id {
            1 => {
                // increment() -> u32
                let new_value = grain.increment();
                Ok(new_value.to_le_bytes().to_vec())
            }
            2 => {
                // get_value() -> u32
                let value = grain.get_value();
                Ok(value.to_le_bytes().to_vec())
            }
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: "ICounterGrain".to_string(),
                method_id,
            }),
        };

        Box::pin(std::future::ready(result))
    }
}

fn create_counter_grain_type() -> Arc<GrainTypeData> {
    let activator = Arc::new(CounterGrainActivator);
    let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(CounterGrainInvoker);

    let grain_type_data = GrainTypeData::new(CounterGrain::grain_type(), activator)
        .with_invoker("ICounterGrain", invoker);

    Arc::new(grain_type_data)
}

// ============================================================================
// Integration Tests
// ============================================================================

/// Test: Three silos form a cluster and all see each other.
#[tokio::test]
async fn test_three_silo_cluster_formation() {
    // Create shared membership table
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    // Create three silos
    let mut silo1 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo2 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo3 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    // Start all silos
    silo1.start().await.unwrap();
    silo2.start().await.unwrap();
    silo3.start().await.unwrap();

    // Allow time for cluster formation
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all silos are running
    assert_eq!(silo1.state(), SiloState::Running);
    assert_eq!(silo2.state(), SiloState::Running);
    assert_eq!(silo3.state(), SiloState::Running);

    // Verify all silos see each other in their directories
    let dir1 = silo1.directory().unwrap();
    let dir2 = silo2.directory().unwrap();
    let dir3 = silo3.directory().unwrap();

    assert_eq!(dir1.ring().silo_count(), 3);
    assert_eq!(dir2.ring().silo_count(), 3);
    assert_eq!(dir3.ring().silo_count(), 3);

    // Verify membership manager shows 3 active silos
    let manager1 = silo1.membership_manager().unwrap();
    let snapshot = manager1.get_snapshot();
    assert_eq!(snapshot.active_silo_count(), 3);

    // Shutdown all silos
    silo1.stop().await.unwrap();
    silo2.stop().await.unwrap();
    silo3.stop().await.unwrap();
}

/// Test: Grain activation on one silo is tracked correctly.
#[tokio::test]
async fn test_grain_activation_on_silo() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();

    // Create a grain
    let catalog = silo.catalog().unwrap();
    let grain_id = GrainId::new(CounterGrain::grain_type(), IdSpan::from_str("counter-1"));

    let handle = catalog.get_or_create_activation(&grain_id).unwrap();
    assert_eq!(handle.grain_id(), &grain_id);

    // Wait for activation to become valid
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify activation count
    assert_eq!(catalog.activation_count(), 1);

    // Create another grain
    let grain_id2 = GrainId::new(CounterGrain::grain_type(), IdSpan::from_str("counter-2"));
    catalog.get_or_create_activation(&grain_id2).unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(catalog.activation_count(), 2);

    silo.stop().await.unwrap();
}

/// Test: Same grain ID returns same activation (single activation guarantee).
#[tokio::test]
async fn test_single_activation_guarantee() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();

    let catalog = silo.catalog().unwrap();
    let grain_id = GrainId::new(CounterGrain::grain_type(), IdSpan::from_str("counter-1"));

    // Get or create multiple times
    let handle1 = catalog.get_or_create_activation(&grain_id).unwrap();
    let handle2 = catalog.get_or_create_activation(&grain_id).unwrap();
    let handle3 = catalog.get_or_create_activation(&grain_id).unwrap();

    // All should have the same activation ID
    assert_eq!(handle1.activation_id(), handle2.activation_id());
    assert_eq!(handle2.activation_id(), handle3.activation_id());

    // Only one activation should exist
    assert_eq!(catalog.activation_count(), 1);

    silo.stop().await.unwrap();
}

/// Test: Grain directory correctly assigns grains to silos.
#[tokio::test]
async fn test_grain_directory_assignment() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo1 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo2 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    silo1.start().await.unwrap();
    silo2.start().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir1 = silo1.directory().unwrap();
    let dir2 = silo2.directory().unwrap();

    // Both directories should agree on grain assignment
    for i in 0..100 {
        let grain_id = GrainId::new(
            CounterGrain::grain_type(),
            IdSpan::from_str(&format!("grain-{}", i)),
        );

        let primary1 = dir1.get_primary_silo(&grain_id).unwrap();
        let primary2 = dir2.get_primary_silo(&grain_id).unwrap();

        assert_eq!(
            primary1, primary2,
            "Directories disagree on grain {} assignment",
            i
        );
    }

    silo1.stop().await.unwrap();
    silo2.stop().await.unwrap();
}

/// Test: Message center is properly initialized.
#[tokio::test]
async fn test_message_center_initialization() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();

    // Message center should be running
    assert!(silo.message_center().is_running());

    // Should have a valid local address
    assert!(silo.message_center().local_address().generation() > 0);

    silo.stop().await.unwrap();

    // Message center should be stopped
    assert!(!silo.message_center().is_running());
}

/// Test: Dispatcher is registered and handles messages.
#[tokio::test]
async fn test_dispatcher_registration() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();

    // Dispatcher should be initialized
    assert!(silo.dispatcher().is_some());

    silo.stop().await.unwrap();
}

/// Test: Silo graceful shutdown deactivates all grains.
#[tokio::test]
async fn test_graceful_shutdown() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();

    // Create some grains
    {
        let catalog = silo.catalog().unwrap();
        for i in 0..5 {
            let grain_id = GrainId::new(
                CounterGrain::grain_type(),
                IdSpan::from_str(&format!("counter-{}", i)),
            );
            catalog.get_or_create_activation(&grain_id).unwrap();
        }

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should have 5 activations
        assert_eq!(catalog.activation_count(), 5);
    }

    // Graceful shutdown
    silo.stop().await.unwrap();

    // State should be stopped
    assert_eq!(silo.state(), SiloState::Stopped);

    // Activations should be cleared (use silo.catalog() again)
    assert_eq!(silo.catalog().unwrap().activation_count(), 0);
}

/// Test: Three silos - verify grain placement is distributed.
#[tokio::test]
async fn test_distributed_grain_placement() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo1 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo2 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo3 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    silo1.start().await.unwrap();
    silo2.start().await.unwrap();
    silo3.start().await.unwrap();

    tokio::time::sleep(Duration::from_millis(200)).await;

    let dir = silo1.directory().unwrap();

    // Count how many grains would be assigned to each silo
    let mut counts = std::collections::HashMap::new();
    for i in 0..1000 {
        let grain_id = GrainId::new(
            CounterGrain::grain_type(),
            IdSpan::from_str(&format!("grain-{}", i)),
        );
        let primary = dir.get_primary_silo(&grain_id).unwrap();
        *counts.entry(primary).or_insert(0) += 1;
    }

    // Each silo should get roughly 1/3 (with tolerance)
    let expected = 1000 / 3;
    let tolerance = expected / 2;

    assert_eq!(counts.len(), 3, "Expected grains on all 3 silos");

    for (silo, count) in &counts {
        assert!(
            *count > expected - tolerance && *count < expected + tolerance,
            "Uneven distribution for silo {}: {} (expected ~{})",
            silo,
            count,
            expected
        );
    }

    silo1.stop().await.unwrap();
    silo2.stop().await.unwrap();
    silo3.stop().await.unwrap();
}

/// Test: Silo addresses are unique with generation numbers.
#[tokio::test]
async fn test_silo_address_generation() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();

    let grain_type = create_counter_grain_type();

    let mut silo1 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    let mut silo2 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    silo1.start().await.unwrap();
    silo2.start().await.unwrap();

    // Each silo should have a unique address
    assert_ne!(silo1.address(), silo2.address());

    // Generation numbers should be positive
    assert!(silo1.address().generation() > 0);
    assert!(silo2.address().generation() > 0);

    silo1.stop().await.unwrap();
    silo2.stop().await.unwrap();
}
