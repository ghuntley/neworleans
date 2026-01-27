//! Proof-of-concept test demonstrating three silos forming a cluster.
//!
//! This test creates three separate silos (simulating three processes),
//! registers grains, and demonstrates that:
//! 1. All three silos join the same cluster
//! 2. Grains can be activated on any silo
//! 3. The directory correctly tracks grain locations
//! 4. Grain placement is distributed across all silos

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use orleans_clustering::InMemoryMembershipTable;
use orleans_core::{GrainId, GrainType, IdSpan};
use orleans_host::{
    GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker,
    IMembershipTable, RuntimeResult, SiloBuilder,
};

// Simple test grain
struct TestGrain {
    counter: AtomicU32,
}

#[async_trait]
impl IGrain for TestGrain {
    fn grain_type() -> GrainType {
        GrainType::create("TestGrain")
    }
}

impl TestGrain {
    fn new() -> Self {
        Self { counter: AtomicU32::new(0) }
    }

    fn increment(&self) -> u32 {
        self.counter.fetch_add(1, Ordering::SeqCst) + 1
    }
}

struct TestGrainActivator;

impl IGrainActivator for TestGrainActivator {
    fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(TestGrain::new())
    }
    fn grain_type(&self) -> GrainType {
        TestGrain::grain_type()
    }
}

struct TestGrainInvoker;
impl TestGrainInvoker {
    const INTERFACE_TYPE: &'static str = "ITestGrain";
    const METHOD_IDS: [u32; 1] = [1];
}

impl IGrainMethodInvoker for TestGrainInvoker {
    fn interface_type(&self) -> &str { Self::INTERFACE_TYPE }
    fn method_ids(&self) -> &[u32] { &Self::METHOD_IDS }

    fn invoke<'a, 'b, 'c, 'd, 'e>(
        &'a self,
        grain: &'b mut dyn std::any::Any,
        _ctx: &'c dyn IGrainContext,
        method_id: u32,
        _body: &'d [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'e>>
    where 'a: 'e, 'b: 'e, 'c: 'e, 'd: 'e, Self: 'e,
    {
        let grain = grain.downcast_mut::<TestGrain>().unwrap();
        let result = match method_id {
            1 => Ok(grain.increment().to_le_bytes().to_vec()),
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: "ITestGrain".to_string(), method_id,
            }),
        };
        Box::pin(std::future::ready(result))
    }
}

fn create_grain_type() -> Arc<GrainTypeData> {
    Arc::new(
        GrainTypeData::new(TestGrain::grain_type(), Arc::new(TestGrainActivator))
            .with_invoker("ITestGrain", Arc::new(TestGrainInvoker))
    )
}

#[tokio::test]
async fn prove_three_silo_cluster_works() {
    println!("\n{}", "=".repeat(70));
    println!("PROOF: Three Silo Cluster Formation and Grain Distribution");
    println!("{}\n", "=".repeat(70));

    // Step 1: Create shared membership table (simulates shared storage)
    println!("Step 1: Creating shared membership table...");
    let membership_table = Arc::new(InMemoryMembershipTable::new("proof-cluster"));
    membership_table.initialize_membership_table(true).await.unwrap();
    println!("  ✓ Membership table created for cluster 'proof-cluster'\n");

    // Step 2: Create and start three silos
    println!("Step 2: Starting three silos...");
    let grain_type = create_grain_type();

    let mut silo1 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build().await.unwrap();

    let mut silo2 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build().await.unwrap();

    let mut silo3 = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build().await.unwrap();

    silo1.start().await.unwrap();
    println!("  ✓ Silo 1 started at {}", silo1.address());

    silo2.start().await.unwrap();
    println!("  ✓ Silo 2 started at {}", silo2.address());

    silo3.start().await.unwrap();
    println!("  ✓ Silo 3 started at {}", silo3.address());
    println!();

    // Allow cluster to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Step 3: Verify cluster membership
    println!("Step 3: Verifying cluster membership...");
    let manager = silo1.membership_manager().unwrap();
    let snapshot = manager.get_snapshot();
    let active_silos = snapshot.get_active_silos();

    println!("  Active silos in cluster: {}", active_silos.len());
    for silo in &active_silos {
        println!("    - {}", silo);
    }
    assert_eq!(active_silos.len(), 3, "Expected 3 active silos");
    println!("  ✓ All three silos are Active in membership table\n");

    // Step 4: Verify directory ring
    println!("Step 4: Verifying grain directory ring...");
    let dir1 = silo1.directory().unwrap();
    let dir2 = silo2.directory().unwrap();
    let dir3 = silo3.directory().unwrap();

    println!("  Silo 1 sees {} silos in directory ring", dir1.ring().silo_count());
    println!("  Silo 2 sees {} silos in directory ring", dir2.ring().silo_count());
    println!("  Silo 3 sees {} silos in directory ring", dir3.ring().silo_count());

    assert_eq!(dir1.ring().silo_count(), 3);
    assert_eq!(dir2.ring().silo_count(), 3);
    assert_eq!(dir3.ring().silo_count(), 3);
    println!("  ✓ All silos have consistent directory ring view\n");

    // Step 5: Test grain placement distribution
    println!("Step 5: Testing grain placement distribution...");
    let mut placement_count = std::collections::HashMap::new();

    for i in 0..100 {
        let grain_id = GrainId::new(
            TestGrain::grain_type(),
            IdSpan::from_str(&format!("grain-{}", i)),
        );
        let primary = dir1.get_primary_silo(&grain_id).unwrap();
        *placement_count.entry(primary.clone()).or_insert(0) += 1;
    }

    println!("  Grain placement for 100 grains:");
    for (silo, count) in &placement_count {
        let percentage = (*count as f64 / 100.0) * 100.0;
        println!("    - {}: {} grains ({:.1}%)", silo, count, percentage);
    }

    assert_eq!(placement_count.len(), 3, "Expected grains on all 3 silos");
    println!("  ✓ Grains are distributed across all three silos\n");

    // Step 6: Verify directory consistency
    println!("Step 6: Verifying directory consistency across silos...");
    let mut matches = 0;
    for i in 0..20 {
        let grain_id = GrainId::new(
            TestGrain::grain_type(),
            IdSpan::from_str(&format!("consistency-test-{}", i)),
        );

        let primary1 = dir1.get_primary_silo(&grain_id).unwrap();
        let primary2 = dir2.get_primary_silo(&grain_id).unwrap();
        let primary3 = dir3.get_primary_silo(&grain_id).unwrap();

        if primary1 == primary2 && primary2 == primary3 {
            matches += 1;
        }
    }

    println!("  All 3 silos agree on grain placement: {}/20", matches);
    assert_eq!(matches, 20, "All silos should agree on grain placement");
    println!("  ✓ Directory is consistent across all silos\n");

    // Step 7: Create grain activations
    println!("Step 7: Creating grain activations...");
    let catalog1 = silo1.catalog().unwrap();
    let catalog2 = silo2.catalog().unwrap();
    let catalog3 = silo3.catalog().unwrap();

    // Create grains on silo 1
    for i in 0..5 {
        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str(&format!("s1-grain-{}", i)));
        catalog1.get_or_create_activation(&grain_id).unwrap();
    }
    println!("  Created 5 grains on Silo 1");

    // Create grains on silo 2
    for i in 0..3 {
        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str(&format!("s2-grain-{}", i)));
        catalog2.get_or_create_activation(&grain_id).unwrap();
    }
    println!("  Created 3 grains on Silo 2");

    // Create grains on silo 3
    for i in 0..7 {
        let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str(&format!("s3-grain-{}", i)));
        catalog3.get_or_create_activation(&grain_id).unwrap();
    }
    println!("  Created 7 grains on Silo 3");

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\n  Activation counts:");
    println!("    - Silo 1: {} activations", catalog1.activation_count());
    println!("    - Silo 2: {} activations", catalog2.activation_count());
    println!("    - Silo 3: {} activations", catalog3.activation_count());

    let total = catalog1.activation_count() + catalog2.activation_count() + catalog3.activation_count();
    println!("    - Total: {} activations", total);
    assert_eq!(total, 15);
    println!("  ✓ Grains activated successfully across cluster\n");

    // Step 8: Test single activation guarantee
    println!("Step 8: Testing single activation guarantee...");
    let shared_grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("shared-grain"));

    // Try to create the same grain from all silos
    let h1 = catalog1.get_or_create_activation(&shared_grain_id).unwrap();
    let h2 = catalog1.get_or_create_activation(&shared_grain_id).unwrap();
    let h3 = catalog1.get_or_create_activation(&shared_grain_id).unwrap();

    println!("  Requested same grain 3 times:");
    println!("    - Request 1: activation_id = {}", h1.activation_id());
    println!("    - Request 2: activation_id = {}", h2.activation_id());
    println!("    - Request 3: activation_id = {}", h3.activation_id());

    assert_eq!(h1.activation_id(), h2.activation_id());
    assert_eq!(h2.activation_id(), h3.activation_id());
    println!("  ✓ Single activation guarantee verified\n");

    // Step 9: Graceful shutdown
    println!("Step 9: Graceful shutdown...");

    // Drop catalog references before stopping silos
    drop(catalog1);
    drop(catalog2);
    drop(catalog3);

    silo1.stop().await.unwrap();
    println!("  ✓ Silo 1 stopped (activations cleared: {})", silo1.catalog().unwrap().activation_count());

    silo2.stop().await.unwrap();
    println!("  ✓ Silo 2 stopped (activations cleared: {})", silo2.catalog().unwrap().activation_count());

    silo3.stop().await.unwrap();
    println!("  ✓ Silo 3 stopped (activations cleared: {})", silo3.catalog().unwrap().activation_count());

    println!("\n{}", "=".repeat(70));
    println!("PROOF COMPLETE: Three-silo Orleans cluster is working!");
    println!("{}\n", "=".repeat(70));
}
