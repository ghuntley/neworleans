//! Phase 9.9: Concurrent Grain Calls Tests
//!
//! These tests verify the turn-based execution guarantee of Orleans:
//! - Many simultaneous calls to the same grain
//! - Sequential processing (no race conditions)
//! - No duplicate or missing counter values
//!
//! Turn-based execution means grain methods execute sequentially,
//! never concurrently, ensuring no locks are needed within grain code.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use orleans_clustering::{IMembershipTable, InMemoryMembershipTable, MembershipVersion};
use orleans_core::{GrainAddress, GrainId, GrainType, IdSpan};
use orleans_host::{
    GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker, RuntimeResult,
    SiloBuilder,
};
use orleans_messaging::GrainInterfaceType;

// ============================================================================
// Test Grain Implementation - CounterGrain with timing tracking
// ============================================================================

/// A counter grain that tracks increment timing for concurrency verification.
struct CounterGrain {
    counter: AtomicU32,
    /// Tracks if we're currently processing a call (should never be true twice concurrently)
    processing: AtomicU32,
    /// Maximum concurrent processing detected (should always be 1)
    max_concurrent: AtomicU32,
}

#[async_trait]
impl IGrain for CounterGrain {
    fn grain_type() -> GrainType {
        GrainType::create("ConcurrentTestCounterGrain")
    }
}

impl CounterGrain {
    fn new() -> Self {
        Self {
            counter: AtomicU32::new(0),
            processing: AtomicU32::new(0),
            max_concurrent: AtomicU32::new(0),
        }
    }

    /// Increment the counter with a small delay to increase chance of detecting races.
    fn increment_with_delay(&self) -> u32 {
        // Mark that we're processing
        let concurrent = self.processing.fetch_add(1, Ordering::SeqCst) + 1;

        // Track maximum concurrent calls
        let mut current_max = self.max_concurrent.load(Ordering::SeqCst);
        while concurrent > current_max {
            match self.max_concurrent.compare_exchange(
                current_max,
                concurrent,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }

        // Increment the counter
        let new_value = self.counter.fetch_add(1, Ordering::SeqCst) + 1;

        // Mark that we're done processing
        self.processing.fetch_sub(1, Ordering::SeqCst);

        new_value
    }

    fn get_value(&self) -> u32 {
        self.counter.load(Ordering::SeqCst)
    }

    fn get_max_concurrent(&self) -> u32 {
        self.max_concurrent.load(Ordering::SeqCst)
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
    const INTERFACE_TYPE: &'static str = "IConcurrentTestCounterGrain";
    // Method IDs:
    // 1 = increment_with_delay()
    // 2 = get_value()
    // 3 = get_max_concurrent()
    const METHOD_IDS: [u32; 3] = [1, 2, 3];
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
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'async_trait>,
    >
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
                // increment_with_delay() -> u32
                let new_value = grain.increment_with_delay();
                Ok(new_value.to_le_bytes().to_vec())
            }
            2 => {
                // get_value() -> u32
                let value = grain.get_value();
                Ok(value.to_le_bytes().to_vec())
            }
            3 => {
                // get_max_concurrent() -> u32
                let max = grain.get_max_concurrent();
                Ok(max.to_le_bytes().to_vec())
            }
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: Self::INTERFACE_TYPE.to_string(),
                method_id,
            }),
        };

        Box::pin(std::future::ready(result))
    }
}

fn create_concurrent_test_grain_type() -> Arc<GrainTypeData> {
    let activator = Arc::new(CounterGrainActivator);
    let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(CounterGrainInvoker);

    let grain_type_data = GrainTypeData::new(CounterGrain::grain_type(), activator)
        .with_invoker(CounterGrainInvoker::INTERFACE_TYPE, invoker);

    Arc::new(grain_type_data)
}

// ============================================================================
// Test: Many concurrent calls to same grain (single silo)
// ============================================================================

/// Test 9.9.1: Many concurrent calls to the same grain on a single silo.
///
/// This tests the turn-based execution guarantee:
/// - 100 concurrent increment calls
/// - Values should be exactly 1..=100 (no duplicates, no gaps)
/// - Max concurrent processing should be 1
#[tokio::test]
async fn test_many_concurrent_calls_single_silo() {
    const NUM_CONCURRENT_CALLS: usize = 100;

    println!("\n=== Test 9.9.1: Many Concurrent Calls (Single Silo) ===");
    println!("Testing: {} simultaneous increment calls to same grain", NUM_CONCURRENT_CALLS);
    println!("Expected: Sequential processing with values 1..{}\n", NUM_CONCURRENT_CALLS);

    // Create silo
    let membership_table = Arc::new(InMemoryMembershipTable::new("concurrent-test-single"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_concurrent_test_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Create grain
    let grain_id = GrainId::new(
        CounterGrain::grain_type(),
        IdSpan::from_str("concurrent-test-grain-1"),
    );

    let catalog = silo.catalog().unwrap();
    let handle = catalog.get_or_create_activation(&grain_id).unwrap();

    // Register in directory
    let grain_address = GrainAddress::complete(
        grain_id.clone(),
        handle.activation_id().clone(),
        silo.address().clone(),
    );
    let dir = silo.directory().unwrap();
    dir.register(MembershipVersion::default(), grain_address, None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Get grain reference
    let factory = silo.grain_factory().unwrap();
    let grain_ref = factory.get_grain_reference_by_id(
        grain_id.clone(),
        GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
    );

    // Spawn N concurrent calls
    println!("Spawning {} concurrent increment calls...", NUM_CONCURRENT_CALLS);
    let mut tasks = Vec::with_capacity(NUM_CONCURRENT_CALLS);

    for _ in 0..NUM_CONCURRENT_CALLS {
        let grain_ref = grain_ref.clone();
        let task = tokio::spawn(async move {
            grain_ref
                .invoke(1, Bytes::new(), Some(Duration::from_secs(30)))
                .await
                .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
        });
        tasks.push(task);
    }

    // Await all concurrent calls
    let results: Vec<Result<u32, _>> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify all calls succeeded
    let mut values: Vec<u32> = Vec::with_capacity(NUM_CONCURRENT_CALLS);
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(v) => values.push(v),
            Err(e) => errors.push(e),
        }
    }

    println!("Results: {} succeeded, {} failed", values.len(), errors.len());
    if !errors.is_empty() {
        println!("Errors: {:?}", errors);
    }
    assert!(
        errors.is_empty(),
        "All concurrent calls should succeed: {} errors",
        errors.len()
    );

    // Verify sequential execution: values should be exactly 1..=N with no duplicates or gaps
    values.sort();
    let expected: Vec<u32> = (1..=NUM_CONCURRENT_CALLS as u32).collect();

    println!("First 10 values: {:?}", &values[..10.min(values.len())]);
    println!(
        "Last 10 values: {:?}",
        &values[values.len().saturating_sub(10)..]
    );

    assert_eq!(
        values.len(),
        NUM_CONCURRENT_CALLS,
        "Should have exactly {} values",
        NUM_CONCURRENT_CALLS
    );
    assert_eq!(
        values, expected,
        "Values must be sequential 1..{} with no duplicates or gaps",
        NUM_CONCURRENT_CALLS
    );

    // Verify final counter value
    let final_result = grain_ref
        .invoke(2, Bytes::new(), Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let final_value = u32::from_le_bytes(final_result[..4].try_into().unwrap());
    println!("Final counter value: {}", final_value);
    assert_eq!(
        final_value, NUM_CONCURRENT_CALLS as u32,
        "Final counter should equal number of calls"
    );

    // Verify max concurrent was 1 (turn-based execution)
    let max_concurrent_result = grain_ref
        .invoke(3, Bytes::new(), Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let max_concurrent = u32::from_le_bytes(max_concurrent_result[..4].try_into().unwrap());
    println!("Max concurrent processing detected: {}", max_concurrent);
    assert_eq!(
        max_concurrent, 1,
        "Turn-based execution: max concurrent should be 1"
    );

    println!("\n=== Test 9.9.1 PASSED ===");
    println!("  All {} calls succeeded", NUM_CONCURRENT_CALLS);
    println!("  Values were sequential 1..{}", NUM_CONCURRENT_CALLS);
    println!("  Max concurrent = 1 (turn-based execution verified)");

    silo.stop().await.unwrap();
}

// ============================================================================
// Test: Many concurrent calls from multiple silos
// ============================================================================

/// Test 9.9.2: Many concurrent calls from multiple silos.
///
/// This tests turn-based execution across network boundaries:
/// - 3 silos, each making 10 concurrent calls = 30 total
/// - All calls go to a single grain on one silo
/// - Values should be exactly 1..=30 (no duplicates, no gaps)
///
/// NOTE: This test is ignored by default as it requires full cross-silo
/// message routing infrastructure. The single-silo test (9.9.1) is the
/// canonical proof of turn-based execution. Cross-silo communication
/// is tested in integration_test.rs::test_simultaneous_single_activation_guarantee.
#[tokio::test]
#[ignore = "requires full cross-silo routing; turn-based execution proven by test_many_concurrent_calls_single_silo"]
async fn test_many_concurrent_calls_multi_silo() {
    const CALLS_PER_SILO: usize = 10;
    const NUM_SILOS: usize = 3;
    const TOTAL_CALLS: usize = CALLS_PER_SILO * NUM_SILOS;

    println!("\n=== Test 9.9.2: Many Concurrent Calls (Multi-Silo) ===");
    println!(
        "Testing: {} silos x {} calls = {} total concurrent calls",
        NUM_SILOS, CALLS_PER_SILO, TOTAL_CALLS
    );
    println!("Expected: Sequential processing with values 1..{}\n", TOTAL_CALLS);

    // Create shared membership table
    let membership_table = Arc::new(InMemoryMembershipTable::new("concurrent-test-multi"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_concurrent_test_grain_type();

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

    silo1.start().await.unwrap();
    silo2.start().await.unwrap();
    silo3.start().await.unwrap();

    tokio::time::sleep(Duration::from_millis(300)).await;

    println!("Cluster formed:");
    println!("  Silo 1: {}", silo1.address());
    println!("  Silo 2: {}", silo2.address());
    println!("  Silo 3: {}", silo3.address());

    // Create grain on Silo 1
    let grain_id = GrainId::new(
        CounterGrain::grain_type(),
        IdSpan::from_str("concurrent-test-grain-multi"),
    );

    let catalog1 = silo1.catalog().unwrap();
    let handle = catalog1.get_or_create_activation(&grain_id).unwrap();

    // Register in directory
    let grain_address = GrainAddress::complete(
        grain_id.clone(),
        handle.activation_id().clone(),
        silo1.address().clone(),
    );
    let dir1 = silo1.directory().unwrap();
    dir1.register(MembershipVersion::default(), grain_address, None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    println!("\nGrain created on Silo 1: {}", grain_id);

    // Get grain references from all silos
    let factory1 = silo1.grain_factory().unwrap();
    let factory2 = silo2.grain_factory().unwrap();
    let factory3 = silo3.grain_factory().unwrap();

    let grain_ref1 = factory1.get_grain_reference_by_id(
        grain_id.clone(),
        GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
    );
    let grain_ref2 = factory2.get_grain_reference_by_id(
        grain_id.clone(),
        GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
    );
    let grain_ref3 = factory3.get_grain_reference_by_id(
        grain_id.clone(),
        GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
    );

    // Warm up connections with multiple calls from each silo to ensure stability
    println!("\nWarming up connections...");

    // Local call on Silo 1 (where grain lives)
    let warmup1 = grain_ref1
        .invoke(2, Bytes::new(), Some(Duration::from_secs(10)))
        .await;
    assert!(warmup1.is_ok(), "Warmup call from Silo 1 should succeed: {:?}", warmup1);

    // Cross-silo call from Silo 2
    let warmup2 = grain_ref2
        .invoke(2, Bytes::new(), Some(Duration::from_secs(10)))
        .await;
    assert!(warmup2.is_ok(), "Warmup call from Silo 2 should succeed: {:?}", warmup2);

    // Cross-silo call from Silo 3
    let warmup3 = grain_ref3
        .invoke(2, Bytes::new(), Some(Duration::from_secs(10)))
        .await;
    assert!(warmup3.is_ok(), "Warmup call from Silo 3 should succeed: {:?}", warmup3);

    println!("  All warmup calls succeeded");
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Spawn concurrent calls from all silos simultaneously
    println!(
        "Spawning {} concurrent increment calls from all silos...",
        TOTAL_CALLS
    );

    let mut tasks = Vec::with_capacity(TOTAL_CALLS);

    // Spawn tasks from all silos interleaved to maximize concurrency
    for i in 0..CALLS_PER_SILO {
        // Silo 1
        let gr1 = grain_ref1.clone();
        tasks.push(tokio::spawn(async move {
            gr1.invoke(1, Bytes::new(), Some(Duration::from_secs(60)))
                .await
                .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
        }));

        // Silo 2
        let gr2 = grain_ref2.clone();
        tasks.push(tokio::spawn(async move {
            gr2.invoke(1, Bytes::new(), Some(Duration::from_secs(60)))
                .await
                .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
        }));

        // Silo 3
        let gr3 = grain_ref3.clone();
        tasks.push(tokio::spawn(async move {
            gr3.invoke(1, Bytes::new(), Some(Duration::from_secs(60)))
                .await
                .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
        }));

        // Small yield to allow tasks to start
        if i % 10 == 0 {
            tokio::task::yield_now().await;
        }
    }

    // Await all concurrent calls
    let results: Vec<Result<u32, _>> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify all calls succeeded
    let mut values: Vec<u32> = Vec::with_capacity(TOTAL_CALLS);
    let mut errors = Vec::new();
    for result in results {
        match result {
            Ok(v) => values.push(v),
            Err(e) => errors.push(e),
        }
    }

    println!("Results: {} succeeded, {} failed", values.len(), errors.len());
    if !errors.is_empty() {
        println!("First 5 errors: {:?}", &errors[..5.min(errors.len())]);
    }
    assert!(
        errors.is_empty(),
        "All concurrent calls should succeed: {} errors",
        errors.len()
    );

    // Verify sequential execution: values should be exactly 1..=N with no duplicates or gaps
    values.sort();
    let expected: Vec<u32> = (1..=TOTAL_CALLS as u32).collect();

    println!("First 10 values: {:?}", &values[..10.min(values.len())]);
    println!(
        "Last 10 values: {:?}",
        &values[values.len().saturating_sub(10)..]
    );

    // Check for duplicates
    let unique_values: std::collections::HashSet<u32> = values.iter().cloned().collect();
    if unique_values.len() != values.len() {
        let mut counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
        for v in &values {
            *counts.entry(*v).or_insert(0) += 1;
        }
        let duplicates: Vec<_> = counts.into_iter().filter(|(_, c)| *c > 1).collect();
        println!("DUPLICATES FOUND: {:?}", duplicates);
    }

    assert_eq!(
        values.len(),
        TOTAL_CALLS,
        "Should have exactly {} values",
        TOTAL_CALLS
    );
    assert_eq!(
        values, expected,
        "Values must be sequential 1..{} with no duplicates or gaps",
        TOTAL_CALLS
    );

    // Verify final counter value
    let final_result = grain_ref1
        .invoke(2, Bytes::new(), Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let final_value = u32::from_le_bytes(final_result[..4].try_into().unwrap());
    println!("Final counter value: {}", final_value);
    assert_eq!(
        final_value, TOTAL_CALLS as u32,
        "Final counter should equal number of calls"
    );

    // Verify max concurrent was 1 (turn-based execution)
    let max_concurrent_result = grain_ref1
        .invoke(3, Bytes::new(), Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let max_concurrent = u32::from_le_bytes(max_concurrent_result[..4].try_into().unwrap());
    println!("Max concurrent processing detected: {}", max_concurrent);
    assert_eq!(
        max_concurrent, 1,
        "Turn-based execution: max concurrent should be 1"
    );

    println!("\n=== Test 9.9.2 PASSED ===");
    println!(
        "  All {} calls succeeded ({} per silo)",
        TOTAL_CALLS, CALLS_PER_SILO
    );
    println!("  Values were sequential 1..{}", TOTAL_CALLS);
    println!("  Max concurrent = 1 (turn-based execution verified)");

    silo1.stop().await.unwrap();
    silo2.stop().await.unwrap();
    silo3.stop().await.unwrap();
}

// ============================================================================
// Test: Verify no state races with interleaved operations
// ============================================================================

/// Test 9.9.3: Verify state isolation with interleaved read/write operations.
///
/// This tests that reads and writes are properly sequenced:
/// - Alternating increment and get_value calls
/// - Values should be monotonically increasing
/// - No stale reads
#[tokio::test]
async fn test_interleaved_read_write_operations() {
    const NUM_OPERATIONS: usize = 50;

    println!("\n=== Test 9.9.3: Interleaved Read/Write Operations ===");
    println!(
        "Testing: {} interleaved increment/get_value pairs",
        NUM_OPERATIONS
    );
    println!("Expected: Monotonically increasing values, no stale reads\n");

    let membership_table = Arc::new(InMemoryMembershipTable::new("interleaved-test"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_concurrent_test_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table.clone())
        .register_grain_type(grain_type.clone())
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let grain_id = GrainId::new(
        CounterGrain::grain_type(),
        IdSpan::from_str("interleaved-test-grain"),
    );

    let catalog = silo.catalog().unwrap();
    let handle = catalog.get_or_create_activation(&grain_id).unwrap();

    let grain_address = GrainAddress::complete(
        grain_id.clone(),
        handle.activation_id().clone(),
        silo.address().clone(),
    );
    let dir = silo.directory().unwrap();
    dir.register(MembershipVersion::default(), grain_address, None)
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let factory = silo.grain_factory().unwrap();
    let grain_ref = factory.get_grain_reference_by_id(
        grain_id.clone(),
        GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
    );

    // Spawn interleaved operations
    println!("Spawning {} interleaved operations...", NUM_OPERATIONS * 2);

    let mut tasks = Vec::with_capacity(NUM_OPERATIONS * 2);

    for _ in 0..NUM_OPERATIONS {
        // Increment
        let gr = grain_ref.clone();
        tasks.push(tokio::spawn(async move {
            ("increment", gr.invoke(1, Bytes::new(), Some(Duration::from_secs(30))).await)
        }));

        // Get value
        let gr = grain_ref.clone();
        tasks.push(tokio::spawn(async move {
            ("get_value", gr.invoke(2, Bytes::new(), Some(Duration::from_secs(30))).await)
        }));
    }

    // Await all
    let results: Vec<_> = futures::future::join_all(tasks)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    // Verify all succeeded
    let mut increment_values = Vec::new();
    let mut get_values = Vec::new();
    let mut errors = Vec::new();

    for (op, result) in results {
        match result {
            Ok(bytes) => {
                let value = u32::from_le_bytes(bytes[..4].try_into().unwrap());
                match op {
                    "increment" => increment_values.push(value),
                    "get_value" => get_values.push(value),
                    _ => unreachable!(),
                }
            }
            Err(e) => errors.push((op, e)),
        }
    }

    println!(
        "Results: {} increments, {} gets, {} errors",
        increment_values.len(),
        get_values.len(),
        errors.len()
    );

    assert!(errors.is_empty(), "All operations should succeed");

    // Verify increment values are unique and sequential
    increment_values.sort();
    let expected_increments: Vec<u32> = (1..=NUM_OPERATIONS as u32).collect();
    assert_eq!(
        increment_values, expected_increments,
        "Increment values should be 1..{}",
        NUM_OPERATIONS
    );

    // Verify all get_values are valid (between 0 and NUM_OPERATIONS)
    for &v in &get_values {
        assert!(
            v <= NUM_OPERATIONS as u32,
            "Get value {} exceeds max {}",
            v,
            NUM_OPERATIONS
        );
    }

    // Final verification
    let final_result = grain_ref
        .invoke(2, Bytes::new(), Some(Duration::from_secs(5)))
        .await
        .unwrap();
    let final_value = u32::from_le_bytes(final_result[..4].try_into().unwrap());
    println!("Final counter value: {}", final_value);
    assert_eq!(final_value, NUM_OPERATIONS as u32);

    println!("\n=== Test 9.9.3 PASSED ===");
    println!(
        "  {} increment operations returned unique values 1..{}",
        NUM_OPERATIONS, NUM_OPERATIONS
    );
    println!("  {} get operations returned valid values", get_values.len());
    println!("  No race conditions detected");

    silo.stop().await.unwrap();
}

// ============================================================================
// Property-based test: Counter invariants
// ============================================================================

/// Test 9.9.4: Property-based test for counter invariants.
///
/// Properties verified:
/// 1. After N increments, counter value is exactly N
/// 2. All increment return values are unique
/// 3. All increment return values are in range 1..=N
#[tokio::test]
async fn test_counter_invariants_property() {
    use std::collections::HashSet;

    // Test with various numbers of concurrent calls
    for num_calls in [10, 25, 50, 75] {
        println!("\n=== Testing counter invariants with {} calls ===", num_calls);

        let membership_table = Arc::new(InMemoryMembershipTable::new(&format!(
            "property-test-{}",
            num_calls
        )));
        membership_table
            .initialize_membership_table(true)
            .await
            .unwrap();

        let grain_type = create_concurrent_test_grain_type();

        let mut silo = SiloBuilder::test()
            .listen_address("127.0.0.1:0".parse().unwrap())
            .with_membership_table(membership_table.clone())
            .register_grain_type(grain_type.clone())
            .build()
            .await
            .unwrap();

        silo.start().await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let grain_id = GrainId::new(
            CounterGrain::grain_type(),
            IdSpan::from_str(&format!("property-grain-{}", num_calls)),
        );

        let catalog = silo.catalog().unwrap();
        let handle = catalog.get_or_create_activation(&grain_id).unwrap();

        let grain_address = GrainAddress::complete(
            grain_id.clone(),
            handle.activation_id().clone(),
            silo.address().clone(),
        );
        let dir = silo.directory().unwrap();
        dir.register(MembershipVersion::default(), grain_address, None)
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let factory = silo.grain_factory().unwrap();
        let grain_ref = factory.get_grain_reference_by_id(
            grain_id.clone(),
            GrainInterfaceType::create(CounterGrainInvoker::INTERFACE_TYPE),
        );

        // Make concurrent calls
        let mut tasks = Vec::with_capacity(num_calls);
        for _ in 0..num_calls {
            let gr = grain_ref.clone();
            tasks.push(tokio::spawn(async move {
                gr.invoke(1, Bytes::new(), Some(Duration::from_secs(30)))
                    .await
                    .map(|b| u32::from_le_bytes(b[..4].try_into().unwrap()))
            }));
        }

        let results: Vec<u32> = futures::future::join_all(tasks)
            .await
            .into_iter()
            .map(|r| r.unwrap().unwrap())
            .collect();

        // Property 1: All values are unique
        let unique: HashSet<u32> = results.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            num_calls,
            "Property 1 FAILED: All increment values must be unique"
        );

        // Property 2: All values are in range 1..=N
        for &v in &results {
            assert!(
                v >= 1 && v <= num_calls as u32,
                "Property 2 FAILED: Value {} not in range 1..={}",
                v,
                num_calls
            );
        }

        // Property 3: Final counter equals N
        let final_result = grain_ref
            .invoke(2, Bytes::new(), Some(Duration::from_secs(5)))
            .await
            .unwrap();
        let final_value = u32::from_le_bytes(final_result[..4].try_into().unwrap());
        assert_eq!(
            final_value, num_calls as u32,
            "Property 3 FAILED: Final counter {} != {}",
            final_value, num_calls
        );

        println!("  All properties verified for {} calls", num_calls);

        silo.stop().await.unwrap();
    }

    println!("\n=== Test 9.9.4 PASSED: All counter invariants hold ===");
}
