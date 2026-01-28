//! Activation benchmarks.
//!
//! This benchmark measures the performance of grain activation operations:
//! - ActivationId generation
//! - GrainType and GrainId operations
//! - Catalog-related data structures

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Create a test GrainId.
fn test_grain_id(i: usize) -> GrainId {
    GrainId::new(
        GrainType::create(&format!("test.grain.{}", i % 10)),
        IdSpan::from_str(&format!("key-{}", i)),
    )
}

/// Create a test SiloAddress.
fn test_silo_address() -> SiloAddress {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 30000));
    SiloAddress::new(addr, 1)
}

/// Create a test GrainAddress.
fn test_grain_address(i: usize) -> GrainAddress {
    let grain_id = test_grain_id(i);
    let activation_id = ActivationId::get_deterministic(&grain_id);
    let silo_address = test_silo_address();
    GrainAddress::new(grain_id, activation_id, Some(silo_address))
}

/// Benchmark ActivationId operations.
fn bench_activation_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("activation_id");

    // Random creation
    group.bench_function("new_random", |b| {
        b.iter(|| ActivationId::new());
    });

    // Deterministic creation
    let grain_id = test_grain_id(1);
    group.bench_function("get_deterministic", |b| {
        b.iter(|| ActivationId::get_deterministic(black_box(&grain_id)));
    });

    // Hash computation
    let id = ActivationId::new();
    group.bench_function("hash", |b| {
        b.iter(|| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            black_box(&id).hash(&mut hasher);
            hasher.finish()
        });
    });

    // Equality comparison
    let id1 = ActivationId::new();
    let id2 = ActivationId::new();
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&id1) == black_box(&id2));
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&id1).clone());
    });

    group.finish();
}

/// Benchmark GrainType operations (used in catalog).
fn bench_grain_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_type");

    // Creation with different name lengths
    group.bench_function("create_short", |b| {
        b.iter(|| GrainType::create(black_box("MyGrain")));
    });

    group.bench_function("create_qualified", |b| {
        b.iter(|| GrainType::create(black_box("MyApp.Grains.CounterGrain")));
    });

    // Hash computation
    let grain_type = GrainType::create("MyApp.Grains.CounterGrain");
    group.bench_function("get_hash", |b| {
        b.iter(|| black_box(&grain_type).get_hash_code());
    });

    // Equality
    let type1 = GrainType::create("MyGrain");
    let type2 = GrainType::create("MyGrain");
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&type1) == black_box(&type2));
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&grain_type).clone());
    });

    group.finish();
}

/// Benchmark batch GrainId generation (simulating catalog population).
fn bench_batch_grain_id_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_grain_id");

    for count in [10, 100, 1000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("generate", count),
            &count,
            |b, &count| {
                b.iter(|| {
                    let mut ids = Vec::with_capacity(count);
                    for i in 0..count {
                        ids.push(test_grain_id(black_box(i)));
                    }
                    ids
                });
            },
        );
    }

    group.finish();
}

/// Benchmark GrainId lookup operations (simulating catalog lookups).
fn bench_grain_id_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_id_lookup");

    // Prepare lookup table
    let grain_ids: Vec<GrainId> = (0..10000).map(test_grain_id).collect();
    let lookup_table: HashMap<GrainId, usize> = grain_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();

    // Lookup existing key
    let target = test_grain_id(5000);
    group.bench_function("hashmap_lookup_hit", |b| {
        b.iter(|| lookup_table.get(black_box(&target)));
    });

    // Lookup missing key
    let missing = test_grain_id(20000);
    group.bench_function("hashmap_lookup_miss", |b| {
        b.iter(|| lookup_table.get(black_box(&missing)));
    });

    // Lookup with DashMap (concurrent map)
    let concurrent_table: DashMap<GrainId, usize> = grain_ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.clone(), i))
        .collect();

    group.bench_function("dashmap_lookup_hit", |b| {
        b.iter(|| concurrent_table.get(black_box(&target)));
    });

    group.bench_function("dashmap_lookup_miss", |b| {
        b.iter(|| concurrent_table.get(black_box(&missing)));
    });

    group.finish();
}

/// Benchmark Arc operations (used extensively in catalog and activation).
fn bench_arc_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("arc_operations");

    // Simple data
    let data = Arc::new(42u64);
    group.bench_function("clone_u64", |b| {
        b.iter(|| black_box(&data).clone());
    });

    // GrainId in Arc
    let grain_id = Arc::new(test_grain_id(1));
    group.bench_function("clone_grain_id", |b| {
        b.iter(|| black_box(&grain_id).clone());
    });

    // Large data
    let large_data = Arc::new(vec![0u8; 1024]);
    group.bench_function("clone_vec_1kb", |b| {
        b.iter(|| black_box(&large_data).clone());
    });

    // Deref
    group.bench_function("deref", |b| {
        b.iter(|| **black_box(&data));
    });

    // Strong count
    group.bench_function("strong_count", |b| {
        b.iter(|| Arc::strong_count(black_box(&data)));
    });

    group.finish();
}

/// Benchmark synchronization primitives used in activation.
fn bench_synchronization(c: &mut Criterion) {
    let mut group = c.benchmark_group("synchronization");

    // Atomic operations
    let atomic = AtomicU64::new(0);
    group.bench_function("atomic_load", |b| {
        b.iter(|| atomic.load(black_box(Ordering::Acquire)));
    });

    group.bench_function("atomic_store", |b| {
        b.iter(|| atomic.store(black_box(42), Ordering::Release));
    });

    group.bench_function("atomic_fetch_add", |b| {
        b.iter(|| atomic.fetch_add(black_box(1), Ordering::Relaxed));
    });

    group.bench_function("atomic_compare_exchange", |b| {
        b.iter(|| {
            let _ = atomic.compare_exchange(
                black_box(0),
                black_box(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        });
    });

    // Mutex operations
    let mutex = Mutex::new(0u64);
    group.bench_function("mutex_lock_uncontended", |b| {
        b.iter(|| {
            let guard = mutex.lock();
            *black_box(guard)
        });
    });

    // RwLock operations
    let rwlock = RwLock::new(0u64);
    group.bench_function("rwlock_read_uncontended", |b| {
        b.iter(|| {
            let guard = rwlock.read();
            *black_box(guard)
        });
    });

    group.bench_function("rwlock_write_uncontended", |b| {
        b.iter(|| {
            let mut guard = rwlock.write();
            *guard = black_box(42);
        });
    });

    group.finish();
}

/// Benchmark GrainAddress operations.
fn bench_grain_address(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_address");

    // Creation
    group.bench_function("create", |b| {
        b.iter(|| test_grain_address(black_box(1)));
    });

    // Clone
    let addr = test_grain_address(1);
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&addr).clone());
    });

    // is_complete
    group.bench_function("is_complete", |b| {
        b.iter(|| black_box(&addr).is_complete());
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_activation_id,
    bench_grain_type,
    bench_batch_grain_id_generation,
    bench_grain_id_lookup,
    bench_arc_operations,
    bench_synchronization,
    bench_grain_address,
);

criterion_main!(benches);
