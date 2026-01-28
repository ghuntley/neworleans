//! Directory benchmarks.
//!
//! This benchmark measures the performance of grain directory operations:
//! - RingRange operations
//! - GrainId hash distribution
//! - Concurrent directory lookups

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use orleans_clustering::MembershipVersion;
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use orleans_directory::{GrainDirectoryPartition, RingRange};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// Create a test GrainId.
fn test_grain_id(i: usize) -> GrainId {
    GrainId::new(
        GrainType::create(&format!("test.grain.{}", i % 10)),
        IdSpan::from_str(&format!("key-{}", i)),
    )
}

/// Create a test SiloAddress.
fn test_silo_address(port: u16) -> SiloAddress {
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), port));
    SiloAddress::new(addr, 1)
}

/// Create a test GrainAddress.
fn test_grain_address(i: usize) -> GrainAddress {
    let grain_id = test_grain_id(i);
    let activation_id = ActivationId::get_deterministic(&grain_id);
    let silo_address = test_silo_address(30000 + (i % 10) as u16);
    GrainAddress::new(grain_id, activation_id, Some(silo_address))
}

/// Benchmark RingRange operations.
fn bench_ring_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_range");

    // Creation
    group.bench_function("single", |b| {
        b.iter(|| RingRange::single(black_box(1000), black_box(5000)));
    });

    group.bench_function("empty", |b| {
        b.iter(|| RingRange::empty());
    });

    group.bench_function("full", |b| {
        b.iter(|| RingRange::full());
    });

    // Contains check
    let range = RingRange::single(1000, 5000);
    group.bench_function("contains_in_range", |b| {
        b.iter(|| range.contains(black_box(3000)));
    });

    group.bench_function("contains_out_of_range", |b| {
        b.iter(|| range.contains(black_box(7000)));
    });

    // Wraparound range
    let wrap_range = RingRange::single(u32::MAX - 1000, 1000);
    group.bench_function("contains_wraparound", |b| {
        b.iter(|| wrap_range.contains(black_box(500)));
    });

    group.finish();
}

/// Benchmark GrainDirectoryPartition operations.
fn bench_grain_directory_partition(c: &mut Criterion) {
    let mut group = c.benchmark_group("directory_partition");

    // Lookup - prepare partition with entries
    let partition = GrainDirectoryPartition::new(test_silo_address(30000));
    for i in 0..10000 {
        let addr = test_grain_address(i);
        let _ = partition.register(MembershipVersion::default(), addr, None);
    }

    // Hit
    let grain_id = test_grain_id(5000);
    group.bench_function("lookup_hit", |b| {
        b.iter(|| partition.lookup(black_box(&grain_id)));
    });

    // Miss
    let missing_id = test_grain_id(20000);
    group.bench_function("lookup_miss", |b| {
        b.iter(|| partition.lookup(black_box(&missing_id)));
    });

    group.finish();
}

/// Benchmark GrainId hash distribution quality.
fn bench_hash_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash_distribution");

    // Generate hashes
    group.throughput(Throughput::Elements(10000));
    group.bench_function("generate_10000_hashes", |b| {
        b.iter(|| {
            let mut hashes = Vec::with_capacity(10000);
            for i in 0..10000 {
                let grain_id = test_grain_id(i);
                hashes.push(grain_id.get_uniform_hash_code());
            }
            hashes
        });
    });

    // Hash computation
    let grain_id = test_grain_id(42);
    group.bench_function("single_hash", |b| {
        b.iter(|| black_box(&grain_id).get_uniform_hash_code());
    });

    group.finish();
}

/// Benchmark concurrent directory access patterns.
fn bench_concurrent_directory(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_directory");

    // Concurrent insert
    let map: DashMap<GrainId, GrainAddress> = DashMap::new();
    group.bench_function("dashmap_insert", |b| {
        let mut counter = 0usize;
        b.iter(|| {
            counter += 1;
            let grain_id = test_grain_id(counter);
            let addr = test_grain_address(counter);
            map.insert(black_box(grain_id), black_box(addr));
        });
    });

    // Prepare map with entries
    let map: DashMap<GrainId, GrainAddress> = DashMap::new();
    for i in 0..10000 {
        map.insert(test_grain_id(i), test_grain_address(i));
    }

    // Concurrent get
    let grain_id = test_grain_id(5000);
    group.bench_function("dashmap_get", |b| {
        b.iter(|| map.get(black_box(&grain_id)));
    });

    // Concurrent remove
    group.bench_function("dashmap_remove", |b| {
        let id = test_grain_id(5001);
        b.iter(|| map.remove(black_box(&id)));
    });

    // Entry API
    group.bench_function("dashmap_entry", |b| {
        let id = test_grain_id(5002);
        let addr = test_grain_address(5002);
        b.iter(|| {
            map.entry(black_box(id.clone()))
                .or_insert(black_box(addr.clone()));
        });
    });

    group.finish();
}

/// Benchmark HashMap vs DashMap lookup performance.
fn bench_map_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_comparison");

    // Prepare data
    let grain_ids: Vec<GrainId> = (0..10000).map(test_grain_id).collect();
    let grain_addresses: Vec<GrainAddress> = (0..10000).map(test_grain_address).collect();

    // HashMap
    let hashmap: HashMap<GrainId, GrainAddress> = grain_ids
        .iter()
        .zip(grain_addresses.iter())
        .map(|(id, addr)| (id.clone(), addr.clone()))
        .collect();

    // DashMap
    let dashmap: DashMap<GrainId, GrainAddress> = grain_ids
        .iter()
        .zip(grain_addresses.iter())
        .map(|(id, addr)| (id.clone(), addr.clone()))
        .collect();

    let target = test_grain_id(5000);

    group.bench_function("hashmap_get", |b| {
        b.iter(|| hashmap.get(black_box(&target)));
    });

    group.bench_function("dashmap_get", |b| {
        b.iter(|| dashmap.get(black_box(&target)));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ring_range,
    bench_grain_directory_partition,
    bench_hash_distribution,
    bench_concurrent_directory,
    bench_map_comparison,
);

criterion_main!(benches);
