//! Serialization benchmarks.
//!
//! This benchmark measures the performance of Orleans-RS serialization:
//! - VarInt encoding/decoding
//! - Primitive type serialization
//! - Identity type creation and hashing

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use orleans_serialization::{read_varint, write_varint, Writer};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

/// Benchmark VarInt encoding for different value sizes.
fn bench_varint_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint_encode");

    // Test different value sizes
    let values: Vec<(u64, &str)> = vec![
        (0, "zero"),
        (127, "1-byte"),
        (16383, "2-byte"),
        (2097151, "3-byte"),
        (268435455, "4-byte"),
        (u64::MAX, "10-byte"),
    ];

    for (value, name) in values {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("encode", name), &value, |b, &val| {
            let mut buffer = [0u8; 16];
            b.iter(|| {
                write_varint(black_box(&mut buffer), black_box(val))
            });
        });
    }

    group.finish();
}

/// Benchmark VarInt decoding for different value sizes.
fn bench_varint_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint_decode");

    let values: Vec<(u64, &str)> = vec![
        (0, "zero"),
        (127, "1-byte"),
        (16383, "2-byte"),
        (2097151, "3-byte"),
        (268435455, "4-byte"),
        (u64::MAX, "10-byte"),
    ];

    for (value, name) in values {
        // Pre-encode the value
        let mut buffer = [0u8; 16];
        let len = write_varint(&mut buffer, value);
        let encoded = &buffer[..len];

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("decode", name), &encoded.to_vec(), |b, data| {
            b.iter(|| {
                read_varint(black_box(data.as_slice()))
            });
        });
    }

    group.finish();
}

/// Benchmark Writer operations.
fn bench_writer_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("writer");

    // Writer creation
    group.bench_function("new", |b| {
        b.iter(|| Writer::new());
    });

    // Writer with capacity
    group.bench_function("with_capacity_1kb", |b| {
        b.iter(|| Writer::with_capacity(black_box(1024)));
    });

    // Writer reset
    group.bench_function("reset", |b| {
        let mut writer = Writer::new();
        b.iter(|| {
            writer.reset();
        });
    });

    group.finish();
}

/// Benchmark IdSpan creation and hashing.
fn bench_id_span(c: &mut Criterion) {
    let mut group = c.benchmark_group("id_span");

    // Creation
    group.bench_function("create_short", |b| {
        b.iter(|| IdSpan::from_str(black_box("key123")));
    });

    group.bench_function("create_long", |b| {
        let key = "this-is-a-very-long-key-for-testing-purposes-1234567890";
        b.iter(|| IdSpan::from_str(black_box(key)));
    });

    // Hash computation
    let id_span = IdSpan::from_str("test-key-123");
    group.bench_function("get_hash", |b| {
        b.iter(|| black_box(&id_span).get_hash_code());
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&id_span).clone());
    });

    // Equality
    let id_span2 = IdSpan::from_str("test-key-123");
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&id_span) == black_box(&id_span2));
    });

    group.finish();
}

/// Benchmark GrainType operations.
fn bench_grain_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_type");

    // Creation
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

/// Benchmark GrainId creation and operations.
fn bench_grain_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_id");

    // Creation
    group.bench_function("create", |b| {
        let grain_type = GrainType::create("my.grain.type");
        let key = IdSpan::from_str("key-123");
        b.iter(|| GrainId::new(black_box(grain_type.clone()), black_box(key.clone())));
    });

    // Hash computation
    let grain_id = GrainId::new(
        GrainType::create("my.grain.type"),
        IdSpan::from_str("key-123"),
    );
    group.bench_function("get_uniform_hash", |b| {
        b.iter(|| black_box(&grain_id).get_uniform_hash_code());
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&grain_id).clone());
    });

    // Equality
    let grain_id2 = GrainId::new(
        GrainType::create("my.grain.type"),
        IdSpan::from_str("key-123"),
    );
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&grain_id) == black_box(&grain_id2));
    });

    group.finish();
}

/// Benchmark SiloAddress creation and operations.
fn bench_silo_address(c: &mut Criterion) {
    let mut group = c.benchmark_group("silo_address");

    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 30000));

    // Creation
    group.bench_function("create", |b| {
        b.iter(|| SiloAddress::new(black_box(addr), black_box(12345)));
    });

    // Hash computation
    let silo_addr = SiloAddress::new(addr, 12345);
    group.bench_function("get_hash", |b| {
        b.iter(|| black_box(&silo_addr).get_hash_code());
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&silo_addr).clone());
    });

    // Equality
    let silo_addr2 = SiloAddress::new(addr, 12345);
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&silo_addr) == black_box(&silo_addr2));
    });

    group.finish();
}

/// Benchmark ActivationId operations.
fn bench_activation_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("activation_id");

    // Random creation
    group.bench_function("new_random", |b| {
        b.iter(|| ActivationId::new());
    });

    // Deterministic creation
    let grain_id = GrainId::new(
        GrainType::create("my.grain"),
        IdSpan::from_str("key-1"),
    );
    group.bench_function("get_deterministic", |b| {
        b.iter(|| ActivationId::get_deterministic(black_box(&grain_id)));
    });

    // Clone
    let id = ActivationId::new();
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&id).clone());
    });

    group.finish();
}

/// Benchmark GrainAddress (full composite type).
fn bench_grain_address(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_address");

    let grain_id = GrainId::new(
        GrainType::create("my.grain.type"),
        IdSpan::from_str("key-123"),
    );
    let activation_id = ActivationId::new();
    let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 30000));
    let silo_addr = SiloAddress::new(addr, 12345);

    // Creation
    group.bench_function("create", |b| {
        b.iter(|| {
            GrainAddress::new(
                black_box(grain_id.clone()),
                black_box(activation_id.clone()),
                black_box(Some(silo_addr.clone())),
            )
        });
    });

    let grain_address = GrainAddress::new(grain_id.clone(), activation_id.clone(), Some(silo_addr.clone()));

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&grain_address).clone());
    });

    // is_complete check
    group.bench_function("is_complete", |b| {
        b.iter(|| black_box(&grain_address).is_complete());
    });

    group.finish();
}

/// Benchmark batch identity type generation.
fn bench_batch_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_generation");

    // Generate GrainIds
    group.throughput(Throughput::Elements(100));
    group.bench_function("100_grain_ids", |b| {
        b.iter(|| {
            let mut ids = Vec::with_capacity(100);
            for i in 0..100 {
                let grain_type = GrainType::create(&format!("grain.type.{}", i % 10));
                let key = IdSpan::from_str(&format!("key-{}", i));
                ids.push(GrainId::new(grain_type, key));
            }
            ids
        });
    });

    // Generate SiloAddresses
    group.bench_function("100_silo_addresses", |b| {
        b.iter(|| {
            let mut addrs = Vec::with_capacity(100);
            for i in 0..100 {
                let addr = SocketAddr::V4(SocketAddrV4::new(
                    Ipv4Addr::new(10, 0, (i / 256) as u8, (i % 256) as u8),
                    30000 + (i % 1000) as u16,
                ));
                addrs.push(SiloAddress::new(addr, (i + 1) as i64));
            }
            addrs
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_varint_encode,
    bench_varint_decode,
    bench_writer_operations,
    bench_id_span,
    bench_grain_type,
    bench_grain_id,
    bench_silo_address,
    bench_activation_id,
    bench_grain_address,
    bench_batch_generation,
);

criterion_main!(benches);
