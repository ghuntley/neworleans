//! Messaging benchmarks.
//!
//! This benchmark measures the performance of Orleans-RS messaging:
//! - Message creation overhead
//! - Message serialization/deserialization
//! - Correlation ID generation

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
use orleans_messaging::{encode_message, decode_message, CorrelationId, GrainInterfaceType, Message};
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

/// Benchmark CorrelationId generation.
fn bench_correlation_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("correlation_id");

    // Generation
    group.bench_function("generate", |b| {
        b.iter(|| CorrelationId::new());
    });

    // Hash computation
    let id = CorrelationId::new();
    group.bench_function("hash", |b| {
        b.iter(|| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            black_box(&id).hash(&mut hasher);
            hasher.finish()
        });
    });

    // Equality comparison
    let id1 = CorrelationId::new();
    let id2 = CorrelationId::new();
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&id1) == black_box(&id2));
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&id1).clone());
    });

    group.finish();
}

/// Benchmark Message creation.
fn bench_message_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_creation");

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");
    let body = Bytes::from(vec![0u8; 64]);

    // Request creation
    group.bench_function("new_request", |b| {
        b.iter(|| {
            Message::new_request(
                black_box(target_grain.clone()),
                black_box(interface_type.clone()),
                black_box(1),
                black_box(body.clone()),
                black_box(sending_silo.clone()),
            )
        });
    });

    // One-way message creation
    group.bench_function("new_one_way", |b| {
        b.iter(|| {
            Message::new_one_way(
                black_box(target_grain.clone()),
                black_box(interface_type.clone()),
                black_box(1),
                black_box(body.clone()),
                black_box(sending_silo.clone()),
            )
        });
    });

    // Response creation
    let request = Message::new_request(
        target_grain.clone(),
        interface_type.clone(),
        1,
        body.clone(),
        sending_silo.clone(),
    );
    group.bench_function("create_response", |b| {
        let response_body = Bytes::from(vec![0u8; 32]);
        let response_silo = test_silo_address(30001);
        b.iter(|| black_box(&request).create_response(black_box(response_body.clone()), black_box(response_silo.clone())));
    });

    group.finish();
}

/// Benchmark Message serialization with different body sizes.
fn bench_message_serialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_serialize");

    let sizes = vec![
        (0, "empty"),
        (64, "64B"),
        (256, "256B"),
        (1024, "1KB"),
        (4096, "4KB"),
        (16384, "16KB"),
    ];

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");

    for (size, name) in sizes {
        let body = Bytes::from(vec![0u8; size]);
        let message = Message::new_request(
            target_grain.clone(),
            interface_type.clone(),
            1,
            body,
            sending_silo.clone(),
        );

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("serialize", name), &message, |b, msg| {
            b.iter(|| encode_message(black_box(msg)));
        });
    }

    group.finish();
}

/// Benchmark Message deserialization with different body sizes.
fn bench_message_deserialize(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_deserialize");

    let sizes = vec![
        (0, "empty"),
        (64, "64B"),
        (256, "256B"),
        (1024, "1KB"),
        (4096, "4KB"),
        (16384, "16KB"),
    ];

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");

    for (size, name) in sizes {
        let body = Bytes::from(vec![0u8; size]);
        let message = Message::new_request(
            target_grain.clone(),
            interface_type.clone(),
            1,
            body,
            sending_silo.clone(),
        );
        let encoded = encode_message(&message).unwrap();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("deserialize", name), &encoded, |b, data| {
            b.iter(|| decode_message(black_box(data.as_ref())));
        });
    }

    group.finish();
}

/// Benchmark Message roundtrip serialization.
fn bench_message_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_roundtrip");

    let sizes = vec![
        (64, "64B"),
        (256, "256B"),
        (1024, "1KB"),
    ];

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");

    for (size, name) in sizes {
        let body = Bytes::from(vec![0u8; size]);
        let message = Message::new_request(
            target_grain.clone(),
            interface_type.clone(),
            1,
            body,
            sending_silo.clone(),
        );

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("roundtrip", name), &message, |b, msg| {
            b.iter(|| {
                let encoded = encode_message(black_box(msg)).unwrap();
                decode_message(encoded.as_ref()).unwrap()
            });
        });
    }

    group.finish();
}

/// Benchmark GrainInterfaceType operations.
fn bench_grain_interface_type(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_interface_type");

    // Creation
    group.bench_function("create", |b| {
        b.iter(|| GrainInterfaceType::create(black_box("ITestGrain")));
    });

    // Hash computation
    let interface_type = GrainInterfaceType::create("ITestGrain");
    group.bench_function("hash", |b| {
        b.iter(|| black_box(&interface_type).get_hash_code());
    });

    // Equality
    let type1 = GrainInterfaceType::create("ITestGrain");
    let type2 = GrainInterfaceType::create("ITestGrain");
    group.bench_function("equality", |b| {
        b.iter(|| black_box(&type1) == black_box(&type2));
    });

    // Clone
    group.bench_function("clone", |b| {
        b.iter(|| black_box(&interface_type).clone());
    });

    group.finish();
}

/// Benchmark Message field access.
fn bench_message_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_access");

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");
    let body = Bytes::from(vec![0u8; 64]);
    let message = Message::new_request(target_grain, interface_type, 1, body, sending_silo);

    // Field access benchmarks
    group.bench_function("id", |b| {
        b.iter(|| black_box(&message).id());
    });

    group.bench_function("target_grain", |b| {
        b.iter(|| black_box(&message).target_grain());
    });

    group.bench_function("direction", |b| {
        b.iter(|| black_box(&message).direction());
    });

    group.bench_function("body", |b| {
        b.iter(|| black_box(&message).body());
    });

    group.bench_function("is_request", |b| {
        b.iter(|| black_box(&message).is_request());
    });

    group.bench_function("is_one_way", |b| {
        b.iter(|| black_box(&message).is_one_way());
    });

    group.finish();
}

/// Benchmark batch message operations.
fn bench_batch_messages(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_messages");

    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");
    let body = Bytes::from(vec![0u8; 64]);

    // Create 100 messages
    let messages: Vec<Message> = (0..100)
        .map(|i| {
            Message::new_request(
                test_grain_id(i),
                interface_type.clone(),
                1,
                body.clone(),
                sending_silo.clone(),
            )
        })
        .collect();

    group.throughput(Throughput::Elements(100));
    group.bench_function("serialize_100", |b| {
        b.iter(|| {
            let mut encoded = Vec::with_capacity(100);
            for msg in &messages {
                encoded.push(encode_message(black_box(msg)).unwrap());
            }
            encoded
        });
    });

    // Pre-encode for deserialization benchmark
    let encoded: Vec<Bytes> = messages
        .iter()
        .map(|msg| encode_message(msg).unwrap())
        .collect();

    group.bench_function("deserialize_100", |b| {
        b.iter(|| {
            let mut decoded = Vec::with_capacity(100);
            for data in &encoded {
                decoded.push(decode_message(black_box(data.as_ref())).unwrap());
            }
            decoded
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_correlation_id,
    bench_message_creation,
    bench_message_serialize,
    bench_message_deserialize,
    bench_message_roundtrip,
    bench_grain_interface_type,
    bench_message_access,
    bench_batch_messages,
);

criterion_main!(benches);
