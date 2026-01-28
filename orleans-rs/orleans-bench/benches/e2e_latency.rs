//! End-to-end latency benchmarks.
//!
//! This benchmark measures the complete request-response cycle overhead:
//! - Message creation + serialization + deserialization
//! - Simulated grain method invocation overhead
//! - Async context switching overhead

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use dashmap::DashMap;
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use orleans_messaging::{encode_message, decode_message, GrainInterfaceType, Message};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use tokio::runtime::Runtime;

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

/// Benchmark the full request-response message cycle.
fn bench_request_response_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("request_response_cycle");

    let target_grain = test_grain_id(1);
    let sending_silo = test_silo_address(30000);
    let response_silo = test_silo_address(30001);
    let interface_type = GrainInterfaceType::create("ITestGrain");

    // Different payload sizes
    let sizes = vec![
        (64, "64B"),
        (256, "256B"),
        (1024, "1KB"),
    ];

    for (size, name) in sizes {
        let request_body = Bytes::from(vec![0u8; size]);
        let response_body = Bytes::from(vec![1u8; size / 2]);

        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("full_cycle", name),
            &size,
            |b, _| {
                b.iter(|| {
                    // 1. Create request
                    let request = Message::new_request(
                        target_grain.clone(),
                        interface_type.clone(),
                        1,
                        request_body.clone(),
                        sending_silo.clone(),
                    );

                    // 2. Serialize request
                    let encoded_request = encode_message(&request).unwrap();

                    // 3. Deserialize request (receiver side)
                    let received_request = decode_message(encoded_request.as_ref()).unwrap();

                    // 4. Create response
                    let response = received_request.create_response(response_body.clone(), response_silo.clone());

                    // 5. Serialize response
                    let encoded_response = encode_message(&response).unwrap();

                    // 6. Deserialize response (sender side)
                    let _received_response = decode_message(encoded_response.as_ref()).unwrap();
                });
            },
        );
    }

    group.finish();
}

/// Benchmark simulated grain method invocation overhead.
fn bench_grain_invocation_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("grain_invocation_overhead");

    // Simulate the overhead of grain method dispatch
    // without actual async execution

    #[derive(Clone)]
    struct MockGrain {
        counter: i64,
    }

    impl MockGrain {
        fn increment(&mut self) -> i64 {
            self.counter += 1;
            self.counter
        }
    }

    let mut grain = MockGrain { counter: 0 };

    // Simple method call
    group.bench_function("simple_method_call", |b| {
        b.iter(|| black_box(&mut grain).increment());
    });

    // Method call with serialization
    let body = Bytes::from(vec![0u8; 8]);
    group.bench_function("method_with_args_deser", |b| {
        b.iter(|| {
            // Simulate deserializing args (just read the bytes)
            let _args: &[u8] = &body[..];
            // Call method
            let result = grain.increment();
            // Simulate serializing result
            result.to_le_bytes()
        });
    });

    group.finish();
}

/// Benchmark async context switching overhead.
fn bench_async_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("async_overhead");

    let rt = Runtime::new().unwrap();

    // Measure spawn + await overhead
    group.bench_function("tokio_spawn_simple", |b| {
        b.to_async(&rt).iter(|| async {
            tokio::spawn(async {
                black_box(42)
            }).await.unwrap()
        });
    });

    // Oneshot channel (typical for request-response)
    group.bench_function("oneshot_channel_roundtrip", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, rx) = tokio::sync::oneshot::channel();
            tx.send(black_box(42)).unwrap();
            rx.await.unwrap()
        });
    });

    group.finish();
}

/// Benchmark complete simulated grain call with lookup.
fn bench_simulated_grain_call(c: &mut Criterion) {
    let mut group = c.benchmark_group("simulated_grain_call");

    // Create a simple cache
    let cache: HashMap<GrainId, GrainAddress> = (0..10000)
        .map(|i| (test_grain_id(i), test_grain_address(i)))
        .collect();

    let sending_silo = test_silo_address(30000);
    let interface_type = GrainInterfaceType::create("ITestGrain");
    let target_id = test_grain_id(5000);

    group.bench_function("local_grain_call_simulation", |b| {
        b.iter(|| {
            // 1. Lookup grain location
            let addr = cache.get(&target_id).unwrap();

            // 2. Check if local (would be a simple comparison)
            let silo = addr.silo_address();
            let is_local = silo == Some(&sending_silo);
            black_box(is_local);

            // 3. Create request message
            let body = Bytes::from(vec![0u8; 64]);
            let request = Message::new_request(
                target_id.clone(),
                interface_type.clone(),
                1,
                body,
                sending_silo.clone(),
            );

            // 4. If local, serialize/deserialize is not needed
            // Just measure the overhead of message creation
            black_box(&request);

            // 5. Simulate response
            let response_body = Bytes::from(vec![1u8; 32]);
            let response_silo = test_silo_address(30001);
            let response = request.create_response(response_body, response_silo);
            black_box(&response);
        });
    });

    group.finish();
}

/// Benchmark concurrent request handling simulation.
fn bench_concurrent_requests(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_requests");

    let rt = Runtime::new().unwrap();

    let cache: Arc<DashMap<GrainId, GrainAddress>> = Arc::new(
        (0..10000)
            .map(|i| (test_grain_id(i), test_grain_address(i)))
            .collect()
    );

    // 10 concurrent requests
    group.throughput(Throughput::Elements(10));
    group.bench_function("10_concurrent_lookups", |b| {
        b.to_async(&rt).iter(|| {
            let cache = cache.clone();
            async move {
                let mut handles = Vec::with_capacity(10);
                for i in 0..10 {
                    let cache = cache.clone();
                    handles.push(tokio::spawn(async move {
                        let id = test_grain_id(i * 1000);
                        cache.get(&id).is_some()
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            }
        });
    });

    // 100 concurrent requests
    group.throughput(Throughput::Elements(100));
    group.bench_function("100_concurrent_lookups", |b| {
        b.to_async(&rt).iter(|| {
            let cache = cache.clone();
            async move {
                let mut handles = Vec::with_capacity(100);
                for i in 0..100 {
                    let cache = cache.clone();
                    handles.push(tokio::spawn(async move {
                        let id = test_grain_id(i * 100);
                        cache.get(&id).is_some()
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_request_response_cycle,
    bench_grain_invocation_overhead,
    bench_async_overhead,
    bench_simulated_grain_call,
    bench_concurrent_requests,
);

criterion_main!(benches);
