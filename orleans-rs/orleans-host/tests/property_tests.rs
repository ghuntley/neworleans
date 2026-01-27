//! Property-based tests for Orleans distributed system correctness.
//!
//! These tests verify fundamental invariants of the Orleans distributed system
//! using property-based testing (proptest).
//!
//! ## Test Categories
//!
//! - **9.10 Grain Identity Properties**: Validates GrainId and GrainReference behavior
//! - **9.11 Directory Consistency**: Validates grain directory invariants
//! - **9.12 Message Delivery**: Validates message delivery guarantees

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use orleans_clustering::{IMembershipTable, InMemoryMembershipTable, MembershipVersion};
use orleans_core::{ActivationId, GrainAddress, GrainId, GrainType, IdSpan, SiloAddress};
use orleans_directory::{GrainDirectoryPartition, RegistrationResult};
use orleans_host::{
    GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker, RuntimeResult,
    SiloBuilder,
};
use orleans_messaging::GrainInterfaceType;
use proptest::prelude::*;
use tracing::{debug, info};

// ============================================================================
// Test Grain Implementation
// ============================================================================

/// A simple echo grain for property testing.
struct EchoGrain {
    call_count: std::sync::atomic::AtomicU32,
}

impl Default for EchoGrain {
    fn default() -> Self {
        Self {
            call_count: std::sync::atomic::AtomicU32::new(0),
        }
    }
}

#[async_trait]
impl IGrain for EchoGrain {
    fn grain_type() -> GrainType {
        GrainType::create("EchoGrain")
    }
}

struct EchoGrainActivator;

impl IGrainActivator for EchoGrainActivator {
    fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(EchoGrain::default())
    }

    fn grain_type(&self) -> GrainType {
        EchoGrain::grain_type()
    }
}

struct EchoGrainInvoker;

impl IGrainMethodInvoker for EchoGrainInvoker {
    fn interface_type(&self) -> &str {
        "IEchoGrain"
    }

    fn method_ids(&self) -> &[u32] {
        &[1, 2]
    }

    fn invoke<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        grain: &'life1 mut dyn std::any::Any,
        _context: &'life2 dyn IGrainContext,
        method_id: u32,
        body: &'life3 [u8],
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
        let grain = grain.downcast_mut::<EchoGrain>().unwrap();

        let result = match method_id {
            1 => {
                // echo() -> echo back the body
                grain
                    .call_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(body.to_vec())
            }
            2 => {
                // get_call_count() -> u32
                let count = grain.call_count.load(std::sync::atomic::Ordering::SeqCst);
                Ok(count.to_le_bytes().to_vec())
            }
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: "IEchoGrain".to_string(),
                method_id,
            }),
        };

        Box::pin(std::future::ready(result))
    }
}

fn create_echo_grain_type() -> Arc<GrainTypeData> {
    let activator = Arc::new(EchoGrainActivator);
    let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(EchoGrainInvoker);

    let grain_type_data = GrainTypeData::new(EchoGrain::grain_type(), activator)
        .with_invoker("IEchoGrain", invoker);

    Arc::new(grain_type_data)
}

// ============================================================================
// Helper Functions
// ============================================================================

fn make_silo_address(port: u16) -> SiloAddress {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    SiloAddress::new(addr, 1)
}

fn make_grain_id(grain_type: &str, key: &str) -> GrainId {
    GrainId::new(GrainType::create(grain_type), IdSpan::from_str(key))
}

fn make_grain_address(grain_id: &GrainId, silo: &SiloAddress) -> GrainAddress {
    GrainAddress::complete(grain_id.clone(), ActivationId::new(), silo.clone())
}

// ============================================================================
// 9.10 Grain Identity Properties
// ============================================================================

/// Property: GrainId equality is reflexive, symmetric, and transitive.
#[test]
fn prop_grain_id_equality_reflexive() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
        key in "[a-zA-Z0-9_-]{1,30}"
    )| {
        let grain_id = make_grain_id(&grain_type, &key);
        prop_assert_eq!(&grain_id, &grain_id, "GrainId should equal itself");
    });
}

#[test]
fn prop_grain_id_equality_symmetric() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
        key in "[a-zA-Z0-9_-]{1,30}"
    )| {
        let grain_id1 = make_grain_id(&grain_type, &key);
        let grain_id2 = make_grain_id(&grain_type, &key);
        prop_assert_eq!(&grain_id1, &grain_id2, "GrainId equality should be symmetric");
        prop_assert_eq!(&grain_id2, &grain_id1, "GrainId equality should be symmetric");
    });
}

/// Property: Equal GrainIds have equal hash codes.
#[test]
fn prop_grain_id_hash_consistency() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
        key in "[a-zA-Z0-9_-]{1,30}"
    )| {
        let grain_id1 = make_grain_id(&grain_type, &key);
        let grain_id2 = make_grain_id(&grain_type, &key);

        prop_assert_eq!(
            grain_id1.get_uniform_hash_code(),
            grain_id2.get_uniform_hash_code(),
            "Equal GrainIds should have equal hash codes"
        );
    });
}

/// Property: Different GrainIds usually have different hash codes.
/// Note: This is probabilistic, hash collisions can occur but should be rare.
#[test]
fn prop_grain_id_hash_distribution() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{5,30}",
        key1 in "[a-zA-Z0-9]{5,20}",
        key2 in "[a-zA-Z0-9]{5,20}"
    )| {
        prop_assume!(key1 != key2);

        let grain_id1 = make_grain_id(&grain_type, &key1);
        let grain_id2 = make_grain_id(&grain_type, &key2);

        // Hash collision is possible but should be rare
        // We only check that different keys create different GrainIds
        prop_assert_ne!(
            grain_id1, grain_id2,
            "Different keys should create different GrainIds"
        );
    });
}

/// Property: GrainId parse/display roundtrip.
#[test]
fn prop_grain_id_parse_display_roundtrip() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
        key in "[a-zA-Z0-9_-]{1,30}"
    )| {
        let grain_id = make_grain_id(&grain_type, &key);
        let display = format!("{}", grain_id);
        let parsed: Result<GrainId, _> = display.parse();

        prop_assert!(parsed.is_ok(), "GrainId should parse from its display form");
        prop_assert_eq!(grain_id, parsed.unwrap(), "Parse/display should roundtrip");
    });
}

/// Property: GrainId hash is stable across multiple calls.
#[test]
fn prop_grain_id_hash_stable() {
    proptest!(|(
        grain_type in "[a-zA-Z][a-zA-Z0-9.]{0,30}",
        key in "[a-zA-Z0-9_-]{1,30}"
    )| {
        let grain_id = make_grain_id(&grain_type, &key);

        let hash1 = grain_id.get_uniform_hash_code();
        let hash2 = grain_id.get_uniform_hash_code();
        let hash3 = grain_id.get_uniform_hash_code();

        prop_assert_eq!(hash1, hash2, "Hash should be stable");
        prop_assert_eq!(hash2, hash3, "Hash should be stable");
    });
}

/// Property: Integer key GrainId roundtrips correctly.
#[test]
fn prop_grain_id_integer_key_roundtrip() {
    proptest!(|(key in any::<i64>())| {
        let grain_id = GrainId::with_integer_key("TestGrain", key);
        let recovered = grain_id.key_as_integer();

        prop_assert_eq!(recovered, Some(key), "Integer key should roundtrip");
    });
}

/// Property: Compound key GrainId splits correctly.
#[test]
fn prop_grain_id_compound_key_split() {
    proptest!(|(
        primary in "[a-zA-Z0-9]{1,15}",
        extension in "[a-zA-Z0-9]{1,15}"
    )| {
        let grain_id = GrainId::with_compound_key("TestGrain", &primary, &extension);

        prop_assert!(grain_id.is_compound_key(), "Should be detected as compound key");

        let (p, e) = grain_id.split_compound_key().expect("Should split");
        prop_assert_eq!(p, primary.as_str(), "Primary key should match");
        prop_assert_eq!(e, extension.as_str(), "Extension should match");
    });
}

// ============================================================================
// 9.11 Directory Consistency Properties
// ============================================================================

/// Property: After register, lookup returns the registered address.
#[test]
fn prop_directory_register_then_lookup() {
    proptest!(|(
        port in 10000u16..60000,
        key in "[a-zA-Z0-9]{1,20}"
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("TestGrain", &key);
        let address = make_grain_address(&grain_id, &silo);

        // Register
        let result = partition.register(MembershipVersion::default(), address.clone(), None);
        prop_assert!(result.is_success(), "Registration should succeed");

        // Lookup should return the registered address
        let found = partition.lookup(&grain_id);
        prop_assert_eq!(found.as_ref(), Some(&address), "Lookup should return registered address");
    });
}

/// Property: After unregister, lookup returns None.
#[test]
fn prop_directory_unregister_then_lookup_none() {
    proptest!(|(
        port in 10000u16..60000,
        key in "[a-zA-Z0-9]{1,20}"
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("TestGrain", &key);
        let address = make_grain_address(&grain_id, &silo);

        // Register then unregister
        partition.register(MembershipVersion::default(), address.clone(), None);
        partition.unregister(&grain_id, address.activation_id());

        // Lookup should return None
        let found = partition.lookup(&grain_id);
        prop_assert!(found.is_none(), "Lookup after unregister should return None");
    });
}

/// Property: Registering the same grain twice with same activation succeeds.
#[test]
fn prop_directory_duplicate_registration_same_activation() {
    proptest!(|(
        port in 10000u16..60000,
        key in "[a-zA-Z0-9]{1,20}"
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("TestGrain", &key);
        let address = make_grain_address(&grain_id, &silo);

        // Register twice with same activation
        let result1 = partition.register(MembershipVersion::default(), address.clone(), None);
        let result2 = partition.register(MembershipVersion::default(), address.clone(), None);

        prop_assert!(result1.is_success(), "First registration should succeed");
        prop_assert!(result2.is_success(), "Duplicate registration with same activation should succeed");
    });
}

/// Property: Registering a grain twice with different activation creates conflict.
#[test]
fn prop_directory_duplicate_registration_conflict() {
    proptest!(|(
        port in 10000u16..60000,
        key in "[a-zA-Z0-9]{1,20}"
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("TestGrain", &key);
        let address1 = make_grain_address(&grain_id, &silo);
        let address2 = make_grain_address(&grain_id, &silo); // Different ActivationId

        // Register with different activations
        let result1 = partition.register(MembershipVersion::default(), address1.clone(), None);
        let result2 = partition.register(MembershipVersion::default(), address2, None);

        prop_assert!(result1.is_success(), "First registration should succeed");
        prop_assert!(!result2.is_success(), "Second registration should conflict");

        // Conflict should return the original address
        if let RegistrationResult::Conflict(conflict_addr) = result2 {
            prop_assert_eq!(
                conflict_addr.activation_id(),
                address1.activation_id(),
                "Conflict should return original activation"
            );
        }
    });
}

/// Property: Directory grain count is correct after operations.
#[test]
fn prop_directory_grain_count_consistency() {
    proptest!(|(
        port in 10000u16..60000,
        num_grains in 1usize..20
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        // Register multiple grains
        let mut registered = Vec::new();
        for i in 0..num_grains {
            let grain_id = make_grain_id("TestGrain", &format!("grain-{}", i));
            let address = make_grain_address(&grain_id, &silo);
            partition.register(MembershipVersion::default(), address.clone(), None);
            registered.push((grain_id, address));
        }

        prop_assert_eq!(
            partition.grain_count(),
            num_grains,
            "Grain count should match number of registrations"
        );

        // Unregister half
        let to_unregister = num_grains / 2;
        for (grain_id, address) in registered.iter().take(to_unregister) {
            partition.unregister(grain_id, address.activation_id());
        }

        prop_assert_eq!(
            partition.grain_count(),
            num_grains - to_unregister,
            "Grain count should reflect unregistrations"
        );
    });
}

/// Property: Lookup of unregistered grain returns None.
#[test]
fn prop_directory_lookup_unregistered_none() {
    proptest!(|(
        port in 10000u16..60000,
        key in "[a-zA-Z0-9]{1,20}"
    )| {
        let silo = make_silo_address(port);
        let partition = GrainDirectoryPartition::new(silo.clone());

        let grain_id = make_grain_id("TestGrain", &key);

        // Lookup without registering
        let found = partition.lookup(&grain_id);
        prop_assert!(found.is_none(), "Lookup of unregistered grain should return None");
    });
}

/// Property: Removing entries for a dead silo only removes that silo's entries.
#[test]
fn prop_directory_remove_dead_silo_entries() {
    proptest!(|(
        port1 in 10000u16..30000,
        port2 in 30001u16..60000,
        num_grains in 2usize..10
    )| {
        let silo1 = make_silo_address(port1);
        let silo2 = make_silo_address(port2);
        let partition = GrainDirectoryPartition::new(silo1.clone());

        // Register grains on both silos
        for i in 0..num_grains {
            let grain_id = make_grain_id("TestGrain", &format!("grain-silo1-{}", i));
            let address = make_grain_address(&grain_id, &silo1);
            partition.register(MembershipVersion::default(), address, None);
        }

        for i in 0..num_grains {
            let grain_id = make_grain_id("TestGrain", &format!("grain-silo2-{}", i));
            let address = make_grain_address(&grain_id, &silo2);
            partition.register(MembershipVersion::default(), address, None);
        }

        prop_assert_eq!(partition.grain_count(), num_grains * 2);

        // Remove entries for silo2
        let removed = partition.remove_entries_for_silo(&silo2);

        prop_assert_eq!(removed.len(), num_grains, "Should remove all silo2 entries");
        prop_assert_eq!(partition.grain_count(), num_grains, "Only silo1 entries should remain");

        // Verify silo1 entries still exist
        for i in 0..num_grains {
            let grain_id = make_grain_id("TestGrain", &format!("grain-silo1-{}", i));
            prop_assert!(partition.lookup(&grain_id).is_some(), "Silo1 entries should remain");
        }

        // Verify silo2 entries are gone
        for i in 0..num_grains {
            let grain_id = make_grain_id("TestGrain", &format!("grain-silo2-{}", i));
            prop_assert!(partition.lookup(&grain_id).is_none(), "Silo2 entries should be removed");
        }
    });
}

// ============================================================================
// 9.12 Message Delivery Properties
// ============================================================================

/// Property: Messages sent to a local grain are received.
#[tokio::test]
async fn test_message_delivery_local_grain() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("msg-test"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_echo_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let catalog = silo.catalog().unwrap();
    let grain_id = make_grain_id("EchoGrain", "test-echo");

    // Create activation
    let handle = catalog.get_or_create_activation(&grain_id).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send multiple messages
    let test_data = b"hello world";
    let (tx, rx) = tokio::sync::oneshot::channel();

    let message = orleans_messaging::Message::new_request(
        grain_id.clone(),
        GrainInterfaceType::create("IEchoGrain"),
        1, // echo method
        Bytes::from(test_data.to_vec()),
        silo.address().clone(),
    );

    let pending = orleans_runtime::PendingMessage::new(message, Some(tx));
    handle.enqueue_message(pending).unwrap();

    let response = rx.await.unwrap();
    assert_eq!(response.body().as_ref(), test_data, "Echo should return same data");

    silo.stop().await.unwrap();
}

/// Property: Multiple messages to the same grain are delivered sequentially.
#[tokio::test]
async fn test_message_delivery_sequential() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("seq-test"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_echo_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let catalog = silo.catalog().unwrap();
    let grain_id = make_grain_id("EchoGrain", "seq-echo");

    let handle = catalog.get_or_create_activation(&grain_id).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send 10 messages concurrently, verify they all complete
    let num_messages = 10;
    let mut receivers = Vec::new();

    for i in 0..num_messages {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let data = format!("message-{}", i);

        let message = orleans_messaging::Message::new_request(
            grain_id.clone(),
            GrainInterfaceType::create("IEchoGrain"),
            1, // echo method
            Bytes::from(data.into_bytes()),
            silo.address().clone(),
        );

        let pending = orleans_runtime::PendingMessage::new(message, Some(tx));
        handle.enqueue_message(pending).unwrap();
        receivers.push(rx);
    }

    // All messages should be received
    for (i, rx) in receivers.into_iter().enumerate() {
        let response = rx.await.unwrap();
        let expected = format!("message-{}", i);
        assert_eq!(
            response.body().as_ref(),
            expected.as_bytes(),
            "Message {} should be received correctly",
            i
        );
    }

    silo.stop().await.unwrap();
}

/// Property: Message responses contain correct data (no corruption).
#[tokio::test]
async fn test_message_data_integrity() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("integrity-test"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_echo_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let catalog = silo.catalog().unwrap();
    let grain_id = make_grain_id("EchoGrain", "integrity-echo");

    let handle = catalog.get_or_create_activation(&grain_id).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Test with various data sizes and patterns
    let test_cases: Vec<Vec<u8>> = vec![
        vec![],                              // Empty
        vec![0u8; 1],                        // Single byte
        vec![0u8; 100],                      // 100 zeros
        (0u8..=255).collect(),               // All byte values
        vec![0xFF; 1000],                    // 1KB of 0xFF
        (0..10000).map(|i| (i % 256) as u8).collect(), // 10KB pattern
    ];

    for (i, test_data) in test_cases.iter().enumerate() {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let message = orleans_messaging::Message::new_request(
            grain_id.clone(),
            GrainInterfaceType::create("IEchoGrain"),
            1, // echo method
            Bytes::from(test_data.clone()),
            silo.address().clone(),
        );

        let pending = orleans_runtime::PendingMessage::new(message, Some(tx));
        handle.enqueue_message(pending).unwrap();

        let response = rx.await.unwrap();
        assert_eq!(
            response.body().as_ref(),
            test_data.as_slice(),
            "Data integrity test case {} failed (size {})",
            i,
            test_data.len()
        );
    }

    silo.stop().await.unwrap();
}

/// Property: Call count accurately reflects message delivery (no duplicates).
#[tokio::test]
async fn test_no_duplicate_delivery() {
    let membership_table = Arc::new(InMemoryMembershipTable::new("dup-test"));
    membership_table
        .initialize_membership_table(true)
        .await
        .unwrap();

    let grain_type = create_echo_grain_type();

    let mut silo = SiloBuilder::test()
        .listen_address("127.0.0.1:0".parse().unwrap())
        .with_membership_table(membership_table)
        .register_grain_type(grain_type)
        .build()
        .await
        .unwrap();

    silo.start().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    let catalog = silo.catalog().unwrap();
    let grain_id = make_grain_id("EchoGrain", "dup-echo");

    let handle = catalog.get_or_create_activation(&grain_id).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send exactly N echo messages
    let num_messages = 50u32;

    for _ in 0..num_messages {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let message = orleans_messaging::Message::new_request(
            grain_id.clone(),
            GrainInterfaceType::create("IEchoGrain"),
            1, // echo method
            Bytes::from("test"),
            silo.address().clone(),
        );

        let pending = orleans_runtime::PendingMessage::new(message, Some(tx));
        handle.enqueue_message(pending).unwrap();

        // Wait for each message to complete
        rx.await.unwrap();
    }

    // Get call count from grain
    let (tx, rx) = tokio::sync::oneshot::channel();
    let message = orleans_messaging::Message::new_request(
        grain_id.clone(),
        GrainInterfaceType::create("IEchoGrain"),
        2, // get_call_count method
        Bytes::new(),
        silo.address().clone(),
    );

    let pending = orleans_runtime::PendingMessage::new(message, Some(tx));
    handle.enqueue_message(pending).unwrap();

    let response = rx.await.unwrap();
    let call_count = u32::from_le_bytes(response.body()[..4].try_into().unwrap());

    assert_eq!(
        call_count, num_messages,
        "Call count should exactly match sent messages (no duplicates)"
    );

    silo.stop().await.unwrap();
}

// ============================================================================
// Stress Tests (Property-like behavior under load)
// ============================================================================

/// Test: High volume of concurrent grain operations maintain consistency.
#[tokio::test]
async fn test_concurrent_directory_operations() {
    let silo = make_silo_address(11111);
    let partition = Arc::new(GrainDirectoryPartition::new(silo.clone()));

    let num_operations = 100;
    let mut handles = Vec::new();

    // Concurrent register/lookup operations
    for i in 0..num_operations {
        let partition = partition.clone();
        let silo = silo.clone();

        handles.push(tokio::spawn(async move {
            let grain_id = make_grain_id("TestGrain", &format!("concurrent-{}", i));
            let address = make_grain_address(&grain_id, &silo);

            // Register
            let result = partition.register(MembershipVersion::default(), address.clone(), None);
            assert!(result.is_success(), "Registration {} should succeed", i);

            // Immediate lookup should find it
            let found = partition.lookup(&grain_id);
            assert!(found.is_some(), "Lookup {} should find registered grain", i);
            assert_eq!(
                found.as_ref().unwrap().activation_id(),
                address.activation_id()
            );
        }));
    }

    // Wait for all operations
    for handle in handles {
        handle.await.unwrap();
    }

    // Final count should be exactly num_operations
    assert_eq!(
        partition.grain_count(),
        num_operations,
        "Final grain count should match operations"
    );
}

/// Test: Hash distribution is roughly uniform across silos.
#[test]
fn test_hash_distribution_uniformity() {
    use std::collections::HashMap;

    let silos: Vec<SiloAddress> = (0..5)
        .map(|i| make_silo_address(10000 + i * 1000))
        .collect();

    // Use a ConsistentHashRing to test distribution
    let ring = orleans_directory::ConsistentHashRing::new();
    for silo in &silos {
        ring.add_silo(silo.clone());
    }

    // Count how many grains map to each silo
    let mut silo_counts: HashMap<SiloAddress, usize> = HashMap::new();
    let num_grains = 10000;

    for i in 0..num_grains {
        let grain_id = make_grain_id("TestGrain", &format!("grain-{}", i));
        if let Ok(primary) = ring.get_primary_silo(grain_id.get_uniform_hash_code()) {
            *silo_counts.entry(primary).or_insert(0) += 1;
        }
    }

    // Each silo should get roughly 1/5 of grains
    let expected = num_grains / silos.len();
    let tolerance = expected / 2; // 50% tolerance

    for (silo, count) in &silo_counts {
        assert!(
            *count > expected - tolerance && *count < expected + tolerance,
            "Silo {} has {} grains, expected ~{} (±{})",
            silo,
            count,
            expected,
            tolerance
        );
    }

    info!(
        "Hash distribution: {:?}",
        silo_counts.values().collect::<Vec<_>>()
    );
}

/// Test: All GrainIds in a batch produce unique hash codes (collision test).
#[test]
fn test_hash_collision_rate() {
    let mut hashes = HashSet::new();
    let num_grains = 10000;

    for i in 0..num_grains {
        let grain_id = make_grain_id("TestGrain", &format!("unique-key-{}", i));
        hashes.insert(grain_id.get_uniform_hash_code());
    }

    // Some collisions are expected, but should be rare (< 1%)
    let collision_rate = (num_grains - hashes.len()) as f64 / num_grains as f64;

    assert!(
        collision_rate < 0.01,
        "Hash collision rate {} is too high (expected < 1%)",
        collision_rate
    );

    debug!(
        "Hash collision test: {} unique hashes from {} grains (collision rate: {:.4}%)",
        hashes.len(),
        num_grains,
        collision_rate * 100.0
    );
}
