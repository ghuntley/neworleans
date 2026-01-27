//! Orleans Messaging Infrastructure
//!
//! This crate provides the messaging layer for Orleans, enabling communication
//! between silos (server nodes) in an Orleans cluster.
//!
//! # Overview
//!
//! The messaging infrastructure consists of several key components:
//!
//! - **Message**: The core message structure carrying requests, responses, and one-way notifications
//! - **CorrelationId**: Unique identifier for matching requests with responses
//! - **MessageCenter**: Central dispatcher that routes messages between silos
//! - **ConnectionManager**: Manages TCP connections to other silos
//!
//! # Example
//!
//! ```ignore
//! use orleans_messaging::{MessageCenter, Message, GrainInterfaceType};
//! use orleans_core::{GrainId, GrainType, SiloAddress};
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create a message center bound to a local address
//!     let local_address = SiloAddress::new(
//!         "127.0.0.1:11111".parse()?,
//!         1234567890,
//!     );
//!     let center = MessageCenter::new(local_address).await?;
//!
//!     // Set up a handler for incoming messages
//!     center.set_message_handler(|msg| {
//!         println!("Received message: {:?}", msg);
//!     });
//!
//!     // Send a request to another silo
//!     let target_grain = GrainId::new(
//!         GrainType::create("HelloGrain"),
//!         "user123".into(),
//!     );
//!     let target_silo = SiloAddress::new("127.0.0.1:22222".parse()?, 1234567890);
//!
//!     let response = center.request(
//!         target_grain,
//!         target_silo,
//!         GrainInterfaceType::create("IHelloGrain"),
//!         1,  // method_id
//!         Bytes::from_static(b"Hello!"),
//!     ).await?;
//!
//!     println!("Response: {:?}", response);
//!
//!     Ok(())
//! }
//! ```
//!
//! # Wire Protocol
//!
//! Messages are framed as:
//! ```text
//! [header_len: i32][body_len: i32][header bytes][body bytes]
//! ```
//!
//! The header contains routing information (grain IDs, silo addresses, correlation ID),
//! while the body contains the serialized method arguments or return value.

pub mod connection;
pub mod connection_manager;
pub mod correlation_id;
pub mod direction;
pub mod error;
pub mod grain_interface_type;
pub mod message;
pub mod message_center;
pub mod message_codec;

// Re-export main types
pub use connection::{Connection, ConnectionStats};
pub use connection_manager::{ConnectionConfig, ConnectionManager};
pub use correlation_id::CorrelationId;
pub use direction::Direction;
pub use error::MessagingError;
pub use grain_interface_type::GrainInterfaceType;
pub use message::{Message, RejectionInfo, RejectionType};
pub use message_center::{MessageCenter, MessageCenterConfig};
pub use message_codec::{decode_message, encode_message, frame_size, FRAME_HEADER_SIZE, MAX_MESSAGE_SIZE};

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orleans_core::{GrainId, GrainType, SiloAddress};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn test_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    fn test_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), key.into())
    }

    /// Integration test: Two message centers exchanging messages.
    #[tokio::test]
    async fn test_two_silos_communication() {
        // Start two silos
        let silo1 = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let silo2 = MessageCenter::new(test_silo_address(0)).await.unwrap();

        let addr1 = silo1.local_address().clone();
        let addr2 = silo2.local_address().clone();

        // Track messages received by silo2
        let received_count = Arc::new(AtomicUsize::new(0));
        let received_count_clone = Arc::clone(&received_count);

        // Set up silo2 to respond to requests
        let silo2_clone = Arc::clone(&silo2);
        silo2.set_message_handler(move |msg| {
            received_count_clone.fetch_add(1, Ordering::Relaxed);
            if msg.is_request() {
                let response = msg.create_response(Bytes::from(format!("Echo: {:?}", msg.body)));
                let silo = Arc::clone(&silo2_clone);
                let addr = addr1.clone();
                tokio::spawn(async move {
                    let _ = silo.send_response(response.with_target_silo(addr)).await;
                });
            }
        });

        // Send a request from silo1 to silo2
        let response = silo1
            .request(
                test_grain_id("test-key"),
                addr2.clone(),
                GrainInterfaceType::create("ITestGrain"),
                42,
                Bytes::from_static(b"Hello, Silo2!"),
            )
            .await
            .unwrap();

        assert!(response.is_response());
        assert!(!response.is_rejection());

        // Verify silo2 received the message
        assert!(received_count.load(Ordering::Relaxed) >= 1);

        silo1.shutdown().await;
        silo2.shutdown().await;
    }

    /// Integration test: Multiple concurrent requests.
    #[tokio::test]
    async fn test_concurrent_requests() {
        let silo1 = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let silo2 = MessageCenter::new(test_silo_address(0)).await.unwrap();

        let addr1 = silo1.local_address().clone();
        let addr2 = silo2.local_address().clone();

        // Set up echo handler on silo2
        let silo2_clone = Arc::clone(&silo2);
        silo2.set_message_handler(move |msg| {
            if msg.is_request() {
                // Echo back the method_id in the response
                let response_body = Bytes::from(format!("method_{}", msg.method_id));
                let response = msg.create_response(response_body);
                let silo = Arc::clone(&silo2_clone);
                let addr = addr1.clone();
                tokio::spawn(async move {
                    let _ = silo.send_response(response.with_target_silo(addr)).await;
                });
            }
        });

        // Send multiple concurrent requests
        let mut handles = Vec::new();
        for i in 0..10u32 {
            let silo1_clone = Arc::clone(&silo1);
            let addr2_clone = addr2.clone();
            handles.push(tokio::spawn(async move {
                silo1_clone
                    .request(
                        test_grain_id(&format!("key-{}", i)),
                        addr2_clone,
                        GrainInterfaceType::create("ITestGrain"),
                        i,
                        Bytes::new(),
                    )
                    .await
            }));
        }

        // Collect results
        for handle in handles {
            let response = handle.await.unwrap().unwrap();
            assert!(response.is_response());
        }

        silo1.shutdown().await;
        silo2.shutdown().await;
    }

    /// Integration test: Connection persistence across multiple requests.
    #[tokio::test]
    async fn test_connection_reuse() {
        let silo1 = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let silo2 = MessageCenter::new(test_silo_address(0)).await.unwrap();

        let addr1 = silo1.local_address().clone();
        let addr2 = silo2.local_address().clone();

        let silo2_clone = Arc::clone(&silo2);
        silo2.set_message_handler(move |msg| {
            if msg.is_request() {
                let response = msg.create_response(Bytes::from_static(b"ok"));
                let silo = Arc::clone(&silo2_clone);
                let addr = addr1.clone();
                tokio::spawn(async move {
                    let _ = silo.send_response(response.with_target_silo(addr)).await;
                });
            }
        });

        // Send multiple requests - should reuse the same connection
        for i in 0..5 {
            let response = silo1
                .request(
                    test_grain_id(&format!("key-{}", i)),
                    addr2.clone(),
                    GrainInterfaceType::create("ITestGrain"),
                    i as u32,
                    Bytes::new(),
                )
                .await
                .unwrap();
            assert!(response.is_response());
        }

        // Should only have one connection
        assert_eq!(silo1.connection_manager().connection_count(), 1);

        silo1.shutdown().await;
        silo2.shutdown().await;
    }
}
