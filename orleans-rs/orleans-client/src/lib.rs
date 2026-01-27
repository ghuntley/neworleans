//! Orleans ClusterClient - External client for Orleans clusters
//!
//! This crate provides the `ClusterClient` for connecting external applications
//! to an Orleans cluster without being a full silo. It enables invoking grain
//! methods from outside the cluster through gateway silos.
//!
//! # Overview
//!
//! The ClusterClient is the primary entry point for external applications that
//! need to communicate with grains in an Orleans cluster. It handles:
//!
//! - Connection management to gateway silos
//! - Request/response correlation
//! - Load balancing across multiple gateways
//! - Automatic reconnection on failures
//! - Gateway health monitoring
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                          ClusterClient                                   │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
//! │  │  GatewayManager │  │ CallbackManager │  │    GrainFactory         │  │
//! │  │  (load balance) │  │ (req/res match) │  │ (grain references)      │  │
//! │  └────────┬────────┘  └────────┬────────┘  └───────────┬─────────────┘  │
//! │           │                    │                       │                 │
//! │           ▼                    ▼                       ▼                 │
//! │  ┌─────────────────────────────────────────────────────────────────────┐│
//! │  │                     ConnectionManager                                ││
//! │  │              (TCP connections to gateways)                           ││
//! │  └─────────────────────────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────────────────────────┘
//!                                    │
//!                                    ▼
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                          Orleans Cluster                                 │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                   │
//! │  │   Gateway    │  │   Gateway    │  │   Gateway    │                   │
//! │  │   (Silo 1)   │  │   (Silo 2)   │  │   (Silo 3)   │                   │
//! │  └──────────────┘  └──────────────┘  └──────────────┘                   │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```rust,no_run
//! use orleans_client::{ClientBuilder, ClusterClient};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Build the client
//! let client = ClientBuilder::new()
//!     .with_cluster_id("my-cluster")
//!     .with_service_id("my-app")
//!     .with_gateway("10.0.0.1:30000".parse()?)
//!     .with_gateway("10.0.0.2:30000".parse()?)
//!     .with_response_timeout(Duration::from_secs(30))
//!     .build()?;
//!
//! // Connect to the cluster
//! client.connect().await?;
//!
//! // Get a grain reference
//! // let grain = client.get_grain::<IMyGrain>("my-key").await?;
//!
//! // Invoke grain methods
//! // let result = grain.my_method("argument").await?;
//!
//! // Disconnect when done
//! client.disconnect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Gateway Discovery
//!
//! There are two ways to discover gateways:
//!
//! 1. **Static configuration**: Provide gateway addresses explicitly via
//!    `with_gateway()` or `with_gateways()`.
//!
//! 2. **Membership table**: Provide a membership table via
//!    `with_membership_table()` for automatic gateway discovery.
//!
//! # Connection Management
//!
//! The client automatically manages connections to gateways:
//!
//! - Maintains a pool of connections
//! - Load balances requests across healthy gateways
//! - Tracks gateway health and marks unhealthy gateways
//! - Periodically refreshes the gateway list
//! - Supports automatic reconnection
//!
//! # Request Timeout
//!
//! Grain method calls will timeout based on the configured `response_timeout`.
//! The timeout can be overridden per-call when using the lower-level APIs.

pub mod builder;
pub mod callback;
pub mod client;
pub mod error;
pub mod gateway;
pub mod options;

// Re-export main types
pub use builder::ClientBuilder;
pub use client::ClusterClient;
pub use error::{ClientError, ClientResult, ClientStatus};
pub use gateway::{GatewayInfo, GatewayManager, GatewayStatus};
pub use options::{ClientOptions, GatewayOptions};

// Re-export commonly used types from dependencies
pub use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
pub use orleans_messaging::GrainInterfaceType;
pub use orleans_runtime::{
    GrainFactory, GrainFactoryExt, GrainInterfaceMarker, GrainReference, IGrainFactory,
    IGrainReference,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<ClusterClient>();
        let _ = std::any::type_name::<ClientBuilder>();
        let _ = std::any::type_name::<ClientOptions>();
        let _ = std::any::type_name::<ClientError>();
        let _ = std::any::type_name::<ClientStatus>();
        let _ = std::any::type_name::<GatewayManager>();
        let _ = std::any::type_name::<GatewayInfo>();
        let _ = std::any::type_name::<GatewayStatus>();
        let _ = std::any::type_name::<GatewayOptions>();
    }

    #[test]
    fn test_reexports() {
        // Verify re-exports are accessible
        let _ = std::any::type_name::<GrainId>();
        let _ = std::any::type_name::<GrainType>();
        let _ = std::any::type_name::<IdSpan>();
        let _ = std::any::type_name::<SiloAddress>();
        let _ = std::any::type_name::<GrainInterfaceType>();
        let _ = std::any::type_name::<GrainFactory>();
    }
}
