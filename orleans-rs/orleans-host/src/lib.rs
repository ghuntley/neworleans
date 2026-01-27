//! Orleans Silo Host
//!
//! This crate provides the silo host assembly for Orleans, combining all components
//! (messaging, clustering, directory, runtime) into a runnable silo process.
//!
//! # Overview
//!
//! A silo is a single node in an Orleans cluster. It hosts grain activations,
//! handles message routing, and participates in cluster membership. Multiple silos
//! form a cluster, providing location transparency for grains.
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use orleans_host::{SiloBuilder, SiloConfig};
//! use orleans_runtime::GrainTypeData;
//! use orleans_core::GrainType;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Define your grain type data (activator, invokers)
//! // let grain_type_data = ...;
//!
//! // Build and start a silo
//! // let mut silo = SiloBuilder::new()
//! //     .listen_address("127.0.0.1:11111".parse()?)
//! //     .register_grain_type(grain_type_data)
//! //     .build()
//! //     .await?;
//! //
//! // silo.start().await?;
//! //
//! // // Silo is now running and accepting requests
//! // // ...
//! //
//! // // Graceful shutdown
//! // silo.stop().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! The silo integrates the following components:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────┐
//! │                              Silo                                           │
//! │ ┌─────────────────┐  ┌───────────────────┐  ┌──────────────────────────┐   │
//! │ │  Membership     │  │  Message Center   │  │   Grain Directory        │   │
//! │ │  Agent          │  │  (TCP listener)   │  │   (Consistent Hash Ring) │   │
//! │ └────────┬────────┘  └────────┬──────────┘  └──────────────┬───────────┘   │
//! │          │                    │                            │               │
//! │          ▼                    ▼                            ▼               │
//! │ ┌─────────────────────────────────────────────────────────────────────────┐│
//! │ │                          Dispatcher                                      ││
//! │ │ (Routes messages to activations, creates activations as needed)          ││
//! │ └─────────────────────────────────────────────────────────────────────────┘│
//! │                                   │                                         │
//! │                                   ▼                                         │
//! │ ┌─────────────────────────────────────────────────────────────────────────┐│
//! │ │                          Catalog                                         ││
//! │ │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     ││
//! │ │  │ Activation  │  │ Activation  │  │ Activation  │  │     ...     │     ││
//! │ │  │  (Grain1)   │  │  (Grain2)   │  │  (Grain3)   │  │             │     ││
//! │ │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘     ││
//! │ └─────────────────────────────────────────────────────────────────────────┘│
//! └─────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Multi-Silo Cluster
//!
//! To form a cluster, multiple silos share the same membership table:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use orleans_host::{SiloBuilder, SiloConfig, IMembershipTable};
//! use orleans_clustering::InMemoryMembershipTable;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a shared membership table
//! let membership_table = Arc::new(InMemoryMembershipTable::new("my-cluster"));
//! membership_table.initialize_membership_table(true).await?;
//!
//! // Start multiple silos sharing the same table
//! // let mut silo1 = SiloBuilder::new()
//! //     .listen_address("127.0.0.1:11111".parse()?)
//! //     .with_membership_table(membership_table.clone())
//! //     .register_grain_type(grain_type.clone())
//! //     .build()
//! //     .await?;
//! //
//! // let mut silo2 = SiloBuilder::new()
//! //     .listen_address("127.0.0.1:22222".parse()?)
//! //     .with_membership_table(membership_table.clone())
//! //     .register_grain_type(grain_type.clone())
//! //     .build()
//! //     .await?;
//! //
//! // silo1.start().await?;
//! // silo2.start().await?;
//! # Ok(())
//! # }
//! ```

pub mod config;
pub mod error;
pub mod silo;
pub mod silo_builder;

// Re-export main types
pub use config::SiloConfig;
pub use error::{SiloError, SiloResult};
pub use silo::{Silo, SiloState};
pub use silo_builder::SiloBuilder;

// Re-export commonly used types from dependencies
pub use orleans_clustering::{InMemoryMembershipTable, IMembershipTable};
pub use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
pub use orleans_messaging::{GrainInterfaceType, Message};
pub use orleans_runtime::{
    GrainFactory, GrainReference, GrainTypeData, IGrain, IGrainActivator,
    IGrainMethodInvoker, IGrainContext, PendingMessage, RuntimeResult,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<Silo>();
        let _ = std::any::type_name::<SiloBuilder>();
        let _ = std::any::type_name::<SiloConfig>();
        let _ = std::any::type_name::<SiloState>();
    }
}
