//! Orleans Runtime - Grain hosting and activation management.
//!
//! This crate provides the core runtime for hosting grains in an Orleans cluster.
//! It includes:
//!
//! - **Activation management**: Creating, managing, and deactivating grain instances
//! - **Message dispatching**: Routing messages to the correct grain activations
//! - **Grain references**: Proxies for invoking methods on grains
//! - **Catalog**: Registry of active grains on this silo
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                         Silo                                     │
//! │  ┌─────────────┐    ┌─────────────┐    ┌─────────────────────┐ │
//! │  │  Message    │───▶│  Dispatcher │───▶│      Catalog        │ │
//! │  │  Center     │    │             │    │  ┌───────────────┐  │ │
//! │  └─────────────┘    └─────────────┘    │  │ Activation 1  │  │ │
//! │         │                  │           │  │ ┌───────────┐ │  │ │
//! │         │                  │           │  │ │  Grain    │ │  │ │
//! │         ▼                  ▼           │  │ │ Instance  │ │  │ │
//! │  ┌─────────────┐    ┌─────────────┐    │  │ └───────────┘ │  │ │
//! │  │   Grain     │◀───│   Grain     │    │  └───────────────┘  │ │
//! │  │  Directory  │    │  Factory    │    │  ┌───────────────┐  │ │
//! │  └─────────────┘    └─────────────┘    │  │ Activation 2  │  │ │
//! │                                        │  └───────────────┘  │ │
//! │                                        └─────────────────────┘ │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use orleans_runtime::{Catalog, Dispatcher, GrainFactory};
//!
//! // Create the catalog
//! let catalog = Catalog::new(silo_address, grain_factory, options);
//!
//! // Register grain types
//! catalog.register_grain_type(hello_grain_data);
//!
//! // Create the dispatcher
//! let dispatcher = Dispatcher::new(
//!     silo_address,
//!     catalog,
//!     directory,
//!     message_center,
//!     dispatcher_options,
//! );
//!
//! // Start processing messages
//! dispatcher.register_handler();
//! ```

pub mod activation_data;
pub mod activation_state;
pub mod catalog;
pub mod dispatcher;
pub mod error;
pub mod grain;
pub mod grain_context;
pub mod grain_factory;
pub mod grain_reference;

// Re-exports for convenience
pub use activation_data::{ActivationData, ActivationHandle, ActivationStats, PendingMessage};
pub use activation_state::{ActivationState, DeactivationReason};
pub use catalog::{Catalog, CatalogOptions, CatalogStats};
pub use dispatcher::{Dispatcher, DispatcherOptions};
pub use error::{RuntimeError, RuntimeResult};
pub use grain::{GrainTypeData, IGrain, IGrainActivator, IGrainMethodInvoker, PlacementHint};
pub use grain_context::{GrainContext, IGrainContext};
pub use grain_factory::{
    ConventionInterfaceResolver, GrainFactory, GrainFactoryExt, GrainInterfaceMarker,
    IGrainFactory, InterfaceResolver, MapInterfaceResolver,
};
pub use grain_reference::{GrainReference, IGrainReference, MessageSender, TypedGrainReference};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_compiles() {
        // This test just verifies that the crate compiles correctly
        // and all public types are accessible
        let _ = std::any::type_name::<Catalog>();
        let _ = std::any::type_name::<Dispatcher>();
        let _ = std::any::type_name::<GrainFactory>();
        let _ = std::any::type_name::<ActivationState>();
    }
}
