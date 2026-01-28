//! Orleans Migration - Graceful grain migration support.
//!
//! This crate provides the infrastructure for migrating grains between silos
//! without losing state. Migration is essential for:
//!
//! - **Graceful silo shutdown**: Moving grains to other silos before a silo stops
//! - **Cluster rebalancing**: Redistributing grains when the cluster topology changes
//! - **Resource optimization**: Moving grains away from overloaded silos
//! - **Rolling upgrades**: Migrating grains to silos with newer code versions
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                         Source Silo                                   │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │                    Grain Activation                          │   │
//! │  │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  │   │
//! │  │  │ GrainState    │  │ Timers        │  │ Custom          │  │   │
//! │  │  │ Participant   │  │ Participant   │  │ Participants    │  │   │
//! │  │  └───────┬───────┘  └───────┬───────┘  └───────┬─────────┘  │   │
//! │  │          │ on_dehydrate()   │                  │            │   │
//! │  │          ▼                  ▼                  ▼            │   │
//! │  │  ┌─────────────────────────────────────────────────────────┐│   │
//! │  │  │               MigrationContext                          ││   │
//! │  │  │  key1 -> bytes, key2 -> bytes, key3 -> bytes           ││   │
//! │  │  └─────────────────────────────────────────────────────────┘│   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! │                              │                                       │
//! │                              │ MigrationContext (serialized)         │
//! │                              ▼                                       │
//! └──────────────────────────────┼───────────────────────────────────────┘
//!                                │ Network Transfer
//!                                ▼
//! ┌──────────────────────────────┼───────────────────────────────────────┐
//! │                         Target Silo                                   │
//! │                              │                                       │
//! │  ┌─────────────────────────────────────────────────────────────┐   │
//! │  │               MigrationContext                               │   │
//! │  │  ┌─────────────────────────────────────────────────────────┐│   │
//! │  │  │  key1 -> bytes, key2 -> bytes, key3 -> bytes           ││   │
//! │  │  └─────────────────────────────────────────────────────────┘│   │
//! │  │          │ on_rehydrate()   │                  │            │   │
//! │  │          ▼                  ▼                  ▼            │   │
//! │  │  ┌───────────────┐  ┌───────────────┐  ┌─────────────────┐  │   │
//! │  │  │ GrainState    │  │ Timers        │  │ Custom          │  │   │
//! │  │  │ (restored)    │  │ (restored)    │  │ Participants    │  │   │
//! │  │  └───────────────┘  └───────────────┘  └─────────────────┘  │   │
//! │  └─────────────────────────────────────────────────────────────┘   │
//! │                    New Grain Activation                             │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Core Concepts
//!
//! ## Migration Participant
//!
//! Components that have state to migrate implement [`IGrainMigrationParticipant`]:
//!
//! ```ignore
//! use orleans_migration::{IGrainMigrationParticipant, MigrationContext};
//!
//! #[derive(Debug)]
//! struct MyState {
//!     counter: i32,
//! }
//!
//! impl IGrainMigrationParticipant for MyState {
//!     fn on_dehydrate(&self, context: &mut MigrationContext) {
//!         context.try_add_value("my_counter", &self.counter);
//!     }
//!
//!     fn on_rehydrate(&mut self, context: &MigrationContext) {
//!         if let Some(value) = context.try_get_value::<i32>("my_counter") {
//!             self.counter = value;
//!         }
//!     }
//! }
//! ```
//!
//! ## Migration Context
//!
//! The [`MigrationContext`] carries serialized state between silos:
//!
//! ```
//! use orleans_migration::MigrationContext;
//!
//! let mut ctx = MigrationContext::new();
//!
//! // Dehydration (source silo)
//! ctx.try_add_value("key1", &42i32);
//! ctx.try_add_value("key2", &"hello".to_string());
//!
//! // Transfer to target silo...
//! let bytes = ctx.to_bytes().unwrap();
//! let ctx2 = MigrationContext::from_bytes(&bytes).unwrap();
//!
//! // Rehydration (target silo)
//! let value: i32 = ctx2.try_get_value("key1").unwrap();
//! assert_eq!(value, 42);
//! ```
//!
//! ## Activation Migration Manager
//!
//! The [`ActivationMigrationManager`] orchestrates the migration process:
//!
//! ```ignore
//! use orleans_migration::{ActivationMigrationManager, MigrationOptions, MigrationReason};
//!
//! let manager = ActivationMigrationManager::new(local_silo, MigrationOptions::default());
//!
//! // Migrate a grain to another silo
//! manager.migrate_activation(&grain_id, &target_silo, MigrationReason::Manual).await?;
//! ```
//!
//! # Migration Flow
//!
//! 1. **Preparation**: Drain pending requests, prevent new requests
//! 2. **Dehydration**: All participants serialize their state
//! 3. **Transfer**: Serialized context sent to target silo
//! 4. **Rehydration**: New activation created, participants restore state
//! 5. **Directory Update**: Grain directory updated with new location
//! 6. **Completion**: Old activation deactivated, new activation active
//!
//! # Configuration
//!
//! Migration behavior is configured via [`MigrationOptions`]:
//!
//! ```
//! use orleans_migration::MigrationOptions;
//! use std::time::Duration;
//!
//! let options = MigrationOptions::new()
//!     .with_migration_timeout(Duration::from_secs(60))
//!     .with_max_concurrent_migrations(10)
//!     .with_max_retry_attempts(3);
//! ```
//!
//! # Grain Configuration
//!
//! Grains can be configured for migration via [`GrainMigrationConfig`]:
//!
//! ```
//! use orleans_migration::GrainMigrationConfig;
//!
//! // Make a grain migratable with custom settings
//! let config = GrainMigrationConfig::migratable()
//!     .with_persist_before_migration(true)
//!     .with_priority(10);
//!
//! // Make a grain immovable
//! let config = GrainMigrationConfig::immovable();
//! ```

pub mod context;
pub mod error;
pub mod manager;
pub mod options;
pub mod participant;

// Re-exports for convenience
pub use context::{MigrationContext, SharedMigrationContext};
pub use error::{MigrationError, MigrationReason, MigrationResult};
pub use manager::{
    ActivationMigrationManager, IActivationMigrationManager, MigrationPhase, MigrationStatistics,
    MigrationStatus,
};
pub use options::{GrainMigrationConfig, MigrationOptions};
pub use participant::{IGrainMigrationParticipant, IMigratable, MigrationParticipantRegistry};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
    use serde::{Deserialize, Serialize};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_grain_id(key: &str) -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(key))
    }

    fn make_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestGrainState {
        counter: i32,
        name: String,
    }

    impl IGrainMigrationParticipant for TestGrainState {
        fn on_dehydrate(&self, context: &mut MigrationContext) {
            context.try_add_value("test_state", self);
        }

        fn on_rehydrate(&mut self, context: &MigrationContext) {
            if let Some(state) = context.try_get_value::<TestGrainState>("test_state") {
                *self = state;
            }
        }
    }

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<MigrationContext>();
        let _ = std::any::type_name::<MigrationOptions>();
        let _ = std::any::type_name::<MigrationError>();
        let _ = std::any::type_name::<ActivationMigrationManager>();
        let _ = std::any::type_name::<MigrationParticipantRegistry>();
    }

    #[test]
    fn test_full_dehydration_rehydration_cycle() {
        // Create initial state
        let state = TestGrainState {
            counter: 42,
            name: "test grain".to_string(),
        };

        // Dehydrate
        let mut ctx = MigrationContext::new();
        state.on_dehydrate(&mut ctx);

        // Simulate transfer (serialize and deserialize)
        let bytes = ctx.to_bytes().unwrap();
        let ctx2 = MigrationContext::from_bytes(&bytes).unwrap();

        // Rehydrate
        let mut restored = TestGrainState {
            counter: 0,
            name: String::new(),
        };
        restored.on_rehydrate(&ctx2);

        // Verify
        assert_eq!(restored.counter, 42);
        assert_eq!(restored.name, "test grain");
    }

    #[test]
    fn test_migration_options_presets() {
        let default_opts = MigrationOptions::default();
        let testing_opts = MigrationOptions::for_testing();
        let shutdown_opts = MigrationOptions::for_shutdown();

        // Testing should have shorter timeouts
        assert!(testing_opts.migration_timeout < default_opts.migration_timeout);

        // Shutdown should allow pending requests
        assert!(shutdown_opts.allow_migration_with_pending_requests);
    }

    #[test]
    fn test_grain_migration_config() {
        let migratable = GrainMigrationConfig::migratable();
        let immovable = GrainMigrationConfig::immovable();

        assert!(migratable.is_migratable);
        assert!(!immovable.is_migratable);
    }

    #[test]
    fn test_migration_error_types() {
        let grain_id = make_grain_id("test");
        let silo = make_silo_address(11111);

        // Test error creation and properties
        let error = MigrationError::GrainImmovable(grain_id.clone());
        assert!(error.is_permanent());
        assert!(!error.is_retryable());

        let error = MigrationError::TargetSiloUnavailable(silo);
        assert!(error.is_retryable());
        assert!(!error.is_permanent());

        let error = MigrationError::DehydrationFailed {
            grain_id,
            reason: "test".to_string(),
        };
        assert!(error.is_serialization_error());
    }

    #[test]
    fn test_migration_reason_display() {
        assert_eq!(MigrationReason::SiloShutdown.to_string(), "silo_shutdown");
        assert_eq!(MigrationReason::Rebalancing.to_string(), "rebalancing");
        assert_eq!(MigrationReason::Manual.to_string(), "manual");
    }

    #[test]
    fn test_registry_dehydration_order() {
        let mut registry = MigrationParticipantRegistry::new();

        // Register participants with different priorities
        registry.register(
            "second",
            20,
            TestGrainState {
                counter: 2,
                name: "second".to_string(),
            },
        );
        registry.register(
            "first",
            10,
            TestGrainState {
                counter: 1,
                name: "first".to_string(),
            },
        );
        registry.register(
            "third",
            30,
            TestGrainState {
                counter: 3,
                name: "third".to_string(),
            },
        );

        // Verify order
        let names = registry.participant_names();
        assert_eq!(names, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn test_migration_manager_basic_flow() {
        let local_silo = make_silo_address(11111);
        let target_silo = make_silo_address(22222);
        let options = MigrationOptions::for_testing();

        let manager = ActivationMigrationManager::new(local_silo, options);

        let grain_id = make_grain_id("test-grain");

        // Should be able to migrate
        assert!(manager.can_migrate(&grain_id));
        assert!(!manager.is_migrating(&grain_id));

        // Perform migration
        let result = manager
            .migrate_activation(&grain_id, &target_silo, MigrationReason::Manual)
            .await;

        assert!(result.is_ok());

        // Check statistics
        let stats = manager.get_statistics();
        assert_eq!(stats.successful_migrations, 1);
        assert_eq!(stats.total_migrations, 1);
    }

    #[tokio::test]
    async fn test_migration_manager_immovable_grain() {
        let local_silo = make_silo_address(11111);
        let target_silo = make_silo_address(22222);

        let manager = ActivationMigrationManager::new(local_silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("immovable-grain");
        manager.mark_immovable(grain_id.clone());

        // Should not be able to migrate
        assert!(!manager.can_migrate(&grain_id));

        let result = manager
            .migrate_activation(&grain_id, &target_silo, MigrationReason::Manual)
            .await;

        assert!(matches!(result, Err(MigrationError::GrainImmovable(_))));
    }

    #[test]
    fn test_shared_migration_context() {
        let shared = SharedMigrationContext::new();

        // Write from one reference
        {
            let mut ctx = shared.write();
            ctx.try_add_value("key", &42i32);
        }

        // Read from another reference
        {
            let ctx = shared.read();
            let value: i32 = ctx.try_get_value("key").unwrap();
            assert_eq!(value, 42);
        }

        // Extract inner
        let inner = shared.into_inner();
        assert!(inner.has_key("key"));
    }
}
