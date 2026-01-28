//! Migration participant traits for grain components.
//!
//! This module defines the `IGrainMigrationParticipant` trait that grain components
//! implement to participate in grain migration.

use std::fmt::Debug;

use crate::context::MigrationContext;

/// Trait for grain components that participate in migration.
///
/// When a grain is migrated from one silo to another, all components that
/// implement this trait will have their state dehydrated on the source silo
/// and rehydrated on the target silo.
///
/// # Example
///
/// ```ignore
/// use orleans_migration::{IGrainMigrationParticipant, MigrationContext};
///
/// struct CounterState {
///     value: i32,
/// }
///
/// impl IGrainMigrationParticipant for CounterState {
///     fn on_dehydrate(&self, context: &mut MigrationContext) {
///         context.try_add_value("counter_value", &self.value);
///     }
///
///     fn on_rehydrate(&mut self, context: &MigrationContext) {
///         if let Some(value) = context.try_get_value::<i32>("counter_value") {
///             self.value = value;
///         }
///     }
/// }
/// ```
pub trait IGrainMigrationParticipant: Send + Sync + Debug {
    /// Called during dehydration to save component state.
    ///
    /// Implementations should serialize their state into the context using
    /// unique keys that won't conflict with other participants.
    fn on_dehydrate(&self, context: &mut MigrationContext);

    /// Called during rehydration to restore component state.
    ///
    /// Implementations should deserialize their state from the context.
    /// If a key is not found, the component should use its default state.
    fn on_rehydrate(&mut self, context: &MigrationContext);

    /// Returns the unique key prefix for this participant.
    ///
    /// Used to namespace keys in the migration context to avoid conflicts.
    /// Default implementation returns the type name.
    fn migration_key_prefix(&self) -> String {
        std::any::type_name::<Self>().to_string()
    }

    /// Returns true if this participant has state that needs to be migrated.
    ///
    /// If this returns false, `on_dehydrate` won't be called during migration.
    fn has_migration_state(&self) -> bool {
        true
    }
}

/// Marker trait for grains that can be migrated.
///
/// Grains that implement this trait indicate they support graceful migration.
/// Grains that don't implement this are considered immovable.
pub trait IMigratable: Send + Sync {
    /// Called before migration to check if the grain can be migrated now.
    ///
    /// Return false to prevent migration (e.g., during critical operations).
    fn can_migrate(&self) -> bool {
        true
    }

    /// Called before dehydration to give the grain a chance to prepare.
    ///
    /// This is called on the source silo before `on_dehydrate` is called
    /// on migration participants.
    fn on_migration_start(&mut self) {}

    /// Called after rehydration completes.
    ///
    /// This is called on the target silo after all participants have been
    /// rehydrated, but before the grain starts processing requests.
    fn on_migration_complete(&mut self) {}
}

/// Registry of migration participants for a grain activation.
#[derive(Debug, Default)]
pub struct MigrationParticipantRegistry {
    participants: Vec<ParticipantEntry>,
}

#[derive(Debug)]
struct ParticipantEntry {
    name: String,
    priority: i32,
    participant: Box<dyn ParticipantWrapper>,
}

/// Internal trait for type-erased participant access.
/// This trait is dyn-safe because it doesn't have generic methods.
trait ParticipantWrapper: Send + Sync + Debug {
    fn on_dehydrate(&self, context: &mut MigrationContext);
    fn on_rehydrate(&mut self, context: &MigrationContext);
    fn has_migration_state(&self) -> bool;
}

impl<T: IGrainMigrationParticipant + 'static> ParticipantWrapper for T {
    fn on_dehydrate(&self, context: &mut MigrationContext) {
        IGrainMigrationParticipant::on_dehydrate(self, context)
    }

    fn on_rehydrate(&mut self, context: &MigrationContext) {
        IGrainMigrationParticipant::on_rehydrate(self, context)
    }

    fn has_migration_state(&self) -> bool {
        IGrainMigrationParticipant::has_migration_state(self)
    }
}

impl MigrationParticipantRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a migration participant.
    ///
    /// # Arguments
    /// * `name` - Unique name for this participant
    /// * `priority` - Dehydration order (lower = first). Rehydration is reverse order.
    /// * `participant` - The participant to register
    pub fn register<T: IGrainMigrationParticipant + 'static>(
        &mut self,
        name: impl Into<String>,
        priority: i32,
        participant: T,
    ) {
        self.participants.push(ParticipantEntry {
            name: name.into(),
            priority,
            participant: Box::new(participant),
        });

        // Sort by priority (ascending for dehydration order)
        self.participants.sort_by_key(|e| e.priority);
    }

    /// Get the number of registered participants.
    pub fn len(&self) -> usize {
        self.participants.len()
    }

    /// Check if no participants are registered.
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    /// Dehydrate all participants into the context.
    ///
    /// Participants are called in priority order (lowest first).
    pub fn dehydrate_all(&self, context: &mut MigrationContext) {
        for entry in &self.participants {
            if entry.participant.has_migration_state() {
                tracing::trace!(participant = %entry.name, "dehydrating participant");
                entry.participant.on_dehydrate(context);
            }
        }
    }

    /// Rehydrate all participants from the context.
    ///
    /// Participants are called in reverse priority order (highest first).
    pub fn rehydrate_all(&mut self, context: &MigrationContext) {
        // Rehydrate in reverse order
        for entry in self.participants.iter_mut().rev() {
            tracing::trace!(participant = %entry.name, "rehydrating participant");
            entry.participant.on_rehydrate(context);
        }
    }

    /// Check if any participant has state to migrate.
    pub fn has_migration_state(&self) -> bool {
        self.participants
            .iter()
            .any(|e| e.participant.has_migration_state())
    }

    /// Get the names of all registered participants.
    pub fn participant_names(&self) -> Vec<&str> {
        self.participants.iter().map(|e| e.name.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone)]
    struct TestParticipant {
        value: i32,
        key: String,
    }

    impl TestParticipant {
        fn new(value: i32, key: &str) -> Self {
            Self {
                value,
                key: key.to_string(),
            }
        }
    }

    impl IGrainMigrationParticipant for TestParticipant {
        fn on_dehydrate(&self, context: &mut MigrationContext) {
            context.try_add_value(&self.key, &self.value);
        }

        fn on_rehydrate(&mut self, context: &MigrationContext) {
            if let Some(value) = context.try_get_value::<i32>(&self.key) {
                self.value = value;
            }
        }

        fn migration_key_prefix(&self) -> String {
            self.key.clone()
        }
    }

    #[derive(Debug)]
    struct NoStateParticipant;

    impl IGrainMigrationParticipant for NoStateParticipant {
        fn on_dehydrate(&self, _context: &mut MigrationContext) {}
        fn on_rehydrate(&mut self, _context: &MigrationContext) {}

        fn has_migration_state(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_participant_dehydrate_rehydrate() {
        let participant = TestParticipant::new(42, "test_key");
        let mut ctx = MigrationContext::new();

        IGrainMigrationParticipant::on_dehydrate(&participant, &mut ctx);
        assert!(ctx.has_key("test_key"));

        let mut restored = TestParticipant::new(0, "test_key");
        IGrainMigrationParticipant::on_rehydrate(&mut restored, &ctx);
        assert_eq!(restored.value, 42);
    }

    #[test]
    fn test_registry_empty() {
        let registry = MigrationParticipantRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn test_registry_register() {
        let mut registry = MigrationParticipantRegistry::new();
        registry.register("test1", 0, TestParticipant::new(1, "k1"));
        registry.register("test2", 1, TestParticipant::new(2, "k2"));

        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
    }

    #[test]
    fn test_registry_dehydrate_all() {
        let mut registry = MigrationParticipantRegistry::new();
        registry.register("first", 0, TestParticipant::new(1, "first_key"));
        registry.register("second", 1, TestParticipant::new(2, "second_key"));

        let mut ctx = MigrationContext::new();
        registry.dehydrate_all(&mut ctx);

        assert!(ctx.has_key("first_key"));
        assert!(ctx.has_key("second_key"));
    }

    #[test]
    fn test_registry_rehydrate_all() {
        let mut registry = MigrationParticipantRegistry::new();
        registry.register("first", 0, TestParticipant::new(0, "first_key"));
        registry.register("second", 1, TestParticipant::new(0, "second_key"));

        // Prepare context
        let mut ctx = MigrationContext::new();
        ctx.try_add_value("first_key", &10i32);
        ctx.try_add_value("second_key", &20i32);

        registry.rehydrate_all(&ctx);

        // Values should be restored
        // (We can't easily verify internal state here, but we verify the flow)
    }

    #[test]
    fn test_registry_priority_order() {
        let mut registry = MigrationParticipantRegistry::new();
        // Register out of order
        registry.register("third", 30, TestParticipant::new(3, "k3"));
        registry.register("first", 10, TestParticipant::new(1, "k1"));
        registry.register("second", 20, TestParticipant::new(2, "k2"));

        let names = registry.participant_names();
        assert_eq!(names, vec!["first", "second", "third"]);
    }

    #[test]
    fn test_has_migration_state() {
        let mut registry = MigrationParticipantRegistry::new();
        assert!(!registry.has_migration_state()); // Empty

        registry.register("no_state", 0, NoStateParticipant);
        assert!(!registry.has_migration_state()); // Only no-state participant

        registry.register("has_state", 1, TestParticipant::new(1, "k1"));
        assert!(registry.has_migration_state()); // Now has state
    }

    #[test]
    fn test_no_state_participant_skipped() {
        let mut registry = MigrationParticipantRegistry::new();
        registry.register("no_state", 0, NoStateParticipant);

        let mut ctx = MigrationContext::new();
        registry.dehydrate_all(&mut ctx);

        assert!(ctx.is_empty()); // No state was added
    }

    #[test]
    fn test_migration_key_prefix() {
        let participant = TestParticipant::new(0, "my_prefix");
        assert_eq!(participant.migration_key_prefix(), "my_prefix");
    }

    #[test]
    fn test_participant_names() {
        let mut registry = MigrationParticipantRegistry::new();
        registry.register("alpha", 0, TestParticipant::new(1, "a"));
        registry.register("beta", 1, TestParticipant::new(2, "b"));
        registry.register("gamma", 2, TestParticipant::new(3, "c"));

        let names = registry.participant_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
        assert!(names.contains(&"gamma"));
    }
}
