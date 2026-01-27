//! Orleans Reminders - Persistent grain scheduled callbacks.
//!
//! This crate provides reminder support for Orleans grains:
//!
//! - **Reminders**: Persistent, cluster-aware scheduled callbacks
//! - Reminders survive silo restarts and grain deactivations
//! - Reminders are managed by the silo that owns the grain's hash range
//!
//! # Reminder Characteristics
//!
//! - Persistent (survives silo restarts)
//! - Cluster-wide (any silo can trigger based on ownership)
//! - Lower frequency (typically minutes/hours, minimum 1 minute by default)
//! - Requires grain to implement [`IRemindable`] trait
//! - Stored in reminder table (pluggable storage backend)
//!
//! # Comparison with Timers
//!
//! | Feature | Timer | Reminder |
//! |---------|-------|----------|
//! | Persistence | No | Yes |
//! | Survives deactivation | No | Yes |
//! | Survives silo restart | No | Yes |
//! | Minimum period | Milliseconds | Minutes |
//! | Cluster-aware | No | Yes |
//! | Storage required | No | Yes |
//! | Use case | In-memory polling | Scheduled tasks |
//!
//! # Example
//!
//! ```ignore
//! use orleans_reminders::{
//!     ReminderService, InMemoryReminderTable, ReminderOptions,
//!     IRemindable, TickStatus, ReminderResult,
//! };
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! // Implement IRemindable for your grain
//! struct MyGrain {
//!     counter: u32,
//! }
//!
//! #[async_trait::async_trait]
//! impl IRemindable for MyGrain {
//!     async fn receive_reminder(
//!         &mut self,
//!         reminder_name: &str,
//!         tick_status: TickStatus,
//!     ) -> ReminderResult<()> {
//!         println!("Reminder {} fired, tick count: {}",
//!             reminder_name, tick_status.tick_count());
//!         Ok(())
//!     }
//! }
//!
//! // Create and use the reminder service
//! async fn example() {
//!     let table = Arc::new(InMemoryReminderTable::new());
//!     let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
//!     let options = ReminderOptions::default();
//!
//!     let service = ReminderService::new(silo_address, table, tx, options);
//!     service.start().await.unwrap();
//!
//!     // Register a reminder
//!     service.register_or_update_reminder(
//!         &grain_id,
//!         "daily-check",
//!         Duration::from_secs(60),   // first tick in 1 minute
//!         Duration::from_secs(3600), // then every hour
//!     ).await.unwrap();
//!
//!     // Later, unregister
//!     service.unregister_reminder(&grain_id, "daily-check").await.unwrap();
//!
//!     service.stop().await.unwrap();
//! }
//! ```

mod error;
mod memory_table;
mod options;
mod reminder;
mod reminder_entry;
mod service;
mod traits;

// Re-export public types
pub use error::{ReminderError, ReminderResult};
pub use memory_table::InMemoryReminderTable;
pub use options::ReminderOptions;
pub use reminder::{GrainReminder, ReminderIdentity, TickStatus};
pub use reminder_entry::ReminderEntry;
pub use service::{ReminderCallbackMessage, ReminderService};
pub use traits::{IRemindable, IReminderRegistry, IReminderTable};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<ReminderError>();
        let _ = std::any::type_name::<ReminderOptions>();
        let _ = std::any::type_name::<GrainReminder>();
        let _ = std::any::type_name::<ReminderEntry>();
        let _ = std::any::type_name::<TickStatus>();
        let _ = std::any::type_name::<ReminderIdentity>();
        let _ = std::any::type_name::<InMemoryReminderTable>();
        let _ = std::any::type_name::<ReminderService>();
        let _ = std::any::type_name::<ReminderCallbackMessage>();
    }

    #[test]
    fn test_traits_are_accessible() {
        fn _takes_remindable(_: &dyn IRemindable) {}
        fn _takes_registry(_: &dyn IReminderRegistry) {}
        fn _takes_table(_: &dyn IReminderTable) {}
    }
}
