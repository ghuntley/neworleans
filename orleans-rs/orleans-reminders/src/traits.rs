//! Core traits for the reminder system.

use crate::error::ReminderResult;
use crate::reminder::{GrainReminder, TickStatus};
use crate::reminder_entry::ReminderEntry;
use async_trait::async_trait;
use orleans_core::GrainId;
use orleans_directory::RingRange;
use std::time::Duration;

/// Trait implemented by grains that want to receive reminder callbacks.
///
/// Grains must implement this trait to receive reminder notifications.
/// When a reminder fires, the `receive_reminder` method is called with
/// the reminder name and tick status.
///
/// # Example
///
/// ```ignore
/// use orleans_reminders::{IRemindable, TickStatus, ReminderResult};
///
/// struct MyGrain {
///     counter: u32,
/// }
///
/// #[async_trait]
/// impl IRemindable for MyGrain {
///     async fn receive_reminder(
///         &mut self,
///         reminder_name: &str,
///         tick_status: TickStatus,
///     ) -> ReminderResult<()> {
///         match reminder_name {
///             "daily-check" => {
///                 self.counter += 1;
///                 println!("Daily check #{}", self.counter);
///             }
///             _ => {
///                 println!("Unknown reminder: {}", reminder_name);
///             }
///         }
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait IRemindable: Send + Sync {
    /// Called when a reminder fires.
    ///
    /// # Arguments
    ///
    /// * `reminder_name` - The name of the reminder that fired
    /// * `tick_status` - Status information about the current tick
    ///
    /// # Errors
    ///
    /// If this method returns an error, the reminder will still continue
    /// to fire at its scheduled intervals. Errors are logged but do not
    /// affect the reminder's schedule.
    async fn receive_reminder(
        &mut self,
        reminder_name: &str,
        tick_status: TickStatus,
    ) -> ReminderResult<()>;
}

/// Trait for registering and managing reminders from within a grain.
///
/// This trait is typically provided to grains through their context,
/// allowing them to register, update, and unregister reminders.
#[async_trait]
pub trait IReminderRegistry: Send + Sync {
    /// Registers a new reminder or updates an existing one.
    ///
    /// If a reminder with the same name already exists for this grain,
    /// it will be updated with the new due time and period.
    ///
    /// # Arguments
    ///
    /// * `reminder_name` - Unique name for the reminder (within this grain)
    /// * `due_time` - Time from now until the first tick
    /// * `period` - Time between subsequent ticks
    ///
    /// # Returns
    ///
    /// A `GrainReminder` handle that can be used to identify the reminder.
    async fn register_or_update_reminder(
        &self,
        reminder_name: &str,
        due_time: Duration,
        period: Duration,
    ) -> ReminderResult<GrainReminder>;

    /// Unregisters a reminder.
    ///
    /// The reminder will stop firing after this call.
    ///
    /// # Arguments
    ///
    /// * `reminder` - The reminder handle returned from `register_or_update_reminder`
    async fn unregister_reminder(&self, reminder: GrainReminder) -> ReminderResult<()>;

    /// Gets a reminder by name.
    ///
    /// # Arguments
    ///
    /// * `reminder_name` - The name of the reminder to retrieve
    ///
    /// # Returns
    ///
    /// `Some(GrainReminder)` if the reminder exists, `None` otherwise.
    async fn get_reminder(&self, reminder_name: &str) -> ReminderResult<Option<GrainReminder>>;

    /// Gets all reminders registered for this grain.
    async fn get_reminders(&self) -> ReminderResult<Vec<GrainReminder>>;
}

/// Storage interface for persisting reminders.
///
/// Implementations of this trait provide the actual storage backend for
/// reminders (e.g., in-memory, SQL database, etc.).
#[async_trait]
pub trait IReminderTable: Send + Sync {
    /// Reads all reminders for a specific grain.
    ///
    /// # Arguments
    ///
    /// * `grain_id` - The grain whose reminders to read
    async fn read_rows(&self, grain_id: &GrainId) -> ReminderResult<Vec<ReminderEntry>>;

    /// Reads a specific reminder.
    ///
    /// # Arguments
    ///
    /// * `grain_id` - The grain that owns the reminder
    /// * `reminder_name` - The name of the reminder
    async fn read_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
    ) -> ReminderResult<Option<ReminderEntry>>;

    /// Reads all reminders in a hash ring range.
    ///
    /// This is used by silos to discover which reminders they are
    /// responsible for based on consistent hashing.
    ///
    /// # Arguments
    ///
    /// * `range` - The hash ring range to query
    async fn read_rows_in_range(&self, range: &RingRange) -> ReminderResult<Vec<ReminderEntry>>;

    /// Inserts or updates a reminder.
    ///
    /// If the reminder doesn't exist, it will be created.
    /// If it exists and the ETag matches (or is empty), it will be updated.
    ///
    /// # Arguments
    ///
    /// * `entry` - The reminder entry to upsert
    ///
    /// # Returns
    ///
    /// The new ETag for the reminder.
    async fn upsert_row(&self, entry: ReminderEntry) -> ReminderResult<String>;

    /// Removes a reminder.
    ///
    /// The reminder will only be removed if the ETag matches.
    ///
    /// # Arguments
    ///
    /// * `grain_id` - The grain that owns the reminder
    /// * `reminder_name` - The name of the reminder
    /// * `etag` - The expected ETag value
    ///
    /// # Returns
    ///
    /// `true` if the reminder was removed, `false` if the ETag didn't match.
    async fn remove_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
        etag: &str,
    ) -> ReminderResult<bool>;

    /// Deletes all reminders (for testing only).
    ///
    /// This method is only intended for use in tests to clean up the table.
    async fn clear_table(&self) -> ReminderResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_traits_are_object_safe() {
        // Verify that traits can be used as trait objects
        fn _takes_remindable(_: &dyn IRemindable) {}
        fn _takes_registry(_: &dyn IReminderRegistry) {}
        fn _takes_table(_: &dyn IReminderTable) {}
    }

    #[test]
    fn test_tick_status_in_trait() {
        // Verify TickStatus can be created for use in trait
        let status = TickStatus::new(
            Utc::now(),
            Utc::now(),
            Duration::from_secs(60),
        );
        assert!(status.period == Duration::from_secs(60));
    }
}
