//! In-memory reminder table implementation for testing and development.

use crate::error::{ReminderError, ReminderResult};
use crate::reminder_entry::ReminderEntry;
use crate::traits::IReminderTable;
use async_trait::async_trait;
use orleans_core::GrainId;
use orleans_directory::RingRange;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, instrument, trace};

/// Key type for the reminder storage map.
type ReminderKey = (GrainId, String);

/// In-memory implementation of the reminder table.
///
/// This implementation is suitable for testing and single-silo development.
/// It does not persist reminders across process restarts.
///
/// # Thread Safety
///
/// This implementation is thread-safe and can be shared between multiple
/// tasks/threads.
///
/// # Example
///
/// ```
/// use orleans_reminders::InMemoryReminderTable;
/// use std::sync::Arc;
///
/// let table = Arc::new(InMemoryReminderTable::new());
/// ```
pub struct InMemoryReminderTable {
    /// The reminder storage.
    reminders: RwLock<HashMap<ReminderKey, ReminderEntry>>,
    /// Counter for generating ETags.
    etag_counter: AtomicU64,
}

impl InMemoryReminderTable {
    /// Creates a new in-memory reminder table.
    pub fn new() -> Self {
        Self {
            reminders: RwLock::new(HashMap::new()),
            etag_counter: AtomicU64::new(1),
        }
    }

    /// Returns the number of reminders in the table.
    pub fn len(&self) -> usize {
        self.reminders.read().len()
    }

    /// Returns true if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.reminders.read().is_empty()
    }

    /// Generates a new ETag.
    fn generate_etag(&self) -> String {
        let value = self.etag_counter.fetch_add(1, Ordering::Relaxed);
        format!("{:016x}", value)
    }
}

impl Default for InMemoryReminderTable {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for InMemoryReminderTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.len();
        f.debug_struct("InMemoryReminderTable")
            .field("reminder_count", &count)
            .finish()
    }
}

#[async_trait]
impl IReminderTable for InMemoryReminderTable {
    #[instrument(skip(self), level = "debug", fields(grain_id = %grain_id))]
    async fn read_rows(&self, grain_id: &GrainId) -> ReminderResult<Vec<ReminderEntry>> {
        let reminders = self.reminders.read();
        let entries: Vec<ReminderEntry> = reminders
            .iter()
            .filter(|((gid, _), _)| gid == grain_id)
            .map(|(_, entry)| entry.clone())
            .collect();

        debug!(count = entries.len(), "Read reminders for grain");
        Ok(entries)
    }

    #[instrument(skip(self), level = "debug", fields(grain_id = %grain_id, reminder_name = %reminder_name))]
    async fn read_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
    ) -> ReminderResult<Option<ReminderEntry>> {
        let reminders = self.reminders.read();
        let key = (grain_id.clone(), reminder_name.to_string());
        let entry = reminders.get(&key).cloned();

        if entry.is_some() {
            trace!("Found reminder");
        } else {
            trace!("Reminder not found");
        }

        Ok(entry)
    }

    #[instrument(skip(self, range), level = "debug")]
    async fn read_rows_in_range(&self, range: &RingRange) -> ReminderResult<Vec<ReminderEntry>> {
        let reminders = self.reminders.read();
        let entries: Vec<ReminderEntry> = reminders
            .values()
            .filter(|entry| range.contains(entry.get_grain_hash_code()))
            .cloned()
            .collect();

        debug!(count = entries.len(), "Read reminders in range");
        Ok(entries)
    }

    #[instrument(skip(self), level = "debug", fields(
        grain_id = %entry.grain_id,
        reminder_name = %entry.reminder_name
    ))]
    async fn upsert_row(&self, mut entry: ReminderEntry) -> ReminderResult<String> {
        let key = (entry.grain_id.clone(), entry.reminder_name.clone());
        let new_etag = self.generate_etag();

        let mut reminders = self.reminders.write();

        // Check ETag if updating existing entry
        if let Some(existing) = reminders.get(&key) {
            if !entry.etag.is_empty() && entry.etag != existing.etag {
                return Err(ReminderError::EtagMismatch {
                    expected: entry.etag.clone(),
                    actual: existing.etag.clone(),
                });
            }
            debug!(old_etag = %existing.etag, new_etag = %new_etag, "Updating existing reminder");
        } else {
            debug!(new_etag = %new_etag, "Creating new reminder");
        }

        entry.etag = new_etag.clone();
        reminders.insert(key, entry);

        Ok(new_etag)
    }

    #[instrument(skip(self), level = "debug", fields(grain_id = %grain_id, reminder_name = %reminder_name))]
    async fn remove_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
        etag: &str,
    ) -> ReminderResult<bool> {
        let key = (grain_id.clone(), reminder_name.to_string());
        let mut reminders = self.reminders.write();

        if let Some(existing) = reminders.get(&key) {
            if existing.etag == etag || etag == "*" {
                reminders.remove(&key);
                debug!(etag = %etag, "Removed reminder");
                return Ok(true);
            } else {
                debug!(
                    expected_etag = %etag,
                    actual_etag = %existing.etag,
                    "ETag mismatch, reminder not removed"
                );
                return Ok(false);
            }
        }

        debug!("Reminder not found, nothing to remove");
        Ok(false)
    }

    #[instrument(skip(self), level = "debug")]
    async fn clear_table(&self) -> ReminderResult<()> {
        let mut reminders = self.reminders.write();
        let count = reminders.len();
        reminders.clear();
        debug!(count, "Cleared all reminders");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::time::Duration;

    fn test_grain_id(key: &str) -> GrainId {
        GrainId::create("TestGrain", key)
    }

    fn test_entry(grain_key: &str, reminder_name: &str) -> ReminderEntry {
        ReminderEntry::new(
            test_grain_id(grain_key),
            reminder_name,
            Utc::now(),
            Duration::from_secs(60),
        )
    }

    #[tokio::test]
    async fn test_new_table_is_empty() {
        let table = InMemoryReminderTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
    }

    #[tokio::test]
    async fn test_upsert_creates_reminder() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        let etag = table.upsert_row(entry.clone()).await.unwrap();

        assert!(!etag.is_empty());
        assert_eq!(table.len(), 1);
    }

    #[tokio::test]
    async fn test_upsert_updates_existing() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        let etag1 = table.upsert_row(entry.clone()).await.unwrap();

        // Update with new period
        let mut updated = test_entry("grain-1", "reminder-1");
        updated.etag = etag1.clone();
        updated.period = Duration::from_secs(120);

        let etag2 = table.upsert_row(updated).await.unwrap();

        assert_ne!(etag1, etag2);
        assert_eq!(table.len(), 1);

        // Verify the update
        let read = table
            .read_row(&test_grain_id("grain-1"), "reminder-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read.period, Duration::from_secs(120));
    }

    #[tokio::test]
    async fn test_upsert_etag_mismatch() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        let _etag = table.upsert_row(entry).await.unwrap();

        // Try to update with wrong ETag
        let mut updated = test_entry("grain-1", "reminder-1");
        updated.etag = "wrong-etag".to_string();

        let result = table.upsert_row(updated).await;
        assert!(matches!(result, Err(ReminderError::EtagMismatch { .. })));
    }

    #[tokio::test]
    async fn test_read_row() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        table.upsert_row(entry.clone()).await.unwrap();

        let read = table
            .read_row(&test_grain_id("grain-1"), "reminder-1")
            .await
            .unwrap();
        assert!(read.is_some());

        let read = read.unwrap();
        assert_eq!(read.grain_id, test_grain_id("grain-1"));
        assert_eq!(read.reminder_name, "reminder-1");
    }

    #[tokio::test]
    async fn test_read_row_not_found() {
        let table = InMemoryReminderTable::new();

        let read = table
            .read_row(&test_grain_id("grain-1"), "nonexistent")
            .await
            .unwrap();
        assert!(read.is_none());
    }

    #[tokio::test]
    async fn test_read_rows_for_grain() {
        let table = InMemoryReminderTable::new();

        // Create multiple reminders for one grain
        table.upsert_row(test_entry("grain-1", "reminder-1")).await.unwrap();
        table.upsert_row(test_entry("grain-1", "reminder-2")).await.unwrap();
        table.upsert_row(test_entry("grain-2", "reminder-1")).await.unwrap();

        let rows = table.read_rows(&test_grain_id("grain-1")).await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn test_read_rows_in_range() {
        let table = InMemoryReminderTable::new();

        // Create reminders with different grain IDs
        table.upsert_row(test_entry("grain-1", "reminder-1")).await.unwrap();
        table.upsert_row(test_entry("grain-2", "reminder-1")).await.unwrap();
        table.upsert_row(test_entry("grain-3", "reminder-1")).await.unwrap();

        // Use full range to get all reminders
        let full_range = RingRange::full();
        let rows = table.read_rows_in_range(&full_range).await.unwrap();
        assert_eq!(rows.len(), 3);

        // Use empty range to get no reminders
        let empty_range = RingRange::empty();
        let rows = table.read_rows_in_range(&empty_range).await.unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[tokio::test]
    async fn test_remove_row_success() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        let etag = table.upsert_row(entry).await.unwrap();

        let removed = table
            .remove_row(&test_grain_id("grain-1"), "reminder-1", &etag)
            .await
            .unwrap();
        assert!(removed);
        assert!(table.is_empty());
    }

    #[tokio::test]
    async fn test_remove_row_wildcard_etag() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        table.upsert_row(entry).await.unwrap();

        // Use wildcard ETag
        let removed = table
            .remove_row(&test_grain_id("grain-1"), "reminder-1", "*")
            .await
            .unwrap();
        assert!(removed);
        assert!(table.is_empty());
    }

    #[tokio::test]
    async fn test_remove_row_etag_mismatch() {
        let table = InMemoryReminderTable::new();
        let entry = test_entry("grain-1", "reminder-1");

        table.upsert_row(entry).await.unwrap();

        let removed = table
            .remove_row(&test_grain_id("grain-1"), "reminder-1", "wrong-etag")
            .await
            .unwrap();
        assert!(!removed);
        assert_eq!(table.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_row_not_found() {
        let table = InMemoryReminderTable::new();

        let removed = table
            .remove_row(&test_grain_id("grain-1"), "nonexistent", "*")
            .await
            .unwrap();
        assert!(!removed);
    }

    #[tokio::test]
    async fn test_clear_table() {
        let table = InMemoryReminderTable::new();

        table.upsert_row(test_entry("grain-1", "reminder-1")).await.unwrap();
        table.upsert_row(test_entry("grain-2", "reminder-1")).await.unwrap();
        assert_eq!(table.len(), 2);

        table.clear_table().await.unwrap();
        assert!(table.is_empty());
    }

    #[tokio::test]
    async fn test_debug_format() {
        let table = InMemoryReminderTable::new();
        table.upsert_row(test_entry("grain-1", "reminder-1")).await.unwrap();

        let debug = format!("{:?}", table);
        assert!(debug.contains("InMemoryReminderTable"));
        assert!(debug.contains("reminder_count"));
    }

    #[tokio::test]
    async fn test_etag_uniqueness() {
        let table = InMemoryReminderTable::new();

        let etag1 = table.upsert_row(test_entry("grain-1", "r1")).await.unwrap();
        let etag2 = table.upsert_row(test_entry("grain-1", "r2")).await.unwrap();
        let etag3 = table.upsert_row(test_entry("grain-2", "r1")).await.unwrap();

        assert_ne!(etag1, etag2);
        assert_ne!(etag2, etag3);
        assert_ne!(etag1, etag3);
    }
}
