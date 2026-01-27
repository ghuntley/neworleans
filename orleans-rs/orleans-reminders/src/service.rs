//! Reminder service - manages reminder execution for a silo.

use crate::error::{ReminderError, ReminderResult};
use crate::options::ReminderOptions;
use crate::reminder::{GrainReminder, ReminderIdentity, TickStatus};
use crate::reminder_entry::ReminderEntry;
use crate::traits::IReminderTable;
use chrono::{Duration as ChronoDuration, Utc};
use orleans_core::{GrainId, SiloAddress};
use orleans_directory::RingRange;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

/// Data for a locally-managed reminder.
struct LocalReminderData {
    /// Handle to the timer task for this reminder.
    timer_handle: JoinHandle<()>,
    /// Token to cancel this reminder's timer.
    cancellation: CancellationToken,
}

/// Inner state of the reminder service.
struct ReminderServiceInner {
    /// The address of this silo.
    silo_address: SiloAddress,
    /// The reminder table for persistence.
    reminder_table: Arc<dyn IReminderTable>,
    /// Channel for sending reminder callbacks.
    callback_sender: mpsc::UnboundedSender<ReminderCallbackMessage>,
    /// Configuration options.
    options: ReminderOptions,
    /// The current hash ring range owned by this silo.
    owned_range: RwLock<RingRange>,
    /// Locally managed reminders.
    local_reminders: RwLock<HashMap<ReminderIdentity, LocalReminderData>>,
    /// Cancellation token for shutdown.
    shutdown_token: CancellationToken,
    /// Handle to the refresh task.
    refresh_handle: RwLock<Option<JoinHandle<()>>>,
    /// Whether the service is running.
    is_running: RwLock<bool>,
}

/// The reminder service manages reminder execution for a silo.
///
/// It is responsible for:
/// - Loading reminders from the reminder table based on consistent hashing
/// - Scheduling and firing reminders at the appropriate times
/// - Refreshing reminder assignments when membership changes
///
/// # Example
///
/// ```ignore
/// use orleans_reminders::{ReminderService, InMemoryReminderTable, ReminderOptions};
/// use std::sync::Arc;
///
/// let table = Arc::new(InMemoryReminderTable::new());
/// let options = ReminderOptions::default();
/// let (callback_tx, callback_rx) = tokio::sync::mpsc::unbounded_channel();
///
/// let service = ReminderService::new(
///     silo_address,
///     table,
///     callback_tx,
///     options,
/// );
///
/// // Start the service
/// service.start().await?;
///
/// // Register a reminder
/// service.register_or_update_reminder(
///     &grain_id,
///     "daily-check",
///     Duration::from_secs(60),
///     Duration::from_secs(86400),
/// ).await?;
///
/// // Stop the service
/// service.stop().await?;
/// ```
#[derive(Clone)]
pub struct ReminderService {
    inner: Arc<ReminderServiceInner>,
}

/// Message sent when a reminder fires.
#[derive(Debug, Clone)]
pub struct ReminderCallbackMessage {
    /// The grain to notify.
    pub grain_id: GrainId,
    /// The reminder name.
    pub reminder_name: String,
    /// The tick status.
    pub tick_status: TickStatus,
}

impl ReminderService {
    /// Creates a new reminder service.
    ///
    /// # Arguments
    ///
    /// * `silo_address` - The address of this silo
    /// * `reminder_table` - The reminder table for persistence
    /// * `callback_sender` - Channel for sending reminder callbacks
    /// * `options` - Configuration options
    pub fn new(
        silo_address: SiloAddress,
        reminder_table: Arc<dyn IReminderTable>,
        callback_sender: mpsc::UnboundedSender<ReminderCallbackMessage>,
        options: ReminderOptions,
    ) -> Self {
        Self {
            inner: Arc::new(ReminderServiceInner {
                silo_address,
                reminder_table,
                callback_sender,
                options,
                owned_range: RwLock::new(RingRange::empty()),
                local_reminders: RwLock::new(HashMap::new()),
                shutdown_token: CancellationToken::new(),
                refresh_handle: RwLock::new(None),
                is_running: RwLock::new(false),
            }),
        }
    }

    /// Returns the silo address.
    pub fn silo_address(&self) -> &SiloAddress {
        &self.inner.silo_address
    }

    /// Returns the number of locally managed reminders.
    pub fn local_reminder_count(&self) -> usize {
        self.inner.local_reminders.read().len()
    }

    /// Returns whether the service is running.
    pub fn is_running(&self) -> bool {
        *self.inner.is_running.read()
    }

    /// Updates the hash ring range owned by this silo.
    ///
    /// This should be called when membership changes affect which
    /// hash ranges this silo is responsible for.
    #[instrument(skip(self), level = "info")]
    pub fn update_owned_range(&self, range: RingRange) {
        info!("Updating owned range");
        *self.inner.owned_range.write() = range;
    }

    /// Starts the reminder service.
    ///
    /// This will:
    /// 1. Load reminders in our owned range
    /// 2. Start the periodic refresh task
    #[instrument(skip(self), level = "info", fields(silo = %self.inner.silo_address))]
    pub async fn start(&self) -> ReminderResult<()> {
        if *self.inner.is_running.read() {
            warn!("Reminder service already running");
            return Ok(());
        }

        info!("Starting reminder service");

        // Load initial reminders
        self.refresh_reminders().await?;

        // Start periodic refresh
        let service = self.clone();
        let refresh_period = self.inner.options.refresh_reminder_period;
        let shutdown = self.inner.shutdown_token.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(refresh_period);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = service.refresh_reminders().await {
                            error!("Failed to refresh reminders: {}", e);
                        }
                    }
                    _ = shutdown.cancelled() => {
                        debug!("Refresh task cancelled");
                        break;
                    }
                }
            }
        });

        *self.inner.refresh_handle.write() = Some(handle);
        *self.inner.is_running.write() = true;

        info!("Reminder service started");
        Ok(())
    }

    /// Stops the reminder service.
    #[instrument(skip(self), level = "info", fields(silo = %self.inner.silo_address))]
    pub async fn stop(&self) -> ReminderResult<()> {
        if !*self.inner.is_running.read() {
            debug!("Reminder service not running");
            return Ok(());
        }

        info!("Stopping reminder service");

        // Signal shutdown
        self.inner.shutdown_token.cancel();

        // Wait for refresh task to complete
        if let Some(handle) = self.inner.refresh_handle.write().take() {
            let _ = handle.await;
        }

        // Cancel all local reminders
        let mut local = self.inner.local_reminders.write();
        for (_, data) in local.drain() {
            data.cancellation.cancel();
            data.timer_handle.abort();
        }

        *self.inner.is_running.write() = false;
        info!("Reminder service stopped");
        Ok(())
    }

    /// Refreshes the local reminder list from the reminder table.
    #[instrument(skip(self), level = "debug")]
    async fn refresh_reminders(&self) -> ReminderResult<()> {
        let range = self.inner.owned_range.read().clone();

        if range.is_empty() {
            debug!("No owned range, skipping refresh");
            return Ok(());
        }

        // Load reminders in our range
        let reminders = self.inner.reminder_table.read_rows_in_range(&range).await?;

        debug!(count = reminders.len(), "Loaded reminders from table");

        // Build set of current identities
        let current_identities: HashSet<_> = reminders
            .iter()
            .map(|e| ReminderIdentity::new(e.grain_id.clone(), &e.reminder_name))
            .collect();

        // Get current local reminders
        let local_identities: HashSet<_> = self
            .inner
            .local_reminders
            .read()
            .keys()
            .cloned()
            .collect();

        // Add new reminders
        for entry in &reminders {
            let identity = ReminderIdentity::new(entry.grain_id.clone(), &entry.reminder_name);
            if !local_identities.contains(&identity) {
                self.start_local_reminder(entry.clone());
            }
        }

        // Remove reminders no longer in our range
        let mut local = self.inner.local_reminders.write();
        local.retain(|identity, data| {
            if current_identities.contains(identity) {
                true
            } else {
                debug!(%identity, "Removing reminder no longer in range");
                data.cancellation.cancel();
                data.timer_handle.abort();
                false
            }
        });

        Ok(())
    }

    /// Starts a local timer for a reminder.
    fn start_local_reminder(&self, entry: ReminderEntry) {
        let identity = ReminderIdentity::new(entry.grain_id.clone(), &entry.reminder_name);

        debug!(%identity, "Starting local reminder");

        let cancellation = CancellationToken::new();
        let sender = self.inner.callback_sender.clone();
        let entry_clone = entry.clone();
        let cancel_clone = cancellation.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Calculate time until next tick
                let next_tick = entry_clone.get_next_tick_time();
                let now = Utc::now();

                if next_tick > now {
                    let delay = (next_tick - now)
                        .to_std()
                        .unwrap_or(Duration::from_millis(100));

                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = cancel_clone.cancelled() => {
                            debug!("Reminder timer cancelled");
                            return;
                        }
                    }
                }

                // Fire the reminder
                let tick_status = TickStatus::new(
                    entry_clone.start_at,
                    Utc::now(),
                    entry_clone.period,
                );

                let message = ReminderCallbackMessage {
                    grain_id: entry_clone.grain_id.clone(),
                    reminder_name: entry_clone.reminder_name.clone(),
                    tick_status,
                };

                if sender.send(message).is_err() {
                    error!("Failed to send reminder callback, channel closed");
                    return;
                }

                // Wait for the next period
                if entry_clone.period.is_zero() {
                    // One-shot reminder, exit after first fire
                    return;
                }

                tokio::select! {
                    _ = tokio::time::sleep(entry_clone.period) => {}
                    _ = cancel_clone.cancelled() => {
                        debug!("Reminder timer cancelled");
                        return;
                    }
                }
            }
        });

        self.inner.local_reminders.write().insert(
            identity,
            LocalReminderData {
                timer_handle: handle,
                cancellation,
            },
        );
    }

    /// Registers a new reminder or updates an existing one.
    #[instrument(skip(self), level = "info", fields(
        grain_id = %grain_id,
        reminder_name = %reminder_name,
        due_secs = due_time.as_secs(),
        period_secs = period.as_secs()
    ))]
    pub async fn register_or_update_reminder(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
        due_time: Duration,
        period: Duration,
    ) -> ReminderResult<GrainReminder> {
        // Validate period
        if period < self.inner.options.min_reminder_period && !period.is_zero() {
            return Err(ReminderError::PeriodTooShort {
                period_secs: period.as_secs_f64(),
                min_secs: self.inner.options.min_reminder_period.as_secs_f64(),
            });
        }

        // Validate reminder name
        if reminder_name.is_empty() {
            return Err(ReminderError::InvalidName("reminder name cannot be empty".to_string()));
        }

        // Calculate start time
        let effective_due_time = due_time.max(self.inner.options.min_due_time);
        let start_at = Utc::now() + ChronoDuration::from_std(effective_due_time).unwrap_or_default();

        // Get existing entry for ETag (if updating)
        let existing = self
            .inner
            .reminder_table
            .read_row(grain_id, reminder_name)
            .await?;

        let entry = ReminderEntry {
            grain_id: grain_id.clone(),
            reminder_name: reminder_name.to_string(),
            start_at,
            period,
            etag: existing.map(|e| e.etag).unwrap_or_default(),
        };

        // Persist to table
        let _new_etag = self.inner.reminder_table.upsert_row(entry).await?;

        info!("Registered reminder");

        // Trigger refresh to pick up the new reminder
        self.refresh_reminders().await?;

        Ok(GrainReminder::new(grain_id.clone(), reminder_name))
    }

    /// Unregisters a reminder.
    #[instrument(skip(self), level = "info", fields(
        grain_id = %grain_id,
        reminder_name = %reminder_name
    ))]
    pub async fn unregister_reminder(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
    ) -> ReminderResult<()> {
        // Get the current entry to get the ETag
        let entry = self
            .inner
            .reminder_table
            .read_row(grain_id, reminder_name)
            .await?
            .ok_or_else(|| ReminderError::NotFound(reminder_name.to_string()))?;

        // Remove from table
        let removed = self
            .inner
            .reminder_table
            .remove_row(grain_id, reminder_name, &entry.etag)
            .await?;

        if !removed {
            warn!("Reminder was modified concurrently, may not have been removed");
        }

        // Remove from local reminders
        let identity = ReminderIdentity::new(grain_id.clone(), reminder_name);
        if let Some(data) = self.inner.local_reminders.write().remove(&identity) {
            data.cancellation.cancel();
            data.timer_handle.abort();
        }

        info!("Unregistered reminder");
        Ok(())
    }

    /// Gets a reminder by name.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_reminder(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
    ) -> ReminderResult<Option<GrainReminder>> {
        let entry = self.inner.reminder_table.read_row(grain_id, reminder_name).await?;

        Ok(entry.map(|e| GrainReminder::new(e.grain_id, e.reminder_name)))
    }

    /// Gets all reminders for a grain.
    #[instrument(skip(self), level = "debug")]
    pub async fn get_reminders(&self, grain_id: &GrainId) -> ReminderResult<Vec<GrainReminder>> {
        let entries = self.inner.reminder_table.read_rows(grain_id).await?;

        Ok(entries
            .into_iter()
            .map(|e| GrainReminder::new(e.grain_id, e.reminder_name))
            .collect())
    }
}

impl Drop for ReminderServiceInner {
    fn drop(&mut self) {
        // Cancel all timers
        self.shutdown_token.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_table::InMemoryReminderTable;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_silo_address() -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111),
            1,
        )
    }

    fn test_grain_id() -> GrainId {
        GrainId::create("TestGrain", "test-key")
    }

    #[tokio::test]
    async fn test_service_creation() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        assert!(!service.is_running());
        assert_eq!(service.local_reminder_count(), 0);
    }

    #[tokio::test]
    async fn test_service_start_stop() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        service.start().await.unwrap();
        assert!(service.is_running());

        service.stop().await.unwrap();
        assert!(!service.is_running());
    }

    #[tokio::test]
    async fn test_register_reminder() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table.clone(), tx, options);
        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        let grain_id = test_grain_id();
        let reminder = service
            .register_or_update_reminder(
                &grain_id,
                "test-reminder",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        assert_eq!(reminder.name(), "test-reminder");
        assert_eq!(table.len(), 1);

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_register_reminder_period_too_short() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::default(); // 1 minute minimum

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        let grain_id = test_grain_id();
        let result = service
            .register_or_update_reminder(
                &grain_id,
                "test-reminder",
                Duration::from_secs(5),
                Duration::from_secs(30), // Less than 1 minute
            )
            .await;

        assert!(matches!(result, Err(ReminderError::PeriodTooShort { .. })));
    }

    #[tokio::test]
    async fn test_register_reminder_empty_name() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        let grain_id = test_grain_id();
        let result = service
            .register_or_update_reminder(
                &grain_id,
                "",
                Duration::from_secs(5),
                Duration::from_secs(60),
            )
            .await;

        assert!(matches!(result, Err(ReminderError::InvalidName(_))));
    }

    #[tokio::test]
    async fn test_unregister_reminder() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table.clone(), tx, options);
        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        let grain_id = test_grain_id();
        service
            .register_or_update_reminder(
                &grain_id,
                "test-reminder",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        assert_eq!(table.len(), 1);

        service
            .unregister_reminder(&grain_id, "test-reminder")
            .await
            .unwrap();

        assert_eq!(table.len(), 0);
        assert_eq!(service.local_reminder_count(), 0);

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_unregister_nonexistent_reminder() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        let grain_id = test_grain_id();
        let result = service
            .unregister_reminder(&grain_id, "nonexistent")
            .await;

        assert!(matches!(result, Err(ReminderError::NotFound(_))));
    }

    #[tokio::test]
    async fn test_get_reminder() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);
        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        let grain_id = test_grain_id();
        service
            .register_or_update_reminder(
                &grain_id,
                "test-reminder",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        let reminder = service
            .get_reminder(&grain_id, "test-reminder")
            .await
            .unwrap();
        assert!(reminder.is_some());
        assert_eq!(reminder.unwrap().name(), "test-reminder");

        let not_found = service
            .get_reminder(&grain_id, "nonexistent")
            .await
            .unwrap();
        assert!(not_found.is_none());

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_get_reminders() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);
        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        let grain_id = test_grain_id();
        service
            .register_or_update_reminder(
                &grain_id,
                "reminder-1",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .await
            .unwrap();
        service
            .register_or_update_reminder(
                &grain_id,
                "reminder-2",
                Duration::from_millis(100),
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        let reminders = service.get_reminders(&grain_id).await.unwrap();
        assert_eq!(reminders.len(), 2);

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_reminder_fires() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);
        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        let grain_id = test_grain_id();
        service
            .register_or_update_reminder(
                &grain_id,
                "quick-reminder",
                Duration::from_millis(10), // Fire quickly
                Duration::from_millis(100),
            )
            .await
            .unwrap();

        // Wait for the reminder to fire
        let message = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("Timeout waiting for reminder")
            .expect("Channel closed");

        assert_eq!(message.grain_id, grain_id);
        assert_eq!(message.reminder_name, "quick-reminder");

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_update_owned_range() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service = ReminderService::new(test_silo_address(), table, tx, options);

        assert_eq!(service.local_reminder_count(), 0);

        service.update_owned_range(RingRange::full());
        service.start().await.unwrap();

        // The range should be updated
        assert!(service.is_running());

        service.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_service_clone() {
        let table = Arc::new(InMemoryReminderTable::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let options = ReminderOptions::for_testing();

        let service1 = ReminderService::new(test_silo_address(), table, tx, options);
        let service2 = service1.clone();

        // Both should reference the same inner state
        service1.update_owned_range(RingRange::full());
        service1.start().await.unwrap();

        assert!(service1.is_running());
        assert!(service2.is_running()); // Same inner state

        service2.stop().await.unwrap();

        assert!(!service1.is_running());
        assert!(!service2.is_running());
    }
}
