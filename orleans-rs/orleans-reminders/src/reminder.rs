//! Reminder types and handles.

use chrono::{DateTime, Utc};
use orleans_core::GrainId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// A handle to a registered reminder.
///
/// This is returned when a reminder is successfully registered and can be used
/// to identify and manage the reminder.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GrainReminder {
    /// The name of the reminder (unique per grain).
    pub reminder_name: String,
    /// The grain that owns this reminder.
    pub grain_id: GrainId,
}

impl GrainReminder {
    /// Creates a new grain reminder handle.
    pub fn new(grain_id: GrainId, reminder_name: impl Into<String>) -> Self {
        Self {
            grain_id,
            reminder_name: reminder_name.into(),
        }
    }

    /// Returns the reminder name.
    pub fn name(&self) -> &str {
        &self.reminder_name
    }

    /// Returns the grain ID that owns this reminder.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }
}

impl fmt::Display for GrainReminder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.grain_id, self.reminder_name)
    }
}

/// Status information passed to `receive_reminder` callback.
///
/// Contains timing information about when the reminder started and
/// the current tick time.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickStatus {
    /// When the first tick of this reminder was scheduled.
    pub first_tick_time: DateTime<Utc>,
    /// When the current tick started.
    pub current_tick_time: DateTime<Utc>,
    /// The period between reminder ticks.
    pub period: Duration,
}

impl TickStatus {
    /// Creates a new tick status.
    pub fn new(first_tick_time: DateTime<Utc>, current_tick_time: DateTime<Utc>, period: Duration) -> Self {
        Self {
            first_tick_time,
            current_tick_time,
            period,
        }
    }

    /// Returns the number of ticks that have occurred since the first tick.
    pub fn tick_count(&self) -> u64 {
        if self.period.is_zero() {
            return 0;
        }

        let elapsed = self
            .current_tick_time
            .signed_duration_since(self.first_tick_time);

        if elapsed.num_milliseconds() < 0 {
            return 0;
        }

        (elapsed.num_milliseconds() as u64) / (self.period.as_millis() as u64)
    }

    /// Returns the time until the next tick.
    pub fn time_until_next_tick(&self) -> Duration {
        if self.period.is_zero() {
            return Duration::ZERO;
        }

        let period_ms = self.period.as_millis() as i64;
        let elapsed_ms = self
            .current_tick_time
            .signed_duration_since(self.first_tick_time)
            .num_milliseconds();

        let elapsed_in_period = elapsed_ms % period_ms;
        let remaining_ms = period_ms - elapsed_in_period;

        Duration::from_millis(remaining_ms.max(0) as u64)
    }
}

/// Unique identifier for a reminder (grain_id + reminder_name).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ReminderIdentity {
    /// The grain that owns this reminder.
    pub grain_id: GrainId,
    /// The name of the reminder.
    pub reminder_name: String,
}

impl ReminderIdentity {
    /// Creates a new reminder identity.
    pub fn new(grain_id: GrainId, reminder_name: impl Into<String>) -> Self {
        Self {
            grain_id,
            reminder_name: reminder_name.into(),
        }
    }

    /// Creates from a GrainReminder.
    pub fn from_reminder(reminder: &GrainReminder) -> Self {
        Self {
            grain_id: reminder.grain_id.clone(),
            reminder_name: reminder.reminder_name.clone(),
        }
    }
}

impl fmt::Display for ReminderIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.grain_id, self.reminder_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn test_grain_id() -> GrainId {
        GrainId::create("TestGrain", "test-key")
    }

    #[test]
    fn test_grain_reminder_new() {
        let grain_id = test_grain_id();
        let reminder = GrainReminder::new(grain_id.clone(), "my-reminder");

        assert_eq!(reminder.name(), "my-reminder");
        assert_eq!(reminder.grain_id(), &grain_id);
    }

    #[test]
    fn test_grain_reminder_display() {
        let grain_id = test_grain_id();
        let reminder = GrainReminder::new(grain_id, "daily-check");

        let display = format!("{}", reminder);
        assert!(display.contains("TestGrain"));
        assert!(display.contains("daily-check"));
    }

    #[test]
    fn test_grain_reminder_equality() {
        let grain_id = test_grain_id();
        let reminder1 = GrainReminder::new(grain_id.clone(), "my-reminder");
        let reminder2 = GrainReminder::new(grain_id.clone(), "my-reminder");
        let reminder3 = GrainReminder::new(grain_id.clone(), "other-reminder");

        assert_eq!(reminder1, reminder2);
        assert_ne!(reminder1, reminder3);
    }

    #[test]
    fn test_tick_status_new() {
        let first = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let current = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap();
        let period = Duration::from_secs(60);

        let status = TickStatus::new(first, current, period);

        assert_eq!(status.first_tick_time, first);
        assert_eq!(status.current_tick_time, current);
        assert_eq!(status.period, period);
    }

    #[test]
    fn test_tick_status_tick_count() {
        let first = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let current = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap();
        let period = Duration::from_secs(60);

        let status = TickStatus::new(first, current, period);

        // 5 minutes elapsed, 1 minute period = 5 ticks
        assert_eq!(status.tick_count(), 5);
    }

    #[test]
    fn test_tick_status_tick_count_zero_period() {
        let first = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let current = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 0).unwrap();
        let period = Duration::ZERO;

        let status = TickStatus::new(first, current, period);
        assert_eq!(status.tick_count(), 0);
    }

    #[test]
    fn test_tick_status_time_until_next_tick() {
        let first = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        // 5 minutes and 30 seconds elapsed
        let current = Utc.with_ymd_and_hms(2024, 1, 1, 12, 5, 30).unwrap();
        let period = Duration::from_secs(60);

        let status = TickStatus::new(first, current, period);

        // 30 seconds remaining until next tick
        let time_until = status.time_until_next_tick();
        assert_eq!(time_until, Duration::from_secs(30));
    }

    #[test]
    fn test_reminder_identity_new() {
        let grain_id = test_grain_id();
        let identity = ReminderIdentity::new(grain_id.clone(), "my-reminder");

        assert_eq!(identity.grain_id, grain_id);
        assert_eq!(identity.reminder_name, "my-reminder");
    }

    #[test]
    fn test_reminder_identity_from_reminder() {
        let grain_id = test_grain_id();
        let reminder = GrainReminder::new(grain_id.clone(), "my-reminder");
        let identity = ReminderIdentity::from_reminder(&reminder);

        assert_eq!(identity.grain_id, grain_id);
        assert_eq!(identity.reminder_name, "my-reminder");
    }

    #[test]
    fn test_reminder_identity_equality() {
        let grain_id = test_grain_id();
        let identity1 = ReminderIdentity::new(grain_id.clone(), "my-reminder");
        let identity2 = ReminderIdentity::new(grain_id.clone(), "my-reminder");
        let identity3 = ReminderIdentity::new(grain_id.clone(), "other-reminder");

        assert_eq!(identity1, identity2);
        assert_ne!(identity1, identity3);
    }

    #[test]
    fn test_reminder_identity_hash() {
        use std::collections::HashSet;

        let grain_id = test_grain_id();
        let identity1 = ReminderIdentity::new(grain_id.clone(), "my-reminder");
        let identity2 = ReminderIdentity::new(grain_id.clone(), "my-reminder");

        let mut set = HashSet::new();
        set.insert(identity1);
        assert!(set.contains(&identity2));
    }
}
