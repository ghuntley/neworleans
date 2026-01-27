//! Reminder entry - the persistence model for reminders.

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use orleans_core::GrainId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

/// A reminder entry stored in the reminder table.
///
/// This represents the persistent state of a reminder, including:
/// - The grain it belongs to
/// - When it should first fire
/// - The period between firings
/// - An ETag for optimistic concurrency control
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReminderEntry {
    /// The grain that owns this reminder.
    pub grain_id: GrainId,
    /// The unique name of the reminder (within the grain).
    pub reminder_name: String,
    /// When the reminder should first fire.
    pub start_at: DateTime<Utc>,
    /// The period between reminder firings.
    #[serde(with = "duration_serde")]
    pub period: Duration,
    /// ETag for optimistic concurrency control.
    pub etag: String,
}

impl ReminderEntry {
    /// Creates a new reminder entry.
    pub fn new(
        grain_id: GrainId,
        reminder_name: impl Into<String>,
        start_at: DateTime<Utc>,
        period: Duration,
    ) -> Self {
        Self {
            grain_id,
            reminder_name: reminder_name.into(),
            start_at,
            period,
            etag: String::new(),
        }
    }

    /// Creates a new reminder entry with a due time from now.
    pub fn with_due_time(
        grain_id: GrainId,
        reminder_name: impl Into<String>,
        due_time: Duration,
        period: Duration,
    ) -> Self {
        let start_at = Utc::now() + ChronoDuration::from_std(due_time).unwrap_or_default();
        Self::new(grain_id, reminder_name, start_at, period)
    }

    /// Sets the ETag on this entry.
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = etag.into();
        self
    }

    /// Returns the grain's uniform hash code for consistent hashing.
    ///
    /// This is used to determine which silo should manage this reminder.
    pub fn get_grain_hash_code(&self) -> u32 {
        self.grain_id.get_uniform_hash_code()
    }

    /// Calculates the next tick time for this reminder.
    ///
    /// If the start time is in the future, returns the start time.
    /// Otherwise, calculates the next tick based on the period.
    pub fn get_next_tick_time(&self) -> DateTime<Utc> {
        let now = Utc::now();

        // If we haven't started yet, next tick is at start_at
        if now < self.start_at {
            return self.start_at;
        }

        // If period is zero, this is a one-shot reminder (edge case)
        if self.period.is_zero() {
            return self.start_at;
        }

        // Calculate how many periods have elapsed
        let elapsed = now.signed_duration_since(self.start_at);
        let elapsed_ms = elapsed.num_milliseconds().max(0) as u64;
        let period_ms = self.period.as_millis() as u64;

        // Calculate the number of complete periods
        let periods_passed = elapsed_ms / period_ms;

        // Next tick is at start_at + (periods_passed + 1) * period
        let next_tick_offset_ms = (periods_passed + 1) * period_ms;
        let next_tick_offset = ChronoDuration::milliseconds(next_tick_offset_ms as i64);

        self.start_at + next_tick_offset
    }

    /// Returns true if the reminder should fire now.
    pub fn should_fire_now(&self) -> bool {
        let now = Utc::now();
        now >= self.start_at
    }

    /// Returns the time until the next tick.
    pub fn time_until_next_tick(&self) -> Duration {
        let now = Utc::now();
        let next_tick = self.get_next_tick_time();

        if next_tick <= now {
            return Duration::ZERO;
        }

        let duration = next_tick.signed_duration_since(now);
        duration.to_std().unwrap_or(Duration::ZERO)
    }
}

impl fmt::Display for ReminderEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Reminder[{}/{}] starts at {}, period {:?}",
            self.grain_id, self.reminder_name, self.start_at, self.period
        )
    }
}

impl PartialEq for ReminderEntry {
    fn eq(&self, other: &Self) -> bool {
        self.grain_id == other.grain_id && self.reminder_name == other.reminder_name
    }
}

impl Eq for ReminderEntry {}

impl std::hash::Hash for ReminderEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.grain_id.hash(state);
        self.reminder_name.hash(state);
    }
}

/// Custom serialization for std::time::Duration.
mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    #[derive(Serialize, Deserialize)]
    struct DurationHelper {
        secs: u64,
        nanos: u32,
    }

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let helper = DurationHelper {
            secs: duration.as_secs(),
            nanos: duration.subsec_nanos(),
        };
        helper.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let helper = DurationHelper::deserialize(deserializer)?;
        Ok(Duration::new(helper.secs, helper.nanos))
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
    fn test_reminder_entry_new() {
        let grain_id = test_grain_id();
        let start_at = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id.clone(), "my-reminder", start_at, period);

        assert_eq!(entry.grain_id, grain_id);
        assert_eq!(entry.reminder_name, "my-reminder");
        assert_eq!(entry.start_at, start_at);
        assert_eq!(entry.period, period);
        assert!(entry.etag.is_empty());
    }

    #[test]
    fn test_reminder_entry_with_etag() {
        let grain_id = test_grain_id();
        let start_at = Utc::now();
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id, "my-reminder", start_at, period)
            .with_etag("etag-123");

        assert_eq!(entry.etag, "etag-123");
    }

    #[test]
    fn test_reminder_entry_with_due_time() {
        let grain_id = test_grain_id();
        let due_time = Duration::from_secs(300); // 5 minutes
        let period = Duration::from_secs(60);

        let before = Utc::now();
        let entry = ReminderEntry::with_due_time(grain_id, "my-reminder", due_time, period);
        let after = Utc::now();

        // start_at should be approximately now + 5 minutes
        let expected_min = before + ChronoDuration::from_std(due_time).unwrap();
        let expected_max = after + ChronoDuration::from_std(due_time).unwrap();

        assert!(entry.start_at >= expected_min);
        assert!(entry.start_at <= expected_max);
    }

    #[test]
    fn test_get_next_tick_time_future_start() {
        let grain_id = test_grain_id();
        let future_start = Utc::now() + ChronoDuration::hours(1);
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id, "my-reminder", future_start, period);

        // Next tick should be at start_at since it's in the future
        assert_eq!(entry.get_next_tick_time(), future_start);
    }

    #[test]
    fn test_get_next_tick_time_past_start() {
        let grain_id = test_grain_id();
        // Start was 5 minutes and 30 seconds ago
        let past_start = Utc::now() - ChronoDuration::minutes(5) - ChronoDuration::seconds(30);
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id, "my-reminder", past_start, period);
        let next_tick = entry.get_next_tick_time();

        // Next tick should be within the next 60 seconds
        let now = Utc::now();
        assert!(next_tick > now);
        assert!(next_tick <= now + ChronoDuration::seconds(60));
    }

    #[test]
    fn test_should_fire_now() {
        let grain_id = test_grain_id();

        // Future start
        let future_entry = ReminderEntry::new(
            grain_id.clone(),
            "future",
            Utc::now() + ChronoDuration::hours(1),
            Duration::from_secs(60),
        );
        assert!(!future_entry.should_fire_now());

        // Past start
        let past_entry = ReminderEntry::new(
            grain_id,
            "past",
            Utc::now() - ChronoDuration::hours(1),
            Duration::from_secs(60),
        );
        assert!(past_entry.should_fire_now());
    }

    #[test]
    fn test_time_until_next_tick() {
        let grain_id = test_grain_id();
        let future_start = Utc::now() + ChronoDuration::seconds(30);
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id, "my-reminder", future_start, period);
        let time_until = entry.time_until_next_tick();

        // Should be approximately 30 seconds
        assert!(time_until.as_secs() >= 29);
        assert!(time_until.as_secs() <= 31);
    }

    #[test]
    fn test_time_until_next_tick_ready() {
        let grain_id = test_grain_id();
        let past_start = Utc::now() - ChronoDuration::hours(1);
        let period = Duration::from_secs(60);

        let entry = ReminderEntry::new(grain_id, "my-reminder", past_start, period);
        let time_until = entry.time_until_next_tick();

        // Should be within 60 seconds
        assert!(time_until <= Duration::from_secs(60));
    }

    #[test]
    fn test_get_grain_hash_code() {
        let grain_id = test_grain_id();
        let expected_hash = grain_id.get_uniform_hash_code();

        let entry = ReminderEntry::new(
            grain_id,
            "my-reminder",
            Utc::now(),
            Duration::from_secs(60),
        );

        assert_eq!(entry.get_grain_hash_code(), expected_hash);
    }

    #[test]
    fn test_reminder_entry_equality() {
        let grain_id = test_grain_id();
        let start1 = Utc::now();
        let start2 = start1 + ChronoDuration::hours(1);

        let entry1 = ReminderEntry::new(grain_id.clone(), "my-reminder", start1, Duration::from_secs(60));
        let entry2 = ReminderEntry::new(grain_id.clone(), "my-reminder", start2, Duration::from_secs(120));
        let entry3 = ReminderEntry::new(grain_id, "other-reminder", start1, Duration::from_secs(60));

        // Equality is based on grain_id + reminder_name only
        assert_eq!(entry1, entry2);
        assert_ne!(entry1, entry3);
    }

    #[test]
    fn test_reminder_entry_display() {
        let grain_id = test_grain_id();
        let entry = ReminderEntry::new(
            grain_id,
            "daily-check",
            Utc::now(),
            Duration::from_secs(86400),
        );

        let display = format!("{}", entry);
        assert!(display.contains("TestGrain"));
        assert!(display.contains("daily-check"));
        assert!(display.contains("period"));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let grain_id = test_grain_id();
        let entry = ReminderEntry::new(
            grain_id,
            "my-reminder",
            Utc::now(),
            Duration::from_secs(60),
        )
        .with_etag("test-etag");

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: ReminderEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(entry.grain_id, deserialized.grain_id);
        assert_eq!(entry.reminder_name, deserialized.reminder_name);
        assert_eq!(entry.period, deserialized.period);
        assert_eq!(entry.etag, deserialized.etag);
    }
}
