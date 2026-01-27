//! Configuration options for the reminder system.

use std::time::Duration;

/// Configuration options for the reminder service.
#[derive(Debug, Clone)]
pub struct ReminderOptions {
    /// Minimum allowed reminder period.
    ///
    /// Reminders cannot be registered with a period shorter than this value.
    /// Default: 1 minute (60 seconds)
    pub min_reminder_period: Duration,

    /// How often to refresh reminder assignments from the reminder table.
    ///
    /// This controls how quickly reminders are redistributed when silos join/leave.
    /// Default: 5 minutes
    pub refresh_reminder_period: Duration,

    /// Timeout for reminder service initialization.
    ///
    /// The reminder service will fail to start if it cannot connect to the
    /// reminder table within this time.
    /// Default: 30 seconds
    pub init_timeout: Duration,

    /// Maximum number of reminders to process per silo.
    ///
    /// If a silo owns more reminders than this, some may be delayed.
    /// Default: 10,000
    pub max_reminders_per_silo: usize,

    /// Delay before firing a newly registered reminder.
    ///
    /// This helps prevent immediate firing of reminders that were just registered.
    /// Default: 5 seconds
    pub min_due_time: Duration,
}

impl Default for ReminderOptions {
    fn default() -> Self {
        Self {
            min_reminder_period: Duration::from_secs(60),
            refresh_reminder_period: Duration::from_secs(300),
            init_timeout: Duration::from_secs(30),
            max_reminders_per_silo: 10_000,
            min_due_time: Duration::from_secs(5),
        }
    }
}

impl ReminderOptions {
    /// Creates a new `ReminderOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the minimum reminder period.
    pub fn with_min_reminder_period(mut self, period: Duration) -> Self {
        self.min_reminder_period = period;
        self
    }

    /// Sets the refresh period for reminder assignments.
    pub fn with_refresh_period(mut self, period: Duration) -> Self {
        self.refresh_reminder_period = period;
        self
    }

    /// Sets the initialization timeout.
    pub fn with_init_timeout(mut self, timeout: Duration) -> Self {
        self.init_timeout = timeout;
        self
    }

    /// Sets the maximum reminders per silo.
    pub fn with_max_reminders_per_silo(mut self, max: usize) -> Self {
        self.max_reminders_per_silo = max;
        self
    }

    /// Sets the minimum due time for newly registered reminders.
    pub fn with_min_due_time(mut self, due_time: Duration) -> Self {
        self.min_due_time = due_time;
        self
    }

    /// Creates options suitable for testing with shorter intervals.
    pub fn for_testing() -> Self {
        Self {
            min_reminder_period: Duration::from_millis(100),
            refresh_reminder_period: Duration::from_secs(1),
            init_timeout: Duration::from_secs(5),
            max_reminders_per_silo: 1_000,
            min_due_time: Duration::from_millis(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = ReminderOptions::default();
        assert_eq!(opts.min_reminder_period, Duration::from_secs(60));
        assert_eq!(opts.refresh_reminder_period, Duration::from_secs(300));
        assert_eq!(opts.init_timeout, Duration::from_secs(30));
        assert_eq!(opts.max_reminders_per_silo, 10_000);
        assert_eq!(opts.min_due_time, Duration::from_secs(5));
    }

    #[test]
    fn test_builder_pattern() {
        let opts = ReminderOptions::new()
            .with_min_reminder_period(Duration::from_secs(30))
            .with_refresh_period(Duration::from_secs(60))
            .with_init_timeout(Duration::from_secs(10))
            .with_max_reminders_per_silo(5_000)
            .with_min_due_time(Duration::from_secs(1));

        assert_eq!(opts.min_reminder_period, Duration::from_secs(30));
        assert_eq!(opts.refresh_reminder_period, Duration::from_secs(60));
        assert_eq!(opts.init_timeout, Duration::from_secs(10));
        assert_eq!(opts.max_reminders_per_silo, 5_000);
        assert_eq!(opts.min_due_time, Duration::from_secs(1));
    }

    #[test]
    fn test_testing_options() {
        let opts = ReminderOptions::for_testing();
        assert_eq!(opts.min_reminder_period, Duration::from_millis(100));
        assert_eq!(opts.refresh_reminder_period, Duration::from_secs(1));
        assert_eq!(opts.init_timeout, Duration::from_secs(5));
        assert_eq!(opts.max_reminders_per_silo, 1_000);
        assert_eq!(opts.min_due_time, Duration::from_millis(10));
    }

    #[test]
    fn test_clone() {
        let opts1 = ReminderOptions::new().with_min_reminder_period(Duration::from_secs(45));
        let opts2 = opts1.clone();
        assert_eq!(opts1.min_reminder_period, opts2.min_reminder_period);
    }

    #[test]
    fn test_debug() {
        let opts = ReminderOptions::default();
        let debug = format!("{:?}", opts);
        assert!(debug.contains("ReminderOptions"));
        assert!(debug.contains("min_reminder_period"));
    }
}
