//! Timer configuration options.

use std::time::Duration;

/// Configuration options for grain timers.
#[derive(Debug, Clone)]
pub struct TimerOptions {
    /// Minimum allowed timer period.
    ///
    /// Timers with a period below this value will return an error.
    /// Default: 10 milliseconds
    pub min_timer_period: Duration,

    /// Maximum number of timer callbacks that can be executed per turn.
    ///
    /// This prevents timer floods from starving other messages.
    /// Default: 100
    pub max_timer_callbacks_per_turn: usize,

    /// Whether to allow zero-duration (immediate) timers.
    ///
    /// Default: true
    pub allow_immediate_timers: bool,
}

impl Default for TimerOptions {
    fn default() -> Self {
        Self {
            min_timer_period: Duration::from_millis(10),
            max_timer_callbacks_per_turn: 100,
            allow_immediate_timers: true,
        }
    }
}

impl TimerOptions {
    /// Create new timer options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum timer period.
    pub fn with_min_timer_period(mut self, period: Duration) -> Self {
        self.min_timer_period = period;
        self
    }

    /// Set the maximum timer callbacks per turn.
    pub fn with_max_timer_callbacks_per_turn(mut self, max: usize) -> Self {
        self.max_timer_callbacks_per_turn = max;
        self
    }

    /// Set whether to allow immediate timers.
    pub fn with_allow_immediate_timers(mut self, allow: bool) -> Self {
        self.allow_immediate_timers = allow;
        self
    }

    /// Validate a timer period against the minimum.
    ///
    /// Returns `true` if the period is valid (>= minimum or zero if allowed).
    pub fn is_valid_period(&self, period: Duration) -> bool {
        if period.is_zero() {
            // Zero period means one-shot timer (no repeat)
            true
        } else {
            period >= self.min_timer_period
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = TimerOptions::default();
        assert_eq!(opts.min_timer_period, Duration::from_millis(10));
        assert_eq!(opts.max_timer_callbacks_per_turn, 100);
        assert!(opts.allow_immediate_timers);
    }

    #[test]
    fn test_options_builder() {
        let opts = TimerOptions::new()
            .with_min_timer_period(Duration::from_millis(50))
            .with_max_timer_callbacks_per_turn(50)
            .with_allow_immediate_timers(false);

        assert_eq!(opts.min_timer_period, Duration::from_millis(50));
        assert_eq!(opts.max_timer_callbacks_per_turn, 50);
        assert!(!opts.allow_immediate_timers);
    }

    #[test]
    fn test_is_valid_period_zero() {
        let opts = TimerOptions::new();
        // Zero period (one-shot) is always valid
        assert!(opts.is_valid_period(Duration::ZERO));
    }

    #[test]
    fn test_is_valid_period_at_minimum() {
        let opts = TimerOptions::new().with_min_timer_period(Duration::from_millis(10));
        assert!(opts.is_valid_period(Duration::from_millis(10)));
    }

    #[test]
    fn test_is_valid_period_above_minimum() {
        let opts = TimerOptions::new().with_min_timer_period(Duration::from_millis(10));
        assert!(opts.is_valid_period(Duration::from_millis(100)));
    }

    #[test]
    fn test_is_valid_period_below_minimum() {
        let opts = TimerOptions::new().with_min_timer_period(Duration::from_millis(10));
        assert!(!opts.is_valid_period(Duration::from_millis(5)));
    }
}
