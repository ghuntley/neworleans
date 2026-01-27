//! Causal clock for transaction timestamp ordering.
//!
//! Implements a monotonically increasing clock that ensures causal ordering
//! of transactions even across clock skew between nodes.

use chrono::{DateTime, Utc};
use std::sync::atomic::{AtomicI64, Ordering};
use tracing::trace;

/// A causal clock that generates monotonically increasing timestamps.
///
/// The clock ensures that:
/// 1. Each timestamp is unique (at least +1 from previous)
/// 2. Timestamps are causally ordered (later events have higher timestamps)
/// 3. Timestamps approximately track wall-clock time when possible
///
/// This is used for transaction ordering in the 2PC protocol.
#[derive(Debug)]
pub struct CausalClock {
    /// The previous timestamp in ticks (100ns units since epoch).
    previous: AtomicI64,
}

impl CausalClock {
    /// Creates a new causal clock.
    pub fn new() -> Self {
        Self {
            previous: AtomicI64::new(0),
        }
    }

    /// Gets the current UTC time as a monotonically increasing timestamp.
    ///
    /// Each call returns a timestamp that is at least 1 tick greater than
    /// the previous call, ensuring unique ordering.
    pub fn utc_now(&self) -> DateTime<Utc> {
        loop {
            let prev = self.previous.load(Ordering::Acquire);
            let now = Self::wall_clock_ticks();
            let next = std::cmp::max(prev + 1, now);

            if self
                .previous
                .compare_exchange(prev, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                let ts = Self::ticks_to_datetime(next);
                trace!(
                    prev_ticks = prev,
                    next_ticks = next,
                    timestamp = %ts,
                    "CausalClock::utc_now"
                );
                return ts;
            }
        }
    }

    /// Merges an external timestamp and returns a new timestamp that is
    /// greater than both the previous local timestamp and the external one.
    ///
    /// This is used when receiving messages with timestamps to ensure
    /// causal ordering is maintained.
    pub fn merge_utc_now(&self, external: DateTime<Utc>) -> DateTime<Utc> {
        let external_ticks = Self::datetime_to_ticks(external);
        loop {
            let prev = self.previous.load(Ordering::Acquire);
            let now = Self::wall_clock_ticks();
            let next = std::cmp::max(std::cmp::max(prev + 1, external_ticks + 1), now);

            if self
                .previous
                .compare_exchange(prev, next, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                let ts = Self::ticks_to_datetime(next);
                trace!(
                    prev_ticks = prev,
                    external_ticks = external_ticks,
                    next_ticks = next,
                    timestamp = %ts,
                    "CausalClock::merge_utc_now"
                );
                return ts;
            }
        }
    }

    /// Gets the last generated timestamp without advancing the clock.
    pub fn last_timestamp(&self) -> Option<DateTime<Utc>> {
        let prev = self.previous.load(Ordering::Acquire);
        if prev == 0 {
            None
        } else {
            Some(Self::ticks_to_datetime(prev))
        }
    }

    /// Gets the current wall clock time in ticks (100ns units).
    fn wall_clock_ticks() -> i64 {
        // .NET ticks are 100ns intervals since 0001-01-01
        // We use Unix epoch and convert for simplicity
        let now = Utc::now();
        Self::datetime_to_ticks(now)
    }

    /// Converts a DateTime to ticks.
    fn datetime_to_ticks(dt: DateTime<Utc>) -> i64 {
        // Convert to nanoseconds since Unix epoch, then to 100ns ticks
        let nanos = dt.timestamp_nanos_opt().unwrap_or(0);
        nanos / 100
    }

    /// Converts ticks back to DateTime.
    fn ticks_to_datetime(ticks: i64) -> DateTime<Utc> {
        // Convert 100ns ticks to nanoseconds
        let nanos = ticks * 100;
        DateTime::from_timestamp_nanos(nanos)
    }
}

impl Default for CausalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CausalClock {
    fn clone(&self) -> Self {
        Self {
            previous: AtomicI64::new(self.previous.load(Ordering::Acquire)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    fn test_causal_clock_new() {
        let clock = CausalClock::new();
        assert_eq!(clock.last_timestamp(), None);
    }

    #[test]
    fn test_causal_clock_utc_now_returns_timestamp() {
        let clock = CausalClock::new();
        let ts = clock.utc_now();

        // Should be within reasonable range of current time
        let now = Utc::now();
        let diff = (now - ts).num_seconds().abs();
        assert!(diff < 10, "Timestamp should be close to current time");
    }

    #[test]
    fn test_causal_clock_monotonic() {
        let clock = CausalClock::new();

        let mut prev = clock.utc_now();
        for _ in 0..100 {
            let next = clock.utc_now();
            assert!(next > prev, "Clock should be monotonically increasing");
            prev = next;
        }
    }

    #[test]
    fn test_causal_clock_unique_timestamps() {
        let clock = CausalClock::new();
        let mut timestamps = HashSet::new();

        for _ in 0..1000 {
            let ts = clock.utc_now();
            let ticks = CausalClock::datetime_to_ticks(ts);
            assert!(
                timestamps.insert(ticks),
                "All timestamps should be unique"
            );
        }
    }

    #[test]
    fn test_causal_clock_merge_advances_clock() {
        let clock = CausalClock::new();

        // Get initial timestamp
        let t1 = clock.utc_now();

        // Create a future timestamp
        let future = t1 + chrono::Duration::hours(1);

        // Merge should return a timestamp > future
        let t2 = clock.merge_utc_now(future);
        assert!(t2 > future, "Merged timestamp should be after external");

        // Subsequent calls should still be monotonic
        let t3 = clock.utc_now();
        assert!(t3 > t2, "Clock should continue being monotonic");
    }

    #[test]
    fn test_causal_clock_merge_with_past() {
        let clock = CausalClock::new();

        // Advance clock
        let t1 = clock.utc_now();

        // Merge with a past timestamp
        let past = t1 - chrono::Duration::hours(1);
        let t2 = clock.merge_utc_now(past);

        // Should still be monotonic from t1
        assert!(t2 > t1, "Merge with past should still advance clock");
    }

    #[test]
    fn test_causal_clock_last_timestamp() {
        let clock = CausalClock::new();

        // No timestamp yet
        assert_eq!(clock.last_timestamp(), None);

        // Generate timestamp
        let t1 = clock.utc_now();
        let last = clock.last_timestamp();
        assert!(last.is_some());
        assert_eq!(CausalClock::datetime_to_ticks(last.unwrap()),
                   CausalClock::datetime_to_ticks(t1));
    }

    #[test]
    fn test_causal_clock_clone() {
        let clock = CausalClock::new();
        let _ = clock.utc_now();

        let cloned = clock.clone();
        let last1 = clock.last_timestamp();
        let last2 = cloned.last_timestamp();

        assert_eq!(
            CausalClock::datetime_to_ticks(last1.unwrap()),
            CausalClock::datetime_to_ticks(last2.unwrap())
        );
    }

    #[test]
    fn test_causal_clock_concurrent_access() {
        let clock = std::sync::Arc::new(CausalClock::new());
        let mut handles = vec![];

        // Spawn multiple threads
        for _ in 0..4 {
            let clock = clock.clone();
            handles.push(thread::spawn(move || {
                let mut timestamps = Vec::new();
                for _ in 0..100 {
                    timestamps.push(CausalClock::datetime_to_ticks(clock.utc_now()));
                }
                timestamps
            }));
        }

        // Collect all timestamps
        let mut all_timestamps: Vec<i64> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // All should be unique
        let count = all_timestamps.len();
        all_timestamps.sort();
        all_timestamps.dedup();
        assert_eq!(all_timestamps.len(), count, "All timestamps must be unique");
    }

    #[test]
    fn test_datetime_roundtrip() {
        let now = Utc::now();
        let ticks = CausalClock::datetime_to_ticks(now);
        let back = CausalClock::ticks_to_datetime(ticks);

        // Should be within 1 microsecond (100ns precision)
        let diff = (now - back).num_microseconds().unwrap_or(0).abs();
        assert!(diff <= 1, "Roundtrip should preserve time within 1us");
    }
}
