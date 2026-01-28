//! Snapshot management for event sourcing.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Metadata about a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// The version (event sequence) at which this snapshot was taken.
    pub version: u64,

    /// Timestamp when the snapshot was taken.
    pub timestamp: DateTime<Utc>,

    /// Size of the serialized snapshot in bytes.
    pub size_bytes: usize,

    /// Optional checksum for integrity verification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
}

impl SnapshotMetadata {
    /// Creates new snapshot metadata.
    pub fn new(version: u64) -> Self {
        Self {
            version,
            timestamp: Utc::now(),
            size_bytes: 0,
            checksum: None,
        }
    }

    /// Creates snapshot metadata with size.
    pub fn with_size(version: u64, size_bytes: usize) -> Self {
        Self {
            version,
            timestamp: Utc::now(),
            size_bytes,
            checksum: None,
        }
    }

    /// Adds a checksum.
    pub fn with_checksum(mut self, checksum: impl Into<String>) -> Self {
        self.checksum = Some(checksum.into());
        self
    }
}

/// Configuration for snapshot behavior.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Number of events between automatic snapshots.
    pub snapshot_interval: u64,

    /// Maximum number of snapshots to retain.
    pub max_snapshots: usize,

    /// Whether to take snapshots automatically.
    pub auto_snapshot: bool,

    /// Minimum time between snapshots (prevents too-frequent snapshots).
    pub min_snapshot_interval_secs: u64,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: 100,
            max_snapshots: 10,
            auto_snapshot: true,
            min_snapshot_interval_secs: 60,
        }
    }
}

impl SnapshotConfig {
    /// Creates configuration for testing with frequent snapshots.
    pub fn for_testing() -> Self {
        Self {
            snapshot_interval: 10,
            max_snapshots: 5,
            auto_snapshot: true,
            min_snapshot_interval_secs: 0,
        }
    }

    /// Creates configuration that disables automatic snapshots.
    pub fn manual_only() -> Self {
        Self {
            snapshot_interval: u64::MAX,
            max_snapshots: 10,
            auto_snapshot: false,
            min_snapshot_interval_secs: 0,
        }
    }

    /// Builder method to set snapshot interval.
    pub fn with_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Builder method to set max snapshots.
    pub fn with_max_snapshots(mut self, count: usize) -> Self {
        self.max_snapshots = count;
        self
    }

    /// Builder method to disable auto snapshots.
    pub fn without_auto_snapshot(mut self) -> Self {
        self.auto_snapshot = false;
        self
    }
}

/// Tracks snapshot state for a grain.
#[derive(Debug, Clone)]
pub struct SnapshotState {
    /// Version of the last snapshot.
    pub last_snapshot_version: u64,

    /// Timestamp of the last snapshot.
    pub last_snapshot_time: Option<DateTime<Utc>>,

    /// Number of snapshots taken.
    pub snapshot_count: u64,
}

impl Default for SnapshotState {
    fn default() -> Self {
        Self::new()
    }
}

impl SnapshotState {
    /// Creates new snapshot state.
    pub fn new() -> Self {
        Self {
            last_snapshot_version: 0,
            last_snapshot_time: None,
            snapshot_count: 0,
        }
    }

    /// Records that a snapshot was taken.
    pub fn record_snapshot(&mut self, version: u64) {
        self.last_snapshot_version = version;
        self.last_snapshot_time = Some(Utc::now());
        self.snapshot_count += 1;
    }

    /// Checks if a snapshot is needed based on the config.
    pub fn needs_snapshot(&self, current_version: u64, config: &SnapshotConfig) -> bool {
        if !config.auto_snapshot {
            return false;
        }

        // Check event count since last snapshot
        let events_since = current_version.saturating_sub(self.last_snapshot_version);
        if events_since < config.snapshot_interval {
            return false;
        }

        // Check minimum time between snapshots
        if let Some(last_time) = self.last_snapshot_time {
            let elapsed = Utc::now()
                .signed_duration_since(last_time)
                .num_seconds() as u64;
            if elapsed < config.min_snapshot_interval_secs {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_metadata_new() {
        let meta = SnapshotMetadata::new(10);
        assert_eq!(meta.version, 10);
        assert_eq!(meta.size_bytes, 0);
        assert!(meta.checksum.is_none());
    }

    #[test]
    fn test_snapshot_metadata_with_size() {
        let meta = SnapshotMetadata::with_size(20, 1024);
        assert_eq!(meta.version, 20);
        assert_eq!(meta.size_bytes, 1024);
    }

    #[test]
    fn test_snapshot_metadata_with_checksum() {
        let meta = SnapshotMetadata::new(30).with_checksum("abc123");
        assert_eq!(meta.checksum.as_deref(), Some("abc123"));
    }

    #[test]
    fn test_snapshot_config_default() {
        let config = SnapshotConfig::default();
        assert_eq!(config.snapshot_interval, 100);
        assert_eq!(config.max_snapshots, 10);
        assert!(config.auto_snapshot);
    }

    #[test]
    fn test_snapshot_config_for_testing() {
        let config = SnapshotConfig::for_testing();
        assert_eq!(config.snapshot_interval, 10);
        assert_eq!(config.max_snapshots, 5);
    }

    #[test]
    fn test_snapshot_config_manual_only() {
        let config = SnapshotConfig::manual_only();
        assert!(!config.auto_snapshot);
    }

    #[test]
    fn test_snapshot_config_builder() {
        let config = SnapshotConfig::default()
            .with_interval(50)
            .with_max_snapshots(3)
            .without_auto_snapshot();

        assert_eq!(config.snapshot_interval, 50);
        assert_eq!(config.max_snapshots, 3);
        assert!(!config.auto_snapshot);
    }

    #[test]
    fn test_snapshot_state_new() {
        let state = SnapshotState::new();
        assert_eq!(state.last_snapshot_version, 0);
        assert!(state.last_snapshot_time.is_none());
        assert_eq!(state.snapshot_count, 0);
    }

    #[test]
    fn test_snapshot_state_record_snapshot() {
        let mut state = SnapshotState::new();
        state.record_snapshot(100);

        assert_eq!(state.last_snapshot_version, 100);
        assert!(state.last_snapshot_time.is_some());
        assert_eq!(state.snapshot_count, 1);

        state.record_snapshot(200);
        assert_eq!(state.last_snapshot_version, 200);
        assert_eq!(state.snapshot_count, 2);
    }

    #[test]
    fn test_snapshot_state_needs_snapshot() {
        let mut state = SnapshotState::new();
        let config = SnapshotConfig::for_testing();

        // Initially needs snapshot after interval events
        assert!(!state.needs_snapshot(5, &config));
        assert!(state.needs_snapshot(10, &config));

        // After taking a snapshot
        state.record_snapshot(10);
        assert!(!state.needs_snapshot(15, &config));
        assert!(state.needs_snapshot(20, &config));
    }

    #[test]
    fn test_snapshot_state_no_auto_snapshot() {
        let state = SnapshotState::new();
        let config = SnapshotConfig::manual_only();

        // Never needs snapshot with auto_snapshot disabled
        assert!(!state.needs_snapshot(1000, &config));
    }
}
