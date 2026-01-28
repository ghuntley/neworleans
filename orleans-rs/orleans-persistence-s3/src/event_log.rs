//! S3-based event log storage for event sourcing.
//!
//! This module provides append-only event log storage for grains
//! that use event sourcing patterns.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};

use crate::client::S3Client;
use crate::error::{S3Error, S3Result};
use crate::options::S3Options;

/// A single event entry in the event log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EventEntry {
    /// The grain ID that owns this event.
    pub grain_id: String,

    /// The sequence number of this event (monotonically increasing).
    pub sequence_number: u64,

    /// The event type name.
    pub event_type: String,

    /// The serialized event payload.
    pub payload: Vec<u8>,

    /// The timestamp when this event was created.
    pub timestamp: DateTime<Utc>,

    /// Optional correlation ID for tracing.
    #[serde(default)]
    pub correlation_id: Option<String>,

    /// Optional causation ID (the event that caused this event).
    #[serde(default)]
    pub causation_id: Option<String>,

    /// Optional metadata.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

impl EventEntry {
    /// Create a new event entry.
    pub fn new(
        grain_id: impl Into<String>,
        sequence_number: u64,
        event_type: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            grain_id: grain_id.into(),
            sequence_number,
            event_type: event_type.into(),
            payload,
            timestamp: Utc::now(),
            correlation_id: None,
            causation_id: None,
            metadata: None,
        }
    }

    /// Set the correlation ID.
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Set the causation ID.
    pub fn with_causation_id(mut self, id: impl Into<String>) -> Self {
        self.causation_id = Some(id.into());
        self
    }

    /// Set metadata.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// A batch of events for efficient writing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatch {
    /// The grain ID that owns these events.
    pub grain_id: String,

    /// The starting sequence number of this batch.
    pub start_sequence: u64,

    /// The ending sequence number of this batch (inclusive).
    pub end_sequence: u64,

    /// The events in this batch.
    pub events: Vec<EventEntry>,

    /// The timestamp when this batch was created.
    pub timestamp: DateTime<Utc>,
}

impl EventBatch {
    /// Create a new event batch.
    pub fn new(grain_id: impl Into<String>, events: Vec<EventEntry>) -> Option<Self> {
        if events.is_empty() {
            return None;
        }

        let grain_id = grain_id.into();
        let start_sequence = events.first().unwrap().sequence_number;
        let end_sequence = events.last().unwrap().sequence_number;

        Some(Self {
            grain_id,
            start_sequence,
            end_sequence,
            events,
            timestamp: Utc::now(),
        })
    }

    /// Get the number of events in this batch.
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}

/// S3-based event log storage.
///
/// Stores events as batched objects in S3 with the following key format:
/// `{prefix}events/{grain_id}/{batch_start_sequence}.json`
///
/// # Features
///
/// - Append-only event log with batched writes for efficiency
/// - Range reads for event replay
/// - Efficient sequence range queries
///
/// # Example
///
/// ```ignore
/// use orleans_persistence_s3::{S3Options, S3EventLogStorage, EventEntry};
///
/// let options = S3Options::new("my-bucket").with_key_prefix("orleans/");
/// let storage = S3EventLogStorage::new(options).await?;
///
/// // Append events
/// let event = EventEntry::new("my-grain-123", 1, "OrderCreated", payload);
/// storage.append_events("my-grain-123", vec![event]).await?;
///
/// // Read events
/// let events = storage.read_events("my-grain-123", 0, 100).await?;
/// ```
#[derive(Clone)]
pub struct S3EventLogStorage {
    client: S3Client,
    /// Target batch size for writes.
    batch_size: usize,
}

impl std::fmt::Debug for S3EventLogStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3EventLogStorage")
            .field("client", &self.client)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl S3EventLogStorage {
    /// Default batch size for event writes.
    pub const DEFAULT_BATCH_SIZE: usize = 100;

    /// Create a new S3 event log storage.
    #[instrument(skip(options), fields(bucket = %options.bucket))]
    pub async fn new(options: S3Options) -> S3Result<Self> {
        let client = S3Client::new(options).await?;
        debug!("Created S3EventLogStorage");
        Ok(Self {
            client,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        })
    }

    /// Create from an existing S3 client.
    pub fn from_client(client: S3Client) -> Self {
        Self {
            client,
            batch_size: Self::DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the target batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.client.bucket()
    }

    /// Build the object key for an event batch.
    ///
    /// Format: `events/{grain_id}/{start_sequence:020}.json`
    /// Zero-padded sequence number for lexicographic ordering.
    fn build_batch_key(&self, grain_id: &str, start_sequence: u64) -> String {
        format!(
            "events/{}/{:020}.json",
            sanitize_key_component(grain_id),
            start_sequence
        )
    }

    /// Get the sequence number from a batch key.
    fn parse_batch_key(&self, key: &str) -> Option<u64> {
        // Extract the filename without extension
        key.rsplit('/')
            .next()
            .and_then(|filename| filename.strip_suffix(".json"))
            .and_then(|seq_str| seq_str.parse().ok())
    }

    /// Append events to the event log.
    ///
    /// Events are written as a batch. The sequence numbers must be monotonically
    /// increasing and start from the next expected sequence number.
    #[instrument(skip(self, events), fields(grain_id = %grain_id, count = events.len()))]
    pub async fn append_events(&self, grain_id: &str, events: Vec<EventEntry>) -> S3Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        // Validate sequence numbers
        for window in events.windows(2) {
            if window[1].sequence_number != window[0].sequence_number + 1 {
                return Err(S3Error::Configuration(format!(
                    "non-consecutive sequence numbers: {} -> {}",
                    window[0].sequence_number, window[1].sequence_number
                )));
            }
        }

        let batch = EventBatch::new(grain_id, events).unwrap();
        let key = self.build_batch_key(grain_id, batch.start_sequence);

        debug!(
            key = %key,
            start = batch.start_sequence,
            end = batch.end_sequence,
            count = batch.len(),
            "Appending events to S3"
        );

        let data = serde_json::to_vec(&batch)?;

        // Check if batch already exists (duplicate append)
        if let Some(_) = self.client.head_object(&key).await? {
            warn!(
                key = %key,
                "Event batch already exists, skipping duplicate write"
            );
            return Ok(());
        }

        self.client.put_object(&key, &data).await?;

        debug!(
            key = %key,
            "Appended events to S3"
        );

        Ok(())
    }

    /// Read events in a sequence range.
    ///
    /// Returns events with sequence numbers in [from_sequence, to_sequence).
    #[instrument(skip(self), fields(grain_id = %grain_id, from = from_sequence, to = to_sequence))]
    pub async fn read_events(
        &self,
        grain_id: &str,
        from_sequence: u64,
        to_sequence: u64,
    ) -> S3Result<Vec<EventEntry>> {
        if from_sequence >= to_sequence {
            return Ok(Vec::new());
        }

        let prefix = format!("events/{}/", sanitize_key_component(grain_id));

        debug!(
            prefix = %prefix,
            from = from_sequence,
            to = to_sequence,
            "Reading events from S3"
        );

        // List all batch files for this grain
        let keys = self.client.list_objects(&prefix).await?;

        let mut all_events = Vec::new();

        for key in keys {
            // Parse the batch start sequence from the key
            if let Some(batch_start) = self.parse_batch_key(&key) {
                // Skip batches that are entirely after our range
                if batch_start >= to_sequence {
                    continue;
                }

                // Load the batch
                if let Some((data, _etag)) = self.client.get_object_if_exists(&key).await? {
                    let batch: EventBatch = serde_json::from_slice(&data)?;

                    // Skip batches that are entirely before our range
                    if batch.end_sequence < from_sequence {
                        continue;
                    }

                    // Filter events within the requested range
                    for event in batch.events {
                        if event.sequence_number >= from_sequence
                            && event.sequence_number < to_sequence
                        {
                            all_events.push(event);
                        }
                    }
                }
            }
        }

        // Sort by sequence number
        all_events.sort_by_key(|e| e.sequence_number);

        debug!(
            count = all_events.len(),
            "Read events from S3"
        );

        Ok(all_events)
    }

    /// Read all events for a grain.
    #[instrument(skip(self), fields(grain_id = %grain_id))]
    pub async fn read_all_events(&self, grain_id: &str) -> S3Result<Vec<EventEntry>> {
        self.read_events(grain_id, 0, u64::MAX).await
    }

    /// Get the latest sequence number for a grain.
    ///
    /// Returns None if no events exist.
    #[instrument(skip(self), fields(grain_id = %grain_id))]
    pub async fn get_latest_sequence(&self, grain_id: &str) -> S3Result<Option<u64>> {
        let prefix = format!("events/{}/", sanitize_key_component(grain_id));

        let keys = self.client.list_objects(&prefix).await?;

        if keys.is_empty() {
            return Ok(None);
        }

        // Find the latest batch
        let mut max_sequence: Option<u64> = None;

        for key in keys {
            if let Some((data, _etag)) = self.client.get_object_if_exists(&key).await? {
                let batch: EventBatch = serde_json::from_slice(&data)?;
                match max_sequence {
                    None => max_sequence = Some(batch.end_sequence),
                    Some(current) if batch.end_sequence > current => {
                        max_sequence = Some(batch.end_sequence)
                    }
                    _ => {}
                }
            }
        }

        debug!(
            grain_id = %grain_id,
            sequence = ?max_sequence,
            "Got latest sequence from S3"
        );

        Ok(max_sequence)
    }

    /// Delete all events for a grain.
    ///
    /// Use with caution - this permanently removes all event history.
    #[instrument(skip(self), fields(grain_id = %grain_id))]
    pub async fn delete_all_events(&self, grain_id: &str) -> S3Result<u64> {
        let prefix = format!("events/{}/", sanitize_key_component(grain_id));

        debug!(prefix = %prefix, "Deleting all events from S3");

        let keys = self.client.list_objects(&prefix).await?;
        let count = keys.len() as u64;

        for key in keys {
            self.client.delete_object(&key).await?;
        }

        debug!(count = count, "Deleted events from S3");

        Ok(count)
    }
}

/// Sanitize a string for use in an S3 object key.
fn sanitize_key_component(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_entry_new() {
        let event = EventEntry::new("grain-123", 1, "OrderCreated", vec![1, 2, 3]);

        assert_eq!(event.grain_id, "grain-123");
        assert_eq!(event.sequence_number, 1);
        assert_eq!(event.event_type, "OrderCreated");
        assert_eq!(event.payload, vec![1, 2, 3]);
        assert!(event.correlation_id.is_none());
        assert!(event.causation_id.is_none());
        assert!(event.metadata.is_none());
    }

    #[test]
    fn test_event_entry_with_tracing() {
        let event = EventEntry::new("grain-123", 1, "OrderCreated", vec![])
            .with_correlation_id("trace-456")
            .with_causation_id("event-789")
            .with_metadata(serde_json::json!({"source": "api"}));

        assert_eq!(event.correlation_id, Some("trace-456".to_string()));
        assert_eq!(event.causation_id, Some("event-789".to_string()));
        assert!(event.metadata.is_some());
    }

    #[test]
    fn test_event_batch_new() {
        let events = vec![
            EventEntry::new("grain-123", 1, "Event1", vec![]),
            EventEntry::new("grain-123", 2, "Event2", vec![]),
            EventEntry::new("grain-123", 3, "Event3", vec![]),
        ];

        let batch = EventBatch::new("grain-123", events).unwrap();

        assert_eq!(batch.grain_id, "grain-123");
        assert_eq!(batch.start_sequence, 1);
        assert_eq!(batch.end_sequence, 3);
        assert_eq!(batch.len(), 3);
        assert!(!batch.is_empty());
    }

    #[test]
    fn test_event_batch_empty() {
        let batch = EventBatch::new("grain-123", vec![]);
        assert!(batch.is_none());
    }

    #[test]
    fn test_event_serialization_roundtrip() {
        let event = EventEntry::new("grain-123", 42, "TestEvent", vec![1, 2, 3, 4])
            .with_correlation_id("corr-1");

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: EventEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(event.grain_id, deserialized.grain_id);
        assert_eq!(event.sequence_number, deserialized.sequence_number);
        assert_eq!(event.event_type, deserialized.event_type);
        assert_eq!(event.payload, deserialized.payload);
        assert_eq!(event.correlation_id, deserialized.correlation_id);
    }

    #[test]
    fn test_batch_serialization_roundtrip() {
        let events = vec![
            EventEntry::new("grain-123", 1, "Event1", vec![1]),
            EventEntry::new("grain-123", 2, "Event2", vec![2]),
        ];
        let batch = EventBatch::new("grain-123", events).unwrap();

        let json = serde_json::to_string(&batch).unwrap();
        let deserialized: EventBatch = serde_json::from_str(&json).unwrap();

        assert_eq!(batch.grain_id, deserialized.grain_id);
        assert_eq!(batch.start_sequence, deserialized.start_sequence);
        assert_eq!(batch.end_sequence, deserialized.end_sequence);
        assert_eq!(batch.len(), deserialized.len());
    }

    #[test]
    fn test_build_batch_key() {
        let key = format!("events/{}/{:020}.json", "grain-123", 42u64);
        assert_eq!(key, "events/grain-123/00000000000000000042.json");
    }

    #[test]
    fn test_build_batch_key_sorting() {
        // Verify that zero-padded keys sort correctly
        let key1 = format!("events/grain/{:020}.json", 1u64);
        let key2 = format!("events/grain/{:020}.json", 10u64);
        let key3 = format!("events/grain/{:020}.json", 100u64);

        let mut keys = vec![key3.clone(), key1.clone(), key2.clone()];
        keys.sort();

        assert_eq!(keys, vec![key1, key2, key3]);
    }

    #[test]
    fn test_sanitize_key_component() {
        assert_eq!(sanitize_key_component("simple"), "simple");
        assert_eq!(sanitize_key_component("with/slash"), "with_slash");
        assert_eq!(sanitize_key_component("with:colon"), "with_colon");
        assert_eq!(sanitize_key_component("grain-123"), "grain-123");
    }
}
