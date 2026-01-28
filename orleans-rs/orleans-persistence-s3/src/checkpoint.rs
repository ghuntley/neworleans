//! S3-based stream checkpoint storage.
//!
//! This module provides checkpoint persistence for stream consumers,
//! enabling reliable stream processing with exactly-once semantics.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::client::S3Client;
use crate::error::S3Result;
use crate::options::S3Options;

/// A checkpoint representing a consumer's position in a stream.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamCheckpoint {
    /// The stream namespace.
    pub namespace: String,

    /// The stream key.
    pub stream_key: String,

    /// The consumer ID.
    pub consumer_id: String,

    /// The sequence number of the last processed message.
    pub sequence_number: u64,

    /// The event index within the sequence (for batched messages).
    pub event_index: u32,

    /// The timestamp when this checkpoint was created.
    pub timestamp: DateTime<Utc>,

    /// Optional metadata associated with the checkpoint.
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

impl StreamCheckpoint {
    /// Create a new checkpoint.
    pub fn new(
        namespace: impl Into<String>,
        stream_key: impl Into<String>,
        consumer_id: impl Into<String>,
        sequence_number: u64,
        event_index: u32,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            stream_key: stream_key.into(),
            consumer_id: consumer_id.into(),
            sequence_number,
            event_index,
            timestamp: Utc::now(),
            metadata: None,
        }
    }

    /// Create a new checkpoint with metadata.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

/// S3-based stream checkpoint storage.
///
/// Stores checkpoints as JSON objects in S3 with the following key format:
/// `{prefix}checkpoints/{namespace}/{stream_key}/{consumer_id}.json`
///
/// # Features
///
/// - Atomic checkpoint updates via S3 versioning
/// - ETag-based optimistic concurrency control
/// - Automatic retry with exponential backoff
///
/// # Example
///
/// ```ignore
/// use orleans_persistence_s3::{S3Options, S3StreamCheckpointStorage, StreamCheckpoint};
///
/// let options = S3Options::new("my-bucket").with_key_prefix("orleans/");
/// let storage = S3StreamCheckpointStorage::new(options).await?;
///
/// // Save a checkpoint
/// let checkpoint = StreamCheckpoint::new("orders", "customer-123", "processor-1", 42, 0);
/// storage.save_checkpoint(&checkpoint).await?;
///
/// // Load a checkpoint
/// let loaded = storage.load_checkpoint("orders", "customer-123", "processor-1").await?;
/// ```
#[derive(Clone)]
pub struct S3StreamCheckpointStorage {
    client: S3Client,
}

impl std::fmt::Debug for S3StreamCheckpointStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3StreamCheckpointStorage")
            .field("client", &self.client)
            .finish()
    }
}

impl S3StreamCheckpointStorage {
    /// Create a new S3 stream checkpoint storage.
    #[instrument(skip(options), fields(bucket = %options.bucket))]
    pub async fn new(options: S3Options) -> S3Result<Self> {
        let client = S3Client::new(options).await?;
        debug!("Created S3StreamCheckpointStorage");
        Ok(Self { client })
    }

    /// Create from an existing S3 client.
    pub fn from_client(client: S3Client) -> Self {
        Self { client }
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.client.bucket()
    }

    /// Build the object key for a checkpoint.
    ///
    /// Format: `checkpoints/{namespace}/{stream_key}/{consumer_id}.json`
    fn build_checkpoint_key(&self, namespace: &str, stream_key: &str, consumer_id: &str) -> String {
        format!(
            "checkpoints/{}/{}/{}.json",
            sanitize_key_component(namespace),
            sanitize_key_component(stream_key),
            sanitize_key_component(consumer_id)
        )
    }

    /// Save a checkpoint to S3.
    ///
    /// This overwrites any existing checkpoint for the same stream/consumer combination.
    #[instrument(skip(self, checkpoint), fields(
        namespace = %checkpoint.namespace,
        stream_key = %checkpoint.stream_key,
        consumer_id = %checkpoint.consumer_id,
        sequence_number = checkpoint.sequence_number
    ))]
    pub async fn save_checkpoint(&self, checkpoint: &StreamCheckpoint) -> S3Result<String> {
        let key = self.build_checkpoint_key(
            &checkpoint.namespace,
            &checkpoint.stream_key,
            &checkpoint.consumer_id,
        );

        let data = serde_json::to_vec(checkpoint)?;

        debug!(
            key = %key,
            sequence_number = checkpoint.sequence_number,
            event_index = checkpoint.event_index,
            "Saving checkpoint to S3"
        );

        let etag = self.client.put_object(&key, &data).await?;

        debug!(key = %key, etag = %etag, "Saved checkpoint to S3");

        Ok(etag)
    }

    /// Save a checkpoint with optimistic concurrency control.
    ///
    /// Only saves if the expected ETag matches, preventing concurrent modifications.
    #[instrument(skip(self, checkpoint, expected_etag), fields(
        namespace = %checkpoint.namespace,
        stream_key = %checkpoint.stream_key,
        consumer_id = %checkpoint.consumer_id,
        sequence_number = checkpoint.sequence_number,
        expected_etag = %expected_etag
    ))]
    pub async fn save_checkpoint_if_match(
        &self,
        checkpoint: &StreamCheckpoint,
        expected_etag: &str,
    ) -> S3Result<String> {
        let key = self.build_checkpoint_key(
            &checkpoint.namespace,
            &checkpoint.stream_key,
            &checkpoint.consumer_id,
        );

        let data = serde_json::to_vec(checkpoint)?;

        debug!(
            key = %key,
            expected_etag = %expected_etag,
            "Saving checkpoint with ETag check"
        );

        let etag = self
            .client
            .put_object_if_match(&key, &data, expected_etag)
            .await?;

        debug!(key = %key, etag = %etag, "Saved checkpoint to S3");

        Ok(etag)
    }

    /// Load a checkpoint from S3.
    ///
    /// Returns None if no checkpoint exists.
    #[instrument(skip(self), fields(namespace = %namespace, stream_key = %stream_key, consumer_id = %consumer_id))]
    pub async fn load_checkpoint(
        &self,
        namespace: &str,
        stream_key: &str,
        consumer_id: &str,
    ) -> S3Result<Option<(StreamCheckpoint, String)>> {
        let key = self.build_checkpoint_key(namespace, stream_key, consumer_id);

        debug!(key = %key, "Loading checkpoint from S3");

        match self.client.get_object_if_exists(&key).await? {
            Some((data, etag)) => {
                let checkpoint: StreamCheckpoint = serde_json::from_slice(&data)?;
                debug!(
                    key = %key,
                    sequence_number = checkpoint.sequence_number,
                    "Loaded checkpoint from S3"
                );
                Ok(Some((checkpoint, etag)))
            }
            None => {
                debug!(key = %key, "Checkpoint not found");
                Ok(None)
            }
        }
    }

    /// Delete a checkpoint from S3.
    #[instrument(skip(self), fields(namespace = %namespace, stream_key = %stream_key, consumer_id = %consumer_id))]
    pub async fn delete_checkpoint(
        &self,
        namespace: &str,
        stream_key: &str,
        consumer_id: &str,
    ) -> S3Result<()> {
        let key = self.build_checkpoint_key(namespace, stream_key, consumer_id);

        debug!(key = %key, "Deleting checkpoint from S3");

        // Ignore not found errors
        if let Err(e) = self.client.delete_object(&key).await {
            if !e.is_not_found() {
                return Err(e);
            }
        }

        debug!(key = %key, "Deleted checkpoint from S3");

        Ok(())
    }

    /// List all checkpoints for a stream.
    #[instrument(skip(self), fields(namespace = %namespace, stream_key = %stream_key))]
    pub async fn list_checkpoints(
        &self,
        namespace: &str,
        stream_key: &str,
    ) -> S3Result<Vec<String>> {
        let prefix = format!(
            "checkpoints/{}/{}/",
            sanitize_key_component(namespace),
            sanitize_key_component(stream_key)
        );

        debug!(prefix = %prefix, "Listing checkpoints in S3");

        let keys = self.client.list_objects(&prefix).await?;

        // Extract consumer IDs from keys
        let consumer_ids: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix(&prefix)
                    .and_then(|s| s.strip_suffix(".json"))
                    .map(|s| s.to_string())
            })
            .collect();

        debug!(count = consumer_ids.len(), "Listed checkpoints");

        Ok(consumer_ids)
    }

    /// List all streams that have checkpoints.
    #[instrument(skip(self), fields(namespace = %namespace))]
    pub async fn list_streams(&self, namespace: &str) -> S3Result<Vec<String>> {
        let prefix = format!("checkpoints/{}/", sanitize_key_component(namespace));

        debug!(prefix = %prefix, "Listing streams with checkpoints");

        let keys = self.client.list_objects(&prefix).await?;

        // Extract unique stream keys
        let mut stream_keys: Vec<String> = keys
            .into_iter()
            .filter_map(|key| {
                key.strip_prefix(&prefix)
                    .and_then(|s| s.split('/').next())
                    .map(|s| s.to_string())
            })
            .collect();

        stream_keys.sort();
        stream_keys.dedup();

        debug!(count = stream_keys.len(), "Listed streams");

        Ok(stream_keys)
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
    fn test_checkpoint_new() {
        let checkpoint = StreamCheckpoint::new("orders", "customer-123", "processor-1", 42, 0);

        assert_eq!(checkpoint.namespace, "orders");
        assert_eq!(checkpoint.stream_key, "customer-123");
        assert_eq!(checkpoint.consumer_id, "processor-1");
        assert_eq!(checkpoint.sequence_number, 42);
        assert_eq!(checkpoint.event_index, 0);
        assert!(checkpoint.metadata.is_none());
    }

    #[test]
    fn test_checkpoint_with_metadata() {
        let metadata = serde_json::json!({"processing_time_ms": 150});
        let checkpoint =
            StreamCheckpoint::new("orders", "customer-123", "processor-1", 42, 0)
                .with_metadata(metadata.clone());

        assert_eq!(checkpoint.metadata, Some(metadata));
    }

    #[test]
    fn test_checkpoint_serialization_roundtrip() {
        let checkpoint = StreamCheckpoint::new("orders", "customer-123", "processor-1", 42, 5);

        let json = serde_json::to_string(&checkpoint).unwrap();
        let deserialized: StreamCheckpoint = serde_json::from_str(&json).unwrap();

        assert_eq!(checkpoint.namespace, deserialized.namespace);
        assert_eq!(checkpoint.stream_key, deserialized.stream_key);
        assert_eq!(checkpoint.consumer_id, deserialized.consumer_id);
        assert_eq!(checkpoint.sequence_number, deserialized.sequence_number);
        assert_eq!(checkpoint.event_index, deserialized.event_index);
    }

    #[test]
    fn test_build_checkpoint_key() {
        let key = format!(
            "checkpoints/{}/{}/{}.json",
            sanitize_key_component("orders"),
            sanitize_key_component("customer-123"),
            sanitize_key_component("processor-1")
        );

        assert_eq!(key, "checkpoints/orders/customer-123/processor-1.json");
    }

    #[test]
    fn test_build_checkpoint_key_with_special_chars() {
        let key = format!(
            "checkpoints/{}/{}/{}.json",
            sanitize_key_component("my/namespace"),
            sanitize_key_component("stream:key"),
            sanitize_key_component("consumer*1")
        );

        assert_eq!(
            key,
            "checkpoints/my_namespace/stream_key/consumer_1.json"
        );
    }

    #[test]
    fn test_sanitize_key_component() {
        assert_eq!(sanitize_key_component("simple"), "simple");
        assert_eq!(sanitize_key_component("with/slash"), "with_slash");
        assert_eq!(sanitize_key_component("with:colon"), "with_colon");
    }
}
