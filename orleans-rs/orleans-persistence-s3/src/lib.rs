//! AWS S3 storage providers for Orleans-RS.
//!
//! This crate provides S3 implementations of Orleans storage interfaces:
//!
//! - [`S3GrainStorage`] - Grain state persistence implementing `IGrainStorage`
//! - [`S3StreamCheckpointStorage`] - Stream consumer checkpoint storage
//! - [`S3EventLogStorage`] - Event sourcing event log storage
//!
//! # Features
//!
//! - Full AWS S3 support via the official AWS SDK
//! - S3-compatible services support (MinIO, LocalStack)
//! - ETag-based optimistic concurrency control
//! - Optional compression (gzip, zstd)
//! - Server-side encryption (SSE-S3, SSE-KMS)
//! - Automatic retry with exponential backoff
//! - Structured logging via `tracing`
//!
//! # Example
//!
//! ```ignore
//! use orleans_persistence_s3::{S3Options, S3GrainStorage};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Configure S3 connection
//!     let options = S3Options::new("my-orleans-bucket")
//!         .with_region("us-east-1")
//!         .with_key_prefix("orleans/");
//!
//!     // Create grain storage provider
//!     let storage = S3GrainStorage::new(options).await?;
//!
//!     // Use with Orleans silo...
//!     Ok(())
//! }
//! ```
//!
//! # S3-Compatible Services
//!
//! For local development with MinIO or LocalStack:
//!
//! ```ignore
//! use orleans_persistence_s3::{S3Options, S3GrainStorage};
//!
//! // For LocalStack
//! let options = S3Options::for_testing("test-bucket");
//!
//! // For MinIO
//! let options = S3Options::for_minio("my-bucket", "http://localhost:9000");
//! ```
//!
//! # Object Key Structure
//!
//! Objects are stored with the following key patterns:
//!
//! ## Grain State
//! ```text
//! {prefix}grains/{grain_type}/{grain_key}/{state_name}.bin
//! ```
//!
//! ## Stream Checkpoints
//! ```text
//! {prefix}checkpoints/{namespace}/{stream_key}/{consumer_id}.json
//! ```
//!
//! ## Event Logs
//! ```text
//! {prefix}events/{grain_id}/{sequence:020}.json
//! ```
//!
//! # Compression
//!
//! Enable compression to reduce storage costs and transfer times:
//!
//! ```ignore
//! use orleans_persistence_s3::{S3Options, CompressionType};
//!
//! let options = S3Options::new("my-bucket")
//!     .with_compression(CompressionType::Gzip)
//!     .with_compression_level(6);
//! ```
//!
//! # Server-Side Encryption
//!
//! Enable encryption at rest:
//!
//! ```ignore
//! use orleans_persistence_s3::S3Options;
//!
//! // SSE-S3 (AES-256)
//! let options = S3Options::new("my-bucket").with_sse();
//!
//! // SSE-KMS
//! let options = S3Options::new("my-bucket")
//!     .with_kms("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012");
//! ```

mod checkpoint;
mod client;
mod error;
mod event_log;
mod grain_storage;
mod options;

pub use checkpoint::{S3StreamCheckpointStorage, StreamCheckpoint};
pub use client::S3Client;
pub use error::{S3Error, S3Result};
pub use event_log::{EventBatch, EventEntry, S3EventLogStorage};
pub use grain_storage::S3GrainStorage;
pub use options::{CompressionType, S3Options};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_options_validation() {
        let opts = S3Options::new("my-bucket");
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_options_for_testing() {
        let opts = S3Options::for_testing("test-bucket");
        assert_eq!(opts.bucket, "test-bucket");
        assert!(opts.force_path_style);
        assert_eq!(opts.endpoint_url, Some("http://localhost:4566".to_string()));
    }

    #[test]
    fn test_options_for_minio() {
        let opts = S3Options::for_minio("my-bucket", "http://minio:9000");
        assert_eq!(opts.bucket, "my-bucket");
        assert!(opts.force_path_style);
        assert_eq!(opts.endpoint_url, Some("http://minio:9000".to_string()));
    }

    #[test]
    fn test_compression_type_default() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(S3Error::Network("timeout".into()).is_retryable());
        assert!(!S3Error::BucketNotFound("bucket".into()).is_retryable());
    }

    #[test]
    fn test_error_is_concurrency_error() {
        assert!(S3Error::EtagMismatch {
            expected: "a".into(),
            actual: "b".into()
        }
        .is_concurrency_error());
        assert!(!S3Error::ObjectNotFound {
            bucket: "b".into(),
            key: "k".into()
        }
        .is_concurrency_error());
    }

    #[test]
    fn test_stream_checkpoint_new() {
        let checkpoint = StreamCheckpoint::new("ns", "stream", "consumer", 42, 0);
        assert_eq!(checkpoint.namespace, "ns");
        assert_eq!(checkpoint.stream_key, "stream");
        assert_eq!(checkpoint.consumer_id, "consumer");
        assert_eq!(checkpoint.sequence_number, 42);
    }

    #[test]
    fn test_event_entry_new() {
        let event = EventEntry::new("grain-1", 1, "Created", vec![1, 2, 3]);
        assert_eq!(event.grain_id, "grain-1");
        assert_eq!(event.sequence_number, 1);
        assert_eq!(event.event_type, "Created");
    }

    #[test]
    fn test_event_batch_creation() {
        let events = vec![
            EventEntry::new("grain-1", 1, "A", vec![]),
            EventEntry::new("grain-1", 2, "B", vec![]),
        ];
        let batch = EventBatch::new("grain-1", events).unwrap();
        assert_eq!(batch.start_sequence, 1);
        assert_eq!(batch.end_sequence, 2);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_options_with_compression() {
        let opts = S3Options::new("my-bucket")
            .with_compression(CompressionType::Gzip)
            .with_compression_level(9);
        assert_eq!(opts.compression, CompressionType::Gzip);
        assert_eq!(opts.compression_level, 9);
    }

    #[test]
    fn test_options_with_encryption() {
        let opts = S3Options::new("my-bucket").with_sse();
        assert!(opts.enable_sse);
        assert!(opts.kms_key_id.is_none());

        let opts_kms = S3Options::new("my-bucket").with_kms("key-id");
        assert!(opts_kms.kms_key_id.is_some());
    }
}
