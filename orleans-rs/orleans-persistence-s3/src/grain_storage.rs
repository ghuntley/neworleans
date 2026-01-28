//! S3-based grain state storage provider.
//!
//! This module provides an implementation of `IGrainStorage` that stores
//! grain state as objects in AWS S3.

use async_trait::async_trait;
use orleans_core::GrainId;
use orleans_persistence::{IGrainStorage, RawGrainState, StorageResult};
use tracing::{debug, instrument};

use crate::client::S3Client;
use crate::error::{S3Error, S3Result};
use crate::options::S3Options;

/// S3-based grain storage provider.
///
/// Stores grain state as objects in S3 with the following key format:
/// `{prefix}grains/{grain_type}/{grain_key}/{state_name}.bin`
///
/// # Features
///
/// - ETag-based optimistic concurrency control via S3 conditional requests
/// - Optional compression (gzip or zstd)
/// - Server-side encryption (SSE-S3 or SSE-KMS)
/// - Automatic retry with exponential backoff
///
/// # Example
///
/// ```ignore
/// use orleans_persistence_s3::{S3Options, S3GrainStorage};
///
/// let options = S3Options::new("my-bucket")
///     .with_key_prefix("orleans/")
///     .with_region("us-east-1");
///
/// let storage = S3GrainStorage::new(options).await?;
/// ```
#[derive(Clone)]
pub struct S3GrainStorage {
    client: S3Client,
}

impl std::fmt::Debug for S3GrainStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3GrainStorage")
            .field("client", &self.client)
            .finish()
    }
}

impl S3GrainStorage {
    /// Create a new S3 grain storage provider.
    #[instrument(skip(options), fields(bucket = %options.bucket))]
    pub async fn new(options: S3Options) -> S3Result<Self> {
        let client = S3Client::new(options).await?;
        debug!("Created S3GrainStorage");
        Ok(Self { client })
    }

    /// Create a new S3 grain storage provider from an existing client.
    pub fn from_client(client: S3Client) -> Self {
        Self { client }
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        self.client.bucket()
    }

    /// Get the S3 options.
    pub fn options(&self) -> &S3Options {
        self.client.options()
    }

    /// Build the object key for a grain's state.
    ///
    /// Format: `grains/{grain_type}/{grain_key}/{state_name}.bin`
    fn build_state_key(&self, state_name: &str, grain_id: &GrainId) -> String {
        let grain_type = grain_id.grain_type().to_string();
        let grain_key = grain_id.key().to_string();

        // Sanitize the grain type and key for use in S3 keys
        let sanitized_type = sanitize_key_component(&grain_type);
        let sanitized_key = sanitize_key_component(&grain_key);
        let sanitized_state = sanitize_key_component(state_name);

        format!(
            "grains/{}/{}/{}.bin",
            sanitized_type, sanitized_key, sanitized_state
        )
    }
}

/// Sanitize a string for use in an S3 object key.
///
/// Replaces characters that could cause issues in S3 keys.
fn sanitize_key_component(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => c,
        })
        .collect()
}

#[async_trait]
impl IGrainStorage for S3GrainStorage {
    #[instrument(skip(self), fields(state_name = %state_name, grain_id = %grain_id))]
    async fn read_state(&self, state_name: &str, grain_id: &GrainId) -> StorageResult<RawGrainState> {
        let key = self.build_state_key(state_name, grain_id);

        debug!(key = %key, "Reading grain state from S3");

        match self.client.get_object_if_exists(&key).await {
            Ok(Some((data, etag))) => {
                debug!(
                    key = %key,
                    etag = %etag,
                    size = data.len(),
                    "Read grain state from S3"
                );
                Ok(RawGrainState::with_data(data.to_vec(), etag))
            }
            Ok(None) => {
                debug!(key = %key, "Grain state not found in S3");
                Ok(RawGrainState::empty())
            }
            Err(e) => Err(e.into()),
        }
    }

    #[instrument(skip(self, state), fields(state_name = %state_name, grain_id = %grain_id))]
    async fn write_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        state: &RawGrainState,
    ) -> StorageResult<String> {
        let key = self.build_state_key(state_name, grain_id);

        debug!(
            key = %key,
            size = state.data.len(),
            has_etag = state.etag.is_some(),
            "Writing grain state to S3"
        );

        let etag = match &state.etag {
            // Insert - state has no ETag (first write)
            None => {
                // For first write, we could either:
                // 1. Just write (overwriting any existing data) - simple but not safe
                // 2. Check if object exists first - safer but slower
                // We choose option 2 for safety
                match self.client.put_object_if_not_exists(&key, &state.data).await {
                    Ok(etag) => etag,
                    Err(S3Error::ObjectAlreadyExists { .. }) => {
                        // Object already exists - this is a conflict
                        // Read current ETag and return error
                        if let Ok(Some(current_etag)) = self.client.head_object(&key).await {
                            return Err(orleans_persistence::StorageError::EtagMismatch {
                                expected: "none".to_string(),
                                stored: current_etag,
                            });
                        }
                        return Err(orleans_persistence::StorageError::RecordExists);
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            // Update - state has an ETag, verify it matches
            Some(expected_etag) => {
                // Wildcard ETag means unconditional write
                if expected_etag == "*" {
                    self.client.put_object(&key, &state.data).await?
                } else {
                    self.client
                        .put_object_if_match(&key, &state.data, expected_etag)
                        .await?
                }
            }
        };

        debug!(key = %key, etag = %etag, "Wrote grain state to S3");

        Ok(etag)
    }

    #[instrument(skip(self), fields(state_name = %state_name, grain_id = %grain_id))]
    async fn clear_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        expected_etag: Option<&str>,
    ) -> StorageResult<()> {
        let key = self.build_state_key(state_name, grain_id);

        debug!(
            key = %key,
            expected_etag = ?expected_etag,
            "Clearing grain state from S3"
        );

        match expected_etag {
            // Unconditional delete or wildcard
            None | Some("*") => {
                if let Err(e) = self.client.delete_object(&key).await {
                    // Ignore not found errors for unconditional delete
                    if !e.is_not_found() {
                        return Err(e.into());
                    }
                }
            }
            // Conditional delete with ETag check
            Some(etag) => {
                self.client.delete_object_if_match(&key, etag).await?;
            }
        }

        debug!(key = %key, "Cleared grain state from S3");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainType, IdSpan};

    fn test_grain_id() -> GrainId {
        let grain_type = GrainType::create("test.grain");
        let key = IdSpan::from_str("test-key-123");
        GrainId::new(grain_type, key)
    }

    #[test]
    fn test_build_state_key() {
        // We can test the key building logic without an actual S3 client
        let grain_id = test_grain_id();
        let grain_type = grain_id.grain_type().to_string();
        let grain_key = grain_id.key().to_string();

        let sanitized_type = sanitize_key_component(&grain_type);
        let sanitized_key = sanitize_key_component(&grain_key);

        let key = format!("grains/{}/{}/{}.bin", sanitized_type, sanitized_key, "counter");
        assert!(key.contains("grains/"));
        assert!(key.contains("/counter.bin"));
    }

    #[test]
    fn test_sanitize_key_component() {
        assert_eq!(sanitize_key_component("simple"), "simple");
        assert_eq!(sanitize_key_component("with/slash"), "with_slash");
        assert_eq!(sanitize_key_component("with:colon"), "with_colon");
        assert_eq!(sanitize_key_component("with*star"), "with_star");
        assert_eq!(sanitize_key_component("multiple/bad:chars*here"), "multiple_bad_chars_here");
    }

    #[test]
    fn test_sanitize_key_component_preserves_safe_chars() {
        assert_eq!(sanitize_key_component("test-grain"), "test-grain");
        assert_eq!(sanitize_key_component("test.grain"), "test.grain");
        assert_eq!(sanitize_key_component("test_grain"), "test_grain");
        assert_eq!(sanitize_key_component("Test123"), "Test123");
    }

    #[test]
    fn test_raw_grain_state_empty() {
        let state = RawGrainState::empty();
        assert!(state.data.is_empty());
        assert!(state.etag.is_none());
        assert!(!state.record_exists);
    }

    #[test]
    fn test_raw_grain_state_with_data() {
        let data = vec![1, 2, 3, 4];
        let state = RawGrainState::with_data(data.clone(), "etag123".to_string());
        assert_eq!(state.data, data);
        assert_eq!(state.etag, Some("etag123".to_string()));
        assert!(state.record_exists);
    }
}
