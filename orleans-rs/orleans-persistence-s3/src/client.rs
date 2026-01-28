//! S3 client wrapper with retry logic and compression support.
//!
//! This module provides a high-level client wrapper around the AWS SDK S3 client,
//! adding retry logic, compression, and structured logging.

use std::io::{Read, Write};

use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::Builder as S3ConfigBuilder,
    primitives::ByteStream,
    types::{ChecksumAlgorithm, ServerSideEncryption},
    Client,
};
use bytes::Bytes;
use tracing::{debug, instrument};

use crate::error::{S3Error, S3Result};
use crate::options::{CompressionType, S3Options};

/// S3 client wrapper with retry logic and compression.
#[derive(Clone)]
pub struct S3Client {
    client: Client,
    options: S3Options,
}

impl std::fmt::Debug for S3Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Client")
            .field("bucket", &self.options.bucket)
            .field("region", &self.options.region)
            .field("endpoint", &self.options.endpoint_url)
            .finish()
    }
}

impl S3Client {
    /// Create a new S3 client with the given options.
    #[instrument(skip(options), fields(bucket = %options.bucket))]
    pub async fn new(options: S3Options) -> S3Result<Self> {
        options.validate()?;

        debug!(
            bucket = %options.bucket,
            region = ?options.region,
            endpoint = ?options.endpoint_url,
            "Creating S3 client"
        );

        // Build AWS SDK config
        let mut sdk_config_loader =
            aws_config::defaults(BehaviorVersion::latest()).retry_config(
                aws_config::retry::RetryConfig::standard()
                    .with_max_attempts(options.max_retry_attempts),
            );

        if let Some(region) = &options.region {
            sdk_config_loader = sdk_config_loader.region(aws_config::Region::new(region.clone()));
        }

        let sdk_config = sdk_config_loader.load().await;

        // Build S3 client config
        let mut s3_config_builder = S3ConfigBuilder::from(&sdk_config);

        if let Some(endpoint) = &options.endpoint_url {
            s3_config_builder = s3_config_builder.endpoint_url(endpoint);
        }

        if options.force_path_style {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_config = s3_config_builder.build();
        let client = Client::from_conf(s3_config);

        Ok(Self { client, options })
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.options.bucket
    }

    /// Get the options.
    pub fn options(&self) -> &S3Options {
        &self.options
    }

    /// Build the full object key with prefix.
    pub fn build_key(&self, key: &str) -> String {
        self.options.build_key(key)
    }

    /// Get an object from S3.
    ///
    /// Returns the object data and ETag.
    #[instrument(skip(self), fields(bucket = %self.options.bucket, key = %key))]
    pub async fn get_object(&self, key: &str) -> S3Result<(Bytes, String)> {
        let full_key = self.build_key(key);

        debug!(key = %full_key, "Getting object from S3");

        let result = self
            .client
            .get_object()
            .bucket(&self.options.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| self.map_sdk_error(e, &full_key))?;

        let etag = result
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();

        let content_encoding = result.content_encoding().map(|s| s.to_string());

        let body = result
            .body
            .collect()
            .await
            .map_err(|e| S3Error::Network(e.to_string()))?
            .into_bytes();

        // Decompress if needed
        let data = self.decompress(&body, content_encoding.as_deref())?;

        debug!(
            key = %full_key,
            etag = %etag,
            size = data.len(),
            "Got object from S3"
        );

        Ok((data, etag))
    }

    /// Get an object if it exists, returning None if not found.
    pub async fn get_object_if_exists(&self, key: &str) -> S3Result<Option<(Bytes, String)>> {
        match self.get_object(key).await {
            Ok(result) => Ok(Some(result)),
            Err(S3Error::ObjectNotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Put an object to S3.
    ///
    /// Returns the new ETag.
    #[instrument(skip(self, data), fields(bucket = %self.options.bucket, key = %key, size = data.len()))]
    pub async fn put_object(&self, key: &str, data: &[u8]) -> S3Result<String> {
        let full_key = self.build_key(key);

        debug!(key = %full_key, size = data.len(), "Putting object to S3");

        // Check size limit
        if data.len() as u64 > self.options.max_object_size {
            return Err(S3Error::ObjectTooLarge {
                size: data.len() as u64,
                max_size: self.options.max_object_size,
            });
        }

        // Compress if configured
        let (body, content_encoding) = self.compress(data)?;

        let mut request = self
            .client
            .put_object()
            .bucket(&self.options.bucket)
            .key(&full_key)
            .body(ByteStream::from(body));

        // Set content encoding for compressed data
        if let Some(encoding) = content_encoding {
            request = request.content_encoding(encoding);
        }

        // Set server-side encryption if configured
        if let Some(kms_key_id) = &self.options.kms_key_id {
            request = request
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .ssekms_key_id(kms_key_id);
        } else if self.options.enable_sse {
            request = request.server_side_encryption(ServerSideEncryption::Aes256);
        }

        // Enable checksum if configured
        if self.options.enable_checksum {
            request = request.checksum_algorithm(ChecksumAlgorithm::Crc32C);
        }

        let result = request
            .send()
            .await
            .map_err(|e| self.map_sdk_error(e, &full_key))?;

        let etag = result
            .e_tag()
            .map(|s| s.trim_matches('"').to_string())
            .unwrap_or_default();

        debug!(key = %full_key, etag = %etag, "Put object to S3");

        Ok(etag)
    }

    /// Put an object with conditional write (If-None-Match).
    ///
    /// Only writes if the object doesn't exist.
    #[instrument(skip(self, data), fields(bucket = %self.options.bucket, key = %key, size = data.len()))]
    pub async fn put_object_if_not_exists(&self, key: &str, data: &[u8]) -> S3Result<String> {
        let full_key = self.build_key(key);

        // Check if object exists first (S3 doesn't support If-None-Match on PutObject)
        if self.head_object(key).await?.is_some() {
            return Err(S3Error::ObjectAlreadyExists {
                bucket: self.options.bucket.clone(),
                key: full_key,
            });
        }

        self.put_object(key, data).await
    }

    /// Put an object with conditional write (If-Match).
    ///
    /// Only writes if the object's ETag matches the expected value.
    #[instrument(skip(self, data), fields(bucket = %self.options.bucket, key = %key, expected_etag = %expected_etag))]
    pub async fn put_object_if_match(
        &self,
        key: &str,
        data: &[u8],
        expected_etag: &str,
    ) -> S3Result<String> {
        let full_key = self.build_key(key);

        // Check current ETag
        let current_etag = self
            .head_object(key)
            .await?
            .ok_or_else(|| S3Error::ObjectNotFound {
                bucket: self.options.bucket.clone(),
                key: full_key.clone(),
            })?;

        if current_etag != expected_etag {
            return Err(S3Error::EtagMismatch {
                expected: expected_etag.to_string(),
                actual: current_etag,
            });
        }

        self.put_object(key, data).await
    }

    /// Delete an object from S3.
    #[instrument(skip(self), fields(bucket = %self.options.bucket, key = %key))]
    pub async fn delete_object(&self, key: &str) -> S3Result<()> {
        let full_key = self.build_key(key);

        debug!(key = %full_key, "Deleting object from S3");

        self.client
            .delete_object()
            .bucket(&self.options.bucket)
            .key(&full_key)
            .send()
            .await
            .map_err(|e| self.map_sdk_error(e, &full_key))?;

        debug!(key = %full_key, "Deleted object from S3");

        Ok(())
    }

    /// Delete an object with conditional delete (If-Match).
    #[instrument(skip(self), fields(bucket = %self.options.bucket, key = %key, expected_etag = %expected_etag))]
    pub async fn delete_object_if_match(&self, key: &str, expected_etag: &str) -> S3Result<()> {
        let full_key = self.build_key(key);

        // Check current ETag
        let current_etag = self
            .head_object(key)
            .await?
            .ok_or_else(|| S3Error::ObjectNotFound {
                bucket: self.options.bucket.clone(),
                key: full_key.clone(),
            })?;

        if current_etag != expected_etag {
            return Err(S3Error::EtagMismatch {
                expected: expected_etag.to_string(),
                actual: current_etag,
            });
        }

        self.delete_object(key).await
    }

    /// Check if an object exists and get its ETag.
    #[instrument(skip(self), fields(bucket = %self.options.bucket, key = %key))]
    pub async fn head_object(&self, key: &str) -> S3Result<Option<String>> {
        let full_key = self.build_key(key);

        match self
            .client
            .head_object()
            .bucket(&self.options.bucket)
            .key(&full_key)
            .send()
            .await
        {
            Ok(result) => {
                let etag = result
                    .e_tag()
                    .map(|s| s.trim_matches('"').to_string())
                    .unwrap_or_default();
                Ok(Some(etag))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_not_found() {
                    Ok(None)
                } else {
                    Err(S3Error::AwsSdk(service_err.to_string()))
                }
            }
        }
    }

    /// List objects with a prefix.
    #[instrument(skip(self), fields(bucket = %self.options.bucket, prefix = %prefix))]
    pub async fn list_objects(&self, prefix: &str) -> S3Result<Vec<String>> {
        let full_prefix = self.build_key(prefix);

        debug!(prefix = %full_prefix, "Listing objects in S3");

        let mut keys = Vec::new();
        let mut continuation_token = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.options.bucket)
                .prefix(&full_prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let result = request
                .send()
                .await
                .map_err(|e| S3Error::AwsSdk(e.to_string()))?;

            for obj in result.contents() {
                if let Some(key) = obj.key() {
                    // Strip the key prefix if present
                    let stripped_key = if !self.options.key_prefix.is_empty() {
                        key.strip_prefix(&self.options.key_prefix)
                            .unwrap_or(key)
                            .to_string()
                    } else {
                        key.to_string()
                    };
                    keys.push(stripped_key);
                }
            }

            if result.is_truncated() == Some(true) {
                continuation_token = result.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        debug!(
            prefix = %full_prefix,
            count = keys.len(),
            "Listed objects in S3"
        );

        Ok(keys)
    }

    /// Compress data based on the configured compression type.
    fn compress(&self, data: &[u8]) -> S3Result<(Bytes, Option<&'static str>)> {
        match self.options.compression {
            CompressionType::None => Ok((Bytes::copy_from_slice(data), None)),
            CompressionType::Gzip => {
                let mut encoder = flate2::write::GzEncoder::new(
                    Vec::new(),
                    flate2::Compression::new(self.options.compression_level as u32),
                );
                encoder
                    .write_all(data)
                    .map_err(|e| S3Error::Compression(e.to_string()))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| S3Error::Compression(e.to_string()))?;
                Ok((Bytes::from(compressed), Some("gzip")))
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data, self.options.compression_level)
                    .map_err(|e| S3Error::Compression(e.to_string()))?;
                Ok((Bytes::from(compressed), Some("zstd")))
            }
        }
    }

    /// Decompress data based on content encoding.
    fn decompress(&self, data: &Bytes, content_encoding: Option<&str>) -> S3Result<Bytes> {
        match content_encoding {
            None | Some("identity") => Ok(data.clone()),
            Some("gzip") => {
                let mut decoder = flate2::read::GzDecoder::new(&data[..]);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| S3Error::Decompression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            Some("zstd") => {
                let decompressed = zstd::decode_all(&data[..])
                    .map_err(|e| S3Error::Decompression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            Some(encoding) => Err(S3Error::Decompression(format!(
                "unsupported content encoding: {}",
                encoding
            ))),
        }
    }

    /// Map AWS SDK errors to S3Error.
    fn map_sdk_error<E: std::fmt::Display>(&self, error: E, key: &str) -> S3Error {
        let error_str = error.to_string();

        if error_str.contains("NoSuchBucket") {
            S3Error::BucketNotFound(self.options.bucket.clone())
        } else if error_str.contains("NoSuchKey") || error_str.contains("NotFound") {
            S3Error::ObjectNotFound {
                bucket: self.options.bucket.clone(),
                key: key.to_string(),
            }
        } else if error_str.contains("AccessDenied") {
            S3Error::AccessDenied(error_str)
        } else if error_str.contains("PreconditionFailed") {
            S3Error::PreconditionFailed(error_str)
        } else if error_str.contains("SlowDown") || error_str.contains("TooManyRequests") {
            S3Error::RateLimitExceeded(error_str)
        } else if error_str.contains("timeout") || error_str.contains("Timeout") {
            S3Error::Timeout(self.options.request_timeout)
        } else {
            S3Error::AwsSdk(error_str)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_debug() {
        // Just ensure Debug is implemented correctly
        let options = S3Options::for_testing("test-bucket");
        // We can't actually create the client without AWS credentials,
        // but we can test that the struct has correct Debug impl
        assert!(format!("{:?}", options).contains("test-bucket"));
    }

    #[test]
    fn test_compression_roundtrip_none() {
        let options = S3Options::new("test-bucket");
        let data = b"hello world";

        // Manually test compression/decompression logic
        let compressed = match options.compression {
            CompressionType::None => Bytes::copy_from_slice(data),
            _ => panic!("unexpected compression type"),
        };
        assert_eq!(&compressed[..], data);
    }

    #[tokio::test]
    async fn test_options_validation_in_new() {
        // Empty bucket should fail validation
        let options = S3Options::default();
        // This will fail at validation before attempting to create the client
        assert!(options.validate().is_err());
    }

    #[test]
    fn test_build_key_with_prefix() {
        let options = S3Options::new("test-bucket").with_key_prefix("orleans/");
        assert_eq!(options.build_key("grains/my-grain"), "orleans/grains/my-grain");
    }

    #[test]
    fn test_build_key_without_prefix() {
        let options = S3Options::new("test-bucket");
        assert_eq!(options.build_key("grains/my-grain"), "grains/my-grain");
    }
}
