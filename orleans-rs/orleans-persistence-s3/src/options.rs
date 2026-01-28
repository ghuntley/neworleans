//! S3 storage configuration options.
//!
//! This module provides configuration types for S3 storage providers,
//! including bucket settings, credentials, compression, and retry policies.

use std::time::Duration;

use crate::error::{S3Error, S3Result};

/// Compression algorithm for S3 objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionType {
    /// No compression.
    #[default]
    None,
    /// Gzip compression.
    Gzip,
    /// Zstandard compression.
    Zstd,
}

impl CompressionType {
    /// Get the content encoding header value for this compression type.
    pub fn content_encoding(&self) -> Option<&'static str> {
        match self {
            CompressionType::None => None,
            CompressionType::Gzip => Some("gzip"),
            CompressionType::Zstd => Some("zstd"),
        }
    }

    /// Get the file extension suffix for this compression type.
    pub fn extension(&self) -> &'static str {
        match self {
            CompressionType::None => "",
            CompressionType::Gzip => ".gz",
            CompressionType::Zstd => ".zst",
        }
    }
}

/// Configuration options for S3 storage providers.
#[derive(Debug, Clone)]
pub struct S3Options {
    /// S3 bucket name for storing objects.
    pub bucket: String,

    /// AWS region (e.g., "us-east-1").
    /// If not specified, uses the default SDK region resolution.
    pub region: Option<String>,

    /// Custom S3 endpoint URL for S3-compatible services (MinIO, LocalStack).
    /// If not specified, uses the default AWS S3 endpoint.
    pub endpoint_url: Option<String>,

    /// Force path-style URLs (required for some S3-compatible services).
    /// When true, uses `http://endpoint/bucket/key` instead of `http://bucket.endpoint/key`.
    pub force_path_style: bool,

    /// Key prefix for all objects (e.g., "orleans/grains/").
    /// The prefix is prepended to all object keys.
    pub key_prefix: String,

    /// Compression type for stored objects.
    pub compression: CompressionType,

    /// Compression level (1-9 for gzip, 1-22 for zstd).
    /// Higher levels provide better compression but are slower.
    pub compression_level: i32,

    /// Request timeout for S3 operations.
    pub request_timeout: Duration,

    /// Connection timeout for establishing connections.
    pub connect_timeout: Duration,

    /// Maximum number of retry attempts for retryable errors.
    pub max_retry_attempts: u32,

    /// Base delay for exponential backoff between retries.
    pub retry_base_delay: Duration,

    /// Maximum delay for exponential backoff.
    pub retry_max_delay: Duration,

    /// Enable server-side encryption (SSE-S3).
    pub enable_sse: bool,

    /// Use AWS KMS for server-side encryption (SSE-KMS).
    /// If set, specifies the KMS key ID.
    pub kms_key_id: Option<String>,

    /// Maximum object size in bytes (default: 5GB).
    pub max_object_size: u64,

    /// Enable multipart upload for large objects.
    pub enable_multipart_upload: bool,

    /// Part size for multipart uploads (default: 5MB).
    pub multipart_part_size: u64,

    /// Enable checksum validation (CRC32C).
    pub enable_checksum: bool,
}

impl Default for S3Options {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: None,
            endpoint_url: None,
            force_path_style: false,
            key_prefix: String::new(),
            compression: CompressionType::None,
            compression_level: 6,
            request_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_retry_attempts: 3,
            retry_base_delay: Duration::from_millis(100),
            retry_max_delay: Duration::from_secs(30),
            enable_sse: false,
            kms_key_id: None,
            max_object_size: 5 * 1024 * 1024 * 1024, // 5GB
            enable_multipart_upload: true,
            multipart_part_size: 5 * 1024 * 1024, // 5MB
            enable_checksum: true,
        }
    }
}

impl S3Options {
    /// Create new S3 options with the specified bucket.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..Default::default()
        }
    }

    /// Create options for testing with LocalStack or MinIO.
    ///
    /// Uses localhost:4566 as the endpoint and force path-style URLs.
    pub fn for_testing(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint_url: Some("http://localhost:4566".to_string()),
            force_path_style: true,
            region: Some("us-east-1".to_string()),
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
            max_retry_attempts: 1,
            ..Default::default()
        }
    }

    /// Create options for MinIO.
    pub fn for_minio(bucket: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            endpoint_url: Some(endpoint.into()),
            force_path_style: true,
            region: Some("us-east-1".to_string()),
            ..Default::default()
        }
    }

    /// Set the AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint URL for S3-compatible services.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint_url = Some(endpoint.into());
        self
    }

    /// Enable force path-style URLs.
    pub fn with_force_path_style(mut self, force: bool) -> Self {
        self.force_path_style = force;
        self
    }

    /// Set the key prefix for all objects.
    pub fn with_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }

    /// Set the compression type.
    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    /// Set the compression level.
    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    /// Set the request timeout.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set the maximum retry attempts.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retry_attempts = retries;
        self
    }

    /// Enable server-side encryption (SSE-S3).
    pub fn with_sse(mut self) -> Self {
        self.enable_sse = true;
        self
    }

    /// Enable KMS server-side encryption (SSE-KMS).
    pub fn with_kms(mut self, key_id: impl Into<String>) -> Self {
        self.kms_key_id = Some(key_id.into());
        self
    }

    /// Set the maximum object size.
    pub fn with_max_object_size(mut self, size: u64) -> Self {
        self.max_object_size = size;
        self
    }

    /// Enable or disable multipart upload.
    pub fn with_multipart_upload(mut self, enable: bool) -> Self {
        self.enable_multipart_upload = enable;
        self
    }

    /// Set the multipart upload part size.
    pub fn with_multipart_part_size(mut self, size: u64) -> Self {
        self.multipart_part_size = size;
        self
    }

    /// Enable or disable checksum validation.
    pub fn with_checksum(mut self, enable: bool) -> Self {
        self.enable_checksum = enable;
        self
    }

    /// Validate the options.
    pub fn validate(&self) -> S3Result<()> {
        if self.bucket.is_empty() {
            return Err(S3Error::Configuration("bucket name is required".into()));
        }

        // Validate bucket name (simplified S3 bucket naming rules)
        if self.bucket.len() < 3 || self.bucket.len() > 63 {
            return Err(S3Error::InvalidBucketName(
                "bucket name must be 3-63 characters".into(),
            ));
        }

        if !self
            .bucket
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.')
        {
            return Err(S3Error::InvalidBucketName(
                "bucket name must contain only lowercase letters, numbers, hyphens, and periods"
                    .into(),
            ));
        }

        // Validate compression level
        match self.compression {
            CompressionType::None => {}
            CompressionType::Gzip => {
                if !(1..=9).contains(&self.compression_level) {
                    return Err(S3Error::Configuration(
                        "gzip compression level must be 1-9".into(),
                    ));
                }
            }
            CompressionType::Zstd => {
                if !(1..=22).contains(&self.compression_level) {
                    return Err(S3Error::Configuration(
                        "zstd compression level must be 1-22".into(),
                    ));
                }
            }
        }

        // Validate multipart settings
        if self.enable_multipart_upload && self.multipart_part_size < 5 * 1024 * 1024 {
            return Err(S3Error::Configuration(
                "multipart part size must be at least 5MB".into(),
            ));
        }

        Ok(())
    }

    /// Build the full object key with prefix.
    pub fn build_key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.key_prefix, key)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_type_content_encoding() {
        assert_eq!(CompressionType::None.content_encoding(), None);
        assert_eq!(CompressionType::Gzip.content_encoding(), Some("gzip"));
        assert_eq!(CompressionType::Zstd.content_encoding(), Some("zstd"));
    }

    #[test]
    fn test_compression_type_extension() {
        assert_eq!(CompressionType::None.extension(), "");
        assert_eq!(CompressionType::Gzip.extension(), ".gz");
        assert_eq!(CompressionType::Zstd.extension(), ".zst");
    }

    #[test]
    fn test_options_default() {
        let opts = S3Options::default();
        assert!(opts.bucket.is_empty());
        assert!(opts.region.is_none());
        assert!(opts.endpoint_url.is_none());
        assert!(!opts.force_path_style);
        assert_eq!(opts.compression, CompressionType::None);
        assert_eq!(opts.max_retry_attempts, 3);
        assert!(opts.enable_checksum);
    }

    #[test]
    fn test_options_new() {
        let opts = S3Options::new("my-bucket");
        assert_eq!(opts.bucket, "my-bucket");
    }

    #[test]
    fn test_options_for_testing() {
        let opts = S3Options::for_testing("test-bucket");
        assert_eq!(opts.bucket, "test-bucket");
        assert_eq!(
            opts.endpoint_url,
            Some("http://localhost:4566".to_string())
        );
        assert!(opts.force_path_style);
        assert_eq!(opts.max_retry_attempts, 1);
    }

    #[test]
    fn test_options_for_minio() {
        let opts = S3Options::for_minio("my-bucket", "http://minio:9000");
        assert_eq!(opts.bucket, "my-bucket");
        assert_eq!(opts.endpoint_url, Some("http://minio:9000".to_string()));
        assert!(opts.force_path_style);
    }

    #[test]
    fn test_options_builder() {
        let opts = S3Options::new("my-bucket")
            .with_region("eu-west-1")
            .with_key_prefix("orleans/")
            .with_compression(CompressionType::Gzip)
            .with_compression_level(9)
            .with_max_retries(5)
            .with_sse();

        assert_eq!(opts.bucket, "my-bucket");
        assert_eq!(opts.region, Some("eu-west-1".to_string()));
        assert_eq!(opts.key_prefix, "orleans/");
        assert_eq!(opts.compression, CompressionType::Gzip);
        assert_eq!(opts.compression_level, 9);
        assert_eq!(opts.max_retry_attempts, 5);
        assert!(opts.enable_sse);
    }

    #[test]
    fn test_options_validate_empty_bucket() {
        let opts = S3Options::default();
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_validate_short_bucket() {
        let opts = S3Options::new("ab");
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_validate_invalid_bucket_chars() {
        let opts = S3Options::new("My_Bucket");
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_validate_valid_bucket() {
        let opts = S3Options::new("my-valid-bucket.name");
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_options_validate_invalid_gzip_level() {
        let opts = S3Options::new("my-bucket")
            .with_compression(CompressionType::Gzip)
            .with_compression_level(10);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_validate_invalid_zstd_level() {
        let opts = S3Options::new("my-bucket")
            .with_compression(CompressionType::Zstd)
            .with_compression_level(25);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_validate_invalid_multipart_size() {
        let opts = S3Options::new("my-bucket").with_multipart_part_size(1024);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_options_build_key_no_prefix() {
        let opts = S3Options::new("my-bucket");
        assert_eq!(opts.build_key("grains/my-grain"), "grains/my-grain");
    }

    #[test]
    fn test_options_build_key_with_prefix() {
        let opts = S3Options::new("my-bucket").with_key_prefix("orleans/");
        assert_eq!(
            opts.build_key("grains/my-grain"),
            "orleans/grains/my-grain"
        );
    }
}
