//! Error types for TLS/security operations.

use std::io;
use thiserror::Error;

/// Result type for security operations.
pub type SecurityResult<T> = Result<T, SecurityError>;

/// Errors that can occur during TLS/security operations.
#[derive(Error, Debug)]
pub enum SecurityError {
    /// TLS handshake failed.
    #[error("TLS handshake failed: {0}")]
    HandshakeFailed(String),

    /// TLS handshake timed out.
    #[error("TLS handshake timed out after {0:?}")]
    HandshakeTimeout(std::time::Duration),

    /// Certificate not found or could not be loaded.
    #[error("certificate not found: {0}")]
    CertificateNotFound(String),

    /// Certificate is invalid or malformed.
    #[error("invalid certificate: {0}")]
    InvalidCertificate(String),

    /// Private key not found or could not be loaded.
    #[error("private key not found: {0}")]
    PrivateKeyNotFound(String),

    /// Private key is invalid or malformed.
    #[error("invalid private key: {0}")]
    InvalidPrivateKey(String),

    /// Certificate and private key do not match.
    #[error("certificate and private key do not match")]
    CertificateKeyMismatch,

    /// Certificate validation failed.
    #[error("certificate validation failed: {0}")]
    CertificateValidationFailed(String),

    /// Certificate has expired.
    #[error("certificate has expired")]
    CertificateExpired,

    /// Certificate is not yet valid.
    #[error("certificate is not yet valid")]
    CertificateNotYetValid,

    /// Certificate chain verification failed.
    #[error("certificate chain verification failed: {0}")]
    CertificateChainVerificationFailed(String),

    /// Remote certificate required but not provided.
    #[error("remote certificate required but not provided")]
    RemoteCertificateRequired,

    /// Remote certificate not trusted.
    #[error("remote certificate not trusted: {0}")]
    RemoteCertificateNotTrusted(String),

    /// TLS protocol version not supported.
    #[error("TLS protocol version not supported: {0}")]
    UnsupportedProtocol(String),

    /// Cipher suite not supported.
    #[error("cipher suite not supported")]
    UnsupportedCipherSuite,

    /// ALPN negotiation failed.
    #[error("ALPN negotiation failed: no common protocol")]
    AlpnNegotiationFailed,

    /// SNI hostname mismatch.
    #[error("SNI hostname mismatch: expected {expected}, got {actual}")]
    SniHostnameMismatch { expected: String, actual: String },

    /// Configuration error.
    #[error("configuration error: {0}")]
    Configuration(String),

    /// TLS not enabled.
    #[error("TLS is not enabled")]
    TlsNotEnabled,

    /// Connection already secured.
    #[error("connection is already secured with TLS")]
    AlreadySecured,

    /// I/O error during TLS operation.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl SecurityError {
    /// Returns true if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            SecurityError::HandshakeTimeout(_) | SecurityError::Io(_)
        )
    }

    /// Returns true if this is a certificate-related error.
    pub fn is_certificate_error(&self) -> bool {
        matches!(
            self,
            SecurityError::CertificateNotFound(_)
                | SecurityError::InvalidCertificate(_)
                | SecurityError::PrivateKeyNotFound(_)
                | SecurityError::InvalidPrivateKey(_)
                | SecurityError::CertificateKeyMismatch
                | SecurityError::CertificateValidationFailed(_)
                | SecurityError::CertificateExpired
                | SecurityError::CertificateNotYetValid
                | SecurityError::CertificateChainVerificationFailed(_)
                | SecurityError::RemoteCertificateRequired
                | SecurityError::RemoteCertificateNotTrusted(_)
        )
    }

    /// Returns true if this is a configuration error.
    pub fn is_configuration_error(&self) -> bool {
        matches!(
            self,
            SecurityError::Configuration(_)
                | SecurityError::UnsupportedProtocol(_)
                | SecurityError::UnsupportedCipherSuite
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = SecurityError::HandshakeFailed("test".to_string());
        assert_eq!(format!("{}", err), "TLS handshake failed: test");

        let err = SecurityError::HandshakeTimeout(std::time::Duration::from_secs(10));
        assert_eq!(format!("{}", err), "TLS handshake timed out after 10s");
    }

    #[test]
    fn test_is_retryable() {
        assert!(SecurityError::HandshakeTimeout(std::time::Duration::from_secs(1)).is_retryable());
        assert!(SecurityError::Io(io::Error::new(io::ErrorKind::TimedOut, "test")).is_retryable());
        assert!(!SecurityError::CertificateExpired.is_retryable());
        assert!(!SecurityError::Configuration("test".to_string()).is_retryable());
    }

    #[test]
    fn test_is_certificate_error() {
        assert!(SecurityError::CertificateNotFound("test".to_string()).is_certificate_error());
        assert!(SecurityError::InvalidCertificate("test".to_string()).is_certificate_error());
        assert!(SecurityError::CertificateExpired.is_certificate_error());
        assert!(SecurityError::RemoteCertificateRequired.is_certificate_error());
        assert!(!SecurityError::HandshakeFailed("test".to_string()).is_certificate_error());
    }

    #[test]
    fn test_is_configuration_error() {
        assert!(SecurityError::Configuration("test".to_string()).is_configuration_error());
        assert!(SecurityError::UnsupportedProtocol("test".to_string()).is_configuration_error());
        assert!(SecurityError::UnsupportedCipherSuite.is_configuration_error());
        assert!(!SecurityError::CertificateExpired.is_configuration_error());
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
        let security_err: SecurityError = io_err.into();
        assert!(matches!(security_err, SecurityError::Io(_)));
    }
}
