//! Orleans Security - TLS/Security support for Orleans Rust port
//!
//! This crate provides TLS encryption for Orleans cluster communication,
//! including support for mutual TLS (mTLS), certificate management,
//! and secure connection handling.
//!
//! # Overview
//!
//! The security module provides:
//! - TLS configuration options for server and client
//! - Certificate loading from files or in-memory
//! - Self-signed certificate generation for testing
//! - Secure connection acceptors and connectors
//! - mTLS (mutual TLS) support
//!
//! # Quick Start
//!
//! ## For Development/Testing
//!
//! ```rust,no_run
//! use orleans_security::{TlsOptions, SecureAcceptor, SecureConnector};
//!
//! // Create options with self-signed certificate (for testing only!)
//! let options = TlsOptions::for_testing();
//!
//! // Create server acceptor
//! let acceptor = SecureAcceptor::new(&options).expect("failed to create acceptor");
//!
//! // Create client connector
//! let connector = SecureConnector::new(&options).expect("failed to create connector");
//! ```
//!
//! ## For Production
//!
//! ```rust,no_run
//! use orleans_security::{TlsOptions, CertificateSource, RemoteCertificateMode};
//! use std::path::PathBuf;
//!
//! // Configure TLS with real certificates
//! let options = TlsOptions::production_mtls()
//!     .with_certificate(CertificateSource::PemFile {
//!         cert_path: PathBuf::from("/etc/orleans/server.crt"),
//!         key_path: PathBuf::from("/etc/orleans/server.key"),
//!     })
//!     .with_custom_ca(PathBuf::from("/etc/orleans/ca.crt"));
//! ```
//!
//! # Certificate Modes
//!
//! The [`RemoteCertificateMode`] enum controls how peer certificates are handled:
//!
//! - `NoCertificate` - Don't request client certificates
//! - `AllowCertificate` - Request but don't require client certificates
//! - `RequireCertificate` - Require valid client certificates (mTLS)
//!
//! # Security Considerations
//!
//! - **Never** use `allow_any_remote_certificate` in production
//! - Always use proper CA-signed certificates in production
//! - Use TLS 1.3 when possible (configured by default)
//! - Enable certificate revocation checking for high-security environments
//!
//! # ALPN Protocol
//!
//! Orleans uses a custom ALPN protocol identifier `orleans1` to ensure
//! that only Orleans connections are accepted.

pub mod certificate;
pub mod config;
pub mod error;
pub mod options;
pub mod stream;

// Re-export main types for convenience
pub use certificate::{load_certificate, generate_self_signed_certificate, LoadedCertificate};
pub use config::{build_client_config, build_server_config, ORLEANS_ALPN_PROTOCOL};
pub use error::{SecurityError, SecurityResult};
pub use options::{CertificateSource, RemoteCertificateMode, TlsOptions, TlsProtocol};
pub use stream::{SecureAcceptor, SecureConnector, TlsConnectionInfo, TlsStream};

#[cfg(test)]
mod integration_tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn start_test_server(
        acceptor: SecureAcceptor,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = tokio::spawn(async move {
            let (tcp_stream, _peer_addr) = listener.accept().await.unwrap();
            let mut tls_stream = acceptor.accept(tcp_stream).await.unwrap();

            // Echo back any received data
            let mut buf = [0u8; 1024];
            let n = tls_stream.read(&mut buf).await.unwrap();
            tls_stream.write_all(&buf[..n]).await.unwrap();
            tls_stream.shutdown().await.unwrap();
        });

        (addr, handle)
    }

    #[tokio::test]
    async fn test_tls_echo_roundtrip() {
        // Create TLS options for testing
        let options = TlsOptions::for_testing();

        // Create acceptor and connector
        let acceptor = SecureAcceptor::new(&options).unwrap();
        let connector = SecureConnector::new(&options).unwrap();

        // Start server
        let (addr, server_handle) = start_test_server(acceptor).await;

        // Connect client
        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let mut tls_stream = connector
            .connect(tcp_stream, "localhost")
            .await
            .unwrap();

        // Send test data
        let test_data = b"Hello, Orleans TLS!";
        tls_stream.write_all(test_data).await.unwrap();

        // Read response
        let mut response = vec![0u8; test_data.len()];
        tls_stream.read_exact(&mut response).await.unwrap();

        // Verify
        assert_eq!(&response, test_data);

        // Cleanup
        tls_stream.shutdown().await.unwrap();
        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_tls_connection_info() {
        let options = TlsOptions::for_testing();
        let acceptor = SecureAcceptor::new(&options).unwrap();
        let connector = SecureConnector::new(&options).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let tls_stream = acceptor.accept(tcp_stream).await.unwrap();

            // Get connection info
            let info = TlsConnectionInfo::from_stream(&tls_stream);
            assert!(info.protocol_version.is_some());
            assert!(!info.is_client);
        });

        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let tls_stream = connector
            .connect(tcp_stream, "localhost")
            .await
            .unwrap();

        let info = TlsConnectionInfo::from_stream(&tls_stream);
        assert!(info.protocol_version.is_some());
        assert!(info.is_client);

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_tls_stream_is_client_server() {
        let options = TlsOptions::for_testing();
        let acceptor = SecureAcceptor::new(&options).unwrap();
        let connector = SecureConnector::new(&options).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let tls_stream = acceptor.accept(tcp_stream).await.unwrap();
            assert!(tls_stream.is_server());
            assert!(!tls_stream.is_client());
        });

        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let tls_stream = connector
            .connect(tcp_stream, "localhost")
            .await
            .unwrap();

        assert!(tls_stream.is_client());
        assert!(!tls_stream.is_server());

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_tls_alpn_negotiation() {
        let options = TlsOptions::for_testing();
        let acceptor = SecureAcceptor::new(&options).unwrap();
        let connector = SecureConnector::new(&options).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let tls_stream = acceptor.accept(tcp_stream).await.unwrap();
            let alpn = tls_stream.alpn_protocol();
            assert_eq!(alpn, Some(b"orleans1".as_slice()));
        });

        let tcp_stream = TcpStream::connect(addr).await.unwrap();
        let tls_stream = connector
            .connect(tcp_stream, "localhost")
            .await
            .unwrap();

        let alpn = tls_stream.alpn_protocol();
        assert_eq!(alpn, Some(b"orleans1".as_slice()));

        server_handle.await.unwrap();
    }

    #[test]
    fn test_options_presets() {
        // Test all preset configurations can be created
        let _ = TlsOptions::new();
        let _ = TlsOptions::for_testing();
        let _ = TlsOptions::production_mtls();
        let _ = TlsOptions::production_server_only();
    }

    #[test]
    fn test_certificate_source_creation() {
        let pem = CertificateSource::PemFile {
            cert_path: "/tmp/cert.pem".into(),
            key_path: "/tmp/key.pem".into(),
        };
        assert!(matches!(pem, CertificateSource::PemFile { .. }));

        let self_signed = CertificateSource::SelfSigned {
            common_name: "test".to_string(),
            san_names: vec!["localhost".to_string()],
            validity_days: 30,
        };
        assert!(matches!(self_signed, CertificateSource::SelfSigned { .. }));
    }

    #[test]
    fn test_remote_certificate_mode() {
        assert!(RemoteCertificateMode::RequireCertificate.is_required());
        assert!(!RemoteCertificateMode::AllowCertificate.is_required());
        assert!(!RemoteCertificateMode::NoCertificate.is_required());

        assert!(RemoteCertificateMode::RequireCertificate.is_requested());
        assert!(RemoteCertificateMode::AllowCertificate.is_requested());
        assert!(!RemoteCertificateMode::NoCertificate.is_requested());
    }
}
