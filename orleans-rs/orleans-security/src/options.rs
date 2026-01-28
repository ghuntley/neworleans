//! TLS configuration options.

use std::path::PathBuf;
use std::time::Duration;

/// Mode for handling remote certificates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RemoteCertificateMode {
    /// No remote certificate is requested or required.
    NoCertificate,

    /// Certificate is requested but not required (optional mTLS).
    AllowCertificate,

    /// Certificate is required and must be valid (strict mTLS).
    #[default]
    RequireCertificate,
}

impl RemoteCertificateMode {
    /// Returns true if a certificate is required.
    pub fn is_required(&self) -> bool {
        matches!(self, RemoteCertificateMode::RequireCertificate)
    }

    /// Returns true if a certificate is requested (but may not be required).
    pub fn is_requested(&self) -> bool {
        matches!(
            self,
            RemoteCertificateMode::AllowCertificate | RemoteCertificateMode::RequireCertificate
        )
    }
}

/// Supported TLS protocol versions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TlsProtocol {
    /// TLS 1.2
    Tls12,
    /// TLS 1.3
    Tls13,
}

impl TlsProtocol {
    /// Returns all supported protocols in preference order (newest first).
    pub fn all() -> Vec<TlsProtocol> {
        vec![TlsProtocol::Tls13, TlsProtocol::Tls12]
    }

    /// Returns only TLS 1.3.
    pub fn tls13_only() -> Vec<TlsProtocol> {
        vec![TlsProtocol::Tls13]
    }

    /// Returns only TLS 1.2.
    pub fn tls12_only() -> Vec<TlsProtocol> {
        vec![TlsProtocol::Tls12]
    }
}

/// Certificate source configuration.
#[derive(Debug, Clone)]
pub enum CertificateSource {
    /// Load certificate from PEM file.
    PemFile {
        /// Path to the certificate PEM file.
        cert_path: PathBuf,
        /// Path to the private key PEM file.
        key_path: PathBuf,
    },

    /// Load certificate from PKCS#12/PFX file.
    Pkcs12 {
        /// Path to the PKCS#12 file.
        path: PathBuf,
        /// Password for the PKCS#12 file.
        password: String,
    },

    /// Use in-memory certificate and key (PEM format).
    InMemory {
        /// Certificate chain in PEM format.
        cert_pem: String,
        /// Private key in PEM format.
        key_pem: String,
    },

    /// Generate a self-signed certificate for testing.
    SelfSigned {
        /// Common name for the certificate.
        common_name: String,
        /// Subject alternative names (SANs).
        san_names: Vec<String>,
        /// Validity period in days.
        validity_days: u32,
    },
}

/// TLS configuration options.
#[derive(Debug, Clone)]
pub struct TlsOptions {
    /// Whether TLS is enabled.
    pub enabled: bool,

    /// Local certificate for authentication.
    pub local_certificate: Option<CertificateSource>,

    /// Mode for remote certificate validation on server side.
    pub remote_certificate_mode: RemoteCertificateMode,

    /// Mode for client certificate requirement (server-side setting).
    pub client_certificate_mode: RemoteCertificateMode,

    /// Allowed TLS protocols.
    pub protocols: Vec<TlsProtocol>,

    /// Whether to check certificate revocation (CRL/OCSP).
    pub check_certificate_revocation: bool,

    /// Custom CA certificates for verification (in addition to system roots).
    pub custom_ca_certificates: Vec<PathBuf>,

    /// Whether to include system root CA certificates.
    pub include_system_roots: bool,

    /// Handshake timeout.
    pub handshake_timeout: Duration,

    /// Whether to allow any remote certificate (skip validation).
    /// WARNING: This is insecure and should only be used for testing.
    pub allow_any_remote_certificate: bool,

    /// ALPN protocols to negotiate.
    pub alpn_protocols: Vec<String>,

    /// SNI hostname override (client-side).
    pub sni_hostname: Option<String>,
}

impl Default for TlsOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            local_certificate: None,
            remote_certificate_mode: RemoteCertificateMode::RequireCertificate,
            client_certificate_mode: RemoteCertificateMode::AllowCertificate,
            protocols: TlsProtocol::all(),
            check_certificate_revocation: false,
            custom_ca_certificates: Vec::new(),
            include_system_roots: true,
            handshake_timeout: Duration::from_secs(10),
            allow_any_remote_certificate: false,
            alpn_protocols: vec!["orleans1".to_string()],
            sni_hostname: None,
        }
    }
}

impl TlsOptions {
    /// Creates new TLS options with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates TLS options for development/testing (insecure).
    pub fn for_testing() -> Self {
        Self {
            enabled: true,
            local_certificate: Some(CertificateSource::SelfSigned {
                common_name: "localhost".to_string(),
                san_names: vec!["localhost".to_string(), "127.0.0.1".to_string()],
                validity_days: 1,
            }),
            remote_certificate_mode: RemoteCertificateMode::AllowCertificate,
            client_certificate_mode: RemoteCertificateMode::AllowCertificate,
            protocols: TlsProtocol::all(),
            check_certificate_revocation: false,
            custom_ca_certificates: Vec::new(),
            include_system_roots: false,
            handshake_timeout: Duration::from_secs(5),
            allow_any_remote_certificate: true,
            alpn_protocols: vec!["orleans1".to_string()],
            sni_hostname: None,
        }
    }

    /// Creates TLS options for production with mTLS enabled.
    pub fn production_mtls() -> Self {
        Self {
            enabled: true,
            local_certificate: None, // Must be set by user
            remote_certificate_mode: RemoteCertificateMode::RequireCertificate,
            client_certificate_mode: RemoteCertificateMode::RequireCertificate,
            protocols: vec![TlsProtocol::Tls13], // TLS 1.3 only for production
            check_certificate_revocation: true,
            custom_ca_certificates: Vec::new(),
            include_system_roots: true,
            handshake_timeout: Duration::from_secs(10),
            allow_any_remote_certificate: false,
            alpn_protocols: vec!["orleans1".to_string()],
            sni_hostname: None,
        }
    }

    /// Creates TLS options for production without client certificates.
    pub fn production_server_only() -> Self {
        Self {
            enabled: true,
            local_certificate: None, // Must be set by user
            remote_certificate_mode: RemoteCertificateMode::RequireCertificate,
            client_certificate_mode: RemoteCertificateMode::NoCertificate,
            protocols: vec![TlsProtocol::Tls13],
            check_certificate_revocation: true,
            custom_ca_certificates: Vec::new(),
            include_system_roots: true,
            handshake_timeout: Duration::from_secs(10),
            allow_any_remote_certificate: false,
            alpn_protocols: vec!["orleans1".to_string()],
            sni_hostname: None,
        }
    }

    /// Builder method: enable TLS.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Builder method: set local certificate.
    pub fn with_certificate(mut self, source: CertificateSource) -> Self {
        self.local_certificate = Some(source);
        self
    }

    /// Builder method: set local certificate from PEM files.
    pub fn with_pem_certificate(mut self, cert_path: PathBuf, key_path: PathBuf) -> Self {
        self.local_certificate = Some(CertificateSource::PemFile { cert_path, key_path });
        self
    }

    /// Builder method: set remote certificate mode.
    pub fn with_remote_certificate_mode(mut self, mode: RemoteCertificateMode) -> Self {
        self.remote_certificate_mode = mode;
        self
    }

    /// Builder method: set client certificate mode.
    pub fn with_client_certificate_mode(mut self, mode: RemoteCertificateMode) -> Self {
        self.client_certificate_mode = mode;
        self
    }

    /// Builder method: set allowed protocols.
    pub fn with_protocols(mut self, protocols: Vec<TlsProtocol>) -> Self {
        self.protocols = protocols;
        self
    }

    /// Builder method: set handshake timeout.
    pub fn with_handshake_timeout(mut self, timeout: Duration) -> Self {
        self.handshake_timeout = timeout;
        self
    }

    /// Builder method: add custom CA certificate path.
    pub fn with_custom_ca(mut self, path: PathBuf) -> Self {
        self.custom_ca_certificates.push(path);
        self
    }

    /// Builder method: set whether to include system roots.
    pub fn with_system_roots(mut self, include: bool) -> Self {
        self.include_system_roots = include;
        self
    }

    /// Builder method: allow any remote certificate (INSECURE - for testing only).
    pub fn with_allow_any_certificate(mut self) -> Self {
        self.allow_any_remote_certificate = true;
        self
    }

    /// Builder method: set SNI hostname.
    pub fn with_sni_hostname(mut self, hostname: String) -> Self {
        self.sni_hostname = Some(hostname);
        self
    }

    /// Builder method: set ALPN protocols.
    pub fn with_alpn_protocols(mut self, protocols: Vec<String>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Validates the TLS options.
    pub fn validate(&self) -> Result<(), String> {
        if !self.enabled {
            return Ok(());
        }

        if self.protocols.is_empty() {
            return Err("at least one TLS protocol must be enabled".to_string());
        }

        // For server mode, we need a local certificate
        // (client mode can work without one if mTLS is not required)

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_remote_certificate_mode_default() {
        let mode = RemoteCertificateMode::default();
        assert_eq!(mode, RemoteCertificateMode::RequireCertificate);
    }

    #[test]
    fn test_remote_certificate_mode_is_required() {
        assert!(RemoteCertificateMode::RequireCertificate.is_required());
        assert!(!RemoteCertificateMode::AllowCertificate.is_required());
        assert!(!RemoteCertificateMode::NoCertificate.is_required());
    }

    #[test]
    fn test_remote_certificate_mode_is_requested() {
        assert!(RemoteCertificateMode::RequireCertificate.is_requested());
        assert!(RemoteCertificateMode::AllowCertificate.is_requested());
        assert!(!RemoteCertificateMode::NoCertificate.is_requested());
    }

    #[test]
    fn test_tls_protocol_all() {
        let protocols = TlsProtocol::all();
        assert_eq!(protocols.len(), 2);
        assert_eq!(protocols[0], TlsProtocol::Tls13); // Newest first
        assert_eq!(protocols[1], TlsProtocol::Tls12);
    }

    #[test]
    fn test_tls_options_default() {
        let options = TlsOptions::default();
        assert!(!options.enabled);
        assert!(options.local_certificate.is_none());
        assert_eq!(
            options.remote_certificate_mode,
            RemoteCertificateMode::RequireCertificate
        );
        assert_eq!(
            options.client_certificate_mode,
            RemoteCertificateMode::AllowCertificate
        );
        assert_eq!(options.protocols.len(), 2);
        assert!(!options.check_certificate_revocation);
        assert!(options.include_system_roots);
        assert_eq!(options.handshake_timeout, Duration::from_secs(10));
        assert!(!options.allow_any_remote_certificate);
        assert_eq!(options.alpn_protocols, vec!["orleans1".to_string()]);
    }

    #[test]
    fn test_tls_options_for_testing() {
        let options = TlsOptions::for_testing();
        assert!(options.enabled);
        assert!(options.local_certificate.is_some());
        assert!(options.allow_any_remote_certificate);
        assert_eq!(options.handshake_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_tls_options_production_mtls() {
        let options = TlsOptions::production_mtls();
        assert!(options.enabled);
        assert_eq!(
            options.remote_certificate_mode,
            RemoteCertificateMode::RequireCertificate
        );
        assert_eq!(
            options.client_certificate_mode,
            RemoteCertificateMode::RequireCertificate
        );
        assert_eq!(options.protocols, vec![TlsProtocol::Tls13]);
        assert!(options.check_certificate_revocation);
        assert!(!options.allow_any_remote_certificate);
    }

    #[test]
    fn test_tls_options_builder() {
        let options = TlsOptions::new()
            .with_enabled(true)
            .with_remote_certificate_mode(RemoteCertificateMode::AllowCertificate)
            .with_protocols(vec![TlsProtocol::Tls13])
            .with_handshake_timeout(Duration::from_secs(30))
            .with_sni_hostname("example.com".to_string());

        assert!(options.enabled);
        assert_eq!(
            options.remote_certificate_mode,
            RemoteCertificateMode::AllowCertificate
        );
        assert_eq!(options.protocols, vec![TlsProtocol::Tls13]);
        assert_eq!(options.handshake_timeout, Duration::from_secs(30));
        assert_eq!(options.sni_hostname, Some("example.com".to_string()));
    }

    #[test]
    fn test_tls_options_validation() {
        let options = TlsOptions::default();
        assert!(options.validate().is_ok());

        let mut options = TlsOptions::new().with_enabled(true);
        options.protocols.clear();
        assert!(options.validate().is_err());
    }

    #[test]
    fn test_certificate_source_variants() {
        let pem = CertificateSource::PemFile {
            cert_path: PathBuf::from("/path/to/cert.pem"),
            key_path: PathBuf::from("/path/to/key.pem"),
        };
        assert!(matches!(pem, CertificateSource::PemFile { .. }));

        let pkcs12 = CertificateSource::Pkcs12 {
            path: PathBuf::from("/path/to/cert.p12"),
            password: "secret".to_string(),
        };
        assert!(matches!(pkcs12, CertificateSource::Pkcs12 { .. }));

        let self_signed = CertificateSource::SelfSigned {
            common_name: "test".to_string(),
            san_names: vec!["localhost".to_string()],
            validity_days: 30,
        };
        assert!(matches!(self_signed, CertificateSource::SelfSigned { .. }));
    }
}
