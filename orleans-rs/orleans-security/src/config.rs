//! TLS configuration builders for server and client.

use crate::certificate::{load_ca_certificates, load_certificate};
use crate::error::{SecurityError, SecurityResult};
use crate::options::{RemoteCertificateMode, TlsOptions, TlsProtocol};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore, ServerConfig, SignatureScheme};
use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

/// Orleans ALPN protocol identifier.
pub const ORLEANS_ALPN_PROTOCOL: &[u8] = b"orleans1";

/// Builds a rustls ServerConfig from TlsOptions.
#[instrument(skip(options))]
pub fn build_server_config(options: &TlsOptions) -> SecurityResult<ServerConfig> {
    info!("building TLS server configuration");

    // Load server certificate
    let cert_source = options.local_certificate.as_ref().ok_or_else(|| {
        SecurityError::Configuration("server certificate is required".to_string())
    })?;

    let loaded_cert = load_certificate(cert_source)?;

    // Verify certificate has server auth EKU
    if !loaded_cert.has_server_auth {
        warn!("server certificate does not have serverAuth EKU");
    }

    // Build the server config
    let builder = ServerConfig::builder();

    // Configure client certificate verification based on mode
    let config = match options.client_certificate_mode {
        RemoteCertificateMode::NoCertificate => {
            debug!("client certificates not requested");
            builder
                .with_no_client_auth()
                .with_single_cert(
                    loaded_cert.certificate_chain.clone(),
                    loaded_cert.private_key.clone_key(),
                )
                .map_err(|e| SecurityError::Configuration(format!("invalid certificate: {}", e)))?
        }
        RemoteCertificateMode::AllowCertificate => {
            debug!("client certificates optional");
            let verifier = build_client_cert_verifier(options, false)?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    loaded_cert.certificate_chain.clone(),
                    loaded_cert.private_key.clone_key(),
                )
                .map_err(|e| SecurityError::Configuration(format!("invalid certificate: {}", e)))?
        }
        RemoteCertificateMode::RequireCertificate => {
            debug!("client certificates required");
            let verifier = build_client_cert_verifier(options, true)?;
            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(
                    loaded_cert.certificate_chain.clone(),
                    loaded_cert.private_key.clone_key(),
                )
                .map_err(|e| SecurityError::Configuration(format!("invalid certificate: {}", e)))?
        }
    };

    let mut config = config;

    // Set ALPN protocols
    if !options.alpn_protocols.is_empty() {
        config.alpn_protocols = options
            .alpn_protocols
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
    }

    info!(
        client_cert_mode = ?options.client_certificate_mode,
        alpn_protocols = ?options.alpn_protocols,
        "TLS server configuration built"
    );

    Ok(config)
}

/// Builds a rustls ClientConfig from TlsOptions.
#[instrument(skip(options))]
pub fn build_client_config(options: &TlsOptions) -> SecurityResult<ClientConfig> {
    info!("building TLS client configuration");

    let builder = ClientConfig::builder();

    // Build root certificate store
    let root_store = build_root_cert_store(options)?;

    // Configure server certificate verification
    let config = if options.allow_any_remote_certificate {
        warn!("allowing any server certificate - NOT SECURE FOR PRODUCTION");
        let verifier = Arc::new(InsecureServerCertVerifier);
        builder.dangerous().with_custom_certificate_verifier(verifier)
    } else {
        builder.with_root_certificates(root_store)
    };

    // Configure client certificate if provided
    let config = if let Some(cert_source) = &options.local_certificate {
        let loaded_cert = load_certificate(cert_source)?;

        // Verify certificate has client auth EKU
        if !loaded_cert.has_client_auth {
            warn!("client certificate does not have clientAuth EKU");
        }

        config
            .with_client_auth_cert(
                loaded_cert.certificate_chain.clone(),
                loaded_cert.private_key.clone_key(),
            )
            .map_err(|e| SecurityError::Configuration(format!("invalid client certificate: {}", e)))?
    } else {
        config.with_no_client_auth()
    };

    let mut config = config;

    // Set ALPN protocols
    if !options.alpn_protocols.is_empty() {
        config.alpn_protocols = options
            .alpn_protocols
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();
    }

    info!(
        has_client_cert = options.local_certificate.is_some(),
        allow_any_cert = options.allow_any_remote_certificate,
        alpn_protocols = ?options.alpn_protocols,
        "TLS client configuration built"
    );

    Ok(config)
}

/// Builds the root certificate store for server certificate verification.
fn build_root_cert_store(options: &TlsOptions) -> SecurityResult<RootCertStore> {
    let mut root_store = RootCertStore::empty();

    // Add system roots if enabled
    if options.include_system_roots {
        debug!("adding system root certificates");
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    // Add custom CA certificates
    for ca_path in &options.custom_ca_certificates {
        debug!(path = %ca_path.display(), "loading custom CA certificates");
        let ca_certs = load_ca_certificates(ca_path)?;
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| SecurityError::InvalidCertificate(format!("invalid CA cert: {}", e)))?;
        }
    }

    if root_store.is_empty() {
        warn!("root certificate store is empty - server verification may fail");
    }

    Ok(root_store)
}

/// Builds a client certificate verifier for server-side verification.
fn build_client_cert_verifier(
    options: &TlsOptions,
    required: bool,
) -> SecurityResult<Arc<dyn ClientCertVerifier>> {
    if options.allow_any_remote_certificate {
        warn!("allowing any client certificate - NOT SECURE FOR PRODUCTION");
        return Ok(Arc::new(InsecureClientCertVerifier { required }));
    }

    // Build root store for client certificate verification
    let root_store = build_root_cert_store(options)?;

    if required {
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| {
                SecurityError::Configuration(format!("failed to build client verifier: {}", e))
            })?;
        Ok(verifier)
    } else {
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .allow_unauthenticated()
            .build()
            .map_err(|e| {
                SecurityError::Configuration(format!("failed to build client verifier: {}", e))
            })?;
        Ok(verifier)
    }
}

/// Returns the supported TLS versions based on options.
pub fn get_tls_versions(protocols: &[TlsProtocol]) -> Vec<&'static rustls::SupportedProtocolVersion> {
    let mut versions = Vec::new();
    for protocol in protocols {
        match protocol {
            TlsProtocol::Tls12 => versions.push(&rustls::version::TLS12),
            TlsProtocol::Tls13 => versions.push(&rustls::version::TLS13),
        }
    }
    versions
}

/// Insecure server certificate verifier that accepts any certificate.
/// WARNING: Only for testing!
#[derive(Debug)]
struct InsecureServerCertVerifier;

impl ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        warn!("accepting server certificate without verification - INSECURE");
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

/// Insecure client certificate verifier that accepts any certificate.
/// WARNING: Only for testing!
#[derive(Debug)]
struct InsecureClientCertVerifier {
    required: bool,
}

impl ClientCertVerifier for InsecureClientCertVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        warn!("accepting client certificate without verification - INSECURE");
        Ok(ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }

    fn client_auth_mandatory(&self) -> bool {
        self.required
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_options() -> TlsOptions {
        TlsOptions::for_testing()
    }

    #[test]
    fn test_build_client_config_with_self_signed() {
        let options = create_test_options();
        let result = build_client_config(&options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_server_config_with_self_signed() {
        let options = create_test_options();
        let result = build_server_config(&options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_server_config_no_certificate() {
        let options = TlsOptions::new().with_enabled(true);
        let result = build_server_config(&options);
        assert!(matches!(result, Err(SecurityError::Configuration(_))));
    }

    #[test]
    fn test_build_client_config_with_system_roots() {
        let options = TlsOptions::new()
            .with_enabled(true)
            .with_system_roots(true);
        let result = build_client_config(&options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_tls_versions() {
        let versions = get_tls_versions(&[TlsProtocol::Tls13, TlsProtocol::Tls12]);
        assert_eq!(versions.len(), 2);

        let versions = get_tls_versions(&[TlsProtocol::Tls13]);
        assert_eq!(versions.len(), 1);
    }

    #[test]
    fn test_orleans_alpn_protocol() {
        assert_eq!(ORLEANS_ALPN_PROTOCOL, b"orleans1");
    }

    #[test]
    fn test_build_server_config_no_client_auth() {
        let mut options = create_test_options();
        options.client_certificate_mode = RemoteCertificateMode::NoCertificate;
        let result = build_server_config(&options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_server_config_require_client_auth() {
        let mut options = create_test_options();
        options.client_certificate_mode = RemoteCertificateMode::RequireCertificate;
        let result = build_server_config(&options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insecure_verifier_supported_schemes() {
        let verifier = InsecureServerCertVerifier;
        let schemes = verifier.supported_verify_schemes();
        assert!(!schemes.is_empty());
        assert!(schemes.contains(&SignatureScheme::RSA_PKCS1_SHA256));
    }
}
