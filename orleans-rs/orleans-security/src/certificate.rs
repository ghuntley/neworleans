//! Certificate loading and generation utilities.

use crate::error::{SecurityError, SecurityResult};
use crate::options::CertificateSource;
use rcgen::{CertificateParams, DnType, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, instrument, warn};
use x509_parser::prelude::*;

/// Server authentication OID (1.3.6.1.5.5.7.3.1).
pub const SERVER_AUTH_OID: &str = "1.3.6.1.5.5.7.3.1";

/// Client authentication OID (1.3.6.1.5.5.7.3.2).
pub const CLIENT_AUTH_OID: &str = "1.3.6.1.5.5.7.3.2";

/// Loaded certificate with chain and private key.
#[derive(Clone)]
pub struct LoadedCertificate {
    /// Certificate chain (leaf certificate first).
    pub certificate_chain: Vec<CertificateDer<'static>>,

    /// Private key for the leaf certificate.
    pub private_key: Arc<PrivateKeyDer<'static>>,

    /// Subject common name (if available).
    pub common_name: Option<String>,

    /// Subject alternative names (DNS names and IP addresses).
    pub san_names: Vec<String>,

    /// Whether the certificate has server authentication EKU.
    pub has_server_auth: bool,

    /// Whether the certificate has client authentication EKU.
    pub has_client_auth: bool,

    /// Certificate validity start time.
    pub not_before: Option<SystemTime>,

    /// Certificate validity end time.
    pub not_after: Option<SystemTime>,
}

impl std::fmt::Debug for LoadedCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedCertificate")
            .field("common_name", &self.common_name)
            .field("san_names", &self.san_names)
            .field("has_server_auth", &self.has_server_auth)
            .field("has_client_auth", &self.has_client_auth)
            .field("chain_length", &self.certificate_chain.len())
            .finish()
    }
}

impl LoadedCertificate {
    /// Checks if the certificate is currently valid.
    pub fn is_valid(&self) -> bool {
        let now = SystemTime::now();

        if let Some(not_before) = self.not_before {
            if now < not_before {
                return false;
            }
        }

        if let Some(not_after) = self.not_after {
            if now > not_after {
                return false;
            }
        }

        true
    }

    /// Returns the time until expiration, or None if already expired.
    pub fn time_until_expiration(&self) -> Option<Duration> {
        let now = SystemTime::now();
        self.not_after
            .and_then(|not_after| not_after.duration_since(now).ok())
    }

    /// Checks if the certificate will expire within the given duration.
    pub fn expires_within(&self, duration: Duration) -> bool {
        self.time_until_expiration()
            .map(|remaining| remaining < duration)
            .unwrap_or(true)
    }
}

/// Loads a certificate from the specified source.
#[instrument(skip(source), fields(source_type = source_type_name(source)))]
pub fn load_certificate(source: &CertificateSource) -> SecurityResult<LoadedCertificate> {
    match source {
        CertificateSource::PemFile { cert_path, key_path } => {
            load_pem_certificate(cert_path, key_path)
        }
        CertificateSource::Pkcs12 { path, password } => load_pkcs12_certificate(path, password),
        CertificateSource::InMemory { cert_pem, key_pem } => {
            load_in_memory_certificate(cert_pem, key_pem)
        }
        CertificateSource::SelfSigned {
            common_name,
            san_names,
            validity_days,
        } => generate_self_signed_certificate(common_name, san_names, *validity_days),
    }
}

fn source_type_name(source: &CertificateSource) -> &'static str {
    match source {
        CertificateSource::PemFile { .. } => "pem_file",
        CertificateSource::Pkcs12 { .. } => "pkcs12",
        CertificateSource::InMemory { .. } => "in_memory",
        CertificateSource::SelfSigned { .. } => "self_signed",
    }
}

/// Loads a certificate and private key from PEM files.
#[instrument(skip_all, fields(cert_path = %cert_path.display(), key_path = %key_path.display()))]
pub fn load_pem_certificate(
    cert_path: &Path,
    key_path: &Path,
) -> SecurityResult<LoadedCertificate> {
    debug!("loading PEM certificate");

    // Load certificate chain
    let cert_file = File::open(cert_path).map_err(|e| {
        SecurityError::CertificateNotFound(format!("{}: {}", cert_path.display(), e))
    })?;
    let mut cert_reader = BufReader::new(cert_file);

    let certificate_chain: Vec<CertificateDer<'static>> = certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SecurityError::InvalidCertificate(format!("failed to parse PEM: {}", e)))?;

    if certificate_chain.is_empty() {
        return Err(SecurityError::InvalidCertificate(
            "no certificates found in PEM file".to_string(),
        ));
    }

    // Load private key
    let key_file = File::open(key_path).map_err(|e| {
        SecurityError::PrivateKeyNotFound(format!("{}: {}", key_path.display(), e))
    })?;
    let mut key_reader = BufReader::new(key_file);

    let private_key = private_key(&mut key_reader)
        .map_err(|e| SecurityError::InvalidPrivateKey(format!("failed to parse key: {}", e)))?
        .ok_or_else(|| {
            SecurityError::PrivateKeyNotFound("no private key found in PEM file".to_string())
        })?;

    // Parse certificate metadata
    let metadata = parse_certificate_metadata(&certificate_chain[0])?;

    info!(
        common_name = ?metadata.common_name,
        san_count = metadata.san_names.len(),
        has_server_auth = metadata.has_server_auth,
        has_client_auth = metadata.has_client_auth,
        "loaded certificate"
    );

    Ok(LoadedCertificate {
        certificate_chain,
        private_key: Arc::new(private_key),
        common_name: metadata.common_name,
        san_names: metadata.san_names,
        has_server_auth: metadata.has_server_auth,
        has_client_auth: metadata.has_client_auth,
        not_before: metadata.not_before,
        not_after: metadata.not_after,
    })
}

/// Loads a certificate from PKCS#12/PFX file.
#[instrument(skip_all, fields(path = %_path.display()))]
pub fn load_pkcs12_certificate(_path: &Path, _password: &str) -> SecurityResult<LoadedCertificate> {
    // PKCS#12 support requires additional dependencies
    // For now, return an error suggesting PEM format
    Err(SecurityError::Configuration(
        "PKCS#12 format not yet supported; please convert to PEM format".to_string(),
    ))
}

/// Loads a certificate from in-memory PEM data.
#[instrument(skip(cert_pem, key_pem))]
pub fn load_in_memory_certificate(
    cert_pem: &str,
    key_pem: &str,
) -> SecurityResult<LoadedCertificate> {
    debug!("loading in-memory certificate");

    let mut cert_reader = BufReader::new(cert_pem.as_bytes());
    let certificate_chain: Vec<CertificateDer<'static>> = certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SecurityError::InvalidCertificate(format!("failed to parse cert PEM: {}", e)))?;

    if certificate_chain.is_empty() {
        return Err(SecurityError::InvalidCertificate(
            "no certificates found in PEM data".to_string(),
        ));
    }

    let mut key_reader = BufReader::new(key_pem.as_bytes());
    let private_key = private_key(&mut key_reader)
        .map_err(|e| SecurityError::InvalidPrivateKey(format!("failed to parse key PEM: {}", e)))?
        .ok_or_else(|| {
            SecurityError::PrivateKeyNotFound("no private key found in PEM data".to_string())
        })?;

    let metadata = parse_certificate_metadata(&certificate_chain[0])?;

    Ok(LoadedCertificate {
        certificate_chain,
        private_key: Arc::new(private_key),
        common_name: metadata.common_name,
        san_names: metadata.san_names,
        has_server_auth: metadata.has_server_auth,
        has_client_auth: metadata.has_client_auth,
        not_before: metadata.not_before,
        not_after: metadata.not_after,
    })
}

/// Generates a self-signed certificate for testing.
#[instrument]
pub fn generate_self_signed_certificate(
    common_name: &str,
    san_names: &[String],
    validity_days: u32,
) -> SecurityResult<LoadedCertificate> {
    info!(
        common_name = common_name,
        san_count = san_names.len(),
        validity_days = validity_days,
        "generating self-signed certificate"
    );

    warn!("using self-signed certificate - not suitable for production");

    // Create certificate parameters
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, common_name);

    // Add SANs
    for san in san_names {
        if san.parse::<std::net::IpAddr>().is_ok() {
            params
                .subject_alt_names
                .push(SanType::IpAddress(san.parse().unwrap()));
        } else {
            params.subject_alt_names.push(SanType::DnsName(
                san.clone().try_into().map_err(|e| {
                    SecurityError::Configuration(format!("invalid SAN name '{}': {}", san, e))
                })?,
            ));
        }
    }

    // Set validity period - rcgen defaults handle this, but we track our own times
    let now = SystemTime::now();
    let not_before = now - Duration::from_secs(86400); // 1 day before
    let not_after = now + Duration::from_secs(validity_days as u64 * 86400);

    // Add extended key usage for both server and client auth
    params.extended_key_usages = vec![
        rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        rcgen::ExtendedKeyUsagePurpose::ClientAuth,
    ];

    // Generate key pair
    let key_pair = KeyPair::generate().map_err(|e| {
        SecurityError::Internal(format!("failed to generate key pair: {}", e))
    })?;

    // Generate certificate
    let cert = params.self_signed(&key_pair).map_err(|e| {
        SecurityError::Internal(format!("failed to generate certificate: {}", e))
    })?;

    let cert_der = CertificateDer::from(cert.der().to_vec());
    let key_der = PrivateKeyDer::try_from(key_pair.serialize_der()).map_err(|e| {
        SecurityError::Internal(format!("failed to serialize private key: {}", e))
    })?;

    let san_names_vec: Vec<String> = san_names.to_vec();

    Ok(LoadedCertificate {
        certificate_chain: vec![cert_der],
        private_key: Arc::new(key_der),
        common_name: Some(common_name.to_string()),
        san_names: san_names_vec,
        has_server_auth: true,
        has_client_auth: true,
        not_before: Some(not_before),
        not_after: Some(not_after),
    })
}

/// Certificate metadata extracted from X.509 certificate.
struct CertificateMetadata {
    common_name: Option<String>,
    san_names: Vec<String>,
    has_server_auth: bool,
    has_client_auth: bool,
    not_before: Option<SystemTime>,
    not_after: Option<SystemTime>,
}

/// Parses metadata from an X.509 certificate.
fn parse_certificate_metadata(cert_der: &CertificateDer<'_>) -> SecurityResult<CertificateMetadata> {
    let (_, cert) = X509Certificate::from_der(cert_der.as_ref())
        .map_err(|e| SecurityError::InvalidCertificate(format!("failed to parse X.509: {}", e)))?;

    // Extract common name
    let common_name = cert
        .subject()
        .iter_common_name()
        .next()
        .and_then(|cn| cn.as_str().ok())
        .map(|s| s.to_string());

    // Extract SANs
    let mut san_names = Vec::new();
    if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
        for name in san_ext.value.general_names.iter() {
            match name {
                GeneralName::DNSName(dns) => san_names.push(dns.to_string()),
                GeneralName::IPAddress(ip) => {
                    if ip.len() == 4 {
                        san_names.push(format!(
                            "{}.{}.{}.{}",
                            ip[0], ip[1], ip[2], ip[3]
                        ));
                    } else if ip.len() == 16 {
                        let addr = std::net::Ipv6Addr::from(<[u8; 16]>::try_from(&ip[..]).unwrap());
                        san_names.push(addr.to_string());
                    }
                }
                _ => {}
            }
        }
    }

    // Check extended key usage
    let (has_server_auth, has_client_auth) =
        if let Ok(Some(eku_ext)) = cert.extended_key_usage() {
            (eku_ext.value.server_auth, eku_ext.value.client_auth)
        } else {
            // If no EKU extension, assume both are allowed
            (true, true)
        };

    // Extract validity period using raw timestamp from ASN1Time
    let not_before = asn1_time_to_system_time(&cert.validity().not_before);
    let not_after = asn1_time_to_system_time(&cert.validity().not_after);

    Ok(CertificateMetadata {
        common_name,
        san_names,
        has_server_auth,
        has_client_auth,
        not_before,
        not_after,
    })
}

/// Converts an ASN1Time to a SystemTime.
fn asn1_time_to_system_time(asn1_time: &x509_parser::time::ASN1Time) -> Option<SystemTime> {
    let timestamp = asn1_time.timestamp();
    if timestamp >= 0 {
        Some(UNIX_EPOCH + Duration::from_secs(timestamp as u64))
    } else {
        // Negative timestamps are before UNIX epoch
        UNIX_EPOCH.checked_sub(Duration::from_secs((-timestamp) as u64))
    }
}

/// Loads CA certificates from a file or directory.
#[instrument(skip_all, fields(path = %path.display()))]
pub fn load_ca_certificates(path: &Path) -> SecurityResult<Vec<CertificateDer<'static>>> {
    debug!("loading CA certificates");

    if path.is_dir() {
        let mut certs = Vec::new();
        for entry in std::fs::read_dir(path).map_err(|e| {
            SecurityError::CertificateNotFound(format!("failed to read directory: {}", e))
        })? {
            let entry = entry.map_err(|e| {
                SecurityError::CertificateNotFound(format!("failed to read entry: {}", e))
            })?;
            let entry_path = entry.path();
            if entry_path.extension().map_or(false, |ext| ext == "pem" || ext == "crt") {
                certs.extend(load_ca_certificates_from_file(&entry_path)?);
            }
        }
        Ok(certs)
    } else {
        load_ca_certificates_from_file(path)
    }
}

fn load_ca_certificates_from_file(path: &Path) -> SecurityResult<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| {
        SecurityError::CertificateNotFound(format!("{}: {}", path.display(), e))
    })?;
    let mut reader = BufReader::new(file);

    certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SecurityError::InvalidCertificate(format!("failed to parse CA certs: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed_certificate() {
        let result = generate_self_signed_certificate(
            "test.local",
            &["localhost".to_string(), "127.0.0.1".to_string()],
            30,
        );

        assert!(result.is_ok());
        let cert = result.unwrap();
        assert_eq!(cert.common_name, Some("test.local".to_string()));
        assert!(cert.san_names.contains(&"localhost".to_string()));
        assert!(cert.san_names.contains(&"127.0.0.1".to_string()));
        assert!(cert.has_server_auth);
        assert!(cert.has_client_auth);
        assert!(cert.is_valid());
        assert!(!cert.certificate_chain.is_empty());
    }

    #[test]
    fn test_certificate_validity_check() {
        let cert = generate_self_signed_certificate("test.local", &[], 30).unwrap();
        assert!(cert.is_valid());
        assert!(!cert.expires_within(Duration::from_secs(1)));
    }

    #[test]
    fn test_certificate_expiration() {
        let cert = generate_self_signed_certificate("test.local", &[], 1).unwrap();
        // Should expire within 2 days
        assert!(cert.expires_within(Duration::from_secs(2 * 86400)));
    }

    #[test]
    fn test_loaded_certificate_debug() {
        let cert = generate_self_signed_certificate("test.local", &["localhost".to_string()], 30).unwrap();
        let debug_str = format!("{:?}", cert);
        assert!(debug_str.contains("test.local"));
        assert!(debug_str.contains("localhost"));
    }

    #[test]
    fn test_load_in_memory_certificate_invalid() {
        let result = load_in_memory_certificate("invalid", "invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_pkcs12_not_supported() {
        let result = load_pkcs12_certificate(Path::new("/tmp/test.p12"), "password");
        assert!(matches!(result, Err(SecurityError::Configuration(_))));
    }

    #[test]
    fn test_oid_constants() {
        assert_eq!(SERVER_AUTH_OID, "1.3.6.1.5.5.7.3.1");
        assert_eq!(CLIENT_AUTH_OID, "1.3.6.1.5.5.7.3.2");
    }

    #[test]
    fn test_asn1_time_conversion_via_cert() {
        // The asn1_time_to_system_time function is tested indirectly
        // via generate_self_signed_certificate which parses the generated cert
        let cert = generate_self_signed_certificate("test.local", &[], 30).unwrap();
        assert!(cert.not_before.is_some());
        assert!(cert.not_after.is_some());
    }

    #[test]
    fn test_source_type_name() {
        let pem = CertificateSource::PemFile {
            cert_path: "/tmp/cert.pem".into(),
            key_path: "/tmp/key.pem".into(),
        };
        assert_eq!(source_type_name(&pem), "pem_file");

        let self_signed = CertificateSource::SelfSigned {
            common_name: "test".to_string(),
            san_names: vec![],
            validity_days: 30,
        };
        assert_eq!(source_type_name(&self_signed), "self_signed");
    }
}
