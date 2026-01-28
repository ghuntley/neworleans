//! TLS stream wrapper and connection utilities.

use crate::config::{build_client_config, build_server_config};
use crate::error::{SecurityError, SecurityResult};
use crate::options::TlsOptions;
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ServerConfig};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream as ServerTlsStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, info, instrument, warn};

/// A TLS-secured stream that can be either client or server side.
pub enum TlsStream {
    /// Client-side TLS stream.
    Client(ClientTlsStream<TcpStream>),
    /// Server-side TLS stream.
    Server(ServerTlsStream<TcpStream>),
}

impl std::fmt::Debug for TlsStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsStream::Client(_) => write!(f, "TlsStream::Client"),
            TlsStream::Server(_) => write!(f, "TlsStream::Server"),
        }
    }
}

impl TlsStream {
    /// Returns the peer's address.
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            TlsStream::Client(s) => s.get_ref().0.peer_addr(),
            TlsStream::Server(s) => s.get_ref().0.peer_addr(),
        }
    }

    /// Returns the local address.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            TlsStream::Client(s) => s.get_ref().0.local_addr(),
            TlsStream::Server(s) => s.get_ref().0.local_addr(),
        }
    }

    /// Returns the negotiated ALPN protocol, if any.
    pub fn alpn_protocol(&self) -> Option<&[u8]> {
        match self {
            TlsStream::Client(s) => s.get_ref().1.alpn_protocol(),
            TlsStream::Server(s) => s.get_ref().1.alpn_protocol(),
        }
    }

    /// Returns the negotiated TLS protocol version.
    pub fn protocol_version(&self) -> Option<rustls::ProtocolVersion> {
        match self {
            TlsStream::Client(s) => s.get_ref().1.protocol_version(),
            TlsStream::Server(s) => s.get_ref().1.protocol_version(),
        }
    }

    /// Returns the negotiated cipher suite.
    pub fn negotiated_cipher_suite(&self) -> Option<rustls::SupportedCipherSuite> {
        match self {
            TlsStream::Client(s) => s.get_ref().1.negotiated_cipher_suite(),
            TlsStream::Server(s) => s.get_ref().1.negotiated_cipher_suite(),
        }
    }

    /// Returns true if this is a client-side stream.
    pub fn is_client(&self) -> bool {
        matches!(self, TlsStream::Client(_))
    }

    /// Returns true if this is a server-side stream.
    pub fn is_server(&self) -> bool {
        matches!(self, TlsStream::Server(_))
    }
}

impl AsyncRead for TlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TlsStream::Client(s) => Pin::new(s).poll_read(cx, buf),
            TlsStream::Server(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            TlsStream::Client(s) => Pin::new(s).poll_write(cx, buf),
            TlsStream::Server(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TlsStream::Client(s) => Pin::new(s).poll_flush(cx),
            TlsStream::Server(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            TlsStream::Client(s) => Pin::new(s).poll_shutdown(cx),
            TlsStream::Server(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// TLS acceptor for server-side connections.
#[derive(Clone)]
pub struct SecureAcceptor {
    acceptor: TlsAcceptor,
    handshake_timeout: Duration,
}

impl SecureAcceptor {
    /// Creates a new secure acceptor from TLS options.
    #[instrument(skip(options))]
    pub fn new(options: &TlsOptions) -> SecurityResult<Self> {
        let config = build_server_config(options)?;
        Ok(Self::from_config(Arc::new(config), options.handshake_timeout))
    }

    /// Creates a new secure acceptor from a pre-built config.
    pub fn from_config(config: Arc<ServerConfig>, handshake_timeout: Duration) -> Self {
        Self {
            acceptor: TlsAcceptor::from(config),
            handshake_timeout,
        }
    }

    /// Accepts a TLS connection from a TCP stream.
    #[instrument(skip(self, stream), fields(peer_addr = %stream.peer_addr().map(|a| a.to_string()).unwrap_or_default()))]
    pub async fn accept(&self, stream: TcpStream) -> SecurityResult<TlsStream> {
        debug!("accepting TLS connection");

        let result = timeout(self.handshake_timeout, self.acceptor.accept(stream)).await;

        match result {
            Ok(Ok(tls_stream)) => {
                info!(
                    protocol = ?tls_stream.get_ref().1.protocol_version(),
                    alpn = ?tls_stream.get_ref().1.alpn_protocol().map(|p| String::from_utf8_lossy(p).to_string()),
                    "TLS server handshake completed"
                );
                Ok(TlsStream::Server(tls_stream))
            }
            Ok(Err(e)) => {
                warn!(error = %e, "TLS handshake failed");
                Err(SecurityError::HandshakeFailed(e.to_string()))
            }
            Err(_) => {
                warn!(timeout = ?self.handshake_timeout, "TLS handshake timed out");
                Err(SecurityError::HandshakeTimeout(self.handshake_timeout))
            }
        }
    }
}

impl std::fmt::Debug for SecureAcceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureAcceptor")
            .field("handshake_timeout", &self.handshake_timeout)
            .finish()
    }
}

/// TLS connector for client-side connections.
#[derive(Clone)]
pub struct SecureConnector {
    connector: TlsConnector,
    handshake_timeout: Duration,
    default_server_name: Option<String>,
}

impl SecureConnector {
    /// Creates a new secure connector from TLS options.
    #[instrument(skip(options))]
    pub fn new(options: &TlsOptions) -> SecurityResult<Self> {
        let config = build_client_config(options)?;
        Ok(Self::from_config(
            Arc::new(config),
            options.handshake_timeout,
            options.sni_hostname.clone(),
        ))
    }

    /// Creates a new secure connector from a pre-built config.
    pub fn from_config(
        config: Arc<ClientConfig>,
        handshake_timeout: Duration,
        default_server_name: Option<String>,
    ) -> Self {
        Self {
            connector: TlsConnector::from(config),
            handshake_timeout,
            default_server_name,
        }
    }

    /// Connects to a remote server with TLS.
    #[instrument(skip(self, stream), fields(peer_addr = %stream.peer_addr().map(|a| a.to_string()).unwrap_or_default()))]
    pub async fn connect(&self, stream: TcpStream, server_name: &str) -> SecurityResult<TlsStream> {
        debug!(server_name = server_name, "initiating TLS connection");

        let name = server_name_from_str(server_name)?;
        let result = timeout(self.handshake_timeout, self.connector.connect(name, stream)).await;

        match result {
            Ok(Ok(tls_stream)) => {
                info!(
                    protocol = ?tls_stream.get_ref().1.protocol_version(),
                    alpn = ?tls_stream.get_ref().1.alpn_protocol().map(|p| String::from_utf8_lossy(p).to_string()),
                    "TLS client handshake completed"
                );
                Ok(TlsStream::Client(tls_stream))
            }
            Ok(Err(e)) => {
                warn!(error = %e, "TLS handshake failed");
                Err(SecurityError::HandshakeFailed(e.to_string()))
            }
            Err(_) => {
                warn!(timeout = ?self.handshake_timeout, "TLS handshake timed out");
                Err(SecurityError::HandshakeTimeout(self.handshake_timeout))
            }
        }
    }

    /// Connects using the default server name (if configured) or the socket address.
    pub async fn connect_with_default(
        &self,
        stream: TcpStream,
    ) -> SecurityResult<TlsStream> {
        let server_name = if let Some(name) = &self.default_server_name {
            name.clone()
        } else {
            // Use the peer address as the server name
            stream
                .peer_addr()
                .map(|a| a.ip().to_string())
                .unwrap_or_else(|_| "localhost".to_string())
        };

        self.connect(stream, &server_name).await
    }
}

impl std::fmt::Debug for SecureConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecureConnector")
            .field("handshake_timeout", &self.handshake_timeout)
            .field("default_server_name", &self.default_server_name)
            .finish()
    }
}

/// Converts a string to a ServerName.
fn server_name_from_str(name: &str) -> SecurityResult<ServerName<'static>> {
    // Try to parse as IP address first
    if let Ok(ip) = name.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }

    // Try to parse as DNS name
    ServerName::try_from(name.to_string())
        .map_err(|_| SecurityError::Configuration(format!("invalid server name: {}", name)))
}

/// Information about a TLS connection.
#[derive(Debug, Clone)]
pub struct TlsConnectionInfo {
    /// The negotiated protocol version.
    pub protocol_version: Option<String>,
    /// The negotiated cipher suite.
    pub cipher_suite: Option<String>,
    /// The negotiated ALPN protocol.
    pub alpn_protocol: Option<String>,
    /// Whether this is a client or server connection.
    pub is_client: bool,
    /// Peer address.
    pub peer_addr: Option<SocketAddr>,
    /// Local address.
    pub local_addr: Option<SocketAddr>,
}

impl TlsConnectionInfo {
    /// Extracts connection info from a TLS stream.
    pub fn from_stream(stream: &TlsStream) -> Self {
        Self {
            protocol_version: stream
                .protocol_version()
                .map(|v| format!("{:?}", v)),
            cipher_suite: stream
                .negotiated_cipher_suite()
                .map(|cs| format!("{:?}", cs.suite())),
            alpn_protocol: stream
                .alpn_protocol()
                .map(|p| String::from_utf8_lossy(p).to_string()),
            is_client: stream.is_client(),
            peer_addr: stream.peer_addr().ok(),
            local_addr: stream.local_addr().ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_name_from_str_ipv4() {
        let name = server_name_from_str("127.0.0.1");
        assert!(name.is_ok());
        assert!(matches!(name.unwrap(), ServerName::IpAddress(_)));
    }

    #[test]
    fn test_server_name_from_str_ipv6() {
        let name = server_name_from_str("::1");
        assert!(name.is_ok());
        assert!(matches!(name.unwrap(), ServerName::IpAddress(_)));
    }

    #[test]
    fn test_server_name_from_str_dns() {
        let name = server_name_from_str("localhost");
        assert!(name.is_ok());
        assert!(matches!(name.unwrap(), ServerName::DnsName(_)));
    }

    #[test]
    fn test_server_name_from_str_domain() {
        let name = server_name_from_str("example.com");
        assert!(name.is_ok());
        assert!(matches!(name.unwrap(), ServerName::DnsName(_)));
    }

    #[test]
    fn test_secure_acceptor_debug() {
        let options = TlsOptions::for_testing();
        let acceptor = SecureAcceptor::new(&options).unwrap();
        let debug_str = format!("{:?}", acceptor);
        assert!(debug_str.contains("SecureAcceptor"));
        assert!(debug_str.contains("handshake_timeout"));
    }

    #[test]
    fn test_secure_connector_debug() {
        let options = TlsOptions::for_testing();
        let connector = SecureConnector::new(&options).unwrap();
        let debug_str = format!("{:?}", connector);
        assert!(debug_str.contains("SecureConnector"));
    }

    #[test]
    fn test_tls_connection_info_default() {
        let info = TlsConnectionInfo {
            protocol_version: Some("TLS1.3".to_string()),
            cipher_suite: Some("TLS_AES_256_GCM_SHA384".to_string()),
            alpn_protocol: Some("orleans1".to_string()),
            is_client: true,
            peer_addr: None,
            local_addr: None,
        };
        assert!(info.is_client);
        assert_eq!(info.alpn_protocol, Some("orleans1".to_string()));
    }

    #[tokio::test]
    async fn test_secure_acceptor_new() {
        let options = TlsOptions::for_testing();
        let result = SecureAcceptor::new(&options);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_secure_connector_new() {
        let options = TlsOptions::for_testing();
        let result = SecureConnector::new(&options);
        assert!(result.is_ok());
    }
}
