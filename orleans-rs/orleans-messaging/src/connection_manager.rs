//! ConnectionManager - Manages connections to other silos.
//!
//! Maintains a pool of connections and handles reconnection logic.

use std::sync::Arc;

use dashmap::DashMap;
use orleans_core::SiloAddress;
use tokio::net::TcpStream;
use tokio::sync::RwLock;

use crate::connection::Connection;
use crate::MessagingError;

/// Configuration for connection management.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Maximum number of connection attempts before giving up.
    pub max_connect_attempts: u32,
    /// Delay between connection attempts.
    pub connect_retry_delay: std::time::Duration,
    /// Timeout for establishing a connection.
    pub connect_timeout: std::time::Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            max_connect_attempts: 3,
            connect_retry_delay: std::time::Duration::from_millis(500),
            connect_timeout: std::time::Duration::from_secs(5),
        }
    }
}

/// Manages connections to other silos.
pub struct ConnectionManager {
    /// Local silo address.
    local_address: SiloAddress,

    /// Active connections indexed by remote silo address.
    connections: DashMap<SiloAddress, Arc<Connection>>,

    /// Configuration.
    config: ConnectionConfig,

    /// Lock for connection establishment (prevents duplicate connections).
    connection_locks: DashMap<SiloAddress, Arc<RwLock<()>>>,

    /// Track which connections have a receiver loop running.
    has_receivers: DashMap<SiloAddress, ()>,
}

impl ConnectionManager {
    /// Creates a new ConnectionManager.
    pub fn new(local_address: SiloAddress) -> Self {
        Self::with_config(local_address, ConnectionConfig::default())
    }

    /// Creates a new ConnectionManager with custom configuration.
    pub fn with_config(local_address: SiloAddress, config: ConnectionConfig) -> Self {
        Self {
            local_address,
            connections: DashMap::new(),
            config,
            connection_locks: DashMap::new(),
            has_receivers: DashMap::new(),
        }
    }

    /// Returns the local silo address.
    pub fn local_address(&self) -> &SiloAddress {
        &self.local_address
    }

    /// Gets or creates a connection to a remote silo.
    pub async fn get_connection(&self, remote: &SiloAddress) -> Result<Arc<Connection>, MessagingError> {
        // Check if we already have a connection
        if let Some(conn) = self.connections.get(remote) {
            if conn.is_connected() {
                return Ok(Arc::clone(&conn));
            }
            // Connection is dead, remove it
            drop(conn);
            self.connections.remove(remote);
        }

        // Get or create a lock for this remote address
        let lock = self
            .connection_locks
            .entry(remote.clone())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone();

        // Acquire write lock to prevent concurrent connection attempts
        let _guard = lock.write().await;

        // Double-check after acquiring lock
        if let Some(conn) = self.connections.get(remote) {
            if conn.is_connected() {
                return Ok(Arc::clone(&conn));
            }
            drop(conn);
            self.connections.remove(remote);
        }

        // Attempt to connect
        let connection = self.connect_to(remote).await?;
        self.connections.insert(remote.clone(), Arc::clone(&connection));

        Ok(connection)
    }

    /// Attempts to connect to a remote silo.
    async fn connect_to(&self, remote: &SiloAddress) -> Result<Arc<Connection>, MessagingError> {
        let endpoint = remote.endpoint();
        let mut last_error = None;

        for attempt in 0..self.config.max_connect_attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.connect_retry_delay).await;
            }

            match tokio::time::timeout(
                self.config.connect_timeout,
                TcpStream::connect(endpoint),
            )
            .await
            {
                Ok(Ok(stream)) => {
                    // Disable Nagle's algorithm for lower latency
                    let _ = stream.set_nodelay(true);

                    let (connection, handle) = Connection::new(
                        stream,
                        remote.clone(),
                        self.local_address.clone(),
                    );

                    // Spawn the connection I/O loop
                    tokio::spawn(handle.run());

                    tracing::info!(
                        "Established connection to {} (attempt {})",
                        remote,
                        attempt + 1
                    );

                    return Ok(connection);
                }
                Ok(Err(e)) => {
                    tracing::debug!(
                        "Failed to connect to {} (attempt {}): {}",
                        remote,
                        attempt + 1,
                        e
                    );
                    last_error = Some(MessagingError::ConnectionFailed(e.to_string()));
                }
                Err(_) => {
                    tracing::debug!(
                        "Connection to {} timed out (attempt {})",
                        remote,
                        attempt + 1
                    );
                    last_error = Some(MessagingError::ConnectionTimeout);
                }
            }
        }

        Err(last_error.unwrap_or(MessagingError::ConnectionFailed(
            "Unknown error".to_string(),
        )))
    }

    /// Registers an incoming connection.
    pub fn register_incoming(&self, connection: Arc<Connection>) {
        self.connections
            .insert(connection.remote_address().clone(), connection);
    }

    /// Removes a connection.
    pub fn remove_connection(&self, remote: &SiloAddress) {
        self.connections.remove(remote);
        self.has_receivers.remove(remote);
    }

    /// Returns all active connections.
    pub fn active_connections(&self) -> Vec<Arc<Connection>> {
        self.connections
            .iter()
            .filter(|entry| entry.value().is_connected())
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Returns the number of active connections.
    pub fn connection_count(&self) -> usize {
        self.connections
            .iter()
            .filter(|entry| entry.value().is_connected())
            .count()
    }

    /// Closes all connections.
    pub fn close_all(&self) {
        for entry in self.connections.iter() {
            entry.value().mark_closed();
        }
        self.connections.clear();
        self.has_receivers.clear();
    }

    /// Checks if a connection has a receiver loop running.
    pub fn has_receiver(&self, remote: &SiloAddress) -> bool {
        self.has_receivers.contains_key(remote)
    }

    /// Marks a connection as having a receiver loop.
    pub fn mark_has_receiver(&self, remote: &SiloAddress) {
        self.has_receivers.insert(remote.clone(), ());
    }

    /// Clears the receiver flag when a connection closes.
    pub fn clear_receiver(&self, remote: &SiloAddress) {
        self.has_receivers.remove(remote);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    #[tokio::test]
    async fn test_connection_manager_creation() {
        let manager = ConnectionManager::new(test_silo_address(11111));
        assert_eq!(manager.local_address().port(), 11111);
        assert_eq!(manager.connection_count(), 0);
    }

    #[tokio::test]
    async fn test_get_connection_creates_connection() {
        // Start a listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept connections in background
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                // Just accept and hold the connection
                tokio::spawn(async move {
                    let _stream = stream;
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                });
            }
        });

        let manager = ConnectionManager::new(test_silo_address(11111));
        let remote = test_silo_address(addr.port());

        let connection = manager.get_connection(&remote).await.unwrap();
        assert!(connection.is_connected());
        assert_eq!(manager.connection_count(), 1);
    }

    #[tokio::test]
    async fn test_connection_reuse() {
        // Start a listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _stream = stream;
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                });
            }
        });

        let manager = ConnectionManager::new(test_silo_address(11111));
        let remote = test_silo_address(addr.port());

        let conn1 = manager.get_connection(&remote).await.unwrap();
        let conn2 = manager.get_connection(&remote).await.unwrap();

        // Should be the same connection
        assert!(Arc::ptr_eq(&conn1, &conn2));
        assert_eq!(manager.connection_count(), 1);
    }

    #[tokio::test]
    async fn test_connection_failed() {
        let manager = ConnectionManager::with_config(
            test_silo_address(11111),
            ConnectionConfig {
                max_connect_attempts: 1,
                connect_retry_delay: std::time::Duration::from_millis(10),
                connect_timeout: std::time::Duration::from_millis(100),
            },
        );

        // Try to connect to a non-existent server
        let remote = test_silo_address(59999); // Hopefully not in use
        let result = manager.get_connection(&remote).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_close_all() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _stream = stream;
                    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                });
            }
        });

        let manager = ConnectionManager::new(test_silo_address(11111));
        let remote = test_silo_address(addr.port());

        let _conn = manager.get_connection(&remote).await.unwrap();
        assert_eq!(manager.connection_count(), 1);

        manager.close_all();
        assert_eq!(manager.connection_count(), 0);
    }
}
