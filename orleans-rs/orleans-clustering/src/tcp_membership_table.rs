//! TCP-based membership table for multi-process clusters.
//!
//! This module provides a TCP server/client implementation for the membership table,
//! enabling separate OS processes to share cluster membership state.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────────┐     TCP     ┌──────────────────────────────────────┐
//! │  Silo Process 1  │◄───────────►│                                      │
//! │  (TcpClient)     │             │   MembershipTableServer              │
//! └──────────────────┘             │   (wraps InMemoryMembershipTable)    │
//!                                  │                                      │
//! ┌──────────────────┐     TCP     │   - Handles concurrent connections   │
//! │  Silo Process 2  │◄───────────►│   - Serializes operations with JSON  │
//! │  (TcpClient)     │             │   - Maintains single source of truth │
//! └──────────────────┘             │                                      │
//!                                  └──────────────────────────────────────┘
//! ```

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_core::SiloAddress;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

use crate::error::{MembershipError, MembershipResult};
use crate::in_memory_table::InMemoryMembershipTable;
use crate::membership_entry::MembershipEntry;
use crate::membership_table::{IMembershipTable, MembershipTableData};
use crate::table_version::TableVersion;

// ============================================================================
// Protocol Messages
// ============================================================================

/// Request message sent from client to server.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum MembershipRequest {
    ReadRow { silo_address: SiloAddress },
    ReadAll,
    InsertRow { entry: MembershipEntry, table_version: TableVersion },
    UpdateRow { entry: MembershipEntry, etag: String, table_version: TableVersion },
    UpdateIAmAlive { entry: MembershipEntry },
    DeleteMembershipTableEntries { cluster_id: String },
    CleanupDefunctSiloEntries { before: DateTime<Utc> },
    InitializeMembershipTable { try_init_table_version: bool },
}

/// Response message sent from server to client.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum MembershipResponse {
    ReadRow(Option<(MembershipEntry, String)>),
    ReadAll(MembershipTableData),
    InsertRow(Result<bool, String>),
    UpdateRow(Result<bool, String>),
    UpdateIAmAlive(Result<(), String>),
    DeleteMembershipTableEntries(Result<(), String>),
    CleanupDefunctSiloEntries(Result<(), String>),
    InitializeMembershipTable(Result<(), String>),
}

// ============================================================================
// Membership Table Server
// ============================================================================

/// A TCP server that hosts a membership table for multi-process clusters.
///
/// This server wraps an `InMemoryMembershipTable` and exposes it over TCP,
/// allowing multiple silo processes to share cluster membership state.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use orleans_clustering::{MembershipTableServer, InMemoryMembershipTable};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create and start the server
/// let table = Arc::new(InMemoryMembershipTable::new("my-cluster"));
/// let server = MembershipTableServer::new(table);
/// let server_addr = server.start("127.0.0.1:0").await?;
///
/// println!("Membership server running on {}", server_addr);
/// // Server runs in background, clients can connect via TcpMembershipTable
/// # Ok(())
/// # }
/// ```
pub struct MembershipTableServer {
    table: Arc<InMemoryMembershipTable>,
    shutdown: Arc<RwLock<bool>>,
}

impl MembershipTableServer {
    /// Create a new membership table server with the given backing table.
    pub fn new(table: Arc<InMemoryMembershipTable>) -> Self {
        Self {
            table,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the server on the given address.
    ///
    /// Returns the actual address the server is listening on (useful when binding to port 0).
    /// The server runs in a background task until `stop()` is called.
    pub async fn start(&self, addr: &str) -> MembershipResult<SocketAddr> {
        let listener = TcpListener::bind(addr).await.map_err(|e| {
            MembershipError::Storage(format!("Failed to bind TCP listener: {}", e))
        })?;

        let local_addr = listener.local_addr().map_err(|e| {
            MembershipError::Storage(format!("Failed to get local address: {}", e))
        })?;

        let table = self.table.clone();
        let shutdown = self.shutdown.clone();

        // Spawn the accept loop
        tokio::spawn(async move {
            loop {
                // Check shutdown flag
                if *shutdown.read().await {
                    break;
                }

                // Accept with timeout to allow shutdown checks
                let accept_result = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    listener.accept()
                ).await;

                if let Ok(Ok((stream, peer_addr))) = accept_result {
                    tracing::debug!("Accepted membership table connection from {}", peer_addr);
                    let table = table.clone();
                    tokio::spawn(Self::handle_connection(stream, table));
                }
            }
            tracing::info!("Membership table server stopped");
        });

        tracing::info!("Membership table server started on {}", local_addr);
        Ok(local_addr)
    }

    /// Signal the server to stop accepting new connections.
    pub async fn stop(&self) {
        *self.shutdown.write().await = true;
    }

    /// Handle a single client connection.
    async fn handle_connection(stream: TcpStream, table: Arc<InMemoryMembershipTable>) {
        let (reader, mut writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // Connection closed
                    break;
                }
                Ok(_) => {
                    // Parse and handle request
                    let response = match serde_json::from_str::<MembershipRequest>(&line) {
                        Ok(request) => Self::handle_request(&table, request).await,
                        Err(e) => {
                            tracing::warn!("Invalid request: {}", e);
                            continue;
                        }
                    };

                    // Send response
                    let response_json = match serde_json::to_string(&response) {
                        Ok(json) => json,
                        Err(e) => {
                            tracing::error!("Failed to serialize response: {}", e);
                            continue;
                        }
                    };

                    if let Err(e) = writer.write_all(response_json.as_bytes()).await {
                        tracing::warn!("Failed to write response: {}", e);
                        break;
                    }
                    if let Err(e) = writer.write_all(b"\n").await {
                        tracing::warn!("Failed to write newline: {}", e);
                        break;
                    }
                    if let Err(e) = writer.flush().await {
                        tracing::warn!("Failed to flush: {}", e);
                        break;
                    }
                }
                Err(e) => {
                    tracing::warn!("Error reading from client: {}", e);
                    break;
                }
            }
        }
    }

    /// Handle a single request and return the response.
    async fn handle_request(
        table: &InMemoryMembershipTable,
        request: MembershipRequest,
    ) -> MembershipResponse {
        match request {
            MembershipRequest::ReadRow { silo_address } => {
                match table.read_row(&silo_address).await {
                    Ok(result) => MembershipResponse::ReadRow(result),
                    Err(_) => MembershipResponse::ReadRow(None),
                }
            }
            MembershipRequest::ReadAll => {
                match table.read_all().await {
                    Ok(data) => MembershipResponse::ReadAll(data),
                    Err(_) => MembershipResponse::ReadAll(MembershipTableData::new()),
                }
            }
            MembershipRequest::InsertRow { entry, table_version } => {
                match table.insert_row(entry, table_version).await {
                    Ok(result) => MembershipResponse::InsertRow(Ok(result)),
                    Err(e) => MembershipResponse::InsertRow(Err(e.to_string())),
                }
            }
            MembershipRequest::UpdateRow { entry, etag, table_version } => {
                match table.update_row(entry, &etag, table_version).await {
                    Ok(result) => MembershipResponse::UpdateRow(Ok(result)),
                    Err(e) => MembershipResponse::UpdateRow(Err(e.to_string())),
                }
            }
            MembershipRequest::UpdateIAmAlive { entry } => {
                match table.update_i_am_alive(&entry).await {
                    Ok(()) => MembershipResponse::UpdateIAmAlive(Ok(())),
                    Err(e) => MembershipResponse::UpdateIAmAlive(Err(e.to_string())),
                }
            }
            MembershipRequest::DeleteMembershipTableEntries { cluster_id } => {
                match table.delete_membership_table_entries(&cluster_id).await {
                    Ok(()) => MembershipResponse::DeleteMembershipTableEntries(Ok(())),
                    Err(e) => MembershipResponse::DeleteMembershipTableEntries(Err(e.to_string())),
                }
            }
            MembershipRequest::CleanupDefunctSiloEntries { before } => {
                match table.cleanup_defunct_silo_entries(before).await {
                    Ok(()) => MembershipResponse::CleanupDefunctSiloEntries(Ok(())),
                    Err(e) => MembershipResponse::CleanupDefunctSiloEntries(Err(e.to_string())),
                }
            }
            MembershipRequest::InitializeMembershipTable { try_init_table_version } => {
                match table.initialize_membership_table(try_init_table_version).await {
                    Ok(()) => MembershipResponse::InitializeMembershipTable(Ok(())),
                    Err(e) => MembershipResponse::InitializeMembershipTable(Err(e.to_string())),
                }
            }
        }
    }
}

// ============================================================================
// TCP Membership Table Client
// ============================================================================

/// A TCP client that implements `IMembershipTable` by connecting to a `MembershipTableServer`.
///
/// This allows separate OS processes to share cluster membership state over the network.
///
/// # Example
///
/// ```rust,no_run
/// use orleans_clustering::{TcpMembershipTable, IMembershipTable};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Connect to a running membership server
/// let table = TcpMembershipTable::connect("127.0.0.1:5000").await?;
/// table.initialize_membership_table(true).await?;
///
/// // Use like any other membership table
/// let all_entries = table.read_all().await?;
/// println!("Cluster has {} silos", all_entries.len());
/// # Ok(())
/// # }
/// ```
pub struct TcpMembershipTable {
    server_addr: SocketAddr,
}

impl TcpMembershipTable {
    /// Connect to a membership table server at the given address.
    pub async fn connect(addr: &str) -> MembershipResult<Self> {
        let server_addr: SocketAddr = addr.parse().map_err(|e| {
            MembershipError::Storage(format!("Invalid server address: {}", e))
        })?;

        // Test the connection
        TcpStream::connect(server_addr).await.map_err(|e| {
            MembershipError::Storage(format!("Failed to connect to membership server: {}", e))
        })?;

        Ok(Self { server_addr })
    }

    /// Create from a SocketAddr directly.
    pub fn from_addr(server_addr: SocketAddr) -> Self {
        Self { server_addr }
    }

    /// Send a request and receive a response.
    async fn send_request(&self, request: MembershipRequest) -> MembershipResult<MembershipResponse> {
        // Connect for each request (simple but functional for MVP)
        let stream = TcpStream::connect(self.server_addr).await.map_err(|e| {
            MembershipError::Storage(format!("Failed to connect: {}", e))
        })?;

        let (reader, mut writer) = stream.into_split();

        // Send request
        let request_json = serde_json::to_string(&request).map_err(|e| {
            MembershipError::Storage(format!("Failed to serialize request: {}", e))
        })?;

        writer.write_all(request_json.as_bytes()).await.map_err(|e| {
            MembershipError::Storage(format!("Failed to send request: {}", e))
        })?;
        writer.write_all(b"\n").await.map_err(|e| {
            MembershipError::Storage(format!("Failed to send newline: {}", e))
        })?;
        writer.flush().await.map_err(|e| {
            MembershipError::Storage(format!("Failed to flush: {}", e))
        })?;

        // Read response
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.map_err(|e| {
            MembershipError::Storage(format!("Failed to read response: {}", e))
        })?;

        let response: MembershipResponse = serde_json::from_str(&line).map_err(|e| {
            MembershipError::Storage(format!("Failed to parse response: {}", e))
        })?;

        Ok(response)
    }
}

#[async_trait]
impl IMembershipTable for TcpMembershipTable {
    async fn read_row(
        &self,
        silo_address: &SiloAddress,
    ) -> MembershipResult<Option<(MembershipEntry, String)>> {
        let request = MembershipRequest::ReadRow {
            silo_address: silo_address.clone(),
        };

        match self.send_request(request).await? {
            MembershipResponse::ReadRow(result) => Ok(result),
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn read_all(&self) -> MembershipResult<MembershipTableData> {
        let request = MembershipRequest::ReadAll;

        match self.send_request(request).await? {
            MembershipResponse::ReadAll(data) => Ok(data),
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn insert_row(
        &self,
        entry: MembershipEntry,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        let request = MembershipRequest::InsertRow { entry, table_version };

        match self.send_request(request).await? {
            MembershipResponse::InsertRow(Ok(result)) => Ok(result),
            MembershipResponse::InsertRow(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn update_row(
        &self,
        entry: MembershipEntry,
        etag: &str,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        let request = MembershipRequest::UpdateRow {
            entry,
            etag: etag.to_string(),
            table_version,
        };

        match self.send_request(request).await? {
            MembershipResponse::UpdateRow(Ok(result)) => Ok(result),
            MembershipResponse::UpdateRow(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn update_i_am_alive(&self, entry: &MembershipEntry) -> MembershipResult<()> {
        let request = MembershipRequest::UpdateIAmAlive {
            entry: entry.clone(),
        };

        match self.send_request(request).await? {
            MembershipResponse::UpdateIAmAlive(Ok(())) => Ok(()),
            MembershipResponse::UpdateIAmAlive(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn delete_membership_table_entries(&self, cluster_id: &str) -> MembershipResult<()> {
        let request = MembershipRequest::DeleteMembershipTableEntries {
            cluster_id: cluster_id.to_string(),
        };

        match self.send_request(request).await? {
            MembershipResponse::DeleteMembershipTableEntries(Ok(())) => Ok(()),
            MembershipResponse::DeleteMembershipTableEntries(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn cleanup_defunct_silo_entries(&self, before: DateTime<Utc>) -> MembershipResult<()> {
        let request = MembershipRequest::CleanupDefunctSiloEntries { before };

        match self.send_request(request).await? {
            MembershipResponse::CleanupDefunctSiloEntries(Ok(())) => Ok(()),
            MembershipResponse::CleanupDefunctSiloEntries(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }

    async fn initialize_membership_table(&self, try_init_table_version: bool) -> MembershipResult<()> {
        let request = MembershipRequest::InitializeMembershipTable { try_init_table_version };

        match self.send_request(request).await? {
            MembershipResponse::InitializeMembershipTable(Ok(())) => Ok(()),
            MembershipResponse::InitializeMembershipTable(Err(e)) => {
                Err(MembershipError::Storage(e))
            }
            _ => Err(MembershipError::Storage("Unexpected response".to_string())),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_address(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    #[tokio::test]
    async fn test_server_client_basic_operations() {
        // Start server
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        let server = MembershipTableServer::new(table);
        let server_addr = server.start("127.0.0.1:0").await.unwrap();

        // Connect client
        let client = TcpMembershipTable::from_addr(server_addr);

        // Initialize
        client.initialize_membership_table(true).await.unwrap();

        // Read all (should be empty)
        let data = client.read_all().await.unwrap();
        assert!(data.is_empty());

        // Insert an entry
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        let version = data.version.clone();
        let result = client.insert_row(entry.clone(), version).await.unwrap();
        assert!(result);

        // Read it back
        let read_result = client.read_row(&addr).await.unwrap();
        assert!(read_result.is_some());
        let (read_entry, _etag) = read_result.unwrap();
        assert_eq!(read_entry.silo_address, addr);

        // Read all should now have one entry
        let data = client.read_all().await.unwrap();
        assert_eq!(data.len(), 1);

        // Stop server
        server.stop().await;
    }

    #[tokio::test]
    async fn test_multiple_clients() {
        // Start server
        let table = Arc::new(InMemoryMembershipTable::new("multi-client-test"));
        let server = MembershipTableServer::new(table);
        let server_addr = server.start("127.0.0.1:0").await.unwrap();

        // Connect multiple clients
        let client1 = TcpMembershipTable::from_addr(server_addr);
        let client2 = TcpMembershipTable::from_addr(server_addr);
        let client3 = TcpMembershipTable::from_addr(server_addr);

        // Initialize via client1
        client1.initialize_membership_table(true).await.unwrap();

        // Insert via client1
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        let data = client1.read_all().await.unwrap();
        client1.insert_row(entry, data.version).await.unwrap();

        // Read via client2
        let read = client2.read_row(&addr).await.unwrap();
        assert!(read.is_some());

        // Read all via client3
        let data = client3.read_all().await.unwrap();
        assert_eq!(data.len(), 1);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_concurrent_inserts() {
        // Start server
        let table = Arc::new(InMemoryMembershipTable::new("concurrent-test"));
        let server = MembershipTableServer::new(table);
        let server_addr = server.start("127.0.0.1:0").await.unwrap();

        let client = TcpMembershipTable::from_addr(server_addr);
        client.initialize_membership_table(true).await.unwrap();

        // Insert multiple entries
        let mut handles = Vec::new();
        for port in 11111..11121 {
            let client = TcpMembershipTable::from_addr(server_addr);
            let handle = tokio::spawn(async move {
                let addr = test_address(port);
                let entry = MembershipEntry::new_joining(addr);
                // Each client reads and inserts - some may fail due to version conflicts
                // which is expected behavior with optimistic concurrency
                for attempt in 0..10 {
                    let data = client.read_all().await.unwrap();
                    if client.insert_row(entry.clone(), data.version).await.unwrap_or(false) {
                        return true;
                    }
                    // Exponential backoff with jitter
                    let delay = 5 * (attempt + 1) + (port % 10) as u64;
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
                false
            });
            handles.push(handle);
        }

        // Wait for all inserts
        let results: Vec<bool> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // With optimistic concurrency, some failures are expected
        // At least half should succeed with enough retries
        let success_count = results.iter().filter(|&&r| r).count();
        assert!(success_count >= 5, "At least 5/10 inserts should succeed, got {}", success_count);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_update_row() {
        let table = Arc::new(InMemoryMembershipTable::new("update-test"));
        let server = MembershipTableServer::new(table);
        let server_addr = server.start("127.0.0.1:0").await.unwrap();

        let client = TcpMembershipTable::from_addr(server_addr);
        client.initialize_membership_table(true).await.unwrap();

        // Insert
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        let data = client.read_all().await.unwrap();
        client.insert_row(entry, data.version).await.unwrap();

        // Read to get ETag
        let (mut entry, etag) = client.read_row(&addr).await.unwrap().unwrap();

        // Update status
        entry.status = crate::silo_status::SiloStatus::Active;
        let data = client.read_all().await.unwrap();
        let result = client.update_row(entry, &etag, data.version).await.unwrap();
        assert!(result);

        // Verify update
        let (updated, _) = client.read_row(&addr).await.unwrap().unwrap();
        assert_eq!(updated.status, crate::silo_status::SiloStatus::Active);

        server.stop().await;
    }

    #[tokio::test]
    async fn test_i_am_alive_update() {
        let table = Arc::new(InMemoryMembershipTable::new("heartbeat-test"));
        let server = MembershipTableServer::new(table);
        let server_addr = server.start("127.0.0.1:0").await.unwrap();

        let client = TcpMembershipTable::from_addr(server_addr);
        client.initialize_membership_table(true).await.unwrap();

        // Insert
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        let data = client.read_all().await.unwrap();
        client.insert_row(entry, data.version).await.unwrap();

        // Get original timestamp
        let (original, _) = client.read_row(&addr).await.unwrap().unwrap();
        let original_time = original.i_am_alive_time;

        // Wait and update heartbeat
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut update_entry = MembershipEntry::new(addr.clone());
        update_entry.i_am_alive_time = Utc::now();
        client.update_i_am_alive(&update_entry).await.unwrap();

        // Verify timestamp updated
        let (updated, _) = client.read_row(&addr).await.unwrap().unwrap();
        assert!(updated.i_am_alive_time > original_time);

        server.stop().await;
    }
}
