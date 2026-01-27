//! Connection - TCP connection wrapper for Orleans messaging.
//!
//! Handles framing, sending, and receiving messages over TCP.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use orleans_core::SiloAddress;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};

use crate::message::Message;
use crate::message_codec::{decode_message, encode_message, frame_size, MAX_MESSAGE_SIZE};
use crate::MessagingError;

/// Statistics for a connection.
#[derive(Debug, Default)]
pub struct ConnectionStats {
    pub messages_sent: AtomicU64,
    pub messages_received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
}

/// A TCP connection to another silo.
pub struct Connection {
    /// The remote silo address (mutable to support learning actual address on incoming connections).
    remote_address: parking_lot::RwLock<SiloAddress>,

    /// The local silo address.
    local_address: SiloAddress,

    /// Whether this connection is currently connected.
    connected: AtomicBool,

    /// Connection statistics.
    stats: ConnectionStats,

    /// Channel for outgoing messages.
    outgoing_tx: mpsc::Sender<Message>,

    /// Channel for incoming messages.
    incoming_rx: Mutex<mpsc::Receiver<Message>>,
}

impl Connection {
    /// Creates a new connection from an established TCP stream.
    pub fn new(
        stream: TcpStream,
        remote_address: SiloAddress,
        local_address: SiloAddress,
    ) -> (Arc<Self>, ConnectionHandle) {
        let (outgoing_tx, outgoing_rx) = mpsc::channel(1024);
        let (incoming_tx, incoming_rx) = mpsc::channel(1024);

        let connection = Arc::new(Self {
            remote_address: parking_lot::RwLock::new(remote_address),
            local_address,
            connected: AtomicBool::new(true),
            stats: ConnectionStats::default(),
            outgoing_tx,
            incoming_rx: Mutex::new(incoming_rx),
        });

        let handle = ConnectionHandle {
            connection: Arc::clone(&connection),
            outgoing_rx: Some(outgoing_rx),
            incoming_tx: Some(incoming_tx),
            stream: Some(stream),
        };

        (connection, handle)
    }

    /// Returns the remote silo address.
    pub fn remote_address(&self) -> SiloAddress {
        self.remote_address.read().clone()
    }

    /// Sets the remote silo address (used when learning actual address on incoming connections).
    pub fn set_remote_address(&self, address: SiloAddress) {
        *self.remote_address.write() = address;
    }

    /// Returns the local silo address.
    pub fn local_address(&self) -> &SiloAddress {
        &self.local_address
    }

    /// Returns true if the connection is currently connected.
    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Acquire)
    }

    /// Sends a message over this connection.
    pub async fn send(&self, message: Message) -> Result<(), MessagingError> {
        if !self.is_connected() {
            return Err(MessagingError::ConnectionClosed);
        }

        self.outgoing_tx
            .send(message)
            .await
            .map_err(|_| MessagingError::ConnectionClosed)?;

        Ok(())
    }

    /// Receives the next message from this connection.
    pub async fn receive(&self) -> Option<Message> {
        self.incoming_rx.lock().await.recv().await
    }

    /// Returns connection statistics.
    pub fn stats(&self) -> &ConnectionStats {
        &self.stats
    }

    /// Marks the connection as closed.
    pub(crate) fn mark_closed(&self) {
        self.connected.store(false, Ordering::Release);
    }
}

/// Handle for the connection I/O task.
pub struct ConnectionHandle {
    connection: Arc<Connection>,
    outgoing_rx: Option<mpsc::Receiver<Message>>,
    incoming_tx: Option<mpsc::Sender<Message>>,
    stream: Option<TcpStream>,
}

impl ConnectionHandle {
    /// Runs the connection I/O loop.
    ///
    /// This should be spawned as a separate task.
    pub async fn run(mut self) {
        let stream = self.stream.take().expect("stream already taken");
        let outgoing_rx = self.outgoing_rx.take().expect("outgoing_rx already taken");
        let incoming_tx = self.incoming_tx.take().expect("incoming_tx already taken");

        let (read_half, write_half) = stream.into_split();

        // Spawn reader and writer tasks
        let connection_clone = Arc::clone(&self.connection);
        let reader_handle = tokio::spawn(async move {
            Self::reader_loop(read_half, incoming_tx, connection_clone).await
        });

        let connection_clone = Arc::clone(&self.connection);
        let writer_handle = tokio::spawn(async move {
            Self::writer_loop(write_half, outgoing_rx, connection_clone).await
        });

        // Wait for either to complete (usually due to disconnect)
        tokio::select! {
            _ = reader_handle => {}
            _ = writer_handle => {}
        }

        self.connection.mark_closed();
    }

    async fn reader_loop(
        mut reader: tokio::net::tcp::OwnedReadHalf,
        incoming_tx: mpsc::Sender<Message>,
        connection: Arc<Connection>,
    ) {
        let mut buffer = BytesMut::with_capacity(16 * 1024);

        loop {
            // Ensure we have capacity
            if buffer.capacity() < 8192 {
                buffer.reserve(8192);
            }

            // Read more data
            match reader.read_buf(&mut buffer).await {
                Ok(0) => {
                    // EOF - connection closed
                    break;
                }
                Ok(n) => {
                    connection.stats.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!("Connection read error: {}", e);
                    break;
                }
            }

            // Try to parse complete frames
            while !buffer.is_empty() {
                match frame_size(&buffer) {
                    Ok(size) => {
                        // We have a complete frame
                        let frame_data = buffer.split_to(size);
                        match decode_message(&frame_data) {
                            Ok(message) => {
                                connection.stats.messages_received.fetch_add(1, Ordering::Relaxed);
                                if incoming_tx.send(message).await.is_err() {
                                    // Receiver dropped
                                    return;
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to decode message: {}", e);
                                // Continue reading - try to recover
                            }
                        }
                    }
                    Err(needed) => {
                        if needed > MAX_MESSAGE_SIZE {
                            tracing::error!("Message too large: {} bytes", needed);
                            return;
                        }
                        // Need more data
                        break;
                    }
                }
            }
        }
    }

    async fn writer_loop(
        mut writer: tokio::net::tcp::OwnedWriteHalf,
        mut outgoing_rx: mpsc::Receiver<Message>,
        connection: Arc<Connection>,
    ) {
        while let Some(message) = outgoing_rx.recv().await {
            match encode_message(&message) {
                Ok(data) => {
                    let len = data.len();
                    if let Err(e) = writer.write_all(&data).await {
                        tracing::warn!("Connection write error: {}", e);
                        break;
                    }
                    connection.stats.bytes_sent.fetch_add(len as u64, Ordering::Relaxed);
                    connection.stats.messages_sent.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::error!("Failed to encode message: {}", e);
                    // Skip this message
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::grain_interface_type::GrainInterfaceType;
    use orleans_core::GrainType;
    use std::net::SocketAddr;

    fn test_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    fn test_grain_id() -> orleans_core::GrainId {
        orleans_core::GrainId::new(GrainType::create("TestGrain"), "key1".into())
    }

    #[tokio::test]
    async fn test_connection_stats_initial() {
        // Create a listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Connect
        let _client_stream = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        let (connection, _handle) = Connection::new(
            server_stream,
            test_silo_address(addr.port()),
            test_silo_address(11111),
        );

        assert!(connection.is_connected());
        assert_eq!(connection.stats().messages_sent.load(Ordering::Relaxed), 0);
        assert_eq!(connection.stats().messages_received.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_send_receive_message() {
        // Create a listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Connect from client side
        let client_stream = TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        // Set up server connection
        let (server_conn, server_handle) = Connection::new(
            server_stream,
            test_silo_address(12345),
            test_silo_address(addr.port()),
        );
        tokio::spawn(server_handle.run());

        // Set up client connection
        let (client_conn, client_handle) = Connection::new(
            client_stream,
            test_silo_address(addr.port()),
            test_silo_address(12345),
        );
        tokio::spawn(client_handle.run());

        // Send a message from client to server
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            42,
            Bytes::from_static(b"hello"),
            test_silo_address(12345),
        );
        let msg_id = msg.id;

        client_conn.send(msg).await.unwrap();

        // Receive on server
        let received = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            server_conn.receive(),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(received.id, msg_id);
        assert_eq!(received.method_id, 42);
        assert_eq!(received.body.as_ref(), b"hello");
    }
}
