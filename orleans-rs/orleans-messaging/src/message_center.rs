//! MessageCenter - Central message dispatcher for Orleans messaging.
//!
//! The MessageCenter is the core component that handles:
//! - Sending messages to remote silos
//! - Receiving messages from remote silos
//! - Matching responses to pending requests
//! - Routing incoming messages to handlers

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use orleans_core::{GrainId, SiloAddress};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

use crate::connection::Connection;
use crate::connection_manager::{ConnectionConfig, ConnectionManager};
use crate::correlation_id::CorrelationId;
use crate::grain_interface_type::GrainInterfaceType;
use crate::message::Message;
use crate::MessagingError;

/// Handler for incoming grain messages.
pub type MessageHandler = Box<dyn Fn(Message) + Send + Sync>;

/// A pending response awaiting a response message.
struct PendingResponse {
    sender: oneshot::Sender<Result<Message, MessagingError>>,
    timeout_at: std::time::Instant,
}

/// Configuration for the MessageCenter.
#[derive(Debug, Clone)]
pub struct MessageCenterConfig {
    /// Configuration for connections.
    pub connection: ConnectionConfig,
    /// Request timeout.
    pub request_timeout: std::time::Duration,
    /// Interval for cleaning up expired requests.
    pub cleanup_interval: std::time::Duration,
}

impl Default for MessageCenterConfig {
    fn default() -> Self {
        Self {
            connection: ConnectionConfig::default(),
            request_timeout: std::time::Duration::from_secs(30),
            cleanup_interval: std::time::Duration::from_secs(5),
        }
    }
}

/// The central message dispatcher for Orleans.
pub struct MessageCenter {
    /// Local silo address.
    local_address: SiloAddress,

    /// Connection manager.
    connection_manager: Arc<ConnectionManager>,

    /// Pending responses indexed by correlation ID.
    pending_responses: Arc<DashMap<CorrelationId, PendingResponse>>,

    /// Message handler for incoming grain messages.
    message_handler: Arc<parking_lot::RwLock<Option<MessageHandler>>>,

    /// Whether the message center is running.
    running: Arc<AtomicBool>,

    /// Channel to signal shutdown.
    shutdown_tx: mpsc::Sender<()>,

    /// Configuration.
    config: MessageCenterConfig,
}

impl MessageCenter {
    /// Creates a new MessageCenter bound to the specified address.
    pub async fn new(local_address: SiloAddress) -> Result<Arc<Self>, MessagingError> {
        Self::with_config(local_address, MessageCenterConfig::default()).await
    }

    /// Creates a new MessageCenter with custom configuration.
    pub async fn with_config(
        local_address: SiloAddress,
        config: MessageCenterConfig,
    ) -> Result<Arc<Self>, MessagingError> {
        // Start the listener first to get the actual bound address
        let listener = TcpListener::bind(local_address.endpoint())
            .await
            .map_err(|e| MessagingError::BindFailed(e.to_string()))?;

        // Get the actual bound address (important when binding to port 0)
        let actual_endpoint = listener
            .local_addr()
            .map_err(|e| MessagingError::BindFailed(e.to_string()))?;
        let actual_address = SiloAddress::new(actual_endpoint, local_address.generation());

        let connection_manager = Arc::new(ConnectionManager::with_config(
            actual_address.clone(),
            config.connection.clone(),
        ));

        let (shutdown_tx, shutdown_rx) = mpsc::channel(1);

        let center = Arc::new(Self {
            local_address: actual_address.clone(),
            connection_manager,
            pending_responses: Arc::new(DashMap::new()),
            message_handler: Arc::new(parking_lot::RwLock::new(None)),
            running: Arc::new(AtomicBool::new(true)),
            shutdown_tx,
            config,
        });

        tracing::info!("MessageCenter listening on {}", actual_address);

        // Spawn listener task
        let center_clone = Arc::clone(&center);
        tokio::spawn(Self::listener_loop(center_clone, listener, shutdown_rx));

        // Spawn cleanup task
        let center_clone = Arc::clone(&center);
        tokio::spawn(Self::cleanup_loop(center_clone));

        Ok(center)
    }

    /// Returns the local silo address.
    pub fn local_address(&self) -> &SiloAddress {
        &self.local_address
    }

    /// Sets the handler for incoming grain messages.
    pub fn set_message_handler<F>(&self, handler: F)
    where
        F: Fn(Message) + Send + Sync + 'static,
    {
        *self.message_handler.write() = Some(Box::new(handler));
    }

    /// Sends a message and returns immediately (fire-and-forget).
    pub async fn send(&self, message: Message) -> Result<(), MessagingError> {
        let target_silo = message.target_silo.as_ref().ok_or_else(|| {
            MessagingError::NoTargetSilo
        })?;

        let connection = self.connection_manager.get_connection(target_silo).await?;
        connection.send(message).await
    }

    /// Sends a request and waits for a response.
    pub async fn send_request(&self, message: Message) -> Result<Message, MessagingError> {
        if !message.is_request() {
            return Err(MessagingError::InvalidMessageType(
                "Expected request message".to_string(),
            ));
        }

        let target_silo = message.target_silo.as_ref().ok_or_else(|| {
            MessagingError::NoTargetSilo
        })?;

        let correlation_id = message.id;
        let timeout = message.timeout.unwrap_or(self.config.request_timeout);
        let timeout_at = std::time::Instant::now() + timeout;

        // Create response channel
        let (tx, rx) = oneshot::channel();

        // Register pending response
        self.pending_responses.insert(
            correlation_id,
            PendingResponse {
                sender: tx,
                timeout_at,
            },
        );

        // Get or create connection and ensure we're receiving messages from it
        let connection = self.connection_manager.get_connection(target_silo).await?;

        // Spawn a task to receive messages from this connection
        // (the ConnectionManager tracks which connections have receivers)
        self.ensure_connection_receiver(Arc::clone(&connection), target_silo.clone());

        if let Err(e) = connection.send(message).await {
            self.pending_responses.remove(&correlation_id);
            return Err(e);
        }

        // Wait for response with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => {
                // Channel closed (sender dropped)
                self.pending_responses.remove(&correlation_id);
                Err(MessagingError::ResponseChannelClosed)
            }
            Err(_) => {
                // Timeout
                self.pending_responses.remove(&correlation_id);
                Err(MessagingError::RequestTimeout)
            }
        }
    }

    /// Ensures there's a receive loop running for an outbound connection.
    fn ensure_connection_receiver(&self, connection: Arc<Connection>, remote_address: SiloAddress) {
        // Check if we already have a receiver for this connection
        // using a simple tracking mechanism
        if !self.connection_manager.has_receiver(&remote_address) {
            self.connection_manager.mark_has_receiver(&remote_address);
            let center = Arc::clone(&self.running).load(Ordering::Acquire);
            if center {
                let pending_responses = Arc::clone(&self.pending_responses);
                let message_handler = Arc::clone(&self.message_handler);
                let connection_manager = Arc::clone(&self.connection_manager);

                tokio::spawn(async move {
                    loop {
                        match connection.receive().await {
                            Some(message) => {
                                // Handle the message (same as handle_message but inline)
                                if message.is_response() {
                                    if let Some((_, pending)) = pending_responses.remove(&message.id) {
                                        let result = if message.is_rejection() {
                                            let info = message.rejection_info.as_ref().unwrap();
                                            Err(MessagingError::RequestRejected {
                                                rejection_type: info.rejection_type,
                                                message: info.message.clone(),
                                            })
                                        } else {
                                            Ok(message)
                                        };
                                        let _ = pending.sender.send(result);
                                    }
                                } else {
                                    // Incoming request on outbound connection
                                    if let Some(handler) = message_handler.read().as_ref() {
                                        handler(message);
                                    }
                                }
                            }
                            None => {
                                // Connection closed
                                break;
                            }
                        }
                    }
                    connection_manager.remove_connection(&remote_address);
                });
            }
        }
    }

    /// Creates and sends a request message.
    pub async fn request(
        &self,
        target_grain: GrainId,
        target_silo: SiloAddress,
        interface_type: GrainInterfaceType,
        method_id: u32,
        body: Bytes,
    ) -> Result<Message, MessagingError> {
        let message = Message::new_request(
            target_grain,
            interface_type,
            method_id,
            body,
            self.local_address.clone(),
        )
        .with_target_silo(target_silo)
        .with_timeout(self.config.request_timeout);

        self.send_request(message).await
    }

    /// Sends a response to a request.
    pub async fn send_response(&self, response: Message) -> Result<(), MessagingError> {
        if !response.is_response() {
            return Err(MessagingError::InvalidMessageType(
                "Expected response message".to_string(),
            ));
        }

        self.send(response).await
    }

    /// Returns the connection manager.
    pub fn connection_manager(&self) -> &ConnectionManager {
        &self.connection_manager
    }

    /// Returns whether the message center is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    /// Shuts down the message center.
    pub async fn shutdown(&self) {
        self.running.store(false, Ordering::Release);
        let _ = self.shutdown_tx.send(()).await;
        self.connection_manager.close_all();

        // Complete all pending requests with error
        // Clear all pending requests (responses will be dropped)
        self.pending_responses.clear();

        tracing::info!("MessageCenter shut down");
    }

    /// Listener loop - accepts incoming connections.
    async fn listener_loop(
        center: Arc<Self>,
        listener: TcpListener,
        mut shutdown_rx: mpsc::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            tracing::debug!("Accepted connection from {}", addr);
                            let center_clone = Arc::clone(&center);
                            tokio::spawn(Self::handle_incoming_connection(center_clone, stream, addr));
                        }
                        Err(e) => {
                            tracing::error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    break;
                }
            }
        }
    }

    /// Handles a new incoming connection.
    async fn handle_incoming_connection(
        center: Arc<Self>,
        stream: TcpStream,
        addr: std::net::SocketAddr,
    ) {
        // Disable Nagle's algorithm
        let _ = stream.set_nodelay(true);

        // For incoming connections, we don't know the remote silo address yet.
        // We'll use a placeholder until we receive the first message.
        let placeholder_address = SiloAddress::new(addr, 0);

        let (connection, handle) = Connection::new(
            stream,
            placeholder_address.clone(),
            center.local_address.clone(),
        );

        // Initially register with placeholder address
        center.connection_manager.register_incoming(Arc::clone(&connection));

        // Start the connection I/O loop
        let handle_task = tokio::spawn(handle.run());

        // Track whether we've updated the remote address
        let mut actual_remote_address = placeholder_address.clone();
        let mut address_updated = false;

        // Process incoming messages
        loop {
            match connection.receive().await {
                Some(message) => {
                    // On first message, learn the actual remote silo address
                    if !address_updated {
                        let sender = &message.sending_silo;
                        if sender.generation() != 0 {
                            // Remove the placeholder registration
                            center.connection_manager.remove_connection(&placeholder_address);
                            // Update connection's remote address
                            connection.set_remote_address(sender.clone());
                            // Re-register with the actual address
                            center.connection_manager.register_incoming(Arc::clone(&connection));
                            actual_remote_address = sender.clone();
                            address_updated = true;
                        }
                    }
                    center.handle_message(message);
                }
                None => {
                    // Connection closed
                    break;
                }
            }
        }

        // Clean up with the actual address
        center.connection_manager.remove_connection(&actual_remote_address);
        handle_task.abort();
    }

    /// Handles an incoming message.
    fn handle_message(&self, message: Message) {
        if message.is_response() {
            // This is a response to a pending request
            if let Some((_, pending)) = self.pending_responses.remove(&message.id) {
                let result = if message.is_rejection() {
                    let info = message.rejection_info.as_ref().unwrap();
                    Err(MessagingError::RequestRejected {
                        rejection_type: info.rejection_type,
                        message: info.message.clone(),
                    })
                } else {
                    Ok(message)
                };
                let _ = pending.sender.send(result);
            } else {
                tracing::warn!("Received response for unknown request: {}", message.id);
            }
        } else {
            // This is an incoming request or one-way message
            if let Some(handler) = self.message_handler.read().as_ref() {
                handler(message);
            } else {
                tracing::warn!("No message handler set, dropping message: {}", message);
            }
        }
    }

    /// Cleanup loop - removes expired pending requests.
    async fn cleanup_loop(center: Arc<Self>) {
        let mut interval = tokio::time::interval(center.config.cleanup_interval);

        while center.is_running() {
            interval.tick().await;

            let now = std::time::Instant::now();
            let mut expired = Vec::new();

            for entry in center.pending_responses.iter() {
                if entry.value().timeout_at <= now {
                    expired.push(entry.key().clone());
                }
            }

            for correlation_id in expired {
                if let Some((_, pending)) = center.pending_responses.remove(&correlation_id) {
                    let _ = pending.sender.send(Err(MessagingError::RequestTimeout));
                }
            }
        }
    }
}

impl Drop for MessageCenter {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::GrainType;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;

    fn test_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            format!("127.0.0.1:{}", port).parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    fn test_grain_id() -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), "key1".into())
    }

    #[tokio::test]
    async fn test_message_center_creation() {
        let center = MessageCenter::new(test_silo_address(0)).await.unwrap();
        assert!(center.is_running());
    }

    #[tokio::test]
    async fn test_send_request_response() {
        // Create two message centers
        let center1 = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let center2 = MessageCenter::new(test_silo_address(0)).await.unwrap();

        let addr1 = center1.local_address().clone();
        let addr2 = center2.local_address().clone();

        // Set up handler on center2 to echo back
        let center2_clone = Arc::clone(&center2);
        center2.set_message_handler(move |msg| {
            if msg.is_request() {
                let response = msg.create_response(Bytes::from_static(b"pong"));
                let center = Arc::clone(&center2_clone);
                tokio::spawn(async move {
                    let _ = center.send_response(response.with_target_silo(addr1.clone())).await;
                });
            }
        });

        // Send request from center1 to center2
        let response = center1
            .request(
                test_grain_id(),
                addr2,
                GrainInterfaceType::create("ITestGrain"),
                1,
                Bytes::from_static(b"ping"),
            )
            .await
            .unwrap();

        assert!(response.is_response());
        assert_eq!(response.body.as_ref(), b"pong");

        center1.shutdown().await;
        center2.shutdown().await;
    }

    #[tokio::test]
    async fn test_message_handler_called() {
        let center = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let addr = center.local_address().clone();

        let call_count = Arc::new(AtomicUsize::new(0));
        let call_count_clone = Arc::clone(&call_count);

        center.set_message_handler(move |_msg| {
            call_count_clone.fetch_add(1, Ordering::Relaxed);
        });

        // Create a second center to send to us
        let center2 = MessageCenter::new(test_silo_address(0)).await.unwrap();

        // Send a one-way message
        let msg = Message::new_one_way(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            center2.local_address().clone(),
        )
        .with_target_silo(addr);

        center2.send(msg).await.unwrap();

        // Wait for message to be delivered
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        assert!(call_count.load(Ordering::Relaxed) >= 1);

        center.shutdown().await;
        center2.shutdown().await;
    }

    #[tokio::test]
    async fn test_request_timeout() {
        let center = MessageCenter::with_config(
            test_silo_address(0),
            MessageCenterConfig {
                request_timeout: std::time::Duration::from_millis(100),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        // Create a target that doesn't respond
        let target_center = MessageCenter::new(test_silo_address(0)).await.unwrap();
        let target_addr = target_center.local_address().clone();

        // Don't set a handler - request will timeout

        let result = center
            .request(
                test_grain_id(),
                target_addr,
                GrainInterfaceType::create("ITestGrain"),
                1,
                Bytes::new(),
            )
            .await;

        assert!(matches!(result, Err(MessagingError::RequestTimeout)));

        center.shutdown().await;
        target_center.shutdown().await;
    }
}
