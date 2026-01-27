//! Orleans ClusterClient implementation.
//!
//! The ClusterClient provides external client access to an Orleans cluster,
//! enabling applications to invoke grain methods without being a silo.

use crate::callback::{start_expiration_task, CallbackDataManager};
use crate::error::{ClientError, ClientResult, ClientStatus};
use crate::gateway::GatewayManager;
use crate::options::{ClientOptions, GatewayOptions};
use orleans_clustering::IMembershipTable;
use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
use orleans_messaging::{ConnectionManager, Direction, GrainInterfaceType, Message};
use orleans_runtime::{
    GrainFactory, GrainFactoryExt, GrainInterfaceMarker, IGrainFactory, IGrainReference,
    InterfaceResolver, MapInterfaceResolver, MessageSender, RuntimeResult,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};

/// Orleans cluster client for external applications.
///
/// The ClusterClient allows applications to connect to an Orleans cluster
/// and invoke methods on grains without being a full silo. It manages
/// connections to gateway silos and handles request/response correlation.
///
/// # Example
///
/// ```rust,no_run
/// use orleans_client::{ClusterClient, ClientBuilder, ClientOptions};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Build and connect the client
/// let client = ClientBuilder::new()
///     .with_cluster_id("my-cluster")
///     .with_gateway("127.0.0.1:30000".parse()?)
///     .build()?;
///
/// client.connect().await?;
///
/// // Get a grain reference and invoke methods
/// // let grain = client.get_grain::<IMyGrain>("my-key");
/// // let result = grain.my_method().await?;
///
/// // Disconnect when done
/// client.disconnect().await?;
/// # Ok(())
/// # }
/// ```
pub struct ClusterClient {
    /// Configuration options.
    options: ClientOptions,

    /// Current status.
    status: AtomicU8,

    /// Gateway manager for routing requests.
    gateway_manager: Arc<GatewayManager>,

    /// Connection manager for TCP connections.
    connection_manager: Arc<ConnectionManager>,

    /// Callback manager for pending requests.
    callback_manager: Arc<CallbackDataManager>,

    /// Grain factory for creating grain references.
    grain_factory: Arc<RwLock<Option<GrainFactory>>>,

    /// Interface resolver for grain types.
    interface_resolver: Arc<dyn InterfaceResolver>,

    /// Shutdown signal.
    shutdown: CancellationToken,

    /// Background task handles.
    tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,

    /// Client's virtual silo address (for message routing).
    client_address: SiloAddress,

    /// Optional membership table for gateway discovery.
    membership_table: Option<Arc<dyn IMembershipTable>>,
}

impl ClusterClient {
    /// Create a new cluster client with the given options.
    #[instrument(skip(options))]
    pub fn new(options: ClientOptions) -> ClientResult<Self> {
        // Validate options first
        options
            .validate()
            .map_err(|e| ClientError::Configuration(e))?;

        // Create a virtual client address
        let client_address = Self::create_client_address();

        // Create connection manager
        let connection_manager = Arc::new(ConnectionManager::new(client_address.clone()));

        // Create gateway manager
        let gateway_options = GatewayOptions {
            refresh_period: options.gateway_refresh_interval,
            ..Default::default()
        };
        let gateway_manager = Arc::new(GatewayManager::new(
            connection_manager.clone(),
            gateway_options,
        ));

        // Add initial gateways
        for endpoint in &options.gateway_endpoints {
            let silo_address = SiloAddress::new(*endpoint, 0);
            gateway_manager.add_gateway(silo_address);
        }

        // Create callback manager
        let callback_manager = Arc::new(CallbackDataManager::new(options.max_pending_requests));

        // Create interface resolver
        let interface_resolver: Arc<dyn InterfaceResolver> = Arc::new(MapInterfaceResolver::new());

        info!(
            cluster_id = %options.cluster_id,
            gateway_count = options.gateway_endpoints.len(),
            "created cluster client"
        );

        Ok(Self {
            options,
            status: AtomicU8::new(ClientStatus::Created as u8),
            gateway_manager,
            connection_manager,
            callback_manager,
            grain_factory: Arc::new(RwLock::new(None)),
            interface_resolver,
            shutdown: CancellationToken::new(),
            tasks: Arc::new(RwLock::new(Vec::new())),
            client_address,
            membership_table: None,
        })
    }

    /// Create a new cluster client with a membership table for gateway discovery.
    pub fn with_membership_table(
        options: ClientOptions,
        membership_table: Arc<dyn IMembershipTable>,
    ) -> ClientResult<Self> {
        let mut client = Self::new(options)?;
        client.membership_table = Some(membership_table.clone());

        // Update gateway manager with membership table
        let gateway_options = GatewayOptions {
            refresh_period: client.options.gateway_refresh_interval,
            ..Default::default()
        };
        client.gateway_manager = Arc::new(GatewayManager::with_membership_table(
            client.connection_manager.clone(),
            gateway_options,
            membership_table,
        ));

        Ok(client)
    }

    /// Create a virtual client address for this client instance.
    fn create_client_address() -> SiloAddress {
        // Use a random port and unique generation to identify this client
        let port = rand_port();
        let generation = chrono::Utc::now().timestamp_millis();
        let addr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, generation)
    }

    /// Get the current status.
    pub fn status(&self) -> ClientStatus {
        match self.status.load(Ordering::SeqCst) {
            0 => ClientStatus::Created,
            1 => ClientStatus::Connecting,
            2 => ClientStatus::Connected,
            3 => ClientStatus::Disconnecting,
            4 => ClientStatus::Disconnected,
            _ => ClientStatus::Disconnected,
        }
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        self.status() == ClientStatus::Connected
    }

    /// Get the client's virtual address.
    pub fn client_address(&self) -> &SiloAddress {
        &self.client_address
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> &str {
        &self.options.cluster_id
    }

    /// Connect to the cluster.
    #[instrument(skip(self))]
    pub async fn connect(&self) -> ClientResult<()> {
        // Check current status
        let current = self.status();
        match current {
            ClientStatus::Connected => return Err(ClientError::AlreadyConnected),
            ClientStatus::Connecting => return Err(ClientError::Connecting),
            ClientStatus::Disconnecting => {
                return Err(ClientError::Internal("client is disconnecting".into()))
            }
            _ => {}
        }

        // Transition to connecting
        self.status
            .store(ClientStatus::Connecting as u8, Ordering::SeqCst);

        info!(
            cluster_id = %self.options.cluster_id,
            gateway_count = self.gateway_manager.gateway_count(),
            "connecting to cluster"
        );

        // If we have a membership table, refresh gateways
        if self.membership_table.is_some() {
            if let Err(e) = self.gateway_manager.refresh_gateways().await {
                warn!(error = %e, "failed to refresh gateways from membership table");
            }
        }

        // Verify we have at least one gateway
        if self.gateway_manager.gateway_count() == 0 {
            self.status
                .store(ClientStatus::Disconnected as u8, Ordering::SeqCst);
            return Err(ClientError::NoGatewaysAvailable);
        }

        // Start background tasks
        let mut tasks = self.tasks.write().await;

        // Start callback expiration task
        let expiration_handle = start_expiration_task(
            self.callback_manager.clone(),
            Duration::from_secs(1),
            self.shutdown.clone(),
        );
        tasks.push(expiration_handle);

        // Start gateway manager
        let gateway_handle = self.gateway_manager.start();
        tasks.push(gateway_handle);

        // Start message receiver task
        let receiver_handle = self.start_receiver_task();
        tasks.push(receiver_handle);

        // Create the grain factory now that we're connected
        {
            let message_sender = Arc::new(ClientMessageSender::new(
                self.gateway_manager.clone(),
                self.callback_manager.clone(),
                self.options.response_timeout,
            ));

            let factory =
                GrainFactory::new(message_sender, self.interface_resolver.clone());

            let mut factory_guard = self.grain_factory.write().await;
            *factory_guard = Some(factory);
        }

        // Mark as connected
        self.status
            .store(ClientStatus::Connected as u8, Ordering::SeqCst);

        info!(
            cluster_id = %self.options.cluster_id,
            "connected to cluster"
        );

        Ok(())
    }

    /// Disconnect from the cluster.
    #[instrument(skip(self))]
    pub async fn disconnect(&self) -> ClientResult<()> {
        let current = self.status();
        if current != ClientStatus::Connected {
            return Ok(()); // Already disconnected or not connected
        }

        // Transition to disconnecting
        self.status
            .store(ClientStatus::Disconnecting as u8, Ordering::SeqCst);

        info!(
            cluster_id = %self.options.cluster_id,
            "disconnecting from cluster"
        );

        // Signal shutdown
        self.shutdown.cancel();

        // Fail all pending callbacks
        let failed_count = self.callback_manager.fail_all(ClientError::ShuttingDown);
        debug!(failed_count = failed_count, "failed pending callbacks");

        // Wait for background tasks
        let mut tasks = self.tasks.write().await;
        for task in tasks.drain(..) {
            let _ = task.await;
        }

        // Stop gateway manager
        self.gateway_manager.stop();

        // Clear grain factory
        {
            let mut factory_guard = self.grain_factory.write().await;
            *factory_guard = None;
        }

        // Mark as disconnected
        self.status
            .store(ClientStatus::Disconnected as u8, Ordering::SeqCst);

        info!(
            cluster_id = %self.options.cluster_id,
            "disconnected from cluster"
        );

        Ok(())
    }

    /// Start the message receiver task.
    fn start_receiver_task(&self) -> tokio::task::JoinHandle<()> {
        let callback_manager = self.callback_manager.clone();
        let gateway_manager = self.gateway_manager.clone();
        let connection_manager = self.connection_manager.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut poll_interval = tokio::time::interval(Duration::from_millis(10));

            loop {
                tokio::select! {
                    _ = poll_interval.tick() => {
                        // Poll all active connections for incoming messages
                        let connections = connection_manager.active_connections();
                        for connection in connections {
                            // Try to receive a message (non-blocking via timeout)
                            if let Ok(Some(message)) = tokio::time::timeout(
                                Duration::from_millis(1),
                                connection.receive(),
                            ).await {
                                let from = connection.remote_address();
                                trace!(
                                    correlation_id = %message.id(),
                                    from = %from,
                                    "received message from gateway"
                                );

                                // Record success for the gateway
                                gateway_manager.record_success(&from);

                                // Complete the callback
                                if message.direction() == Direction::Response {
                                    callback_manager.try_complete(message);
                                }
                            }
                        }
                    }
                    _ = shutdown.cancelled() => {
                        debug!("message receiver task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Get a grain reference by type and key.
    #[instrument(skip(self))]
    pub async fn get_grain_reference(
        &self,
        grain_type: GrainType,
        key: IdSpan,
    ) -> ClientResult<Arc<dyn IGrainReference>> {
        let factory_guard = self.grain_factory.read().await;
        let factory = factory_guard
            .as_ref()
            .ok_or(ClientError::NotConnected)?;

        Ok(factory.get_grain_reference(grain_type, key))
    }

    /// Get a grain reference by ID.
    #[instrument(skip(self))]
    pub async fn get_grain_reference_by_id(
        &self,
        grain_id: GrainId,
        interface_type: GrainInterfaceType,
    ) -> ClientResult<Arc<dyn IGrainReference>> {
        let factory_guard = self.grain_factory.read().await;
        let factory = factory_guard
            .as_ref()
            .ok_or(ClientError::NotConnected)?;

        Ok(factory.get_grain_reference_by_id(grain_id, interface_type))
    }

    /// Get a typed grain reference.
    pub async fn get_grain<T: GrainInterfaceMarker>(
        &self,
        key: &str,
    ) -> ClientResult<orleans_runtime::grain_reference::TypedGrainReference<T>> {
        let factory_guard = self.grain_factory.read().await;
        let factory = factory_guard
            .as_ref()
            .ok_or(ClientError::NotConnected)?;

        Ok(factory.get_grain::<T>(key))
    }

    /// Get the number of pending requests.
    pub fn pending_request_count(&self) -> usize {
        self.callback_manager.active_count()
    }

    /// Get the number of connected gateways.
    pub fn gateway_count(&self) -> usize {
        self.gateway_manager.gateway_count()
    }

    /// Get the number of healthy gateways.
    pub fn healthy_gateway_count(&self) -> usize {
        self.gateway_manager.healthy_gateway_count()
    }
}

/// Generate a random port for the client address.
fn rand_port() -> u16 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (40000 + (nanos % 10000)) as u16
}

/// Message sender implementation for the cluster client.
struct ClientMessageSender {
    gateway_manager: Arc<GatewayManager>,
    callback_manager: Arc<CallbackDataManager>,
    default_timeout: Duration,
}

impl ClientMessageSender {
    fn new(
        gateway_manager: Arc<GatewayManager>,
        callback_manager: Arc<CallbackDataManager>,
        default_timeout: Duration,
    ) -> Self {
        Self {
            gateway_manager,
            callback_manager,
            default_timeout,
        }
    }
}

impl MessageSender for ClientMessageSender {
    fn send_request(
        &self,
        message: Message,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Future<Output = RuntimeResult<Message>> + Send + '_>> {
        let timeout = timeout.unwrap_or(self.default_timeout);

        Box::pin(async move {
            // Add callback for response
            let receiver = self
                .callback_manager
                .add(message.clone(), timeout)
                .map_err(|e| orleans_runtime::RuntimeError::Internal(e.to_string()))?;

            // Send the message through a gateway
            self.gateway_manager
                .send(message)
                .await
                .map_err(|e| orleans_runtime::RuntimeError::Internal(e.to_string()))?;

            // Wait for the response
            let response = tokio::time::timeout(timeout, receiver)
                .await
                .map_err(|_| {
                    orleans_runtime::RuntimeError::Timeout {
                        duration_ms: timeout.as_millis() as u64,
                    }
                })?
                .map_err(|_| {
                    orleans_runtime::RuntimeError::Internal("callback channel closed".into())
                })?
                .map_err(|e| orleans_runtime::RuntimeError::Internal(e.to_string()))?;

            Ok(response)
        })
    }

    fn send_one_way(&self, message: Message) -> RuntimeResult<()> {
        let gateway_manager = self.gateway_manager.clone();

        tokio::spawn(async move {
            if let Err(e) = gateway_manager.send(message).await {
                warn!(error = %e, "failed to send one-way message");
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_options() -> ClientOptions {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        ClientOptions::new("test-cluster").with_gateway(addr)
    }

    #[test]
    fn test_create_client() {
        let client = ClusterClient::new(test_options()).unwrap();
        assert_eq!(client.status(), ClientStatus::Created);
        assert!(!client.is_connected());
    }

    #[test]
    fn test_client_address() {
        let client = ClusterClient::new(test_options()).unwrap();
        let addr = client.client_address();
        assert!(addr.endpoint().port() >= 40000);
        assert!(addr.endpoint().port() < 50000);
    }

    #[test]
    fn test_cluster_id() {
        let client = ClusterClient::new(test_options()).unwrap();
        assert_eq!(client.cluster_id(), "test-cluster");
    }

    #[test]
    fn test_invalid_options() {
        let options = ClientOptions::new(""); // Empty cluster ID
        let result = ClusterClient::new(options);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connect_with_gateway() {
        let options = ClientOptions::new("test")
            .with_gateway("127.0.0.1:59999".parse().unwrap()); // Non-existent gateway

        let _client = ClusterClient::new(options).unwrap();
        // Note: connect would try to connect to the gateway
        // In a real test, we'd mock the connection
    }

    #[tokio::test]
    async fn test_disconnect_not_connected() {
        let client = ClusterClient::new(test_options()).unwrap();
        // Should not error when disconnecting while not connected
        let result = client.disconnect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_grain_reference_not_connected() {
        let client = ClusterClient::new(test_options()).unwrap();

        let result = client
            .get_grain_reference(GrainType::create("Test"), IdSpan::from_str("key"))
            .await;

        assert!(matches!(result, Err(ClientError::NotConnected)));
    }

    #[test]
    fn test_pending_request_count() {
        let client = ClusterClient::new(test_options()).unwrap();
        assert_eq!(client.pending_request_count(), 0);
    }

    #[test]
    fn test_gateway_count() {
        let client = ClusterClient::new(test_options()).unwrap();
        assert_eq!(client.gateway_count(), 1);
    }

    #[test]
    fn test_client_status_transitions() {
        assert_eq!(ClientStatus::Created as u8, 0);
        assert_eq!(ClientStatus::Connecting as u8, 1);
        assert_eq!(ClientStatus::Connected as u8, 2);
        assert_eq!(ClientStatus::Disconnecting as u8, 3);
        assert_eq!(ClientStatus::Disconnected as u8, 4);
    }
}
