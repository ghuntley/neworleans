//! Silo implementation - the main entry point for hosting grains.
//!
//! A silo is a single node in an Orleans cluster. It hosts grain activations,
//! handles message routing, and participates in cluster membership.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use orleans_clustering::{
    IMembershipTable, MembershipAgent, MembershipTableManager,
};
use orleans_core::SiloAddress;
use orleans_directory::DistributedGrainDirectory;
use orleans_messaging::{Message, MessageCenter};
use orleans_runtime::{
    Catalog, DeactivationReason, Dispatcher,
    GrainFactory, GrainTypeData, ConventionInterfaceResolver,
    MessageSender, RuntimeResult,
};

use crate::config::SiloConfig;
use crate::error::{SiloError, SiloResult};

/// Adapter to make MessageCenter implement MessageSender.
struct MessageCenterSender {
    center: Arc<MessageCenter>,
}

impl MessageSender for MessageCenterSender {
    fn send_request(
        &self,
        message: Message,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn std::future::Future<Output = RuntimeResult<Message>> + Send + '_>> {
        let center = self.center.clone();
        Box::pin(async move {
            // Update message with timeout
            let message = if let Some(t) = timeout {
                message.with_timeout(Some(t))
            } else {
                message
            };

            // Determine target silo from message
            let target_silo = message.target_silo().cloned()
                .ok_or_else(|| orleans_runtime::RuntimeError::Internal(
                    "No target silo in message".to_string()
                ))?;

            // Use the request method which properly tracks correlation
            center.request(
                message.target_grain().clone(),
                target_silo,
                message.interface_type().clone(),
                message.method_id(),
                message.body().clone(),
            ).await.map_err(|e| orleans_runtime::RuntimeError::Internal(e.to_string()))
        })
    }

    fn send_one_way(&self, message: Message) -> RuntimeResult<()> {
        let center = self.center.clone();
        tokio::spawn(async move {
            if let Err(e) = center.send(message).await {
                warn!(error = %e, "Failed to send one-way message");
            }
        });
        Ok(())
    }
}

/// Directory-aware message sender that looks up grain locations before sending.
///
/// This sender consults the grain directory to find the target silo for a grain,
/// enabling cross-silo grain invocation with location transparency.
///
/// It also implements failure handling with retry logic:
/// - When a request fails due to silo unavailability, it invalidates the cache
/// - Retries by finding the new primary silo
/// - Supports configurable max retries
struct DirectoryAwareMessageSender {
    center: Arc<MessageCenter>,
    directory: Arc<DistributedGrainDirectory>,
    local_silo: SiloAddress,
}

/// Maximum number of retry attempts for failed requests.
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Delay between retry attempts (in milliseconds).
const RETRY_DELAY_MS: u64 = 100;

impl MessageSender for DirectoryAwareMessageSender {
    fn send_request(
        &self,
        message: Message,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn std::future::Future<Output = RuntimeResult<Message>> + Send + '_>> {
        let center = self.center.clone();
        let directory = self.directory.clone();
        let local_silo = self.local_silo.clone();

        Box::pin(async move {
            // Update message with timeout
            let message = if let Some(t) = timeout {
                message.with_timeout(Some(t))
            } else {
                message
            };

            let grain_id = message.target_grain().clone();
            let mut last_error: Option<orleans_runtime::RuntimeError> = None;
            let mut tried_silos: Vec<SiloAddress> = Vec::new();

            // Retry loop for handling silo failures
            for attempt in 0..MAX_RETRY_ATTEMPTS {
                // Determine target silo:
                // 1. If message already has a target_silo (and this is first attempt), use it
                // 2. Otherwise, look up in directory (skip cache if retrying)
                // 3. If not in directory, get the primary silo from consistent hash
                let target_silo = if attempt == 0 && message.target_silo().is_some() {
                    message.target_silo().unwrap().clone()
                } else {
                    // On retry, skip the cache and go directly to the directory/ring
                    let silo = if attempt > 0 {
                        // Invalidate cache entry for the grain since the previous silo failed
                        if let Some(failed_silo) = tried_silos.last() {
                            debug!(
                                grain_id = %grain_id,
                                failed_silo = %failed_silo,
                                attempt = attempt,
                                "Invalidating cache after silo failure"
                            );
                            directory.cache().invalidate_silo(failed_silo);
                        }

                        // Get primary silo from the hash ring (skipping cache)
                        let primary = directory.get_primary_silo(&grain_id)
                            .map_err(|e| orleans_runtime::RuntimeError::Internal(
                                format!("Failed to get primary silo: {}", e)
                            ))?;

                        // If primary was already tried, try to find an alternative
                        if tried_silos.contains(&primary) {
                            // Get all silos from the ring and pick one we haven't tried
                            let all_silos = directory.ring().get_silos();
                            let alternative = all_silos.iter()
                                .find(|s| !tried_silos.contains(s))
                                .cloned();

                            if let Some(alt) = alternative {
                                debug!(
                                    grain_id = %grain_id,
                                    alternative = %alt,
                                    "Using alternative silo after primary failed"
                                );
                                alt
                            } else {
                                // No untried silos available
                                return Err(orleans_runtime::RuntimeError::Internal(
                                    format!("All silos have been tried for grain {}", grain_id)
                                ));
                            }
                        } else {
                            primary
                        }
                    } else {
                        // First attempt: try directory lookup
                        match directory.lookup(&grain_id).await {
                            Ok(Some(address)) => {
                                if let Some(silo) = address.silo_address() {
                                    debug!(
                                        grain_id = %grain_id,
                                        silo = %silo,
                                        "Found grain in directory"
                                    );
                                    silo.clone()
                                } else {
                                    directory.get_primary_silo(&grain_id)
                                        .map_err(|e| orleans_runtime::RuntimeError::Internal(
                                            format!("Failed to get primary silo: {}", e)
                                        ))?
                                }
                            }
                            Ok(None) => {
                                let primary = directory.get_primary_silo(&grain_id)
                                    .map_err(|e| orleans_runtime::RuntimeError::Internal(
                                        format!("Failed to get primary silo: {}", e)
                                    ))?;
                                debug!(
                                    grain_id = %grain_id,
                                    primary = %primary,
                                    "Grain not in directory, routing to primary silo"
                                );
                                primary
                            }
                            Err(e) => {
                                warn!(
                                    grain_id = %grain_id,
                                    error = %e,
                                    "Directory lookup failed, using primary silo"
                                );
                                directory.get_primary_silo(&grain_id)
                                    .map_err(|e| orleans_runtime::RuntimeError::Internal(
                                        format!("Failed to get primary silo: {}", e)
                                    ))?
                            }
                        }
                    };
                    silo
                };

                // Track this silo as tried
                if !tried_silos.contains(&target_silo) {
                    tried_silos.push(target_silo.clone());
                }

                debug!(
                    grain_id = %grain_id,
                    target_silo = %target_silo,
                    local_silo = %local_silo,
                    attempt = attempt,
                    "Sending request"
                );

                // Attempt to send the request
                let result = center.request(
                    grain_id.clone(),
                    target_silo.clone(),
                    message.interface_type().clone(),
                    message.method_id(),
                    message.body().clone(),
                ).await;

                match result {
                    Ok(response) => {
                        // Check if the response is a rejection that indicates silo failure
                        if let Some(rejection_info) = response.rejection_info() {
                            use orleans_messaging::RejectionType;
                            match rejection_info.rejection_type() {
                                RejectionType::SiloUnavailable | RejectionType::Transient => {
                                    // Silo is unavailable, retry on another silo
                                    warn!(
                                        grain_id = %grain_id,
                                        target_silo = %target_silo,
                                        rejection = ?rejection_info.rejection_type(),
                                        attempt = attempt,
                                        "Request rejected due to silo unavailability, will retry"
                                    );
                                    last_error = Some(orleans_runtime::RuntimeError::Internal(
                                        format!("Silo {} unavailable: {}", target_silo, rejection_info.message())
                                    ));

                                    // Wait before retry
                                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
                                    }
                                    continue;
                                }
                                _ => {
                                    // Other rejection types are not retryable
                                    return Ok(response);
                                }
                            }
                        } else {
                            // Successful response
                            if attempt > 0 {
                                info!(
                                    grain_id = %grain_id,
                                    target_silo = %target_silo,
                                    attempts = attempt + 1,
                                    "Request succeeded after retry"
                                );
                            }
                            return Ok(response);
                        }
                    }
                    Err(e) => {
                        // Network or other error - might be silo failure
                        warn!(
                            grain_id = %grain_id,
                            target_silo = %target_silo,
                            error = %e,
                            attempt = attempt,
                            "Request failed, will retry on another silo"
                        );
                        last_error = Some(orleans_runtime::RuntimeError::Internal(e.to_string()));

                        // Wait before retry
                        if attempt < MAX_RETRY_ATTEMPTS - 1 {
                            tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * (attempt as u64 + 1))).await;
                        }
                        continue;
                    }
                }
            }

            // All retries exhausted
            Err(last_error.unwrap_or_else(|| orleans_runtime::RuntimeError::Internal(
                format!("All {} retry attempts failed for grain {}", MAX_RETRY_ATTEMPTS, grain_id)
            )))
        })
    }

    fn send_one_way(&self, message: Message) -> RuntimeResult<()> {
        let center = self.center.clone();
        let directory = self.directory.clone();

        tokio::spawn(async move {
            let grain_id = message.target_grain();

            // Determine target silo
            let target_silo = if let Some(silo) = message.target_silo() {
                silo.clone()
            } else {
                match directory.lookup(grain_id).await {
                    Ok(Some(address)) => {
                        if let Some(silo) = address.silo_address() {
                            silo.clone()
                        } else {
                            match directory.get_primary_silo(grain_id) {
                                Ok(silo) => silo,
                                Err(e) => {
                                    warn!(error = %e, "Failed to get primary silo for one-way");
                                    return;
                                }
                            }
                        }
                    }
                    _ => {
                        match directory.get_primary_silo(grain_id) {
                            Ok(silo) => silo,
                            Err(e) => {
                                warn!(error = %e, "Failed to get primary silo for one-way");
                                return;
                            }
                        }
                    }
                }
            };

            let message = message.with_target_silo(Some(target_silo));
            if let Err(e) = center.send(message).await {
                warn!(error = %e, "Failed to send one-way message");
            }
        });
        Ok(())
    }
}

/// The lifecycle state of a silo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SiloState {
    /// Silo is created but not started.
    Created,
    /// Silo is starting up.
    Starting,
    /// Silo is running and accepting requests.
    Running,
    /// Silo is shutting down.
    Stopping,
    /// Silo has stopped.
    Stopped,
}

impl std::fmt::Display for SiloState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SiloState::Created => write!(f, "Created"),
            SiloState::Starting => write!(f, "Starting"),
            SiloState::Running => write!(f, "Running"),
            SiloState::Stopping => write!(f, "Stopping"),
            SiloState::Stopped => write!(f, "Stopped"),
        }
    }
}

/// A silo - a single node in an Orleans cluster.
pub struct Silo {
    /// The silo's address.
    silo_address: SiloAddress,

    /// The silo's configuration.
    config: SiloConfig,

    /// The grain types registered with this silo.
    grain_types: Vec<Arc<GrainTypeData>>,

    /// The membership table.
    membership_table: Arc<dyn IMembershipTable>,

    /// The message center.
    message_center: Arc<MessageCenter>,

    /// The membership agent.
    membership_agent: Option<MembershipAgent>,

    /// The membership manager.
    membership_manager: Option<Arc<MembershipTableManager>>,

    /// The grain directory.
    directory: Option<Arc<DistributedGrainDirectory>>,

    /// The catalog.
    catalog: Option<Arc<Catalog>>,

    /// The dispatcher.
    dispatcher: Option<Arc<Dispatcher>>,

    /// Current state.
    state: RwLock<SiloState>,

    /// Shutdown signal sender.
    shutdown_tx: watch::Sender<bool>,

    /// Shutdown signal receiver.
    shutdown_rx: watch::Receiver<bool>,
}

impl Silo {
    /// Create a new silo (internal - use SiloBuilder).
    pub(crate) async fn new(
        config: SiloConfig,
        grain_types: Vec<Arc<GrainTypeData>>,
        membership_table: Arc<dyn IMembershipTable>,
    ) -> SiloResult<Self> {
        let silo_address = SiloAddress::new(config.listen_address, config.generation);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        // Create message center (starts listening immediately)
        let message_center = MessageCenter::with_config(silo_address.clone(), config.messaging.clone()).await?;

        info!(
            silo = %message_center.local_address(),
            "Silo created"
        );

        Ok(Self {
            silo_address: message_center.local_address().clone(),
            config,
            grain_types,
            membership_table,
            message_center,
            membership_agent: None,
            membership_manager: None,
            directory: None,
            catalog: None,
            dispatcher: None,
            state: RwLock::new(SiloState::Created),
            shutdown_tx,
            shutdown_rx,
        })
    }

    /// Start the silo.
    ///
    /// This method:
    /// 1. Initializes and starts the message center
    /// 2. Joins the cluster
    /// 3. Sets up the grain directory
    /// 4. Creates the catalog and registers grain types
    /// 5. Creates and registers the dispatcher
    pub async fn start(&mut self) -> SiloResult<()> {
        // Check state
        {
            let mut state = self.state.write();
            if *state != SiloState::Created {
                return Err(SiloError::InvalidState {
                    expected: "Created".to_string(),
                    actual: state.to_string(),
                });
            }
            *state = SiloState::Starting;
        }

        info!(silo = %self.silo_address, "Starting silo");

        // Step 1: Create membership manager and agent
        let manager = Arc::new(MembershipTableManager::new(
            self.membership_table.clone(),
            self.silo_address.clone(),
            self.config.membership.clone(),
        ));
        self.membership_manager = Some(manager.clone());

        let agent = MembershipAgent::new(manager.clone(), self.config.membership.clone());

        // Step 2: Join the cluster
        agent.start().await?;
        self.membership_agent = Some(agent);

        info!(silo = %self.silo_address, "Joined cluster");

        // Step 3: Set up grain directory
        let directory = Arc::new(DistributedGrainDirectory::local_only(self.silo_address.clone()));

        // Add ourselves to the ring
        directory.ring().add_silo(self.silo_address.clone());

        // Add other active silos to the ring
        let snapshot = manager.get_snapshot();
        for silo in snapshot.get_active_silos() {
            if silo != &self.silo_address {
                directory.ring().add_silo(silo.clone());
            }
        }
        self.directory = Some(directory.clone());

        info!(
            silo = %self.silo_address,
            silos_in_ring = directory.ring().silo_count(),
            "Grain directory initialized"
        );

        // Step 4: Create catalog and register grain types
        let message_sender: Arc<dyn MessageSender> = Arc::new(MessageCenterSender {
            center: self.message_center.clone(),
        });
        let interface_resolver = Arc::new(ConventionInterfaceResolver);
        let grain_factory = Arc::new(GrainFactory::new(message_sender, interface_resolver));

        let catalog = Arc::new(Catalog::new(
            self.silo_address.clone(),
            grain_factory,
            self.config.catalog.clone(),
        ));

        for grain_type_data in &self.grain_types {
            catalog.register_grain_type(grain_type_data.clone());
        }
        self.catalog = Some(catalog.clone());

        info!(
            silo = %self.silo_address,
            grain_types = self.grain_types.len(),
            "Catalog initialized"
        );

        // Step 5: Create and register dispatcher
        let dispatcher = Arc::new(Dispatcher::new(
            self.silo_address.clone(),
            catalog.clone(),
            directory.clone(),
            self.message_center.clone(),
            self.config.dispatcher.clone(),
        ));
        dispatcher.clone().register_handler();
        self.dispatcher = Some(dispatcher);

        info!(silo = %self.silo_address, "Dispatcher registered");

        // Step 6: Set up membership change listener
        self.start_membership_listener(manager, directory);

        // Mark as running
        {
            let mut state = self.state.write();
            *state = SiloState::Running;
        }

        info!(silo = %self.silo_address, "Silo started successfully");

        Ok(())
    }

    /// Start listening for membership changes.
    fn start_membership_listener(
        &self,
        manager: Arc<MembershipTableManager>,
        directory: Arc<DistributedGrainDirectory>,
    ) {
        let mut rx = manager.subscribe();
        let silo_address = self.silo_address.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Ok(event) => {
                                debug!(
                                    silo = %silo_address,
                                    event = ?event,
                                    "Membership change"
                                );

                                // Update directory ring based on membership changes
                                let snapshot = manager.get_snapshot();
                                for active_silo in snapshot.get_active_silos() {
                                    if !directory.ring().contains_silo(active_silo) {
                                        directory.ring().add_silo(active_silo.clone());
                                        info!(
                                            silo = %silo_address,
                                            added = %active_silo,
                                            "Added silo to directory ring"
                                        );
                                    }
                                }

                                // Remove dead silos from directory
                                for (dead_silo, entry) in snapshot.all_entries() {
                                    if entry.entry.status == orleans_clustering::SiloStatus::Dead {
                                        if directory.ring().contains_silo(dead_silo) {
                                            directory.ring().remove_silo(dead_silo);
                                            directory.local_partition().remove_entries_for_silo(dead_silo);
                                            info!(
                                                silo = %silo_address,
                                                removed = %dead_silo,
                                                "Removed dead silo from directory"
                                            );
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!(
                                    silo = %silo_address,
                                    error = %e,
                                    "Membership subscription error"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!(silo = %silo_address, "Membership listener shutting down");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Stop the silo gracefully.
    pub async fn stop(&mut self) -> SiloResult<()> {
        // Check state
        {
            let mut state = self.state.write();
            if *state != SiloState::Running {
                return Err(SiloError::InvalidState {
                    expected: "Running".to_string(),
                    actual: state.to_string(),
                });
            }
            *state = SiloState::Stopping;
        }

        info!(silo = %self.silo_address, "Stopping silo");

        // Signal shutdown to background tasks
        let _ = self.shutdown_tx.send(true);

        // Step 1: Deactivate all grains
        if let Some(catalog) = &self.catalog {
            let grain_ids = catalog.all_grain_ids();
            info!(
                silo = %self.silo_address,
                count = grain_ids.len(),
                "Deactivating all grains"
            );

            for grain_id in grain_ids {
                if let Err(e) = catalog
                    .deactivate_grain(&grain_id, DeactivationReason::SiloShutdown)
                    .await
                {
                    warn!(
                        silo = %self.silo_address,
                        grain_id = %grain_id,
                        error = %e,
                        "Failed to deactivate grain"
                    );
                }
            }
        }

        // Step 2: Leave the cluster
        if let Some(agent) = &self.membership_agent {
            if let Err(e) = agent.stop().await {
                error!(
                    silo = %self.silo_address,
                    error = %e,
                    "Failed to leave cluster"
                );
            }
        }

        // Step 3: Shutdown message center
        self.message_center.shutdown().await;

        // Mark as stopped
        {
            let mut state = self.state.write();
            *state = SiloState::Stopped;
        }

        info!(silo = %self.silo_address, "Silo stopped");

        Ok(())
    }

    /// Get the silo's address.
    pub fn address(&self) -> &SiloAddress {
        &self.silo_address
    }

    /// Get the silo's state.
    pub fn state(&self) -> SiloState {
        *self.state.read()
    }

    /// Check if the silo is running.
    pub fn is_running(&self) -> bool {
        self.state() == SiloState::Running
    }

    /// Get the message center.
    pub fn message_center(&self) -> &Arc<MessageCenter> {
        &self.message_center
    }

    /// Get the membership manager.
    pub fn membership_manager(&self) -> Option<&Arc<MembershipTableManager>> {
        self.membership_manager.as_ref()
    }

    /// Get the grain directory.
    pub fn directory(&self) -> Option<&Arc<DistributedGrainDirectory>> {
        self.directory.as_ref()
    }

    /// Get the catalog.
    pub fn catalog(&self) -> Option<&Arc<Catalog>> {
        self.catalog.as_ref()
    }

    /// Get the dispatcher.
    pub fn dispatcher(&self) -> Option<&Arc<Dispatcher>> {
        self.dispatcher.as_ref()
    }

    /// Get a reference to the grain factory for creating grain references.
    ///
    /// The grain factory uses a directory-aware message sender that automatically
    /// looks up grain locations before sending, enabling cross-silo grain invocation
    /// with location transparency.
    pub fn grain_factory(&self) -> Option<Arc<GrainFactory>> {
        // Need both catalog and directory to be initialized
        if self.catalog.is_some() && self.directory.is_some() {
            let directory = self.directory.as_ref().unwrap().clone();
            let message_sender: Arc<dyn MessageSender> = Arc::new(DirectoryAwareMessageSender {
                center: self.message_center.clone(),
                directory,
                local_silo: self.silo_address.clone(),
            });
            let interface_resolver = Arc::new(ConventionInterfaceResolver);
            Some(Arc::new(GrainFactory::new(message_sender, interface_resolver)))
        } else {
            None
        }
    }

    /// Wait for the silo to stop.
    pub async fn wait_for_shutdown(&self) {
        let mut rx = self.shutdown_rx.clone();
        while !*rx.borrow() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
}

impl Drop for Silo {
    fn drop(&mut self) {
        if self.state() == SiloState::Running {
            warn!(
                silo = %self.silo_address,
                "Silo dropped while running - should call stop() first"
            );
        }
    }
}

impl std::fmt::Debug for Silo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Silo")
            .field("silo_address", &self.silo_address)
            .field("state", &self.state())
            .field("grain_types_count", &self.grain_types.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SiloBuilder;
    use async_trait::async_trait;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use orleans_runtime::{IGrain, IGrainActivator, IGrainMethodInvoker, IGrainContext, RuntimeResult};

    /// A simple test grain.
    pub struct HelloGrain {
        greeting_count: u32,
    }

    #[async_trait]
    impl IGrain for HelloGrain {
        fn grain_type() -> GrainType {
            GrainType::create("HelloGrain")
        }
    }

    impl HelloGrain {
        pub fn new() -> Self {
            Self { greeting_count: 0 }
        }

        pub fn say_hello(&mut self, name: &str) -> String {
            self.greeting_count += 1;
            format!("Hello, {}! (greeting #{})", name, self.greeting_count)
        }

        pub fn get_greeting_count(&self) -> u32 {
            self.greeting_count
        }
    }

    /// Activator for HelloGrain.
    pub struct HelloGrainActivator;

    impl IGrainActivator for HelloGrainActivator {
        fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
            Box::new(HelloGrain::new())
        }

        fn grain_type(&self) -> GrainType {
            HelloGrain::grain_type()
        }
    }

    /// Invoker for HelloGrain.
    pub struct HelloGrainInvoker;

    impl HelloGrainInvoker {
        const INTERFACE_TYPE: &'static str = "IHelloGrain";
        const METHOD_IDS: [u32; 2] = [1, 2];
    }

    impl IGrainMethodInvoker for HelloGrainInvoker {
        fn interface_type(&self) -> &str {
            Self::INTERFACE_TYPE
        }

        fn method_ids(&self) -> &[u32] {
            &Self::METHOD_IDS
        }

        fn invoke<'life0, 'life1, 'life2, 'life3, 'async_trait>(
            &'life0 self,
            grain: &'life1 mut dyn std::any::Any,
            _context: &'life2 dyn IGrainContext,
            method_id: u32,
            body: &'life3 [u8],
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'async_trait>>
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            'life2: 'async_trait,
            'life3: 'async_trait,
            Self: 'async_trait,
        {
            // Perform synchronous work immediately
            let grain = grain.downcast_mut::<HelloGrain>().unwrap();

            let result = match method_id {
                1 => {
                    // say_hello(name: String) -> String
                    let name = String::from_utf8_lossy(body);
                    let result = grain.say_hello(&name);
                    Ok(result.into_bytes())
                }
                2 => {
                    // get_greeting_count() -> u32
                    let count = grain.get_greeting_count();
                    Ok(count.to_le_bytes().to_vec())
                }
                _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                    interface_type: "IHelloGrain".to_string(),
                    method_id,
                }),
            };

            // Return an immediately-ready future
            Box::pin(std::future::ready(result))
        }
    }

    fn create_hello_grain_type() -> Arc<GrainTypeData> {
        let activator = Arc::new(HelloGrainActivator);
        let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(HelloGrainInvoker);

        let grain_type_data = GrainTypeData::new(HelloGrain::grain_type(), activator)
            .with_invoker("IHelloGrain", invoker);

        Arc::new(grain_type_data)
    }

    #[tokio::test]
    async fn test_silo_creation() {
        let grain_type = create_hello_grain_type();

        let silo = SiloBuilder::test()
            .register_grain_type(grain_type)
            .build()
            .await;

        assert!(silo.is_ok());
        let silo = silo.unwrap();
        assert_eq!(silo.state(), SiloState::Created);
    }

    #[tokio::test]
    async fn test_silo_start_stop() {
        let grain_type = create_hello_grain_type();

        let mut silo = SiloBuilder::test()
            .register_grain_type(grain_type)
            .build()
            .await
            .unwrap();

        // Start
        let result = silo.start().await;
        assert!(result.is_ok(), "Start failed: {:?}", result);
        assert_eq!(silo.state(), SiloState::Running);

        // Give it a moment
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Stop
        let result = silo.stop().await;
        assert!(result.is_ok(), "Stop failed: {:?}", result);
        assert_eq!(silo.state(), SiloState::Stopped);
    }

    #[tokio::test]
    async fn test_silo_components_initialized() {
        let grain_type = create_hello_grain_type();

        let mut silo = SiloBuilder::test()
            .register_grain_type(grain_type)
            .build()
            .await
            .unwrap();

        silo.start().await.unwrap();

        // Verify all components are initialized
        assert!(silo.membership_manager().is_some());
        assert!(silo.directory().is_some());
        assert!(silo.catalog().is_some());
        assert!(silo.dispatcher().is_some());

        silo.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_no_grain_types_error() {
        let result = SiloBuilder::test().build().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            SiloError::NoGrainTypesRegistered => {}
            e => panic!("Expected NoGrainTypesRegistered, got {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_grain_activation() {
        let grain_type = create_hello_grain_type();

        let mut silo = SiloBuilder::test()
            .register_grain_type(grain_type)
            .build()
            .await
            .unwrap();

        silo.start().await.unwrap();

        // Create a grain
        let catalog = silo.catalog().unwrap();
        let grain_id = GrainId::new(HelloGrain::grain_type(), IdSpan::from_str("test-key"));

        let handle = catalog.get_or_create_activation(&grain_id);
        assert!(handle.is_ok());

        // Verify it's tracked
        assert_eq!(catalog.activation_count(), 1);

        silo.stop().await.unwrap();
    }
}
