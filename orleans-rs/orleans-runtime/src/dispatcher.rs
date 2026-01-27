//! Message dispatcher for routing messages to grain activations.
//!
//! The dispatcher is responsible for:
//! - Receiving incoming messages from the network
//! - Looking up or creating the target activation
//! - Routing the message to the activation
//! - Handling responses and errors

use orleans_core::{GrainAddress, GrainId, SiloAddress};
use orleans_clustering::MembershipVersion;
use orleans_directory::{DirectoryError, DistributedGrainDirectory};
use orleans_messaging::{Direction, Message, MessageCenter, RejectionType};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::oneshot;
use tracing::{debug, instrument, warn};

use crate::activation_data::PendingMessage;
use crate::catalog::Catalog;
use crate::error::RuntimeResult;

/// Configuration for the dispatcher.
#[derive(Debug, Clone)]
pub struct DispatcherOptions {
    /// Default timeout for requests.
    pub default_timeout: Duration,

    /// Whether to forward messages to other silos.
    pub enable_forwarding: bool,

    /// Maximum number of hops for forwarded messages.
    pub max_forward_hops: u32,
}

impl Default for DispatcherOptions {
    fn default() -> Self {
        Self {
            default_timeout: Duration::from_secs(30),
            enable_forwarding: true,
            max_forward_hops: 2,
        }
    }
}

/// The message dispatcher.
pub struct Dispatcher {
    /// The local silo address.
    silo_address: SiloAddress,

    /// The activation catalog.
    catalog: Arc<Catalog>,

    /// The grain directory.
    directory: Arc<DistributedGrainDirectory>,

    /// The message center for sending messages.
    message_center: Arc<MessageCenter>,

    /// Configuration options.
    options: DispatcherOptions,
}

impl Dispatcher {
    /// Create a new dispatcher.
    pub fn new(
        silo_address: SiloAddress,
        catalog: Arc<Catalog>,
        directory: Arc<DistributedGrainDirectory>,
        message_center: Arc<MessageCenter>,
        options: DispatcherOptions,
    ) -> Self {
        Self {
            silo_address,
            catalog,
            directory,
            message_center,
            options,
        }
    }

    /// Register this dispatcher as the message handler.
    pub fn register_handler(self: Arc<Self>) {
        let dispatcher = self.clone();
        self.message_center.set_message_handler(move |message| {
            let dispatcher = dispatcher.clone();
            tokio::spawn(async move {
                dispatcher.handle_message(message).await;
            });
        });
    }

    /// Handle an incoming message.
    pub async fn handle_message(&self, message: Message) {
        match message.direction() {
            Direction::Request => self.handle_request(message).await,
            Direction::Response => self.handle_response(message).await,
            Direction::OneWay => self.handle_one_way(message).await,
        }
    }

    /// Handle an incoming request.
    #[instrument(skip(self, message), fields(
        grain_id = %message.target_grain(),
        correlation_id = ?message.id(),
        method_id = message.method_id()
    ))]
    async fn handle_request(&self, message: Message) {
        let grain_id = message.target_grain();
        let correlation_id = message.id();

        debug!(
            grain_id = %grain_id,
            correlation_id = ?correlation_id,
            method_id = message.method_id(),
            "Handling request"
        );

        // Check if we have this grain locally
        if let Some(handle) = self.catalog.lookup(grain_id) {
            // Dispatch to local activation
            let (response_tx, response_rx) = oneshot::channel();
            let pending = PendingMessage::new(message.clone(), Some(response_tx));

            match handle.enqueue_message(pending) {
                Ok(()) => {
                    // Wait for response
                    match response_rx.await {
                        Ok(response) => {
                            self.send_response(response).await;
                        }
                        Err(_) => {
                            self.send_rejection(
                                &message,
                                RejectionType::Unrecoverable,
                                "Activation worker closed".to_string(),
                            )
                            .await;
                        }
                    }
                }
                Err(e) => {
                    self.send_rejection(
                        &message,
                        RejectionType::Transient,
                        e.to_string(),
                    )
                    .await;
                }
            }
            return;
        }

        // Grain not local, try to find or create it
        match self.find_or_create_grain(grain_id).await {
            Ok(address) => {
                if address.silo_address() == Some(&self.silo_address) {
                    // We should host this grain
                    match self.catalog.get_or_create_activation(grain_id) {
                        Ok(handle) => {
                            let (response_tx, response_rx) = oneshot::channel();
                            let pending = PendingMessage::new(message.clone(), Some(response_tx));

                            match handle.enqueue_message(pending) {
                                Ok(()) => {
                                    match response_rx.await {
                                        Ok(response) => {
                                            self.send_response(response).await;
                                        }
                                        Err(_) => {
                                            self.send_rejection(
                                                &message,
                                                RejectionType::Unrecoverable,
                                                "Activation worker closed".to_string(),
                                            )
                                            .await;
                                        }
                                    }
                                }
                                Err(e) => {
                                    self.send_rejection(
                                        &message,
                                        RejectionType::Transient,
                                        e.to_string(),
                                    )
                                    .await;
                                }
                            }
                        }
                        Err(e) => {
                            self.send_rejection(
                                &message,
                                RejectionType::Transient,
                                e.to_string(),
                            )
                            .await;
                        }
                    }
                } else if let Some(target_silo) = address.silo_address() {
                    // Forward to another silo
                    if self.options.enable_forwarding {
                        self.forward_message(message, target_silo.clone()).await;
                    } else {
                        self.send_rejection(
                            &message,
                            RejectionType::Transient,
                            format!("Grain is hosted on silo {}", target_silo),
                        )
                        .await;
                    }
                } else {
                    self.send_rejection(
                        &message,
                        RejectionType::GrainNotFound,
                        "Could not determine grain location".to_string(),
                    )
                    .await;
                }
            }
            Err(e) => {
                self.send_rejection(
                    &message,
                    RejectionType::GrainNotFound,
                    e.to_string(),
                )
                .await;
            }
        }
    }

    /// Handle an incoming response.
    async fn handle_response(&self, message: Message) {
        debug!(
            correlation_id = ?message.id(),
            "Handling response"
        );

        // Responses are handled by the message center's pending request tracking
        // This is called when a response comes in for a request we didn't send
        // (e.g., the response was forwarded to us)
        warn!(
            correlation_id = ?message.id(),
            "Received unexpected response"
        );
    }

    /// Handle an incoming one-way message.
    async fn handle_one_way(&self, message: Message) {
        let grain_id = message.target_grain().clone();

        debug!(
            grain_id = %grain_id,
            method_id = message.method_id(),
            "Handling one-way message"
        );

        // Check if we have this grain locally
        if let Some(handle) = self.catalog.lookup(&grain_id) {
            let pending = PendingMessage::new(message, None);
            if let Err(e) = handle.enqueue_message(pending) {
                warn!(
                    grain_id = %grain_id,
                    error = %e,
                    "Failed to enqueue one-way message"
                );
            }
            return;
        }

        // Try to find or create the grain
        match self.find_or_create_grain(&grain_id).await {
            Ok(address) => {
                if address.silo_address() == Some(&self.silo_address) {
                    // We should host this grain
                    match self.catalog.get_or_create_activation(&grain_id) {
                        Ok(handle) => {
                            let pending = PendingMessage::new(message, None);
                            if let Err(e) = handle.enqueue_message(pending) {
                                warn!(
                                    grain_id = %grain_id,
                                    error = %e,
                                    "Failed to enqueue one-way message"
                                );
                            }
                        }
                        Err(e) => {
                            warn!(
                                grain_id = %grain_id,
                                error = %e,
                                "Failed to create activation for one-way message"
                            );
                        }
                    }
                } else if let Some(target_silo) = address.silo_address() {
                    // Forward to another silo
                    if self.options.enable_forwarding {
                        self.forward_message(message, target_silo.clone()).await;
                    }
                }
            }
            Err(e) => {
                warn!(
                    grain_id = %grain_id,
                    error = %e,
                    "Failed to find grain for one-way message"
                );
            }
        }
    }

    /// Find the grain location or create a new activation.
    #[instrument(skip(self), fields(grain_id = %grain_id, silo = %self.silo_address))]
    async fn find_or_create_grain(&self, grain_id: &GrainId) -> RuntimeResult<GrainAddress> {
        // First, look up in the directory
        match self.directory.lookup(grain_id).await {
            Ok(Some(address)) => {
                debug!(
                    grain_id = %grain_id,
                    address = ?address,
                    "Found grain in directory"
                );
                return Ok(address);
            }
            Ok(None) => {
                // Not in directory, need to create
                debug!(
                    grain_id = %grain_id,
                    "Grain not in directory, will create"
                );
            }
            Err(e) => {
                warn!(
                    grain_id = %grain_id,
                    error = %e,
                    "Directory lookup failed"
                );
                return Err(e.into());
            }
        }

        // Create a new activation on this silo
        let handle = self.catalog.get_or_create_activation(grain_id)?;
        let address = handle.address().clone();

        // Register in the directory
        match self.directory.register(MembershipVersion::default(), address.clone(), None).await {
            Ok(registered_address) => {
                debug!(
                    grain_id = %grain_id,
                    address = ?registered_address,
                    "Registered grain in directory"
                );
                Ok(registered_address)
            }
            Err(DirectoryError::RegistrationConflict { existing, .. }) => {
                // Someone else registered first, remove our activation
                debug!(
                    grain_id = %grain_id,
                    existing = ?existing,
                    "Registration conflict, using existing"
                );
                self.catalog.remove_activation(grain_id);
                Ok(existing)
            }
            Err(e) => {
                // Registration failed, clean up
                self.catalog.remove_activation(grain_id);
                Err(e.into())
            }
        }
    }

    /// Forward a message to another silo.
    async fn forward_message(&self, message: Message, target_silo: SiloAddress) {
        debug!(
            grain_id = %message.target_grain(),
            target_silo = %target_silo,
            "Forwarding message"
        );

        // Update the target silo in the message
        let forwarded = message.with_target_silo(Some(target_silo.clone()));

        if let Err(e) = self.message_center.send(forwarded).await {
            warn!(
                target_silo = %target_silo,
                error = %e,
                "Failed to forward message"
            );
        }
    }

    /// Send a response message.
    async fn send_response(&self, response: Message) {
        if response.target_silo().is_some() {
            if let Err(e) = self.message_center.send(response).await {
                warn!(
                    error = %e,
                    "Failed to send response"
                );
            }
        }
    }

    /// Send a rejection message.
    async fn send_rejection(&self, request: &Message, rejection_type: RejectionType, reason: String) {
        let rejection = Message::create_rejection(
            request,
            rejection_type,
            reason,
            self.silo_address.clone(),
        );

        let rejection = rejection.with_target_silo(Some(request.sending_silo().clone()));
        if let Err(e) = self.message_center.send(rejection).await {
            warn!(
                error = %e,
                "Failed to send rejection"
            );
        }
    }

    /// Get the silo address.
    pub fn silo_address(&self) -> &SiloAddress {
        &self.silo_address
    }

    /// Get the catalog.
    pub fn catalog(&self) -> &Arc<Catalog> {
        &self.catalog
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::CatalogOptions;
    use crate::grain::{GrainTypeData, IGrain, IGrainActivator};
    use crate::grain_factory::IGrainFactory;
    use async_trait::async_trait;
    use orleans_core::{GrainType, IdSpan};

    struct TestGrain {
        value: i32,
    }

    #[async_trait]
    impl IGrain for TestGrain {
        fn grain_type() -> GrainType {
            GrainType::create("TestGrain")
        }
    }

    struct TestActivator;

    impl IGrainActivator for TestActivator {
        fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
            Box::new(TestGrain { value: 42 })
        }

        fn grain_type(&self) -> GrainType {
            TestGrain::grain_type()
        }
    }

    struct MockGrainFactory;

    impl IGrainFactory for MockGrainFactory {
        fn get_grain_reference(
            &self,
            _grain_type: GrainType,
            _key: IdSpan,
        ) -> Arc<dyn crate::grain_reference::IGrainReference> {
            unimplemented!()
        }
    }

    #[test]
    fn test_dispatcher_options_default() {
        let options = DispatcherOptions::default();
        assert_eq!(options.default_timeout, Duration::from_secs(30));
        assert!(options.enable_forwarding);
        assert_eq!(options.max_forward_hops, 2);
    }

    // Note: Full dispatcher tests require the MessageCenter and Directory
    // which involve network operations. See integration tests for full coverage.
}
