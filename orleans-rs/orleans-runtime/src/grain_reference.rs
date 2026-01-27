//! Grain references (proxies) for calling grains.
//!
//! A grain reference is a lightweight handle that represents a grain.
//! It can be used to invoke methods on the grain, regardless of where
//! the grain is activated in the cluster.

use bytes::Bytes;
use orleans_core::{GrainId, GrainType, IdSpan};
use orleans_messaging::{GrainInterfaceType, Message};
use std::sync::Arc;
use std::time::Duration;

use crate::error::{RuntimeError, RuntimeResult};

/// A reference to a grain that can be used to invoke methods.
///
/// Grain references are lightweight and can be freely cloned and passed
/// around. They don't hold a connection to the grain; instead, they
/// contain the information needed to route messages to the grain.
pub trait IGrainReference: Send + Sync {
    /// Returns the grain ID.
    fn grain_id(&self) -> &GrainId;

    /// Returns the grain type.
    fn grain_type(&self) -> &GrainType;

    /// Returns the interface type for this reference.
    fn interface_type(&self) -> &GrainInterfaceType;

    /// Invoke a method on the grain.
    ///
    /// This is the low-level invocation method. Generated proxies
    /// provide typed method wrappers around this.
    ///
    /// # Arguments
    ///
    /// * `method_id` - The method to invoke.
    /// * `request_body` - The serialized request arguments.
    /// * `timeout` - Optional timeout for the request.
    ///
    /// # Returns
    ///
    /// The serialized response body.
    fn invoke(
        &self,
        method_id: u32,
        request_body: Bytes,
        timeout: Option<Duration>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Bytes>> + Send + '_>>;

    /// Invoke a method as one-way (fire and forget).
    ///
    /// The method is sent but no response is awaited.
    fn invoke_one_way(&self, method_id: u32, request_body: Bytes) -> RuntimeResult<()>;

    /// Cast this reference to a different interface type.
    ///
    /// This creates a new reference with a different interface type
    /// but pointing to the same grain.
    fn cast(&self, interface_type: GrainInterfaceType) -> Arc<dyn IGrainReference>;
}

/// A concrete grain reference implementation.
#[derive(Clone)]
pub struct GrainReference {
    grain_id: GrainId,
    grain_type: GrainType,
    interface_type: GrainInterfaceType,
    message_sender: Arc<dyn MessageSender>,
}

/// Trait for sending messages (allows mocking in tests).
pub trait MessageSender: Send + Sync {
    /// Send a request and await the response.
    fn send_request(
        &self,
        message: Message,
        timeout: Option<Duration>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Message>> + Send + '_>>;

    /// Send a one-way message (no response expected).
    fn send_one_way(&self, message: Message) -> RuntimeResult<()>;
}

impl GrainReference {
    /// Create a new grain reference.
    pub fn new(
        grain_id: GrainId,
        grain_type: GrainType,
        interface_type: GrainInterfaceType,
        message_sender: Arc<dyn MessageSender>,
    ) -> Self {
        Self {
            grain_id,
            grain_type,
            interface_type,
            message_sender,
        }
    }

    /// Create a grain reference from a key.
    pub fn from_key(
        grain_type: GrainType,
        key: IdSpan,
        interface_type: GrainInterfaceType,
        message_sender: Arc<dyn MessageSender>,
    ) -> Self {
        let grain_id = GrainId::new(grain_type.clone(), key);
        Self::new(grain_id, grain_type, interface_type, message_sender)
    }
}

impl IGrainReference for GrainReference {
    fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    fn grain_type(&self) -> &GrainType {
        &self.grain_type
    }

    fn interface_type(&self) -> &GrainInterfaceType {
        &self.interface_type
    }

    fn invoke(
        &self,
        method_id: u32,
        request_body: Bytes,
        timeout: Option<Duration>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Bytes>> + Send + '_>>
    {
        let grain_id = self.grain_id.clone();
        let interface_type = self.interface_type.clone();
        let message_sender = self.message_sender.clone();

        Box::pin(async move {
            // Create the request message
            // Note: sending_silo will be filled in by the message center
            let message = Message::new_request(
                grain_id,
                interface_type,
                method_id,
                request_body,
                orleans_core::SiloAddress::new(
                    "0.0.0.0:0".parse().unwrap(),
                    0,
                ),
            )
            .with_timeout(timeout);

            // Send and await response
            let response = message_sender.send_request(message, timeout).await?;

            // Check for rejection
            if let Some(rejection) = response.rejection_info() {
                return Err(RuntimeError::Internal(format!(
                    "Request rejected: {:?} - {}",
                    rejection.rejection_type(),
                    rejection.message()
                )));
            }

            Ok(response.body().clone())
        })
    }

    fn invoke_one_way(&self, method_id: u32, request_body: Bytes) -> RuntimeResult<()> {
        let message = Message::new_one_way(
            self.grain_id.clone(),
            self.interface_type.clone(),
            method_id,
            request_body,
            orleans_core::SiloAddress::new("0.0.0.0:0".parse().unwrap(), 0),
        );

        self.message_sender.send_one_way(message)
    }

    fn cast(&self, interface_type: GrainInterfaceType) -> Arc<dyn IGrainReference> {
        Arc::new(GrainReference {
            grain_id: self.grain_id.clone(),
            grain_type: self.grain_type.clone(),
            interface_type,
            message_sender: self.message_sender.clone(),
        })
    }
}

/// A typed grain reference with compile-time interface checking.
///
/// This is a wrapper around `GrainReference` that provides type-safe
/// method invocation for a specific grain interface.
pub struct TypedGrainReference<T> {
    inner: Arc<dyn IGrainReference>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedGrainReference<T> {
    /// Create a new typed grain reference.
    pub fn new(inner: Arc<dyn IGrainReference>) -> Self {
        Self {
            inner,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the inner untyped reference.
    pub fn as_untyped(&self) -> &Arc<dyn IGrainReference> {
        &self.inner
    }
}

impl<T> Clone for TypedGrainReference<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::SiloAddress;
    use std::net::SocketAddr;

    // Mock message sender for testing
    struct MockMessageSender {
        response_body: Bytes,
    }

    impl MessageSender for MockMessageSender {
        fn send_request(
            &self,
            _message: Message,
            _timeout: Option<Duration>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = RuntimeResult<Message>> + Send + '_>,
        > {
            let body = self.response_body.clone();
            Box::pin(async move {
                let silo =
                    SiloAddress::new("127.0.0.1:11111".parse::<SocketAddr>().unwrap(), 1);
                Ok(Message::new_request(
                    GrainId::new(
                        GrainType::create("Test"),
                        IdSpan::from_str("key"),
                    ),
                    GrainInterfaceType::create("ITest"),
                    1,
                    body,
                    silo,
                ))
            })
        }

        fn send_one_way(&self, _message: Message) -> RuntimeResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_grain_reference_creation() {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::from_static(b"response"),
        });

        let reference = GrainReference::new(
            grain_id.clone(),
            grain_type.clone(),
            interface_type.clone(),
            sender,
        );

        assert_eq!(reference.grain_id(), &grain_id);
        assert_eq!(reference.grain_type(), &grain_type);
        assert_eq!(reference.interface_type(), &interface_type);
    }

    #[test]
    fn test_grain_reference_from_key() {
        let grain_type = GrainType::create("TestGrain");
        let key = IdSpan::from_str("key1");
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::from_static(b"response"),
        });

        let reference = GrainReference::from_key(
            grain_type.clone(),
            key.clone(),
            interface_type.clone(),
            sender,
        );

        assert_eq!(reference.grain_type(), &grain_type);
        assert_eq!(reference.grain_id().key(), &key);
    }

    #[tokio::test]
    async fn test_grain_reference_invoke() {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::from_static(b"hello"),
        });

        let reference = GrainReference::new(grain_id, grain_type, interface_type, sender);

        let result = reference
            .invoke(1, Bytes::from_static(b"request"), None)
            .await;

        assert!(result.is_ok());
        // The mock returns the response body
        assert_eq!(result.unwrap(), Bytes::from_static(b"hello"));
    }

    #[test]
    fn test_grain_reference_invoke_one_way() {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::new(),
        });

        let reference = GrainReference::new(grain_id, grain_type, interface_type, sender);

        let result = reference.invoke_one_way(1, Bytes::from_static(b"request"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_grain_reference_cast() {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::new(),
        });

        let reference = GrainReference::new(grain_id.clone(), grain_type, interface_type, sender);

        let new_interface = GrainInterfaceType::create("IOtherInterface");
        let cast_ref = reference.cast(new_interface.clone());

        assert_eq!(cast_ref.interface_type(), &new_interface);
        assert_eq!(cast_ref.grain_id(), &grain_id);
    }

    #[test]
    fn test_typed_grain_reference() {
        struct ITestGrain;

        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface_type = GrainInterfaceType::create("ITestGrain");
        let sender = Arc::new(MockMessageSender {
            response_body: Bytes::new(),
        });

        let reference: Arc<dyn IGrainReference> =
            Arc::new(GrainReference::new(grain_id, grain_type, interface_type, sender));

        let typed = TypedGrainReference::<ITestGrain>::new(reference.clone());
        let typed2 = typed.clone();

        assert_eq!(typed.as_untyped().grain_type().as_str(), Some("TestGrain"));
        assert_eq!(typed2.as_untyped().grain_type().as_str(), Some("TestGrain"));
    }
}
