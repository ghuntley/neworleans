//! Message - Core message structure for Orleans inter-silo communication.
//!
//! Messages are the fundamental unit of communication in Orleans. They carry
//! requests, responses, and one-way notifications between grains and silos.

use std::fmt;

use bytes::Bytes;
use orleans_core::{ActivationId, GrainId, SiloAddress};

use crate::correlation_id::CorrelationId;
use crate::direction::Direction;
use crate::grain_interface_type::GrainInterfaceType;

/// The core message structure for Orleans communication.
///
/// A message contains all the information needed to route a request to a grain
/// and deliver a response back to the sender.
#[derive(Debug, Clone)]
pub struct Message {
    /// Unique identifier for request/response correlation.
    pub id: CorrelationId,

    /// Direction of the message (Request, Response, or OneWay).
    pub direction: Direction,

    /// The target grain being invoked.
    pub target_grain: GrainId,

    /// Optional target silo (if known).
    pub target_silo: Option<SiloAddress>,

    /// Optional target activation (if known).
    pub target_activation: Option<ActivationId>,

    /// The grain sending this message (if applicable).
    pub sending_grain: Option<GrainId>,

    /// The silo sending this message.
    pub sending_silo: SiloAddress,

    /// Optional sending activation.
    pub sending_activation: Option<ActivationId>,

    /// The interface type being invoked.
    pub interface_type: GrainInterfaceType,

    /// The method ID within the interface.
    pub method_id: u32,

    /// Serialized body (arguments for request, result for response).
    pub body: Bytes,

    /// Optional error for rejection responses.
    pub rejection_info: Option<RejectionInfo>,

    /// Timestamp when the message was created.
    pub created_at: std::time::Instant,

    /// Request timeout (for requests only).
    pub timeout: Option<std::time::Duration>,
}

/// Information about why a message was rejected.
#[derive(Debug, Clone)]
pub struct RejectionInfo {
    /// The type of rejection.
    pub rejection_type: RejectionType,
    /// Human-readable rejection message.
    pub message: String,
}

/// Types of message rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RejectionType {
    /// Transient error - retry may succeed.
    Transient = 0,
    /// The target grain doesn't exist or can't be activated.
    GrainNotFound = 1,
    /// The method or interface doesn't exist.
    MethodNotFound = 2,
    /// The target silo is unavailable.
    SiloUnavailable = 3,
    /// The request timed out.
    Timeout = 4,
    /// The request was cancelled.
    Cancelled = 5,
    /// Unrecoverable error.
    Unrecoverable = 6,
}

impl RejectionType {
    /// Converts from a u8 value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(RejectionType::Transient),
            1 => Some(RejectionType::GrainNotFound),
            2 => Some(RejectionType::MethodNotFound),
            3 => Some(RejectionType::SiloUnavailable),
            4 => Some(RejectionType::Timeout),
            5 => Some(RejectionType::Cancelled),
            6 => Some(RejectionType::Unrecoverable),
            _ => None,
        }
    }

    /// Returns true if this rejection is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, RejectionType::Transient | RejectionType::Timeout)
    }
}

impl Message {
    /// Creates a new request message.
    pub fn new_request(
        target_grain: GrainId,
        interface_type: GrainInterfaceType,
        method_id: u32,
        body: Bytes,
        sending_silo: SiloAddress,
    ) -> Self {
        Self {
            id: CorrelationId::new(),
            direction: Direction::Request,
            target_grain,
            target_silo: None,
            target_activation: None,
            sending_grain: None,
            sending_silo,
            sending_activation: None,
            interface_type,
            method_id,
            body,
            rejection_info: None,
            created_at: std::time::Instant::now(),
            timeout: Some(std::time::Duration::from_secs(30)),
        }
    }

    /// Creates a response message for a request.
    pub fn create_response(&self, body: Bytes) -> Self {
        Self {
            id: self.id,
            direction: Direction::Response,
            target_grain: self.sending_grain.clone().unwrap_or_else(|| self.target_grain.clone()),
            target_silo: Some(self.sending_silo.clone()),
            target_activation: self.sending_activation.clone(),
            sending_grain: Some(self.target_grain.clone()),
            sending_silo: self.target_silo.clone().unwrap_or_else(SiloAddress::zero),
            sending_activation: self.target_activation.clone(),
            interface_type: self.interface_type.clone(),
            method_id: self.method_id,
            body,
            rejection_info: None,
            created_at: std::time::Instant::now(),
            timeout: None,
        }
    }

    /// Creates a rejection response for a request.
    pub fn create_rejection(&self, rejection_type: RejectionType, message: String) -> Self {
        let mut response = self.create_response(Bytes::new());
        response.rejection_info = Some(RejectionInfo {
            rejection_type,
            message,
        });
        response
    }

    /// Creates a one-way message.
    pub fn new_one_way(
        target_grain: GrainId,
        interface_type: GrainInterfaceType,
        method_id: u32,
        body: Bytes,
        sending_silo: SiloAddress,
    ) -> Self {
        Self {
            id: CorrelationId::new(),
            direction: Direction::OneWay,
            target_grain,
            target_silo: None,
            target_activation: None,
            sending_grain: None,
            sending_silo,
            sending_activation: None,
            interface_type,
            method_id,
            body,
            rejection_info: None,
            created_at: std::time::Instant::now(),
            timeout: None,
        }
    }

    /// Returns true if this is a request message.
    pub fn is_request(&self) -> bool {
        self.direction.is_request()
    }

    /// Returns true if this is a response message.
    pub fn is_response(&self) -> bool {
        self.direction.is_response()
    }

    /// Returns true if this is a one-way message.
    pub fn is_one_way(&self) -> bool {
        self.direction.is_one_way()
    }

    /// Returns true if this is a rejection response.
    pub fn is_rejection(&self) -> bool {
        self.rejection_info.is_some()
    }

    /// Returns true if this message has expired.
    pub fn is_expired(&self) -> bool {
        if let Some(timeout) = self.timeout {
            self.created_at.elapsed() > timeout
        } else {
            false
        }
    }

    /// Sets the sending grain information.
    pub fn with_sending_grain(mut self, grain_id: GrainId, activation_id: Option<ActivationId>) -> Self {
        self.sending_grain = Some(grain_id);
        self.sending_activation = activation_id;
        self
    }

    /// Sets the target silo.
    pub fn with_target_silo(mut self, silo: SiloAddress) -> Self {
        self.target_silo = Some(silo);
        self
    }

    /// Sets the target activation.
    pub fn with_target_activation(mut self, activation_id: ActivationId) -> Self {
        self.target_activation = Some(activation_id);
        self
    }

    /// Sets the timeout for this message.
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Message[id={}, dir={}, target={}, interface={}, method={}]",
            self.id, self.direction, self.target_grain, self.interface_type, self.method_id
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::GrainType;
    use std::net::SocketAddr;

    fn test_silo_address() -> SiloAddress {
        SiloAddress::new(
            "127.0.0.1:11111".parse::<SocketAddr>().unwrap(),
            1234567890,
        )
    }

    fn test_grain_id() -> GrainId {
        GrainId::new(GrainType::create("TestGrain"), "key1".into())
    }

    #[test]
    fn test_new_request() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::from_static(b"hello"),
            test_silo_address(),
        );

        assert!(msg.is_request());
        assert!(!msg.is_response());
        assert!(!msg.is_one_way());
        assert!(!msg.is_rejection());
        assert_eq!(msg.method_id, 1);
        assert_eq!(msg.body.as_ref(), b"hello");
    }

    #[test]
    fn test_create_response() {
        let request = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::from_static(b"request"),
            test_silo_address(),
        );

        let response = request.create_response(Bytes::from_static(b"response"));

        assert!(response.is_response());
        assert_eq!(response.id, request.id);
        assert_eq!(response.body.as_ref(), b"response");
        assert!(!response.is_rejection());
    }

    #[test]
    fn test_create_rejection() {
        let request = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        );

        let rejection = request.create_rejection(
            RejectionType::GrainNotFound,
            "Grain not found".to_string(),
        );

        assert!(rejection.is_response());
        assert!(rejection.is_rejection());
        assert_eq!(
            rejection.rejection_info.as_ref().unwrap().rejection_type,
            RejectionType::GrainNotFound
        );
    }

    #[test]
    fn test_one_way_message() {
        let msg = Message::new_one_way(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        );

        assert!(!msg.is_request());
        assert!(!msg.is_response());
        assert!(msg.is_one_way());
    }

    #[test]
    fn test_message_expiry() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        )
        .with_timeout(std::time::Duration::from_millis(1));

        // Should not be expired immediately
        assert!(!msg.is_expired());

        // Wait a bit longer than timeout
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Should now be expired
        assert!(msg.is_expired());
    }

    #[test]
    fn test_rejection_type_retryable() {
        assert!(RejectionType::Transient.is_retryable());
        assert!(RejectionType::Timeout.is_retryable());
        assert!(!RejectionType::GrainNotFound.is_retryable());
        assert!(!RejectionType::Unrecoverable.is_retryable());
    }

    #[test]
    fn test_message_display() {
        let msg = Message::new_request(
            test_grain_id(),
            GrainInterfaceType::create("ITestGrain"),
            1,
            Bytes::new(),
            test_silo_address(),
        );

        let display = msg.to_string();
        assert!(display.contains("Message"));
        assert!(display.contains("Request"));
        assert!(display.contains("ITestGrain"));
    }
}
