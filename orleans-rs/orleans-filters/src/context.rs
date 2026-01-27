//! Call context types for grain method invocations.
//!
//! This module provides the context objects that are passed through
//! the filter pipeline during grain method calls. The context contains
//! information about the request, target grain, and allows filters
//! to modify the invocation.

use bytes::Bytes;
use orleans_core::{GrainId, GrainType};
use orleans_messaging::GrainInterfaceType;
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::error::FilterResult;
use crate::request_context::ContextProperties;
use crate::response::Response;

/// Base context for all grain method calls.
///
/// This provides common information and functionality for both
/// incoming (server-side) and outgoing (client-side) calls.
pub struct GrainCallContext {
    /// The source grain ID (if called from within a grain).
    pub source_id: Option<GrainId>,

    /// The target grain ID.
    pub target_id: GrainId,

    /// The grain interface type.
    pub interface_type: GrainInterfaceType,

    /// The interface name (human-readable).
    pub interface_name: String,

    /// The method name being called.
    pub method_name: String,

    /// The method ID.
    pub method_id: u32,

    /// The serialized request arguments.
    pub request_body: Bytes,

    /// The response (set after invocation).
    pub response: Option<Response>,

    /// The request context properties.
    pub context_properties: ContextProperties,

    /// Additional data that can be attached by filters.
    extensions: parking_lot::RwLock<std::collections::HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>>,
}

impl GrainCallContext {
    /// Create a new grain call context.
    pub fn new(
        target_id: GrainId,
        interface_type: GrainInterfaceType,
        interface_name: impl Into<String>,
        method_name: impl Into<String>,
        method_id: u32,
        request_body: Bytes,
    ) -> Self {
        Self {
            source_id: None,
            target_id,
            interface_type,
            interface_name: interface_name.into(),
            method_name: method_name.into(),
            method_id,
            request_body,
            response: None,
            context_properties: ContextProperties::new(),
            extensions: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Set the source grain ID.
    pub fn with_source_id(mut self, source_id: GrainId) -> Self {
        self.source_id = Some(source_id);
        self
    }

    /// Set the context properties.
    pub fn with_context_properties(mut self, properties: ContextProperties) -> Self {
        self.context_properties = properties;
        self
    }

    /// Get the result value if the response contains a successful result.
    pub fn get_result<T: Clone + 'static>(&self) -> Option<T> {
        self.response.as_ref().and_then(|r| r.get_result())
    }

    /// Set the result value.
    pub fn set_result<T: Send + Sync + 'static>(&mut self, value: T) {
        self.response = Some(Response::from_result(value));
    }

    /// Set an exception as the response.
    pub fn set_exception(&mut self, message: impl Into<String>) {
        self.response = Some(Response::from_exception(message));
    }

    /// Set the response directly.
    pub fn set_response(&mut self, response: Response) {
        self.response = Some(response);
    }

    /// Get an extension value.
    pub fn get_extension<T: 'static>(&self) -> Option<Arc<T>> {
        let extensions = self.extensions.read();
        extensions
            .get(&std::any::TypeId::of::<T>())
            .and_then(|v| v.downcast_ref::<Arc<T>>().cloned())
    }

    /// Set an extension value.
    pub fn set_extension<T: Send + Sync + 'static>(&self, value: T) {
        let mut extensions = self.extensions.write();
        extensions.insert(std::any::TypeId::of::<T>(), Box::new(Arc::new(value)));
    }

    /// Check if the response is set and successful.
    pub fn is_success(&self) -> bool {
        self.response.as_ref().map(|r| r.is_success()).unwrap_or(false)
    }

    /// Check if the response is an exception.
    pub fn is_exception(&self) -> bool {
        self.response.as_ref().map(|r| r.is_exception()).unwrap_or(false)
    }
}

impl fmt::Debug for GrainCallContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrainCallContext")
            .field("source_id", &self.source_id)
            .field("target_id", &self.target_id)
            .field("interface_name", &self.interface_name)
            .field("method_name", &self.method_name)
            .field("method_id", &self.method_id)
            .field("response", &self.response)
            .finish()
    }
}

/// Context for incoming (server-side) grain calls.
///
/// This context is passed to `IIncomingGrainCallFilter` implementations
/// when a grain receives a method call.
pub struct IncomingGrainCallContext {
    /// The base call context.
    pub base: GrainCallContext,

    /// The grain type being invoked.
    pub grain_type: GrainType,

    /// A function to invoke the next stage of the pipeline.
    invoke_next: Option<Box<dyn FnOnce(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + 'static>>,

    /// Whether the chain has been invoked.
    invoked: bool,
}

impl IncomingGrainCallContext {
    /// Create a new incoming call context.
    pub fn new(
        target_id: GrainId,
        grain_type: GrainType,
        interface_type: GrainInterfaceType,
        interface_name: impl Into<String>,
        method_name: impl Into<String>,
        method_id: u32,
        request_body: Bytes,
    ) -> Self {
        Self {
            base: GrainCallContext::new(
                target_id,
                interface_type,
                interface_name,
                method_name,
                method_id,
                request_body,
            ),
            grain_type,
            invoke_next: None,
            invoked: false,
        }
    }

    /// Set the source grain ID.
    pub fn with_source_id(mut self, source_id: GrainId) -> Self {
        self.base = self.base.with_source_id(source_id);
        self
    }

    /// Set the context properties.
    pub fn with_context_properties(mut self, properties: ContextProperties) -> Self {
        self.base = self.base.with_context_properties(properties);
        self
    }

    /// Set the invoke callback.
    pub fn set_invoke_callback<F>(&mut self, callback: F)
    where
        F: FnOnce(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        self.invoke_next = Some(Box::new(callback));
    }

    /// Invoke the next stage in the filter pipeline.
    ///
    /// This must be called by each filter to continue the chain.
    /// After this returns, the response should be set.
    pub fn invoke(&mut self) -> FilterResult<()> {
        if let Some(invoke_fn) = self.invoke_next.take() {
            self.invoked = true;
            invoke_fn(self)
        } else {
            // No more stages, this shouldn't happen in normal use
            Ok(())
        }
    }

    /// Check if the chain has been invoked.
    pub fn was_invoked(&self) -> bool {
        self.invoked
    }

    /// Get the target grain ID.
    pub fn target_id(&self) -> &GrainId {
        &self.base.target_id
    }

    /// Get the grain type.
    pub fn grain_type(&self) -> &GrainType {
        &self.grain_type
    }

    /// Get the interface type.
    pub fn interface_type(&self) -> &GrainInterfaceType {
        &self.base.interface_type
    }

    /// Get the interface name.
    pub fn interface_name(&self) -> &str {
        &self.base.interface_name
    }

    /// Get the method name.
    pub fn method_name(&self) -> &str {
        &self.base.method_name
    }

    /// Get the method ID.
    pub fn method_id(&self) -> u32 {
        self.base.method_id
    }

    /// Get the request body.
    pub fn request_body(&self) -> &Bytes {
        &self.base.request_body
    }

    /// Get the response.
    pub fn response(&self) -> Option<&Response> {
        self.base.response.as_ref()
    }

    /// Get a mutable reference to the response.
    pub fn response_mut(&mut self) -> &mut Option<Response> {
        &mut self.base.response
    }

    /// Set the response.
    pub fn set_response(&mut self, response: Response) {
        self.base.set_response(response);
    }

    /// Set the result.
    pub fn set_result<T: Send + Sync + 'static>(&mut self, value: T) {
        self.base.set_result(value);
    }

    /// Set an exception.
    pub fn set_exception(&mut self, message: impl Into<String>) {
        self.base.set_exception(message);
    }

    /// Get the source grain ID.
    pub fn source_id(&self) -> Option<&GrainId> {
        self.base.source_id.as_ref()
    }

    /// Get the context properties.
    pub fn context_properties(&self) -> &ContextProperties {
        &self.base.context_properties
    }

    /// Get a mutable reference to the context properties.
    pub fn context_properties_mut(&mut self) -> &mut ContextProperties {
        &mut self.base.context_properties
    }
}

impl fmt::Debug for IncomingGrainCallContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IncomingGrainCallContext")
            .field("target_id", &self.base.target_id)
            .field("grain_type", &self.grain_type)
            .field("interface_name", &self.base.interface_name)
            .field("method_name", &self.base.method_name)
            .field("method_id", &self.base.method_id)
            .field("invoked", &self.invoked)
            .finish()
    }
}

/// Context for outgoing (client-side) grain calls.
///
/// This context is passed to `IOutgoingGrainCallFilter` implementations
/// when making a grain method call.
pub struct OutgoingGrainCallContext {
    /// The base call context.
    pub base: GrainCallContext,

    /// A function to invoke the next stage of the pipeline.
    invoke_next: Option<Box<dyn FnOnce(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + 'static>>,

    /// Whether the chain has been invoked.
    invoked: bool,
}

impl OutgoingGrainCallContext {
    /// Create a new outgoing call context.
    pub fn new(
        target_id: GrainId,
        interface_type: GrainInterfaceType,
        interface_name: impl Into<String>,
        method_name: impl Into<String>,
        method_id: u32,
        request_body: Bytes,
    ) -> Self {
        Self {
            base: GrainCallContext::new(
                target_id,
                interface_type,
                interface_name,
                method_name,
                method_id,
                request_body,
            ),
            invoke_next: None,
            invoked: false,
        }
    }

    /// Set the source grain ID.
    pub fn with_source_id(mut self, source_id: GrainId) -> Self {
        self.base = self.base.with_source_id(source_id);
        self
    }

    /// Set the context properties.
    pub fn with_context_properties(mut self, properties: ContextProperties) -> Self {
        self.base = self.base.with_context_properties(properties);
        self
    }

    /// Set the invoke callback.
    pub fn set_invoke_callback<F>(&mut self, callback: F)
    where
        F: FnOnce(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        self.invoke_next = Some(Box::new(callback));
    }

    /// Invoke the next stage in the filter pipeline.
    pub fn invoke(&mut self) -> FilterResult<()> {
        if let Some(invoke_fn) = self.invoke_next.take() {
            self.invoked = true;
            invoke_fn(self)
        } else {
            Ok(())
        }
    }

    /// Check if the chain has been invoked.
    pub fn was_invoked(&self) -> bool {
        self.invoked
    }

    /// Get the target grain ID.
    pub fn target_id(&self) -> &GrainId {
        &self.base.target_id
    }

    /// Get the interface type.
    pub fn interface_type(&self) -> &GrainInterfaceType {
        &self.base.interface_type
    }

    /// Get the interface name.
    pub fn interface_name(&self) -> &str {
        &self.base.interface_name
    }

    /// Get the method name.
    pub fn method_name(&self) -> &str {
        &self.base.method_name
    }

    /// Get the method ID.
    pub fn method_id(&self) -> u32 {
        self.base.method_id
    }

    /// Get the request body.
    pub fn request_body(&self) -> &Bytes {
        &self.base.request_body
    }

    /// Get the response.
    pub fn response(&self) -> Option<&Response> {
        self.base.response.as_ref()
    }

    /// Get a mutable reference to the response.
    pub fn response_mut(&mut self) -> &mut Option<Response> {
        &mut self.base.response
    }

    /// Set the response.
    pub fn set_response(&mut self, response: Response) {
        self.base.set_response(response);
    }

    /// Set the result.
    pub fn set_result<T: Send + Sync + 'static>(&mut self, value: T) {
        self.base.set_result(value);
    }

    /// Set an exception.
    pub fn set_exception(&mut self, message: impl Into<String>) {
        self.base.set_exception(message);
    }

    /// Get the source grain ID.
    pub fn source_id(&self) -> Option<&GrainId> {
        self.base.source_id.as_ref()
    }

    /// Get the context properties.
    pub fn context_properties(&self) -> &ContextProperties {
        &self.base.context_properties
    }

    /// Get a mutable reference to the context properties.
    pub fn context_properties_mut(&mut self) -> &mut ContextProperties {
        &mut self.base.context_properties
    }
}

impl fmt::Debug for OutgoingGrainCallContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutgoingGrainCallContext")
            .field("target_id", &self.base.target_id)
            .field("interface_name", &self.base.interface_name)
            .field("method_name", &self.base.method_name)
            .field("method_id", &self.base.method_id)
            .field("invoked", &self.invoked)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::IdSpan;

    fn create_test_grain_id() -> GrainId {
        GrainId::new(
            GrainType::create("TestGrain"),
            IdSpan::from_str("test-key"),
        )
    }

    #[test]
    fn test_grain_call_context_creation() {
        let grain_id = create_test_grain_id();
        let ctx = GrainCallContext::new(
            grain_id.clone(),
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        assert_eq!(ctx.target_id, grain_id);
        assert_eq!(ctx.interface_name, "ITestGrain");
        assert_eq!(ctx.method_name, "TestMethod");
        assert_eq!(ctx.method_id, 1);
        assert!(ctx.response.is_none());
        assert!(ctx.source_id.is_none());
    }

    #[test]
    fn test_grain_call_context_with_source() {
        let target_id = create_test_grain_id();
        let source_id = GrainId::new(
            GrainType::create("SourceGrain"),
            IdSpan::from_str("source-key"),
        );

        let ctx = GrainCallContext::new(
            target_id,
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        )
        .with_source_id(source_id.clone());

        assert_eq!(ctx.source_id, Some(source_id));
    }

    #[test]
    fn test_grain_call_context_result() {
        let grain_id = create_test_grain_id();
        let mut ctx = GrainCallContext::new(
            grain_id,
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        assert!(!ctx.is_success());

        ctx.set_result(42i32);
        assert!(ctx.is_success());
        assert!(!ctx.is_exception());
        assert_eq!(ctx.get_result::<i32>(), Some(42));
    }

    #[test]
    fn test_grain_call_context_exception() {
        let grain_id = create_test_grain_id();
        let mut ctx = GrainCallContext::new(
            grain_id,
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        ctx.set_exception("Something went wrong");
        assert!(!ctx.is_success());
        assert!(ctx.is_exception());
    }

    #[test]
    fn test_grain_call_context_extensions() {
        let grain_id = create_test_grain_id();
        let ctx = GrainCallContext::new(
            grain_id,
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        ctx.set_extension("custom_data".to_string());
        let ext = ctx.get_extension::<String>();
        assert!(ext.is_some());
        assert_eq!(*ext.unwrap(), "custom_data".to_string());
    }

    #[test]
    fn test_incoming_call_context() {
        let grain_id = create_test_grain_id();
        let grain_type = GrainType::create("TestGrain");
        let ctx = IncomingGrainCallContext::new(
            grain_id.clone(),
            grain_type.clone(),
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        assert_eq!(ctx.target_id(), &grain_id);
        assert_eq!(ctx.grain_type(), &grain_type);
        assert_eq!(ctx.interface_name(), "ITestGrain");
        assert_eq!(ctx.method_name(), "TestMethod");
        assert_eq!(ctx.method_id(), 1);
        assert!(!ctx.was_invoked());
    }

    #[test]
    fn test_incoming_call_context_invoke() {
        let grain_id = create_test_grain_id();
        let mut ctx = IncomingGrainCallContext::new(
            grain_id,
            GrainType::create("TestGrain"),
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        ctx.set_invoke_callback(|ctx| {
            ctx.set_result(42i32);
            Ok(())
        });

        assert!(!ctx.was_invoked());
        let result = ctx.invoke();
        assert!(result.is_ok());
        assert!(ctx.was_invoked());
        assert_eq!(ctx.base.get_result::<i32>(), Some(42));
    }

    #[test]
    fn test_outgoing_call_context() {
        let grain_id = create_test_grain_id();
        let ctx = OutgoingGrainCallContext::new(
            grain_id.clone(),
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        assert_eq!(ctx.target_id(), &grain_id);
        assert_eq!(ctx.interface_name(), "ITestGrain");
        assert_eq!(ctx.method_name(), "TestMethod");
        assert_eq!(ctx.method_id(), 1);
        assert!(!ctx.was_invoked());
    }

    #[test]
    fn test_outgoing_call_context_invoke() {
        let grain_id = create_test_grain_id();
        let mut ctx = OutgoingGrainCallContext::new(
            grain_id,
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        ctx.set_invoke_callback(|ctx| {
            ctx.set_result("response".to_string());
            Ok(())
        });

        assert!(!ctx.was_invoked());
        let result = ctx.invoke();
        assert!(result.is_ok());
        assert!(ctx.was_invoked());
        assert_eq!(ctx.base.get_result::<String>(), Some("response".to_string()));
    }

    #[test]
    fn test_context_debug() {
        let grain_id = create_test_grain_id();
        let ctx = IncomingGrainCallContext::new(
            grain_id,
            GrainType::create("TestGrain"),
            GrainInterfaceType::create("ITestGrain"),
            "ITestGrain",
            "TestMethod",
            1,
            Bytes::new(),
        );

        let debug_str = format!("{:?}", ctx);
        assert!(debug_str.contains("IncomingGrainCallContext"));
        assert!(debug_str.contains("TestMethod"));
    }
}
