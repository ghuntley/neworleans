//! Built-in filter implementations.
//!
//! This module provides commonly used filters that are ready to use
//! in Orleans applications.

use async_trait::async_trait;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::context::{IncomingGrainCallContext, OutgoingGrainCallContext};
use crate::error::FilterResult;
use crate::filter::{IIncomingGrainCallFilter, IOutgoingGrainCallFilter};
use crate::request_context::{context_keys, RequestContext};

/// A filter that logs grain method calls with timing information.
///
/// # Example
///
/// ```ignore
/// let filter = LoggingFilter::new()
///     .with_log_level(LogLevel::Debug)
///     .log_arguments(true);
///
/// pipeline.add_filter(Arc::new(filter));
/// ```
#[derive(Debug, Clone)]
pub struct LoggingFilter {
    /// Whether to log at info level (default) or debug level.
    log_at_info: bool,

    /// Whether to log the request arguments.
    log_arguments: bool,

    /// Whether to log the response.
    log_response: bool,

    /// Filter order.
    order: i32,
}

impl LoggingFilter {
    /// Create a new logging filter with default settings.
    pub fn new() -> Self {
        Self {
            log_at_info: true,
            log_arguments: false,
            log_response: false,
            order: -100, // Run early
        }
    }

    /// Set whether to log at info level (true) or debug level (false).
    pub fn log_at_info(mut self, value: bool) -> Self {
        self.log_at_info = value;
        self
    }

    /// Set whether to log request arguments.
    pub fn log_arguments(mut self, value: bool) -> Self {
        self.log_arguments = value;
        self
    }

    /// Set whether to log the response.
    pub fn log_response(mut self, value: bool) -> Self {
        self.log_response = value;
        self
    }

    /// Set the filter order.
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }
}

impl Default for LoggingFilter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl IIncomingGrainCallFilter for LoggingFilter {
    async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
        let start = Instant::now();
        let interface_name = context.interface_name().to_string();
        let method_name = context.method_name().to_string();
        let grain_id = context.target_id().to_string();

        if self.log_at_info {
            info!(
                grain_id = %grain_id,
                interface = %interface_name,
                method = %method_name,
                "Incoming grain call"
            );
        } else {
            debug!(
                grain_id = %grain_id,
                interface = %interface_name,
                method = %method_name,
                "Incoming grain call"
            );
        }

        let result = context.invoke();

        let elapsed = start.elapsed();

        if let Some(response) = context.response() {
            if response.is_exception() {
                error!(
                    grain_id = %grain_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Grain call failed"
                );
            } else if self.log_at_info {
                info!(
                    grain_id = %grain_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Grain call completed"
                );
            } else {
                debug!(
                    grain_id = %grain_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Grain call completed"
                );
            }
        }

        result
    }

    fn name(&self) -> &str {
        "LoggingFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

#[async_trait]
impl IOutgoingGrainCallFilter for LoggingFilter {
    async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
        let start = Instant::now();
        let interface_name = context.interface_name().to_string();
        let method_name = context.method_name().to_string();
        let target_id = context.target_id().to_string();

        if self.log_at_info {
            info!(
                target_id = %target_id,
                interface = %interface_name,
                method = %method_name,
                "Outgoing grain call"
            );
        } else {
            debug!(
                target_id = %target_id,
                interface = %interface_name,
                method = %method_name,
                "Outgoing grain call"
            );
        }

        let result = context.invoke();

        let elapsed = start.elapsed();

        if let Some(response) = context.response() {
            if response.is_exception() {
                error!(
                    target_id = %target_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Outgoing call failed"
                );
            } else if self.log_at_info {
                info!(
                    target_id = %target_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Outgoing call completed"
                );
            } else {
                debug!(
                    target_id = %target_id,
                    interface = %interface_name,
                    method = %method_name,
                    duration_ms = elapsed.as_millis() as u64,
                    "Outgoing call completed"
                );
            }
        }

        result
    }

    fn name(&self) -> &str {
        "LoggingFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

/// A filter that propagates activity/tracing context through grain calls.
///
/// This filter integrates with distributed tracing by:
/// - Extracting trace IDs from incoming calls
/// - Propagating trace IDs to outgoing calls
/// - Creating spans for each grain method call
#[derive(Debug, Clone, Default)]
pub struct ActivityPropagationFilter {
    /// Filter order.
    order: i32,
}

impl ActivityPropagationFilter {
    /// Create a new activity propagation filter.
    pub fn new() -> Self {
        Self { order: -90 } // Run early, after logging
    }

    /// Set the filter order.
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }

    /// Generate a new trace ID.
    fn generate_trace_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{:016x}", now.as_nanos())
    }

    /// Generate a new span ID.
    fn generate_span_id() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        format!("{:08x}", (now.as_nanos() & 0xFFFFFFFF) as u32)
    }
}

#[async_trait]
impl IIncomingGrainCallFilter for ActivityPropagationFilter {
    async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
        // Extract trace context from incoming request
        let trace_id = context
            .context_properties()
            .get::<String>(context_keys::TRACE_ID)
            .unwrap_or_else(|| Self::generate_trace_id());

        let parent_span_id = context
            .context_properties()
            .get::<String>(context_keys::SPAN_ID);

        // Generate a new span for this call
        let span_id = Self::generate_span_id();

        // Update context properties
        context.context_properties_mut().set(context_keys::TRACE_ID, trace_id.clone());
        context.context_properties_mut().set(context_keys::SPAN_ID, span_id.clone());
        if let Some(parent) = parent_span_id {
            context.context_properties_mut().set(context_keys::PARENT_SPAN_ID, parent);
        }

        // Set up the tracing span
        let span = tracing::info_span!(
            "grain_call",
            trace_id = %trace_id,
            span_id = %span_id,
            grain_id = %context.target_id(),
            interface = %context.interface_name(),
            method = %context.method_name(),
        );

        let _guard = span.enter();

        debug!(
            trace_id = %trace_id,
            span_id = %span_id,
            "Activity propagation: incoming call"
        );

        context.invoke()
    }

    fn name(&self) -> &str {
        "ActivityPropagationFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

#[async_trait]
impl IOutgoingGrainCallFilter for ActivityPropagationFilter {
    async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
        // Get or create trace context
        let trace_id = RequestContext::get::<String>(context_keys::TRACE_ID)
            .or_else(|| context.context_properties().get(context_keys::TRACE_ID))
            .unwrap_or_else(|| Self::generate_trace_id());

        let current_span_id = RequestContext::get::<String>(context_keys::SPAN_ID)
            .or_else(|| context.context_properties().get(context_keys::SPAN_ID));

        // Generate new span for outgoing call
        let new_span_id = Self::generate_span_id();

        // Propagate trace context
        context.context_properties_mut().set(context_keys::TRACE_ID, trace_id.clone());
        context.context_properties_mut().set(context_keys::SPAN_ID, new_span_id.clone());
        if let Some(parent) = current_span_id {
            context.context_properties_mut().set(context_keys::PARENT_SPAN_ID, parent);
        }

        debug!(
            trace_id = %trace_id,
            span_id = %new_span_id,
            target_id = %context.target_id(),
            "Activity propagation: outgoing call"
        );

        context.invoke()
    }

    fn name(&self) -> &str {
        "ActivityPropagationFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

/// A filter that transforms exceptions into more user-friendly error messages.
///
/// This is useful for hiding internal implementation details from clients
/// while still providing useful error information.
#[derive(Debug, Clone, Default)]
pub struct ExceptionTransformFilter {
    /// Whether to include the original exception message.
    include_original_message: bool,

    /// Whether to include the stack trace.
    include_stack_trace: bool,

    /// Filter order.
    order: i32,
}

impl ExceptionTransformFilter {
    /// Create a new exception transform filter.
    pub fn new() -> Self {
        Self {
            include_original_message: false,
            include_stack_trace: false,
            order: 100, // Run late
        }
    }

    /// Set whether to include the original exception message.
    pub fn include_original_message(mut self, value: bool) -> Self {
        self.include_original_message = value;
        self
    }

    /// Set whether to include the stack trace.
    pub fn include_stack_trace(mut self, value: bool) -> Self {
        self.include_stack_trace = value;
        self
    }

    /// Set the filter order.
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }
}

#[async_trait]
impl IIncomingGrainCallFilter for ExceptionTransformFilter {
    async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
        let result = context.invoke();

        // Check if the response is an exception
        if let Some(response) = context.response() {
            if let Some(exc) = response.get_exception() {
                let transformed_message = if self.include_original_message {
                    format!("Operation failed: {}", exc.message)
                } else {
                    "An error occurred while processing the request".to_string()
                };

                warn!(
                    original_message = %exc.message,
                    "Transforming exception"
                );

                context.set_exception(transformed_message);
            }
        }

        result
    }

    fn name(&self) -> &str {
        "ExceptionTransformFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

#[async_trait]
impl IOutgoingGrainCallFilter for ExceptionTransformFilter {
    async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
        context.invoke()
    }

    fn name(&self) -> &str {
        "ExceptionTransformFilter"
    }

    fn order(&self) -> i32 {
        self.order
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use orleans_messaging::GrainInterfaceType;
    use crate::filter::{IIncomingGrainCallFilter, IOutgoingGrainCallFilter};
    use crate::response::Response;

    fn create_test_incoming_context() -> IncomingGrainCallContext {
        IncomingGrainCallContext::new(
            GrainId::new(GrainType::create("Test"), IdSpan::from_str("key")),
            GrainType::create("Test"),
            GrainInterfaceType::create("ITest"),
            "ITest",
            "Method",
            1,
            Bytes::new(),
        )
    }

    fn create_test_outgoing_context() -> OutgoingGrainCallContext {
        OutgoingGrainCallContext::new(
            GrainId::new(GrainType::create("Test"), IdSpan::from_str("key")),
            GrainInterfaceType::create("ITest"),
            "ITest",
            "Method",
            1,
            Bytes::new(),
        )
    }

    #[test]
    fn test_logging_filter_creation() {
        let filter = LoggingFilter::new();
        assert!(filter.log_at_info);
        assert!(!filter.log_arguments);
        assert!(!filter.log_response);
    }

    #[test]
    fn test_logging_filter_builder() {
        let filter = LoggingFilter::new()
            .log_at_info(false)
            .log_arguments(true)
            .log_response(true)
            .with_order(10);

        assert!(!filter.log_at_info);
        assert!(filter.log_arguments);
        assert!(filter.log_response);
        assert_eq!(IIncomingGrainCallFilter::order(&filter), 10);
    }

    #[tokio::test]
    async fn test_logging_filter_incoming() {
        let filter = LoggingFilter::new();
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(42i32);
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_logging_filter_outgoing() {
        let filter = LoggingFilter::new();
        let mut context = create_test_outgoing_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(42i32);
            Ok(())
        });

        let result = IOutgoingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_activity_filter_creation() {
        let filter = ActivityPropagationFilter::new();
        assert_eq!(IIncomingGrainCallFilter::order(&filter), -90);
    }

    #[tokio::test]
    async fn test_activity_filter_incoming() {
        let filter = ActivityPropagationFilter::new();
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());

        // Verify trace context was set
        assert!(context.context_properties().contains_key(context_keys::TRACE_ID));
        assert!(context.context_properties().contains_key(context_keys::SPAN_ID));
    }

    #[tokio::test]
    async fn test_activity_filter_outgoing() {
        let filter = ActivityPropagationFilter::new();
        let mut context = create_test_outgoing_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = IOutgoingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());

        // Verify trace context was set
        assert!(context.context_properties().contains_key(context_keys::TRACE_ID));
        assert!(context.context_properties().contains_key(context_keys::SPAN_ID));
    }

    #[test]
    fn test_exception_transform_filter_creation() {
        let filter = ExceptionTransformFilter::new();
        assert!(!filter.include_original_message);
        assert!(!filter.include_stack_trace);
        assert_eq!(IIncomingGrainCallFilter::order(&filter), 100);
    }

    #[tokio::test]
    async fn test_exception_transform_filter_success() {
        let filter = ExceptionTransformFilter::new();
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(42i32);
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());
        assert!(context.response().unwrap().is_success());
    }

    #[tokio::test]
    async fn test_exception_transform_filter_exception() {
        let filter = ExceptionTransformFilter::new();
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_response(Response::from_exception("Internal error details"));
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());

        let response = context.response().unwrap();
        assert!(response.is_exception());
        let exc = response.get_exception().unwrap();
        // Should not contain original message
        assert!(!exc.message.contains("Internal error details"));
    }

    #[tokio::test]
    async fn test_exception_transform_filter_with_original() {
        let filter = ExceptionTransformFilter::new().include_original_message(true);
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_response(Response::from_exception("Specific error"));
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());

        let response = context.response().unwrap();
        let exc = response.get_exception().unwrap();
        // Should contain original message
        assert!(exc.message.contains("Specific error"));
    }

    #[test]
    fn test_generate_trace_id() {
        let id1 = ActivityPropagationFilter::generate_trace_id();
        let id2 = ActivityPropagationFilter::generate_trace_id();

        // IDs should be non-empty
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());

        // Format should be hex
        assert!(id1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_span_id() {
        let id1 = ActivityPropagationFilter::generate_span_id();
        let id2 = ActivityPropagationFilter::generate_span_id();

        // IDs should be non-empty
        assert!(!id1.is_empty());
        assert!(!id2.is_empty());

        // Format should be hex
        assert!(id1.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
