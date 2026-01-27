//! Orleans Call Filters and Interceptors
//!
//! This crate provides a middleware pipeline for intercepting grain method calls.
//! Call filters enable cross-cutting concerns like logging, tracing, authentication,
//! and error handling without modifying grain code.
//!
//! # Architecture
//!
//! Filters form a pipeline that wraps grain method invocations:
//!
//! ```text
//! Request → [Filter 1] → [Filter 2] → [Grain Filter] → [Method] → Response
//! ```
//!
//! There are two types of filters:
//!
//! - **Incoming filters** (`IIncomingGrainCallFilter`): Execute when a grain receives a call
//! - **Outgoing filters** (`IOutgoingGrainCallFilter`): Execute when making a call to another grain
//!
//! # Example
//!
//! ```ignore
//! use orleans_filters::{
//!     IncomingFilterPipeline, LoggingFilter, ActivityPropagationFilter,
//!     IIncomingGrainCallFilter,
//! };
//! use std::sync::Arc;
//!
//! // Create a filter pipeline
//! let mut pipeline = IncomingFilterPipeline::new();
//!
//! // Add built-in filters
//! pipeline.add_filter(Arc::new(LoggingFilter::new()));
//! pipeline.add_filter(Arc::new(ActivityPropagationFilter::new()));
//!
//! // Execute the pipeline
//! pipeline.execute(&mut context, |ctx| {
//!     // Invoke the actual grain method
//!     grain.invoke_method(ctx.method_id(), ctx.request_body())
//! }).await?;
//! ```
//!
//! # Request Context
//!
//! The `RequestContext` provides task-local storage for propagating data through
//! async call chains:
//!
//! ```ignore
//! use orleans_filters::{RequestContext, ContextProperties};
//!
//! // Set context values
//! RequestContext::set("user_id", "user123".to_string());
//!
//! // Get context values
//! let user_id: Option<String> = RequestContext::get("user_id");
//!
//! // Run with a specific context
//! let mut props = ContextProperties::new();
//! props.set("tenant", "acme".to_string());
//!
//! RequestContext::scope(props, async {
//!     // Context is available here
//!     assert_eq!(RequestContext::get::<String>("tenant"), Some("acme".to_string()));
//! }).await;
//! ```
//!
//! # Built-in Filters
//!
//! The crate provides several ready-to-use filters:
//!
//! - `LoggingFilter`: Logs method calls with timing information
//! - `ActivityPropagationFilter`: Propagates distributed tracing context
//! - `ExceptionTransformFilter`: Transforms exceptions for client consumption
//!
//! # Custom Filters
//!
//! Implement `IIncomingGrainCallFilter` or `IOutgoingGrainCallFilter`:
//!
//! ```ignore
//! use async_trait::async_trait;
//! use orleans_filters::{
//!     IIncomingGrainCallFilter, IncomingGrainCallContext, FilterResult,
//! };
//!
//! struct AuthFilter {
//!     required_role: String,
//! }
//!
//! #[async_trait]
//! impl IIncomingGrainCallFilter for AuthFilter {
//!     async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
//!         // Check authorization
//!         let role: Option<String> = context.context_properties().get("role");
//!         if role.as_deref() != Some(&self.required_role) {
//!             context.set_exception("Access denied");
//!             return Ok(());
//!         }
//!
//!         // Continue the chain
//!         context.invoke()
//!     }
//! }
//! ```

pub mod builtin;
pub mod context;
pub mod error;
pub mod filter;
pub mod pipeline;
pub mod request_context;
pub mod response;

// Re-exports for convenience
pub use builtin::{ActivityPropagationFilter, ExceptionTransformFilter, LoggingFilter};
pub use context::{GrainCallContext, IncomingGrainCallContext, OutgoingGrainCallContext};
pub use error::{FilterError, FilterResult};
pub use filter::{
    DelegateIncomingFilter, DelegateOutgoingFilter, IIncomingGrainCallFilter,
    IOutgoingGrainCallFilter, IncomingGrainCallFilterDelegate, OutgoingGrainCallFilterDelegate,
};
pub use pipeline::{IncomingFilterPipeline, OutgoingFilterPipeline, PipelineOptions};
pub use request_context::{context_keys, ContextProperties, ContextValue, RequestContext};
pub use response::{Response, ResponseException, ResponseResult};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<LoggingFilter>();
        let _ = std::any::type_name::<ActivityPropagationFilter>();
        let _ = std::any::type_name::<ExceptionTransformFilter>();
        let _ = std::any::type_name::<IncomingFilterPipeline>();
        let _ = std::any::type_name::<OutgoingFilterPipeline>();
        let _ = std::any::type_name::<RequestContext>();
        let _ = std::any::type_name::<ContextProperties>();
        let _ = std::any::type_name::<Response>();
    }

    #[test]
    fn test_filter_error_display() {
        let err = FilterError::BrokenFilterChain {
            stage: 1,
            filter_name: "TestFilter".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("TestFilter"));
        assert!(msg.contains("stage 1"));
    }

    #[test]
    fn test_response_creation() {
        let completed = Response::completed();
        assert!(completed.is_success());

        let result = Response::from_result(42i32);
        assert!(result.is_success());
        assert_eq!(result.get_result::<i32>(), Some(42));

        let exception = Response::from_exception("error");
        assert!(exception.is_exception());
    }

    #[test]
    fn test_context_properties() {
        let mut props = ContextProperties::new();
        assert!(props.is_empty());

        props.set("key", "value".to_string());
        assert_eq!(props.get::<String>("key"), Some("value".to_string()));
        assert!(!props.is_empty());
    }

    #[tokio::test]
    async fn test_request_context_scope() {
        let mut props = ContextProperties::new();
        props.set("test_key", 123i32);

        RequestContext::scope(props, async {
            let value = RequestContext::get::<i32>("test_key");
            assert_eq!(value, Some(123));
        })
        .await;
    }

    #[test]
    fn test_builtin_filter_names() {
        let logging = LoggingFilter::new();
        assert_eq!(IIncomingGrainCallFilter::name(&logging), "LoggingFilter");

        let activity = ActivityPropagationFilter::new();
        assert_eq!(IIncomingGrainCallFilter::name(&activity), "ActivityPropagationFilter");

        let exception = ExceptionTransformFilter::new();
        assert_eq!(IIncomingGrainCallFilter::name(&exception), "ExceptionTransformFilter");
    }

    #[test]
    fn test_pipeline_options_default() {
        let options = PipelineOptions::default();
        assert!(options.enforce_chain_continuation);
        assert!(options.enforce_response_set);
        assert_eq!(options.max_filters, 100);
    }
}
