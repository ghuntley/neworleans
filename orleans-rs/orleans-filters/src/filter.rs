//! Filter traits for intercepting grain method calls.
//!
//! This module defines the core filter interfaces for both incoming
//! (server-side) and outgoing (client-side) grain calls.

use async_trait::async_trait;
use std::sync::Arc;

use crate::context::{IncomingGrainCallContext, OutgoingGrainCallContext};
use crate::error::FilterResult;

/// A filter for incoming grain method calls (server-side).
///
/// Incoming filters are executed when a grain receives a method call.
/// They can be used for:
/// - Logging and tracing
/// - Authentication and authorization
/// - Input validation
/// - Exception handling
/// - Performance monitoring
///
/// # Implementation
///
/// Filters must call `context.invoke()` to continue the filter chain.
/// After `invoke()` returns, the response should be set in the context.
///
/// # Example
///
/// ```ignore
/// struct LoggingFilter {
///     logger: Logger,
/// }
///
/// #[async_trait]
/// impl IIncomingGrainCallFilter for LoggingFilter {
///     async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
///         let start = Instant::now();
///         self.logger.info("Calling {}.{}", context.interface_name(), context.method_name());
///
///         context.invoke()?;
///
///         self.logger.info("Completed in {:?}", start.elapsed());
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait IIncomingGrainCallFilter: Send + Sync {
    /// Invoke the filter.
    ///
    /// The filter should call `context.invoke()` to continue the chain.
    /// After `invoke()` returns, the response should be available.
    async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()>;

    /// Get the filter name for diagnostics.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Get the filter order (lower values execute first).
    /// Default is 0.
    fn order(&self) -> i32 {
        0
    }
}

/// A filter for outgoing grain method calls (client-side).
///
/// Outgoing filters are executed when making a call to another grain.
/// They can be used for:
/// - Request tracing and correlation
/// - Request transformation
/// - Caching
/// - Retry logic
/// - Circuit breaking
///
/// # Implementation
///
/// Filters must call `context.invoke()` to continue the filter chain.
/// After `invoke()` returns, the response should be set in the context.
///
/// # Example
///
/// ```ignore
/// struct TracingFilter {
///     tracer: Tracer,
/// }
///
/// #[async_trait]
/// impl IOutgoingGrainCallFilter for TracingFilter {
///     async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
///         // Add tracing headers
///         context.context_properties_mut().set("trace_id", self.tracer.new_trace_id());
///
///         context.invoke()?;
///
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait IOutgoingGrainCallFilter: Send + Sync {
    /// Invoke the filter.
    ///
    /// The filter should call `context.invoke()` to continue the chain.
    /// After `invoke()` returns, the response should be available.
    async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()>;

    /// Get the filter name for diagnostics.
    fn name(&self) -> &str {
        std::any::type_name::<Self>()
    }

    /// Get the filter order (lower values execute first).
    /// Default is 0.
    fn order(&self) -> i32 {
        0
    }
}

/// A delegate-based incoming filter.
///
/// This allows creating filters from closures or function pointers.
pub type IncomingGrainCallFilterDelegate =
    Arc<dyn Fn(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + Sync>;

/// A delegate-based outgoing filter.
pub type OutgoingGrainCallFilterDelegate =
    Arc<dyn Fn(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + Sync>;

/// Wrapper for delegate-based incoming filters.
pub struct DelegateIncomingFilter {
    delegate: IncomingGrainCallFilterDelegate,
    name: String,
    order: i32,
}

impl DelegateIncomingFilter {
    /// Create a new delegate filter.
    pub fn new<F>(name: impl Into<String>, delegate: F) -> Self
    where
        F: Fn(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + Sync + 'static,
    {
        Self {
            delegate: Arc::new(delegate),
            name: name.into(),
            order: 0,
        }
    }

    /// Set the filter order.
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }
}

#[async_trait]
impl IIncomingGrainCallFilter for DelegateIncomingFilter {
    async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
        (self.delegate)(context)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn order(&self) -> i32 {
        self.order
    }
}

/// Wrapper for delegate-based outgoing filters.
pub struct DelegateOutgoingFilter {
    delegate: OutgoingGrainCallFilterDelegate,
    name: String,
    order: i32,
}

impl DelegateOutgoingFilter {
    /// Create a new delegate filter.
    pub fn new<F>(name: impl Into<String>, delegate: F) -> Self
    where
        F: Fn(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + Sync + 'static,
    {
        Self {
            delegate: Arc::new(delegate),
            name: name.into(),
            order: 0,
        }
    }

    /// Set the filter order.
    pub fn with_order(mut self, order: i32) -> Self {
        self.order = order;
        self
    }
}

#[async_trait]
impl IOutgoingGrainCallFilter for DelegateOutgoingFilter {
    async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
        (self.delegate)(context)
    }

    fn name(&self) -> &str {
        &self.name
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
    use std::sync::atomic::{AtomicU32, Ordering};

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

    struct CountingFilter {
        count: AtomicU32,
    }

    impl CountingFilter {
        fn new() -> Self {
            Self {
                count: AtomicU32::new(0),
            }
        }

        fn count(&self) -> u32 {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl IIncomingGrainCallFilter for CountingFilter {
        async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            context.invoke()
        }
    }

    #[async_trait]
    impl IOutgoingGrainCallFilter for CountingFilter {
        async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            context.invoke()
        }
    }

    #[tokio::test]
    async fn test_incoming_filter_invoke() {
        let filter = CountingFilter::new();
        let mut context = create_test_incoming_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = IIncomingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());
        assert_eq!(filter.count(), 1);
    }

    #[tokio::test]
    async fn test_outgoing_filter_invoke() {
        let filter = CountingFilter::new();
        let mut context = create_test_outgoing_context();

        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = IOutgoingGrainCallFilter::invoke(&filter, &mut context).await;
        assert!(result.is_ok());
        assert_eq!(filter.count(), 1);
    }

    #[tokio::test]
    async fn test_delegate_incoming_filter() {
        let invoked = Arc::new(AtomicU32::new(0));
        let invoked_clone = invoked.clone();

        let filter = DelegateIncomingFilter::new("TestFilter", move |ctx| {
            invoked_clone.fetch_add(1, Ordering::SeqCst);
            ctx.invoke()
        });

        assert_eq!(filter.name(), "TestFilter");
        assert_eq!(filter.order(), 0);

        let mut context = create_test_incoming_context();
        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = filter.invoke(&mut context).await;
        assert!(result.is_ok());
        assert_eq!(invoked.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_delegate_outgoing_filter() {
        let invoked = Arc::new(AtomicU32::new(0));
        let invoked_clone = invoked.clone();

        let filter = DelegateOutgoingFilter::new("TestFilter", move |ctx| {
            invoked_clone.fetch_add(1, Ordering::SeqCst);
            ctx.invoke()
        });

        assert_eq!(filter.name(), "TestFilter");
        assert_eq!(filter.order(), 0);

        let mut context = create_test_outgoing_context();
        context.set_invoke_callback(|ctx| {
            ctx.set_result(());
            Ok(())
        });

        let result = filter.invoke(&mut context).await;
        assert!(result.is_ok());
        assert_eq!(invoked.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_delegate_filter_with_order() {
        let filter = DelegateIncomingFilter::new("TestFilter", |ctx| ctx.invoke())
            .with_order(10);

        assert_eq!(filter.order(), 10);
    }

    #[test]
    fn test_filter_name_default() {
        let filter = CountingFilter::new();
        assert!(IIncomingGrainCallFilter::name(&filter).contains("CountingFilter"));
    }
}
