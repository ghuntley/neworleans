//! Filter pipeline execution.
//!
//! This module provides the infrastructure for executing filters
//! in order, forming a middleware pipeline for grain method calls.

use std::sync::Arc;
use tracing::{debug, instrument, warn};

use crate::context::{IncomingGrainCallContext, OutgoingGrainCallContext};
use crate::error::{FilterError, FilterResult};
use crate::filter::{IIncomingGrainCallFilter, IOutgoingGrainCallFilter};
use crate::response::Response;

/// Configuration for filter pipeline execution.
#[derive(Debug, Clone)]
pub struct PipelineOptions {
    /// Whether to enforce that all filters call invoke().
    pub enforce_chain_continuation: bool,

    /// Whether to enforce that a response is set after invocation.
    pub enforce_response_set: bool,

    /// Maximum number of filters in a pipeline.
    pub max_filters: usize,
}

impl Default for PipelineOptions {
    fn default() -> Self {
        Self {
            enforce_chain_continuation: true,
            enforce_response_set: true,
            max_filters: 100,
        }
    }
}

/// A pipeline of incoming grain call filters.
///
/// Filters are executed in order, with each filter calling `context.invoke()`
/// to continue the chain.
pub struct IncomingFilterPipeline {
    /// The filters in execution order.
    filters: Vec<Arc<dyn IIncomingGrainCallFilter>>,

    /// Pipeline options.
    options: PipelineOptions,
}

impl IncomingFilterPipeline {
    /// Create a new empty pipeline.
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            options: PipelineOptions::default(),
        }
    }

    /// Create a new pipeline with the given options.
    pub fn with_options(options: PipelineOptions) -> Self {
        Self {
            filters: Vec::new(),
            options,
        }
    }

    /// Add a filter to the pipeline.
    pub fn add_filter(&mut self, filter: Arc<dyn IIncomingGrainCallFilter>) -> FilterResult<()> {
        if self.filters.len() >= self.options.max_filters {
            return Err(FilterError::Configuration(format!(
                "Maximum filter count ({}) exceeded",
                self.options.max_filters
            )));
        }
        self.filters.push(filter);
        Ok(())
    }

    /// Sort filters by their order.
    pub fn sort_by_order(&mut self) {
        self.filters.sort_by_key(|f| f.order());
    }

    /// Get the number of filters.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Check if the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Execute the filter pipeline.
    ///
    /// # Arguments
    ///
    /// * `context` - The call context
    /// * `final_handler` - The handler to invoke after all filters
    #[instrument(skip(self, context, final_handler), fields(
        target_id = %context.target_id(),
        method = %context.method_name(),
        filter_count = self.filters.len()
    ))]
    pub async fn execute<F>(
        &self,
        context: &mut IncomingGrainCallContext,
        final_handler: F,
    ) -> FilterResult<()>
    where
        F: FnOnce(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        if self.filters.is_empty() {
            debug!("No filters, invoking final handler directly");
            return final_handler(context);
        }

        // Execute filters sequentially
        let result = self.execute_filters(context, final_handler).await;

        // Verify response is set
        if self.options.enforce_response_set && result.is_ok() && context.response().is_none() {
            return Err(FilterError::NoResponseSet {
                filter_name: "pipeline".to_string(),
            });
        }

        result
    }

    /// Execute all filters in sequence.
    async fn execute_filters<F>(
        &self,
        context: &mut IncomingGrainCallContext,
        final_handler: F,
    ) -> FilterResult<()>
    where
        F: FnOnce(&mut IncomingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        // We'll execute filters one at a time
        // Each filter sets up a callback that marks invocation happened
        // After all filters run, we call the final handler

        for (stage, filter) in self.filters.iter().enumerate() {
            let filter_name = filter.name().to_string();

            debug!(stage, filter_name = %filter_name, "Executing incoming filter");

            // Set up the invoke callback for this stage
            // The callback just marks the context as having been invoked
            context.set_invoke_callback(move |ctx| {
                // Mark as invoked by setting a placeholder response
                // The actual response will be set later
                if ctx.response().is_none() {
                    ctx.set_response(Response::completed());
                }
                Ok(())
            });

            // Execute the filter
            let result = filter.invoke(context).await;

            if let Err(e) = result {
                warn!(stage, filter_name = %filter_name, error = %e, "Filter error");
                return Err(e);
            }

            // Check if the filter called invoke()
            if self.options.enforce_chain_continuation && !context.was_invoked() {
                return Err(FilterError::BrokenFilterChain {
                    stage,
                    filter_name,
                });
            }
        }

        // All filters have run, now invoke the final handler
        debug!("All filters executed, invoking final handler");
        final_handler(context)
    }
}

impl Default for IncomingFilterPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// A pipeline of outgoing grain call filters.
pub struct OutgoingFilterPipeline {
    /// The filters in execution order.
    filters: Vec<Arc<dyn IOutgoingGrainCallFilter>>,

    /// Pipeline options.
    options: PipelineOptions,
}

impl OutgoingFilterPipeline {
    /// Create a new empty pipeline.
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
            options: PipelineOptions::default(),
        }
    }

    /// Create a new pipeline with the given options.
    pub fn with_options(options: PipelineOptions) -> Self {
        Self {
            filters: Vec::new(),
            options,
        }
    }

    /// Add a filter to the pipeline.
    pub fn add_filter(&mut self, filter: Arc<dyn IOutgoingGrainCallFilter>) -> FilterResult<()> {
        if self.filters.len() >= self.options.max_filters {
            return Err(FilterError::Configuration(format!(
                "Maximum filter count ({}) exceeded",
                self.options.max_filters
            )));
        }
        self.filters.push(filter);
        Ok(())
    }

    /// Sort filters by their order.
    pub fn sort_by_order(&mut self) {
        self.filters.sort_by_key(|f| f.order());
    }

    /// Get the number of filters.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Check if the pipeline is empty.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Execute the filter pipeline.
    #[instrument(skip(self, context, final_handler), fields(
        target_id = %context.target_id(),
        method = %context.method_name(),
        filter_count = self.filters.len()
    ))]
    pub async fn execute<F>(
        &self,
        context: &mut OutgoingGrainCallContext,
        final_handler: F,
    ) -> FilterResult<()>
    where
        F: FnOnce(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        if self.filters.is_empty() {
            debug!("No filters, invoking final handler directly");
            return final_handler(context);
        }

        let result = self.execute_filters(context, final_handler).await;

        if self.options.enforce_response_set && result.is_ok() && context.response().is_none() {
            return Err(FilterError::NoResponseSet {
                filter_name: "pipeline".to_string(),
            });
        }

        result
    }

    /// Execute all filters in sequence.
    async fn execute_filters<F>(
        &self,
        context: &mut OutgoingGrainCallContext,
        final_handler: F,
    ) -> FilterResult<()>
    where
        F: FnOnce(&mut OutgoingGrainCallContext) -> FilterResult<()> + Send + 'static,
    {
        for (stage, filter) in self.filters.iter().enumerate() {
            let filter_name = filter.name().to_string();

            debug!(stage, filter_name = %filter_name, "Executing outgoing filter");

            context.set_invoke_callback(move |ctx| {
                if ctx.response().is_none() {
                    ctx.set_response(Response::completed());
                }
                Ok(())
            });

            let result = filter.invoke(context).await;

            if let Err(e) = result {
                warn!(stage, filter_name = %filter_name, error = %e, "Filter error");
                return Err(e);
            }

            if self.options.enforce_chain_continuation && !context.was_invoked() {
                return Err(FilterError::BrokenFilterChain {
                    stage,
                    filter_name,
                });
            }
        }

        debug!("All filters executed, invoking final handler");
        final_handler(context)
    }
}

impl Default for OutgoingFilterPipeline {
    fn default() -> Self {
        Self::new()
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

    struct TestFilter {
        id: u32,
        counter: Arc<AtomicU32>,
    }

    #[async_trait::async_trait]
    impl IIncomingGrainCallFilter for TestFilter {
        async fn invoke(&self, context: &mut IncomingGrainCallContext) -> FilterResult<()> {
            // Record this filter was called
            self.counter.fetch_add(1, Ordering::SeqCst);
            // Continue the chain
            context.invoke()
        }

        fn name(&self) -> &str {
            "TestFilter"
        }

        fn order(&self) -> i32 {
            self.id as i32
        }
    }

    #[async_trait::async_trait]
    impl IOutgoingGrainCallFilter for TestFilter {
        async fn invoke(&self, context: &mut OutgoingGrainCallContext) -> FilterResult<()> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            context.invoke()
        }

        fn name(&self) -> &str {
            "TestFilter"
        }

        fn order(&self) -> i32 {
            self.id as i32
        }
    }

    #[test]
    fn test_pipeline_options_default() {
        let options = PipelineOptions::default();
        assert!(options.enforce_chain_continuation);
        assert!(options.enforce_response_set);
        assert_eq!(options.max_filters, 100);
    }

    #[test]
    fn test_incoming_pipeline_creation() {
        let pipeline = IncomingFilterPipeline::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);
    }

    #[test]
    fn test_incoming_pipeline_add_filter() {
        let mut pipeline = IncomingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));
        let filter = Arc::new(TestFilter { id: 1, counter });

        pipeline.add_filter(filter).unwrap();
        assert_eq!(pipeline.len(), 1);
    }

    #[test]
    fn test_incoming_pipeline_max_filters() {
        let mut pipeline = IncomingFilterPipeline::with_options(PipelineOptions {
            max_filters: 2,
            ..Default::default()
        });

        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 2, counter: counter.clone() })).unwrap();

        // Third should fail
        let result = pipeline.add_filter(Arc::new(TestFilter { id: 3, counter }));
        assert!(result.is_err());
    }

    #[test]
    fn test_incoming_pipeline_sort() {
        let mut pipeline = IncomingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 3, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 2, counter })).unwrap();

        pipeline.sort_by_order();

        // Verify order by checking the internal filter orders
        assert_eq!(pipeline.filters[0].order(), 1);
        assert_eq!(pipeline.filters[1].order(), 2);
        assert_eq!(pipeline.filters[2].order(), 3);
    }

    #[tokio::test]
    async fn test_incoming_pipeline_empty_execution() {
        let pipeline = IncomingFilterPipeline::new();
        let mut context = create_test_incoming_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result(42i32);
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(context.base.get_result::<i32>(), Some(42));
    }

    #[tokio::test]
    async fn test_incoming_pipeline_single_filter() {
        let mut pipeline = IncomingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();

        let mut context = create_test_incoming_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result(42i32);
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_incoming_pipeline_multiple_filters() {
        let mut pipeline = IncomingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 2, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 3, counter: counter.clone() })).unwrap();

        let mut context = create_test_incoming_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result(42i32);
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_outgoing_pipeline_empty_execution() {
        let pipeline = OutgoingFilterPipeline::new();
        let mut context = create_test_outgoing_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result("response".to_string());
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(context.base.get_result::<String>(), Some("response".to_string()));
    }

    #[tokio::test]
    async fn test_outgoing_pipeline_single_filter() {
        let mut pipeline = OutgoingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();

        let mut context = create_test_outgoing_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result("response".to_string());
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_outgoing_pipeline_multiple_filters() {
        let mut pipeline = OutgoingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));

        pipeline.add_filter(Arc::new(TestFilter { id: 1, counter: counter.clone() })).unwrap();
        pipeline.add_filter(Arc::new(TestFilter { id: 2, counter: counter.clone() })).unwrap();

        let mut context = create_test_outgoing_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result("response".to_string());
            Ok(())
        }).await;

        assert!(result.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_outgoing_pipeline_creation() {
        let pipeline = OutgoingFilterPipeline::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);
    }

    #[test]
    fn test_outgoing_pipeline_add_filter() {
        let mut pipeline = OutgoingFilterPipeline::new();
        let counter = Arc::new(AtomicU32::new(0));
        let filter = Arc::new(TestFilter { id: 1, counter });

        pipeline.add_filter(filter).unwrap();
        assert_eq!(pipeline.len(), 1);
    }

    struct BrokenFilter;

    #[async_trait::async_trait]
    impl IIncomingGrainCallFilter for BrokenFilter {
        async fn invoke(&self, _context: &mut IncomingGrainCallContext) -> FilterResult<()> {
            // Does not call context.invoke() - broken!
            Ok(())
        }

        fn name(&self) -> &str {
            "BrokenFilter"
        }
    }

    #[tokio::test]
    async fn test_broken_filter_chain_detection() {
        let mut pipeline = IncomingFilterPipeline::new();
        pipeline.add_filter(Arc::new(BrokenFilter)).unwrap();

        let mut context = create_test_incoming_context();

        let result = pipeline.execute(&mut context, |ctx| {
            ctx.set_result(42i32);
            Ok(())
        }).await;

        assert!(result.is_err());
        match result {
            Err(FilterError::BrokenFilterChain { stage, filter_name }) => {
                assert_eq!(stage, 0);
                assert_eq!(filter_name, "BrokenFilter");
            }
            _ => panic!("Expected BrokenFilterChain error"),
        }
    }
}
