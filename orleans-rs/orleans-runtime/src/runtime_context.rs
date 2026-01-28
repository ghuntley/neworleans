//! Runtime context - thread-local grain context tracking.
//!
//! This module provides thread-local storage for tracking which grain activation
//! is currently executing on a thread. This is fundamental to Orleans' turn-based
//! execution model where each grain processes messages sequentially.
//!
//! # Architecture
//!
//! ```text
//! Thread 1                        Thread 2
//! ┌──────────────────────┐       ┌──────────────────────┐
//! │ RuntimeContext       │       │ RuntimeContext       │
//! │ ┌──────────────────┐ │       │ ┌──────────────────┐ │
//! │ │ GrainContext A   │ │       │ │ GrainContext B   │ │
//! │ │ (currently exec) │ │       │ │ (currently exec) │ │
//! │ └──────────────────┘ │       │ └──────────────────┘ │
//! └──────────────────────┘       └──────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use orleans_runtime::{RuntimeContext, GrainContext};
//!
//! // Enter a grain context for execution
//! let _guard = RuntimeContext::enter(&grain_context);
//!
//! // Now RuntimeContext::current() returns Some(grain_context)
//! if let Some(ctx) = RuntimeContext::current() {
//!     println!("Executing in grain: {:?}", ctx.grain_id());
//! }
//!
//! // Guard drops, context is restored to previous (or None)
//! ```

use crate::grain_context::IGrainContext;
use std::cell::RefCell;
use std::sync::Arc;
use tracing::{debug, trace, warn};

thread_local! {
    /// The current grain context for this thread.
    ///
    /// This uses a stack to support nested context switches, though in practice
    /// Orleans typically uses single-level context per thread.
    static CURRENT_CONTEXT: RefCell<Option<Arc<dyn IGrainContext>>> = const { RefCell::new(None) };
}

/// Thread-local runtime context for grain execution tracking.
///
/// `RuntimeContext` provides access to the currently executing grain's context
/// on the current thread. This is essential for:
///
/// - Determining if code is executing within a grain activation
/// - Enabling inline task execution optimization
/// - Supporting grain-level logging and tracing
/// - Enforcing turn-based execution semantics
///
/// # Thread Safety
///
/// Each thread has its own isolated context. Context changes on one thread
/// do not affect other threads.
pub struct RuntimeContext;

impl RuntimeContext {
    /// Returns the current grain context if executing within a grain.
    ///
    /// Returns `None` if called from outside a grain execution context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(ctx) = RuntimeContext::current() {
    ///     tracing::info!(
    ///         grain_id = %ctx.grain_id(),
    ///         "Executing grain method"
    ///     );
    /// } else {
    ///     tracing::info!("Executing outside of grain context");
    /// }
    /// ```
    #[inline]
    pub fn current() -> Option<Arc<dyn IGrainContext>> {
        CURRENT_CONTEXT.with(|ctx| ctx.borrow().clone())
    }

    /// Returns the current grain context, panicking if not in a grain context.
    ///
    /// # Panics
    ///
    /// Panics if called from outside a grain execution context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Only use this when you're certain you're in a grain context
    /// let ctx = RuntimeContext::current_required();
    /// let grain_id = ctx.grain_id();
    /// ```
    pub fn current_required() -> Arc<dyn IGrainContext> {
        Self::current().expect("RuntimeContext::current_required called outside of grain context")
    }

    /// Checks if code is currently executing within a grain context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if RuntimeContext::is_grain_context() {
    ///     // Safe to call grain-specific operations
    /// }
    /// ```
    #[inline]
    pub fn is_grain_context() -> bool {
        CURRENT_CONTEXT.with(|ctx| ctx.borrow().is_some())
    }

    /// Enters a grain context for execution.
    ///
    /// Returns a guard that restores the previous context when dropped.
    /// This uses RAII to ensure context is always properly restored.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let _guard = RuntimeContext::enter(&grain_context);
    /// // grain_context is now active
    /// process_message().await;
    /// // guard drops here, previous context restored
    /// ```
    pub fn enter(context: &Arc<dyn IGrainContext>) -> RuntimeContextGuard {
        let previous = CURRENT_CONTEXT.with(|ctx| ctx.replace(Some(context.clone())));

        trace!(
            grain_id = %context.grain_id(),
            activation_id = %context.activation_id(),
            "Entering grain context"
        );

        RuntimeContextGuard { previous }
    }

    /// Temporarily exits the current grain context.
    ///
    /// Returns a guard that restores the context when dropped.
    /// Useful when executing code that should not be associated with any grain.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let _guard = RuntimeContext::exit();
    /// // No grain context active
    /// do_silo_level_work();
    /// // guard drops, original context restored
    /// ```
    pub fn exit() -> RuntimeContextGuard {
        let previous = CURRENT_CONTEXT.with(|ctx| ctx.replace(None));

        if previous.is_some() {
            trace!("Exiting grain context temporarily");
        }

        RuntimeContextGuard { previous }
    }

    /// Checks if the given context is the current context.
    ///
    /// This is used for inline task execution optimization - tasks can only
    /// be executed inline if they belong to the currently active context.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if RuntimeContext::is_current(&target_context) {
    ///     // Safe to execute inline
    ///     task.execute();
    /// } else {
    ///     // Must queue for later execution
    ///     scheduler.enqueue(task);
    /// }
    /// ```
    pub fn is_current(context: &Arc<dyn IGrainContext>) -> bool {
        CURRENT_CONTEXT.with(|ctx| {
            ctx.borrow()
                .as_ref()
                .map(|current| {
                    // Compare by grain ID and activation ID
                    current.grain_id() == context.grain_id()
                        && current.activation_id() == context.activation_id()
                })
                .unwrap_or(false)
        })
    }

    /// Validates that we're in the expected grain context.
    ///
    /// Logs a warning if the current context doesn't match the expected one.
    /// This is useful for debugging context-related issues.
    pub fn validate_context(expected: &Arc<dyn IGrainContext>) {
        if !Self::is_current(expected) {
            if let Some(current) = Self::current() {
                warn!(
                    expected_grain_id = %expected.grain_id(),
                    expected_activation_id = %expected.activation_id(),
                    actual_grain_id = %current.grain_id(),
                    actual_activation_id = %current.activation_id(),
                    "Context mismatch detected"
                );
            } else {
                warn!(
                    expected_grain_id = %expected.grain_id(),
                    expected_activation_id = %expected.activation_id(),
                    "Expected grain context but none is active"
                );
            }
        }
    }

    /// Gets the current grain ID if in a grain context.
    ///
    /// Convenience method that avoids cloning the full context.
    #[inline]
    pub fn current_grain_id() -> Option<orleans_core::GrainId> {
        CURRENT_CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| c.grain_id().clone()))
    }

    /// Gets the current activation ID if in a grain context.
    ///
    /// Convenience method that avoids cloning the full context.
    #[inline]
    pub fn current_activation_id() -> Option<orleans_core::ActivationId> {
        CURRENT_CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| c.activation_id().clone()))
    }
}

/// RAII guard for managing runtime context lifecycle.
///
/// When the guard is dropped, the previous context is automatically restored.
/// This ensures context is always properly managed even in the presence of panics.
pub struct RuntimeContextGuard {
    /// The previous context to restore when this guard is dropped.
    previous: Option<Arc<dyn IGrainContext>>,
}

impl Drop for RuntimeContextGuard {
    fn drop(&mut self) {
        let restored = CURRENT_CONTEXT.with(|ctx| ctx.replace(self.previous.take()));

        if let Some(exiting) = restored {
            debug!(
                grain_id = %exiting.grain_id(),
                activation_id = %exiting.activation_id(),
                "Exiting grain context"
            );
        }
    }
}

/// Extension trait for scoped execution within a grain context.
///
/// Provides convenient methods for running code within a specific grain context.
pub trait RuntimeContextExt {
    /// Execute a closure within this grain context.
    fn with_context<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R;

    /// Execute an async block within this grain context.
    ///
    /// Note: For async code, prefer using the guard pattern directly
    /// to ensure the context remains active across await points.
    fn with_context_async<F, Fut, R>(&self, f: F) -> impl std::future::Future<Output = R>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>;
}

impl RuntimeContextExt for Arc<dyn IGrainContext> {
    fn with_context<F, R>(&self, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let _guard = RuntimeContext::enter(self);
        f()
    }

    fn with_context_async<F, Fut, R>(&self, f: F) -> impl std::future::Future<Output = R>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = R>,
    {
        let ctx = self.clone();
        async move {
            let _guard = RuntimeContext::enter(&ctx);
            f().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grain_context::GrainContext;
    use crate::grain_factory::IGrainFactory;
    use crate::grain_reference::IGrainReference;
    use orleans_core::{ActivationId, GrainId, GrainType, IdSpan, SiloAddress};
    use std::net::SocketAddr;

    // Mock grain factory for testing
    struct MockGrainFactory;

    impl IGrainFactory for MockGrainFactory {
        fn get_grain_reference(
            &self,
            _grain_type: GrainType,
            _key: IdSpan,
        ) -> Arc<dyn IGrainReference> {
            unimplemented!()
        }
    }

    fn create_test_context(name: &str) -> Arc<dyn IGrainContext> {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("test-key"));
        let activation_id = ActivationId::new();
        let silo_address =
            SiloAddress::new("127.0.0.1:11111".parse::<SocketAddr>().unwrap(), 1234);
        let grain_factory: Arc<dyn IGrainFactory> = Arc::new(MockGrainFactory);

        Arc::new(GrainContext::new(
            grain_id,
            grain_type,
            activation_id,
            silo_address,
            grain_factory,
        ))
    }

    #[test]
    fn test_no_context_initially() {
        assert!(RuntimeContext::current().is_none());
        assert!(!RuntimeContext::is_grain_context());
    }

    #[test]
    fn test_enter_and_exit() {
        let ctx = create_test_context("TestGrain");

        assert!(!RuntimeContext::is_grain_context());

        {
            let _guard = RuntimeContext::enter(&ctx);
            assert!(RuntimeContext::is_grain_context());
            assert!(RuntimeContext::current().is_some());
            assert!(RuntimeContext::is_current(&ctx));
        }

        assert!(!RuntimeContext::is_grain_context());
    }

    #[test]
    fn test_nested_contexts() {
        let ctx1 = create_test_context("Grain1");
        let ctx2 = create_test_context("Grain2");

        {
            let _guard1 = RuntimeContext::enter(&ctx1);
            assert!(RuntimeContext::is_current(&ctx1));
            assert!(!RuntimeContext::is_current(&ctx2));

            {
                let _guard2 = RuntimeContext::enter(&ctx2);
                assert!(!RuntimeContext::is_current(&ctx1));
                assert!(RuntimeContext::is_current(&ctx2));
            }

            // After guard2 drops, ctx1 is restored
            assert!(RuntimeContext::is_current(&ctx1));
            assert!(!RuntimeContext::is_current(&ctx2));
        }

        assert!(!RuntimeContext::is_grain_context());
    }

    #[test]
    fn test_temporary_exit() {
        let ctx = create_test_context("TestGrain");

        let _guard1 = RuntimeContext::enter(&ctx);
        assert!(RuntimeContext::is_grain_context());

        {
            let _guard2 = RuntimeContext::exit();
            assert!(!RuntimeContext::is_grain_context());
        }

        // Context restored after exit guard drops
        assert!(RuntimeContext::is_grain_context());
    }

    #[test]
    fn test_current_required_panics() {
        let result = std::panic::catch_unwind(|| RuntimeContext::current_required());
        assert!(result.is_err());
    }

    #[test]
    fn test_current_required_succeeds() {
        let ctx = create_test_context("TestGrain");
        let _guard = RuntimeContext::enter(&ctx);

        let current = RuntimeContext::current_required();
        assert_eq!(current.grain_type().as_str(), Some("TestGrain"));
    }

    #[test]
    fn test_current_grain_id() {
        let ctx = create_test_context("TestGrain");

        assert!(RuntimeContext::current_grain_id().is_none());

        let _guard = RuntimeContext::enter(&ctx);
        let grain_id = RuntimeContext::current_grain_id();
        assert!(grain_id.is_some());
        assert_eq!(grain_id.unwrap().grain_type().as_str(), Some("TestGrain"));
    }

    #[test]
    fn test_current_activation_id() {
        let ctx = create_test_context("TestGrain");

        assert!(RuntimeContext::current_activation_id().is_none());

        let _guard = RuntimeContext::enter(&ctx);
        let activation_id = RuntimeContext::current_activation_id();
        assert!(activation_id.is_some());
    }

    #[test]
    fn test_with_context() {
        let ctx = create_test_context("TestGrain");

        let result = ctx.with_context(|| {
            assert!(RuntimeContext::is_grain_context());
            42
        });

        assert_eq!(result, 42);
        assert!(!RuntimeContext::is_grain_context());
    }

    #[tokio::test]
    async fn test_with_context_async() {
        let ctx = create_test_context("TestGrain");

        let result = ctx
            .with_context_async(|| async {
                // Note: Context is active here but may not survive across await
                // in the current implementation
                assert!(RuntimeContext::is_grain_context());
                42
            })
            .await;

        assert_eq!(result, 42);
        assert!(!RuntimeContext::is_grain_context());
    }

    #[test]
    fn test_different_threads_isolated() {
        use std::sync::Barrier;

        let ctx = create_test_context("TestGrain");
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = barrier.clone();
        let ctx_clone = ctx.clone();

        // Thread 1: Enter context
        let handle = std::thread::spawn(move || {
            let _guard = RuntimeContext::enter(&ctx_clone);
            assert!(RuntimeContext::is_grain_context());

            barrier_clone.wait();

            // Still in context after other thread checked
            assert!(RuntimeContext::is_grain_context());
        });

        // Main thread: Wait for thread 1 to enter context
        barrier.wait();

        // Main thread should NOT see the context from thread 1
        assert!(!RuntimeContext::is_grain_context());

        handle.join().unwrap();
    }

    #[test]
    fn test_validate_context_mismatch() {
        let ctx1 = create_test_context("Grain1");
        let ctx2 = create_test_context("Grain2");

        let _guard = RuntimeContext::enter(&ctx1);

        // This should log a warning (check logs manually or with tracing-test)
        RuntimeContext::validate_context(&ctx2);
    }

    #[test]
    fn test_validate_context_no_context() {
        let ctx = create_test_context("TestGrain");

        // This should log a warning about no active context
        RuntimeContext::validate_context(&ctx);
    }

    #[test]
    fn test_guard_drop_on_panic() {
        use std::panic::AssertUnwindSafe;

        let ctx = create_test_context("TestGrain");

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard = RuntimeContext::enter(&ctx);
            assert!(RuntimeContext::is_grain_context());
            panic!("Test panic");
        }));

        assert!(result.is_err());
        // Context should be cleaned up despite panic
        assert!(!RuntimeContext::is_grain_context());
    }
}
