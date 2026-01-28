//! Orleans Stateless Workers - High-throughput parallelizable grain operations.
//!
//! Stateless workers are grains designed for high-throughput, parallelizable operations
//! without state preservation between calls. They enable multiple concurrent activations
//! of the same grain identity on each silo, providing automatic load balancing for
//! CPU-intensive or I/O-bound operations.
//!
//! # Key Features
//!
//! - **Multiple Activations**: Unlike regular grains (one per grain ID), stateless workers
//!   can have multiple activations per silo (up to `max_local`).
//! - **No Directory Registration**: Stateless workers bypass the grain directory for faster
//!   placement decisions.
//! - **Adaptive Pool Sizing**: Uses a PID controller to automatically adjust the worker pool
//!   based on load, removing idle workers to conserve resources.
//! - **Local Preference**: Placement prefers the local silo for better cache locality.
//! - **Unordered Execution**: Messages can execute in any order across workers.
//!
//! # When to Use Stateless Workers
//!
//! - CPU-intensive parallel processing (map/reduce, data transformation)
//! - I/O-bound operations (HTTP calls, database queries)
//! - Request-response APIs (REST endpoints, message processing)
//! - Operations that don't need state between calls
//!
//! # When NOT to Use Stateless Workers
//!
//! - Stateful operations requiring state between calls
//! - Long-running computations needing explicit lifecycle
//! - Coordinated workflows requiring guaranteed ordering
//! - User sessions requiring state persistence
//!
//! # Example
//!
//! ```rust,ignore
//! use orleans_stateless_workers::{
//!     StatelessWorkerContext, StatelessWorkerPlacement, StatelessWorkerOptions,
//! };
//!
//! // Create a stateless worker context with 8 max workers
//! let placement = StatelessWorkerPlacement::with_max_local(8);
//! let options = StatelessWorkerOptions::default();
//! let ctx = StatelessWorkerContext::new(grain_id, silo_address, placement, options);
//!
//! // Start the context (begins idle worker collection)
//! ctx.start()?;
//!
//! // Route messages to workers
//! let worker_id = ctx.route_message()?;
//!
//! // Track processing
//! ctx.worker_start_processing(&worker_id);
//! // ... process message ...
//! ctx.worker_finish_processing(&worker_id);
//!
//! // Shutdown gracefully
//! ctx.shutdown().await?;
//! ```
//!
//! # Architecture
//!
//! ```text
//! StatelessWorkerContext (coordinator)
//!     └─ WorkerState[] (workers)
//!         ├─ Worker 1 (unique ActivationId)
//!         ├─ Worker 2 (unique ActivationId)
//!         └─ Worker N (unique ActivationId)
//! ```
//!
//! The coordinator manages a pool of workers, routing messages using this priority:
//! 1. Reuse inactive workers (highest priority)
//! 2. Create new worker if pool not full
//! 3. Queue to worker with minimum waiting count
//!
//! # PID Controller
//!
//! The adaptive pool sizing uses a PID controller with tuned constants:
//! - Kp (Proportional): 0.433
//! - Ki (Integral): 0.468
//! - Kd (Derivative): 0.480
//!
//! These values were optimized via genetic algorithm in the original Orleans implementation.

pub mod context;
pub mod director;
pub mod error;
pub mod options;
pub mod pid_controller;
pub mod worker_state;

// Re-export main types
pub use context::{StatelessWorkerContext, WorkItem};
pub use director::{PlacementContext, StatelessWorkerDirector};
pub use error::{StatelessWorkerError, StatelessWorkerResult};
pub use options::{StatelessWorkerOptions, StatelessWorkerPlacement};
pub use pid_controller::PidController;
pub use worker_state::{WorkerPoolStats, WorkerState};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_grain_id() -> GrainId {
        GrainId::new(
            GrainType::create("test.stateless.worker"),
            IdSpan::from_str("test-1"),
        )
    }

    fn test_silo() -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111),
            1,
        )
    }

    #[test]
    fn test_integration_basic_flow() {
        let placement = StatelessWorkerPlacement::with_max_local(4);
        // Disable idle worker removal to avoid needing tokio runtime
        let options = StatelessWorkerOptions::for_testing()
            .with_remove_idle_workers(false);
        let ctx = StatelessWorkerContext::new(
            test_grain_id(),
            test_silo(),
            placement,
            options,
        );

        // Start context (no timer started since remove_idle_workers is false)
        ctx.start().unwrap();

        // Route first message - should create worker
        let id1 = ctx.route_message().unwrap();
        assert_eq!(ctx.worker_count(), 1);

        // Process message
        ctx.worker_start_processing(&id1);
        ctx.worker_finish_processing(&id1);

        // Route second message - should reuse idle worker
        let id2 = ctx.route_message().unwrap();
        assert_eq!(id1, id2);
        assert_eq!(ctx.worker_count(), 1);

        // Keep first worker busy
        ctx.worker_start_processing(&id2);

        // Route third message - should create new worker
        let id3 = ctx.route_message().unwrap();
        assert_ne!(id2, id3);
        assert_eq!(ctx.worker_count(), 2);
    }

    #[test]
    fn test_integration_pool_limit() {
        let placement = StatelessWorkerPlacement::with_max_local(2);
        let options = StatelessWorkerOptions::for_testing();
        let ctx = StatelessWorkerContext::new(
            test_grain_id(),
            test_silo(),
            placement,
            options,
        );

        // Fill the pool
        let id1 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id1);

        let id2 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id2);

        assert_eq!(ctx.worker_count(), 2);

        // Next message should go to existing worker (pool full)
        let id3 = ctx.route_message().unwrap();
        assert!(id3 == id1 || id3 == id2);
        assert_eq!(ctx.worker_count(), 2);
    }

    #[test]
    fn test_integration_director() {
        let director = StatelessWorkerDirector::new();
        let local = test_silo();
        let other = SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 22222),
            1,
        );

        // Local silo should be preferred
        let compatible = vec![other.clone(), local.clone()];
        let selected = director.select_silo(&local, false, &compatible);
        assert_eq!(selected, Some(local.clone()));

        // When local is terminating, should pick other
        let compatible = vec![other.clone()];
        let selected = director.select_silo(&local, true, &compatible);
        assert_eq!(selected, Some(other));
    }

    #[test]
    fn test_integration_pid_controller() {
        let mut pid = PidController::new();

        // Simulate load increase
        let signal1 = pid.compute(0.0); // No waiting
        let signal2 = pid.compute(5.0); // Some waiting
        let signal3 = pid.compute(10.0); // More waiting

        // Higher load should produce more negative signals
        assert!(signal2 <= signal1);
        assert!(signal3 <= signal2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_integration_shutdown() {
        let placement = StatelessWorkerPlacement::with_max_local(4);
        // Disable idle worker removal - no need to start the context
        let options = StatelessWorkerOptions::for_testing()
            .with_remove_idle_workers(false)
            .with_deactivation_timeout(std::time::Duration::from_millis(100));
        let ctx = StatelessWorkerContext::new(
            test_grain_id(),
            test_silo(),
            placement,
            options,
        );
        // Don't call start() - not needed when remove_idle_workers is false

        // Create some workers and complete their processing
        for _ in 0..3 {
            let id = ctx.route_message().unwrap();
            ctx.worker_start_processing(&id);
            ctx.worker_finish_processing(&id);
        }

        // Use tokio timeout to prevent hanging
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            ctx.shutdown()
        ).await;

        assert!(result.is_ok(), "shutdown timed out");
        assert!(ctx.is_shutting_down());
        assert_eq!(ctx.worker_count(), 0);
    }

    #[test]
    fn test_exports_are_accessible() {
        // Verify all public types are accessible
        let _: StatelessWorkerError = StatelessWorkerError::NoWorkersAvailable;
        let _: StatelessWorkerPlacement = StatelessWorkerPlacement::new();
        let _: StatelessWorkerOptions = StatelessWorkerOptions::default();
        let _: PidController = PidController::new();
        let _: StatelessWorkerDirector = StatelessWorkerDirector::new();
        let _: WorkerPoolStats = WorkerPoolStats::default();
    }
}
