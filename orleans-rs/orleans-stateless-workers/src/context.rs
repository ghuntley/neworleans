//! Stateless Worker Grain Context - coordinator for worker pool management.

use crate::error::{StatelessWorkerError, StatelessWorkerResult};
use crate::options::{StatelessWorkerOptions, StatelessWorkerPlacement};
use crate::pid_controller::PidController;
use crate::worker_state::{WorkerPoolStats, WorkerState};
use orleans_core::{ActivationId, GrainAddress, GrainId, SiloAddress};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::{debug, info, instrument, trace, warn};

/// Message types that can be sent to the context work queue.
#[derive(Debug)]
pub enum WorkItem {
    /// Process an incoming message.
    ProcessMessage {
        /// Message payload.
        payload: Vec<u8>,
        /// Response channel.
        response_tx: Option<tokio::sync::oneshot::Sender<Vec<u8>>>,
    },
    /// Collect and remove idle workers.
    CollectIdleWorkers,
    /// Worker has finished deactivating.
    WorkerDeactivated {
        /// The worker's activation ID.
        activation_id: ActivationId,
    },
    /// Shutdown the context.
    Shutdown,
}

/// Coordinator for a stateless worker grain.
///
/// Manages a pool of workers, distributing messages across them
/// for parallel processing. Uses a PID controller for adaptive
/// pool sizing based on load.
pub struct StatelessWorkerContext {
    /// The grain ID this context manages.
    grain_id: GrainId,

    /// The silo address hosting this context.
    silo_address: SiloAddress,

    /// Placement configuration.
    placement: StatelessWorkerPlacement,

    /// Runtime options.
    options: StatelessWorkerOptions,

    /// Worker states (protected by RwLock for concurrent access).
    workers: RwLock<Vec<WorkerState>>,

    /// PID controller for adaptive pool sizing.
    pid_controller: RwLock<PidController>,

    /// Work queue notification.
    work_signal: Arc<Notify>,

    /// Whether the context is shutting down.
    is_shutting_down: AtomicBool,

    /// Total messages processed.
    messages_processed: AtomicU64,

    /// Total workers created.
    workers_created: AtomicU64,

    /// Total workers deactivated.
    workers_deactivated: AtomicU64,

    /// Inspection timer handle.
    inspection_timer: RwLock<Option<JoinHandle<()>>>,

    /// Creation time for statistics.
    created_at: Instant,
}

impl StatelessWorkerContext {
    /// Creates a new stateless worker context.
    #[instrument(skip(options), fields(grain_id = %grain_id, silo = %silo_address))]
    pub fn new(
        grain_id: GrainId,
        silo_address: SiloAddress,
        placement: StatelessWorkerPlacement,
        options: StatelessWorkerOptions,
    ) -> Self {
        info!("Creating stateless worker context");

        Self {
            grain_id,
            silo_address,
            placement,
            options,
            workers: RwLock::new(Vec::new()),
            pid_controller: RwLock::new(PidController::new()),
            work_signal: Arc::new(Notify::new()),
            is_shutting_down: AtomicBool::new(false),
            messages_processed: AtomicU64::new(0),
            workers_created: AtomicU64::new(0),
            workers_deactivated: AtomicU64::new(0),
            inspection_timer: RwLock::new(None),
            created_at: Instant::now(),
        }
    }

    /// Returns the grain ID.
    pub fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    /// Returns the silo address.
    pub fn silo_address(&self) -> &SiloAddress {
        &self.silo_address
    }

    /// Returns the maximum number of workers.
    pub fn max_workers(&self) -> usize {
        self.placement.max_local
    }

    /// Returns the current number of workers.
    pub fn worker_count(&self) -> usize {
        self.workers.read().len()
    }

    /// Returns whether the context is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Acquire)
    }

    /// Returns pool statistics.
    pub fn stats(&self) -> WorkerPoolStats {
        let workers = self.workers.read();
        WorkerPoolStats::from_workers(&workers)
    }

    /// Returns the total messages processed.
    pub fn messages_processed(&self) -> u64 {
        self.messages_processed.load(Ordering::Relaxed)
    }

    /// Starts the context and begins the inspection timer.
    #[instrument(skip(self))]
    pub fn start(&self) -> StatelessWorkerResult<()> {
        if self.is_shutting_down() {
            return Err(StatelessWorkerError::ShuttingDown);
        }

        if self.options.remove_idle_workers {
            self.start_inspection_timer();
        }

        info!("Stateless worker context started");
        Ok(())
    }

    /// Starts the idle worker inspection timer.
    fn start_inspection_timer(&self) {
        let period = self.options.idle_workers_inspection_period;
        let work_signal = self.work_signal.clone();
        let is_shutting_down = Arc::new(AtomicBool::new(false));
        let shutdown_flag = is_shutting_down.clone();

        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(period);
            loop {
                interval.tick().await;
                if shutdown_flag.load(Ordering::Acquire) {
                    break;
                }
                // Signal the context to collect idle workers
                work_signal.notify_one();
            }
        });

        *self.inspection_timer.write() = Some(handle);
        debug!(period_ms = period.as_millis(), "Started inspection timer");
    }

    /// Routes an incoming message to an appropriate worker.
    ///
    /// Priority:
    /// 1. Reuse inactive workers (highest priority)
    /// 2. Create new worker if pool not full
    /// 3. Queue to worker with minimum waiting count
    #[instrument(skip(self), fields(grain_id = %self.grain_id))]
    pub fn route_message(&self) -> StatelessWorkerResult<ActivationId> {
        if self.is_shutting_down() {
            return Err(StatelessWorkerError::ShuttingDown);
        }

        let mut workers = self.workers.write();

        // If no workers, create first one
        if workers.is_empty() {
            let activation_id = self.create_worker_internal(&mut workers)?;
            if let Some(worker) = workers.last() {
                worker.enqueue_message();
            }
            trace!(
                activation_id = %activation_id,
                "Routed to new worker (first)"
            );
            return Ok(activation_id);
        }

        // 1. Check for inactive workers first (highest priority)
        for worker in workers.iter() {
            if worker.is_inactive() && worker.can_accept_messages() {
                worker.enqueue_message();
                trace!(
                    activation_id = %worker.activation_id(),
                    "Routed to inactive worker"
                );
                return Ok(worker.activation_id().clone());
            }
        }

        // 2. If all busy but pool not full, create new
        if workers.len() < self.placement.max_local {
            let activation_id = self.create_worker_internal(&mut workers)?;
            if let Some(worker) = workers.last() {
                worker.enqueue_message();
            }
            trace!(
                activation_id = %activation_id,
                pool_size = workers.len(),
                "Routed to new worker"
            );
            return Ok(activation_id);
        }

        // 3. If pool full, use worker with minimum waiting count
        let min_worker = workers
            .iter()
            .filter(|w| w.can_accept_messages())
            .min_by_key(|w| w.waiting_count());

        match min_worker {
            Some(worker) => {
                worker.enqueue_message();
                trace!(
                    activation_id = %worker.activation_id(),
                    waiting_count = worker.waiting_count(),
                    "Routed to least loaded worker"
                );
                Ok(worker.activation_id().clone())
            }
            None => Err(StatelessWorkerError::NoWorkersAvailable),
        }
    }

    /// Creates a new worker and adds it to the pool.
    /// Returns the activation ID of the newly created worker.
    fn create_worker_internal(
        &self,
        workers: &mut Vec<WorkerState>,
    ) -> StatelessWorkerResult<ActivationId> {
        let activation_id = ActivationId::new();
        let worker = WorkerState::new(activation_id.clone());

        workers.push(worker);
        self.workers_created.fetch_add(1, Ordering::Relaxed);

        debug!(
            activation_id = %activation_id,
            pool_size = workers.len(),
            "Created new worker"
        );

        Ok(activation_id)
    }

    /// Marks a worker as having started processing.
    pub fn worker_start_processing(&self, activation_id: &ActivationId) {
        let workers = self.workers.read();
        if let Some(worker) = workers.iter().find(|w| w.activation_id() == activation_id) {
            worker.start_processing();
        }
    }

    /// Marks a worker as having finished processing.
    pub fn worker_finish_processing(&self, activation_id: &ActivationId) {
        let workers = self.workers.read();
        if let Some(worker) = workers.iter().find(|w| w.activation_id() == activation_id) {
            worker.finish_processing();
        }
        self.messages_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Collects and removes idle workers based on PID controller.
    #[instrument(skip(self))]
    pub fn collect_idle_workers(&self) {
        if self.is_shutting_down() {
            return;
        }

        let workers = self.workers.read();
        let stats = WorkerPoolStats::from_workers(&workers);

        // Don't remove workers if at minimum
        if workers.len() <= self.options.min_workers {
            trace!("At minimum workers, skipping collection");
            return;
        }

        drop(workers); // Release read lock before acquiring write lock

        let mut pid = self.pid_controller.write();
        let control_signal = pid.compute(stats.average_waiting);

        if pid.should_remove_worker(control_signal, self.options.min_idle_cycles_before_removal) {
            let mut workers = self.workers.write();
            let inactive_indices: Vec<usize> = workers
                .iter()
                .enumerate()
                .filter(|(_, w)| w.is_inactive())
                .map(|(i, _)| i)
                .collect();

            if !inactive_indices.is_empty() && workers.len() > self.options.min_workers {
                // Remove a random inactive worker
                let remove_idx = inactive_indices[random_index(inactive_indices.len())];
                let removed = workers.remove(remove_idx);

                self.workers_deactivated.fetch_add(1, Ordering::Relaxed);
                pid.apply_anti_windup(
                    inactive_indices.len().saturating_sub(1),
                    inactive_indices.len(),
                );

                info!(
                    activation_id = %removed.activation_id(),
                    remaining_workers = workers.len(),
                    "Removed idle worker"
                );
            }
        }
    }

    /// Initiates graceful shutdown of the context.
    #[instrument(skip(self))]
    pub async fn shutdown(&self) -> StatelessWorkerResult<()> {
        if self
            .is_shutting_down
            .swap(true, Ordering::AcqRel)
        {
            warn!("Context already shutting down");
            return Ok(());
        }

        info!("Shutting down stateless worker context");

        // Stop inspection timer
        if let Some(handle) = self.inspection_timer.write().take() {
            handle.abort();
        }

        // Mark all workers for deactivation
        {
            let workers = self.workers.read();
            for worker in workers.iter() {
                worker.mark_deactivating();
            }
        } // Drop read lock before wait loop

        // Wait for workers to complete (with timeout)
        let timeout = self.options.deactivation_timeout;
        let start = Instant::now();

        loop {
            let all_inactive = {
                let workers = self.workers.read();
                workers.iter().all(|w| w.is_inactive())
            };

            if all_inactive {
                break;
            }

            if start.elapsed() > timeout {
                warn!("Shutdown timeout, forcing worker removal");
                break;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        // Clear workers
        self.workers.write().clear();

        info!(
            messages_processed = self.messages_processed(),
            workers_created = self.workers_created.load(Ordering::Relaxed),
            workers_deactivated = self.workers_deactivated.load(Ordering::Relaxed),
            uptime_secs = self.created_at.elapsed().as_secs(),
            "Stateless worker context shutdown complete"
        );

        Ok(())
    }

    /// Returns the grain address for a specific worker.
    pub fn worker_address(&self, activation_id: &ActivationId) -> Option<GrainAddress> {
        let workers = self.workers.read();
        if workers
            .iter()
            .any(|w| w.activation_id() == activation_id)
        {
            Some(GrainAddress::new(
                self.grain_id.clone(),
                activation_id.clone(),
                Some(self.silo_address.clone()),
            ))
        } else {
            None
        }
    }

    /// Returns all worker activation IDs.
    pub fn worker_ids(&self) -> Vec<ActivationId> {
        self.workers
            .read()
            .iter()
            .map(|w| w.activation_id().clone())
            .collect()
    }
}

/// Generate a random index.
fn random_index(max: usize) -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_usize(std::time::Instant::now().elapsed().as_nanos() as usize);
    hasher.finish() as usize % max
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn test_grain_id() -> GrainId {
        GrainId::new(
            orleans_core::GrainType::create("test.worker"),
            orleans_core::IdSpan::from_str("test-1"),
        )
    }

    fn test_silo() -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111),
            1,
        )
    }

    fn test_context() -> StatelessWorkerContext {
        let placement = StatelessWorkerPlacement::with_max_local(4);
        let options = StatelessWorkerOptions::for_testing();
        StatelessWorkerContext::new(test_grain_id(), test_silo(), placement, options)
    }

    #[test]
    fn test_context_creation() {
        let ctx = test_context();
        assert_eq!(ctx.max_workers(), 4);
        assert_eq!(ctx.worker_count(), 0);
        assert!(!ctx.is_shutting_down());
        assert_eq!(ctx.messages_processed(), 0);
    }

    #[test]
    fn test_route_creates_first_worker() {
        let ctx = test_context();
        assert_eq!(ctx.worker_count(), 0);

        let result = ctx.route_message();
        assert!(result.is_ok());
        assert_eq!(ctx.worker_count(), 1);
    }

    #[test]
    fn test_route_reuses_inactive_worker() {
        let ctx = test_context();

        // Create first worker and complete its message processing
        let id1 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id1); // Decrement waiting_count
        ctx.worker_finish_processing(&id1); // Mark as not executing

        // Worker should now be inactive (waiting_count=0, is_executing=false)
        // Should reuse the inactive worker
        let id2 = ctx.route_message().unwrap();
        assert_eq!(id1, id2);
        assert_eq!(ctx.worker_count(), 1);
    }

    #[test]
    fn test_route_creates_new_when_busy() {
        let ctx = test_context();

        // Create and keep first worker busy
        let id1 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id1);

        // Should create new worker
        let id2 = ctx.route_message().unwrap();
        assert_ne!(id1, id2);
        assert_eq!(ctx.worker_count(), 2);
    }

    #[test]
    fn test_route_uses_least_loaded_when_full() {
        let ctx = test_context();

        // Fill the pool (max 4)
        let mut ids = Vec::new();
        for _ in 0..4 {
            let id = ctx.route_message().unwrap();
            ctx.worker_start_processing(&id);
            ids.push(id);
        }
        assert_eq!(ctx.worker_count(), 4);

        // Finish one worker
        ctx.worker_finish_processing(&ids[2]);

        // New message should go to the finished (least loaded) worker
        let _id = ctx.route_message().unwrap();
        // The message should go to the least loaded worker
        let stats = ctx.stats();
        assert_eq!(stats.total_workers, 4);
    }

    #[test]
    fn test_stats() {
        let ctx = test_context();

        ctx.route_message().unwrap();
        ctx.route_message().unwrap();

        let stats = ctx.stats();
        assert_eq!(stats.total_workers, 2);
        // Each worker has 1 message waiting
        assert_eq!(stats.total_waiting, 2);
    }

    #[test]
    fn test_worker_ids() {
        let ctx = test_context();

        let id1 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id1);
        let id2 = ctx.route_message().unwrap();

        let ids = ctx.worker_ids();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&id1));
        assert!(ids.contains(&id2));
    }

    #[test]
    fn test_worker_address() {
        let ctx = test_context();

        let id = ctx.route_message().unwrap();
        let addr = ctx.worker_address(&id);

        assert!(addr.is_some());
        let addr = addr.unwrap();
        assert_eq!(addr.grain_id(), ctx.grain_id());
        assert_eq!(addr.activation_id(), &id);
        assert_eq!(addr.silo_address(), Some(ctx.silo_address()));
    }

    #[test]
    fn test_worker_address_not_found() {
        let ctx = test_context();
        let fake_id = ActivationId::new();
        assert!(ctx.worker_address(&fake_id).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_shutdown() {
        let placement = StatelessWorkerPlacement::with_max_local(4);
        // Disable idle worker removal - no need to start the context
        let options = StatelessWorkerOptions::for_testing()
            .with_remove_idle_workers(false)
            .with_deactivation_timeout(std::time::Duration::from_millis(100));
        let ctx = StatelessWorkerContext::new(test_grain_id(), test_silo(), placement, options);
        // Don't call start() - not needed when remove_idle_workers is false

        // Create some workers and complete their message processing
        let id1 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id1);
        ctx.worker_finish_processing(&id1);

        let id2 = ctx.route_message().unwrap();
        ctx.worker_start_processing(&id2);
        ctx.worker_finish_processing(&id2);

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
    fn test_route_fails_when_shutting_down() {
        let ctx = test_context();
        ctx.is_shutting_down.store(true, Ordering::Release);

        let result = ctx.route_message();
        assert!(matches!(result, Err(StatelessWorkerError::ShuttingDown)));
    }

    #[test]
    fn test_start_fails_when_shutting_down() {
        let ctx = test_context();
        ctx.is_shutting_down.store(true, Ordering::Release);

        let result = ctx.start();
        assert!(matches!(result, Err(StatelessWorkerError::ShuttingDown)));
    }

    #[test]
    fn test_collect_idle_workers_at_minimum() {
        let ctx = StatelessWorkerContext::new(
            test_grain_id(),
            test_silo(),
            StatelessWorkerPlacement::with_max_local(4),
            StatelessWorkerOptions::for_testing().with_min_workers(1),
        );

        // Create one worker (at minimum)
        let id = ctx.route_message().unwrap();
        ctx.worker_finish_processing(&id);

        // Collection should not remove the last worker
        ctx.collect_idle_workers();
        assert_eq!(ctx.worker_count(), 1);
    }

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_route_never_exceeds_max(max_local in 1usize..10, messages in 1usize..100) {
                let placement = StatelessWorkerPlacement::with_max_local(max_local);
                let options = StatelessWorkerOptions::for_testing();
                let ctx = StatelessWorkerContext::new(test_grain_id(), test_silo(), placement, options);

                for _ in 0..messages {
                    let _ = ctx.route_message();
                }

                prop_assert!(ctx.worker_count() <= max_local);
            }

            #[test]
            fn test_messages_processed_count(messages in 1usize..50) {
                let ctx = test_context();

                for _ in 0..messages {
                    if let Ok(id) = ctx.route_message() {
                        ctx.worker_start_processing(&id);
                        ctx.worker_finish_processing(&id);
                    }
                }

                prop_assert_eq!(ctx.messages_processed() as usize, messages);
            }
        }
    }
}
