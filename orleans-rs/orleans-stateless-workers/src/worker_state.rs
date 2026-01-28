//! Worker state tracking for stateless workers.

use orleans_core::ActivationId;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Instant;

/// State of an individual worker in a stateless worker pool.
#[derive(Debug)]
pub struct WorkerState {
    /// Unique identifier for this worker.
    activation_id: ActivationId,

    /// Number of messages waiting to be processed by this worker.
    waiting_count: AtomicU64,

    /// Whether the worker is currently executing a message.
    is_executing: AtomicBool,

    /// Time of the last activity (message received or completed).
    last_activity: parking_lot::Mutex<Instant>,

    /// Whether the worker has been marked for deactivation.
    is_deactivating: AtomicBool,
}

impl WorkerState {
    /// Creates a new worker state with the given activation ID.
    pub fn new(activation_id: ActivationId) -> Self {
        Self {
            activation_id,
            waiting_count: AtomicU64::new(0),
            is_executing: AtomicBool::new(false),
            last_activity: parking_lot::Mutex::new(Instant::now()),
            is_deactivating: AtomicBool::new(false),
        }
    }

    /// Returns the worker's activation ID.
    pub fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    /// Returns the current number of waiting messages.
    pub fn waiting_count(&self) -> u64 {
        self.waiting_count.load(Ordering::Acquire)
    }

    /// Increments the waiting count when a message is enqueued.
    pub fn enqueue_message(&self) {
        self.waiting_count.fetch_add(1, Ordering::Release);
        *self.last_activity.lock() = Instant::now();
    }

    /// Decrements the waiting count when a message starts processing.
    /// Also marks the worker as executing.
    pub fn start_processing(&self) {
        self.waiting_count.fetch_sub(1, Ordering::Release);
        self.is_executing.store(true, Ordering::Release);
        *self.last_activity.lock() = Instant::now();
    }

    /// Marks the worker as no longer executing.
    pub fn finish_processing(&self) {
        self.is_executing.store(false, Ordering::Release);
        *self.last_activity.lock() = Instant::now();
    }

    /// Returns whether the worker is currently executing a message.
    pub fn is_executing(&self) -> bool {
        self.is_executing.load(Ordering::Acquire)
    }

    /// Returns whether the worker is inactive (not executing and no waiting messages).
    pub fn is_inactive(&self) -> bool {
        !self.is_executing() && self.waiting_count() == 0
    }

    /// Returns the time of the last activity.
    pub fn last_activity(&self) -> Instant {
        *self.last_activity.lock()
    }

    /// Marks the worker for deactivation.
    pub fn mark_deactivating(&self) {
        self.is_deactivating.store(true, Ordering::Release);
    }

    /// Returns whether the worker is marked for deactivation.
    pub fn is_deactivating(&self) -> bool {
        self.is_deactivating.load(Ordering::Acquire)
    }

    /// Returns whether the worker can accept new messages.
    pub fn can_accept_messages(&self) -> bool {
        !self.is_deactivating()
    }
}

impl Clone for WorkerState {
    fn clone(&self) -> Self {
        Self {
            activation_id: self.activation_id.clone(),
            waiting_count: AtomicU64::new(self.waiting_count()),
            is_executing: AtomicBool::new(self.is_executing()),
            last_activity: parking_lot::Mutex::new(self.last_activity()),
            is_deactivating: AtomicBool::new(self.is_deactivating()),
        }
    }
}

/// Summary statistics for a worker pool.
#[derive(Debug, Clone, Default)]
pub struct WorkerPoolStats {
    /// Total number of workers in the pool.
    pub total_workers: usize,

    /// Number of active (executing) workers.
    pub active_workers: usize,

    /// Number of inactive (idle) workers.
    pub inactive_workers: usize,

    /// Total number of waiting messages across all workers.
    pub total_waiting: u64,

    /// Average waiting count per worker.
    pub average_waiting: f64,

    /// Maximum waiting count among all workers.
    pub max_waiting: u64,

    /// Minimum waiting count among all workers.
    pub min_waiting: u64,
}

impl WorkerPoolStats {
    /// Computes statistics from a collection of worker states.
    pub fn from_workers(workers: &[WorkerState]) -> Self {
        if workers.is_empty() {
            return Self::default();
        }

        let mut total_waiting = 0u64;
        let mut active_workers = 0usize;
        let mut inactive_workers = 0usize;
        let mut max_waiting = 0u64;
        let mut min_waiting = u64::MAX;

        for worker in workers {
            let waiting = worker.waiting_count();
            total_waiting += waiting;

            if worker.is_inactive() {
                inactive_workers += 1;
            } else {
                active_workers += 1;
            }

            if waiting > max_waiting {
                max_waiting = waiting;
            }
            if waiting < min_waiting {
                min_waiting = waiting;
            }
        }

        let average_waiting = if workers.is_empty() {
            0.0
        } else {
            total_waiting as f64 / workers.len() as f64
        };

        Self {
            total_workers: workers.len(),
            active_workers,
            inactive_workers,
            total_waiting,
            average_waiting,
            max_waiting,
            min_waiting: if workers.is_empty() { 0 } else { min_waiting },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_activation_id() -> ActivationId {
        ActivationId::new()
    }

    #[test]
    fn test_new_worker_state() {
        let state = WorkerState::new(test_activation_id());
        assert_eq!(state.waiting_count(), 0);
        assert!(!state.is_executing());
        assert!(state.is_inactive());
        assert!(!state.is_deactivating());
        assert!(state.can_accept_messages());
    }

    #[test]
    fn test_enqueue_message() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        assert_eq!(state.waiting_count(), 1);
        assert!(!state.is_inactive());
    }

    #[test]
    fn test_start_processing() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        state.start_processing();
        assert_eq!(state.waiting_count(), 0);
        assert!(state.is_executing());
        assert!(!state.is_inactive());
    }

    #[test]
    fn test_finish_processing() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        state.start_processing();
        state.finish_processing();
        assert!(!state.is_executing());
        assert!(state.is_inactive());
    }

    #[test]
    fn test_multiple_messages() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        state.enqueue_message();
        state.enqueue_message();
        assert_eq!(state.waiting_count(), 3);

        state.start_processing();
        assert_eq!(state.waiting_count(), 2);
        state.finish_processing();

        state.start_processing();
        assert_eq!(state.waiting_count(), 1);
    }

    #[test]
    fn test_mark_deactivating() {
        let state = WorkerState::new(test_activation_id());
        assert!(state.can_accept_messages());

        state.mark_deactivating();
        assert!(state.is_deactivating());
        assert!(!state.can_accept_messages());
    }

    #[test]
    fn test_worker_state_clone() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        state.start_processing();

        let cloned = state.clone();
        assert_eq!(cloned.waiting_count(), state.waiting_count());
        assert_eq!(cloned.is_executing(), state.is_executing());
    }

    #[test]
    fn test_pool_stats_empty() {
        let stats = WorkerPoolStats::from_workers(&[]);
        assert_eq!(stats.total_workers, 0);
        assert_eq!(stats.total_waiting, 0);
        assert!((stats.average_waiting - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_pool_stats_single_worker() {
        let state = WorkerState::new(test_activation_id());
        state.enqueue_message();
        state.enqueue_message();

        let stats = WorkerPoolStats::from_workers(&[state]);
        assert_eq!(stats.total_workers, 1);
        assert_eq!(stats.total_waiting, 2);
        assert!((stats.average_waiting - 2.0).abs() < f64::EPSILON);
        assert_eq!(stats.max_waiting, 2);
        assert_eq!(stats.min_waiting, 2);
    }

    #[test]
    fn test_pool_stats_multiple_workers() {
        let state1 = WorkerState::new(test_activation_id());
        state1.enqueue_message();

        let state2 = WorkerState::new(test_activation_id());
        state2.enqueue_message();
        state2.enqueue_message();
        state2.enqueue_message();

        let state3 = WorkerState::new(test_activation_id());
        // Inactive worker

        let stats = WorkerPoolStats::from_workers(&[state1, state2, state3]);
        assert_eq!(stats.total_workers, 3);
        assert_eq!(stats.total_waiting, 4);
        assert!((stats.average_waiting - 4.0 / 3.0).abs() < 0.001);
        assert_eq!(stats.max_waiting, 3);
        assert_eq!(stats.min_waiting, 0);
        assert_eq!(stats.inactive_workers, 1);
        assert_eq!(stats.active_workers, 2);
    }

    #[test]
    fn test_pool_stats_active_vs_inactive() {
        let state1 = WorkerState::new(test_activation_id());
        state1.enqueue_message();
        state1.start_processing(); // executing

        let state2 = WorkerState::new(test_activation_id());
        // inactive

        let stats = WorkerPoolStats::from_workers(&[state1, state2]);
        assert_eq!(stats.active_workers, 1);
        assert_eq!(stats.inactive_workers, 1);
    }

    #[test]
    fn test_last_activity_updates() {
        let state = WorkerState::new(test_activation_id());
        let initial = state.last_activity();

        std::thread::sleep(std::time::Duration::from_millis(10));
        state.enqueue_message();
        let after_enqueue = state.last_activity();

        assert!(after_enqueue > initial);

        std::thread::sleep(std::time::Duration::from_millis(10));
        state.start_processing();
        let after_start = state.last_activity();

        assert!(after_start > after_enqueue);
    }
}
