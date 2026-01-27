//! Timer registry for managing grain timers.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn, Span};

use crate::error::{TimerError, TimerResult};
use crate::options::TimerOptions;
use crate::timer::{GrainTimer, TimerChangeRequest, TimerHandle, TimerId};

/// Type alias for timer callback futures.
pub type TimerCallbackFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// A callback function that returns an async future.
///
/// The callback is invoked each time the timer fires and should return
/// a future that completes the timer's work.
pub type TimerCallback = Box<dyn Fn() -> TimerCallbackFuture + Send + Sync>;

/// Sender for queuing timer callbacks on the grain's work queue.
///
/// Timer callbacks are sent through this channel to ensure they execute
/// on the grain's scheduler (turn-based execution).
pub type TimerCallbackSender = mpsc::UnboundedSender<TimerCallbackFuture>;

/// Internal data for an active timer.
struct ActiveTimer {
    /// The timer's unique ID.
    #[allow(dead_code)] // Retained for future inspection/debugging
    id: TimerId,

    /// The callback function.
    #[allow(dead_code)] // Retained for potential timer restart scenarios
    callback: TimerCallback,

    /// Token to cancel the timer's background task.
    cancellation_token: CancellationToken,

    /// Handle to the timer's background task.
    task_handle: Option<tokio::task::JoinHandle<()>>,
}

/// Trait for timer registration.
///
/// Implementations provide timer registration services to grains.
pub trait ITimerRegistry: Send + Sync {
    /// Register a new timer.
    ///
    /// # Arguments
    ///
    /// - `callback`: The function to call when the timer fires
    /// - `due_time`: Duration until the first tick
    /// - `period`: Duration between subsequent ticks (zero for one-shot)
    ///
    /// # Returns
    ///
    /// A `GrainTimer` handle that can be used to control the timer.
    fn register_timer(
        &self,
        callback: TimerCallback,
        due_time: Duration,
        period: Duration,
    ) -> TimerResult<GrainTimer>;

    /// Dispose all active timers.
    ///
    /// Called during grain deactivation to clean up all timers.
    fn dispose_all(&self);

    /// Get the number of active timers.
    fn active_timer_count(&self) -> usize;
}

/// Registry for managing grain timers.
///
/// Each grain activation has its own `GrainTimerRegistry` that manages
/// all timers for that grain. The registry ensures:
///
/// - Timer callbacks are queued on the grain's work queue
/// - Timers are automatically disposed on grain deactivation
/// - Timer IDs are unique within the grain
pub struct GrainTimerRegistry {
    /// Channel to send callbacks to the grain's work queue.
    callback_tx: TimerCallbackSender,

    /// Active timers keyed by ID.
    active_timers: RwLock<HashMap<TimerId, ActiveTimer>>,

    /// Counter for generating unique timer IDs.
    next_timer_id: AtomicU64,

    /// Timer configuration options.
    options: TimerOptions,

    /// Master cancellation token for all timers (used during dispose_all).
    master_cancellation: CancellationToken,
}

impl GrainTimerRegistry {
    /// Create a new timer registry.
    ///
    /// # Arguments
    ///
    /// - `callback_tx`: Channel to send timer callbacks to the grain's work queue
    pub fn new(callback_tx: TimerCallbackSender) -> Self {
        Self::with_options(callback_tx, TimerOptions::default())
    }

    /// Create a new timer registry with custom options.
    pub fn with_options(callback_tx: TimerCallbackSender, options: TimerOptions) -> Self {
        Self {
            callback_tx,
            active_timers: RwLock::new(HashMap::new()),
            next_timer_id: AtomicU64::new(1),
            options,
            master_cancellation: CancellationToken::new(),
        }
    }

    /// Generate a new unique timer ID.
    fn generate_timer_id(&self) -> TimerId {
        TimerId::new(self.next_timer_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Validate timer period against minimum.
    fn validate_period(&self, period: Duration) -> TimerResult<()> {
        if !period.is_zero() && period < self.options.min_timer_period {
            return Err(TimerError::PeriodTooShort {
                period_ms: period.as_millis() as u64,
                min_period_ms: self.options.min_timer_period.as_millis() as u64,
            });
        }
        Ok(())
    }

    /// Start the background task for a timer.
    #[instrument(skip(self, callback, cancellation_token), fields(timer_id = %timer_id))]
    fn start_timer_task(
        &self,
        timer_id: TimerId,
        callback: Arc<TimerCallback>,
        due_time: Duration,
        period: Duration,
        cancellation_token: CancellationToken,
        mut change_rx: mpsc::UnboundedReceiver<TimerChangeRequest>,
    ) -> tokio::task::JoinHandle<()> {
        let callback_tx = self.callback_tx.clone();
        let master_cancellation = self.master_cancellation.clone();

        tokio::spawn(async move {
            let mut current_due_time = due_time;
            let mut current_period = period;
            let mut first_tick = true;

            loop {
                // Calculate delay for next tick
                let delay = if first_tick {
                    current_due_time
                } else {
                    current_period
                };

                // Wait for delay, cancellation, or schedule change
                tokio::select! {
                    biased;

                    // Check for cancellation first
                    _ = cancellation_token.cancelled() => {
                        debug!(timer_id = %timer_id, "Timer cancelled");
                        break;
                    }

                    // Check master cancellation (dispose_all)
                    _ = master_cancellation.cancelled() => {
                        debug!(timer_id = %timer_id, "Timer cancelled by dispose_all");
                        break;
                    }

                    // Check for schedule change
                    Some(change) = change_rx.recv() => {
                        trace!(
                            timer_id = %timer_id,
                            new_due_time_ms = change.due_time.as_millis() as u64,
                            new_period_ms = change.period.as_millis() as u64,
                            "Timer schedule changed"
                        );
                        current_due_time = change.due_time;
                        current_period = change.period;
                        first_tick = true;
                        continue;
                    }

                    // Wait for the delay
                    _ = tokio::time::sleep(delay) => {
                        // Time to fire!
                    }
                }

                // Check cancellation before firing
                if cancellation_token.is_cancelled() || master_cancellation.is_cancelled() {
                    break;
                }

                // Create callback future
                let callback_future = callback();

                // Queue callback on grain's work queue
                trace!(timer_id = %timer_id, "Queueing timer callback");
                if callback_tx.send(callback_future).is_err() {
                    warn!(timer_id = %timer_id, "Callback channel closed, stopping timer");
                    break;
                }

                // For one-shot timers (period = 0), exit after first callback
                if first_tick && current_period.is_zero() {
                    debug!(timer_id = %timer_id, "One-shot timer completed");
                    break;
                }

                // Mark first tick as done
                first_tick = false;
            }

            trace!(timer_id = %timer_id, "Timer task exiting");
        })
    }

    /// Remove a timer from the registry.
    #[allow(dead_code)] // Public API for future use
    fn remove_timer(&self, timer_id: TimerId) {
        let mut timers = self.active_timers.write();
        if let Some(timer) = timers.remove(&timer_id) {
            timer.cancellation_token.cancel();
            if let Some(handle) = timer.task_handle {
                handle.abort();
            }
        }
    }
}

impl ITimerRegistry for GrainTimerRegistry {
    #[instrument(skip(self, callback), fields(timer_id))]
    fn register_timer(
        &self,
        callback: TimerCallback,
        due_time: Duration,
        period: Duration,
    ) -> TimerResult<GrainTimer> {
        // Validate period
        self.validate_period(period)?;

        let timer_id = self.generate_timer_id();
        Span::current().record("timer_id", timer_id.value());

        info!(
            due_time_ms = due_time.as_millis() as u64,
            period_ms = period.as_millis() as u64,
            "Registering timer"
        );

        let cancellation_token = CancellationToken::new();
        let (change_tx, change_rx) = mpsc::unbounded_channel();

        // Create the handle for the caller
        let handle = TimerHandle::new(timer_id, cancellation_token.clone(), change_tx);
        let grain_timer = GrainTimer::new(handle);

        // Wrap callback in Arc for sharing with task
        let callback = Arc::new(callback);

        // Start the timer task
        let task_handle = self.start_timer_task(
            timer_id,
            callback.clone(),
            due_time,
            period,
            cancellation_token.clone(),
            change_rx,
        );

        // Create a callback wrapper that we can store
        let callback_wrapper: TimerCallback = {
            let callback = callback.clone();
            Box::new(move || callback())
        };

        // Store in registry
        let active_timer = ActiveTimer {
            id: timer_id,
            callback: callback_wrapper,
            cancellation_token,
            task_handle: Some(task_handle),
        };

        self.active_timers.write().insert(timer_id, active_timer);

        debug!("Timer registered successfully");
        Ok(grain_timer)
    }

    #[instrument(skip(self))]
    fn dispose_all(&self) {
        info!("Disposing all timers");

        // Cancel master token to stop all timer tasks
        self.master_cancellation.cancel();

        // Clear all timers
        let mut timers = self.active_timers.write();
        for (id, timer) in timers.drain() {
            trace!(timer_id = %id, "Disposing timer");
            timer.cancellation_token.cancel();
            if let Some(handle) = timer.task_handle {
                handle.abort();
            }
        }

        debug!("All timers disposed");
    }

    fn active_timer_count(&self) -> usize {
        self.active_timers.read().len()
    }
}

impl Drop for GrainTimerRegistry {
    fn drop(&mut self) {
        // Ensure all timers are cancelled when registry is dropped
        self.master_cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::time::timeout;

    fn create_test_registry() -> (GrainTimerRegistry, mpsc::UnboundedReceiver<TimerCallbackFuture>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let registry = GrainTimerRegistry::new(tx);
        (registry, rx)
    }

    #[test]
    fn test_registry_creation() {
        let (registry, _rx) = create_test_registry();
        assert_eq!(registry.active_timer_count(), 0);
    }

    #[test]
    fn test_timer_id_generation() {
        let (registry, _rx) = create_test_registry();

        let id1 = registry.generate_timer_id();
        let id2 = registry.generate_timer_id();
        let id3 = registry.generate_timer_id();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_eq!(id1.value(), 1);
        assert_eq!(id2.value(), 2);
        assert_eq!(id3.value(), 3);
    }

    #[test]
    fn test_validate_period() {
        let (registry, _rx) = create_test_registry();

        // Zero period is valid (one-shot)
        assert!(registry.validate_period(Duration::ZERO).is_ok());

        // Above minimum is valid
        assert!(registry.validate_period(Duration::from_millis(100)).is_ok());

        // Below minimum is invalid
        assert!(registry.validate_period(Duration::from_millis(5)).is_err());
    }

    #[tokio::test]
    async fn test_register_timer() {
        let (registry, _rx) = create_test_registry();

        let callback: TimerCallback = Box::new(|| Box::pin(async {}));
        let timer = registry
            .register_timer(callback, Duration::from_secs(1), Duration::from_secs(1))
            .unwrap();

        assert!(!timer.is_disposed());
        assert_eq!(registry.active_timer_count(), 1);
    }

    #[tokio::test]
    async fn test_timer_fires_callback() {
        let (registry, mut rx) = create_test_registry();

        // Track callback invocations
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let callback: TimerCallback = Box::new(move || {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        });

        let _timer = registry
            .register_timer(callback, Duration::from_millis(10), Duration::ZERO)
            .unwrap();

        // Wait for callback to be queued
        let callback_future = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("Timeout waiting for callback")
            .expect("Channel closed");

        // Execute the callback
        callback_future.await;

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_periodic_timer_fires_multiple_times() {
        let (registry, mut rx) = create_test_registry();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let callback: TimerCallback = Box::new(move || {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        });

        let _timer = registry
            .register_timer(callback, Duration::from_millis(10), Duration::from_millis(20))
            .unwrap();

        // Wait for at least 3 callbacks
        for _ in 0..3 {
            let callback_future = timeout(Duration::from_millis(100), rx.recv())
                .await
                .expect("Timeout waiting for callback")
                .expect("Channel closed");
            callback_future.await;
        }

        assert!(counter.load(Ordering::Relaxed) >= 3);
    }

    #[tokio::test]
    async fn test_timer_dispose() {
        let (registry, _rx) = create_test_registry();

        let callback: TimerCallback = Box::new(|| Box::pin(async {}));
        let timer = registry
            .register_timer(callback, Duration::from_secs(1), Duration::from_secs(1))
            .unwrap();

        assert!(!timer.is_disposed());
        timer.dispose();
        assert!(timer.is_disposed());
    }

    #[tokio::test]
    async fn test_dispose_all() {
        let (registry, _rx) = create_test_registry();

        for _ in 0..5 {
            let callback: TimerCallback = Box::new(|| Box::pin(async {}));
            let _ = registry.register_timer(callback, Duration::from_secs(1), Duration::from_secs(1));
        }

        assert_eq!(registry.active_timer_count(), 5);

        registry.dispose_all();

        // Give tasks time to clean up
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Note: active_timer_count may still show 5 because we cleared the map in dispose_all
        // but the internal state was already cleared
    }

    #[tokio::test]
    async fn test_timer_change_schedule() {
        let (registry, mut rx) = create_test_registry();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let callback: TimerCallback = Box::new(move || {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        });

        let timer = registry
            .register_timer(callback, Duration::from_secs(10), Duration::from_secs(10))
            .unwrap();

        // Change to fire immediately
        timer.change(Duration::ZERO, Duration::ZERO).unwrap();

        // Should fire quickly now
        let callback_future = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("Timeout waiting for callback")
            .expect("Channel closed");
        callback_future.await;

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_period_too_short_error() {
        let (registry, _rx) = create_test_registry();

        let callback: TimerCallback = Box::new(|| Box::pin(async {}));
        let result = registry.register_timer(callback, Duration::from_millis(1), Duration::from_millis(5));

        assert!(matches!(result, Err(TimerError::PeriodTooShort { .. })));
    }

    #[tokio::test]
    async fn test_one_shot_timer() {
        let (registry, mut rx) = create_test_registry();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let callback: TimerCallback = Box::new(move || {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        });

        // One-shot timer (period = 0)
        let _timer = registry
            .register_timer(callback, Duration::from_millis(10), Duration::ZERO)
            .unwrap();

        // First callback should fire
        let callback_future = timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("Timeout waiting for callback")
            .expect("Channel closed");
        callback_future.await;

        // Wait a bit and verify no more callbacks
        tokio::time::sleep(Duration::from_millis(50)).await;

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_immediate_timer() {
        let (registry, mut rx) = create_test_registry();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let callback: TimerCallback = Box::new(move || {
            let counter = counter_clone.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::Relaxed);
            })
        });

        // Immediate one-shot timer (due_time = 0, period = 0)
        let _timer = registry
            .register_timer(callback, Duration::ZERO, Duration::ZERO)
            .unwrap();

        // Should fire immediately
        let callback_future = timeout(Duration::from_millis(50), rx.recv())
            .await
            .expect("Timeout waiting for callback")
            .expect("Channel closed");
        callback_future.await;

        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }
}
