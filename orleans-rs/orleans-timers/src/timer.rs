//! Timer types and handles.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace};

use crate::error::{TimerError, TimerResult};

/// Unique identifier for a timer within a grain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(u64);

impl TimerId {
    /// Create a new timer ID.
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw ID value.
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for TimerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Timer({})", self.0)
    }
}

/// Internal handle for managing a timer's lifecycle.
#[derive(Debug, Clone)]
pub struct TimerHandle {
    /// The timer's unique ID.
    id: TimerId,

    /// Whether the timer has been cancelled.
    cancelled: Arc<AtomicBool>,

    /// Token for cancelling the timer's background task.
    cancellation_token: CancellationToken,

    /// Channel to send schedule change requests.
    change_tx: tokio::sync::mpsc::UnboundedSender<TimerChangeRequest>,
}

/// Request to change a timer's schedule.
#[derive(Debug, Clone)]
pub struct TimerChangeRequest {
    /// New due time (time until next tick).
    pub due_time: Duration,

    /// New period (time between subsequent ticks).
    pub period: Duration,
}

impl TimerHandle {
    /// Create a new timer handle.
    pub fn new(
        id: TimerId,
        cancellation_token: CancellationToken,
        change_tx: tokio::sync::mpsc::UnboundedSender<TimerChangeRequest>,
    ) -> Self {
        Self {
            id,
            cancelled: Arc::new(AtomicBool::new(false)),
            cancellation_token,
            change_tx,
        }
    }

    /// Get the timer's ID.
    pub fn id(&self) -> TimerId {
        self.id
    }

    /// Check if the timer has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Cancel the timer.
    #[instrument(skip(self), fields(timer_id = %self.id))]
    pub fn cancel(&self) {
        if !self.cancelled.swap(true, Ordering::AcqRel) {
            debug!("Cancelling timer");
            self.cancellation_token.cancel();
        } else {
            trace!("Timer already cancelled");
        }
    }

    /// Change the timer's schedule.
    ///
    /// - `due_time`: Duration until the next tick
    /// - `period`: Duration between subsequent ticks (zero for one-shot)
    #[instrument(skip(self), fields(timer_id = %self.id))]
    pub fn change(&self, due_time: Duration, period: Duration) -> TimerResult<()> {
        if self.is_cancelled() {
            return Err(TimerError::AlreadyDisposed { timer_id: self.id });
        }

        debug!(
            due_time_ms = due_time.as_millis() as u64,
            period_ms = period.as_millis() as u64,
            "Changing timer schedule"
        );

        self.change_tx
            .send(TimerChangeRequest { due_time, period })
            .map_err(|_| TimerError::ChannelClosed)
    }

    /// Get the cancellation token for this timer.
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }
}

/// A grain timer handle returned to the grain.
///
/// This is the public interface for controlling a timer after registration.
/// Dropping this handle does NOT automatically dispose the timer - you must
/// explicitly call `dispose()` to stop the timer.
#[derive(Debug, Clone)]
pub struct GrainTimer {
    handle: TimerHandle,
}

impl GrainTimer {
    /// Create a new grain timer from a handle.
    pub fn new(handle: TimerHandle) -> Self {
        Self { handle }
    }

    /// Get the timer's unique ID.
    pub fn id(&self) -> TimerId {
        self.handle.id()
    }

    /// Check if the timer has been disposed.
    pub fn is_disposed(&self) -> bool {
        self.handle.is_cancelled()
    }

    /// Dispose (cancel) the timer.
    ///
    /// After disposal, the timer will not fire any more callbacks.
    /// This is idempotent - calling dispose multiple times is safe.
    #[instrument(skip(self), fields(timer_id = %self.handle.id()))]
    pub fn dispose(&self) {
        debug!("Disposing grain timer");
        self.handle.cancel();
    }

    /// Change the timer's schedule.
    ///
    /// # Arguments
    ///
    /// - `due_time`: Duration until the next tick. Use `Duration::ZERO` for
    ///   immediate execution.
    /// - `period`: Duration between subsequent ticks. Use `Duration::ZERO` for
    ///   a one-shot timer that only fires once.
    ///
    /// # Errors
    ///
    /// Returns `TimerError::AlreadyDisposed` if the timer has been disposed.
    #[instrument(skip(self), fields(timer_id = %self.handle.id()))]
    pub fn change(&self, due_time: Duration, period: Duration) -> TimerResult<()> {
        debug!(
            due_time_ms = due_time.as_millis() as u64,
            period_ms = period.as_millis() as u64,
            "Changing grain timer schedule"
        );
        self.handle.change(due_time, period)
    }

    /// Get access to the internal handle.
    pub fn handle(&self) -> &TimerHandle {
        &self.handle
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_handle() -> (TimerHandle, tokio::sync::mpsc::UnboundedReceiver<TimerChangeRequest>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = TimerHandle::new(
            TimerId::new(42),
            CancellationToken::new(),
            tx,
        );
        (handle, rx)
    }

    #[test]
    fn test_timer_id_display() {
        let id = TimerId::new(123);
        assert_eq!(format!("{}", id), "Timer(123)");
    }

    #[test]
    fn test_timer_id_equality() {
        let id1 = TimerId::new(42);
        let id2 = TimerId::new(42);
        let id3 = TimerId::new(99);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_timer_id_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(TimerId::new(1));
        set.insert(TimerId::new(2));
        set.insert(TimerId::new(1)); // duplicate

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_timer_handle_cancel() {
        let (handle, _rx) = create_test_handle();

        assert!(!handle.is_cancelled());
        handle.cancel();
        assert!(handle.is_cancelled());

        // Idempotent
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_timer_handle_change() {
        let (handle, mut rx) = create_test_handle();

        handle
            .change(Duration::from_secs(5), Duration::from_secs(10))
            .unwrap();

        let request = rx.try_recv().unwrap();
        assert_eq!(request.due_time, Duration::from_secs(5));
        assert_eq!(request.period, Duration::from_secs(10));
    }

    #[test]
    fn test_timer_handle_change_after_cancel() {
        let (handle, _rx) = create_test_handle();

        handle.cancel();
        let result = handle.change(Duration::from_secs(1), Duration::from_secs(1));

        assert!(matches!(result, Err(TimerError::AlreadyDisposed { .. })));
    }

    #[test]
    fn test_grain_timer_dispose() {
        let (handle, _rx) = create_test_handle();
        let timer = GrainTimer::new(handle);

        assert!(!timer.is_disposed());
        timer.dispose();
        assert!(timer.is_disposed());
    }

    #[test]
    fn test_grain_timer_change() {
        let (handle, mut rx) = create_test_handle();
        let timer = GrainTimer::new(handle);

        timer
            .change(Duration::from_millis(100), Duration::from_millis(500))
            .unwrap();

        let request = rx.try_recv().unwrap();
        assert_eq!(request.due_time, Duration::from_millis(100));
        assert_eq!(request.period, Duration::from_millis(500));
    }

    #[test]
    fn test_grain_timer_clone() {
        let (handle, _rx) = create_test_handle();
        let timer1 = GrainTimer::new(handle);
        let timer2 = timer1.clone();

        // Both point to same timer
        assert_eq!(timer1.id(), timer2.id());

        // Disposing one disposes both
        timer1.dispose();
        assert!(timer2.is_disposed());
    }
}
