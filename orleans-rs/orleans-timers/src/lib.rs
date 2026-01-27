//! Orleans Timers - In-memory grain-scoped scheduled callbacks.
//!
//! This crate provides timer support for Orleans grains:
//!
//! - **Timers**: Non-persistent, grain-scoped scheduled callbacks
//! - Timer callbacks are queued on the grain's work queue for turn-based execution
//! - Timers are automatically disposed when the grain deactivates
//!
//! # Timer Characteristics
//!
//! - In-memory only (lost on deactivation)
//! - Grain-scoped (tied to specific activation)
//! - No persistence across silo restarts
//! - High frequency capable (milliseconds)
//! - Stopped automatically on grain deactivation
//!
//! # Example
//!
//! ```ignore
//! use orleans_timers::{GrainTimerRegistry, TimerCallback};
//! use std::time::Duration;
//!
//! // Create a timer registry for the grain
//! let registry = GrainTimerRegistry::new(callback_sender);
//!
//! // Register a periodic timer
//! let timer = registry.register_timer(
//!     Duration::from_secs(5),   // due time (first tick after 5s)
//!     Duration::from_secs(10),  // period (every 10s thereafter)
//! );
//!
//! // Later, dispose the timer
//! timer.dispose();
//!
//! // Or change the timer's schedule
//! timer.change(Duration::from_secs(1), Duration::from_secs(5))?;
//! ```

mod error;
mod options;
mod registry;
mod timer;

pub use error::{TimerError, TimerResult};
pub use options::TimerOptions;
pub use registry::{GrainTimerRegistry, ITimerRegistry, TimerCallback, TimerCallbackSender};
pub use timer::{GrainTimer, TimerHandle, TimerId};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crate_compiles() {
        // Verify all public types are accessible
        let _ = std::any::type_name::<TimerId>();
        let _ = std::any::type_name::<GrainTimer>();
        let _ = std::any::type_name::<GrainTimerRegistry>();
        let _ = std::any::type_name::<TimerOptions>();
    }
}
