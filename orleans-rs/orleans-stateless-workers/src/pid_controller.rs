//! PID Controller for adaptive worker pool sizing.
//!
//! Uses a tuned PID algorithm (via genetic algorithm in original Orleans)
//! to dynamically adjust the worker pool based on load.

use tracing::{debug, trace};

/// PID controller constants tuned via genetic algorithm.
/// These values were optimized for Orleans stateless worker load balancing.
const KP: f64 = 0.433; // Proportional gain
const KI: f64 = 0.468; // Integral gain
const KD: f64 = 0.480; // Derivative gain

/// PID controller for managing worker pool size based on load metrics.
///
/// The controller uses the average waiting count across workers as the metric
/// and outputs a control signal indicating whether the pool should grow or shrink.
#[derive(Debug, Clone)]
pub struct PidController {
    /// Proportional gain.
    kp: f64,
    /// Integral gain.
    ki: f64,
    /// Derivative gain.
    kd: f64,
    /// Accumulated integral term.
    integral_term: f64,
    /// Previous error for derivative calculation.
    previous_error: f64,
    /// Number of consecutive idle cycles detected.
    detected_idle_cycles_count: u32,
    /// Anti-windup maximum for integral term.
    integral_max: f64,
}

impl Default for PidController {
    fn default() -> Self {
        Self::new()
    }
}

impl PidController {
    /// Creates a new PID controller with tuned constants.
    pub fn new() -> Self {
        Self {
            kp: KP,
            ki: KI,
            kd: KD,
            integral_term: 0.0,
            previous_error: 0.0,
            detected_idle_cycles_count: 0,
            integral_max: 100.0, // Reasonable anti-windup limit
        }
    }

    /// Creates a PID controller with custom gains.
    pub fn with_gains(kp: f64, ki: f64, kd: f64) -> Self {
        Self {
            kp,
            ki,
            kd,
            integral_term: 0.0,
            previous_error: 0.0,
            detected_idle_cycles_count: 0,
            integral_max: 100.0,
        }
    }

    /// Sets the anti-windup maximum for the integral term.
    pub fn with_integral_max(mut self, max: f64) -> Self {
        self.integral_max = max;
        self
    }

    /// Computes the control signal based on the current error.
    ///
    /// The error is calculated as: 0 (target waiting count) - average_waiting_count
    /// A negative error means there's work queued (need more workers).
    /// A positive error means workers are idle (can remove workers).
    ///
    /// # Arguments
    /// * `average_waiting_count` - The average number of waiting messages per worker.
    ///
    /// # Returns
    /// The control signal. Negative means excess capacity (can remove workers).
    pub fn compute(&mut self, average_waiting_count: f64) -> f64 {
        // Error from target (0 waiting count is the goal)
        let error = -average_waiting_count;

        // Update integral term with anti-windup
        self.integral_term += error;
        self.integral_term = self.integral_term.clamp(-self.integral_max, self.integral_max);

        // Calculate derivative term
        let derivative = error - self.previous_error;
        self.previous_error = error;

        // PID formula
        let control_signal = self.kp * error + self.ki * self.integral_term + self.kd * derivative;

        trace!(
            error = error,
            integral = self.integral_term,
            derivative = derivative,
            control_signal = control_signal,
            "PID controller computed"
        );

        control_signal
    }

    /// Updates the idle cycle detection based on the control signal.
    ///
    /// Returns true if an idle worker should be removed (after enough consecutive idle cycles).
    ///
    /// # Arguments
    /// * `control_signal` - The control signal from the last `compute()` call.
    /// * `min_idle_cycles` - Minimum consecutive idle cycles before removal.
    pub fn should_remove_worker(&mut self, control_signal: f64, min_idle_cycles: u32) -> bool {
        // Negative control signal indicates excess capacity
        if control_signal < 0.0 {
            self.detected_idle_cycles_count += 1;
            debug!(
                idle_cycles = self.detected_idle_cycles_count,
                min_required = min_idle_cycles,
                "Detected idle cycle"
            );
        } else {
            self.detected_idle_cycles_count = 0;
        }

        self.detected_idle_cycles_count >= min_idle_cycles
    }

    /// Applies anti-windup after removing a worker.
    ///
    /// This prevents the integral term from causing oscillations.
    ///
    /// # Arguments
    /// * `remaining_idle_workers` - Number of idle workers remaining after removal.
    /// * `previous_idle_workers` - Number of idle workers before removal.
    pub fn apply_anti_windup(&mut self, remaining_idle_workers: usize, previous_idle_workers: usize) {
        if previous_idle_workers > 0 {
            let factor = remaining_idle_workers as f64 / previous_idle_workers as f64;
            self.integral_term *= factor;
            self.detected_idle_cycles_count = 0;

            debug!(
                factor = factor,
                new_integral = self.integral_term,
                "Applied anti-windup"
            );
        }
    }

    /// Resets the controller state.
    pub fn reset(&mut self) {
        self.integral_term = 0.0;
        self.previous_error = 0.0;
        self.detected_idle_cycles_count = 0;
    }

    /// Returns the current number of detected idle cycles.
    pub fn idle_cycles_count(&self) -> u32 {
        self.detected_idle_cycles_count
    }

    /// Returns the current integral term value.
    pub fn integral_term(&self) -> f64 {
        self.integral_term
    }

    /// Returns the previous error value.
    pub fn previous_error(&self) -> f64 {
        self.previous_error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_controller() {
        let controller = PidController::new();
        assert!((controller.kp - KP).abs() < f64::EPSILON);
        assert!((controller.ki - KI).abs() < f64::EPSILON);
        assert!((controller.kd - KD).abs() < f64::EPSILON);
        assert_eq!(controller.integral_term, 0.0);
        assert_eq!(controller.previous_error, 0.0);
        assert_eq!(controller.detected_idle_cycles_count, 0);
    }

    #[test]
    fn test_with_custom_gains() {
        let controller = PidController::with_gains(1.0, 2.0, 3.0);
        assert!((controller.kp - 1.0).abs() < f64::EPSILON);
        assert!((controller.ki - 2.0).abs() < f64::EPSILON);
        assert!((controller.kd - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_compute_zero_waiting() {
        let mut controller = PidController::new();
        let signal = controller.compute(0.0);
        // With zero waiting, error is 0, so signal should be 0
        assert!((signal - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_compute_positive_waiting() {
        let mut controller = PidController::new();
        let signal = controller.compute(5.0);
        // With positive waiting, error is negative, so signal should be negative
        assert!(signal < 0.0);
    }

    #[test]
    fn test_compute_accumulates_integral() {
        let mut controller = PidController::new();

        // First computation
        controller.compute(5.0);
        let integral1 = controller.integral_term();

        // Second computation with same load
        controller.compute(5.0);
        let integral2 = controller.integral_term();

        // Integral should accumulate (become more negative)
        assert!(integral2 < integral1);
    }

    #[test]
    fn test_compute_derivative() {
        let mut controller = PidController::new();

        // First computation
        controller.compute(5.0);

        // Second computation with higher load (derivative effect)
        let signal1 = controller.compute(10.0);

        // Reset and try with decreasing load
        controller.reset();
        controller.compute(10.0);
        let signal2 = controller.compute(5.0);

        // With increasing load, signal should be more negative
        // With decreasing load, signal should be less negative
        assert!(signal1 < signal2);
    }

    #[test]
    fn test_should_remove_worker_after_cycles() {
        let mut controller = PidController::new();

        // Negative signal indicates excess capacity
        // Need min_idle_cycles consecutive cycles before removal
        assert!(!controller.should_remove_worker(-0.5, 2));
        assert_eq!(controller.idle_cycles_count(), 1);

        // After 2 consecutive cycles (count >= min), should return true
        assert!(controller.should_remove_worker(-0.5, 2));
        assert_eq!(controller.idle_cycles_count(), 2);

        // Continue to return true as long as signal stays negative
        assert!(controller.should_remove_worker(-0.5, 2));
        assert_eq!(controller.idle_cycles_count(), 3);
    }

    #[test]
    fn test_should_remove_resets_on_positive_signal() {
        let mut controller = PidController::new();

        // Build up idle cycles
        controller.should_remove_worker(-0.5, 2);
        controller.should_remove_worker(-0.5, 2);
        assert_eq!(controller.idle_cycles_count(), 2);

        // Positive signal resets count
        controller.should_remove_worker(0.5, 2);
        assert_eq!(controller.idle_cycles_count(), 0);
    }

    #[test]
    fn test_apply_anti_windup() {
        let mut controller = PidController::new();
        controller.integral_term = -10.0;
        controller.detected_idle_cycles_count = 5;

        // Remove 1 of 3 idle workers (2 remaining)
        controller.apply_anti_windup(2, 3);

        // Integral should be scaled by 2/3
        assert!((controller.integral_term - (-10.0 * 2.0 / 3.0)).abs() < 0.001);
        assert_eq!(controller.detected_idle_cycles_count, 0);
    }

    #[test]
    fn test_anti_windup_with_zero_previous() {
        let mut controller = PidController::new();
        controller.integral_term = -10.0;

        // Edge case: 0 previous workers (should not panic)
        controller.apply_anti_windup(0, 0);

        // Integral should remain unchanged
        assert!((controller.integral_term - (-10.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn test_reset() {
        let mut controller = PidController::new();
        controller.integral_term = -10.0;
        controller.previous_error = -5.0;
        controller.detected_idle_cycles_count = 3;

        controller.reset();

        assert_eq!(controller.integral_term, 0.0);
        assert_eq!(controller.previous_error, 0.0);
        assert_eq!(controller.detected_idle_cycles_count, 0);
    }

    #[test]
    fn test_integral_anti_windup_clamping() {
        let mut controller = PidController::new().with_integral_max(5.0);

        // Simulate many iterations with high load
        for _ in 0..100 {
            controller.compute(10.0);
        }

        // Integral should be clamped
        assert!(controller.integral_term >= -5.0);
    }

    #[test]
    fn test_accessors() {
        let mut controller = PidController::new();
        controller.compute(5.0);

        assert!(controller.integral_term() < 0.0);
        assert!(controller.previous_error() < 0.0);
        assert_eq!(controller.idle_cycles_count(), 0);
    }

    mod property_tests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_compute_is_deterministic(waiting in 0.0..100.0f64) {
                let mut controller1 = PidController::new();
                let mut controller2 = PidController::new();

                let signal1 = controller1.compute(waiting);
                let signal2 = controller2.compute(waiting);

                prop_assert!((signal1 - signal2).abs() < f64::EPSILON);
            }

            #[test]
            fn test_higher_load_produces_more_negative_signal(
                low in 0.0..50.0f64,
                high in 50.0..100.0f64
            ) {
                let mut controller1 = PidController::new();
                let mut controller2 = PidController::new();

                let signal_low = controller1.compute(low);
                let signal_high = controller2.compute(high);

                // Higher load (more waiting) should produce more negative signal
                prop_assert!(signal_high <= signal_low);
            }

            #[test]
            fn test_integral_bounded(iterations in 1..100usize, waiting in 1.0..10.0f64) {
                let mut controller = PidController::new().with_integral_max(50.0);

                for _ in 0..iterations {
                    controller.compute(waiting);
                }

                prop_assert!(controller.integral_term().abs() <= 50.0);
            }
        }
    }
}
