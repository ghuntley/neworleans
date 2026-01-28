//! Configuration options for stateless workers.

use std::time::Duration;

/// Configuration options for stateless worker behavior.
#[derive(Debug, Clone)]
pub struct StatelessWorkerOptions {
    /// Whether to automatically remove idle workers.
    /// Default: true
    pub remove_idle_workers: bool,

    /// How often to inspect workers for idle removal.
    /// Default: 500ms
    pub idle_workers_inspection_period: Duration,

    /// Minimum number of consecutive idle cycles before removing a worker.
    /// Default: 1
    pub min_idle_cycles_before_removal: u32,

    /// Default maximum workers per silo when not specified on the attribute.
    /// Default: number of CPU cores
    pub default_max_local_workers: usize,

    /// Minimum number of workers to maintain even during low load.
    /// Default: 1
    pub min_workers: usize,

    /// Timeout for worker activation.
    /// Default: 30 seconds
    pub activation_timeout: Duration,

    /// Timeout for worker deactivation.
    /// Default: 30 seconds
    pub deactivation_timeout: Duration,
}

impl Default for StatelessWorkerOptions {
    fn default() -> Self {
        Self {
            remove_idle_workers: true,
            idle_workers_inspection_period: Duration::from_millis(500),
            min_idle_cycles_before_removal: 1,
            default_max_local_workers: num_cpus(),
            min_workers: 1,
            activation_timeout: Duration::from_secs(30),
            deactivation_timeout: Duration::from_secs(30),
        }
    }
}

impl StatelessWorkerOptions {
    /// Creates a new `StatelessWorkerOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options optimized for testing with shorter timeouts.
    pub fn for_testing() -> Self {
        Self {
            remove_idle_workers: true,
            idle_workers_inspection_period: Duration::from_millis(50),
            min_idle_cycles_before_removal: 1,
            default_max_local_workers: 4,
            min_workers: 1,
            activation_timeout: Duration::from_secs(5),
            deactivation_timeout: Duration::from_secs(5),
        }
    }

    /// Sets whether to remove idle workers.
    pub fn with_remove_idle_workers(mut self, remove: bool) -> Self {
        self.remove_idle_workers = remove;
        self
    }

    /// Sets the idle workers inspection period.
    pub fn with_idle_workers_inspection_period(mut self, period: Duration) -> Self {
        self.idle_workers_inspection_period = period;
        self
    }

    /// Sets the minimum idle cycles before removal.
    pub fn with_min_idle_cycles_before_removal(mut self, cycles: u32) -> Self {
        self.min_idle_cycles_before_removal = cycles;
        self
    }

    /// Sets the default maximum local workers.
    pub fn with_default_max_local_workers(mut self, max: usize) -> Self {
        self.default_max_local_workers = max;
        self
    }

    /// Sets the minimum number of workers.
    pub fn with_min_workers(mut self, min: usize) -> Self {
        self.min_workers = min;
        self
    }

    /// Sets the activation timeout.
    pub fn with_activation_timeout(mut self, timeout: Duration) -> Self {
        self.activation_timeout = timeout;
        self
    }

    /// Sets the deactivation timeout.
    pub fn with_deactivation_timeout(mut self, timeout: Duration) -> Self {
        self.deactivation_timeout = timeout;
        self
    }

    /// Validates the options and returns an error if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.default_max_local_workers == 0 {
            return Err("default_max_local_workers must be greater than 0".to_string());
        }
        if self.min_workers > self.default_max_local_workers {
            return Err("min_workers cannot exceed default_max_local_workers".to_string());
        }
        if self.idle_workers_inspection_period.is_zero() && self.remove_idle_workers {
            return Err(
                "idle_workers_inspection_period must be non-zero when remove_idle_workers is true"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// Placement configuration for stateless workers.
#[derive(Debug, Clone)]
pub struct StatelessWorkerPlacement {
    /// Maximum number of workers on this silo.
    pub max_local: usize,

    /// Whether to automatically remove idle workers.
    pub remove_idle_workers: bool,
}

impl Default for StatelessWorkerPlacement {
    fn default() -> Self {
        Self {
            max_local: num_cpus(),
            remove_idle_workers: true,
        }
    }
}

impl StatelessWorkerPlacement {
    /// Creates a new placement with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new placement with specified max local workers.
    pub fn with_max_local(max_local: usize) -> Self {
        Self {
            max_local,
            remove_idle_workers: true,
        }
    }

    /// Creates a new placement with specified max local workers and idle removal setting.
    pub fn with_max_local_and_remove_idle(max_local: usize, remove_idle_workers: bool) -> Self {
        Self {
            max_local,
            remove_idle_workers,
        }
    }

    /// Returns whether this placement uses the grain directory.
    /// Stateless workers do NOT use the grain directory.
    pub fn is_using_grain_directory(&self) -> bool {
        false
    }
}

/// Get the number of CPU cores, with a fallback.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = StatelessWorkerOptions::default();
        assert!(opts.remove_idle_workers);
        assert_eq!(opts.idle_workers_inspection_period, Duration::from_millis(500));
        assert_eq!(opts.min_idle_cycles_before_removal, 1);
        assert!(opts.default_max_local_workers > 0);
        assert_eq!(opts.min_workers, 1);
        assert_eq!(opts.activation_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_for_testing_options() {
        let opts = StatelessWorkerOptions::for_testing();
        assert!(opts.remove_idle_workers);
        assert_eq!(opts.idle_workers_inspection_period, Duration::from_millis(50));
        assert_eq!(opts.default_max_local_workers, 4);
        assert_eq!(opts.activation_timeout, Duration::from_secs(5));
    }

    #[test]
    fn test_builder_pattern() {
        let opts = StatelessWorkerOptions::new()
            .with_remove_idle_workers(false)
            .with_idle_workers_inspection_period(Duration::from_secs(1))
            .with_min_idle_cycles_before_removal(3)
            .with_default_max_local_workers(16)
            .with_min_workers(2)
            .with_activation_timeout(Duration::from_secs(60));

        assert!(!opts.remove_idle_workers);
        assert_eq!(opts.idle_workers_inspection_period, Duration::from_secs(1));
        assert_eq!(opts.min_idle_cycles_before_removal, 3);
        assert_eq!(opts.default_max_local_workers, 16);
        assert_eq!(opts.min_workers, 2);
        assert_eq!(opts.activation_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_validate_success() {
        let opts = StatelessWorkerOptions::default();
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_max_workers() {
        let opts = StatelessWorkerOptions::new().with_default_max_local_workers(0);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_validate_min_exceeds_max() {
        let opts = StatelessWorkerOptions::new()
            .with_default_max_local_workers(2)
            .with_min_workers(4);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_validate_zero_inspection_period() {
        let opts = StatelessWorkerOptions::new()
            .with_remove_idle_workers(true)
            .with_idle_workers_inspection_period(Duration::ZERO);
        assert!(opts.validate().is_err());
    }

    #[test]
    fn test_default_placement() {
        let placement = StatelessWorkerPlacement::default();
        assert!(placement.max_local > 0);
        assert!(placement.remove_idle_workers);
        assert!(!placement.is_using_grain_directory());
    }

    #[test]
    fn test_placement_with_max_local() {
        let placement = StatelessWorkerPlacement::with_max_local(8);
        assert_eq!(placement.max_local, 8);
        assert!(placement.remove_idle_workers);
    }

    #[test]
    fn test_placement_with_max_local_and_remove_idle() {
        let placement = StatelessWorkerPlacement::with_max_local_and_remove_idle(16, false);
        assert_eq!(placement.max_local, 16);
        assert!(!placement.remove_idle_workers);
    }

    #[test]
    fn test_placement_not_using_directory() {
        let placement = StatelessWorkerPlacement::new();
        assert!(!placement.is_using_grain_directory());
    }
}
