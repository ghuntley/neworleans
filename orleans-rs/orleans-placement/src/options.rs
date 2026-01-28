//! Configuration options for placement strategies.

use serde::{Deserialize, Serialize};

/// Options for activation count-based placement.
///
/// Uses the "power of k choices" algorithm where k candidates are randomly
/// selected and the one with the lowest activation count is chosen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationCountBasedPlacementOptions {
    /// Number of silos to randomly sample and compare (default: 2).
    ///
    /// Higher values improve load balancing but increase decision latency.
    /// Based on the "Power of Two Choices" algorithm by Mitzenmacher.
    choose_out_of: usize,
}

impl Default for ActivationCountBasedPlacementOptions {
    fn default() -> Self {
        Self { choose_out_of: 2 }
    }
}

impl ActivationCountBasedPlacementOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the number of silos to compare.
    pub fn with_choose_out_of(mut self, k: usize) -> Self {
        self.choose_out_of = k.max(1);
        self
    }

    /// Returns the number of silos to compare.
    pub fn choose_out_of(&self) -> usize {
        self.choose_out_of
    }
}

/// Options for resource-optimized placement.
///
/// Multi-dimensional scoring based on CPU, memory, and activation count
/// with configurable weights.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceOptimizedPlacementOptions {
    /// Weight for CPU usage in scoring (default: 40).
    cpu_usage_weight: f64,

    /// Weight for memory usage in scoring (default: 20).
    memory_usage_weight: f64,

    /// Weight for available memory in scoring (default: 20).
    available_memory_weight: f64,

    /// Weight for max available memory in scoring (default: 5).
    max_available_memory_weight: f64,

    /// Weight for activation count in scoring (default: 15).
    activation_count_weight: f64,

    /// Preference margin for local silo (default: 0.05 = 5%).
    ///
    /// If the local silo's score is within this margin of the best score,
    /// prefer local placement to reduce network hops.
    local_silo_preference_margin: f64,
}

impl Default for ResourceOptimizedPlacementOptions {
    fn default() -> Self {
        Self {
            cpu_usage_weight: 40.0,
            memory_usage_weight: 20.0,
            available_memory_weight: 20.0,
            max_available_memory_weight: 5.0,
            activation_count_weight: 15.0,
            local_silo_preference_margin: 0.05,
        }
    }
}

impl ResourceOptimizedPlacementOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the CPU usage weight.
    pub fn with_cpu_usage_weight(mut self, weight: f64) -> Self {
        self.cpu_usage_weight = weight.max(0.0);
        self
    }

    /// Sets the memory usage weight.
    pub fn with_memory_usage_weight(mut self, weight: f64) -> Self {
        self.memory_usage_weight = weight.max(0.0);
        self
    }

    /// Sets the available memory weight.
    pub fn with_available_memory_weight(mut self, weight: f64) -> Self {
        self.available_memory_weight = weight.max(0.0);
        self
    }

    /// Sets the max available memory weight.
    pub fn with_max_available_memory_weight(mut self, weight: f64) -> Self {
        self.max_available_memory_weight = weight.max(0.0);
        self
    }

    /// Sets the activation count weight.
    pub fn with_activation_count_weight(mut self, weight: f64) -> Self {
        self.activation_count_weight = weight.max(0.0);
        self
    }

    /// Sets the local silo preference margin.
    pub fn with_local_silo_preference_margin(mut self, margin: f64) -> Self {
        self.local_silo_preference_margin = margin.clamp(0.0, 1.0);
        self
    }

    /// Returns the CPU usage weight.
    pub fn cpu_usage_weight(&self) -> f64 {
        self.cpu_usage_weight
    }

    /// Returns the memory usage weight.
    pub fn memory_usage_weight(&self) -> f64 {
        self.memory_usage_weight
    }

    /// Returns the available memory weight.
    pub fn available_memory_weight(&self) -> f64 {
        self.available_memory_weight
    }

    /// Returns the max available memory weight.
    pub fn max_available_memory_weight(&self) -> f64 {
        self.max_available_memory_weight
    }

    /// Returns the activation count weight.
    pub fn activation_count_weight(&self) -> f64 {
        self.activation_count_weight
    }

    /// Returns the local silo preference margin.
    pub fn local_silo_preference_margin(&self) -> f64 {
        self.local_silo_preference_margin
    }

    /// Returns the total weight sum for normalization.
    pub fn total_weight(&self) -> f64 {
        self.cpu_usage_weight
            + self.memory_usage_weight
            + self.available_memory_weight
            + self.max_available_memory_weight
            + self.activation_count_weight
    }

    /// Creates options optimized for CPU-bound workloads.
    pub fn cpu_optimized() -> Self {
        Self {
            cpu_usage_weight: 60.0,
            memory_usage_weight: 10.0,
            available_memory_weight: 10.0,
            max_available_memory_weight: 5.0,
            activation_count_weight: 15.0,
            local_silo_preference_margin: 0.05,
        }
    }

    /// Creates options optimized for memory-bound workloads.
    pub fn memory_optimized() -> Self {
        Self {
            cpu_usage_weight: 15.0,
            memory_usage_weight: 35.0,
            available_memory_weight: 30.0,
            max_available_memory_weight: 10.0,
            activation_count_weight: 10.0,
            local_silo_preference_margin: 0.05,
        }
    }

    /// Creates options that prioritize local placement.
    pub fn prefer_local() -> Self {
        Self {
            cpu_usage_weight: 30.0,
            memory_usage_weight: 15.0,
            available_memory_weight: 15.0,
            max_available_memory_weight: 5.0,
            activation_count_weight: 10.0,
            local_silo_preference_margin: 0.25, // 25% margin for local preference
        }
    }
}

/// General placement configuration options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementOptions {
    /// Default placement strategy type name (default: "RandomPlacement").
    default_strategy: String,

    /// Options for activation count-based placement.
    activation_count_options: ActivationCountBasedPlacementOptions,

    /// Options for resource-optimized placement.
    resource_optimized_options: ResourceOptimizedPlacementOptions,
}

impl Default for PlacementOptions {
    fn default() -> Self {
        Self {
            default_strategy: "RandomPlacement".to_string(),
            activation_count_options: ActivationCountBasedPlacementOptions::default(),
            resource_optimized_options: ResourceOptimizedPlacementOptions::default(),
        }
    }
}

impl PlacementOptions {
    /// Creates new options with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the default placement strategy.
    pub fn with_default_strategy(mut self, strategy: impl Into<String>) -> Self {
        self.default_strategy = strategy.into();
        self
    }

    /// Sets the activation count-based placement options.
    pub fn with_activation_count_options(
        mut self,
        options: ActivationCountBasedPlacementOptions,
    ) -> Self {
        self.activation_count_options = options;
        self
    }

    /// Sets the resource-optimized placement options.
    pub fn with_resource_optimized_options(
        mut self,
        options: ResourceOptimizedPlacementOptions,
    ) -> Self {
        self.resource_optimized_options = options;
        self
    }

    /// Returns the default placement strategy name.
    pub fn default_strategy(&self) -> &str {
        &self.default_strategy
    }

    /// Returns the activation count-based placement options.
    pub fn activation_count_options(&self) -> &ActivationCountBasedPlacementOptions {
        &self.activation_count_options
    }

    /// Returns the resource-optimized placement options.
    pub fn resource_optimized_options(&self) -> &ResourceOptimizedPlacementOptions {
        &self.resource_optimized_options
    }

    /// Creates options for testing with simpler configuration.
    pub fn for_testing() -> Self {
        Self {
            default_strategy: "RandomPlacement".to_string(),
            activation_count_options: ActivationCountBasedPlacementOptions::new()
                .with_choose_out_of(2),
            resource_optimized_options: ResourceOptimizedPlacementOptions::new()
                .with_local_silo_preference_margin(0.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_activation_count_options_defaults() {
        let options = ActivationCountBasedPlacementOptions::default();
        assert_eq!(options.choose_out_of(), 2);
    }

    #[test]
    fn test_activation_count_options_builder() {
        let options = ActivationCountBasedPlacementOptions::new().with_choose_out_of(4);
        assert_eq!(options.choose_out_of(), 4);
    }

    #[test]
    fn test_activation_count_options_min_value() {
        let options = ActivationCountBasedPlacementOptions::new().with_choose_out_of(0);
        assert_eq!(options.choose_out_of(), 1); // Clamped to 1
    }

    #[test]
    fn test_resource_optimized_options_defaults() {
        let options = ResourceOptimizedPlacementOptions::default();
        assert_eq!(options.cpu_usage_weight(), 40.0);
        assert_eq!(options.memory_usage_weight(), 20.0);
        assert_eq!(options.available_memory_weight(), 20.0);
        assert_eq!(options.max_available_memory_weight(), 5.0);
        assert_eq!(options.activation_count_weight(), 15.0);
        assert_eq!(options.local_silo_preference_margin(), 0.05);
    }

    #[test]
    fn test_resource_optimized_options_builder() {
        let options = ResourceOptimizedPlacementOptions::new()
            .with_cpu_usage_weight(50.0)
            .with_memory_usage_weight(25.0)
            .with_local_silo_preference_margin(0.1);

        assert_eq!(options.cpu_usage_weight(), 50.0);
        assert_eq!(options.memory_usage_weight(), 25.0);
        assert_eq!(options.local_silo_preference_margin(), 0.1);
    }

    #[test]
    fn test_resource_optimized_options_total_weight() {
        let options = ResourceOptimizedPlacementOptions::default();
        assert_eq!(options.total_weight(), 100.0);
    }

    #[test]
    fn test_resource_optimized_options_margin_clamping() {
        let options = ResourceOptimizedPlacementOptions::new()
            .with_local_silo_preference_margin(-0.5);
        assert_eq!(options.local_silo_preference_margin(), 0.0);

        let options = ResourceOptimizedPlacementOptions::new()
            .with_local_silo_preference_margin(1.5);
        assert_eq!(options.local_silo_preference_margin(), 1.0);
    }

    #[test]
    fn test_resource_optimized_options_presets() {
        let cpu = ResourceOptimizedPlacementOptions::cpu_optimized();
        assert!(cpu.cpu_usage_weight() > cpu.memory_usage_weight());

        let mem = ResourceOptimizedPlacementOptions::memory_optimized();
        assert!(mem.memory_usage_weight() > mem.cpu_usage_weight());

        let local = ResourceOptimizedPlacementOptions::prefer_local();
        assert_eq!(local.local_silo_preference_margin(), 0.25);
    }

    #[test]
    fn test_placement_options_defaults() {
        let options = PlacementOptions::default();
        assert_eq!(options.default_strategy(), "RandomPlacement");
    }

    #[test]
    fn test_placement_options_builder() {
        let options = PlacementOptions::new()
            .with_default_strategy("HashBasedPlacement")
            .with_activation_count_options(
                ActivationCountBasedPlacementOptions::new().with_choose_out_of(4),
            );

        assert_eq!(options.default_strategy(), "HashBasedPlacement");
        assert_eq!(options.activation_count_options().choose_out_of(), 4);
    }

    #[test]
    fn test_placement_options_for_testing() {
        let options = PlacementOptions::for_testing();
        assert_eq!(options.default_strategy(), "RandomPlacement");
        assert_eq!(
            options
                .resource_optimized_options()
                .local_silo_preference_margin(),
            0.0
        );
    }

    #[test]
    fn test_options_serialization() {
        let options = PlacementOptions::default();
        let json = serde_json::to_string(&options).unwrap();
        let deserialized: PlacementOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.default_strategy(), options.default_strategy());
    }
}
