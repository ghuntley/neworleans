//! Placement strategy types and trait.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Marker trait for placement strategies.
///
/// Placement strategies define the placement policy for a grain type.
/// They are paired with placement directors that implement the actual
/// placement logic.
pub trait PlacementStrategy: Send + Sync + fmt::Debug {
    /// Returns the strategy name.
    fn name(&self) -> &'static str;

    /// Returns whether this strategy uses the grain directory.
    ///
    /// Strategies like StatelessWorker don't register with the directory
    /// since multiple activations are allowed.
    fn is_using_grain_directory(&self) -> bool {
        true
    }
}

/// Random placement strategy.
///
/// Selects a random silo from compatible silos.
/// Good for simple load distribution without affinity.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct RandomPlacement;

impl PlacementStrategy for RandomPlacement {
    fn name(&self) -> &'static str {
        "RandomPlacement"
    }
}

/// Hash-based placement strategy.
///
/// Selects silo based on grain ID hash.
/// Provides deterministic placement - same grain always goes to same silo
/// (assuming stable cluster membership).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct HashBasedPlacement;

impl PlacementStrategy for HashBasedPlacement {
    fn name(&self) -> &'static str {
        "HashBasedPlacement"
    }
}

/// Prefer local placement strategy.
///
/// Prefers the local silo if compatible, falls back to random.
/// Good for reducing network hops when grains are called from
/// co-located code.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct PreferLocalPlacement;

impl PlacementStrategy for PreferLocalPlacement {
    fn name(&self) -> &'static str {
        "PreferLocalPlacement"
    }
}

/// Activation count-based placement strategy.
///
/// Uses the "power of k choices" algorithm: randomly select k silos
/// and choose the one with the lowest activation count.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivationCountBasedPlacement {
    /// Number of silos to compare (default: 2).
    choose_out_of: usize,
}

impl Default for ActivationCountBasedPlacement {
    fn default() -> Self {
        Self { choose_out_of: 2 }
    }
}

impl ActivationCountBasedPlacement {
    /// Creates a new strategy with the specified k value.
    pub fn new(choose_out_of: usize) -> Self {
        Self {
            choose_out_of: choose_out_of.max(1),
        }
    }

    /// Returns the k value (number of silos to compare).
    pub fn choose_out_of(&self) -> usize {
        self.choose_out_of
    }
}

impl PlacementStrategy for ActivationCountBasedPlacement {
    fn name(&self) -> &'static str {
        "ActivationCountBasedPlacement"
    }
}

/// Resource-optimized placement strategy.
///
/// Multi-dimensional scoring based on CPU, memory, and activation count.
/// Best for heterogeneous clusters or resource-intensive grains.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct ResourceOptimizedPlacement;

impl PlacementStrategy for ResourceOptimizedPlacement {
    fn name(&self) -> &'static str {
        "ResourceOptimizedPlacement"
    }
}

/// Silo role-based placement strategy.
///
/// Places grains on silos with matching role names.
/// Useful for workload isolation or specialized node types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiloRoleBasedPlacement {
    /// Required silo role name.
    role: String,
}

impl SiloRoleBasedPlacement {
    /// Creates a new strategy requiring the specified role.
    pub fn new(role: impl Into<String>) -> Self {
        Self { role: role.into() }
    }

    /// Returns the required role name.
    pub fn role(&self) -> &str {
        &self.role
    }
}

impl PlacementStrategy for SiloRoleBasedPlacement {
    fn name(&self) -> &'static str {
        "SiloRoleBasedPlacement"
    }
}

/// Enumeration of built-in placement strategy types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PlacementStrategyType {
    /// Random placement.
    Random,
    /// Hash-based (deterministic) placement.
    HashBased,
    /// Prefer local silo.
    PreferLocal,
    /// Activation count load balancing.
    ActivationCountBased,
    /// Resource-optimized placement.
    ResourceOptimized,
    /// Silo role-based placement with role name.
    SiloRoleBased(String),
}

impl Default for PlacementStrategyType {
    fn default() -> Self {
        PlacementStrategyType::Random
    }
}

impl PlacementStrategyType {
    /// Parses a strategy type from its name.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "RandomPlacement" | "Random" => Some(PlacementStrategyType::Random),
            "HashBasedPlacement" | "HashBased" => Some(PlacementStrategyType::HashBased),
            "PreferLocalPlacement" | "PreferLocal" => Some(PlacementStrategyType::PreferLocal),
            "ActivationCountBasedPlacement" | "ActivationCountBased" => {
                Some(PlacementStrategyType::ActivationCountBased)
            }
            "ResourceOptimizedPlacement" | "ResourceOptimized" => {
                Some(PlacementStrategyType::ResourceOptimized)
            }
            _ => None,
        }
    }

    /// Returns the strategy name.
    pub fn name(&self) -> &'static str {
        match self {
            PlacementStrategyType::Random => "RandomPlacement",
            PlacementStrategyType::HashBased => "HashBasedPlacement",
            PlacementStrategyType::PreferLocal => "PreferLocalPlacement",
            PlacementStrategyType::ActivationCountBased => "ActivationCountBasedPlacement",
            PlacementStrategyType::ResourceOptimized => "ResourceOptimizedPlacement",
            PlacementStrategyType::SiloRoleBased(_) => "SiloRoleBasedPlacement",
        }
    }
}

impl fmt::Display for PlacementStrategyType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_placement() {
        let strategy = RandomPlacement;
        assert_eq!(strategy.name(), "RandomPlacement");
        assert!(strategy.is_using_grain_directory());
    }

    #[test]
    fn test_hash_based_placement() {
        let strategy = HashBasedPlacement;
        assert_eq!(strategy.name(), "HashBasedPlacement");
        assert!(strategy.is_using_grain_directory());
    }

    #[test]
    fn test_prefer_local_placement() {
        let strategy = PreferLocalPlacement;
        assert_eq!(strategy.name(), "PreferLocalPlacement");
        assert!(strategy.is_using_grain_directory());
    }

    #[test]
    fn test_activation_count_placement() {
        let strategy = ActivationCountBasedPlacement::default();
        assert_eq!(strategy.name(), "ActivationCountBasedPlacement");
        assert_eq!(strategy.choose_out_of(), 2);
        assert!(strategy.is_using_grain_directory());

        let custom = ActivationCountBasedPlacement::new(4);
        assert_eq!(custom.choose_out_of(), 4);

        // Test min value clamping
        let min = ActivationCountBasedPlacement::new(0);
        assert_eq!(min.choose_out_of(), 1);
    }

    #[test]
    fn test_resource_optimized_placement() {
        let strategy = ResourceOptimizedPlacement;
        assert_eq!(strategy.name(), "ResourceOptimizedPlacement");
        assert!(strategy.is_using_grain_directory());
    }

    #[test]
    fn test_silo_role_based_placement() {
        let strategy = SiloRoleBasedPlacement::new("worker");
        assert_eq!(strategy.name(), "SiloRoleBasedPlacement");
        assert_eq!(strategy.role(), "worker");
        assert!(strategy.is_using_grain_directory());
    }

    #[test]
    fn test_strategy_type_from_name() {
        assert!(matches!(
            PlacementStrategyType::from_name("RandomPlacement"),
            Some(PlacementStrategyType::Random)
        ));
        assert!(matches!(
            PlacementStrategyType::from_name("Random"),
            Some(PlacementStrategyType::Random)
        ));
        assert!(matches!(
            PlacementStrategyType::from_name("HashBasedPlacement"),
            Some(PlacementStrategyType::HashBased)
        ));
        assert!(matches!(
            PlacementStrategyType::from_name("PreferLocalPlacement"),
            Some(PlacementStrategyType::PreferLocal)
        ));
        assert!(matches!(
            PlacementStrategyType::from_name("ActivationCountBasedPlacement"),
            Some(PlacementStrategyType::ActivationCountBased)
        ));
        assert!(matches!(
            PlacementStrategyType::from_name("ResourceOptimizedPlacement"),
            Some(PlacementStrategyType::ResourceOptimized)
        ));
        assert!(PlacementStrategyType::from_name("Unknown").is_none());
    }

    #[test]
    fn test_strategy_type_name() {
        assert_eq!(PlacementStrategyType::Random.name(), "RandomPlacement");
        assert_eq!(PlacementStrategyType::HashBased.name(), "HashBasedPlacement");
        assert_eq!(
            PlacementStrategyType::PreferLocal.name(),
            "PreferLocalPlacement"
        );
        assert_eq!(
            PlacementStrategyType::ActivationCountBased.name(),
            "ActivationCountBasedPlacement"
        );
        assert_eq!(
            PlacementStrategyType::ResourceOptimized.name(),
            "ResourceOptimizedPlacement"
        );
        assert_eq!(
            PlacementStrategyType::SiloRoleBased("worker".to_string()).name(),
            "SiloRoleBasedPlacement"
        );
    }

    #[test]
    fn test_strategy_type_display() {
        assert_eq!(format!("{}", PlacementStrategyType::Random), "RandomPlacement");
        assert_eq!(
            format!("{}", PlacementStrategyType::HashBased),
            "HashBasedPlacement"
        );
    }

    #[test]
    fn test_strategy_type_default() {
        let default = PlacementStrategyType::default();
        assert!(matches!(default, PlacementStrategyType::Random));
    }

    #[test]
    fn test_strategy_serialization() {
        let strategy = RandomPlacement;
        let json = serde_json::to_string(&strategy).unwrap();
        let _: RandomPlacement = serde_json::from_str(&json).unwrap();

        let strategy = ActivationCountBasedPlacement::new(4);
        let json = serde_json::to_string(&strategy).unwrap();
        let deserialized: ActivationCountBasedPlacement = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.choose_out_of(), 4);
    }
}
