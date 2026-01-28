//! Placement director trait and utilities.

use crate::context::{PlacementContext, PlacementTarget};
use crate::error::PlacementResult;
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;
use std::fmt;
use std::sync::Arc;

/// Trait for placement directors that implement placement logic.
///
/// Each placement strategy has a corresponding director that implements
/// the actual silo selection algorithm.
#[async_trait]
pub trait PlacementDirector: Send + Sync + fmt::Debug {
    /// Selects a silo for a new grain activation.
    ///
    /// # Arguments
    /// * `strategy` - The placement strategy for this grain type
    /// * `target` - The placement target (grain identity and context)
    /// * `context` - Context providing silo information
    ///
    /// # Returns
    /// The selected silo address, or an error if placement fails.
    async fn on_add_activation(
        &self,
        strategy: &dyn PlacementStrategy,
        target: &PlacementTarget,
        context: &dyn PlacementContext,
    ) -> PlacementResult<SiloAddress>;

    /// Returns the director name for logging.
    fn name(&self) -> &'static str;
}

/// Hints that can influence placement decisions.
pub mod hints {
    /// Request context key for placement hint (preferred silo).
    pub const PLACEMENT_HINT: &str = "orleans.placement.hint";

    /// Request context key for placement affinity group.
    pub const AFFINITY_GROUP: &str = "orleans.placement.affinity";

    /// Request context key for excluded silos.
    pub const EXCLUDE_SILOS: &str = "orleans.placement.exclude";
}

/// Extracts a placement hint from the target's request context.
pub fn get_placement_hint(target: &PlacementTarget) -> Option<SiloAddress> {
    target.get_context::<SiloAddress>(hints::PLACEMENT_HINT).cloned()
}

/// Registry of placement directors.
#[derive(Debug)]
pub struct PlacementDirectorRegistry {
    directors: dashmap::DashMap<String, Arc<dyn PlacementDirector>>,
}

impl Default for PlacementDirectorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl PlacementDirectorRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            directors: dashmap::DashMap::new(),
        }
    }

    /// Registers a director for a strategy.
    pub fn register(&self, strategy_name: impl Into<String>, director: Arc<dyn PlacementDirector>) {
        let name = strategy_name.into();
        tracing::debug!(
            strategy = %name,
            director = director.name(),
            "Registering placement director"
        );
        self.directors.insert(name, director);
    }

    /// Gets a director for a strategy.
    pub fn get(&self, strategy_name: &str) -> Option<Arc<dyn PlacementDirector>> {
        self.directors.get(strategy_name).map(|r| r.clone())
    }

    /// Returns all registered strategy names.
    pub fn strategy_names(&self) -> Vec<String> {
        self.directors.iter().map(|r| r.key().clone()).collect()
    }

    /// Returns the number of registered directors.
    pub fn len(&self) -> usize {
        self.directors.len()
    }

    /// Returns true if no directors are registered.
    pub fn is_empty(&self) -> bool {
        self.directors.is_empty()
    }

    /// Removes a director by strategy name.
    pub fn remove(&self, strategy_name: &str) -> Option<Arc<dyn PlacementDirector>> {
        self.directors.remove(strategy_name).map(|(_, v)| v)
    }

    /// Clears all registered directors.
    pub fn clear(&self) {
        self.directors.clear();
    }
}

/// Utility functions for directors.
pub mod utils {
    use super::*;
    use rand::seq::SliceRandom;
    use rand::Rng;

    /// Selects a random silo from the list.
    pub fn select_random(silos: &[SiloAddress]) -> Option<SiloAddress> {
        if silos.is_empty() {
            return None;
        }
        let mut rng = rand::thread_rng();
        silos.choose(&mut rng).cloned()
    }

    /// Selects multiple random silos from the list (without replacement).
    pub fn select_random_k(silos: &[SiloAddress], k: usize) -> Vec<SiloAddress> {
        if silos.is_empty() || k == 0 {
            return Vec::new();
        }
        let k = k.min(silos.len());
        let mut rng = rand::thread_rng();
        silos
            .choose_multiple(&mut rng, k)
            .cloned()
            .collect()
    }

    /// Fisher-Yates shuffle prefix - shuffles and returns first k elements.
    ///
    /// More efficient than full shuffle when k << n.
    pub fn fisher_yates_prefix<T: Clone>(items: &[T], k: usize) -> Vec<T> {
        if items.is_empty() || k == 0 {
            return Vec::new();
        }

        let k = k.min(items.len());
        let mut result: Vec<T> = items.to_vec();
        let mut rng = rand::thread_rng();

        for i in 0..k {
            let j = rng.gen_range(i..result.len());
            result.swap(i, j);
        }

        result.truncate(k);
        result
    }

    /// Selects a silo by hash (deterministic).
    pub fn select_by_hash(silos: &[SiloAddress], hash: u32) -> Option<SiloAddress> {
        if silos.is_empty() {
            return None;
        }
        // Ensure positive index
        let hash = hash & 0x7fffffff;
        let index = (hash as usize) % silos.len();
        Some(silos[index].clone())
    }

    /// Filters out overloaded silos based on statistics.
    pub fn filter_overloaded<'a>(
        silos: &'a [SiloAddress],
        context: &'a dyn PlacementContext,
    ) -> Vec<&'a SiloAddress> {
        silos
            .iter()
            .filter(|s| {
                context
                    .get_silo_statistics(s)
                    .map(|stats| !stats.is_overloaded())
                    .unwrap_or(true) // Include if no stats available
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::error::PlacementError;
    use crate::statistics::SiloRuntimeStatistics;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    fn make_silos(n: u16) -> Vec<SiloAddress> {
        (0..n).map(|i| make_silo(11111 + i)).collect()
    }

    fn make_target(name: &str) -> PlacementTarget {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        PlacementTarget::new(grain_id, grain_type)
    }

    #[test]
    fn test_get_placement_hint() {
        let target = make_target("test.grain");
        assert!(get_placement_hint(&target).is_none());

        let hint_silo = make_silo(59999);
        let target_with_hint = make_target("test.grain")
            .with_context(hints::PLACEMENT_HINT, hint_silo.clone());
        assert_eq!(get_placement_hint(&target_with_hint), Some(hint_silo));
    }

    #[test]
    fn test_registry_operations() {
        #[derive(Debug)]
        struct TestDirector;

        #[async_trait]
        impl PlacementDirector for TestDirector {
            async fn on_add_activation(
                &self,
                _strategy: &dyn PlacementStrategy,
                _target: &PlacementTarget,
                _context: &dyn PlacementContext,
            ) -> PlacementResult<SiloAddress> {
                Err(PlacementError::NoCompatibleSilos)
            }

            fn name(&self) -> &'static str {
                "TestDirector"
            }
        }

        let registry = PlacementDirectorRegistry::new();
        assert!(registry.is_empty());

        registry.register("RandomPlacement", Arc::new(TestDirector));
        assert_eq!(registry.len(), 1);
        assert!(registry.get("RandomPlacement").is_some());
        assert!(registry.get("Unknown").is_none());

        let names = registry.strategy_names();
        assert!(names.contains(&"RandomPlacement".to_string()));

        registry.remove("RandomPlacement");
        assert!(registry.is_empty());
    }

    #[test]
    fn test_select_random() {
        assert!(utils::select_random(&[]).is_none());

        let silos = make_silos(5);
        let selected = utils::select_random(&silos);
        assert!(selected.is_some());
        assert!(silos.contains(&selected.unwrap()));
    }

    #[test]
    fn test_select_random_k() {
        let silos = make_silos(10);

        let empty: Vec<SiloAddress> = Vec::new();
        assert!(utils::select_random_k(&empty, 5).is_empty());
        assert!(utils::select_random_k(&silos, 0).is_empty());

        let selected = utils::select_random_k(&silos, 3);
        assert_eq!(selected.len(), 3);

        // All selected should be from original list
        for s in &selected {
            assert!(silos.contains(s));
        }

        // No duplicates
        let mut unique = selected.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), selected.len());

        // k > len should return all
        let all = utils::select_random_k(&silos, 100);
        assert_eq!(all.len(), 10);
    }

    #[test]
    fn test_fisher_yates_prefix() {
        let items: Vec<i32> = (0..10).collect();

        assert!(utils::fisher_yates_prefix::<i32>(&[], 5).is_empty());
        assert!(utils::fisher_yates_prefix(&items, 0).is_empty());

        let prefix = utils::fisher_yates_prefix(&items, 3);
        assert_eq!(prefix.len(), 3);
        for p in &prefix {
            assert!(items.contains(p));
        }
    }

    #[test]
    fn test_select_by_hash() {
        let silos = make_silos(5);

        assert!(utils::select_by_hash(&[], 12345).is_none());

        // Same hash should give same result
        let selected1 = utils::select_by_hash(&silos, 12345);
        let selected2 = utils::select_by_hash(&silos, 12345);
        assert_eq!(selected1, selected2);

        // Different hash should give (possibly) different result
        let _ = utils::select_by_hash(&silos, 67890);
    }

    #[test]
    fn test_filter_overloaded() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);
        let silos = vec![silo1.clone(), silo2.clone(), silo3.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos.clone())
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone()).with_overloaded(false),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone()).with_overloaded(true),
            )
            .with_statistics(
                silo3.clone(),
                SiloRuntimeStatistics::new(silo3.clone()).with_overloaded(false),
            );

        let non_overloaded = utils::filter_overloaded(&silos, &context);
        assert_eq!(non_overloaded.len(), 2);
        assert!(non_overloaded.contains(&&silo1));
        assert!(!non_overloaded.contains(&&silo2));
        assert!(non_overloaded.contains(&&silo3));
    }

    #[test]
    fn test_filter_overloaded_no_stats() {
        let silos = make_silos(3);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        // When no stats available, all silos should pass
        let filtered = utils::filter_overloaded(&silos, &context);
        assert_eq!(filtered.len(), 3);
    }
}
