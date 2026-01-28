//! Activation count-based placement director implementation.

use crate::context::{PlacementContext, PlacementTarget};
use crate::director::{utils, PlacementDirector};
use crate::error::{PlacementError, PlacementResult};
use crate::options::ActivationCountBasedPlacementOptions;
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;

/// Placement director using the "power of k choices" algorithm.
///
/// This director randomly selects k silos and chooses the one with
/// the lowest activation count. This provides good load balancing
/// with minimal overhead - O(k) instead of O(n) for full scan.
///
/// Based on Mitzenmacher's "Power of Two Choices" research which shows
/// that comparing just 2 random choices dramatically improves load
/// distribution compared to pure random selection.
///
/// Features:
/// - Filters out overloaded silos before selection
/// - Falls back to all silos if all are overloaded
/// - Uses total activation count (active + recently used)
#[derive(Debug)]
pub struct ActivationCountPlacementDirector {
    options: ActivationCountBasedPlacementOptions,
}

impl Default for ActivationCountPlacementDirector {
    fn default() -> Self {
        Self::new(ActivationCountBasedPlacementOptions::default())
    }
}

impl ActivationCountPlacementDirector {
    /// Creates a new director with the specified options.
    pub fn new(options: ActivationCountBasedPlacementOptions) -> Self {
        Self { options }
    }

    /// Creates a new director with default options.
    pub fn with_defaults() -> Self {
        Self::default()
    }

    /// Returns the k value (number of choices).
    pub fn choose_out_of(&self) -> usize {
        self.options.choose_out_of()
    }
}

#[async_trait]
impl PlacementDirector for ActivationCountPlacementDirector {
    async fn on_add_activation(
        &self,
        _strategy: &dyn PlacementStrategy,
        target: &PlacementTarget,
        context: &dyn PlacementContext,
    ) -> PlacementResult<SiloAddress> {
        let compatible_silos = context.get_compatible_silos(target);

        if compatible_silos.is_empty() {
            tracing::warn!(
                grain_id = %target.grain_id(),
                "No compatible silos for activation-count placement"
            );
            return Err(PlacementError::NoCompatibleSilos);
        }

        // Use k value from options
        let k = self.options.choose_out_of();

        // Filter out overloaded silos
        let non_overloaded = utils::filter_overloaded(&compatible_silos, context);

        let candidates: Vec<SiloAddress> = if non_overloaded.is_empty() {
            tracing::debug!(
                grain_id = %target.grain_id(),
                "All silos overloaded, falling back to all compatible silos"
            );
            compatible_silos.clone()
        } else {
            non_overloaded.iter().map(|s| (*s).clone()).collect()
        };

        // Select k random candidates
        let k = k.min(candidates.len());
        let selected_candidates = utils::select_random_k(&candidates, k);

        if selected_candidates.is_empty() {
            return Err(PlacementError::NoCompatibleSilos);
        }

        // Find the one with minimum activation count
        let best = selected_candidates
            .into_iter()
            .min_by_key(|silo| {
                context
                    .get_silo_statistics(silo)
                    .map(|stats| stats.total_activation_count())
                    .unwrap_or(0)
            })
            .ok_or(PlacementError::NoCompatibleSilos)?;

        let best_count = context
            .get_silo_statistics(&best)
            .map(|s| s.total_activation_count())
            .unwrap_or(0);

        tracing::debug!(
            grain_id = %target.grain_id(),
            silo = %best,
            activation_count = best_count,
            k = k,
            candidates = compatible_silos.len(),
            "Activation-count placement selected silo"
        );

        Ok(best)
    }

    fn name(&self) -> &'static str {
        "ActivationCountPlacementDirector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::statistics::SiloRuntimeStatistics;
    use crate::strategy::ActivationCountBasedPlacement;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use std::collections::HashMap;
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

    fn make_target(name: &str, key: &str) -> PlacementTarget {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str(key));
        PlacementTarget::new(grain_id, grain_type)
    }

    #[tokio::test]
    async fn test_activation_count_empty_silos() {
        let director = ActivationCountPlacementDirector::default();
        let strategy = ActivationCountBasedPlacement::default();
        let target = make_target("test.grain", "key1");
        let local = make_silo(11111);
        let context = SimplePlacementContext::new(local, vec![]);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;
        assert!(matches!(result, Err(PlacementError::NoCompatibleSilos)));
    }

    #[tokio::test]
    async fn test_activation_count_single_silo() {
        let director = ActivationCountPlacementDirector::default();
        let strategy = ActivationCountBasedPlacement::default();
        let target = make_target("test.grain", "key1");
        let silos = make_silos(1);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        assert_eq!(result, silos[0]);
    }

    #[tokio::test]
    async fn test_activation_count_prefers_lower_count() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10), // Check all
        );
        let strategy = ActivationCountBasedPlacement::new(10);
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);
        let silos = vec![silo1.clone(), silo2.clone(), silo3.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone()).with_activation_count(100),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone()).with_activation_count(10), // Lowest
            )
            .with_statistics(
                silo3.clone(),
                SiloRuntimeStatistics::new(silo3.clone()).with_activation_count(50),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select silo2 which has lowest count
        assert_eq!(result, silo2);
    }

    #[tokio::test]
    async fn test_activation_count_uses_total_count() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10),
        );
        let strategy = ActivationCountBasedPlacement::new(10);
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_activation_count(10)
                    .with_recently_used_activation_count(50), // Total: 60
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_activation_count(30)
                    .with_recently_used_activation_count(10), // Total: 40, lower
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select silo2 which has lower total count
        assert_eq!(result, silo2);
    }

    #[tokio::test]
    async fn test_activation_count_filters_overloaded() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10),
        );
        let strategy = ActivationCountBasedPlacement::new(10);
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);
        let silos = vec![silo1.clone(), silo2.clone(), silo3.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_activation_count(1) // Lowest but overloaded
                    .with_overloaded(true),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_activation_count(100)
                    .with_overloaded(false),
            )
            .with_statistics(
                silo3.clone(),
                SiloRuntimeStatistics::new(silo3.clone())
                    .with_activation_count(50)
                    .with_overloaded(false),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should not select silo1 (overloaded), should select silo3 (lowest non-overloaded)
        assert_eq!(result, silo3);
    }

    #[tokio::test]
    async fn test_activation_count_falls_back_when_all_overloaded() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10),
        );
        let strategy = ActivationCountBasedPlacement::new(10);
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos.clone())
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_activation_count(100)
                    .with_overloaded(true),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_activation_count(200)
                    .with_overloaded(true),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should still select from overloaded silos (silo1 has lower count)
        assert_eq!(result, silo1);
    }

    #[tokio::test]
    async fn test_activation_count_no_stats_defaults_to_zero() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10),
        );
        let strategy = ActivationCountBasedPlacement::new(10);
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        // Only silo2 has stats
        let context = SimplePlacementContext::new(silo1.clone(), silos).with_statistics(
            silo2.clone(),
            SiloRuntimeStatistics::new(silo2.clone()).with_activation_count(100),
        );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Silo1 with no stats (assumed 0) should be preferred
        assert_eq!(result, silo1);
    }

    #[tokio::test]
    async fn test_activation_count_distribution() {
        let director = ActivationCountPlacementDirector::default(); // k=2
        let strategy = ActivationCountBasedPlacement::default();
        let silos = make_silos(10);

        // All silos have same count
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
        for silo in &silos {
            context.update_statistics(
                silo.clone(),
                SiloRuntimeStatistics::new(silo.clone()).with_activation_count(50),
            );
        }

        let mut selections: HashMap<SiloAddress, u32> = HashMap::new();

        for i in 0..100 {
            let target = make_target("test.grain", &format!("key{}", i));
            let result = director
                .on_add_activation(&strategy, &target, &context)
                .await
                .unwrap();
            *selections.entry(result).or_insert(0) += 1;
        }

        // Should have distributed across multiple silos
        assert!(selections.len() > 1);
    }

    #[test]
    fn test_director_name() {
        let director = ActivationCountPlacementDirector::default();
        assert_eq!(director.name(), "ActivationCountPlacementDirector");
    }

    #[test]
    fn test_choose_out_of() {
        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(5),
        );
        assert_eq!(director.choose_out_of(), 5);
    }
}
