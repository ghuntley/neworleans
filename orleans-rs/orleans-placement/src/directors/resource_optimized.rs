//! Resource-optimized placement director implementation.

use crate::context::{PlacementContext, PlacementTarget, SiloStatus};
use crate::director::{utils, PlacementDirector};
use crate::error::{PlacementError, PlacementResult};
use crate::options::ResourceOptimizedPlacementOptions;
use crate::statistics::SiloRuntimeStatistics;
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;

/// Placement director using multi-dimensional resource scoring.
///
/// Calculates a composite score based on:
/// - CPU usage
/// - Memory usage
/// - Available memory
/// - Activation count
///
/// Uses sqrt(n) candidate selection (power-of-k choices with k = sqrt(n))
/// and includes a preference margin for local placement to reduce hops.
///
/// Lower scores are better. The director selects the candidate with
/// the lowest score, with a preference for local silo if its score
/// is within the configured margin of the best score.
#[derive(Debug)]
pub struct ResourceOptimizedPlacementDirector {
    options: ResourceOptimizedPlacementOptions,
}

impl Default for ResourceOptimizedPlacementDirector {
    fn default() -> Self {
        Self::new(ResourceOptimizedPlacementOptions::default())
    }
}

impl ResourceOptimizedPlacementDirector {
    /// Creates a new director with the specified options.
    pub fn new(options: ResourceOptimizedPlacementOptions) -> Self {
        Self { options }
    }

    /// Creates a new director with default options.
    pub fn with_defaults() -> Self {
        Self::default()
    }

    /// Calculates the placement score for a silo (lower is better).
    fn calculate_score(
        &self,
        stats: &SiloRuntimeStatistics,
        max_activations: u32,
        cluster_max_memory: u64,
    ) -> f64 {
        let total_weight = self.options.total_weight();
        if total_weight == 0.0 {
            return 0.0;
        }

        // CPU score: higher CPU = higher score (worse)
        let cpu_score = self.options.cpu_usage_weight() * (stats.cpu_usage() / 100.0);

        // Memory usage score: higher usage = higher score (worse)
        let memory_score = self.options.memory_usage_weight() * stats.memory_usage();

        // Available memory score: less available = higher score (worse)
        let available_score =
            self.options.available_memory_weight() * (1.0 - stats.normalized_available_memory());

        // Max available memory score: less max = higher score (worse)
        let max_available_score = self.options.max_available_memory_weight()
            * (1.0 - stats.normalized_max_available_memory(cluster_max_memory));

        // Activation count score: more activations = higher score (worse)
        let activation_score = if max_activations > 0 {
            self.options.activation_count_weight()
                * (stats.activation_count() as f64 / max_activations as f64)
        } else {
            0.0
        };

        cpu_score + memory_score + available_score + max_available_score + activation_score
    }
}

#[async_trait]
impl PlacementDirector for ResourceOptimizedPlacementDirector {
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
                "No compatible silos for resource-optimized placement"
            );
            return Err(PlacementError::NoCompatibleSilos);
        }

        // Get cluster-wide maximums for normalization
        let max_activations = context.max_activation_count();
        let cluster_max_memory = context.max_available_memory();

        // Select sqrt(n) candidates
        let k = ((compatible_silos.len() as f64).sqrt().ceil() as usize).max(1);
        let candidates = utils::fisher_yates_prefix(&compatible_silos, k);

        // Find best candidate
        let mut best: Option<SiloAddress> = None;
        let mut best_score = f64::MAX;

        for silo in &candidates {
            // Skip overloaded silos
            if let Some(stats) = context.get_silo_statistics(silo) {
                if stats.is_overloaded() {
                    tracing::trace!(silo = %silo, "Skipping overloaded silo");
                    continue;
                }

                let score = self.calculate_score(&stats, max_activations, cluster_max_memory);

                if score < best_score {
                    best_score = score;
                    best = Some(silo.clone());
                }
            } else {
                // No stats available - treat as neutral (score 0)
                if best.is_none() || 0.0 < best_score {
                    best_score = 0.0;
                    best = Some(silo.clone());
                }
            }
        }

        // Check local silo preference
        let local_silo = context.local_silo();
        let local_status = context.local_silo_status();

        if local_status == SiloStatus::Active && compatible_silos.contains(local_silo) {
            if let Some(local_stats) = context.get_silo_statistics(local_silo) {
                if !local_stats.is_overloaded() {
                    let local_score =
                        self.calculate_score(&local_stats, max_activations, cluster_max_memory);

                    // Use local if within margin of best
                    let threshold =
                        best_score * (1.0 + self.options.local_silo_preference_margin());

                    if local_score <= threshold {
                        tracing::debug!(
                            grain_id = %target.grain_id(),
                            silo = %local_silo,
                            local_score = local_score,
                            best_score = best_score,
                            margin = self.options.local_silo_preference_margin(),
                            "Resource-optimized placement preferred local silo"
                        );
                        return Ok(local_silo.clone());
                    }
                }
            } else {
                // No stats for local - use if no better option
                if best.is_none() || best_score > 0.0 {
                    return Ok(local_silo.clone());
                }
            }
        }

        let selected = best.ok_or(PlacementError::AllSilosOverloaded)?;

        tracing::debug!(
            grain_id = %target.grain_id(),
            silo = %selected,
            score = best_score,
            k = k,
            candidates = compatible_silos.len(),
            "Resource-optimized placement selected silo"
        );

        Ok(selected)
    }

    fn name(&self) -> &'static str {
        "ResourceOptimizedPlacementDirector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::strategy::ResourceOptimizedPlacement;
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

    fn make_target(name: &str, key: &str) -> PlacementTarget {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str(key));
        PlacementTarget::new(grain_id, grain_type)
    }

    #[tokio::test]
    async fn test_resource_optimized_empty_silos() {
        let director = ResourceOptimizedPlacementDirector::default();
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");
        let local = make_silo(11111);
        let context = SimplePlacementContext::new(local, vec![]);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;
        assert!(matches!(result, Err(PlacementError::NoCompatibleSilos)));
    }

    #[tokio::test]
    async fn test_resource_optimized_single_silo() {
        let director = ResourceOptimizedPlacementDirector::default();
        let strategy = ResourceOptimizedPlacement;
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
    async fn test_resource_optimized_prefers_lower_score() {
        let director = ResourceOptimizedPlacementDirector::new(
            ResourceOptimizedPlacementOptions::new().with_local_silo_preference_margin(0.0),
        );
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silo3 = make_silo(33333);
        let silos = vec![silo1.clone(), silo2.clone(), silo3.clone()];

        // silo2 has lowest resource usage
        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_cpu_usage(80.0)
                    .with_memory_usage(0.9)
                    .with_activation_count(100),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_cpu_usage(20.0) // Lowest CPU
                    .with_memory_usage(0.3) // Lowest memory
                    .with_activation_count(10), // Lowest activations
            )
            .with_statistics(
                silo3.clone(),
                SiloRuntimeStatistics::new(silo3.clone())
                    .with_cpu_usage(50.0)
                    .with_memory_usage(0.5)
                    .with_activation_count(50),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select silo2 which has best resources
        assert_eq!(result, silo2);
    }

    #[tokio::test]
    async fn test_resource_optimized_local_preference() {
        let director = ResourceOptimizedPlacementDirector::new(
            ResourceOptimizedPlacementOptions::new().with_local_silo_preference_margin(0.1), // 10%
        );
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        // silo2 is slightly better, but local (silo1) is within 10% margin
        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_cpu_usage(30.0)
                    .with_memory_usage(0.35)
                    .with_activation_count(55),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_cpu_usage(28.0) // Slightly better
                    .with_memory_usage(0.33) // Slightly better
                    .with_activation_count(50), // Slightly better
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select local silo1 because it's within margin
        assert_eq!(result, silo1);
    }

    #[tokio::test]
    async fn test_resource_optimized_skips_overloaded() {
        let director = ResourceOptimizedPlacementDirector::default();
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        // silo1 has best resources but is overloaded
        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_cpu_usage(10.0)
                    .with_memory_usage(0.1)
                    .with_activation_count(5)
                    .with_overloaded(true), // Overloaded!
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_cpu_usage(50.0)
                    .with_memory_usage(0.5)
                    .with_activation_count(50)
                    .with_overloaded(false),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select silo2, not overloaded silo1
        assert_eq!(result, silo2);
    }

    #[tokio::test]
    async fn test_resource_optimized_all_overloaded() {
        let director = ResourceOptimizedPlacementDirector::default();
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");

        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone()).with_overloaded(true),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone()).with_overloaded(true),
            );

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;

        assert!(matches!(result, Err(PlacementError::AllSilosOverloaded)));
    }

    #[tokio::test]
    async fn test_resource_optimized_no_stats() {
        let director = ResourceOptimizedPlacementDirector::default();
        let strategy = ResourceOptimizedPlacement;
        let target = make_target("test.grain", "key1");

        let silos = make_silos(3);
        // No statistics set - all silos should be treated as neutral
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        assert!(silos.contains(&result));
    }

    #[test]
    fn test_calculate_score() {
        let director = ResourceOptimizedPlacementDirector::default();

        // Low resource usage = low score
        let low_usage = SiloRuntimeStatistics::default()
            .with_cpu_usage(10.0)
            .with_memory_usage(0.1)
            .with_activation_count(10);

        // High resource usage = high score
        let high_usage = SiloRuntimeStatistics::default()
            .with_cpu_usage(90.0)
            .with_memory_usage(0.9)
            .with_activation_count(100);

        let low_score = director.calculate_score(&low_usage, 100, 1_000_000);
        let high_score = director.calculate_score(&high_usage, 100, 1_000_000);

        assert!(low_score < high_score);
    }

    #[test]
    fn test_director_name() {
        let director = ResourceOptimizedPlacementDirector::default();
        assert_eq!(director.name(), "ResourceOptimizedPlacementDirector");
    }

    #[test]
    fn test_options_presets() {
        let cpu = ResourceOptimizedPlacementOptions::cpu_optimized();
        assert!(cpu.cpu_usage_weight() > cpu.memory_usage_weight());

        let mem = ResourceOptimizedPlacementOptions::memory_optimized();
        assert!(mem.memory_usage_weight() > mem.cpu_usage_weight());

        let local = ResourceOptimizedPlacementOptions::prefer_local();
        assert_eq!(local.local_silo_preference_margin(), 0.25);
    }
}
