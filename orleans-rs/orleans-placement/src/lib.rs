//! # Orleans Placement
//!
//! Placement strategies and directors for the Orleans Rust port.
//!
//! This crate provides the placement subsystem that determines which silo
//! hosts each grain activation. It includes:
//!
//! - **Placement Strategies**: Define the placement policy for grain types
//! - **Placement Directors**: Implement the actual silo selection algorithms
//! - **Placement Context**: Provides silo information for placement decisions
//! - **Silo Statistics**: Runtime metrics used for load-based placement
//!
//! ## Available Strategies
//!
//! | Strategy | Description | Use Case |
//! |----------|-------------|----------|
//! | `RandomPlacement` | Uniform random selection | Simple distribution |
//! | `HashBasedPlacement` | Deterministic based on grain ID | Cache affinity |
//! | `PreferLocalPlacement` | Prefer local silo, fallback random | Reduce network hops |
//! | `ActivationCountBasedPlacement` | Power-of-k-choices load balancing | Even distribution |
//! | `ResourceOptimizedPlacement` | Multi-dimensional resource scoring | Optimal utilization |
//!
//! ## Example
//!
//! ```rust
//! use orleans_placement::{
//!     RandomPlacementDirector, PlacementDirector, PlacementStrategy,
//!     RandomPlacement, SimplePlacementContext, PlacementTarget,
//! };
//! use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
//! use std::net::{IpAddr, Ipv4Addr, SocketAddr};
//!
//! # tokio_test::block_on(async {
//! // Create silos
//! let silo1 = SiloAddress::new(
//!     SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 11111),
//!     1,
//! );
//! let silo2 = SiloAddress::new(
//!     SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 22222),
//!     1,
//! );
//!
//! // Create context with compatible silos
//! let context = SimplePlacementContext::new(
//!     silo1.clone(),
//!     vec![silo1.clone(), silo2.clone()],
//! );
//!
//! // Create placement target
//! let grain_type = GrainType::create("my.grain");
//! let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
//! let target = PlacementTarget::new(grain_id, grain_type);
//!
//! // Use random placement
//! let director = RandomPlacementDirector::new();
//! let strategy = RandomPlacement;
//!
//! let selected = director.on_add_activation(&strategy, &target, &context).await.unwrap();
//! assert!(selected == silo1 || selected == silo2);
//! # });
//! ```

mod error;
mod options;
mod statistics;
mod strategy;
mod context;
mod director;
mod directors;

pub use error::{PlacementError, PlacementResult};
pub use options::{
    ActivationCountBasedPlacementOptions, PlacementOptions, ResourceOptimizedPlacementOptions,
};
pub use statistics::{SiloRuntimeStatistics, SiloStatisticsCache};
pub use strategy::{
    ActivationCountBasedPlacement, HashBasedPlacement, PlacementStrategy, PlacementStrategyType,
    PreferLocalPlacement, RandomPlacement, ResourceOptimizedPlacement, SiloRoleBasedPlacement,
};
pub use context::{PlacementContext, PlacementTarget, SiloStatus, SimplePlacementContext};
pub use director::{hints, utils, get_placement_hint, PlacementDirector, PlacementDirectorRegistry};
pub use directors::{
    ActivationCountPlacementDirector, HashBasedPlacementDirector, PreferLocalPlacementDirector,
    RandomPlacementDirector, ResourceOptimizedPlacementDirector,
};

/// Creates a default placement director registry with all built-in directors.
pub fn create_default_registry() -> PlacementDirectorRegistry {
    use std::sync::Arc;

    let registry = PlacementDirectorRegistry::new();

    registry.register(
        "RandomPlacement",
        Arc::new(RandomPlacementDirector::new()),
    );
    registry.register(
        "HashBasedPlacement",
        Arc::new(HashBasedPlacementDirector::new()),
    );
    registry.register(
        "PreferLocalPlacement",
        Arc::new(PreferLocalPlacementDirector::new()),
    );
    registry.register(
        "ActivationCountBasedPlacement",
        Arc::new(ActivationCountPlacementDirector::default()),
    );
    registry.register(
        "ResourceOptimizedPlacement",
        Arc::new(ResourceOptimizedPlacementDirector::default()),
    );

    registry
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    fn make_target(name: &str) -> PlacementTarget {
        let grain_type = GrainType::create(name);
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        PlacementTarget::new(grain_id, grain_type)
    }

    #[test]
    fn test_default_registry() {
        let registry = create_default_registry();
        assert_eq!(registry.len(), 5);
        assert!(registry.get("RandomPlacement").is_some());
        assert!(registry.get("HashBasedPlacement").is_some());
        assert!(registry.get("PreferLocalPlacement").is_some());
        assert!(registry.get("ActivationCountBasedPlacement").is_some());
        assert!(registry.get("ResourceOptimizedPlacement").is_some());
    }

    #[tokio::test]
    async fn test_random_placement_integration() {
        let silos = vec![make_silo(11111), make_silo(22222), make_silo(33333)];
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
        let target = make_target("test.grain");

        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        assert!(silos.contains(&result));
    }

    #[tokio::test]
    async fn test_hash_based_placement_integration() {
        let silos = vec![make_silo(11111), make_silo(22222), make_silo(33333)];
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
        let target = make_target("test.grain");

        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;

        let result1 = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        let result2 = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Hash-based should be deterministic
        assert_eq!(result1, result2);
    }

    #[tokio::test]
    async fn test_prefer_local_placement_integration() {
        let silos = vec![make_silo(11111), make_silo(22222), make_silo(33333)];
        let local = silos[1].clone();
        let context = SimplePlacementContext::new(local.clone(), silos);
        let target = make_target("test.grain");

        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should prefer local
        assert_eq!(result, local);
    }

    #[tokio::test]
    async fn test_activation_count_placement_integration() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone()).with_activation_count(100),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone()).with_activation_count(10),
            );

        let target = make_target("test.grain");

        let director = ActivationCountPlacementDirector::new(
            ActivationCountBasedPlacementOptions::new().with_choose_out_of(10),
        );
        let strategy = ActivationCountBasedPlacement::new(10);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should prefer silo2 with lower count
        assert_eq!(result, silo2);
    }

    #[tokio::test]
    async fn test_resource_optimized_placement_integration() {
        let silo1 = make_silo(11111);
        let silo2 = make_silo(22222);
        let silos = vec![silo1.clone(), silo2.clone()];

        let context = SimplePlacementContext::new(silo1.clone(), silos)
            .with_statistics(
                silo1.clone(),
                SiloRuntimeStatistics::new(silo1.clone())
                    .with_cpu_usage(90.0)
                    .with_memory_usage(0.9),
            )
            .with_statistics(
                silo2.clone(),
                SiloRuntimeStatistics::new(silo2.clone())
                    .with_cpu_usage(20.0)
                    .with_memory_usage(0.2),
            );

        let target = make_target("test.grain");

        let director = ResourceOptimizedPlacementDirector::new(
            ResourceOptimizedPlacementOptions::new().with_local_silo_preference_margin(0.0),
        );
        let strategy = ResourceOptimizedPlacement;

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should prefer silo2 with better resources
        assert_eq!(result, silo2);
    }

    #[test]
    fn test_strategy_types() {
        assert_eq!(RandomPlacement.name(), "RandomPlacement");
        assert_eq!(HashBasedPlacement.name(), "HashBasedPlacement");
        assert_eq!(PreferLocalPlacement.name(), "PreferLocalPlacement");
        assert_eq!(
            ActivationCountBasedPlacement::default().name(),
            "ActivationCountBasedPlacement"
        );
        assert_eq!(
            ResourceOptimizedPlacement.name(),
            "ResourceOptimizedPlacement"
        );
    }

    #[test]
    fn test_options_builder() {
        let options = PlacementOptions::new()
            .with_default_strategy("PreferLocalPlacement")
            .with_activation_count_options(
                ActivationCountBasedPlacementOptions::new().with_choose_out_of(4),
            )
            .with_resource_optimized_options(
                ResourceOptimizedPlacementOptions::cpu_optimized(),
            );

        assert_eq!(options.default_strategy(), "PreferLocalPlacement");
        assert_eq!(options.activation_count_options().choose_out_of(), 4);
        assert!(
            options.resource_optimized_options().cpu_usage_weight()
                > options.resource_optimized_options().memory_usage_weight()
        );
    }
}

#[cfg(test)]
mod property_tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress};
    use proptest::prelude::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn arb_silo() -> impl Strategy<Value = SiloAddress> {
        (1u16..=65534, 1i64..=1000).prop_map(|(port, gen)| {
            SiloAddress::new(
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
                gen,
            )
        })
    }

    fn arb_silos(min: usize, max: usize) -> impl Strategy<Value = Vec<SiloAddress>> {
        prop::collection::vec(arb_silo(), min..=max)
    }

    proptest! {
        #[test]
        fn prop_random_placement_returns_compatible_silo(
            silos in arb_silos(1, 10)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
                let grain_type = GrainType::create("test.grain");
                let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
                let target = PlacementTarget::new(grain_id, grain_type);

                let director = RandomPlacementDirector::new();
                let strategy = RandomPlacement;

                let result = director
                    .on_add_activation(&strategy, &target, &context)
                    .await
                    .unwrap();

                prop_assert!(silos.contains(&result));
                Ok(())
            }).unwrap();
        }

        #[test]
        fn prop_hash_based_placement_is_deterministic(
            silos in arb_silos(1, 10),
            key in "[a-z]{1,10}"
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
                let grain_type = GrainType::create("test.grain");
                let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str(&key));
                let target = PlacementTarget::new(grain_id, grain_type);

                let director = HashBasedPlacementDirector::new();
                let strategy = HashBasedPlacement;

                let result1 = director
                    .on_add_activation(&strategy, &target, &context)
                    .await
                    .unwrap();
                let result2 = director
                    .on_add_activation(&strategy, &target, &context)
                    .await
                    .unwrap();

                prop_assert_eq!(result1, result2);
                Ok(())
            }).unwrap();
        }

        #[test]
        fn prop_prefer_local_returns_local_when_available(
            silos in arb_silos(2, 10),
            local_idx in 0usize..10
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let local_idx = local_idx % silos.len();
                let local = silos[local_idx].clone();
                let context = SimplePlacementContext::new(local.clone(), silos.clone());
                let grain_type = GrainType::create("test.grain");
                let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
                let target = PlacementTarget::new(grain_id, grain_type);

                let director = PreferLocalPlacementDirector::new();
                let strategy = PreferLocalPlacement;

                let result = director
                    .on_add_activation(&strategy, &target, &context)
                    .await
                    .unwrap();

                // Should always return local when available
                prop_assert_eq!(result, local);
                Ok(())
            }).unwrap();
        }

        #[test]
        fn prop_activation_count_selects_from_compatible(
            silos in arb_silos(1, 10)
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());
                let grain_type = GrainType::create("test.grain");
                let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
                let target = PlacementTarget::new(grain_id, grain_type);

                let director = ActivationCountPlacementDirector::default();
                let strategy = ActivationCountBasedPlacement::default();

                let result = director
                    .on_add_activation(&strategy, &target, &context)
                    .await
                    .unwrap();

                prop_assert!(silos.contains(&result));
                Ok(())
            }).unwrap();
        }
    }
}
