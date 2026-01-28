//! Random placement director implementation.

use crate::context::{PlacementContext, PlacementTarget};
use crate::director::{get_placement_hint, utils, PlacementDirector};
use crate::error::{PlacementError, PlacementResult};
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;

/// Placement director that selects a random compatible silo.
///
/// This is the simplest placement strategy - it randomly selects
/// from all compatible silos with uniform probability.
///
/// If a placement hint is provided in the request context and the
/// hinted silo is compatible, it will be used instead of random selection.
#[derive(Debug, Clone, Copy, Default)]
pub struct RandomPlacementDirector;

impl RandomPlacementDirector {
    /// Creates a new random placement director.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PlacementDirector for RandomPlacementDirector {
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
                "No compatible silos for random placement"
            );
            return Err(PlacementError::NoCompatibleSilos);
        }

        // Check for placement hint
        if let Some(hint) = get_placement_hint(target) {
            if compatible_silos.contains(&hint) {
                tracing::debug!(
                    grain_id = %target.grain_id(),
                    silo = %hint,
                    "Using placement hint for random placement"
                );
                return Ok(hint);
            }
        }

        // Random selection
        let selected = utils::select_random(&compatible_silos)
            .ok_or(PlacementError::NoCompatibleSilos)?;

        tracing::debug!(
            grain_id = %target.grain_id(),
            silo = %selected,
            candidates = compatible_silos.len(),
            "Random placement selected silo"
        );

        Ok(selected)
    }

    fn name(&self) -> &'static str {
        "RandomPlacementDirector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::director::hints;
    use crate::strategy::RandomPlacement;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use std::collections::HashSet;
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

    #[tokio::test]
    async fn test_random_placement_empty_silos() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let target = make_target("test.grain");
        let local = make_silo(11111);
        let context = SimplePlacementContext::new(local, vec![]);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;
        assert!(matches!(result, Err(PlacementError::NoCompatibleSilos)));
    }

    #[tokio::test]
    async fn test_random_placement_single_silo() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(1);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        assert_eq!(result, silos[0]);
    }

    #[tokio::test]
    async fn test_random_placement_multiple_silos() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(5);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        assert!(silos.contains(&result));
    }

    #[tokio::test]
    async fn test_random_placement_distribution() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let silos = make_silos(3);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let mut selections = HashSet::new();

        // Run many times to check distribution
        for i in 0..100 {
            let grain_type = GrainType::create("test.grain");
            let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str(&format!("key{}", i)));
            let target = PlacementTarget::new(grain_id, grain_type);

            let result = director
                .on_add_activation(&strategy, &target, &context)
                .await
                .unwrap();
            selections.insert(result);
        }

        // Should have selected from multiple silos
        assert!(selections.len() > 1);
    }

    #[tokio::test]
    async fn test_random_placement_with_hint() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let silos = make_silos(5);
        let hint_silo = silos[3].clone();

        let grain_type = GrainType::create("test.grain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let target = PlacementTarget::new(grain_id, grain_type)
            .with_context(hints::PLACEMENT_HINT, hint_silo.clone());

        let context = SimplePlacementContext::new(silos[0].clone(), silos);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        assert_eq!(result, hint_silo);
    }

    #[tokio::test]
    async fn test_random_placement_hint_not_compatible() {
        let director = RandomPlacementDirector::new();
        let strategy = RandomPlacement;
        let silos = make_silos(3);
        let hint_silo = make_silo(59999); // Not in compatible list

        let grain_type = GrainType::create("test.grain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let target = PlacementTarget::new(grain_id, grain_type)
            .with_context(hints::PLACEMENT_HINT, hint_silo);

        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should not use hint, but should select from compatible silos
        assert!(silos.contains(&result));
    }

    #[test]
    fn test_director_name() {
        let director = RandomPlacementDirector::new();
        assert_eq!(director.name(), "RandomPlacementDirector");
    }
}
