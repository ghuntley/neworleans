//! Prefer local placement director implementation.

use crate::context::{PlacementContext, PlacementTarget, SiloStatus};
use crate::director::{utils, PlacementDirector};
use crate::error::{PlacementError, PlacementResult};
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;

/// Placement director that prefers the local silo.
///
/// If the local silo is active and compatible with the grain type,
/// it will be selected. Otherwise, falls back to random selection.
///
/// This strategy is useful for reducing network hops when grains
/// are likely to be called from co-located code.
#[derive(Debug, Clone, Copy, Default)]
pub struct PreferLocalPlacementDirector;

impl PreferLocalPlacementDirector {
    /// Creates a new prefer-local placement director.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PlacementDirector for PreferLocalPlacementDirector {
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
                "No compatible silos for prefer-local placement"
            );
            return Err(PlacementError::NoCompatibleSilos);
        }

        let local_silo = context.local_silo();
        let local_status = context.local_silo_status();

        // Prefer local if active and compatible
        if local_status == SiloStatus::Active && compatible_silos.contains(local_silo) {
            tracing::debug!(
                grain_id = %target.grain_id(),
                silo = %local_silo,
                "Prefer-local placement selected local silo"
            );
            return Ok(local_silo.clone());
        }

        // Fallback to random selection
        let selected = utils::select_random(&compatible_silos)
            .ok_or(PlacementError::NoCompatibleSilos)?;

        tracing::debug!(
            grain_id = %target.grain_id(),
            silo = %selected,
            local_status = ?local_status,
            local_compatible = compatible_silos.contains(local_silo),
            "Prefer-local placement fell back to random selection"
        );

        Ok(selected)
    }

    fn name(&self) -> &'static str {
        "PreferLocalPlacementDirector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::strategy::PreferLocalPlacement;
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

    #[tokio::test]
    async fn test_prefer_local_empty_silos() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let local = make_silo(11111);
        let context = SimplePlacementContext::new(local, vec![]);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;
        assert!(matches!(result, Err(PlacementError::NoCompatibleSilos)));
    }

    #[tokio::test]
    async fn test_prefer_local_selects_local() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(5);
        let local = silos[2].clone(); // Local is in the middle
        let context = SimplePlacementContext::new(local.clone(), silos);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should always select local when available
        assert_eq!(result, local);
    }

    #[tokio::test]
    async fn test_prefer_local_consistent() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let silos = make_silos(5);
        let local = silos[0].clone();
        let context = SimplePlacementContext::new(local.clone(), silos);

        // Multiple calls should all return local
        for _ in 0..10 {
            let target = make_target("test.grain");
            let result = director
                .on_add_activation(&strategy, &target, &context)
                .await
                .unwrap();
            assert_eq!(result, local);
        }
    }

    #[tokio::test]
    async fn test_prefer_local_falls_back_when_local_not_compatible() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let local = make_silo(59999); // Not in compatible list
        let silos = make_silos(3);
        let context = SimplePlacementContext::new(local, silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select from compatible silos, not local
        assert!(silos.contains(&result));
    }

    #[tokio::test]
    async fn test_prefer_local_falls_back_when_shutting_down() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(3);
        let local = silos[0].clone();
        let context =
            SimplePlacementContext::new(local.clone(), silos.clone())
                .with_status(SiloStatus::ShuttingDown);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select from compatible silos, may or may not be local
        assert!(silos.contains(&result));
    }

    #[tokio::test]
    async fn test_prefer_local_falls_back_when_stopping() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(3);
        let local = silos[0].clone();
        let context = SimplePlacementContext::new(local.clone(), silos.clone())
            .with_status(SiloStatus::Stopping);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        // Should select from compatible silos
        assert!(silos.contains(&result));
    }

    #[tokio::test]
    async fn test_prefer_local_single_silo() {
        let director = PreferLocalPlacementDirector::new();
        let strategy = PreferLocalPlacement;
        let target = make_target("test.grain");
        let silos = make_silos(1);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        assert_eq!(result, silos[0]);
    }

    #[test]
    fn test_director_name() {
        let director = PreferLocalPlacementDirector::new();
        assert_eq!(director.name(), "PreferLocalPlacementDirector");
    }

    #[test]
    fn test_silo_status_is_terminating() {
        assert!(!SiloStatus::Active.is_terminating());
        assert!(SiloStatus::ShuttingDown.is_terminating());
        assert!(SiloStatus::Stopping.is_terminating());
        assert!(SiloStatus::Dead.is_terminating());
    }
}
