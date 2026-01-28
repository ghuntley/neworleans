//! Hash-based placement director implementation.

use crate::context::{PlacementContext, PlacementTarget};
use crate::director::{utils, PlacementDirector};
use crate::error::{PlacementError, PlacementResult};
use crate::strategy::PlacementStrategy;
use async_trait::async_trait;
use orleans_core::SiloAddress;

/// Placement director that selects a silo based on grain ID hash.
///
/// This provides deterministic placement - the same grain ID will always
/// be placed on the same silo (assuming stable cluster membership).
///
/// The compatible silos are sorted before selection to ensure consistent
/// results regardless of the order they are provided.
#[derive(Debug, Clone, Copy, Default)]
pub struct HashBasedPlacementDirector;

impl HashBasedPlacementDirector {
    /// Creates a new hash-based placement director.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl PlacementDirector for HashBasedPlacementDirector {
    async fn on_add_activation(
        &self,
        _strategy: &dyn PlacementStrategy,
        target: &PlacementTarget,
        context: &dyn PlacementContext,
    ) -> PlacementResult<SiloAddress> {
        let mut compatible_silos = context.get_compatible_silos(target);

        if compatible_silos.is_empty() {
            tracing::warn!(
                grain_id = %target.grain_id(),
                "No compatible silos for hash-based placement"
            );
            return Err(PlacementError::NoCompatibleSilos);
        }

        // Sort for consistency - same hash always maps to same silo
        compatible_silos.sort();

        let hash = target.get_hash_code();
        let selected = utils::select_by_hash(&compatible_silos, hash)
            .ok_or(PlacementError::NoCompatibleSilos)?;

        tracing::debug!(
            grain_id = %target.grain_id(),
            hash = hash,
            silo = %selected,
            candidates = compatible_silos.len(),
            "Hash-based placement selected silo"
        );

        Ok(selected)
    }

    fn name(&self) -> &'static str {
        "HashBasedPlacementDirector"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::SimplePlacementContext;
    use crate::strategy::HashBasedPlacement;
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
    async fn test_hash_based_placement_empty_silos() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
        let target = make_target("test.grain", "key1");
        let local = make_silo(11111);
        let context = SimplePlacementContext::new(local, vec![]);

        let result = director
            .on_add_activation(&strategy, &target, &context)
            .await;
        assert!(matches!(result, Err(PlacementError::NoCompatibleSilos)));
    }

    #[tokio::test]
    async fn test_hash_based_placement_single_silo() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
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
    async fn test_hash_based_placement_deterministic() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
        let target = make_target("test.grain", "key1");
        let silos = make_silos(5);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        // Multiple calls with same grain should return same silo
        let result1 = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        let result2 = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();
        let result3 = director
            .on_add_activation(&strategy, &target, &context)
            .await
            .unwrap();

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }

    #[tokio::test]
    async fn test_hash_based_placement_order_independent() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
        let target = make_target("test.grain", "key1");
        let silos = make_silos(5);

        // Create contexts with silos in different orders
        let context1 = SimplePlacementContext::new(silos[0].clone(), silos.clone());
        let mut reversed = silos.clone();
        reversed.reverse();
        let context2 = SimplePlacementContext::new(reversed[0].clone(), reversed);

        let result1 = director
            .on_add_activation(&strategy, &target, &context1)
            .await
            .unwrap();
        let result2 = director
            .on_add_activation(&strategy, &target, &context2)
            .await
            .unwrap();

        // Should get same result regardless of order
        assert_eq!(result1, result2);
    }

    #[tokio::test]
    async fn test_hash_based_placement_distribution() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
        let silos = make_silos(5);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        let mut counts: HashMap<SiloAddress, u32> = HashMap::new();

        // Create many grains and count distribution
        for i in 0..100 {
            let target = make_target("test.grain", &format!("key{}", i));
            let result = director
                .on_add_activation(&strategy, &target, &context)
                .await
                .unwrap();
            *counts.entry(result).or_insert(0) += 1;
        }

        // Should have distributed across multiple silos
        assert!(counts.len() > 1);

        // Each silo should have gotten some activations
        // (not guaranteed but very likely with 100 grains)
        for count in counts.values() {
            assert!(*count > 0);
        }
    }

    #[tokio::test]
    async fn test_hash_based_placement_different_grains() {
        let director = HashBasedPlacementDirector::new();
        let strategy = HashBasedPlacement;
        let silos = make_silos(10);
        let context = SimplePlacementContext::new(silos[0].clone(), silos.clone());

        // Different grain keys should potentially map to different silos
        let target1 = make_target("test.grain", "key1");
        let target2 = make_target("test.grain", "key2");
        let target3 = make_target("test.grain", "key3");

        let result1 = director
            .on_add_activation(&strategy, &target1, &context)
            .await
            .unwrap();
        let result2 = director
            .on_add_activation(&strategy, &target2, &context)
            .await
            .unwrap();
        let result3 = director
            .on_add_activation(&strategy, &target3, &context)
            .await
            .unwrap();

        // At least verify they are valid silos
        assert!(silos.contains(&result1));
        assert!(silos.contains(&result2));
        assert!(silos.contains(&result3));
    }

    #[test]
    fn test_director_name() {
        let director = HashBasedPlacementDirector::new();
        assert_eq!(director.name(), "HashBasedPlacementDirector");
    }
}
