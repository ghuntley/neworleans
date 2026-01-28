//! Stateless Worker placement director.
//!
//! Implements silo-level placement strategy for stateless workers,
//! preferring local placement with random fallback.

use crate::options::StatelessWorkerPlacement;
use orleans_core::SiloAddress;
use tracing::{debug, trace};

/// Placement director for stateless workers.
///
/// Unlike regular grains, stateless workers:
/// - Do NOT use the grain directory
/// - Prefer local silo placement
/// - Fall back to random selection from compatible silos
#[derive(Debug, Default)]
pub struct StatelessWorkerDirector;

impl StatelessWorkerDirector {
    /// Creates a new stateless worker director.
    pub fn new() -> Self {
        Self
    }

    /// Determines which silo should handle the activation.
    ///
    /// Strategy:
    /// 1. Prefer local silo if it's in the compatible list and not terminating
    /// 2. Otherwise, randomly select from compatible silos
    ///
    /// # Arguments
    /// * `local_silo` - The local silo's address
    /// * `local_silo_terminating` - Whether the local silo is shutting down
    /// * `compatible_silos` - List of silos that can handle this grain type
    ///
    /// # Returns
    /// The selected silo address, or None if no compatible silos
    pub fn select_silo(
        &self,
        local_silo: &SiloAddress,
        local_silo_terminating: bool,
        compatible_silos: &[SiloAddress],
    ) -> Option<SiloAddress> {
        if compatible_silos.is_empty() {
            debug!("No compatible silos available for stateless worker placement");
            return None;
        }

        // Prefer local silo if not terminating
        if !local_silo_terminating {
            for silo in compatible_silos {
                if silo == local_silo {
                    trace!(
                        silo = %local_silo,
                        "Selected local silo for stateless worker"
                    );
                    return Some(local_silo.clone());
                }
            }
        }

        // Random selection from compatible silos
        let index = random_index(compatible_silos.len());
        let selected = compatible_silos[index].clone();

        debug!(
            selected_silo = %selected,
            total_compatible = compatible_silos.len(),
            "Selected random silo for stateless worker"
        );

        Some(selected)
    }

    /// Returns the placement strategy for this director.
    pub fn placement(&self) -> StatelessWorkerPlacement {
        StatelessWorkerPlacement::default()
    }

    /// Returns whether this director uses the grain directory.
    /// Stateless workers do NOT use the grain directory.
    pub fn uses_grain_directory(&self) -> bool {
        false
    }
}

/// Generate a random index for silo selection.
fn random_index(max: usize) -> usize {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};

    let state = RandomState::new();
    let mut hasher = state.build_hasher();
    hasher.write_usize(std::time::Instant::now().elapsed().as_nanos() as usize);
    hasher.finish() as usize % max
}

/// Context for placement decisions.
#[derive(Debug, Clone)]
pub struct PlacementContext {
    /// The local silo address.
    pub local_silo: SiloAddress,

    /// Whether the local silo is terminating.
    pub is_terminating: bool,

    /// List of silos compatible with this grain type.
    pub compatible_silos: Vec<SiloAddress>,
}

impl PlacementContext {
    /// Creates a new placement context.
    pub fn new(
        local_silo: SiloAddress,
        is_terminating: bool,
        compatible_silos: Vec<SiloAddress>,
    ) -> Self {
        Self {
            local_silo,
            is_terminating,
            compatible_silos,
        }
    }

    /// Creates a context for a healthy local silo.
    pub fn healthy(local_silo: SiloAddress, compatible_silos: Vec<SiloAddress>) -> Self {
        Self::new(local_silo, false, compatible_silos)
    }

    /// Creates a context for a terminating local silo.
    pub fn terminating(local_silo: SiloAddress, compatible_silos: Vec<SiloAddress>) -> Self {
        Self::new(local_silo, true, compatible_silos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn make_silo(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    #[test]
    fn test_director_new() {
        let director = StatelessWorkerDirector::new();
        assert!(!director.uses_grain_directory());
    }

    #[test]
    fn test_select_local_silo_preferred() {
        let director = StatelessWorkerDirector::new();
        let local = make_silo(11111);
        let silos = vec![make_silo(22222), local.clone(), make_silo(33333)];

        let selected = director.select_silo(&local, false, &silos);
        assert_eq!(selected, Some(local));
    }

    #[test]
    fn test_select_avoids_terminating_local() {
        let director = StatelessWorkerDirector::new();
        let local = make_silo(11111);
        let other = make_silo(22222);
        let silos = vec![other.clone()];

        let selected = director.select_silo(&local, true, &silos);
        // Should select from compatible silos since local is terminating
        assert!(selected.is_some());
        assert_ne!(selected, Some(local));
    }

    #[test]
    fn test_select_empty_silos() {
        let director = StatelessWorkerDirector::new();
        let local = make_silo(11111);

        let selected = director.select_silo(&local, false, &[]);
        assert!(selected.is_none());
    }

    #[test]
    fn test_select_local_not_in_compatible() {
        let director = StatelessWorkerDirector::new();
        let local = make_silo(11111);
        let other1 = make_silo(22222);
        let other2 = make_silo(33333);
        let silos = vec![other1.clone(), other2.clone()];

        let selected = director.select_silo(&local, false, &silos);
        assert!(selected.is_some());
        assert!(silos.contains(&selected.unwrap()));
    }

    #[test]
    fn test_select_single_silo() {
        let director = StatelessWorkerDirector::new();
        let local = make_silo(11111);
        let only = make_silo(22222);
        let silos = vec![only.clone()];

        let selected = director.select_silo(&local, false, &silos);
        assert_eq!(selected, Some(only));
    }

    #[test]
    fn test_placement_default() {
        let director = StatelessWorkerDirector::new();
        let placement = director.placement();
        assert!(placement.max_local > 0);
        assert!(placement.remove_idle_workers);
        assert!(!placement.is_using_grain_directory());
    }

    #[test]
    fn test_placement_context_healthy() {
        let local = make_silo(11111);
        let silos = vec![make_silo(22222)];
        let ctx = PlacementContext::healthy(local.clone(), silos.clone());

        assert_eq!(ctx.local_silo, local);
        assert!(!ctx.is_terminating);
        assert_eq!(ctx.compatible_silos, silos);
    }

    #[test]
    fn test_placement_context_terminating() {
        let local = make_silo(11111);
        let silos = vec![make_silo(22222)];
        let ctx = PlacementContext::terminating(local.clone(), silos.clone());

        assert_eq!(ctx.local_silo, local);
        assert!(ctx.is_terminating);
        assert_eq!(ctx.compatible_silos, silos);
    }

    #[test]
    fn test_uses_grain_directory() {
        let director = StatelessWorkerDirector::new();
        assert!(!director.uses_grain_directory());
    }

    mod distribution_tests {
        use super::*;
        use std::collections::HashMap;

        #[test]
        fn test_random_distribution() {
            let director = StatelessWorkerDirector::new();
            let local = make_silo(11111); // Not in compatible list
            let silos = vec![make_silo(22222), make_silo(33333), make_silo(44444)];

            let mut counts: HashMap<SiloAddress, usize> = HashMap::new();
            let iterations = 1000;

            for _ in 0..iterations {
                if let Some(selected) = director.select_silo(&local, false, &silos) {
                    *counts.entry(selected).or_insert(0) += 1;
                }
            }

            // All silos should be selected at least once
            // (statistically very unlikely to fail)
            for silo in &silos {
                assert!(counts.get(silo).copied().unwrap_or(0) > 0);
            }
        }
    }
}
