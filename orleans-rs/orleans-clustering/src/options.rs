//! Configuration options for cluster membership.

use std::time::Duration;

/// Configuration options for cluster membership behavior.
#[derive(Clone, Debug)]
pub struct ClusterMembershipOptions {
    /// Enable/disable the liveness protocol.
    pub liveness_enabled: bool,

    /// Timeout for probe requests.
    pub probe_timeout: Duration,

    /// How often to refresh membership from storage.
    pub table_refresh_timeout: Duration,

    /// How long suspect votes remain valid.
    pub death_vote_expiration_timeout: Duration,

    /// How often to publish heartbeats.
    pub i_am_alive_table_publish_timeout: Duration,

    /// Maximum time to attempt joining the cluster.
    pub max_join_attempt_time: Duration,

    /// Enable gossip-based dissemination of membership changes.
    pub use_liveness_gossip: bool,

    /// Number of silos each silo monitors for health.
    pub num_probed_silos: usize,

    /// Number of missed probes before suspecting a silo.
    pub num_missed_probes_limit: i32,

    /// Number of votes required to declare a silo dead.
    pub num_votes_for_death_declaration: usize,

    /// How long to keep dead silo entries.
    pub defunct_silo_expiration: Duration,

    /// How often to clean up defunct entries.
    pub defunct_silo_cleanup_period: Duration,

    /// How often to check local health.
    pub local_health_degradation_monitoring_period: Duration,

    /// Extend probe timeout when local silo is degraded.
    pub extend_probe_timeout_during_degradation: bool,

    /// Enable indirect probing via healthy intermediaries.
    pub enable_indirect_probes: bool,

    /// Evict silos that exceed max join attempt time.
    pub evict_when_max_join_attempt_time_exceeded: bool,
}

impl Default for ClusterMembershipOptions {
    fn default() -> Self {
        Self {
            liveness_enabled: true,
            probe_timeout: Duration::from_secs(5),
            table_refresh_timeout: Duration::from_secs(60),
            death_vote_expiration_timeout: Duration::from_secs(120),
            i_am_alive_table_publish_timeout: Duration::from_secs(30),
            max_join_attempt_time: Duration::from_secs(300),
            use_liveness_gossip: true,
            num_probed_silos: 10,
            num_missed_probes_limit: 3,
            num_votes_for_death_declaration: 2,
            defunct_silo_expiration: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            defunct_silo_cleanup_period: Duration::from_secs(60 * 60),      // 1 hour
            local_health_degradation_monitoring_period: Duration::from_secs(10),
            extend_probe_timeout_during_degradation: true,
            enable_indirect_probes: true,
            evict_when_max_join_attempt_time_exceeded: true,
        }
    }
}

impl ClusterMembershipOptions {
    /// Create options suitable for development/testing with faster timeouts.
    pub fn development() -> Self {
        Self {
            liveness_enabled: true,
            probe_timeout: Duration::from_secs(1),
            table_refresh_timeout: Duration::from_secs(5),
            death_vote_expiration_timeout: Duration::from_secs(30),
            i_am_alive_table_publish_timeout: Duration::from_secs(5),
            max_join_attempt_time: Duration::from_secs(30),
            use_liveness_gossip: true,
            num_probed_silos: 10,
            num_missed_probes_limit: 2,
            num_votes_for_death_declaration: 2,
            defunct_silo_expiration: Duration::from_secs(60),
            defunct_silo_cleanup_period: Duration::from_secs(30),
            local_health_degradation_monitoring_period: Duration::from_secs(5),
            extend_probe_timeout_during_degradation: false,
            enable_indirect_probes: false,
            evict_when_max_join_attempt_time_exceeded: true,
        }
    }

    /// Create options with liveness disabled (for testing).
    pub fn no_liveness() -> Self {
        Self {
            liveness_enabled: false,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = ClusterMembershipOptions::default();
        assert!(opts.liveness_enabled);
        assert_eq!(opts.probe_timeout, Duration::from_secs(5));
        assert_eq!(opts.num_votes_for_death_declaration, 2);
    }

    #[test]
    fn test_development_options() {
        let opts = ClusterMembershipOptions::development();
        assert!(opts.liveness_enabled);
        assert!(opts.probe_timeout < Duration::from_secs(5));
        assert!(!opts.enable_indirect_probes);
    }

    #[test]
    fn test_no_liveness_options() {
        let opts = ClusterMembershipOptions::no_liveness();
        assert!(!opts.liveness_enabled);
    }
}
