//! Orleans-RS Real Network Integration Testing Framework
//!
//! This crate provides utilities for comprehensive integration testing of Orleans-RS
//! clusters with real network communication between separate processes.
//!
//! # Overview
//!
//! The framework enables:
//! - Spawning multiple silo processes
//! - Cluster formation and membership verification
//! - Cross-process grain invocation testing
//! - Failure scenario testing (silo crashes, network issues)
//! - Performance baseline measurements
//!
//! # Example
//!
//! ```rust,ignore
//! use orleans_tests_integration::{TestClusterBuilder, ClusterAssertions};
//!
//! #[tokio::test]
//! async fn test_cluster_formation() {
//!     let cluster = TestClusterBuilder::new()
//!         .with_silo_count(3)
//!         .with_startup_timeout(Duration::from_secs(30))
//!         .build()
//!         .await
//!         .expect("Cluster should start");
//!
//!     cluster.assert_all_silos_active();
//!     cluster.assert_consistent_membership();
//!
//!     cluster.stop().await;
//! }
//! ```

mod error;
mod cluster_builder;
mod silo_process;
mod client_harness;
mod assertions;

pub use error::{TestError, TestResult};
pub use cluster_builder::{TestClusterBuilder, TestCluster, ClusterConfig};
pub use silo_process::{SiloProcess, SiloProcessConfig, SiloOutput, ProcessEvent};
pub use client_harness::{TestClientHarness, GrainInvocationResult};
pub use assertions::{ClusterAssertions, MembershipAssertions, DirectoryAssertions, TestClusterAssertions};

/// Re-export commonly used Orleans types
pub mod prelude {
    pub use super::{
        TestClusterBuilder, TestCluster, ClusterConfig,
        SiloProcess, SiloProcessConfig, SiloOutput, ProcessEvent,
        TestClientHarness, GrainInvocationResult,
        ClusterAssertions, MembershipAssertions, DirectoryAssertions,
        TestClusterAssertions,
        TestError, TestResult,
    };

    pub use orleans_core::{GrainId, GrainType, IdSpan, SiloAddress, ActivationId};
    pub use orleans_clustering::{SiloStatus, MembershipEntry};
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_crate_compiles() {
        // Basic compilation test
        assert!(true);
    }
}
