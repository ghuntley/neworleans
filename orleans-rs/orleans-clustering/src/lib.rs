//! Orleans Clustering - Cluster Membership Protocol
//!
//! This crate implements the Orleans cluster membership protocol, enabling
//! silos to discover each other and track cluster state.
//!
//! # Overview
//!
//! Orleans does not use formal consensus protocols like Paxos or Raft. Instead,
//! it relies on:
//!
//! - **External Storage Backend**: A shared membership table (in-memory, Redis, SQL, etc.)
//! - **Optimistic Concurrency**: Version/ETag validation for atomic updates
//! - **Gossip Protocol**: Peer-to-peer dissemination of membership changes
//! - **Suspect Voting**: Distributed failure detection through probing
//!
//! # Key Components
//!
//! - [`SiloStatus`]: Lifecycle states of a silo (Created -> Joining -> Active -> Dead)
//! - [`MembershipEntry`]: A silo's entry in the membership table
//! - [`IMembershipTable`]: Storage backend interface for membership data
//! - [`InMemoryMembershipTable`]: In-memory implementation for testing
//! - [`MembershipTableManager`]: Coordinates membership table operations
//! - [`MembershipAgent`]: Orchestrates silo join/leave protocol
//!
//! # Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use orleans_core::SiloAddress;
//! use orleans_clustering::{
//!     MembershipAgent, MembershipTableManager, InMemoryMembershipTable,
//!     ClusterMembershipOptions, IMembershipTable,
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a membership table (use a shared backend in production)
//! let table = Arc::new(InMemoryMembershipTable::new("my-cluster"));
//! table.initialize_membership_table(true).await?;
//!
//! // Create the local silo address
//! let local_silo = SiloAddress::new("127.0.0.1:11111".parse()?, 1);
//!
//! // Create the manager
//! let options = ClusterMembershipOptions::default();
//! let manager = Arc::new(MembershipTableManager::new(table, local_silo, options.clone()));
//!
//! // Create and start the agent
//! let agent = MembershipAgent::new(manager.clone(), options);
//! agent.start().await?;
//!
//! // The silo is now active in the cluster
//! assert!(manager.is_silo_active(manager.local_silo()));
//!
//! // To leave the cluster gracefully
//! agent.stop().await?;
//! # Ok(())
//! # }
//! ```

mod error;
mod in_memory_table;
mod membership_agent;
mod membership_entry;
mod membership_manager;
mod membership_snapshot;
mod membership_table;
mod options;
mod silo_status;
mod table_version;

// Re-export public API
pub use error::{MembershipError, MembershipResult};
pub use in_memory_table::InMemoryMembershipTable;
pub use membership_agent::{MembershipAgent, MembershipAgentBuilder};
pub use membership_entry::MembershipEntry;
pub use membership_manager::{MembershipChangeEvent, MembershipTableManager};
pub use membership_snapshot::{MembershipTableSnapshot, MembershipVersion, SnapshotEntry};
pub use membership_table::{IMembershipTable, MembershipTableData};
pub use options::ClusterMembershipOptions;
pub use silo_status::SiloStatus;
pub use table_version::TableVersion;

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::SiloAddress;
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn test_address(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    /// Integration test: Three silos form a cluster
    #[tokio::test]
    async fn test_three_silo_cluster() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo1 = test_address(11111);
        let silo2 = test_address(22222);
        let silo3 = test_address(33333);

        // Create managers and agents for each silo
        let manager1 = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo1.clone(),
            options.clone(),
        ));
        let agent1 = MembershipAgent::new(manager1.clone(), options.clone());

        let manager2 = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo2.clone(),
            options.clone(),
        ));
        let agent2 = MembershipAgent::new(manager2.clone(), options.clone());

        let manager3 = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo3.clone(),
            options.clone(),
        ));
        let agent3 = MembershipAgent::new(manager3.clone(), options);

        // All silos join the cluster
        agent1.start().await.unwrap();
        agent2.start().await.unwrap();
        agent3.start().await.unwrap();

        // Verify all are active
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();

        assert_eq!(snapshot.active_silo_count(), 3);
        assert!(snapshot.is_silo_active(&silo1));
        assert!(snapshot.is_silo_active(&silo2));
        assert!(snapshot.is_silo_active(&silo3));

        // Get active silos from any manager (after refresh)
        manager2.refresh().await.unwrap();
        let active = manager2.get_active_silos();
        assert_eq!(active.len(), 3);

        // Gracefully stop all
        agent1.stop().await.unwrap();
        agent2.stop().await.unwrap();
        agent3.stop().await.unwrap();

        // Verify all dead
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();
        assert_eq!(snapshot.active_silo_count(), 0);
    }

    /// Integration test: Silo marked dead after missed heartbeats
    #[tokio::test]
    async fn test_failure_detection() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo1 = test_address(11111);
        let silo2 = test_address(22222);

        let manager1 = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo1.clone(),
            options.clone(),
        ));
        let agent1 = MembershipAgent::new(manager1.clone(), options.clone());

        let manager2 = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo2.clone(),
            options.clone(),
        ));
        let agent2 = MembershipAgent::new(manager2.clone(), options);

        // Both join
        agent1.start().await.unwrap();
        agent2.start().await.unwrap();

        // silo1 suspects silo2 (simulating probe failure)
        manager1
            .try_suspect_or_kill(silo2.clone(), silo1.clone())
            .await
            .unwrap();

        // Check vote recorded
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();
        let entry = snapshot.get_entry(&silo2).unwrap();
        assert_eq!(entry.entry.suspect_count(), 1);

        // silo2 is still alive (need 2 votes)
        assert!(snapshot.is_silo_active(&silo2));

        // Add another vote (different voter would be needed in production)
        // For testing, we simulate the threshold being reached
        let third_silo = test_address(33333);
        manager1
            .try_suspect_or_kill(silo2.clone(), third_silo)
            .await
            .unwrap();

        // Now silo2 should be dead
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();
        assert_eq!(snapshot.get_silo_status(&silo2), SiloStatus::Dead);
    }

    /// Integration test: Membership version only increases
    #[tokio::test]
    async fn test_version_monotonicity() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo = test_address(11111);
        let manager = Arc::new(MembershipTableManager::new(
            table.clone(),
            silo.clone(),
            options.clone(),
        ));
        let agent = MembershipAgent::new(manager.clone(), options);

        // Track versions
        let mut versions = Vec::new();

        // Initial version
        let snapshot = manager.refresh().await.unwrap();
        versions.push(snapshot.version);

        // Start agent (creates entry, transitions to active)
        agent.start().await.unwrap();
        let snapshot = manager.get_snapshot();
        versions.push(snapshot.version);

        // Stop agent (transitions through shutdown states)
        agent.stop().await.unwrap();
        let snapshot = manager.refresh().await.unwrap();
        versions.push(snapshot.version);

        // Verify versions are strictly increasing
        for window in versions.windows(2) {
            assert!(window[1] > window[0], "Version should increase: {:?}", window);
        }
    }

    /// Integration test: Concurrent operations don't corrupt state
    #[tokio::test]
    async fn test_concurrent_operations() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        // Create multiple silos that join concurrently
        let mut handles = Vec::new();

        for port in 11111..11121 {
            let table = table.clone();
            let options = options.clone();
            let silo = test_address(port);

            let handle = tokio::spawn(async move {
                let manager = Arc::new(MembershipTableManager::new(
                    table,
                    silo,
                    options.clone(),
                ));
                let agent = MembershipAgent::new(manager.clone(), options);
                agent.start().await.unwrap();
                manager
            });

            handles.push(handle);
        }

        // Wait for all to join
        let managers: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all are active
        let snapshot = managers[0].refresh().await.unwrap();
        assert_eq!(snapshot.active_silo_count(), 10);

        // All managers should see the same state after refresh
        for manager in &managers {
            manager.refresh().await.unwrap();
            assert_eq!(manager.active_silo_count(), 10);
        }
    }
}
