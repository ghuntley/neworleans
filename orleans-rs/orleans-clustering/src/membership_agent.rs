//! Membership agent that orchestrates silo join/leave protocol.

use std::sync::Arc;
use tokio::sync::watch;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, instrument, warn};

use crate::error::MembershipResult;
use crate::membership_entry::MembershipEntry;
use crate::membership_manager::MembershipTableManager;
use crate::options::ClusterMembershipOptions;
use crate::silo_status::SiloStatus;

/// Agent that manages the silo's lifecycle in the cluster.
///
/// The agent handles:
/// - Joining the cluster (Created -> Joining -> Active)
/// - Periodic heartbeats (I Am Alive updates)
/// - Graceful shutdown (Active -> ShuttingDown -> Stopping -> Dead)
pub struct MembershipAgent {
    /// The membership table manager.
    manager: Arc<MembershipTableManager>,
    /// Configuration options.
    options: ClusterMembershipOptions,
    /// Shutdown signal.
    shutdown_tx: watch::Sender<bool>,
    /// Shutdown receiver (for background tasks).
    shutdown_rx: watch::Receiver<bool>,
}

impl MembershipAgent {
    /// Create a new membership agent.
    pub fn new(
        manager: Arc<MembershipTableManager>,
        options: ClusterMembershipOptions,
    ) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Self {
            manager,
            options,
            shutdown_tx,
            shutdown_rx,
        }
    }

    /// Get a reference to the membership manager.
    pub fn manager(&self) -> &Arc<MembershipTableManager> {
        &self.manager
    }

    /// Start the membership agent and join the cluster.
    ///
    /// This method:
    /// 1. Inserts the silo as Joining
    /// 2. Validates connectivity to existing silos
    /// 3. Transitions to Active
    /// 4. Starts the heartbeat background task
    #[instrument(skip(self), fields(silo = %self.manager.local_silo()))]
    pub async fn start(&self) -> MembershipResult<()> {
        info!(
            silo = %self.manager.local_silo(),
            "Starting membership agent"
        );

        // Phase 1: Become Joining
        self.become_joining().await?;

        // Phase 2: Validate connectivity (optional for MVP)
        self.validate_initial_connectivity().await?;

        // Phase 3: Become Active
        self.become_active().await?;

        // Phase 4: Start heartbeat task
        self.start_heartbeat_task();

        // Phase 5: Start table refresh task
        self.start_table_refresh_task();

        info!(
            silo = %self.manager.local_silo(),
            "Membership agent started successfully"
        );

        Ok(())
    }

    /// Insert self as Joining.
    async fn become_joining(&self) -> MembershipResult<()> {
        let entry = MembershipEntry::new_joining(self.manager.local_silo().clone());

        info!(
            silo = %self.manager.local_silo(),
            "Attempting to join cluster"
        );

        self.manager.insert_self(entry).await?;

        debug!(
            silo = %self.manager.local_silo(),
            "Inserted as Joining"
        );

        Ok(())
    }

    /// Validate that we can communicate with existing active silos.
    async fn validate_initial_connectivity(&self) -> MembershipResult<()> {
        let snapshot = self.manager.refresh().await?;
        let active_silos = snapshot.get_active_silos();

        if active_silos.is_empty() {
            debug!("No active silos to validate connectivity with");
            return Ok(());
        }

        info!(
            count = active_silos.len(),
            "Validating connectivity with existing silos"
        );

        // For MVP, we just log the active silos rather than probing them
        // Full implementation would use the MessageCenter to ping each silo
        for silo in active_silos {
            debug!(silo = %silo, "Found active silo in cluster");
        }

        Ok(())
    }

    /// Transition to Active status.
    async fn become_active(&self) -> MembershipResult<()> {
        self.manager.update_status(SiloStatus::Active).await?;

        info!(
            silo = %self.manager.local_silo(),
            "Silo is now Active"
        );

        Ok(())
    }

    /// Start the periodic heartbeat task.
    fn start_heartbeat_task(&self) {
        let manager = self.manager.clone();
        let timeout = self.options.i_am_alive_table_publish_timeout;
        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::spawn(async move {
            let mut heartbeat_interval = interval(timeout);

            loop {
                tokio::select! {
                    _ = heartbeat_interval.tick() => {
                        match manager.update_i_am_alive().await {
                            Ok(()) => {
                                debug!(
                                    silo = %manager.local_silo(),
                                    "Heartbeat sent"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    silo = %manager.local_silo(),
                                    error = %e,
                                    "Failed to send heartbeat"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("Heartbeat task shutting down");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Start the periodic table refresh task.
    fn start_table_refresh_task(&self) {
        let manager = self.manager.clone();
        let timeout = self.options.table_refresh_timeout;
        let mut shutdown_rx = self.shutdown_rx.clone();

        tokio::spawn(async move {
            let mut refresh_interval = interval(timeout);

            loop {
                tokio::select! {
                    _ = refresh_interval.tick() => {
                        match manager.refresh().await {
                            Ok(snapshot) => {
                                debug!(
                                    version = snapshot.version.value,
                                    entries = snapshot.len(),
                                    active = snapshot.active_silo_count(),
                                    "Membership table refreshed"
                                );
                            }
                            Err(e) => {
                                warn!(
                                    error = %e,
                                    "Failed to refresh membership table"
                                );
                            }
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            debug!("Table refresh task shutting down");
                            break;
                        }
                    }
                }
            }
        });
    }

    /// Gracefully stop the membership agent and leave the cluster.
    #[instrument(skip(self), fields(silo = %self.manager.local_silo()))]
    pub async fn stop(&self) -> MembershipResult<()> {
        info!(
            silo = %self.manager.local_silo(),
            "Stopping membership agent"
        );

        // Signal background tasks to stop
        let _ = self.shutdown_tx.send(true);

        // Give tasks a moment to stop
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Transition through shutdown states
        if let Err(e) = self.manager.update_status(SiloStatus::ShuttingDown).await {
            warn!(error = %e, "Failed to update status to ShuttingDown");
        }

        if let Err(e) = self.manager.update_status(SiloStatus::Stopping).await {
            warn!(error = %e, "Failed to update status to Stopping");
        }

        if let Err(e) = self.manager.update_status(SiloStatus::Dead).await {
            error!(error = %e, "Failed to update status to Dead");
            return Err(e);
        }

        info!(
            silo = %self.manager.local_silo(),
            "Membership agent stopped"
        );

        Ok(())
    }

    /// Check if the agent is shutting down.
    pub fn is_shutting_down(&self) -> bool {
        *self.shutdown_rx.borrow()
    }
}

/// Builder for creating a MembershipAgent with custom configuration.
pub struct MembershipAgentBuilder {
    manager: Arc<MembershipTableManager>,
    options: ClusterMembershipOptions,
}

impl MembershipAgentBuilder {
    /// Create a new builder with the required manager.
    pub fn new(manager: Arc<MembershipTableManager>) -> Self {
        Self {
            manager,
            options: ClusterMembershipOptions::default(),
        }
    }

    /// Set custom membership options.
    pub fn with_options(mut self, options: ClusterMembershipOptions) -> Self {
        self.options = options;
        self
    }

    /// Set heartbeat interval.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.options.i_am_alive_table_publish_timeout = interval;
        self
    }

    /// Set table refresh interval.
    pub fn with_refresh_interval(mut self, interval: Duration) -> Self {
        self.options.table_refresh_timeout = interval;
        self
    }

    /// Build the membership agent.
    pub fn build(self) -> MembershipAgent {
        MembershipAgent::new(self.manager, self.options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory_table::InMemoryMembershipTable;
    use crate::membership_table::IMembershipTable;
    use orleans_core::SiloAddress;
    use std::net::SocketAddr;

    fn test_address(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    async fn create_agent(port: u16) -> MembershipAgent {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();

        let local_silo = test_address(port);
        let options = ClusterMembershipOptions::development();
        let manager = Arc::new(MembershipTableManager::new(
            table,
            local_silo,
            options.clone(),
        ));

        MembershipAgent::new(manager, options)
    }

    #[tokio::test]
    async fn test_start() {
        let agent = create_agent(11111).await;

        // Start should succeed
        agent.start().await.unwrap();

        // Verify we're active
        let snapshot = agent.manager.get_snapshot();
        assert!(snapshot.is_silo_active(agent.manager.local_silo()));
    }

    #[tokio::test]
    async fn test_start_and_stop() {
        let agent = create_agent(11111).await;

        // Start
        agent.start().await.unwrap();

        // Verify active
        assert!(agent.manager.is_silo_active(agent.manager.local_silo()));

        // Stop
        agent.stop().await.unwrap();

        // Verify dead
        let snapshot = agent.manager.get_snapshot();
        assert_eq!(
            snapshot.get_silo_status(agent.manager.local_silo()),
            SiloStatus::Dead
        );
    }

    #[tokio::test]
    async fn test_multiple_silos_join() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo1 = test_address(11111);
        let silo2 = test_address(22222);
        let silo3 = test_address(33333);

        // Create agents
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

        // All join
        agent1.start().await.unwrap();
        agent2.start().await.unwrap();
        agent3.start().await.unwrap();

        // Refresh and check
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();

        assert_eq!(snapshot.active_silo_count(), 3);
        assert!(snapshot.is_silo_active(&silo1));
        assert!(snapshot.is_silo_active(&silo2));
        assert!(snapshot.is_silo_active(&silo3));
    }

    #[tokio::test]
    async fn test_silo_leaves_cluster() {
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

        // Verify both active
        manager1.refresh().await.unwrap();
        assert_eq!(manager1.active_silo_count(), 2);

        // silo2 leaves
        agent2.stop().await.unwrap();

        // Verify only silo1 active
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();
        assert_eq!(snapshot.active_silo_count(), 1);
        assert!(snapshot.is_silo_active(&silo1));
        assert!(!snapshot.is_silo_active(&silo2));
        assert_eq!(snapshot.get_silo_status(&silo2), SiloStatus::Dead);
    }

    #[tokio::test]
    async fn test_builder() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();

        let local_silo = test_address(11111);
        let options = ClusterMembershipOptions::development();
        let manager = Arc::new(MembershipTableManager::new(
            table,
            local_silo,
            options,
        ));

        let agent = MembershipAgentBuilder::new(manager)
            .with_heartbeat_interval(Duration::from_secs(1))
            .with_refresh_interval(Duration::from_secs(2))
            .build();

        agent.start().await.unwrap();

        assert!(agent.manager.is_silo_active(agent.manager.local_silo()));
    }

    #[tokio::test]
    async fn test_heartbeat_updates() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();

        let local_silo = test_address(11111);
        let mut options = ClusterMembershipOptions::development();
        options.i_am_alive_table_publish_timeout = Duration::from_millis(50);

        let manager = Arc::new(MembershipTableManager::new(
            table,
            local_silo.clone(),
            options.clone(),
        ));
        let agent = MembershipAgent::new(manager.clone(), options);

        agent.start().await.unwrap();

        // Get initial heartbeat time
        let snapshot = manager.get_snapshot();
        let initial_time = snapshot
            .get_entry(&local_silo)
            .unwrap()
            .entry
            .i_am_alive_time;

        // Wait for a few heartbeats
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Refresh and check timestamp updated
        manager.refresh().await.unwrap();
        let snapshot = manager.get_snapshot();
        let new_time = snapshot
            .get_entry(&local_silo)
            .unwrap()
            .entry
            .i_am_alive_time;

        assert!(new_time > initial_time);
    }

    #[tokio::test]
    async fn test_is_shutting_down() {
        let agent = create_agent(11111).await;

        assert!(!agent.is_shutting_down());

        agent.start().await.unwrap();
        assert!(!agent.is_shutting_down());

        agent.stop().await.unwrap();
        assert!(agent.is_shutting_down());
    }
}
