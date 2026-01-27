//! Membership table manager for coordinating cluster membership operations.

use chrono::Utc;
use orleans_core::SiloAddress;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::error::{MembershipError, MembershipResult};
use crate::membership_entry::MembershipEntry;
use crate::membership_snapshot::{MembershipTableSnapshot, MembershipVersion};
use crate::membership_table::IMembershipTable;
use crate::options::ClusterMembershipOptions;
use crate::silo_status::SiloStatus;

/// Event emitted when membership changes.
#[derive(Clone, Debug)]
pub struct MembershipChangeEvent {
    /// The new snapshot after the change.
    pub snapshot: Arc<MembershipTableSnapshot>,
    /// The previous version.
    pub previous_version: MembershipVersion,
}

/// Manages membership table operations for a silo.
///
/// This struct coordinates reading and writing to the membership table,
/// maintains a local snapshot, and notifies listeners of changes.
pub struct MembershipTableManager {
    /// The underlying membership table storage.
    membership_table: Arc<dyn IMembershipTable>,
    /// Address of the local silo.
    local_silo: SiloAddress,
    /// Current membership snapshot.
    snapshot: RwLock<Arc<MembershipTableSnapshot>>,
    /// Channel for broadcasting membership changes.
    change_sender: broadcast::Sender<MembershipChangeEvent>,
    /// Configuration options.
    options: ClusterMembershipOptions,
}

impl MembershipTableManager {
    /// Create a new membership table manager.
    pub fn new(
        membership_table: Arc<dyn IMembershipTable>,
        local_silo: SiloAddress,
        options: ClusterMembershipOptions,
    ) -> Self {
        let (change_sender, _) = broadcast::channel(16);

        Self {
            membership_table,
            local_silo,
            snapshot: RwLock::new(Arc::new(MembershipTableSnapshot::empty())),
            change_sender,
            options,
        }
    }

    /// Get the local silo address.
    pub fn local_silo(&self) -> &SiloAddress {
        &self.local_silo
    }

    /// Get the current membership snapshot.
    pub fn get_snapshot(&self) -> Arc<MembershipTableSnapshot> {
        self.snapshot.read().clone()
    }

    /// Subscribe to membership change events.
    pub fn subscribe(&self) -> broadcast::Receiver<MembershipChangeEvent> {
        self.change_sender.subscribe()
    }

    /// Get the underlying membership table.
    pub fn table(&self) -> &Arc<dyn IMembershipTable> {
        &self.membership_table
    }

    /// Refresh the membership snapshot from storage.
    pub async fn refresh(&self) -> MembershipResult<Arc<MembershipTableSnapshot>> {
        let data = self.membership_table.read_all().await?;
        let new_snapshot = Arc::new(MembershipTableSnapshot::from_table_data(data));

        let previous_version = {
            let mut current = self.snapshot.write();
            let previous_version = current.version;

            if new_snapshot.is_successor_to(&current) {
                debug!(
                    version = new_snapshot.version.value,
                    entries = new_snapshot.len(),
                    active = new_snapshot.active_silo_count(),
                    "Membership snapshot updated"
                );

                *current = new_snapshot.clone();
                previous_version
            } else {
                return Ok(current.clone());
            }
        };

        // Notify listeners outside the lock
        let _ = self.change_sender.send(MembershipChangeEvent {
            snapshot: new_snapshot.clone(),
            previous_version,
        });

        Ok(new_snapshot)
    }

    /// Insert a new entry for the local silo.
    pub async fn insert_self(&self, entry: MembershipEntry) -> MembershipResult<()> {
        let max_retries = 5;
        for attempt in 0..max_retries {
            let snapshot = self.refresh().await?;
            let table_version = crate::table_version::TableVersion::with_version(snapshot.version.value);

            match self.membership_table.insert_row(entry.clone(), table_version).await {
                Ok(true) => {
                    info!(
                        silo = %self.local_silo,
                        status = %entry.status,
                        "Successfully inserted self into membership table"
                    );
                    self.refresh().await?;
                    return Ok(());
                }
                Ok(false) => {
                    // Entry already exists
                    return Err(MembershipError::SiloAlreadyExists(self.local_silo.clone()));
                }
                Err(MembershipError::VersionMismatch { .. }) if attempt < max_retries - 1 => {
                    debug!(
                        attempt = attempt + 1,
                        "Version mismatch on insert, retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(MembershipError::Internal(
            "Max retries exceeded for insert".to_string(),
        ))
    }

    /// Update the local silo's status.
    pub async fn update_status(&self, status: SiloStatus) -> MembershipResult<()> {
        let max_retries = 10;
        for attempt in 0..max_retries {
            let snapshot = self.refresh().await?;

            let Some(entry) = snapshot.get_entry(&self.local_silo) else {
                return Err(MembershipError::SiloNotFound(self.local_silo.clone()));
            };

            if entry.entry.status == status {
                // Already at target status
                return Ok(());
            }

            if !entry.entry.status.can_transition_to(status) {
                return Err(MembershipError::InvalidStatusTransition {
                    from: entry.entry.status,
                    to: status,
                });
            }

            let mut updated_entry = entry.entry.clone();
            updated_entry.status = status;
            updated_entry.i_am_alive_time = Utc::now();

            // Clear suspect times when becoming Active
            if status == SiloStatus::Active {
                updated_entry.clear_suspect_times();
            }

            let table_version = crate::table_version::TableVersion::with_version(snapshot.version.value);

            match self
                .membership_table
                .update_row(updated_entry, &entry.etag, table_version)
                .await
            {
                Ok(true) => {
                    info!(
                        silo = %self.local_silo,
                        from = %entry.entry.status,
                        to = %status,
                        "Status updated"
                    );
                    self.refresh().await?;
                    return Ok(());
                }
                Ok(false) => {
                    debug!(
                        attempt = attempt + 1,
                        "ETag mismatch on status update, retrying"
                    );
                    continue;
                }
                Err(MembershipError::VersionMismatch { .. }) if attempt < max_retries - 1 => {
                    debug!(
                        attempt = attempt + 1,
                        "Version mismatch on status update, retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(MembershipError::Internal(
            "Max retries exceeded for status update".to_string(),
        ))
    }

    /// Update the heartbeat timestamp (I Am Alive).
    pub async fn update_i_am_alive(&self) -> MembershipResult<()> {
        let entry = MembershipEntry {
            silo_address: self.local_silo.clone(),
            i_am_alive_time: Utc::now(),
            ..Default::default()
        };

        self.membership_table.update_i_am_alive(&entry).await?;
        debug!(silo = %self.local_silo, "Heartbeat updated");
        Ok(())
    }

    /// Try to suspect or kill another silo.
    pub async fn try_suspect_or_kill(
        &self,
        target: SiloAddress,
        voter: SiloAddress,
    ) -> MembershipResult<()> {
        let max_retries = 10;
        for attempt in 0..max_retries {
            let snapshot = self.refresh().await?;

            let Some(entry) = snapshot.get_entry(&target) else {
                // Already removed
                return Ok(());
            };

            if entry.entry.status == SiloStatus::Dead {
                // Already dead
                return Ok(());
            }

            let mut updated_entry = entry.entry.clone();

            // Add suspect vote
            updated_entry.add_or_update_suspector(
                voter.clone(),
                Utc::now(),
                self.options.num_votes_for_death_declaration,
            );

            // Check if death threshold reached
            let fresh_votes =
                updated_entry.get_fresh_votes(chrono::Duration::from_std(self.options.death_vote_expiration_timeout).unwrap());

            if fresh_votes.len() >= self.options.num_votes_for_death_declaration {
                info!(
                    target = %target,
                    votes = fresh_votes.len(),
                    threshold = self.options.num_votes_for_death_declaration,
                    "Death threshold reached, marking silo as Dead"
                );
                updated_entry.status = SiloStatus::Dead;
            } else {
                warn!(
                    target = %target,
                    voter = %voter,
                    votes = fresh_votes.len(),
                    threshold = self.options.num_votes_for_death_declaration,
                    "Suspect vote recorded"
                );
            }

            let table_version = crate::table_version::TableVersion::with_version(snapshot.version.value);

            match self
                .membership_table
                .update_row(updated_entry, &entry.etag, table_version)
                .await
            {
                Ok(true) => {
                    self.refresh().await?;
                    return Ok(());
                }
                Ok(false) | Err(MembershipError::VersionMismatch { .. })
                    if attempt < max_retries - 1 =>
                {
                    debug!(
                        attempt = attempt + 1,
                        "Conflict on suspect/kill, retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
                _ => {}
            }
        }

        Err(MembershipError::Internal(
            "Max retries exceeded for suspect/kill".to_string(),
        ))
    }

    /// Get the number of active silos in the cluster.
    pub fn active_silo_count(&self) -> usize {
        self.get_snapshot().active_silo_count()
    }

    /// Check if a silo is active.
    pub fn is_silo_active(&self, address: &SiloAddress) -> bool {
        self.get_snapshot().is_silo_active(address)
    }

    /// Get all active silos.
    pub fn get_active_silos(&self) -> Vec<SiloAddress> {
        self.get_snapshot()
            .get_active_silos()
            .into_iter()
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory_table::InMemoryMembershipTable;
    use std::net::SocketAddr;

    fn test_address(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    async fn create_manager(port: u16) -> MembershipTableManager {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let local_silo = test_address(port);
        let options = ClusterMembershipOptions::development();
        MembershipTableManager::new(table, local_silo, options)
    }

    #[tokio::test]
    async fn test_insert_and_refresh() {
        let manager = create_manager(11111).await;

        // Insert self
        let entry = MembershipEntry::new_joining(manager.local_silo().clone());
        manager.insert_self(entry).await.unwrap();

        // Verify in snapshot
        let snapshot = manager.get_snapshot();
        assert!(snapshot.get_entry(manager.local_silo()).is_some());
        assert_eq!(snapshot.len(), 1);
    }

    #[tokio::test]
    async fn test_update_status() {
        let manager = create_manager(11111).await;

        // Insert as Joining
        let entry = MembershipEntry::new_joining(manager.local_silo().clone());
        manager.insert_self(entry).await.unwrap();

        // Update to Active
        manager.update_status(SiloStatus::Active).await.unwrap();

        // Verify
        let snapshot = manager.get_snapshot();
        assert_eq!(
            snapshot.get_silo_status(manager.local_silo()),
            SiloStatus::Active
        );
    }

    #[tokio::test]
    async fn test_update_i_am_alive() {
        let manager = create_manager(11111).await;

        // Insert
        let entry = MembershipEntry::new_joining(manager.local_silo().clone());
        manager.insert_self(entry).await.unwrap();

        // Get original timestamp
        let snapshot = manager.get_snapshot();
        let original_time = snapshot
            .get_entry(manager.local_silo())
            .unwrap()
            .entry
            .i_am_alive_time;

        // Wait and update heartbeat
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        manager.update_i_am_alive().await.unwrap();

        // Refresh and verify
        manager.refresh().await.unwrap();
        let snapshot = manager.get_snapshot();
        let new_time = snapshot
            .get_entry(manager.local_silo())
            .unwrap()
            .entry
            .i_am_alive_time;

        assert!(new_time > original_time);
    }

    #[tokio::test]
    async fn test_try_suspect_or_kill() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo1 = test_address(11111);
        let silo2 = test_address(22222);

        // Create manager for silo1
        let manager1 = MembershipTableManager::new(table.clone(), silo1.clone(), options.clone());

        // Create manager for silo2
        let manager2 = MembershipTableManager::new(table.clone(), silo2.clone(), options);

        // Both join
        manager1
            .insert_self(MembershipEntry::new_joining(silo1.clone()))
            .await
            .unwrap();
        manager2
            .insert_self(MembershipEntry::new_joining(silo2.clone()))
            .await
            .unwrap();

        // Both become active
        manager1.update_status(SiloStatus::Active).await.unwrap();
        manager2.update_status(SiloStatus::Active).await.unwrap();

        // silo1 suspects silo2
        manager1
            .try_suspect_or_kill(silo2.clone(), silo1.clone())
            .await
            .unwrap();

        // Check vote was recorded
        manager1.refresh().await.unwrap();
        let snapshot = manager1.get_snapshot();
        let entry = snapshot.get_entry(&silo2).unwrap();
        assert_eq!(entry.entry.suspect_count(), 1);
    }

    #[tokio::test]
    async fn test_get_active_silos() {
        let table = Arc::new(InMemoryMembershipTable::new("test-cluster"));
        table.initialize_membership_table(true).await.unwrap();
        let options = ClusterMembershipOptions::development();

        let silo1 = test_address(11111);
        let silo2 = test_address(22222);
        let silo3 = test_address(33333);

        let manager = MembershipTableManager::new(table.clone(), silo1.clone(), options.clone());

        // Insert all silos
        manager
            .insert_self(MembershipEntry::new_joining(silo1.clone()))
            .await
            .unwrap();

        let manager2 = MembershipTableManager::new(table.clone(), silo2.clone(), options.clone());
        manager2
            .insert_self(MembershipEntry::new_joining(silo2.clone()))
            .await
            .unwrap();

        let manager3 = MembershipTableManager::new(table.clone(), silo3.clone(), options);
        manager3
            .insert_self(MembershipEntry::new_joining(silo3.clone()))
            .await
            .unwrap();

        // Make silo1 and silo2 active
        manager.update_status(SiloStatus::Active).await.unwrap();
        manager2.update_status(SiloStatus::Active).await.unwrap();
        // silo3 stays Joining

        // Refresh and check
        manager.refresh().await.unwrap();
        let active = manager.get_active_silos();

        assert_eq!(active.len(), 2);
        assert!(active.contains(&silo1));
        assert!(active.contains(&silo2));
        assert!(!active.contains(&silo3));
    }

    #[tokio::test]
    async fn test_subscribe_to_changes() {
        let manager = create_manager(11111).await;
        let mut receiver = manager.subscribe();

        // Insert triggers change
        let entry = MembershipEntry::new_joining(manager.local_silo().clone());
        manager.insert_self(entry).await.unwrap();

        // Should receive event
        let event = receiver.recv().await.unwrap();
        assert_eq!(event.snapshot.len(), 1);
    }

    #[tokio::test]
    async fn test_invalid_status_transition() {
        let manager = create_manager(11111).await;

        // Insert as Joining
        let entry = MembershipEntry::new_joining(manager.local_silo().clone());
        manager.insert_self(entry).await.unwrap();

        // Update to Active
        manager.update_status(SiloStatus::Active).await.unwrap();

        // Try to go back to Joining (invalid)
        let result = manager.update_status(SiloStatus::Joining).await;
        assert!(matches!(
            result,
            Err(MembershipError::InvalidStatusTransition { .. })
        ));
    }
}
