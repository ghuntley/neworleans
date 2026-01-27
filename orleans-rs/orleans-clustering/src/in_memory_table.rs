//! In-memory implementation of the membership table for testing and development.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_core::SiloAddress;
use parking_lot::RwLock;
use std::collections::HashMap;
use uuid::Uuid;

use crate::error::{MembershipError, MembershipResult};
use crate::membership_entry::MembershipEntry;
use crate::membership_table::{IMembershipTable, MembershipTableData};
use crate::silo_status::SiloStatus;
use crate::table_version::TableVersion;

/// In-memory implementation of the membership table.
///
/// This is suitable for testing and single-process scenarios.
/// For multi-process clusters, use a shared backend like Redis or SQL.
#[derive(Debug)]
pub struct InMemoryMembershipTable {
    /// Entries keyed by silo address, with their ETags.
    entries: RwLock<HashMap<SiloAddress, (MembershipEntry, String)>>,
    /// Current table version.
    version: RwLock<TableVersion>,
    /// Cluster identifier (for multi-cluster scenarios).
    cluster_id: String,
}

impl InMemoryMembershipTable {
    /// Create a new in-memory membership table.
    pub fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            version: RwLock::new(TableVersion::new()),
            cluster_id: cluster_id.into(),
        }
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Generate a new unique ETag.
    fn generate_etag() -> String {
        Uuid::new_v4().to_string()
    }

    /// Get the current number of entries.
    pub fn entry_count(&self) -> usize {
        self.entries.read().len()
    }

    /// Get the current table version.
    pub fn current_version(&self) -> TableVersion {
        self.version.read().clone()
    }
}

#[async_trait]
impl IMembershipTable for InMemoryMembershipTable {
    async fn read_row(
        &self,
        silo_address: &SiloAddress,
    ) -> MembershipResult<Option<(MembershipEntry, String)>> {
        let entries = self.entries.read();
        Ok(entries.get(silo_address).cloned())
    }

    async fn read_all(&self) -> MembershipResult<MembershipTableData> {
        let entries = self.entries.read();
        let version = self.version.read().clone();

        Ok(MembershipTableData {
            entries: entries.values().cloned().collect(),
            version,
        })
    }

    async fn insert_row(
        &self,
        entry: MembershipEntry,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        let mut entries = self.entries.write();
        let mut version = self.version.write();

        // Check if entry already exists
        if entries.contains_key(&entry.silo_address) {
            return Ok(false);
        }

        // Check version match
        if table_version.version != version.version {
            return Err(MembershipError::VersionMismatch {
                expected: table_version.version,
                actual: version.version,
            });
        }

        // Insert with new ETag
        let etag = Self::generate_etag();
        entries.insert(entry.silo_address.clone(), (entry, etag.clone()));

        // Increment version
        *version = TableVersion::with_etag(version.version + 1, Self::generate_etag());

        Ok(true)
    }

    async fn update_row(
        &self,
        entry: MembershipEntry,
        etag: &str,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        let mut entries = self.entries.write();
        let mut version = self.version.write();

        // Check if entry exists
        let Some((existing, existing_etag)) = entries.get(&entry.silo_address) else {
            return Err(MembershipError::SiloNotFound(entry.silo_address.clone()));
        };

        // Check ETag match
        if existing_etag != etag {
            return Ok(false);
        }

        // Check version match
        if table_version.version != version.version {
            return Err(MembershipError::VersionMismatch {
                expected: table_version.version,
                actual: version.version,
            });
        }

        // Validate state transition
        if !existing.status.can_transition_to(entry.status) {
            return Err(MembershipError::InvalidStatusTransition {
                from: existing.status,
                to: entry.status,
            });
        }

        // Update with new ETag
        let new_etag = Self::generate_etag();
        entries.insert(entry.silo_address.clone(), (entry, new_etag));

        // Increment version
        *version = TableVersion::with_etag(version.version + 1, Self::generate_etag());

        Ok(true)
    }

    async fn update_i_am_alive(&self, entry: &MembershipEntry) -> MembershipResult<()> {
        let mut entries = self.entries.write();

        if let Some((existing, _)) = entries.get_mut(&entry.silo_address) {
            existing.i_am_alive_time = entry.i_am_alive_time;
            Ok(())
        } else {
            Err(MembershipError::SiloNotFound(entry.silo_address.clone()))
        }
    }

    async fn delete_membership_table_entries(&self, _cluster_id: &str) -> MembershipResult<()> {
        let mut entries = self.entries.write();
        let mut version = self.version.write();

        entries.clear();
        *version = TableVersion::new();

        Ok(())
    }

    async fn cleanup_defunct_silo_entries(&self, before: DateTime<Utc>) -> MembershipResult<()> {
        let mut entries = self.entries.write();

        entries.retain(|_, (entry, _)| {
            // Keep if not dead or if died after the threshold
            entry.status != SiloStatus::Dead || entry.i_am_alive_time >= before
        });

        Ok(())
    }

    async fn initialize_membership_table(&self, try_init_table_version: bool) -> MembershipResult<()> {
        if try_init_table_version {
            let mut version = self.version.write();
            if version.version == 0 {
                *version = TableVersion::with_etag(0, Self::generate_etag());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn test_address(port: u16) -> SiloAddress {
        let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
        SiloAddress::new(addr, 1)
    }

    #[tokio::test]
    async fn test_insert_and_read() {
        let table = InMemoryMembershipTable::new("test-cluster");
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        // Initialize table
        table.initialize_membership_table(true).await.unwrap();

        // Insert
        let version = table.current_version();
        let result = table.insert_row(entry.clone(), version).await.unwrap();
        assert!(result);

        // Read back
        let read_result = table.read_row(&addr).await.unwrap();
        assert!(read_result.is_some());
        let (read_entry, _) = read_result.unwrap();
        assert_eq!(read_entry.silo_address, addr);
        assert_eq!(read_entry.status, SiloStatus::Joining);
    }

    #[tokio::test]
    async fn test_insert_duplicate() {
        let table = InMemoryMembershipTable::new("test-cluster");
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        table.initialize_membership_table(true).await.unwrap();

        // First insert should succeed
        let version = table.current_version();
        let result = table.insert_row(entry.clone(), version).await.unwrap();
        assert!(result);

        // Second insert should fail
        let version = table.current_version();
        let result = table.insert_row(entry, version).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_update_row() {
        let table = InMemoryMembershipTable::new("test-cluster");
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        table.initialize_membership_table(true).await.unwrap();

        // Insert
        let version = table.current_version();
        table.insert_row(entry, version).await.unwrap();

        // Read to get ETag
        let (mut entry, etag) = table.read_row(&addr).await.unwrap().unwrap();

        // Update to Active
        entry.status = SiloStatus::Active;
        let version = table.current_version();
        let result = table.update_row(entry, &etag, version).await.unwrap();
        assert!(result);

        // Verify update
        let (updated, _) = table.read_row(&addr).await.unwrap().unwrap();
        assert_eq!(updated.status, SiloStatus::Active);
    }

    #[tokio::test]
    async fn test_update_wrong_etag() {
        let table = InMemoryMembershipTable::new("test-cluster");
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        table.initialize_membership_table(true).await.unwrap();

        // Insert
        let version = table.current_version();
        table.insert_row(entry, version).await.unwrap();

        // Try update with wrong ETag
        let (mut entry, _) = table.read_row(&addr).await.unwrap().unwrap();
        entry.status = SiloStatus::Active;
        let version = table.current_version();
        let result = table.update_row(entry, "wrong-etag", version).await.unwrap();
        assert!(!result);
    }

    #[tokio::test]
    async fn test_update_i_am_alive() {
        let table = InMemoryMembershipTable::new("test-cluster");
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        table.initialize_membership_table(true).await.unwrap();

        // Insert
        let version = table.current_version();
        table.insert_row(entry, version).await.unwrap();

        // Get original timestamp
        let (original, _) = table.read_row(&addr).await.unwrap().unwrap();
        let original_time = original.i_am_alive_time;

        // Wait a bit and update heartbeat
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let mut update_entry = MembershipEntry::new(addr.clone());
        update_entry.i_am_alive_time = Utc::now();
        table.update_i_am_alive(&update_entry).await.unwrap();

        // Verify timestamp updated
        let (updated, _) = table.read_row(&addr).await.unwrap().unwrap();
        assert!(updated.i_am_alive_time > original_time);
    }

    #[tokio::test]
    async fn test_read_all() {
        let table = InMemoryMembershipTable::new("test-cluster");
        table.initialize_membership_table(true).await.unwrap();

        // Insert multiple entries
        for port in [11111, 22222, 33333] {
            let addr = test_address(port);
            let entry = MembershipEntry::new_joining(addr);
            let version = table.current_version();
            table.insert_row(entry, version).await.unwrap();
        }

        // Read all
        let data = table.read_all().await.unwrap();
        assert_eq!(data.len(), 3);
    }

    #[tokio::test]
    async fn test_delete_all() {
        let table = InMemoryMembershipTable::new("test-cluster");
        table.initialize_membership_table(true).await.unwrap();

        // Insert entries
        for port in [11111, 22222] {
            let addr = test_address(port);
            let entry = MembershipEntry::new_joining(addr);
            let version = table.current_version();
            table.insert_row(entry, version).await.unwrap();
        }

        assert_eq!(table.entry_count(), 2);

        // Delete all
        table
            .delete_membership_table_entries("test-cluster")
            .await
            .unwrap();

        assert_eq!(table.entry_count(), 0);
    }

    #[tokio::test]
    async fn test_cleanup_defunct() {
        let table = InMemoryMembershipTable::new("test-cluster");
        table.initialize_membership_table(true).await.unwrap();

        // Insert an entry and mark it dead
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        let version = table.current_version();
        table.insert_row(entry, version).await.unwrap();

        let (mut entry, etag) = table.read_row(&addr).await.unwrap().unwrap();
        entry.status = SiloStatus::Dead;
        let version = table.current_version();
        table.update_row(entry, &etag, version).await.unwrap();

        // Insert another active entry
        let addr2 = test_address(22222);
        let entry2 = MembershipEntry::new_joining(addr2.clone());
        let version = table.current_version();
        table.insert_row(entry2, version).await.unwrap();

        // Cleanup - the dead entry should be removed
        table
            .cleanup_defunct_silo_entries(Utc::now() + chrono::Duration::seconds(1))
            .await
            .unwrap();

        assert!(table.read_row(&addr).await.unwrap().is_none());
        assert!(table.read_row(&addr2).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_version_increment() {
        let table = InMemoryMembershipTable::new("test-cluster");
        table.initialize_membership_table(true).await.unwrap();

        let v1 = table.current_version();

        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());
        table.insert_row(entry, v1.clone()).await.unwrap();

        let v2 = table.current_version();
        assert!(v2.version > v1.version);

        let (mut entry, etag) = table.read_row(&addr).await.unwrap().unwrap();
        entry.status = SiloStatus::Active;
        table.update_row(entry, &etag, v2.clone()).await.unwrap();

        let v3 = table.current_version();
        assert!(v3.version > v2.version);
    }

    #[tokio::test]
    async fn test_invalid_status_transition() {
        let table = InMemoryMembershipTable::new("test-cluster");
        table.initialize_membership_table(true).await.unwrap();

        // Insert as Active
        let addr = test_address(11111);
        let mut entry = MembershipEntry::new(addr.clone());
        entry.status = SiloStatus::Joining;
        let version = table.current_version();
        table.insert_row(entry, version).await.unwrap();

        // Transition to Active
        let (mut entry, etag) = table.read_row(&addr).await.unwrap().unwrap();
        entry.status = SiloStatus::Active;
        let version = table.current_version();
        table.update_row(entry, &etag, version).await.unwrap();

        // Try invalid transition back to Joining (should fail)
        let (mut entry, etag) = table.read_row(&addr).await.unwrap().unwrap();
        entry.status = SiloStatus::Joining;
        let version = table.current_version();
        let result = table.update_row(entry, &etag, version).await;

        assert!(matches!(
            result,
            Err(MembershipError::InvalidStatusTransition { .. })
        ));
    }
}
