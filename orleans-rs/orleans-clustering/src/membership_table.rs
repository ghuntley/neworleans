//! Membership table interface for storing cluster state.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_core::SiloAddress;
use serde::{Deserialize, Serialize};

use crate::error::MembershipResult;
use crate::membership_entry::MembershipEntry;
use crate::table_version::TableVersion;

/// Data returned from reading the membership table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MembershipTableData {
    /// All entries in the table with their ETags.
    pub entries: Vec<(MembershipEntry, String)>,
    /// Current table version.
    pub version: TableVersion,
}

impl MembershipTableData {
    /// Create empty membership table data.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            version: TableVersion::new(),
        }
    }

    /// Create membership table data with a specific version.
    pub fn with_version(version: TableVersion) -> Self {
        Self {
            entries: Vec::new(),
            version,
        }
    }

    /// Find an entry by silo address.
    pub fn get(&self, silo_address: &SiloAddress) -> Option<&(MembershipEntry, String)> {
        self.entries
            .iter()
            .find(|(e, _)| e.silo_address == *silo_address)
    }

    /// Get all entries (without ETags).
    pub fn all_entries(&self) -> Vec<&MembershipEntry> {
        self.entries.iter().map(|(e, _)| e).collect()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if there are no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for MembershipTableData {
    fn default() -> Self {
        Self::new()
    }
}

/// Interface for the membership table storage backend.
///
/// Implementations must be thread-safe and support async operations.
/// The table uses optimistic concurrency control via ETags.
#[async_trait]
pub trait IMembershipTable: Send + Sync {
    /// Read a single silo entry.
    ///
    /// Returns the entry and its ETag if found, None if not found.
    async fn read_row(
        &self,
        silo_address: &SiloAddress,
    ) -> MembershipResult<Option<(MembershipEntry, String)>>;

    /// Read all entries in the membership table.
    async fn read_all(&self) -> MembershipResult<MembershipTableData>;

    /// Insert a new entry into the table.
    ///
    /// Returns true if successful, false if the entry already exists
    /// or there was a version conflict.
    async fn insert_row(
        &self,
        entry: MembershipEntry,
        table_version: TableVersion,
    ) -> MembershipResult<bool>;

    /// Update an existing entry with ETag verification.
    ///
    /// Returns true if successful, false if ETag mismatch.
    async fn update_row(
        &self,
        entry: MembershipEntry,
        etag: &str,
        table_version: TableVersion,
    ) -> MembershipResult<bool>;

    /// Update only the I Am Alive timestamp (dirty write, no ETag check).
    ///
    /// This is a fast-path for heartbeats that doesn't require version checks.
    async fn update_i_am_alive(&self, entry: &MembershipEntry) -> MembershipResult<()>;

    /// Delete all entries for a cluster.
    async fn delete_membership_table_entries(&self, cluster_id: &str) -> MembershipResult<()>;

    /// Clean up entries for silos that died before the given time.
    async fn cleanup_defunct_silo_entries(&self, before: DateTime<Utc>) -> MembershipResult<()>;

    /// Initialize the table (create schema if needed).
    async fn initialize_membership_table(&self, try_init_table_version: bool) -> MembershipResult<()> {
        // Default implementation does nothing
        let _ = try_init_table_version;
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

    #[test]
    fn test_membership_table_data_new() {
        let data = MembershipTableData::new();
        assert!(data.is_empty());
        assert_eq!(data.len(), 0);
        assert_eq!(data.version.version, 0);
    }

    #[test]
    fn test_membership_table_data_with_version() {
        let version = TableVersion::with_version(42);
        let data = MembershipTableData::with_version(version.clone());
        assert_eq!(data.version, version);
    }

    #[test]
    fn test_membership_table_data_get() {
        let addr1 = test_address(11111);
        let addr2 = test_address(22222);
        let entry1 = MembershipEntry::new(addr1.clone());
        let entry2 = MembershipEntry::new(addr2.clone());

        let data = MembershipTableData {
            entries: vec![
                (entry1, "etag1".to_string()),
                (entry2, "etag2".to_string()),
            ],
            version: TableVersion::new(),
        };

        assert!(data.get(&addr1).is_some());
        assert!(data.get(&addr2).is_some());
        assert!(data.get(&test_address(33333)).is_none());
    }

    #[test]
    fn test_membership_table_data_all_entries() {
        let addr1 = test_address(11111);
        let addr2 = test_address(22222);
        let entry1 = MembershipEntry::new(addr1.clone());
        let entry2 = MembershipEntry::new(addr2.clone());

        let data = MembershipTableData {
            entries: vec![
                (entry1, "etag1".to_string()),
                (entry2, "etag2".to_string()),
            ],
            version: TableVersion::new(),
        };

        let entries = data.all_entries();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_serialization() {
        let addr = test_address(11111);
        let entry = MembershipEntry::new(addr);
        let data = MembershipTableData {
            entries: vec![(entry, "etag".to_string())],
            version: TableVersion::with_version(5),
        };

        let json = serde_json::to_string(&data).unwrap();
        let deserialized: MembershipTableData = serde_json::from_str(&json).unwrap();

        assert_eq!(data.len(), deserialized.len());
        assert_eq!(data.version, deserialized.version);
    }
}
