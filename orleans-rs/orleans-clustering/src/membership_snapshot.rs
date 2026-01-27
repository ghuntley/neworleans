//! Membership snapshot representing a point-in-time view of cluster state.

use chrono::Utc;
use orleans_core::SiloAddress;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::membership_entry::MembershipEntry;
use crate::membership_table::MembershipTableData;
use crate::silo_status::SiloStatus;

/// A monotonically increasing version number for membership snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MembershipVersion {
    pub value: i64,
}

impl MembershipVersion {
    /// Create a new membership version.
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    /// Create a zero version.
    pub fn zero() -> Self {
        Self { value: 0 }
    }

    /// Get the next version.
    pub fn next(&self) -> Self {
        Self {
            value: self.value + 1,
        }
    }
}

impl Default for MembershipVersion {
    fn default() -> Self {
        Self::zero()
    }
}

impl std::fmt::Display for MembershipVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.value)
    }
}

/// Entry in the membership snapshot with its ETag.
#[derive(Clone, Debug)]
pub struct SnapshotEntry {
    /// The membership entry.
    pub entry: MembershipEntry,
    /// ETag for optimistic concurrency.
    pub etag: String,
}

impl SnapshotEntry {
    /// Create a new snapshot entry.
    pub fn new(entry: MembershipEntry, etag: String) -> Self {
        Self { entry, etag }
    }
}

/// A point-in-time snapshot of the cluster membership.
///
/// This is an immutable view that can be safely shared across threads.
#[derive(Clone, Debug)]
pub struct MembershipTableSnapshot {
    /// Version of this snapshot.
    pub version: MembershipVersion,
    /// All entries indexed by silo address.
    pub entries: HashMap<SiloAddress, SnapshotEntry>,
}

impl MembershipTableSnapshot {
    /// Create an empty snapshot.
    pub fn empty() -> Self {
        Self {
            version: MembershipVersion::zero(),
            entries: HashMap::new(),
        }
    }

    /// Create a snapshot from membership table data.
    pub fn from_table_data(data: MembershipTableData) -> Self {
        let entries = data
            .entries
            .into_iter()
            .map(|(entry, etag)| (entry.silo_address.clone(), SnapshotEntry::new(entry, etag)))
            .collect();

        Self {
            version: MembershipVersion::new(data.version.version),
            entries,
        }
    }

    /// Get an entry by silo address.
    pub fn get_entry(&self, address: &SiloAddress) -> Option<&SnapshotEntry> {
        self.entries.get(address)
    }

    /// Get the status of a silo.
    pub fn get_silo_status(&self, address: &SiloAddress) -> SiloStatus {
        self.entries
            .get(address)
            .map(|e| e.entry.status)
            .unwrap_or(SiloStatus::Dead)
    }

    /// Get all active silos.
    pub fn get_active_silos(&self) -> Vec<&SiloAddress> {
        self.entries
            .iter()
            .filter(|(_, e)| e.entry.status == SiloStatus::Active)
            .map(|(a, _)| a)
            .collect()
    }

    /// Get all silos with a specific status.
    pub fn get_silos_with_status(&self, status: SiloStatus) -> Vec<&SiloAddress> {
        self.entries
            .iter()
            .filter(|(_, e)| e.entry.status == status)
            .map(|(a, _)| a)
            .collect()
    }

    /// Get all alive silos (Joining, Active, or ShuttingDown).
    pub fn get_alive_silos(&self) -> Vec<&SiloAddress> {
        self.entries
            .iter()
            .filter(|(_, e)| e.entry.status.is_alive())
            .map(|(a, _)| a)
            .collect()
    }

    /// Check if a silo is active.
    pub fn is_silo_active(&self, address: &SiloAddress) -> bool {
        self.get_silo_status(address) == SiloStatus::Active
    }

    /// Check if a silo is alive (participating in cluster).
    pub fn is_silo_alive(&self, address: &SiloAddress) -> bool {
        self.entries
            .get(address)
            .map(|e| e.entry.status.is_alive())
            .unwrap_or(false)
    }

    /// Check if this snapshot is a successor to another.
    ///
    /// A snapshot is a successor if it has a higher version, or if it has the same
    /// version but fresher heartbeat timestamps.
    pub fn is_successor_to(&self, other: &Self) -> bool {
        if self.version > other.version {
            return true;
        }
        if self.version < other.version {
            return false;
        }

        // Same version: check if any heartbeat is fresher
        self.entries.iter().any(|(addr, entry)| {
            other
                .entries
                .get(addr)
                .map(|other_entry| {
                    entry.entry.effective_i_am_alive_time()
                        > other_entry.entry.effective_i_am_alive_time()
                })
                .unwrap_or(true)
        })
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if snapshot is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of active silos.
    pub fn active_silo_count(&self) -> usize {
        self.entries
            .values()
            .filter(|e| e.entry.status == SiloStatus::Active)
            .count()
    }

    /// Get all entries.
    pub fn all_entries(&self) -> impl Iterator<Item = (&SiloAddress, &SnapshotEntry)> {
        self.entries.iter()
    }

    /// Get all entries as a vector.
    pub fn entries_vec(&self) -> Vec<(&SiloAddress, &MembershipEntry)> {
        self.entries.iter().map(|(a, e)| (a, &e.entry)).collect()
    }

    /// Find silos that might be defunct (no recent heartbeat).
    pub fn find_stale_silos(
        &self,
        heartbeat_threshold: chrono::Duration,
    ) -> Vec<&SiloAddress> {
        let threshold = Utc::now() - heartbeat_threshold;
        self.entries
            .iter()
            .filter(|(_, e)| {
                e.entry.status.is_alive() && e.entry.effective_i_am_alive_time() < threshold
            })
            .map(|(a, _)| a)
            .collect()
    }
}

impl Default for MembershipTableSnapshot {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<MembershipTableData> for MembershipTableSnapshot {
    fn from(data: MembershipTableData) -> Self {
        Self::from_table_data(data)
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

    fn create_entry(port: u16, status: SiloStatus) -> (MembershipEntry, String) {
        let mut entry = MembershipEntry::new(test_address(port));
        entry.status = status;
        (entry, format!("etag-{}", port))
    }

    #[test]
    fn test_membership_version() {
        let v1 = MembershipVersion::new(5);
        let v2 = v1.next();

        assert_eq!(v1.value, 5);
        assert_eq!(v2.value, 6);
        assert!(v2 > v1);
        assert_eq!(format!("{}", v1), "v5");
    }

    #[test]
    fn test_empty_snapshot() {
        let snapshot = MembershipTableSnapshot::empty();
        assert!(snapshot.is_empty());
        assert_eq!(snapshot.version, MembershipVersion::zero());
        assert_eq!(snapshot.active_silo_count(), 0);
    }

    #[test]
    fn test_from_table_data() {
        let data = MembershipTableData {
            entries: vec![
                create_entry(11111, SiloStatus::Active),
                create_entry(22222, SiloStatus::Active),
                create_entry(33333, SiloStatus::Joining),
            ],
            version: crate::table_version::TableVersion::with_version(5),
        };

        let snapshot = MembershipTableSnapshot::from_table_data(data);

        assert_eq!(snapshot.version.value, 5);
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot.active_silo_count(), 2);
    }

    #[test]
    fn test_get_active_silos() {
        let data = MembershipTableData {
            entries: vec![
                create_entry(11111, SiloStatus::Active),
                create_entry(22222, SiloStatus::Active),
                create_entry(33333, SiloStatus::Joining),
                create_entry(44444, SiloStatus::Dead),
            ],
            version: crate::table_version::TableVersion::with_version(1),
        };

        let snapshot = MembershipTableSnapshot::from_table_data(data);
        let active = snapshot.get_active_silos();

        assert_eq!(active.len(), 2);
        assert!(active.contains(&&test_address(11111)));
        assert!(active.contains(&&test_address(22222)));
    }

    #[test]
    fn test_get_alive_silos() {
        let data = MembershipTableData {
            entries: vec![
                create_entry(11111, SiloStatus::Active),
                create_entry(22222, SiloStatus::Joining),
                create_entry(33333, SiloStatus::ShuttingDown),
                create_entry(44444, SiloStatus::Dead),
            ],
            version: crate::table_version::TableVersion::with_version(1),
        };

        let snapshot = MembershipTableSnapshot::from_table_data(data);
        let alive = snapshot.get_alive_silos();

        assert_eq!(alive.len(), 3);
        assert!(!alive.contains(&&test_address(44444)));
    }

    #[test]
    fn test_get_silo_status() {
        let data = MembershipTableData {
            entries: vec![create_entry(11111, SiloStatus::Active)],
            version: crate::table_version::TableVersion::with_version(1),
        };

        let snapshot = MembershipTableSnapshot::from_table_data(data);

        assert_eq!(
            snapshot.get_silo_status(&test_address(11111)),
            SiloStatus::Active
        );
        // Unknown silo returns Dead
        assert_eq!(
            snapshot.get_silo_status(&test_address(55555)),
            SiloStatus::Dead
        );
    }

    #[test]
    fn test_is_silo_active() {
        let data = MembershipTableData {
            entries: vec![
                create_entry(11111, SiloStatus::Active),
                create_entry(22222, SiloStatus::Joining),
            ],
            version: crate::table_version::TableVersion::with_version(1),
        };

        let snapshot = MembershipTableSnapshot::from_table_data(data);

        assert!(snapshot.is_silo_active(&test_address(11111)));
        assert!(!snapshot.is_silo_active(&test_address(22222)));
        assert!(!snapshot.is_silo_active(&test_address(55555)));
    }

    #[test]
    fn test_is_successor_to_version() {
        let s1 = MembershipTableSnapshot {
            version: MembershipVersion::new(5),
            entries: HashMap::new(),
        };

        let s2 = MembershipTableSnapshot {
            version: MembershipVersion::new(10),
            entries: HashMap::new(),
        };

        assert!(s2.is_successor_to(&s1));
        assert!(!s1.is_successor_to(&s2));
    }

    #[test]
    fn test_is_successor_to_same_version_fresher_heartbeat() {
        let addr = test_address(11111);

        let mut entry1 = MembershipEntry::new(addr.clone());
        entry1.i_am_alive_time = Utc::now() - chrono::Duration::seconds(10);

        let mut entry2 = MembershipEntry::new(addr.clone());
        entry2.i_am_alive_time = Utc::now();

        let s1 = MembershipTableSnapshot {
            version: MembershipVersion::new(5),
            entries: [(addr.clone(), SnapshotEntry::new(entry1, "e1".to_string()))]
                .into_iter()
                .collect(),
        };

        let s2 = MembershipTableSnapshot {
            version: MembershipVersion::new(5),
            entries: [(addr, SnapshotEntry::new(entry2, "e2".to_string()))]
                .into_iter()
                .collect(),
        };

        assert!(s2.is_successor_to(&s1));
    }

    #[test]
    fn test_find_stale_silos() {
        let addr1 = test_address(11111);
        let addr2 = test_address(22222);

        let mut entry1 = MembershipEntry::new(addr1.clone());
        entry1.status = SiloStatus::Active;
        // Both start_time and i_am_alive_time must be old for effective time to be old
        entry1.start_time = Utc::now() - chrono::Duration::minutes(15);
        entry1.i_am_alive_time = Utc::now() - chrono::Duration::minutes(10);

        let mut entry2 = MembershipEntry::new(addr2.clone());
        entry2.status = SiloStatus::Active;
        entry2.i_am_alive_time = Utc::now();

        let snapshot = MembershipTableSnapshot {
            version: MembershipVersion::new(1),
            entries: [
                (addr1.clone(), SnapshotEntry::new(entry1, "e1".to_string())),
                (addr2, SnapshotEntry::new(entry2, "e2".to_string())),
            ]
            .into_iter()
            .collect(),
        };

        let stale = snapshot.find_stale_silos(chrono::Duration::minutes(5));
        assert_eq!(stale.len(), 1);
        assert_eq!(*stale[0], addr1);
    }
}
