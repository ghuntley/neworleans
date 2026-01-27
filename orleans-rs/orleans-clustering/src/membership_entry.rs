//! Membership entry representing a silo's state in the cluster.

use chrono::{DateTime, Duration, Utc};
use orleans_core::SiloAddress;
use serde::{Deserialize, Serialize};

use crate::silo_status::SiloStatus;

/// Represents a silo's entry in the membership table.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MembershipEntry {
    /// The silo's unique address (endpoint + generation).
    pub silo_address: SiloAddress,
    /// Current status of the silo.
    pub status: SiloStatus,
    /// Human-readable name for the silo.
    pub silo_name: String,
    /// Hostname of the machine running the silo.
    pub host_name: String,
    /// Optional proxy port for client connections.
    pub proxy_port: Option<u16>,
    /// Optional role name (for deployment scenarios).
    pub role_name: Option<String>,
    /// Update zone for fault tolerance.
    pub update_zone: u32,
    /// Fault zone for placement decisions.
    pub fault_zone: u32,
    /// When the silo started.
    pub start_time: DateTime<Utc>,
    /// Last heartbeat timestamp.
    pub i_am_alive_time: DateTime<Utc>,
    /// List of silos that suspect this silo is dead.
    pub suspect_times: Vec<(SiloAddress, DateTime<Utc>)>,
}

impl MembershipEntry {
    /// Create a new membership entry for a silo.
    pub fn new(silo_address: SiloAddress) -> Self {
        let now = Utc::now();
        Self {
            silo_address,
            status: SiloStatus::Created,
            silo_name: String::new(),
            host_name: String::new(),
            proxy_port: None,
            role_name: None,
            update_zone: 0,
            fault_zone: 0,
            start_time: now,
            i_am_alive_time: now,
            suspect_times: Vec::new(),
        }
    }

    /// Create a new membership entry with joining status.
    pub fn new_joining(silo_address: SiloAddress) -> Self {
        let mut entry = Self::new(silo_address);
        entry.status = SiloStatus::Joining;
        entry
    }

    /// Returns the effective "I am alive" time, which is the maximum of
    /// start_time and i_am_alive_time.
    pub fn effective_i_am_alive_time(&self) -> DateTime<Utc> {
        std::cmp::max(self.start_time, self.i_am_alive_time)
    }

    /// Add or update a suspect vote from another silo.
    pub fn add_or_update_suspector(
        &mut self,
        voter: SiloAddress,
        vote_time: DateTime<Utc>,
        max_votes: usize,
    ) {
        // Check if voter already voted
        if let Some(existing) = self.suspect_times.iter_mut().find(|(s, _)| *s == voter) {
            existing.1 = vote_time;
            return;
        }

        // Add new vote if under limit
        if self.suspect_times.len() < max_votes {
            self.suspect_times.push((voter, vote_time));
            return;
        }

        // Replace oldest vote if new vote is fresher
        let oldest = self
            .suspect_times
            .iter()
            .enumerate()
            .min_by_key(|(_, (_, t))| t)
            .map(|(i, _)| i);

        if let Some(oldest_idx) = oldest {
            if vote_time > self.suspect_times[oldest_idx].1 {
                self.suspect_times[oldest_idx] = (voter, vote_time);
            }
        }
    }

    /// Get all votes that are still fresh (within expiration timeout).
    pub fn get_fresh_votes(&self, expiration: Duration) -> Vec<&(SiloAddress, DateTime<Utc>)> {
        let threshold = Utc::now() - expiration;
        self.suspect_times
            .iter()
            .filter(|(_, t)| *t > threshold)
            .collect()
    }

    /// Clear all suspect votes.
    pub fn clear_suspect_times(&mut self) {
        self.suspect_times.clear();
    }

    /// Update the heartbeat timestamp.
    pub fn update_i_am_alive(&mut self) {
        self.i_am_alive_time = Utc::now();
    }

    /// Check if this entry matches another by silo address.
    pub fn matches(&self, other: &SiloAddress) -> bool {
        self.silo_address == *other
    }

    /// Returns true if this silo has been suspected by at least one other silo.
    pub fn is_suspected(&self) -> bool {
        !self.suspect_times.is_empty()
    }

    /// Returns the number of current suspect votes.
    pub fn suspect_count(&self) -> usize {
        self.suspect_times.len()
    }
}

impl Default for MembershipEntry {
    fn default() -> Self {
        Self::new(SiloAddress::new(
            "127.0.0.1:11111".parse().unwrap(),
            0,
        ))
    }
}

impl PartialEq for MembershipEntry {
    fn eq(&self, other: &Self) -> bool {
        self.silo_address == other.silo_address
    }
}

impl Eq for MembershipEntry {}

impl std::hash::Hash for MembershipEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.silo_address.hash(state);
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
    fn test_new() {
        let addr = test_address(11111);
        let entry = MembershipEntry::new(addr.clone());

        assert_eq!(entry.silo_address, addr);
        assert_eq!(entry.status, SiloStatus::Created);
        assert!(entry.suspect_times.is_empty());
    }

    #[test]
    fn test_new_joining() {
        let addr = test_address(11111);
        let entry = MembershipEntry::new_joining(addr.clone());

        assert_eq!(entry.silo_address, addr);
        assert_eq!(entry.status, SiloStatus::Joining);
    }

    #[test]
    fn test_effective_i_am_alive_time() {
        let addr = test_address(11111);
        let mut entry = MembershipEntry::new(addr);

        // Initially, both times should be close
        let effective = entry.effective_i_am_alive_time();
        assert!(effective >= entry.start_time);
        assert!(effective >= entry.i_am_alive_time);

        // After updating i_am_alive_time
        std::thread::sleep(std::time::Duration::from_millis(10));
        entry.update_i_am_alive();
        let new_effective = entry.effective_i_am_alive_time();
        assert!(new_effective >= effective);
    }

    #[test]
    fn test_add_or_update_suspector() {
        let addr = test_address(11111);
        let voter1 = test_address(22222);
        let voter2 = test_address(33333);
        let mut entry = MembershipEntry::new(addr);

        // Add first vote
        entry.add_or_update_suspector(voter1.clone(), Utc::now(), 3);
        assert_eq!(entry.suspect_count(), 1);

        // Add second vote
        entry.add_or_update_suspector(voter2.clone(), Utc::now(), 3);
        assert_eq!(entry.suspect_count(), 2);

        // Update existing vote
        entry.add_or_update_suspector(voter1.clone(), Utc::now(), 3);
        assert_eq!(entry.suspect_count(), 2); // Still 2, not 3
    }

    #[test]
    fn test_add_suspector_max_votes() {
        let addr = test_address(11111);
        let mut entry = MembershipEntry::new(addr);
        let max_votes = 2;

        let voter1 = test_address(22222);
        let voter2 = test_address(33333);
        let voter3 = test_address(44444);

        entry.add_or_update_suspector(voter1.clone(), Utc::now() - Duration::seconds(10), max_votes);
        entry.add_or_update_suspector(voter2.clone(), Utc::now() - Duration::seconds(5), max_votes);

        // Third vote should replace oldest if fresher
        entry.add_or_update_suspector(voter3.clone(), Utc::now(), max_votes);

        assert_eq!(entry.suspect_count(), 2);
        // voter3 should have replaced voter1 (oldest)
        assert!(entry
            .suspect_times
            .iter()
            .any(|(s, _)| *s == voter3));
    }

    #[test]
    fn test_get_fresh_votes() {
        let addr = test_address(11111);
        let voter1 = test_address(22222);
        let voter2 = test_address(33333);
        let mut entry = MembershipEntry::new(addr);

        // Add an old vote
        entry
            .suspect_times
            .push((voter1.clone(), Utc::now() - Duration::minutes(10)));

        // Add a fresh vote
        entry.suspect_times.push((voter2.clone(), Utc::now()));

        let expiration = Duration::minutes(5);
        let fresh = entry.get_fresh_votes(expiration);

        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].0, voter2);
    }

    #[test]
    fn test_clear_suspect_times() {
        let addr = test_address(11111);
        let voter = test_address(22222);
        let mut entry = MembershipEntry::new(addr);

        entry.add_or_update_suspector(voter, Utc::now(), 3);
        assert!(entry.is_suspected());

        entry.clear_suspect_times();
        assert!(!entry.is_suspected());
        assert_eq!(entry.suspect_count(), 0);
    }

    #[test]
    fn test_matches() {
        let addr1 = test_address(11111);
        let addr2 = test_address(22222);
        let entry = MembershipEntry::new(addr1.clone());

        assert!(entry.matches(&addr1));
        assert!(!entry.matches(&addr2));
    }

    #[test]
    fn test_equality() {
        let addr = test_address(11111);
        let entry1 = MembershipEntry::new(addr.clone());
        let mut entry2 = MembershipEntry::new(addr);
        entry2.status = SiloStatus::Active; // Different status

        // Equality is based on silo_address only
        assert_eq!(entry1, entry2);
    }

    #[test]
    fn test_serialization() {
        let addr = test_address(11111);
        let entry = MembershipEntry::new(addr);

        let json = serde_json::to_string(&entry).unwrap();
        let deserialized: MembershipEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(entry.silo_address, deserialized.silo_address);
        assert_eq!(entry.status, deserialized.status);
    }
}
