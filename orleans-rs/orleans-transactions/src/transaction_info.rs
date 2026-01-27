//! Transaction information and participant tracking.
//!
//! Core types for tracking transaction state, participants, and their access patterns.

use chrono::{DateTime, Utc};
use orleans_core::GrainId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use uuid::Uuid;

/// Unique identifier for a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionId(Uuid);

impl TransactionId {
    /// Creates a new random transaction ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Creates a transaction ID from a UUID.
    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Gets the underlying UUID.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Roles a participant can play in a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Role {
    /// Basic participant resource (can prepare/commit).
    Resource = 1,
    /// Can act as transaction manager.
    Manager = 2,
    /// Preferred transaction manager (has priority).
    PriorityManager = 4,
}

impl Role {
    /// Returns true if this role includes manager capability.
    pub fn is_manager(&self) -> bool {
        matches!(self, Role::Manager | Role::PriorityManager)
    }

    /// Returns true if this is a priority manager.
    pub fn is_priority_manager(&self) -> bool {
        matches!(self, Role::PriorityManager)
    }
}

/// Identifies a participant in a transaction.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ParticipantId {
    /// Human-readable name for the participant (usually state name).
    pub name: String,
    /// The grain ID of the participant.
    pub grain_id: GrainId,
    /// Roles this participant supports.
    pub supported_roles: Vec<Role>,
}

impl ParticipantId {
    /// Creates a new participant ID.
    pub fn new(name: impl Into<String>, grain_id: GrainId) -> Self {
        Self {
            name: name.into(),
            grain_id,
            supported_roles: vec![Role::Resource],
        }
    }

    /// Creates a participant ID with manager capability.
    pub fn with_manager(name: impl Into<String>, grain_id: GrainId) -> Self {
        Self {
            name: name.into(),
            grain_id,
            supported_roles: vec![Role::Resource, Role::Manager],
        }
    }

    /// Creates a participant ID with priority manager capability.
    pub fn with_priority_manager(name: impl Into<String>, grain_id: GrainId) -> Self {
        Self {
            name: name.into(),
            grain_id,
            supported_roles: vec![Role::Resource, Role::Manager, Role::PriorityManager],
        }
    }

    /// Returns true if this participant can be a manager.
    pub fn can_be_manager(&self) -> bool {
        self.supported_roles.iter().any(|r| r.is_manager())
    }

    /// Returns true if this participant is a priority manager.
    pub fn is_priority_manager(&self) -> bool {
        self.supported_roles.iter().any(|r| r.is_priority_manager())
    }
}

impl fmt::Display for ParticipantId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.name, self.grain_id)
    }
}

/// Tracks read and write counts for a participant.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessCounter {
    /// Number of read operations.
    pub reads: i32,
    /// Number of write operations.
    pub writes: i32,
}

impl AccessCounter {
    /// Creates a new access counter.
    pub const fn new() -> Self {
        Self { reads: 0, writes: 0 }
    }

    /// Creates an access counter with the given counts.
    pub const fn with_counts(reads: i32, writes: i32) -> Self {
        Self { reads, writes }
    }

    /// Increments the read count.
    pub fn increment_read(&mut self) {
        self.reads += 1;
    }

    /// Increments the write count.
    pub fn increment_write(&mut self) {
        self.writes += 1;
    }

    /// Returns true if this is read-only access.
    pub fn is_read_only(&self) -> bool {
        self.writes == 0
    }

    /// Returns true if this includes write access.
    pub fn has_writes(&self) -> bool {
        self.writes > 0
    }

    /// Returns the total number of accesses.
    pub fn total(&self) -> i32 {
        self.reads + self.writes
    }
}

impl std::ops::Add for AccessCounter {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self {
            reads: self.reads + rhs.reads,
            writes: self.writes + rhs.writes,
        }
    }
}

impl std::ops::AddAssign for AccessCounter {
    fn add_assign(&mut self, rhs: Self) {
        self.reads += rhs.reads;
        self.writes += rhs.writes;
    }
}

/// Information about an active transaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransactionInfo {
    /// Unique identifier for this transaction.
    pub transaction_id: TransactionId,
    /// Timestamp for causal ordering.
    pub timestamp: DateTime<Utc>,
    /// Whether this is a read-only transaction.
    pub is_read_only: bool,
    /// Participants and their access counts.
    pub participants: HashMap<ParticipantId, AccessCounter>,
    /// Transaction timeout.
    pub timeout: Duration,
    /// The selected transaction manager (if any).
    pub transaction_manager: Option<ParticipantId>,
}

impl TransactionInfo {
    /// Creates a new transaction info.
    pub fn new(transaction_id: TransactionId, timestamp: DateTime<Utc>, timeout: Duration) -> Self {
        Self {
            transaction_id,
            timestamp,
            is_read_only: true,
            participants: HashMap::new(),
            timeout,
            transaction_manager: None,
        }
    }

    /// Creates a read-only transaction.
    pub fn read_only(transaction_id: TransactionId, timestamp: DateTime<Utc>, timeout: Duration) -> Self {
        let mut info = Self::new(transaction_id, timestamp, timeout);
        info.is_read_only = true;
        info
    }

    /// Records a read operation on a participant.
    pub fn record_read(&mut self, participant: ParticipantId) {
        self.participants
            .entry(participant)
            .or_insert_with(AccessCounter::new)
            .increment_read();
    }

    /// Records a write operation on a participant.
    pub fn record_write(&mut self, participant: ParticipantId) {
        self.is_read_only = false;
        self.participants
            .entry(participant)
            .or_insert_with(AccessCounter::new)
            .increment_write();
    }

    /// Gets the access counter for a participant.
    pub fn get_access(&self, participant: &ParticipantId) -> Option<&AccessCounter> {
        self.participants.get(participant)
    }

    /// Returns all participants with write access.
    pub fn write_participants(&self) -> Vec<&ParticipantId> {
        self.participants
            .iter()
            .filter(|(_, ac)| ac.has_writes())
            .map(|(p, _)| p)
            .collect()
    }

    /// Returns all participants (read and write).
    pub fn all_participants(&self) -> Vec<&ParticipantId> {
        self.participants.keys().collect()
    }

    /// Returns the number of participants.
    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    /// Selects the transaction manager from participants.
    ///
    /// Priority order:
    /// 1. Priority managers
    /// 2. Regular managers
    /// 3. Any participant with writes
    pub fn select_transaction_manager(&mut self) -> Option<ParticipantId> {
        // Only select from write participants
        let write_participants: Vec<_> = self
            .participants
            .iter()
            .filter(|(_, ac)| ac.has_writes())
            .map(|(p, _)| p.clone())
            .collect();

        if write_participants.is_empty() {
            return None;
        }

        // Priority managers first
        if let Some(p) = write_participants.iter().find(|p| p.is_priority_manager()) {
            self.transaction_manager = Some(p.clone());
            return self.transaction_manager.clone();
        }

        // Regular managers next
        if let Some(p) = write_participants.iter().find(|p| p.can_be_manager()) {
            self.transaction_manager = Some(p.clone());
            return self.transaction_manager.clone();
        }

        // Fall back to any write participant
        if let Some(p) = write_participants.first() {
            self.transaction_manager = Some(p.clone());
            return self.transaction_manager.clone();
        }

        None
    }

    /// Returns the deadline for this transaction.
    pub fn deadline(&self) -> DateTime<Utc> {
        self.timestamp + chrono::Duration::from_std(self.timeout).unwrap_or_default()
    }

    /// Returns true if this transaction has expired.
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.deadline()
    }
}

impl fmt::Display for TransactionInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Transaction({}, {}, participants: {})",
            self.transaction_id,
            if self.is_read_only { "read-only" } else { "read-write" },
            self.participants.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainType, IdSpan};

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(GrainType::create(name), IdSpan::from_str("test"))
    }

    #[test]
    fn test_transaction_id_new() {
        let id1 = TransactionId::new();
        let id2 = TransactionId::new();
        assert_ne!(id1, id2, "Transaction IDs should be unique");
    }

    #[test]
    fn test_transaction_id_display() {
        let id = TransactionId::new();
        let s = id.to_string();
        assert!(!s.is_empty());
    }

    #[test]
    fn test_role_is_manager() {
        assert!(!Role::Resource.is_manager());
        assert!(Role::Manager.is_manager());
        assert!(Role::PriorityManager.is_manager());
    }

    #[test]
    fn test_role_is_priority_manager() {
        assert!(!Role::Resource.is_priority_manager());
        assert!(!Role::Manager.is_priority_manager());
        assert!(Role::PriorityManager.is_priority_manager());
    }

    #[test]
    fn test_participant_id_new() {
        let grain_id = make_grain_id("TestGrain");
        let participant = ParticipantId::new("state", grain_id.clone());

        assert_eq!(participant.name, "state");
        assert_eq!(participant.grain_id, grain_id);
        assert!(!participant.can_be_manager());
    }

    #[test]
    fn test_participant_id_with_manager() {
        let grain_id = make_grain_id("TestGrain");
        let participant = ParticipantId::with_manager("state", grain_id);

        assert!(participant.can_be_manager());
        assert!(!participant.is_priority_manager());
    }

    #[test]
    fn test_participant_id_with_priority_manager() {
        let grain_id = make_grain_id("TestGrain");
        let participant = ParticipantId::with_priority_manager("state", grain_id);

        assert!(participant.can_be_manager());
        assert!(participant.is_priority_manager());
    }

    #[test]
    fn test_access_counter_new() {
        let counter = AccessCounter::new();
        assert_eq!(counter.reads, 0);
        assert_eq!(counter.writes, 0);
        assert!(counter.is_read_only());
    }

    #[test]
    fn test_access_counter_increment() {
        let mut counter = AccessCounter::new();
        counter.increment_read();
        counter.increment_write();

        assert_eq!(counter.reads, 1);
        assert_eq!(counter.writes, 1);
        assert!(!counter.is_read_only());
        assert!(counter.has_writes());
    }

    #[test]
    fn test_access_counter_add() {
        let a = AccessCounter::with_counts(1, 2);
        let b = AccessCounter::with_counts(3, 4);
        let c = a + b;

        assert_eq!(c.reads, 4);
        assert_eq!(c.writes, 6);
    }

    #[test]
    fn test_transaction_info_new() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let info = TransactionInfo::new(id, ts, timeout);

        assert_eq!(info.transaction_id, id);
        assert!(info.is_read_only);
        assert!(info.participants.is_empty());
    }

    #[test]
    fn test_transaction_info_record_operations() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let mut info = TransactionInfo::new(id, ts, timeout);
        let grain_id = make_grain_id("TestGrain");
        let participant = ParticipantId::new("state", grain_id);

        info.record_read(participant.clone());
        assert!(info.is_read_only);
        assert_eq!(info.get_access(&participant).unwrap().reads, 1);

        info.record_write(participant.clone());
        assert!(!info.is_read_only);
        assert_eq!(info.get_access(&participant).unwrap().writes, 1);
    }

    #[test]
    fn test_transaction_info_write_participants() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let mut info = TransactionInfo::new(id, ts, timeout);

        let grain1 = make_grain_id("Grain1");
        let grain2 = make_grain_id("Grain2");
        let p1 = ParticipantId::new("state1", grain1);
        let p2 = ParticipantId::new("state2", grain2);

        info.record_read(p1.clone());
        info.record_write(p2.clone());

        let writers = info.write_participants();
        assert_eq!(writers.len(), 1);
        assert_eq!(writers[0], &p2);
    }

    #[test]
    fn test_transaction_info_select_tm() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let mut info = TransactionInfo::new(id, ts, timeout);

        let grain1 = make_grain_id("Grain1");
        let grain2 = make_grain_id("Grain2");
        let grain3 = make_grain_id("Grain3");

        let p1 = ParticipantId::new("state1", grain1);
        let p2 = ParticipantId::with_manager("state2", grain2);
        let p3 = ParticipantId::with_priority_manager("state3", grain3);

        info.record_write(p1);
        info.record_write(p2);
        info.record_write(p3.clone());

        let tm = info.select_transaction_manager();
        assert!(tm.is_some());
        assert_eq!(tm.unwrap(), p3, "Priority manager should be selected");
    }

    #[test]
    fn test_transaction_info_select_tm_no_priority() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let mut info = TransactionInfo::new(id, ts, timeout);

        let grain1 = make_grain_id("Grain1");
        let grain2 = make_grain_id("Grain2");

        let p1 = ParticipantId::new("state1", grain1);
        let p2 = ParticipantId::with_manager("state2", grain2.clone());

        info.record_write(p1);
        info.record_write(p2.clone());

        let tm = info.select_transaction_manager();
        assert!(tm.is_some());
        assert_eq!(tm.unwrap(), p2, "Regular manager should be selected");
    }

    #[test]
    fn test_transaction_info_deadline() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let info = TransactionInfo::new(id, ts, timeout);
        let deadline = info.deadline();

        let expected = ts + chrono::Duration::seconds(30);
        assert_eq!(deadline, expected);
    }

    #[test]
    fn test_transaction_info_is_expired() {
        let id = TransactionId::new();
        let ts = Utc::now() - chrono::Duration::seconds(60);
        let timeout = Duration::from_secs(30);

        let info = TransactionInfo::new(id, ts, timeout);
        assert!(info.is_expired());
    }

    #[test]
    fn test_transaction_info_display() {
        let id = TransactionId::new();
        let ts = Utc::now();
        let timeout = Duration::from_secs(30);

        let info = TransactionInfo::new(id, ts, timeout);
        let s = info.to_string();

        assert!(s.contains("Transaction"));
        assert!(s.contains("read-only"));
    }
}
