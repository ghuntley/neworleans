//! Reader-writer lock for transactional concurrency control.
//!
//! Implements lock groups that allow multiple non-conflicting transactions
//! to proceed concurrently while serializing conflicting ones.

use crate::error::{TransactionError, TransactionResult};
use crate::transaction_info::{AccessCounter, TransactionId};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::time::Duration;
use tracing::{debug, trace, warn};

/// Role of a transaction in the commit protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitRole {
    /// Role not yet determined (transaction still active).
    NotYetDetermined,
    /// Transaction is read-only (no writes).
    ReadOnly,
    /// Transaction wrote but is not the TM - commits remotely.
    RemoteCommit,
    /// Transaction is the TM - commits locally.
    LocalCommit,
}

/// Record of a transaction within a lock group.
#[derive(Clone, Debug)]
pub struct TransactionRecord<TState> {
    /// Transaction identifier.
    pub transaction_id: TransactionId,
    /// Timestamp for priority ordering.
    pub timestamp: DateTime<Utc>,
    /// Copy of state for this transaction (copy-on-write).
    pub state: Option<TState>,
    /// Sequence number for ordering writes.
    pub sequence_number: i64,
    /// Whether state has been copied (indicates write).
    pub has_copied_state: bool,
    /// Role in commit protocol.
    pub role: CommitRole,
    /// Access counter.
    pub access: AccessCounter,
}

impl<TState> TransactionRecord<TState> {
    /// Creates a new transaction record.
    pub fn new(transaction_id: TransactionId, timestamp: DateTime<Utc>) -> Self {
        Self {
            transaction_id,
            timestamp,
            state: None,
            sequence_number: 0,
            has_copied_state: false,
            role: CommitRole::NotYetDetermined,
            access: AccessCounter::new(),
        }
    }

    /// Returns true if this transaction has performed a write.
    pub fn has_write(&self) -> bool {
        self.has_copied_state
    }
}

/// A group of non-conflicting transactions sharing a lock.
#[derive(Debug)]
pub struct LockGroup<TState> {
    /// Active transactions in this group.
    transactions: HashMap<TransactionId, TransactionRecord<TState>>,
    /// Deadline for this lock group.
    lock_deadline: Option<DateTime<Utc>>,
    /// Maximum size of this group.
    max_size: usize,
}

impl<TState: Clone> LockGroup<TState> {
    /// Creates a new lock group.
    pub fn new(max_size: usize) -> Self {
        Self {
            transactions: HashMap::new(),
            lock_deadline: None,
            max_size,
        }
    }

    /// Returns the number of transactions in this group.
    pub fn len(&self) -> usize {
        self.transactions.len()
    }

    /// Returns true if the group is empty.
    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty()
    }

    /// Returns true if the group is full.
    pub fn is_full(&self) -> bool {
        self.transactions.len() >= self.max_size
    }

    /// Returns true if the lock has expired.
    pub fn is_expired(&self) -> bool {
        self.lock_deadline.is_some_and(|deadline| Utc::now() > deadline)
    }

    /// Sets the lock deadline.
    pub fn set_deadline(&mut self, deadline: DateTime<Utc>) {
        if self.lock_deadline.is_none() {
            self.lock_deadline = Some(deadline);
        }
    }

    /// Gets a transaction record.
    pub fn get(&self, tx_id: &TransactionId) -> Option<&TransactionRecord<TState>> {
        self.transactions.get(tx_id)
    }

    /// Gets a mutable transaction record.
    pub fn get_mut(&mut self, tx_id: &TransactionId) -> Option<&mut TransactionRecord<TState>> {
        self.transactions.get_mut(tx_id)
    }

    /// Adds a transaction to this group.
    pub fn insert(&mut self, record: TransactionRecord<TState>) {
        self.transactions.insert(record.transaction_id, record);
    }

    /// Removes a transaction from this group.
    pub fn remove(&mut self, tx_id: &TransactionId) -> Option<TransactionRecord<TState>> {
        self.transactions.remove(tx_id)
    }

    /// Returns true if the transaction is in this group.
    pub fn contains(&self, tx_id: &TransactionId) -> bool {
        self.transactions.contains_key(tx_id)
    }

    /// Checks for conflicts with an incoming operation.
    ///
    /// Returns (has_conflict, is_resolvable):
    /// - has_conflict: true if there's a conflict with existing transactions
    /// - is_resolvable: true if the conflict can be resolved by aborting lower priority
    pub fn check_conflict(
        &self,
        is_read: bool,
        priority: DateTime<Utc>,
        tx_id: TransactionId,
    ) -> (bool, bool) {
        for (id, record) in &self.transactions {
            if *id == tx_id {
                continue;
            }

            // Read-Read: no conflict
            let both_reads = is_read && !record.has_copied_state;
            if both_reads {
                continue;
            }

            // Read-Write or Write-Write conflict
            if priority < record.timestamp {
                // Higher priority (earlier timestamp) - resolvable
                return (true, true);
            } else {
                // Lower priority - not resolvable
                return (true, false);
            }
        }

        (false, false)
    }

    /// Aborts all transactions with lower priority than the given timestamp.
    pub fn abort_lower_priority(&mut self, priority: DateTime<Utc>) -> Vec<TransactionId> {
        let to_abort: Vec<_> = self
            .transactions
            .iter()
            .filter(|(_, record)| record.timestamp > priority)
            .map(|(id, _)| *id)
            .collect();

        for id in &to_abort {
            self.transactions.remove(id);
        }

        to_abort
    }

    /// Returns all transaction IDs in this group.
    pub fn transaction_ids(&self) -> Vec<TransactionId> {
        self.transactions.keys().copied().collect()
    }
}

/// Reader-writer lock for transactional state.
#[derive(Debug)]
pub struct ReaderWriterLock<TState> {
    /// Current active lock group.
    inner: Mutex<ReaderWriterLockInner<TState>>,
    /// Lock timeout duration.
    lock_timeout: Duration,
    /// Maximum lock group size.
    max_group_size: usize,
}

#[derive(Debug)]
struct ReaderWriterLockInner<TState> {
    /// Current active group.
    current_group: Option<LockGroup<TState>>,
    /// Queue of waiting groups.
    queued_groups: VecDeque<LockGroup<TState>>,
    /// Stable committed state.
    committed_state: TState,
    /// Sequence number for the committed state.
    committed_sequence: i64,
}

impl<TState: Clone + Default> ReaderWriterLock<TState> {
    /// Creates a new reader-writer lock.
    pub fn new(lock_timeout: Duration, max_group_size: usize) -> Self {
        Self {
            inner: Mutex::new(ReaderWriterLockInner {
                current_group: None,
                queued_groups: VecDeque::new(),
                committed_state: TState::default(),
                committed_sequence: 0,
            }),
            lock_timeout,
            max_group_size,
        }
    }

    /// Creates a new reader-writer lock with initial state.
    pub fn with_state(state: TState, lock_timeout: Duration, max_group_size: usize) -> Self {
        Self {
            inner: Mutex::new(ReaderWriterLockInner {
                current_group: None,
                queued_groups: VecDeque::new(),
                committed_state: state,
                committed_sequence: 0,
            }),
            lock_timeout,
            max_group_size,
        }
    }
}

impl<TState: Clone> ReaderWriterLock<TState> {
    /// Enters the lock for a transaction.
    ///
    /// Returns the current state if successful, or an error if the lock cannot be acquired.
    pub fn enter(
        &self,
        tx_id: TransactionId,
        timestamp: DateTime<Utc>,
        is_read: bool,
    ) -> TransactionResult<TState> {
        let mut inner = self.inner.lock();

        // Check if lock group is expired
        if let Some(ref group) = inner.current_group {
            if group.is_expired() {
                warn!(
                    transaction_id = %tx_id,
                    "Lock group expired, breaking lock"
                );
                // Break the lock by clearing current group
                inner.current_group = None;
            }
        }

        // Try to join current group or create new one
        let group = inner.current_group.get_or_insert_with(|| {
            LockGroup::new(self.max_group_size)
        });

        // Check for conflicts
        let (has_conflict, is_resolvable) = group.check_conflict(is_read, timestamp, tx_id);

        if has_conflict {
            if is_resolvable {
                // Abort lower priority transactions
                let aborted = group.abort_lower_priority(timestamp);
                debug!(
                    transaction_id = %tx_id,
                    aborted_count = aborted.len(),
                    "Aborted lower priority transactions"
                );
            } else {
                trace!(
                    transaction_id = %tx_id,
                    "Conflict not resolvable, transaction must abort"
                );
                return Err(TransactionError::Aborted(
                    crate::error::AbortedReason::CascadingAbort(
                        "Conflict with higher priority transaction".to_string(),
                    ),
                ));
            }
        }

        // Check group capacity
        if group.is_full() && !group.contains(&tx_id) {
            trace!(
                transaction_id = %tx_id,
                "Lock group full, must wait"
            );
            return Err(TransactionError::Aborted(
                crate::error::AbortedReason::CascadingAbort(
                    "Lock group at capacity".to_string(),
                ),
            ));
        }

        // Set deadline on first write
        if !is_read && group.lock_deadline.is_none() {
            let deadline = Utc::now() + chrono::Duration::from_std(self.lock_timeout).unwrap();
            group.set_deadline(deadline);
        }

        // Add or update transaction record
        if !group.contains(&tx_id) {
            let record = TransactionRecord::new(tx_id, timestamp);
            group.insert(record);
        }

        trace!(
            transaction_id = %tx_id,
            is_read = is_read,
            group_size = group.len(),
            "Entered lock"
        );

        Ok(inner.committed_state.clone())
    }

    /// Prepares state for writing (copy-on-write).
    pub fn prepare_write(
        &self,
        tx_id: TransactionId,
        _timestamp: DateTime<Utc>,
    ) -> TransactionResult<TState> {
        let mut inner = self.inner.lock();

        // Extract values we need before getting mutable references
        let committed_state = inner.committed_state.clone();
        let next_sequence = inner.committed_sequence + 1;

        let group = inner.current_group.as_mut().ok_or_else(|| {
            TransactionError::InvalidState("No active lock group".to_string())
        })?;

        // Check if deadline needs to be set (before getting record)
        let needs_deadline = group.lock_deadline.is_none();

        let record = group.get_mut(&tx_id).ok_or_else(|| {
            TransactionError::NotFound(format!("Transaction {} not in lock group", tx_id))
        })?;

        if !record.has_copied_state {
            record.state = Some(committed_state);
            record.sequence_number = next_sequence;
            record.has_copied_state = true;
            record.access.increment_write();

            debug!(
                transaction_id = %tx_id,
                sequence_number = record.sequence_number,
                "Prepared state for write"
            );
        }

        let result = record
            .state
            .clone()
            .ok_or_else(|| TransactionError::Internal("State not copied".to_string()));

        // Set deadline after releasing record borrow
        if needs_deadline {
            let group = inner.current_group.as_mut().unwrap();
            let deadline = Utc::now() + chrono::Duration::from_std(self.lock_timeout).unwrap();
            group.set_deadline(deadline);
        }

        result
    }

    /// Commits a transaction's changes.
    pub fn commit(
        &self,
        tx_id: TransactionId,
        state: TState,
        sequence_number: i64,
    ) -> TransactionResult<()> {
        let mut inner = self.inner.lock();

        // Validate sequence
        if sequence_number <= inner.committed_sequence {
            return Err(TransactionError::Conflict(
                "Sequence number conflict".to_string(),
            ));
        }

        // Update committed state
        inner.committed_state = state;
        inner.committed_sequence = sequence_number;

        // Remove transaction from group
        if let Some(ref mut group) = inner.current_group {
            group.remove(&tx_id);

            // If group is empty, clear it
            if group.is_empty() {
                inner.current_group = None;

                // Promote next queued group if any
                if let Some(next) = inner.queued_groups.pop_front() {
                    inner.current_group = Some(next);
                }
            }
        }

        debug!(
            transaction_id = %tx_id,
            sequence_number = sequence_number,
            "Committed transaction"
        );

        Ok(())
    }

    /// Aborts a transaction.
    pub fn abort(&self, tx_id: TransactionId) {
        let mut inner = self.inner.lock();

        if let Some(ref mut group) = inner.current_group {
            if group.remove(&tx_id).is_some() {
                trace!(
                    transaction_id = %tx_id,
                    "Aborted transaction"
                );
            }

            // If group is empty, clear it
            if group.is_empty() {
                inner.current_group = None;

                // Promote next queued group if any
                if let Some(next) = inner.queued_groups.pop_front() {
                    inner.current_group = Some(next);
                }
            }
        }
    }

    /// Gets the current committed state.
    pub fn get_committed_state(&self) -> TState {
        self.inner.lock().committed_state.clone()
    }

    /// Gets the current committed sequence number.
    pub fn get_committed_sequence(&self) -> i64 {
        self.inner.lock().committed_sequence
    }

    /// Gets the current group size.
    pub fn current_group_size(&self) -> usize {
        self.inner
            .lock()
            .current_group
            .as_ref()
            .map(|g| g.len())
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Default, Debug, PartialEq)]
    struct TestState {
        value: i32,
    }

    #[test]
    fn test_commit_role_variants() {
        assert_eq!(CommitRole::NotYetDetermined, CommitRole::NotYetDetermined);
        assert_eq!(CommitRole::ReadOnly, CommitRole::ReadOnly);
        assert_eq!(CommitRole::RemoteCommit, CommitRole::RemoteCommit);
        assert_eq!(CommitRole::LocalCommit, CommitRole::LocalCommit);
    }

    #[test]
    fn test_transaction_record_new() {
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();
        let record: TransactionRecord<TestState> = TransactionRecord::new(tx_id, timestamp);

        assert_eq!(record.transaction_id, tx_id);
        assert!(record.state.is_none());
        assert!(!record.has_copied_state);
        assert_eq!(record.role, CommitRole::NotYetDetermined);
    }

    #[test]
    fn test_lock_group_new() {
        let group: LockGroup<TestState> = LockGroup::new(20);
        assert!(group.is_empty());
        assert!(!group.is_full());
    }

    #[test]
    fn test_lock_group_insert_and_get() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();
        let record = TransactionRecord::new(tx_id, timestamp);

        group.insert(record);

        assert_eq!(group.len(), 1);
        assert!(group.contains(&tx_id));
        assert!(group.get(&tx_id).is_some());
    }

    #[test]
    fn test_lock_group_remove() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx_id = TransactionId::new();
        let record = TransactionRecord::new(tx_id, Utc::now());

        group.insert(record);
        let removed = group.remove(&tx_id);

        assert!(removed.is_some());
        assert!(group.is_empty());
    }

    #[test]
    fn test_lock_group_is_full() {
        let mut group: LockGroup<TestState> = LockGroup::new(2);

        group.insert(TransactionRecord::new(TransactionId::new(), Utc::now()));
        assert!(!group.is_full());

        group.insert(TransactionRecord::new(TransactionId::new(), Utc::now()));
        assert!(group.is_full());
    }

    #[test]
    fn test_lock_group_check_conflict_read_read() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();
        let timestamp = Utc::now();

        // First read enters
        group.insert(TransactionRecord::new(tx1, timestamp));

        // Second read - no conflict
        let (conflict, _) = group.check_conflict(true, timestamp, tx2);
        assert!(!conflict);
    }

    #[test]
    fn test_lock_group_check_conflict_read_write() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();
        let timestamp = Utc::now();

        // First transaction writes
        let mut record = TransactionRecord::new(tx1, timestamp);
        record.has_copied_state = true;
        group.insert(record);

        // Second read - conflict with write
        let later = timestamp + chrono::Duration::seconds(1);
        let (conflict, resolvable) = group.check_conflict(true, later, tx2);
        assert!(conflict);
        assert!(!resolvable); // Lower priority
    }

    #[test]
    fn test_lock_group_check_conflict_higher_priority() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();
        let timestamp = Utc::now();

        // First transaction writes with later timestamp
        let later = timestamp + chrono::Duration::seconds(1);
        let mut record = TransactionRecord::new(tx1, later);
        record.has_copied_state = true;
        group.insert(record);

        // Second write with earlier timestamp - higher priority
        let (conflict, resolvable) = group.check_conflict(false, timestamp, tx2);
        assert!(conflict);
        assert!(resolvable); // Higher priority can resolve
    }

    #[test]
    fn test_lock_group_abort_lower_priority() {
        let mut group: LockGroup<TestState> = LockGroup::new(20);
        let tx1 = TransactionId::new();
        let tx2 = TransactionId::new();
        let timestamp = Utc::now();

        // Insert lower priority transaction
        let later = timestamp + chrono::Duration::seconds(1);
        group.insert(TransactionRecord::new(tx1, later));

        // Insert higher priority transaction
        group.insert(TransactionRecord::new(tx2, timestamp));

        // Abort lower priority
        let aborted = group.abort_lower_priority(timestamp);
        assert_eq!(aborted.len(), 1);
        assert_eq!(aborted[0], tx1);
        assert_eq!(group.len(), 1);
    }

    #[test]
    fn test_reader_writer_lock_enter() {
        let lock = ReaderWriterLock::<TestState>::new(Duration::from_secs(8), 20);
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();

        let state = lock.enter(tx_id, timestamp, true).unwrap();
        assert_eq!(state, TestState::default());
        assert_eq!(lock.current_group_size(), 1);
    }

    #[test]
    fn test_reader_writer_lock_prepare_write() {
        let initial = TestState { value: 42 };
        let lock = ReaderWriterLock::with_state(initial.clone(), Duration::from_secs(8), 20);
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();

        // Enter lock
        let _ = lock.enter(tx_id, timestamp, false).unwrap();

        // Prepare write
        let state = lock.prepare_write(tx_id, timestamp).unwrap();
        assert_eq!(state.value, 42);
    }

    #[test]
    fn test_reader_writer_lock_commit() {
        let lock = ReaderWriterLock::<TestState>::new(Duration::from_secs(8), 20);
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();

        // Enter and prepare write
        let _ = lock.enter(tx_id, timestamp, false).unwrap();
        let _ = lock.prepare_write(tx_id, timestamp).unwrap();

        // Commit
        let new_state = TestState { value: 100 };
        lock.commit(tx_id, new_state.clone(), 1).unwrap();

        // Verify committed state
        assert_eq!(lock.get_committed_state(), new_state);
        assert_eq!(lock.get_committed_sequence(), 1);
        assert_eq!(lock.current_group_size(), 0);
    }

    #[test]
    fn test_reader_writer_lock_abort() {
        let lock = ReaderWriterLock::<TestState>::new(Duration::from_secs(8), 20);
        let tx_id = TransactionId::new();
        let timestamp = Utc::now();

        // Enter lock
        let _ = lock.enter(tx_id, timestamp, true).unwrap();
        assert_eq!(lock.current_group_size(), 1);

        // Abort
        lock.abort(tx_id);
        assert_eq!(lock.current_group_size(), 0);
    }

    #[test]
    fn test_reader_writer_lock_multiple_readers() {
        let lock = ReaderWriterLock::<TestState>::new(Duration::from_secs(8), 20);
        let timestamp = Utc::now();

        for _ in 0..5 {
            let tx_id = TransactionId::new();
            let _ = lock.enter(tx_id, timestamp, true).unwrap();
        }

        assert_eq!(lock.current_group_size(), 5);
    }
}
