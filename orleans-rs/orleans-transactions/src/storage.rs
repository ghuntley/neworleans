//! Transactional state storage interfaces and types.
//!
//! Defines the storage abstraction for persisting transactional state
//! with support for pending transactions and commit records.

use crate::error::{TransactionError, TransactionResult};
use crate::transaction_info::{ParticipantId, TransactionId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, trace};

/// Metadata for transactional state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransactionalStateMetaData {
    /// Timestamp of the last commit.
    pub timestamp: DateTime<Utc>,
    /// Commit records for pending confirmations.
    pub commit_records: HashMap<String, CommitRecord>,
}

impl TransactionalStateMetaData {
    /// Creates new metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a commit record.
    pub fn add_commit_record(&mut self, tx_id: TransactionId, record: CommitRecord) {
        self.commit_records.insert(tx_id.to_string(), record);
    }

    /// Removes a commit record.
    pub fn remove_commit_record(&mut self, tx_id: &TransactionId) -> Option<CommitRecord> {
        self.commit_records.remove(&tx_id.to_string())
    }

    /// Gets a commit record.
    pub fn get_commit_record(&self, tx_id: &TransactionId) -> Option<&CommitRecord> {
        self.commit_records.get(&tx_id.to_string())
    }
}

/// Record of a committed transaction awaiting confirmation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommitRecord {
    /// Timestamp when committed.
    pub timestamp: DateTime<Utc>,
    /// Participants that need to confirm.
    pub write_participants: Vec<ParticipantId>,
    /// Number of confirmations received.
    pub confirmations_received: usize,
}

impl CommitRecord {
    /// Creates a new commit record.
    pub fn new(timestamp: DateTime<Utc>, write_participants: Vec<ParticipantId>) -> Self {
        Self {
            timestamp,
            write_participants,
            confirmations_received: 0,
        }
    }

    /// Records a confirmation from a participant.
    pub fn confirm(&mut self) {
        self.confirmations_received += 1;
    }

    /// Returns true if all participants have confirmed.
    pub fn is_complete(&self) -> bool {
        self.confirmations_received >= self.write_participants.len()
    }
}

/// Pending transaction state awaiting commit decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingTransactionState<TState> {
    /// Sequence ID for ordering.
    pub sequence_id: i64,
    /// Transaction identifier.
    pub transaction_id: String,
    /// Timestamp of the transaction.
    pub timestamp: DateTime<Utc>,
    /// Transaction manager for this transaction.
    pub transaction_manager: ParticipantId,
    /// The pending state.
    pub state: TState,
}

impl<TState> PendingTransactionState<TState> {
    /// Creates new pending state.
    pub fn new(
        sequence_id: i64,
        transaction_id: TransactionId,
        timestamp: DateTime<Utc>,
        transaction_manager: ParticipantId,
        state: TState,
    ) -> Self {
        Self {
            sequence_id,
            transaction_id: transaction_id.to_string(),
            timestamp,
            transaction_manager,
            state,
        }
    }
}

/// Response from loading transactional state from storage.
#[derive(Clone, Debug)]
pub struct TransactionalStorageLoadResponse<TState> {
    /// The committed state.
    pub committed_state: TState,
    /// Sequence ID of the committed state.
    pub committed_sequence_id: i64,
    /// Pending states awaiting commit decision.
    pub pending_states: Vec<PendingTransactionState<TState>>,
    /// State metadata.
    pub metadata: TransactionalStateMetaData,
    /// ETag for optimistic concurrency.
    pub etag: String,
}

/// Interface for transactional state storage.
#[async_trait]
pub trait ITransactionalStateStorage<TState>: Send + Sync
where
    TState: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>,
{
    /// Loads the current state from storage.
    async fn load(&self) -> TransactionResult<TransactionalStorageLoadResponse<TState>>;

    /// Stores state changes to storage.
    ///
    /// # Arguments
    /// * `expected_etag` - Expected ETag for optimistic concurrency
    /// * `metadata` - Updated metadata
    /// * `states_to_prepare` - Pending states to add
    /// * `commit_up_to` - Commit all states up to this sequence ID
    /// * `abort_after` - Abort all states after this sequence ID
    ///
    /// # Returns
    /// New ETag on success
    async fn store(
        &self,
        expected_etag: &str,
        metadata: TransactionalStateMetaData,
        states_to_prepare: Vec<PendingTransactionState<TState>>,
        commit_up_to: Option<i64>,
        abort_after: Option<i64>,
    ) -> TransactionResult<String>;
}

/// Events for storage batch processing.
pub trait ITransactionalStateStorageEvents<TState> {
    /// Records a prepare operation.
    fn prepare(
        &mut self,
        seq: i64,
        tx_id: TransactionId,
        ts: DateTime<Utc>,
        tm: ParticipantId,
        state: TState,
    );

    /// Records a read operation.
    fn read(&mut self, timestamp: DateTime<Utc>);

    /// Cancels a pending transaction.
    fn cancel(&mut self, sequence_number: i64);

    /// Confirms a committed transaction.
    fn confirm(&mut self, sequence_number: i64);

    /// Records a commit decision.
    fn commit(&mut self, tx_id: TransactionId, ts: DateTime<Utc>, write_resources: Vec<ParticipantId>);

    /// Collects (cleans up) a fully confirmed transaction.
    fn collect(&mut self, transaction_id: TransactionId);
}

/// Batch of storage operations to apply.
#[derive(Debug)]
pub struct StorageBatch<TState> {
    /// Updated metadata.
    pub metadata: TransactionalStateMetaData,
    /// Pending states to prepare.
    pub states_to_prepare: Vec<PendingTransactionState<TState>>,
    /// Confirm states up to this sequence.
    pub confirm_up_to: Option<i64>,
    /// Cancel states after this sequence.
    pub cancel_above: Option<i64>,
}

impl<TState> StorageBatch<TState> {
    /// Creates a new empty batch.
    pub fn new() -> Self {
        Self {
            metadata: TransactionalStateMetaData::new(),
            states_to_prepare: Vec::new(),
            confirm_up_to: None,
            cancel_above: None,
        }
    }

    /// Returns true if the batch is empty (no operations).
    pub fn is_empty(&self) -> bool {
        self.states_to_prepare.is_empty()
            && self.confirm_up_to.is_none()
            && self.cancel_above.is_none()
    }
}

impl<TState> Default for StorageBatch<TState> {
    fn default() -> Self {
        Self::new()
    }
}

impl<TState> ITransactionalStateStorageEvents<TState> for StorageBatch<TState> {
    fn prepare(
        &mut self,
        seq: i64,
        tx_id: TransactionId,
        ts: DateTime<Utc>,
        tm: ParticipantId,
        state: TState,
    ) {
        trace!(
            sequence = seq,
            transaction_id = %tx_id,
            "StorageBatch::prepare"
        );
        self.states_to_prepare.push(PendingTransactionState::new(
            seq, tx_id, ts, tm, state,
        ));
    }

    fn read(&mut self, timestamp: DateTime<Utc>) {
        trace!(timestamp = %timestamp, "StorageBatch::read");
        self.metadata.timestamp = timestamp;
    }

    fn cancel(&mut self, sequence_number: i64) {
        trace!(sequence = sequence_number, "StorageBatch::cancel");
        self.cancel_above = Some(
            self.cancel_above
                .map(|s| s.min(sequence_number))
                .unwrap_or(sequence_number),
        );
    }

    fn confirm(&mut self, sequence_number: i64) {
        trace!(sequence = sequence_number, "StorageBatch::confirm");
        self.confirm_up_to = Some(
            self.confirm_up_to
                .map(|s| s.max(sequence_number))
                .unwrap_or(sequence_number),
        );
    }

    fn commit(
        &mut self,
        tx_id: TransactionId,
        ts: DateTime<Utc>,
        write_resources: Vec<ParticipantId>,
    ) {
        debug!(
            transaction_id = %tx_id,
            participants = write_resources.len(),
            "StorageBatch::commit"
        );
        self.metadata.add_commit_record(
            tx_id,
            CommitRecord::new(ts, write_resources),
        );
    }

    fn collect(&mut self, transaction_id: TransactionId) {
        trace!(transaction_id = %transaction_id, "StorageBatch::collect");
        self.metadata.remove_commit_record(&transaction_id);
    }
}

/// In-memory implementation of transactional state storage for testing.
pub struct InMemoryTransactionalStorage<TState> {
    inner: parking_lot::Mutex<InMemoryStorageInner<TState>>,
}

struct InMemoryStorageInner<TState> {
    committed_state: TState,
    committed_sequence: i64,
    pending_states: Vec<PendingTransactionState<TState>>,
    metadata: TransactionalStateMetaData,
    etag: u64,
}

impl<TState: Clone + Default> InMemoryTransactionalStorage<TState> {
    /// Creates new in-memory storage.
    pub fn new() -> Self {
        Self {
            inner: parking_lot::Mutex::new(InMemoryStorageInner {
                committed_state: TState::default(),
                committed_sequence: 0,
                pending_states: Vec::new(),
                metadata: TransactionalStateMetaData::new(),
                etag: 0,
            }),
        }
    }
}

impl<TState: Clone> InMemoryTransactionalStorage<TState> {
    /// Creates new in-memory storage with initial state.
    pub fn with_state(state: TState) -> Self {
        Self {
            inner: parking_lot::Mutex::new(InMemoryStorageInner {
                committed_state: state,
                committed_sequence: 0,
                pending_states: Vec::new(),
                metadata: TransactionalStateMetaData::new(),
                etag: 0,
            }),
        }
    }
}

impl<TState: Clone + Default> Default for InMemoryTransactionalStorage<TState> {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl<TState> ITransactionalStateStorage<TState> for InMemoryTransactionalStorage<TState>
where
    TState: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + 'static,
{
    async fn load(&self) -> TransactionResult<TransactionalStorageLoadResponse<TState>> {
        let inner = self.inner.lock();
        Ok(TransactionalStorageLoadResponse {
            committed_state: inner.committed_state.clone(),
            committed_sequence_id: inner.committed_sequence,
            pending_states: inner.pending_states.clone(),
            metadata: inner.metadata.clone(),
            etag: inner.etag.to_string(),
        })
    }

    async fn store(
        &self,
        expected_etag: &str,
        metadata: TransactionalStateMetaData,
        states_to_prepare: Vec<PendingTransactionState<TState>>,
        commit_up_to: Option<i64>,
        abort_after: Option<i64>,
    ) -> TransactionResult<String> {
        let mut inner = self.inner.lock();

        // Check ETag
        if expected_etag != inner.etag.to_string() {
            return Err(TransactionError::Storage(
                "ETag mismatch".to_string(),
            ));
        }

        // Apply commit operations
        if let Some(seq) = commit_up_to {
            // Find the state to commit
            if let Some(pending) = inner.pending_states.iter().find(|p| p.sequence_id == seq) {
                inner.committed_state = pending.state.clone();
                inner.committed_sequence = seq;
            }
            // Remove committed states
            inner.pending_states.retain(|p| p.sequence_id > seq);
        }

        // Apply abort operations
        if let Some(seq) = abort_after {
            inner.pending_states.retain(|p| p.sequence_id <= seq);
        }

        // Add new pending states
        inner.pending_states.extend(states_to_prepare);

        // Update metadata
        inner.metadata = metadata;

        // Increment ETag
        inner.etag += 1;

        debug!(
            new_etag = inner.etag,
            pending_count = inner.pending_states.len(),
            "Store completed"
        );

        Ok(inner.etag.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};

    #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
    struct TestState {
        value: i32,
    }

    fn make_participant(name: &str) -> ParticipantId {
        let grain_id = GrainId::new(GrainType::create("Test"), IdSpan::from_str("key"));
        ParticipantId::new(name, grain_id)
    }

    #[test]
    fn test_transactional_state_metadata() {
        let mut metadata = TransactionalStateMetaData::new();
        let tx_id = TransactionId::new();
        let record = CommitRecord::new(Utc::now(), vec![make_participant("p1")]);

        metadata.add_commit_record(tx_id, record);
        assert!(metadata.get_commit_record(&tx_id).is_some());

        let removed = metadata.remove_commit_record(&tx_id);
        assert!(removed.is_some());
        assert!(metadata.get_commit_record(&tx_id).is_none());
    }

    #[test]
    fn test_commit_record_confirm() {
        let mut record = CommitRecord::new(
            Utc::now(),
            vec![make_participant("p1"), make_participant("p2")],
        );

        assert!(!record.is_complete());
        record.confirm();
        assert!(!record.is_complete());
        record.confirm();
        assert!(record.is_complete());
    }

    #[test]
    fn test_pending_transaction_state() {
        let state = TestState { value: 42 };
        let tx_id = TransactionId::new();
        let pending = PendingTransactionState::new(
            1,
            tx_id,
            Utc::now(),
            make_participant("tm"),
            state.clone(),
        );

        assert_eq!(pending.sequence_id, 1);
        assert_eq!(pending.state.value, 42);
    }

    #[test]
    fn test_storage_batch_new() {
        let batch: StorageBatch<TestState> = StorageBatch::new();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_storage_batch_prepare() {
        let mut batch: StorageBatch<TestState> = StorageBatch::new();
        let tx_id = TransactionId::new();
        let state = TestState { value: 42 };

        batch.prepare(1, tx_id, Utc::now(), make_participant("tm"), state);

        assert!(!batch.is_empty());
        assert_eq!(batch.states_to_prepare.len(), 1);
    }

    #[test]
    fn test_storage_batch_confirm() {
        let mut batch: StorageBatch<TestState> = StorageBatch::new();

        batch.confirm(5);
        assert_eq!(batch.confirm_up_to, Some(5));

        batch.confirm(3);
        assert_eq!(batch.confirm_up_to, Some(5)); // Max is kept

        batch.confirm(10);
        assert_eq!(batch.confirm_up_to, Some(10));
    }

    #[test]
    fn test_storage_batch_cancel() {
        let mut batch: StorageBatch<TestState> = StorageBatch::new();

        batch.cancel(10);
        assert_eq!(batch.cancel_above, Some(10));

        batch.cancel(5);
        assert_eq!(batch.cancel_above, Some(5)); // Min is kept

        batch.cancel(15);
        assert_eq!(batch.cancel_above, Some(5));
    }

    #[tokio::test]
    async fn test_in_memory_storage_load() {
        let storage = InMemoryTransactionalStorage::<TestState>::new();
        let response = storage.load().await.unwrap();

        assert_eq!(response.committed_sequence_id, 0);
        assert!(response.pending_states.is_empty());
        assert_eq!(response.etag, "0");
    }

    #[tokio::test]
    async fn test_in_memory_storage_store() {
        let storage = InMemoryTransactionalStorage::<TestState>::new();
        let response = storage.load().await.unwrap();

        let state = TestState { value: 42 };
        let pending = PendingTransactionState::new(
            1,
            TransactionId::new(),
            Utc::now(),
            make_participant("tm"),
            state,
        );

        let new_etag = storage
            .store(
                &response.etag,
                TransactionalStateMetaData::new(),
                vec![pending],
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(new_etag, "1");

        let response2 = storage.load().await.unwrap();
        assert_eq!(response2.pending_states.len(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_storage_commit() {
        let storage = InMemoryTransactionalStorage::<TestState>::new();
        let response = storage.load().await.unwrap();

        // Add pending state
        let state = TestState { value: 42 };
        let pending = PendingTransactionState::new(
            1,
            TransactionId::new(),
            Utc::now(),
            make_participant("tm"),
            state,
        );

        let etag1 = storage
            .store(
                &response.etag,
                TransactionalStateMetaData::new(),
                vec![pending],
                None,
                None,
            )
            .await
            .unwrap();

        // Commit the pending state
        let _etag2 = storage
            .store(
                &etag1,
                TransactionalStateMetaData::new(),
                vec![],
                Some(1),
                None,
            )
            .await
            .unwrap();

        let response2 = storage.load().await.unwrap();
        assert_eq!(response2.committed_state.value, 42);
        assert_eq!(response2.committed_sequence_id, 1);
        assert!(response2.pending_states.is_empty());
    }

    #[tokio::test]
    async fn test_in_memory_storage_etag_mismatch() {
        let storage = InMemoryTransactionalStorage::<TestState>::new();

        let result = storage
            .store(
                "wrong",
                TransactionalStateMetaData::new(),
                vec![],
                None,
                None,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::Storage(_)));
    }
}
