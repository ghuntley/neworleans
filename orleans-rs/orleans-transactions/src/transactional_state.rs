//! Transactional state wrapper for grains.
//!
//! Provides the `TransactionalState` type that wraps grain state with
//! transactional semantics including read/write tracking, copy-on-write,
//! and integration with the 2PC protocol.

use crate::error::{AbortedReason, TransactionError, TransactionResult};
use crate::lock::{CommitRole, ReaderWriterLock};
use crate::options::TransactionalStateOptions;
use crate::storage::{ITransactionalStateStorage, PendingTransactionState};
use crate::transaction_info::{ParticipantId, TransactionId};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_core::GrainId;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, instrument, trace, warn};

/// Interface for transactional state operations.
#[async_trait]
pub trait ITransactionalState<TState>: Send + Sync {
    /// Performs a read operation within a transaction.
    async fn perform_read<TResult, F>(&self, tx_id: TransactionId, read_function: F) -> TransactionResult<TResult>
    where
        F: FnOnce(&TState) -> TResult + Send,
        TResult: Send;

    /// Performs an update operation within a transaction.
    async fn perform_update<TResult, F>(&self, tx_id: TransactionId, update_function: F) -> TransactionResult<TResult>
    where
        F: FnOnce(&mut TState) -> TResult + Send,
        TResult: Send;
}

/// Context for the current transaction.
#[derive(Clone, Debug)]
pub struct TransactionContext {
    /// Transaction ID.
    pub transaction_id: TransactionId,
    /// Transaction timestamp.
    pub timestamp: DateTime<Utc>,
    /// Whether this is a read-only transaction.
    pub is_read_only: bool,
}

impl TransactionContext {
    /// Creates a new transaction context.
    pub fn new(transaction_id: TransactionId, timestamp: DateTime<Utc>, is_read_only: bool) -> Self {
        Self {
            transaction_id,
            timestamp,
            is_read_only,
        }
    }
}

/// Transactional state wrapper providing ACID semantics.
pub struct TransactionalState<TState>
where
    TState: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Default + 'static,
{
    /// The grain ID owning this state.
    grain_id: GrainId,
    /// Name of this state (for multi-state grains).
    state_name: String,
    /// Participant ID for this state.
    participant_id: ParticipantId,
    /// Reader-writer lock for concurrency control.
    lock: ReaderWriterLock<TState>,
    /// Storage provider.
    storage: Arc<dyn ITransactionalStateStorage<TState>>,
    /// Configuration options (used in full implementation for prepare timeouts).
    #[allow(dead_code)]
    options: TransactionalStateOptions,
    /// Current transaction state per transaction.
    transaction_states: Mutex<std::collections::HashMap<TransactionId, TransactionState<TState>>>,
}

/// Per-transaction state tracking.
#[derive(Clone)]
struct TransactionState<TState> {
    /// The working state for this transaction.
    state: TState,
    /// Sequence number assigned for writes.
    sequence_number: i64,
    /// Whether this transaction has performed a write.
    has_write: bool,
    /// Commit role for this transaction (used in full 2PC implementation).
    #[allow(dead_code)]
    role: CommitRole,
}

impl<TState> TransactionalState<TState>
where
    TState: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Default + 'static,
{
    /// Creates a new transactional state.
    pub fn new(
        grain_id: GrainId,
        state_name: impl Into<String>,
        storage: Arc<dyn ITransactionalStateStorage<TState>>,
        options: TransactionalStateOptions,
    ) -> Self {
        let state_name = state_name.into();
        let participant_id = ParticipantId::with_manager(state_name.clone(), grain_id.clone());

        Self {
            grain_id,
            state_name,
            participant_id,
            lock: ReaderWriterLock::new(options.lock_timeout, options.max_lock_group_size),
            storage,
            options,
            transaction_states: Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Gets the participant ID for this state.
    pub fn participant_id(&self) -> &ParticipantId {
        &self.participant_id
    }

    /// Loads state from storage on activation.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, state_name = %self.state_name))]
    pub async fn on_activate(&self) -> TransactionResult<()> {
        let response = self.storage.load().await?;

        debug!(
            committed_sequence = response.committed_sequence_id,
            pending_count = response.pending_states.len(),
            "Loaded transactional state"
        );

        // Recovery: handle pending states
        for pending in &response.pending_states {
            trace!(
                sequence = pending.sequence_id,
                transaction_id = %pending.transaction_id,
                "Found pending state during activation"
            );
            // In a full implementation, we would query the TM for commit status
            // For now, we assume pending states are aborted
        }

        Ok(())
    }

    /// Saves state on deactivation.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, state_name = %self.state_name))]
    pub async fn on_deactivate(&self) -> TransactionResult<()> {
        // Clear any pending transaction states
        let mut states = self.transaction_states.lock();
        if !states.is_empty() {
            warn!(
                pending_transactions = states.len(),
                "Deactivating with pending transactions"
            );
            states.clear();
        }
        Ok(())
    }

    /// Enters a transaction for reading.
    fn enter_read(&self, tx_id: TransactionId, timestamp: DateTime<Utc>) -> TransactionResult<TState> {
        let state = self.lock.enter(tx_id, timestamp, true)?;

        let mut states = self.transaction_states.lock();
        states.insert(
            tx_id,
            TransactionState {
                state: state.clone(),
                sequence_number: 0,
                has_write: false,
                role: CommitRole::NotYetDetermined,
            },
        );

        trace!(
            transaction_id = %tx_id,
            "Entered transaction for read"
        );

        Ok(state)
    }

    /// Prepares a transaction for writing.
    fn prepare_write(&self, tx_id: TransactionId, timestamp: DateTime<Utc>) -> TransactionResult<TState> {
        // Check if already in transaction
        let already_in_tx = {
            let states = self.transaction_states.lock();
            states.contains_key(&tx_id)
        };

        if !already_in_tx {
            // Not yet in transaction - enter first
            let _ = self.lock.enter(tx_id, timestamp, false)?;
        }

        // Now prepare for write
        let state = self.lock.prepare_write(tx_id, timestamp)?;

        let mut states = self.transaction_states.lock();
        if let Some(tx_state) = states.get_mut(&tx_id) {
            tx_state.state = state.clone();
            tx_state.has_write = true;
            tx_state.sequence_number = self.lock.get_committed_sequence() + 1;
        } else {
            states.insert(
                tx_id,
                TransactionState {
                    state: state.clone(),
                    sequence_number: self.lock.get_committed_sequence() + 1,
                    has_write: true,
                    role: CommitRole::NotYetDetermined,
                },
            );
        }

        trace!(
            transaction_id = %tx_id,
            "Prepared transaction for write"
        );

        Ok(state)
    }

    /// Gets the current state for a transaction.
    pub fn get_state(&self, tx_id: TransactionId) -> TransactionResult<TState> {
        let states = self.transaction_states.lock();
        states
            .get(&tx_id)
            .map(|s| s.state.clone())
            .ok_or_else(|| TransactionError::NotFound(format!("Transaction {} not found", tx_id)))
    }

    /// Updates the state for a transaction.
    pub fn set_state(&self, tx_id: TransactionId, state: TState) -> TransactionResult<()> {
        let mut states = self.transaction_states.lock();
        if let Some(tx_state) = states.get_mut(&tx_id) {
            tx_state.state = state;
            tx_state.has_write = true;
            Ok(())
        } else {
            Err(TransactionError::NotFound(format!(
                "Transaction {} not found",
                tx_id
            )))
        }
    }

    /// Prepares a transaction for commit.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    pub async fn prepare(
        &self,
        tx_id: TransactionId,
        timestamp: DateTime<Utc>,
        tm: ParticipantId,
    ) -> TransactionResult<()> {
        let states = self.transaction_states.lock();
        let tx_state = states.get(&tx_id).ok_or_else(|| {
            TransactionError::NotFound(format!("Transaction {} not found", tx_id))
        })?;

        if !tx_state.has_write {
            // Read-only - nothing to prepare
            return Ok(());
        }

        // Persist prepare record
        let response = self.storage.load().await?;
        let pending = PendingTransactionState::new(
            tx_state.sequence_number,
            tx_id,
            timestamp,
            tm,
            tx_state.state.clone(),
        );

        self.storage
            .store(
                &response.etag,
                response.metadata,
                vec![pending],
                None,
                None,
            )
            .await?;

        debug!(
            transaction_id = %tx_id,
            sequence = tx_state.sequence_number,
            "Transaction prepared"
        );

        Ok(())
    }

    /// Commits a transaction.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    pub async fn commit(&self, tx_id: TransactionId) -> TransactionResult<()> {
        let tx_state = {
            let mut states = self.transaction_states.lock();
            states.remove(&tx_id)
        };

        let tx_state = tx_state.ok_or_else(|| {
            TransactionError::NotFound(format!("Transaction {} not found", tx_id))
        })?;

        if !tx_state.has_write {
            // Read-only - just release lock
            self.lock.abort(tx_id);
            debug!(transaction_id = %tx_id, "Read-only transaction committed");
            return Ok(());
        }

        // Commit to lock
        self.lock
            .commit(tx_id, tx_state.state.clone(), tx_state.sequence_number)?;

        // Persist commit
        let response = self.storage.load().await?;
        self.storage
            .store(
                &response.etag,
                response.metadata,
                vec![],
                Some(tx_state.sequence_number),
                None,
            )
            .await?;

        debug!(
            transaction_id = %tx_id,
            sequence = tx_state.sequence_number,
            "Transaction committed"
        );

        Ok(())
    }

    /// Aborts a transaction.
    #[instrument(skip(self), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    pub async fn abort(&self, tx_id: TransactionId, reason: Option<AbortedReason>) -> TransactionResult<()> {
        let _ = {
            let mut states = self.transaction_states.lock();
            states.remove(&tx_id)
        };

        self.lock.abort(tx_id);

        debug!(
            transaction_id = %tx_id,
            reason = ?reason,
            "Transaction aborted"
        );

        Ok(())
    }

    /// Confirms a committed transaction (cleanup).
    #[instrument(skip(self), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    pub async fn confirm(&self, tx_id: TransactionId, sequence: i64) -> TransactionResult<()> {
        let response = self.storage.load().await?;

        // Mark as confirmed in storage
        let mut metadata = response.metadata;
        metadata.remove_commit_record(&tx_id);

        self.storage
            .store(&response.etag, metadata, vec![], None, None)
            .await?;

        trace!(
            transaction_id = %tx_id,
            sequence = sequence,
            "Transaction confirmed"
        );

        Ok(())
    }
}

#[async_trait]
impl<TState> ITransactionalState<TState> for TransactionalState<TState>
where
    TState: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + Default + 'static,
{
    #[instrument(skip(self, read_function), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    async fn perform_read<TResult, F>(&self, tx_id: TransactionId, read_function: F) -> TransactionResult<TResult>
    where
        F: FnOnce(&TState) -> TResult + Send,
        TResult: Send,
    {
        // Check if already in transaction
        let state = {
            let states = self.transaction_states.lock();
            states.get(&tx_id).map(|s| s.state.clone())
        };

        let state = if let Some(s) = state {
            s
        } else {
            // Enter transaction for read
            self.enter_read(tx_id, Utc::now())?
        };

        trace!(transaction_id = %tx_id, "Performing read");
        Ok(read_function(&state))
    }

    #[instrument(skip(self, update_function), fields(grain_id = %self.grain_id, transaction_id = %tx_id))]
    async fn perform_update<TResult, F>(&self, tx_id: TransactionId, update_function: F) -> TransactionResult<TResult>
    where
        F: FnOnce(&mut TState) -> TResult + Send,
        TResult: Send,
    {
        let timestamp = Utc::now();

        // Prepare for write
        let mut state = self.prepare_write(tx_id, timestamp)?;

        trace!(transaction_id = %tx_id, "Performing update");
        let result = update_function(&mut state);

        // Update transaction state
        self.set_state(tx_id, state)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::InMemoryTransactionalStorage;
    use orleans_core::{GrainType, IdSpan};

    #[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
    struct CounterState {
        value: i32,
    }

    fn make_grain_id() -> GrainId {
        GrainId::new(GrainType::create("Counter"), IdSpan::from_str("test"))
    }

    fn make_transactional_state() -> TransactionalState<CounterState> {
        let grain_id = make_grain_id();
        let storage = Arc::new(InMemoryTransactionalStorage::<CounterState>::new());
        let options = TransactionalStateOptions::for_testing();

        TransactionalState::new(grain_id, "state", storage, options)
    }

    #[test]
    fn test_transaction_context() {
        let ctx = TransactionContext::new(TransactionId::new(), Utc::now(), true);
        assert!(ctx.is_read_only);
    }

    #[tokio::test]
    async fn test_transactional_state_on_activate() {
        let state = make_transactional_state();
        let result = state.on_activate().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transactional_state_perform_read() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        let result = ts.perform_read(tx_id, |s| s.value).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_transactional_state_perform_update() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        // First read to enter transaction
        let _ = ts.perform_read(tx_id, |s| s.value).await.unwrap();

        // Then update
        let result = ts
            .perform_update(tx_id, |s| {
                s.value = 42;
                s.value
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);

        // Verify state was updated
        let state = ts.get_state(tx_id).unwrap();
        assert_eq!(state.value, 42);
    }

    #[tokio::test]
    async fn test_transactional_state_commit() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        // Perform update
        let _ = ts
            .perform_update(tx_id, |s| {
                s.value = 100;
            })
            .await
            .unwrap();

        // Commit
        let result = ts.commit(tx_id).await;
        assert!(result.is_ok());

        // Verify new transaction sees committed value
        let tx_id2 = TransactionId::new();
        let value = ts.perform_read(tx_id2, |s| s.value).await.unwrap();
        assert_eq!(value, 100);
    }

    #[tokio::test]
    async fn test_transactional_state_abort() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        // Perform update
        let _ = ts
            .perform_update(tx_id, |s| {
                s.value = 100;
            })
            .await
            .unwrap();

        // Abort
        let result = ts.abort(tx_id, None).await;
        assert!(result.is_ok());

        // Verify new transaction sees original value
        let tx_id2 = TransactionId::new();
        let value = ts.perform_read(tx_id2, |s| s.value).await.unwrap();
        assert_eq!(value, 0);
    }

    #[tokio::test]
    async fn test_transactional_state_read_only_commit() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        // Just read
        let _ = ts.perform_read(tx_id, |s| s.value).await.unwrap();

        // Commit (should be quick for read-only)
        let result = ts.commit(tx_id).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_transactional_state_multiple_reads() {
        let ts = make_transactional_state();

        // Multiple transactions can read concurrently
        let mut handles = vec![];
        for _ in 0..5 {
            let tx_id = TransactionId::new();
            // Note: We can't easily spawn here without Arc, so test sequentially
            let value = ts.perform_read(tx_id, |s| s.value).await.unwrap();
            handles.push(value);
        }

        assert_eq!(handles.len(), 5);
        for v in handles {
            assert_eq!(v, 0);
        }
    }

    #[tokio::test]
    async fn test_transactional_state_on_deactivate() {
        let ts = make_transactional_state();
        let tx_id = TransactionId::new();

        // Start a transaction
        let _ = ts.perform_read(tx_id, |s| s.value).await.unwrap();

        // Deactivate should clear pending transactions
        let result = ts.on_deactivate().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_participant_id() {
        let ts = make_transactional_state();
        let participant = ts.participant_id();

        assert_eq!(participant.name, "state");
        assert!(participant.can_be_manager());
    }
}
