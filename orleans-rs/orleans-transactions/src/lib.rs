//! # Orleans Transactions
//!
//! ACID transaction support for Orleans grains using an asymmetric Two-Phase Commit (2PC) protocol.
//!
//! This crate provides:
//! - **TransactionAgent**: Orchestrates the 2PC protocol from the client side
//! - **TransactionalState**: Wraps grain state with transactional semantics
//! - **ReaderWriterLock**: Lock groups for non-blocking concurrency control
//! - **CausalClock**: Monotonically increasing timestamps for causal ordering
//! - **Storage interfaces**: Persistence for transactional state and commit records
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use orleans_transactions::{
//!     TransactionAgent, TransactionalState, ITransactionalState,
//!     TransactionAgentOptions, TransactionalStateOptions,
//! };
//!
//! // Create a transaction agent
//! let agent = TransactionAgent::new(TransactionAgentOptions::default());
//!
//! // Start a transaction
//! let info = agent.start_transaction(false, None)?;
//!
//! // Perform operations
//! state.perform_update(info.transaction_id, |s| {
//!     s.value += 1;
//! }).await?;
//!
//! // Commit
//! let status = agent.resolve(info.transaction_id).await?;
//! ```
//!
//! ## Transaction Flow
//!
//! ### Read-Only Transactions (1-Phase)
//!
//! 1. Agent starts transaction with `is_read_only = true`
//! 2. Application performs reads via `perform_read()`
//! 3. Agent calls `resolve()` which sends `CommitReadOnly` to all participants
//!
//! ### Read-Write Transactions (2-Phase)
//!
//! 1. Agent starts transaction with `is_read_only = false`
//! 2. Application performs reads and writes
//! 3. Agent calls `resolve()`:
//!    - Phase 1: `Prepare` sent to all non-TM participants, `PrepareAndCommit` to TM
//!    - TM waits for all `Prepared` responses
//!    - Phase 2: TM sends `Confirm` to all participants
//!
//! ## Concurrency Control
//!
//! The system uses **lock groups** for efficient concurrency:
//!
//! - Multiple non-conflicting transactions share a lock group
//! - Read-Read: No conflict
//! - Read-Write or Write-Write: Conflict resolved by timestamp priority
//! - Higher priority (earlier timestamp) wins
//!
//! ## ACID Guarantees
//!
//! - **Atomicity**: 2PC ensures all-or-nothing semantics
//! - **Consistency**: Serializable isolation via lock groups
//! - **Isolation**: Copy-on-write prevents intermediate state visibility
//! - **Durability**: State persisted before commit decision

pub mod agent;
pub mod clock;
pub mod error;
pub mod lock;
pub mod options;
pub mod storage;
pub mod transaction_info;
pub mod transactional_state;

// Re-export main types
pub use agent::{TransactionAgent, TransactionAgentBuilder, TransactionOverloadDetector};
pub use clock::CausalClock;
pub use error::{AbortedReason, TransactionError, TransactionResult, TransactionalStatus};
pub use lock::{CommitRole, LockGroup, ReaderWriterLock, TransactionRecord};
pub use options::{TransactionAgentOptions, TransactionalStateOptions};
pub use storage::{
    CommitRecord, ITransactionalStateStorage, ITransactionalStateStorageEvents,
    InMemoryTransactionalStorage, PendingTransactionState, StorageBatch,
    TransactionalStateMetaData, TransactionalStorageLoadResponse,
};
pub use transaction_info::{AccessCounter, ParticipantId, Role, TransactionId, TransactionInfo};
pub use transactional_state::{ITransactionalState, TransactionContext, TransactionalState};

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone, Default, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct BankAccount {
        balance: i64,
    }

    fn make_grain_id(name: &str) -> GrainId {
        GrainId::new(GrainType::create(name), IdSpan::from_str("account1"))
    }

    #[tokio::test]
    async fn test_full_transaction_flow() {
        // Setup
        let agent = TransactionAgent::new(TransactionAgentOptions::for_testing());
        let storage = Arc::new(InMemoryTransactionalStorage::<BankAccount>::new());
        let options = TransactionalStateOptions::for_testing();
        let state = TransactionalState::new(
            make_grain_id("BankAccount"),
            "balance",
            storage,
            options,
        );

        // Activate
        state.on_activate().await.unwrap();

        // Start transaction
        let info = agent.start_transaction(false, None).unwrap();
        let tx_id = info.transaction_id;

        // Deposit money
        state
            .perform_update(tx_id, |s| {
                s.balance += 100;
            })
            .await
            .unwrap();

        // Verify in-transaction state
        let balance = state.perform_read(tx_id, |s| s.balance).await.unwrap();
        assert_eq!(balance, 100);

        // Commit
        state.commit(tx_id).await.unwrap();

        // Verify committed state in new transaction
        let tx2 = agent.start_transaction(true, None).unwrap();
        let balance = state.perform_read(tx2.transaction_id, |s| s.balance).await.unwrap();
        assert_eq!(balance, 100);

        // Deactivate
        state.on_deactivate().await.unwrap();
    }

    #[tokio::test]
    async fn test_transaction_abort() {
        let agent = TransactionAgent::new(TransactionAgentOptions::for_testing());
        let storage = Arc::new(InMemoryTransactionalStorage::<BankAccount>::new());
        let options = TransactionalStateOptions::for_testing();
        let state = TransactionalState::new(
            make_grain_id("BankAccount"),
            "balance",
            storage,
            options,
        );

        state.on_activate().await.unwrap();

        // Start transaction and make changes
        let info = agent.start_transaction(false, None).unwrap();
        state
            .perform_update(info.transaction_id, |s| {
                s.balance += 500;
            })
            .await
            .unwrap();

        // Abort
        state.abort(info.transaction_id, None).await.unwrap();
        agent.abort(info.transaction_id, None).await;

        // Verify balance is unchanged
        let tx2 = agent.start_transaction(true, None).unwrap();
        let balance = state.perform_read(tx2.transaction_id, |s| s.balance).await.unwrap();
        assert_eq!(balance, 0);
    }

    #[tokio::test]
    async fn test_multiple_transactions() {
        let agent = TransactionAgent::new(TransactionAgentOptions::for_testing());
        let storage = Arc::new(InMemoryTransactionalStorage::<BankAccount>::new());
        let options = TransactionalStateOptions::for_testing();
        let state = TransactionalState::new(
            make_grain_id("BankAccount"),
            "balance",
            storage,
            options,
        );

        state.on_activate().await.unwrap();

        // First transaction: deposit
        let tx1 = agent.start_transaction(false, None).unwrap();
        state
            .perform_update(tx1.transaction_id, |s| {
                s.balance = 1000;
            })
            .await
            .unwrap();
        state.commit(tx1.transaction_id).await.unwrap();

        // Second transaction: withdraw
        let tx2 = agent.start_transaction(false, None).unwrap();
        state
            .perform_update(tx2.transaction_id, |s| {
                s.balance -= 300;
            })
            .await
            .unwrap();
        state.commit(tx2.transaction_id).await.unwrap();

        // Verify final balance
        let tx3 = agent.start_transaction(true, None).unwrap();
        let balance = state.perform_read(tx3.transaction_id, |s| s.balance).await.unwrap();
        assert_eq!(balance, 700);
    }

    #[test]
    fn test_causal_clock_ordering() {
        let clock = CausalClock::new();

        let t1 = clock.utc_now();
        let t2 = clock.utc_now();
        let t3 = clock.utc_now();

        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn test_transactional_status() {
        assert!(TransactionalStatus::Ok.is_success());
        assert!(!TransactionalStatus::Ok.definitely_aborted());

        assert!(!TransactionalStatus::PrepareTimeout.is_success());
        assert!(TransactionalStatus::PrepareTimeout.definitely_aborted());

        assert!(TransactionalStatus::TMResponseTimeout.is_in_doubt());
    }

    #[test]
    fn test_access_counter() {
        let mut counter = AccessCounter::new();
        assert!(counter.is_read_only());

        counter.increment_read();
        assert!(counter.is_read_only());

        counter.increment_write();
        assert!(!counter.is_read_only());
        assert!(counter.has_writes());
        assert_eq!(counter.total(), 2);
    }

    #[test]
    fn test_transaction_info_participant_tracking() {
        let info = TransactionInfo::new(
            TransactionId::new(),
            chrono::Utc::now(),
            Duration::from_secs(30),
        );

        assert!(info.is_read_only);
        assert_eq!(info.participant_count(), 0);
    }

    #[test]
    fn test_storage_batch() {
        let mut batch = StorageBatch::<BankAccount>::new();
        assert!(batch.is_empty());

        batch.confirm(1);
        batch.confirm(5);
        assert_eq!(batch.confirm_up_to, Some(5));

        batch.cancel(10);
        batch.cancel(3);
        assert_eq!(batch.cancel_above, Some(3));
    }
}
