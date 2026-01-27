//! Transaction Agent for orchestrating 2PC protocol.
//!
//! The Transaction Agent is responsible for:
//! - Starting transactions with unique IDs and timestamps
//! - Tracking participants and their access patterns
//! - Resolving transactions (commit or abort)
//! - Coordinating the two-phase commit protocol

use crate::clock::CausalClock;
use crate::error::{AbortedReason, TransactionError, TransactionResult, TransactionalStatus};
use crate::options::TransactionAgentOptions;
use crate::transaction_info::{ParticipantId, TransactionId, TransactionInfo};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, info, instrument, trace, warn};

/// Detects transaction overload conditions.
#[derive(Debug)]
pub struct TransactionOverloadDetector {
    /// Current number of pending transactions.
    pending_count: AtomicUsize,
    /// Threshold for overload detection.
    threshold: usize,
    /// Whether overload detection is enabled.
    enabled: bool,
}

impl TransactionOverloadDetector {
    /// Creates a new overload detector.
    pub fn new(threshold: usize, enabled: bool) -> Self {
        Self {
            pending_count: AtomicUsize::new(0),
            threshold,
            enabled,
        }
    }

    /// Checks if the system is overloaded.
    pub fn is_overloaded(&self) -> bool {
        self.enabled && self.pending_count.load(Ordering::Relaxed) >= self.threshold
    }

    /// Increments the pending transaction count.
    pub fn increment(&self) {
        self.pending_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the pending transaction count.
    pub fn decrement(&self) {
        self.pending_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// Gets the current pending count.
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Relaxed)
    }
}

/// Transaction Agent that orchestrates the 2PC protocol.
pub struct TransactionAgent {
    /// Causal clock for timestamp generation.
    clock: CausalClock,
    /// Overload detector.
    overload_detector: TransactionOverloadDetector,
    /// Configuration options.
    options: TransactionAgentOptions,
    /// Active transactions.
    active_transactions: DashMap<TransactionId, TransactionInfo>,
}

impl TransactionAgent {
    /// Creates a new transaction agent.
    pub fn new(options: TransactionAgentOptions) -> Self {
        let overload_detector = TransactionOverloadDetector::new(
            options.overload_threshold,
            options.enable_overload_detection,
        );

        Self {
            clock: CausalClock::new(),
            overload_detector,
            options,
            active_transactions: DashMap::new(),
        }
    }

    /// Starts a new transaction.
    #[instrument(skip(self), fields(read_only = read_only))]
    pub fn start_transaction(
        &self,
        read_only: bool,
        timeout: Option<Duration>,
    ) -> TransactionResult<TransactionInfo> {
        // Check for overload
        if self.overload_detector.is_overloaded() {
            warn!(
                pending = self.overload_detector.pending_count(),
                threshold = self.options.overload_threshold,
                "Transaction system overloaded"
            );
            return Err(TransactionError::Overloaded);
        }

        // Check capacity
        if self.active_transactions.len() >= self.options.max_concurrent_transactions {
            warn!(
                active = self.active_transactions.len(),
                max = self.options.max_concurrent_transactions,
                "Maximum concurrent transactions reached"
            );
            return Err(TransactionError::Overloaded);
        }

        // Generate unique ID and timestamp
        let transaction_id = TransactionId::new();
        let timestamp = self.clock.utc_now();
        let timeout = timeout.unwrap_or(self.options.default_timeout);

        // Create transaction info
        let mut info = TransactionInfo::new(transaction_id, timestamp, timeout);
        info.is_read_only = read_only;

        // Track the transaction
        self.active_transactions.insert(transaction_id, info.clone());
        self.overload_detector.increment();

        info!(
            transaction_id = %transaction_id,
            read_only = read_only,
            timeout_secs = timeout.as_secs(),
            "Started transaction"
        );

        Ok(info)
    }

    /// Records a read operation for a participant.
    pub fn record_read(&self, tx_id: TransactionId, participant: ParticipantId) -> TransactionResult<()> {
        if let Some(mut info) = self.active_transactions.get_mut(&tx_id) {
            info.record_read(participant);
            Ok(())
        } else {
            Err(TransactionError::NotFound(format!(
                "Transaction {} not found",
                tx_id
            )))
        }
    }

    /// Records a write operation for a participant.
    pub fn record_write(&self, tx_id: TransactionId, participant: ParticipantId) -> TransactionResult<()> {
        if let Some(mut info) = self.active_transactions.get_mut(&tx_id) {
            info.record_write(participant);
            Ok(())
        } else {
            Err(TransactionError::NotFound(format!(
                "Transaction {} not found",
                tx_id
            )))
        }
    }

    /// Gets a transaction's info.
    pub fn get_transaction(&self, tx_id: TransactionId) -> TransactionResult<TransactionInfo> {
        self.active_transactions
            .get(&tx_id)
            .map(|r| r.clone())
            .ok_or_else(|| TransactionError::NotFound(format!("Transaction {} not found", tx_id)))
    }

    /// Resolves a transaction (commit or abort).
    #[instrument(skip(self), fields(transaction_id = %tx_id))]
    pub async fn resolve(&self, tx_id: TransactionId) -> TransactionResult<TransactionalStatus> {
        let info = self.get_transaction(tx_id)?;

        // Check if expired
        if info.is_expired() {
            self.cleanup_transaction(tx_id);
            warn!(
                transaction_id = %tx_id,
                "Transaction expired"
            );
            return Err(TransactionError::Timeout(info.timeout));
        }

        let status = if info.is_read_only {
            self.commit_read_only(&info).await?
        } else {
            self.commit_read_write(&info).await?
        };

        // Cleanup on success
        if status.is_success() {
            self.cleanup_transaction(tx_id);
        }

        debug!(
            transaction_id = %tx_id,
            status = %status,
            "Transaction resolved"
        );

        Ok(status)
    }

    /// Commits a read-only transaction (1-phase).
    #[instrument(skip(self, info), fields(transaction_id = %info.transaction_id))]
    async fn commit_read_only(&self, info: &TransactionInfo) -> TransactionResult<TransactionalStatus> {
        trace!(
            participants = info.participant_count(),
            "Committing read-only transaction"
        );

        // For read-only transactions, just release all participants
        // In a full implementation, this would send CommitReadOnly to all participants

        Ok(TransactionalStatus::Ok)
    }

    /// Commits a read-write transaction (2-phase).
    #[instrument(skip(self, info), fields(transaction_id = %info.transaction_id))]
    async fn commit_read_write(&self, info: &TransactionInfo) -> TransactionResult<TransactionalStatus> {
        // Select transaction manager
        let mut info = info.clone();
        let tm = info.select_transaction_manager().ok_or_else(|| {
            TransactionError::InvalidState("No write participants to select TM".to_string())
        })?;

        debug!(
            transaction_manager = %tm,
            write_participants = info.write_participants().len(),
            "Selected transaction manager"
        );

        // Phase 1: Prepare
        let prepare_result = self.prepare_phase(&info, &tm).await;
        if let Err(e) = prepare_result {
            warn!(
                transaction_id = %info.transaction_id,
                error = %e,
                "Prepare phase failed"
            );
            return Ok(TransactionalStatus::PrepareTimeout);
        }

        // Phase 2: Commit
        let commit_result = self.commit_phase(&info, &tm).await;
        if let Err(e) = commit_result {
            warn!(
                transaction_id = %info.transaction_id,
                error = %e,
                "Commit phase failed"
            );
            return Ok(TransactionalStatus::CommitFailure);
        }

        info!(
            transaction_id = %info.transaction_id,
            "Transaction committed successfully"
        );

        Ok(TransactionalStatus::Ok)
    }

    /// Execute Phase 1: Prepare.
    async fn prepare_phase(
        &self,
        info: &TransactionInfo,
        _tm: &ParticipantId,
    ) -> TransactionResult<()> {
        trace!(
            transaction_id = %info.transaction_id,
            "Executing prepare phase"
        );

        // In a full implementation, this would:
        // 1. Send Prepare to all non-TM participants
        // 2. Send PrepareAndCommit to TM
        // 3. Wait for TM response

        // For now, simulate success with timeout
        tokio::time::timeout(self.options.default_timeout, async {
            // Simulate prepare work
            tokio::time::sleep(Duration::from_millis(1)).await;
        })
        .await
        .map_err(|_| TransactionError::Timeout(self.options.default_timeout))?;

        Ok(())
    }

    /// Execute Phase 2: Commit.
    async fn commit_phase(
        &self,
        info: &TransactionInfo,
        _tm: &ParticipantId,
    ) -> TransactionResult<()> {
        trace!(
            transaction_id = %info.transaction_id,
            "Executing commit phase"
        );

        // In a full implementation, this would:
        // 1. TM sends Confirm to all participants
        // 2. Wait for confirmations
        // 3. Clean up commit records

        Ok(())
    }

    /// Aborts a transaction.
    #[instrument(skip(self), fields(transaction_id = %tx_id))]
    pub async fn abort(&self, tx_id: TransactionId, reason: Option<AbortedReason>) {
        let info = self.active_transactions.remove(&tx_id);
        if info.is_none() {
            trace!(
                transaction_id = %tx_id,
                "Transaction not found for abort"
            );
            return;
        }

        self.overload_detector.decrement();

        debug!(
            transaction_id = %tx_id,
            reason = ?reason,
            "Transaction aborted"
        );

        // In a full implementation, send Cancel to all participants
    }

    /// Cleans up a completed transaction.
    fn cleanup_transaction(&self, tx_id: TransactionId) {
        if self.active_transactions.remove(&tx_id).is_some() {
            self.overload_detector.decrement();
            trace!(
                transaction_id = %tx_id,
                "Transaction cleaned up"
            );
        }
    }

    /// Gets the number of active transactions.
    pub fn active_transaction_count(&self) -> usize {
        self.active_transactions.len()
    }

    /// Gets the pending transaction count from overload detector.
    pub fn pending_count(&self) -> usize {
        self.overload_detector.pending_count()
    }

    /// Merges an external timestamp into the causal clock.
    pub fn merge_timestamp(&self, external: DateTime<Utc>) -> DateTime<Utc> {
        self.clock.merge_utc_now(external)
    }
}

impl Default for TransactionAgent {
    fn default() -> Self {
        Self::new(TransactionAgentOptions::default())
    }
}

/// Builder for TransactionAgent.
pub struct TransactionAgentBuilder {
    options: TransactionAgentOptions,
}

impl TransactionAgentBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self {
            options: TransactionAgentOptions::default(),
        }
    }

    /// Sets the default timeout.
    pub fn with_default_timeout(mut self, timeout: Duration) -> Self {
        self.options.default_timeout = timeout;
        self
    }

    /// Sets the maximum concurrent transactions.
    pub fn with_max_concurrent_transactions(mut self, max: usize) -> Self {
        self.options.max_concurrent_transactions = max;
        self
    }

    /// Enables or disables overload detection.
    pub fn with_overload_detection(mut self, enabled: bool) -> Self {
        self.options.enable_overload_detection = enabled;
        self
    }

    /// Sets the overload threshold.
    pub fn with_overload_threshold(mut self, threshold: usize) -> Self {
        self.options.overload_threshold = threshold;
        self
    }

    /// Builds the TransactionAgent.
    pub fn build(self) -> TransactionAgent {
        TransactionAgent::new(self.options)
    }
}

impl Default for TransactionAgentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainId, GrainType, IdSpan};

    fn make_participant(name: &str) -> ParticipantId {
        let grain_id = GrainId::new(GrainType::create("Test"), IdSpan::from_str("key"));
        ParticipantId::new(name, grain_id)
    }

    #[test]
    fn test_overload_detector() {
        let detector = TransactionOverloadDetector::new(5, true);
        assert!(!detector.is_overloaded());
        assert_eq!(detector.pending_count(), 0);

        for _ in 0..5 {
            detector.increment();
        }
        assert!(detector.is_overloaded());

        detector.decrement();
        assert!(!detector.is_overloaded());
    }

    #[test]
    fn test_overload_detector_disabled() {
        let detector = TransactionOverloadDetector::new(5, false);
        for _ in 0..10 {
            detector.increment();
        }
        assert!(!detector.is_overloaded()); // Disabled
    }

    #[test]
    fn test_transaction_agent_new() {
        let agent = TransactionAgent::default();
        assert_eq!(agent.active_transaction_count(), 0);
    }

    #[test]
    fn test_transaction_agent_start_transaction() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(false, None).unwrap();

        assert!(!info.is_read_only);
        assert_eq!(agent.active_transaction_count(), 1);
    }

    #[test]
    fn test_transaction_agent_start_read_only() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(true, None).unwrap();

        assert!(info.is_read_only);
    }

    #[test]
    fn test_transaction_agent_record_operations() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(false, None).unwrap();

        let p1 = make_participant("state1");
        let p2 = make_participant("state2");

        agent.record_read(info.transaction_id, p1.clone()).unwrap();
        agent.record_write(info.transaction_id, p2.clone()).unwrap();

        let updated = agent.get_transaction(info.transaction_id).unwrap();
        assert_eq!(updated.participant_count(), 2);
        assert!(!updated.is_read_only);
    }

    #[test]
    fn test_transaction_agent_overload() {
        let options = TransactionAgentOptions {
            max_concurrent_transactions: 2,
            enable_overload_detection: true,
            overload_threshold: 2,
            ..Default::default()
        };
        let agent = TransactionAgent::new(options);

        // Start two transactions
        agent.start_transaction(false, None).unwrap();
        agent.start_transaction(false, None).unwrap();

        // Third should fail
        let result = agent.start_transaction(false, None);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), TransactionError::Overloaded));
    }

    #[tokio::test]
    async fn test_transaction_agent_resolve_read_only() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(true, None).unwrap();

        let status = agent.resolve(info.transaction_id).await.unwrap();
        assert_eq!(status, TransactionalStatus::Ok);
        assert_eq!(agent.active_transaction_count(), 0);
    }

    #[tokio::test]
    async fn test_transaction_agent_resolve_read_write() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(false, None).unwrap();

        // Add a write participant
        let p = make_participant("state");
        agent.record_write(info.transaction_id, p).unwrap();

        let status = agent.resolve(info.transaction_id).await.unwrap();
        assert_eq!(status, TransactionalStatus::Ok);
        assert_eq!(agent.active_transaction_count(), 0);
    }

    #[tokio::test]
    async fn test_transaction_agent_abort() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(false, None).unwrap();

        agent.abort(info.transaction_id, None).await;
        assert_eq!(agent.active_transaction_count(), 0);
    }

    #[test]
    fn test_transaction_agent_get_transaction() {
        let agent = TransactionAgent::default();
        let info = agent.start_transaction(false, None).unwrap();

        let retrieved = agent.get_transaction(info.transaction_id).unwrap();
        assert_eq!(retrieved.transaction_id, info.transaction_id);
    }

    #[test]
    fn test_transaction_agent_get_transaction_not_found() {
        let agent = TransactionAgent::default();
        let result = agent.get_transaction(TransactionId::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_agent_merge_timestamp() {
        let agent = TransactionAgent::default();
        let external = Utc::now() + chrono::Duration::hours(1);
        let merged = agent.merge_timestamp(external);
        assert!(merged > external);
    }

    #[test]
    fn test_transaction_agent_builder() {
        let agent = TransactionAgentBuilder::new()
            .with_default_timeout(Duration::from_secs(60))
            .with_max_concurrent_transactions(50)
            .with_overload_detection(false)
            .build();

        // Start many transactions
        for _ in 0..50 {
            agent.start_transaction(false, None).unwrap();
        }

        // 51st should fail
        let result = agent.start_transaction(false, None);
        assert!(result.is_err());
    }
}
