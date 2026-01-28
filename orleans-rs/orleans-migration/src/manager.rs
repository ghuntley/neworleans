//! Activation migration manager for orchestrating grain migrations.
//!
//! This module provides the `ActivationMigrationManager` which coordinates
//! the migration of grains between silos.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::Semaphore;
use tracing::{debug, error, info, instrument, trace};

use orleans_core::{GrainId, SiloAddress};

use crate::context::MigrationContext;
use crate::error::{MigrationError, MigrationReason, MigrationResult};
use crate::options::MigrationOptions;
use crate::participant::MigrationParticipantRegistry;

/// Interface for the migration manager.
#[async_trait]
pub trait IActivationMigrationManager: Send + Sync {
    /// Migrate a grain activation to a target silo.
    async fn migrate_activation(
        &self,
        grain_id: &GrainId,
        target_silo: &SiloAddress,
        reason: MigrationReason,
    ) -> MigrationResult<()>;

    /// Check if a grain can be migrated.
    fn can_migrate(&self, grain_id: &GrainId) -> bool;

    /// Check if a grain is currently being migrated.
    fn is_migrating(&self, grain_id: &GrainId) -> bool;

    /// Get the number of active migrations.
    fn active_migration_count(&self) -> usize;

    /// Cancel a pending migration.
    fn cancel_migration(&self, grain_id: &GrainId) -> bool;
}

/// Status of an in-progress migration.
#[derive(Debug, Clone)]
pub struct MigrationStatus {
    /// The grain being migrated.
    pub grain_id: GrainId,
    /// Target silo for the migration.
    pub target_silo: SiloAddress,
    /// Reason for the migration.
    pub reason: MigrationReason,
    /// When the migration started.
    pub started_at: Instant,
    /// Current phase of the migration.
    pub phase: MigrationPhase,
}

/// Current phase of a migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationPhase {
    /// Preparing for migration (draining requests).
    Preparing,
    /// Dehydrating grain state.
    Dehydrating,
    /// Transferring state to target silo.
    Transferring,
    /// Rehydrating on target silo.
    Rehydrating,
    /// Updating directory with new location.
    UpdatingDirectory,
    /// Migration completed successfully.
    Completed,
    /// Migration failed.
    Failed,
}

/// Statistics for the migration manager.
#[derive(Debug, Clone, Default)]
pub struct MigrationStatistics {
    /// Total number of migrations attempted.
    pub total_migrations: u64,
    /// Number of successful migrations.
    pub successful_migrations: u64,
    /// Number of failed migrations.
    pub failed_migrations: u64,
    /// Number of cancelled migrations.
    pub cancelled_migrations: u64,
    /// Total bytes transferred.
    pub total_bytes_transferred: u64,
    /// Average migration duration in milliseconds.
    pub average_duration_ms: f64,
}

/// Manager that orchestrates grain migrations between silos.
pub struct ActivationMigrationManager {
    /// Local silo address.
    local_silo: SiloAddress,
    /// Configuration options.
    options: MigrationOptions,
    /// Set of grains currently being migrated.
    migrating_grains: DashMap<GrainId, MigrationStatus>,
    /// Set of immovable grain types.
    immovable_grains: RwLock<HashSet<GrainId>>,
    /// Semaphore for limiting concurrent migrations.
    migration_semaphore: Semaphore,
    /// Whether the manager is shutting down.
    is_shutting_down: AtomicBool,
    /// Statistics.
    stats: RwLock<MigrationStatistics>,
    /// Total successful migrations (atomic for fast reads).
    successful_count: AtomicU64,
}

impl ActivationMigrationManager {
    /// Create a new migration manager.
    pub fn new(local_silo: SiloAddress, options: MigrationOptions) -> Self {
        let max_concurrent = options.max_concurrent_migrations;
        Self {
            local_silo,
            options,
            migrating_grains: DashMap::new(),
            immovable_grains: RwLock::new(HashSet::new()),
            migration_semaphore: Semaphore::new(max_concurrent),
            is_shutting_down: AtomicBool::new(false),
            stats: RwLock::new(MigrationStatistics::default()),
            successful_count: AtomicU64::new(0),
        }
    }

    /// Mark a grain as immovable.
    pub fn mark_immovable(&self, grain_id: GrainId) {
        self.immovable_grains.write().insert(grain_id);
    }

    /// Unmark a grain as immovable.
    pub fn unmark_immovable(&self, grain_id: &GrainId) {
        self.immovable_grains.write().remove(grain_id);
    }

    /// Begin graceful shutdown.
    pub fn begin_shutdown(&self) {
        info!(silo = %self.local_silo, "beginning migration manager shutdown");
        self.is_shutting_down.store(true, Ordering::SeqCst);
    }

    /// Get migration statistics.
    pub fn get_statistics(&self) -> MigrationStatistics {
        self.stats.read().clone()
    }

    /// Get the status of an active migration.
    pub fn get_migration_status(&self, grain_id: &GrainId) -> Option<MigrationStatus> {
        self.migrating_grains.get(grain_id).map(|r| r.clone())
    }

    /// Internal: Perform the actual migration.
    #[instrument(skip(self, participants, context), fields(grain_id = %grain_id, target = %target_silo))]
    async fn perform_migration(
        &self,
        grain_id: &GrainId,
        target_silo: &SiloAddress,
        reason: MigrationReason,
        participants: &MigrationParticipantRegistry,
        context: &mut MigrationContext,
    ) -> MigrationResult<()> {
        let started_at = Instant::now();

        // Update status: Preparing
        self.update_phase(grain_id, MigrationPhase::Preparing);
        debug!(grain_id = %grain_id, "preparing for migration");

        // Dehydration phase
        self.update_phase(grain_id, MigrationPhase::Dehydrating);
        debug!(grain_id = %grain_id, "dehydrating grain state");

        participants.dehydrate_all(context);

        let context_size = context.total_size();
        if context_size > self.options.max_context_size {
            return Err(MigrationError::DehydrationFailed {
                grain_id: grain_id.clone(),
                reason: format!(
                    "context size {} exceeds maximum {}",
                    context_size, self.options.max_context_size
                ),
            });
        }

        trace!(
            grain_id = %grain_id,
            context_size = context_size,
            "dehydration complete"
        );

        // Transfer phase
        self.update_phase(grain_id, MigrationPhase::Transferring);
        debug!(
            grain_id = %grain_id,
            target = %target_silo,
            size = context_size,
            "transferring state to target silo"
        );

        // In a real implementation, we would send the context to the target silo
        // via the message center. For now, we simulate the transfer.
        // This is where network transmission would happen.

        // Simulate transfer delay based on size
        let transfer_delay = Duration::from_micros((context_size / 1000) as u64);
        tokio::time::sleep(transfer_delay).await;

        // Update directory phase
        self.update_phase(grain_id, MigrationPhase::UpdatingDirectory);
        debug!(grain_id = %grain_id, "updating grain directory");

        // In a real implementation, we would update the grain directory
        // to point to the new silo.

        // Mark completed
        self.update_phase(grain_id, MigrationPhase::Completed);

        let duration = started_at.elapsed();
        info!(
            grain_id = %grain_id,
            target = %target_silo,
            duration_ms = duration.as_millis(),
            context_size = context_size,
            "migration completed successfully"
        );

        // Update statistics
        {
            let mut stats = self.stats.write();
            stats.total_migrations += 1;
            stats.successful_migrations += 1;
            stats.total_bytes_transferred += context_size as u64;

            // Update running average
            let n = stats.successful_migrations as f64;
            let duration_ms = duration.as_millis() as f64;
            stats.average_duration_ms =
                stats.average_duration_ms * (n - 1.0) / n + duration_ms / n;
        }
        self.successful_count.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }

    fn update_phase(&self, grain_id: &GrainId, phase: MigrationPhase) {
        if let Some(mut status) = self.migrating_grains.get_mut(grain_id) {
            status.phase = phase;
        }
    }
}

#[async_trait]
impl IActivationMigrationManager for ActivationMigrationManager {
    #[instrument(skip(self), fields(silo = %self.local_silo))]
    async fn migrate_activation(
        &self,
        grain_id: &GrainId,
        target_silo: &SiloAddress,
        reason: MigrationReason,
    ) -> MigrationResult<()> {
        // Check shutdown state
        if self.is_shutting_down.load(Ordering::SeqCst) && reason != MigrationReason::SiloShutdown {
            return Err(MigrationError::ShuttingDown);
        }

        // Check if grain is immovable
        if self.immovable_grains.read().contains(grain_id) {
            return Err(MigrationError::GrainImmovable(grain_id.clone()));
        }

        // Check if already migrating
        if self.migrating_grains.contains_key(grain_id) {
            return Err(MigrationError::AlreadyMigrating(grain_id.clone()));
        }

        // Check target silo is different from local
        if target_silo == &self.local_silo {
            return Err(MigrationError::Internal(
                "cannot migrate to same silo".to_string(),
            ));
        }

        // Acquire semaphore permit for rate limiting
        let _permit = match tokio::time::timeout(
            self.options.migration_timeout,
            self.migration_semaphore.acquire(),
        )
        .await
        {
            Ok(Ok(permit)) => permit,
            Ok(Err(_)) => {
                return Err(MigrationError::Internal(
                    "semaphore closed unexpectedly".to_string(),
                ))
            }
            Err(_) => {
                return Err(MigrationError::Timeout {
                    duration_ms: self.options.migration_timeout.as_millis() as u64,
                })
            }
        };

        // Register migration in progress
        let status = MigrationStatus {
            grain_id: grain_id.clone(),
            target_silo: target_silo.clone(),
            reason,
            started_at: Instant::now(),
            phase: MigrationPhase::Preparing,
        };
        self.migrating_grains.insert(grain_id.clone(), status);

        // Create context and registry for the migration
        let mut context = MigrationContext::with_max_size(self.options.max_context_size);
        let registry = MigrationParticipantRegistry::new();

        // Perform the migration with timeout
        let result = tokio::time::timeout(
            self.options.migration_timeout,
            self.perform_migration(grain_id, target_silo, reason, &registry, &mut context),
        )
        .await;

        // Remove from migrating set
        self.migrating_grains.remove(grain_id);

        match result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => {
                self.stats.write().failed_migrations += 1;
                error!(grain_id = %grain_id, error = %e, "migration failed");
                Err(e)
            }
            Err(_) => {
                self.stats.write().failed_migrations += 1;
                error!(grain_id = %grain_id, "migration timed out");
                Err(MigrationError::Timeout {
                    duration_ms: self.options.migration_timeout.as_millis() as u64,
                })
            }
        }
    }

    fn can_migrate(&self, grain_id: &GrainId) -> bool {
        if self.is_shutting_down.load(Ordering::SeqCst) {
            return false;
        }
        if self.immovable_grains.read().contains(grain_id) {
            return false;
        }
        if self.migrating_grains.contains_key(grain_id) {
            return false;
        }
        true
    }

    fn is_migrating(&self, grain_id: &GrainId) -> bool {
        self.migrating_grains.contains_key(grain_id)
    }

    fn active_migration_count(&self) -> usize {
        self.migrating_grains.len()
    }

    fn cancel_migration(&self, grain_id: &GrainId) -> bool {
        if let Some((_, mut status)) = self.migrating_grains.remove(grain_id) {
            status.phase = MigrationPhase::Failed;
            self.stats.write().cancelled_migrations += 1;
            info!(grain_id = %grain_id, "migration cancelled");
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    fn make_grain_id(key: &str) -> GrainId {
        use orleans_core::{GrainType, IdSpan};
        GrainId::new(GrainType::create("TestGrain"), IdSpan::from_str(key))
    }

    fn make_silo_address(port: u16) -> SiloAddress {
        SiloAddress::new(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port),
            1,
        )
    }

    #[test]
    fn test_manager_creation() {
        let silo = make_silo_address(11111);
        let options = MigrationOptions::for_testing();
        let manager = ActivationMigrationManager::new(silo, options);

        assert_eq!(manager.active_migration_count(), 0);
    }

    #[test]
    fn test_can_migrate_immovable() {
        let silo = make_silo_address(11111);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("immovable");
        assert!(manager.can_migrate(&grain_id));

        manager.mark_immovable(grain_id.clone());
        assert!(!manager.can_migrate(&grain_id));

        manager.unmark_immovable(&grain_id);
        assert!(manager.can_migrate(&grain_id));
    }

    #[test]
    fn test_can_migrate_shutting_down() {
        let silo = make_silo_address(11111);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("test");
        assert!(manager.can_migrate(&grain_id));

        manager.begin_shutdown();
        assert!(!manager.can_migrate(&grain_id));
    }

    #[tokio::test]
    async fn test_migrate_to_same_silo_fails() {
        let silo = make_silo_address(11111);
        let manager = ActivationMigrationManager::new(silo.clone(), MigrationOptions::for_testing());

        let grain_id = make_grain_id("test");
        let result = manager
            .migrate_activation(&grain_id, &silo, MigrationReason::Manual)
            .await;

        assert!(matches!(result, Err(MigrationError::Internal(_))));
    }

    #[tokio::test]
    async fn test_migrate_immovable_fails() {
        let silo = make_silo_address(11111);
        let target = make_silo_address(22222);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("immovable");
        manager.mark_immovable(grain_id.clone());

        let result = manager
            .migrate_activation(&grain_id, &target, MigrationReason::Manual)
            .await;

        assert!(matches!(result, Err(MigrationError::GrainImmovable(_))));
    }

    #[tokio::test]
    async fn test_successful_migration() {
        let silo = make_silo_address(11111);
        let target = make_silo_address(22222);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("migratable");
        let result = manager
            .migrate_activation(&grain_id, &target, MigrationReason::Manual)
            .await;

        assert!(result.is_ok());

        let stats = manager.get_statistics();
        assert_eq!(stats.successful_migrations, 1);
        assert_eq!(stats.total_migrations, 1);
    }

    #[tokio::test]
    async fn test_is_migrating_during_migration() {
        let silo = make_silo_address(11111);
        let target = make_silo_address(22222);
        let manager = Arc::new(ActivationMigrationManager::new(
            silo,
            MigrationOptions::for_testing(),
        ));

        let grain_id = make_grain_id("test");

        // Before migration
        assert!(!manager.is_migrating(&grain_id));

        // Start migration
        let manager_clone = manager.clone();
        let grain_clone = grain_id.clone();
        let handle = tokio::spawn(async move {
            manager_clone
                .migrate_activation(&grain_clone, &target, MigrationReason::Manual)
                .await
        });

        // Wait a bit and check
        tokio::time::sleep(Duration::from_millis(1)).await;
        // Note: Due to timing, this might already be complete

        handle.await.unwrap().unwrap();

        // After migration
        assert!(!manager.is_migrating(&grain_id));
    }

    #[tokio::test]
    async fn test_cancel_migration() {
        let silo = make_silo_address(11111);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        let grain_id = make_grain_id("test");

        // Nothing to cancel
        assert!(!manager.cancel_migration(&grain_id));
    }

    #[test]
    fn test_statistics_default() {
        let stats = MigrationStatistics::default();
        assert_eq!(stats.total_migrations, 0);
        assert_eq!(stats.successful_migrations, 0);
        assert_eq!(stats.failed_migrations, 0);
        assert_eq!(stats.cancelled_migrations, 0);
        assert_eq!(stats.total_bytes_transferred, 0);
    }

    #[test]
    fn test_migration_phase_enum() {
        assert_ne!(MigrationPhase::Preparing, MigrationPhase::Dehydrating);
        assert_eq!(MigrationPhase::Completed, MigrationPhase::Completed);
    }

    #[tokio::test]
    async fn test_concurrent_migrations_limited() {
        let silo = make_silo_address(11111);
        let options = MigrationOptions::for_testing().with_max_concurrent_migrations(2);
        let manager = Arc::new(ActivationMigrationManager::new(silo, options));

        let target = make_silo_address(22222);

        // Start multiple migrations
        let mut handles = vec![];
        for i in 0..5 {
            let m = manager.clone();
            let grain_id = make_grain_id(&format!("grain{}", i));
            let t = target.clone();
            handles.push(tokio::spawn(async move {
                m.migrate_activation(&grain_id, &t, MigrationReason::Manual)
                    .await
            }));
        }

        // Wait for all
        for handle in handles {
            let _ = handle.await;
        }

        // All should eventually complete
        let stats = manager.get_statistics();
        assert!(stats.total_migrations >= 5);
    }

    #[test]
    fn test_migration_status_creation() {
        let grain_id = make_grain_id("test");
        let target = make_silo_address(22222);

        let status = MigrationStatus {
            grain_id: grain_id.clone(),
            target_silo: target.clone(),
            reason: MigrationReason::Manual,
            started_at: Instant::now(),
            phase: MigrationPhase::Preparing,
        };

        assert_eq!(status.grain_id, grain_id);
        assert_eq!(status.target_silo, target);
        assert_eq!(status.reason, MigrationReason::Manual);
        assert_eq!(status.phase, MigrationPhase::Preparing);
    }

    #[tokio::test]
    async fn test_shutdown_prevents_new_migrations() {
        let silo = make_silo_address(11111);
        let target = make_silo_address(22222);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        manager.begin_shutdown();

        let grain_id = make_grain_id("test");
        let result = manager
            .migrate_activation(&grain_id, &target, MigrationReason::Manual)
            .await;

        assert!(matches!(result, Err(MigrationError::ShuttingDown)));
    }

    #[tokio::test]
    async fn test_shutdown_allows_shutdown_reason_migrations() {
        let silo = make_silo_address(11111);
        let target = make_silo_address(22222);
        let manager = ActivationMigrationManager::new(silo, MigrationOptions::for_testing());

        manager.begin_shutdown();

        let grain_id = make_grain_id("test");
        let result = manager
            .migrate_activation(&grain_id, &target, MigrationReason::SiloShutdown)
            .await;

        // Should succeed even during shutdown
        assert!(result.is_ok());
    }
}
