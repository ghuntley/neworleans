//! PostgreSQL implementation of the reminder table.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_core::GrainId;
use orleans_directory::RingRange;
use orleans_reminders::{IReminderTable, ReminderEntry, ReminderError, ReminderResult};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, instrument, warn};

use crate::options::PostgresOptions;
use crate::PostgresResult;

/// PostgreSQL implementation of the reminder table.
///
/// This implementation stores reminders in PostgreSQL with optimistic
/// concurrency control via ETags.
#[derive(Clone)]
pub struct PostgresReminderTable {
    pool: PgPool,
    schema: String,
    etag_counter: std::sync::Arc<AtomicU64>,
}

impl PostgresReminderTable {
    /// Creates a new PostgreSQL reminder table with the given options.
    #[instrument(skip(options), fields(schema = %options.schema))]
    pub async fn new(options: &PostgresOptions) -> PostgresResult<Self> {
        options.validate().map_err(|e| {
            error!(error = %e, "invalid PostgreSQL options");
            crate::PostgresError::Configuration(e)
        })?;

        info!("connecting to PostgreSQL for reminder table");

        let pool = PgPoolOptions::new()
            .min_connections(options.min_connections)
            .max_connections(options.max_connections)
            .acquire_timeout(options.connect_timeout)
            .idle_timeout(options.idle_timeout)
            .max_lifetime(options.max_lifetime)
            .connect(&options.connection_string)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create connection pool");
                crate::PostgresError::ConnectionFailed(e.to_string())
            })?;

        let table = Self {
            pool,
            schema: options.schema.clone(),
            etag_counter: std::sync::Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            )),
        };

        if options.run_migrations {
            table.run_migrations().await?;
        }

        info!("PostgreSQL reminder table initialized");
        Ok(table)
    }

    /// Creates a new PostgreSQL reminder table from an existing pool.
    pub fn from_pool(pool: PgPool, schema: String) -> Self {
        Self {
            pool,
            schema,
            etag_counter: std::sync::Arc::new(AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64,
            )),
        }
    }

    /// Runs the database migrations to create the required tables.
    #[instrument(skip(self))]
    async fn run_migrations(&self) -> PostgresResult<()> {
        info!(schema = %self.schema, "running reminder table migrations");

        // Create schema if not exists
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
        sqlx::query(&create_schema)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create schema");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create reminders table
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.reminders (
                grain_type VARCHAR(255) NOT NULL,
                grain_key VARCHAR(255) NOT NULL,
                grain_hash INTEGER NOT NULL,
                reminder_name VARCHAR(255) NOT NULL,
                start_at TIMESTAMPTZ NOT NULL,
                period_secs BIGINT NOT NULL,
                period_nanos INTEGER NOT NULL,
                etag VARCHAR(64) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (grain_type, grain_key, reminder_name)
            )
            "#,
            schema = self.schema
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create reminders table");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create index on grain_hash for range queries
        let create_hash_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_reminders_grain_hash
            ON {schema}.reminders (grain_hash)
            "#,
            schema = self.schema
        );
        sqlx::query(&create_hash_index)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create hash index");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create index for start_at for scheduling queries
        let create_time_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_reminders_start_at
            ON {schema}.reminders (start_at)
            "#,
            schema = self.schema
        );
        sqlx::query(&create_time_index)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create time index");
                crate::PostgresError::Migration(e.to_string())
            })?;

        info!("reminder table migrations completed");
        Ok(())
    }

    /// Generates a new ETag.
    fn generate_etag(&self) -> String {
        let counter = self.etag_counter.fetch_add(1, Ordering::SeqCst);
        format!("{:016x}", counter)
    }

    /// Converts a database row to a ReminderEntry.
    fn row_to_entry(row: &sqlx::postgres::PgRow) -> ReminderResult<ReminderEntry> {
        let grain_type: String = row.get("grain_type");
        let grain_key: String = row.get("grain_key");

        let grain_id = GrainId::create(&grain_type, &grain_key);

        let reminder_name: String = row.get("reminder_name");
        let start_at: DateTime<Utc> = row.get("start_at");
        let period_secs: i64 = row.get("period_secs");
        let period_nanos: i32 = row.get("period_nanos");
        let etag: String = row.get("etag");

        let period = Duration::new(period_secs as u64, period_nanos as u32);

        Ok(ReminderEntry::new(grain_id, reminder_name, start_at, period).with_etag(etag))
    }

    /// Gets the grain type as a string.
    fn grain_type(grain_id: &GrainId) -> String {
        grain_id.grain_type().to_string()
    }

    /// Gets the grain key as a string.
    fn grain_key(grain_id: &GrainId) -> String {
        grain_id.key().to_string()
    }
}

#[async_trait]
impl IReminderTable for PostgresReminderTable {
    #[instrument(skip(self), fields(grain_id = %grain_id))]
    async fn read_rows(&self, grain_id: &GrainId) -> ReminderResult<Vec<ReminderEntry>> {
        debug!("reading all reminders for grain");

        let query = format!(
            r#"
            SELECT grain_type, grain_key, reminder_name, start_at, period_secs, period_nanos, etag
            FROM {schema}.reminders
            WHERE grain_type = $1 AND grain_key = $2
            ORDER BY reminder_name
            "#,
            schema = self.schema
        );

        let rows = sqlx::query(&query)
            .bind(Self::grain_type(grain_id))
            .bind(Self::grain_key(grain_id))
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read reminders");
                ReminderError::Storage(e.to_string())
            })?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }

        debug!(count = entries.len(), "found reminders");
        Ok(entries)
    }

    #[instrument(skip(self), fields(grain_id = %grain_id, reminder_name = %reminder_name))]
    async fn read_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
    ) -> ReminderResult<Option<ReminderEntry>> {
        debug!("reading reminder");

        let query = format!(
            r#"
            SELECT grain_type, grain_key, reminder_name, start_at, period_secs, period_nanos, etag
            FROM {schema}.reminders
            WHERE grain_type = $1 AND grain_key = $2 AND reminder_name = $3
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(Self::grain_type(grain_id))
            .bind(Self::grain_key(grain_id))
            .bind(reminder_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read reminder");
                ReminderError::Storage(e.to_string())
            })?;

        match result {
            Some(row) => {
                let entry = Self::row_to_entry(&row)?;
                debug!(etag = %entry.etag, "reminder found");
                Ok(Some(entry))
            }
            None => {
                debug!("reminder not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self))]
    async fn read_rows_in_range(&self, range: &RingRange) -> ReminderResult<Vec<ReminderEntry>> {
        debug!("reading reminders in hash range");

        // If the range is empty, return empty
        if range.is_empty() {
            debug!("range is empty, returning no reminders");
            return Ok(Vec::new());
        }

        // Check if the range covers everything (a full range has a single segment with start == end)
        let is_full = range.segments().len() == 1 && {
            let seg = &range.segments()[0];
            seg.start == seg.end
        };

        // If the range covers everything, return all
        if is_full {
            debug!("range is full, reading all reminders");
            let query = format!(
                r#"
                SELECT grain_type, grain_key, reminder_name, start_at, period_secs, period_nanos, etag
                FROM {schema}.reminders
                ORDER BY grain_type, grain_key, reminder_name
                "#,
                schema = self.schema
            );

            let rows = sqlx::query(&query)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    error!(error = %e, "failed to read all reminders");
                    ReminderError::Storage(e.to_string())
                })?;

            let mut entries = Vec::with_capacity(rows.len());
            for row in &rows {
                entries.push(Self::row_to_entry(row)?);
            }

            debug!(count = entries.len(), "found all reminders");
            return Ok(entries);
        }

        // For specific ranges, we need to query based on hash values
        // This is complex because ring ranges can wrap around
        // For simplicity, we'll read all and filter in memory
        // A production implementation might use more sophisticated range queries

        let query = format!(
            r#"
            SELECT grain_type, grain_key, grain_hash, reminder_name, start_at, period_secs, period_nanos, etag
            FROM {schema}.reminders
            ORDER BY grain_type, grain_key, reminder_name
            "#,
            schema = self.schema
        );

        let rows = sqlx::query(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read reminders for range");
                ReminderError::Storage(e.to_string())
            })?;

        let mut entries = Vec::new();
        for row in &rows {
            let grain_hash: i32 = row.get("grain_hash");
            let hash = grain_hash as u32;

            if range.contains(hash) {
                entries.push(Self::row_to_entry(&row)?);
            }
        }

        debug!(count = entries.len(), "found reminders in range");
        Ok(entries)
    }

    #[instrument(skip(self, entry), fields(grain_id = %entry.grain_id, reminder_name = %entry.reminder_name))]
    async fn upsert_row(&self, entry: ReminderEntry) -> ReminderResult<String> {
        let new_etag = self.generate_etag();
        let grain_hash = entry.get_grain_hash_code() as i32;

        debug!("upserting reminder");

        // If etag is empty, this is an insert/update (upsert)
        // If etag is non-empty, we should validate it on update
        if entry.etag.is_empty() || entry.etag == "*" {
            // Unconditional upsert
            let query = format!(
                r#"
                INSERT INTO {schema}.reminders
                    (grain_type, grain_key, grain_hash, reminder_name, start_at, period_secs, period_nanos, etag, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                ON CONFLICT (grain_type, grain_key, reminder_name)
                DO UPDATE SET start_at = $5, period_secs = $6, period_nanos = $7, etag = $8, updated_at = NOW()
                "#,
                schema = self.schema
            );

            sqlx::query(&query)
                .bind(Self::grain_type(&entry.grain_id))
                .bind(Self::grain_key(&entry.grain_id))
                .bind(grain_hash)
                .bind(&entry.reminder_name)
                .bind(entry.start_at)
                .bind(entry.period.as_secs() as i64)
                .bind(entry.period.subsec_nanos() as i32)
                .bind(&new_etag)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    error!(error = %e, "failed to upsert reminder");
                    ReminderError::Storage(e.to_string())
                })?;

            info!(etag = %new_etag, "reminder upserted");
            Ok(new_etag)
        } else {
            // Update with ETag check
            let query = format!(
                r#"
                UPDATE {schema}.reminders
                SET start_at = $1, period_secs = $2, period_nanos = $3, etag = $4, updated_at = NOW()
                WHERE grain_type = $5 AND grain_key = $6 AND reminder_name = $7 AND etag = $8
                "#,
                schema = self.schema
            );

            let result = sqlx::query(&query)
                .bind(entry.start_at)
                .bind(entry.period.as_secs() as i64)
                .bind(entry.period.subsec_nanos() as i32)
                .bind(&new_etag)
                .bind(Self::grain_type(&entry.grain_id))
                .bind(Self::grain_key(&entry.grain_id))
                .bind(&entry.reminder_name)
                .bind(&entry.etag)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    error!(error = %e, "failed to update reminder");
                    ReminderError::Storage(e.to_string())
                })?;

            if result.rows_affected() == 0 {
                // Check if record exists
                let exists = self
                    .read_row(&entry.grain_id, &entry.reminder_name)
                    .await?;

                match exists {
                    Some(existing) => {
                        warn!(
                            stored = %existing.etag,
                            expected = %entry.etag,
                            "etag mismatch"
                        );
                        return Err(ReminderError::EtagMismatch {
                            expected: entry.etag,
                            actual: existing.etag,
                        });
                    }
                    None => {
                        // Record doesn't exist, insert it
                        let insert_query = format!(
                            r#"
                            INSERT INTO {schema}.reminders
                                (grain_type, grain_key, grain_hash, reminder_name, start_at, period_secs, period_nanos, etag)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                            "#,
                            schema = self.schema
                        );

                        sqlx::query(&insert_query)
                            .bind(Self::grain_type(&entry.grain_id))
                            .bind(Self::grain_key(&entry.grain_id))
                            .bind(grain_hash)
                            .bind(&entry.reminder_name)
                            .bind(entry.start_at)
                            .bind(entry.period.as_secs() as i64)
                            .bind(entry.period.subsec_nanos() as i32)
                            .bind(&new_etag)
                            .execute(&self.pool)
                            .await
                            .map_err(|e| {
                                error!(error = %e, "failed to insert reminder");
                                ReminderError::Storage(e.to_string())
                            })?;

                        info!(etag = %new_etag, "reminder inserted (new)");
                        return Ok(new_etag);
                    }
                }
            }

            info!(etag = %new_etag, "reminder updated");
            Ok(new_etag)
        }
    }

    #[instrument(skip(self), fields(grain_id = %grain_id, reminder_name = %reminder_name))]
    async fn remove_row(
        &self,
        grain_id: &GrainId,
        reminder_name: &str,
        etag: &str,
    ) -> ReminderResult<bool> {
        debug!("removing reminder");

        let query = if etag == "*" {
            // Unconditional delete
            format!(
                r#"
                DELETE FROM {schema}.reminders
                WHERE grain_type = $1 AND grain_key = $2 AND reminder_name = $3
                "#,
                schema = self.schema
            )
        } else {
            // Delete with ETag check
            format!(
                r#"
                DELETE FROM {schema}.reminders
                WHERE grain_type = $1 AND grain_key = $2 AND reminder_name = $3 AND etag = $4
                "#,
                schema = self.schema
            )
        };

        let result = if etag == "*" {
            sqlx::query(&query)
                .bind(Self::grain_type(grain_id))
                .bind(Self::grain_key(grain_id))
                .bind(reminder_name)
                .execute(&self.pool)
                .await
        } else {
            sqlx::query(&query)
                .bind(Self::grain_type(grain_id))
                .bind(Self::grain_key(grain_id))
                .bind(reminder_name)
                .bind(etag)
                .execute(&self.pool)
                .await
        };

        let result = result.map_err(|e| {
            error!(error = %e, "failed to remove reminder");
            ReminderError::Storage(e.to_string())
        })?;

        let removed = result.rows_affected() > 0;
        if removed {
            info!("reminder removed");
        } else {
            warn!("reminder not removed (not found or etag mismatch)");
        }

        Ok(removed)
    }

    #[instrument(skip(self))]
    async fn clear_table(&self) -> ReminderResult<()> {
        info!("clearing all reminders");

        let query = format!("DELETE FROM {schema}.reminders", schema = self.schema);

        let result = sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to clear reminders");
                ReminderError::Storage(e.to_string())
            })?;

        info!(deleted = result.rows_affected(), "cleared all reminders");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainType, IdSpan};

    fn make_grain_id(grain_type: &str, key: &str) -> GrainId {
        GrainId::new(GrainType::create(grain_type), IdSpan::from_str(key))
    }

    #[test]
    fn test_grain_type() {
        let grain_id = make_grain_id("my.grain", "test-key");
        let grain_type = PostgresReminderTable::grain_type(&grain_id);
        assert_eq!(grain_type, "my.grain");
    }

    #[test]
    fn test_grain_key() {
        let grain_id = make_grain_id("my.grain", "test-key");
        let key = PostgresReminderTable::grain_key(&grain_id);
        assert_eq!(key, "test-key");
    }

    #[test]
    fn test_etag_generation() {
        // Test the etag generation logic without requiring a real pool
        let counter = std::sync::Arc::new(AtomicU64::new(1000));

        // Generate etags using the same logic as generate_etag
        let etag1 = {
            let counter_val = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("{:016x}", counter_val)
        };
        let etag2 = {
            let counter_val = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            format!("{:016x}", counter_val)
        };

        assert_ne!(etag1, etag2);
        assert_eq!(etag1.len(), 16); // 16 hex chars
        assert_eq!(etag2.len(), 16);
        assert_eq!(etag1, "00000000000003e8"); // 1000 in hex
        assert_eq!(etag2, "00000000000003e9"); // 1001 in hex
    }
}
