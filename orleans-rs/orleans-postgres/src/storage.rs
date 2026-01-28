//! PostgreSQL implementation of grain state storage.

use async_trait::async_trait;
use orleans_core::GrainId;
use orleans_persistence::{IGrainStorage, RawGrainState, StorageError, StorageResult};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::options::PostgresOptions;
use crate::PostgresResult;

/// PostgreSQL implementation of grain state storage.
///
/// This implementation stores grain state in PostgreSQL with optimistic
/// concurrency control via ETags. State is stored as BYTEA (raw bytes).
#[derive(Clone)]
pub struct PostgresGrainStorage {
    pool: PgPool,
    schema: String,
}

impl PostgresGrainStorage {
    /// Creates a new PostgreSQL grain storage with the given options.
    #[instrument(skip(options), fields(schema = %options.schema))]
    pub async fn new(options: &PostgresOptions) -> PostgresResult<Self> {
        options.validate().map_err(|e| {
            error!(error = %e, "invalid PostgreSQL options");
            crate::PostgresError::Configuration(e)
        })?;

        info!("connecting to PostgreSQL for grain storage");

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

        let storage = Self {
            pool,
            schema: options.schema.clone(),
        };

        if options.run_migrations {
            storage.run_migrations().await?;
        }

        info!("PostgreSQL grain storage initialized");
        Ok(storage)
    }

    /// Creates a new PostgreSQL grain storage from an existing pool.
    pub fn from_pool(pool: PgPool, schema: String) -> Self {
        Self { pool, schema }
    }

    /// Runs the database migrations to create the required tables.
    #[instrument(skip(self))]
    async fn run_migrations(&self) -> PostgresResult<()> {
        info!(schema = %self.schema, "running grain storage migrations");

        // Create schema if not exists
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
        sqlx::query(&create_schema)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create schema");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create grain state table
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.grain_state (
                grain_type VARCHAR(255) NOT NULL,
                grain_key VARCHAR(255) NOT NULL,
                state_name VARCHAR(255) NOT NULL,
                state_data BYTEA NOT NULL,
                etag VARCHAR(64) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (grain_type, grain_key, state_name)
            )
            "#,
            schema = self.schema
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create grain_state table");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create index on updated_at for cleanup queries
        let create_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_grain_state_updated_at
            ON {schema}.grain_state (updated_at)
            "#,
            schema = self.schema
        );
        sqlx::query(&create_index)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create index");
                crate::PostgresError::Migration(e.to_string())
            })?;

        info!("grain storage migrations completed");
        Ok(())
    }

    /// Constructs the storage key from grain ID.
    fn grain_key(grain_id: &GrainId) -> String {
        grain_id.key().to_string()
    }

    /// Gets the grain type as a string.
    fn grain_type(grain_id: &GrainId) -> String {
        grain_id.grain_type().to_string()
    }
}

#[async_trait]
impl IGrainStorage for PostgresGrainStorage {
    #[instrument(skip(self), fields(state_name = %state_name, grain_id = %grain_id))]
    async fn read_state(&self, state_name: &str, grain_id: &GrainId) -> StorageResult<RawGrainState> {
        debug!("reading grain state from PostgreSQL");

        let query = format!(
            r#"
            SELECT state_data, etag
            FROM {schema}.grain_state
            WHERE grain_type = $1 AND grain_key = $2 AND state_name = $3
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(Self::grain_type(grain_id))
            .bind(Self::grain_key(grain_id))
            .bind(state_name)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read grain state");
                StorageError::Io(e.to_string())
            })?;

        match result {
            Some(row) => {
                let data: Vec<u8> = row.get("state_data");
                let etag: String = row.get("etag");
                debug!(data_len = data.len(), etag = %etag, "grain state found");
                Ok(RawGrainState::with_data(data, etag))
            }
            None => {
                debug!("grain state not found");
                Ok(RawGrainState::empty())
            }
        }
    }

    #[instrument(skip(self, state), fields(state_name = %state_name, grain_id = %grain_id, data_len = state.data.len()))]
    async fn write_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        state: &RawGrainState,
    ) -> StorageResult<String> {
        let new_etag = Uuid::new_v4().to_string();

        match &state.etag {
            None => {
                // Insert - must not exist
                debug!("inserting new grain state");
                let query = format!(
                    r#"
                    INSERT INTO {schema}.grain_state (grain_type, grain_key, state_name, state_data, etag)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (grain_type, grain_key, state_name) DO NOTHING
                    "#,
                    schema = self.schema
                );

                let result = sqlx::query(&query)
                    .bind(Self::grain_type(grain_id))
                    .bind(Self::grain_key(grain_id))
                    .bind(state_name)
                    .bind(&state.data)
                    .bind(&new_etag)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "failed to insert grain state");
                        StorageError::Io(e.to_string())
                    })?;

                if result.rows_affected() == 0 {
                    warn!("record already exists");
                    return Err(StorageError::RecordExists);
                }

                info!(etag = %new_etag, "grain state inserted");
                Ok(new_etag)
            }
            Some(expected_etag) if expected_etag == "*" => {
                // Upsert - always succeeds
                debug!("upserting grain state (wildcard etag)");
                let query = format!(
                    r#"
                    INSERT INTO {schema}.grain_state (grain_type, grain_key, state_name, state_data, etag, updated_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (grain_type, grain_key, state_name)
                    DO UPDATE SET state_data = $4, etag = $5, updated_at = NOW()
                    "#,
                    schema = self.schema
                );

                sqlx::query(&query)
                    .bind(Self::grain_type(grain_id))
                    .bind(Self::grain_key(grain_id))
                    .bind(state_name)
                    .bind(&state.data)
                    .bind(&new_etag)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "failed to upsert grain state");
                        StorageError::Io(e.to_string())
                    })?;

                info!(etag = %new_etag, "grain state upserted");
                Ok(new_etag)
            }
            Some(expected_etag) => {
                // Update with ETag check
                debug!(expected_etag = %expected_etag, "updating grain state with etag check");
                let query = format!(
                    r#"
                    UPDATE {schema}.grain_state
                    SET state_data = $1, etag = $2, updated_at = NOW()
                    WHERE grain_type = $3 AND grain_key = $4 AND state_name = $5 AND etag = $6
                    "#,
                    schema = self.schema
                );

                let result = sqlx::query(&query)
                    .bind(&state.data)
                    .bind(&new_etag)
                    .bind(Self::grain_type(grain_id))
                    .bind(Self::grain_key(grain_id))
                    .bind(state_name)
                    .bind(expected_etag)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "failed to update grain state");
                        StorageError::Io(e.to_string())
                    })?;

                if result.rows_affected() == 0 {
                    // Check if record exists to determine error type
                    let exists_query = format!(
                        r#"
                        SELECT etag FROM {schema}.grain_state
                        WHERE grain_type = $1 AND grain_key = $2 AND state_name = $3
                        "#,
                        schema = self.schema
                    );

                    let current = sqlx::query(&exists_query)
                        .bind(Self::grain_type(grain_id))
                        .bind(Self::grain_key(grain_id))
                        .bind(state_name)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| StorageError::Io(e.to_string()))?;

                    match current {
                        Some(row) => {
                            let stored_etag: String = row.get("etag");
                            warn!(stored = %stored_etag, expected = %expected_etag, "etag mismatch");
                            return Err(StorageError::EtagMismatch {
                                stored: stored_etag,
                                expected: expected_etag.clone(),
                            });
                        }
                        None => {
                            warn!("record not found");
                            return Err(StorageError::RecordNotFound);
                        }
                    }
                }

                info!(etag = %new_etag, "grain state updated");
                Ok(new_etag)
            }
        }
    }

    #[instrument(skip(self), fields(state_name = %state_name, grain_id = %grain_id))]
    async fn clear_state(
        &self,
        state_name: &str,
        grain_id: &GrainId,
        expected_etag: Option<&str>,
    ) -> StorageResult<()> {
        debug!("clearing grain state");

        match expected_etag {
            Some(etag) if etag != "*" => {
                // Delete with ETag check
                let query = format!(
                    r#"
                    DELETE FROM {schema}.grain_state
                    WHERE grain_type = $1 AND grain_key = $2 AND state_name = $3 AND etag = $4
                    "#,
                    schema = self.schema
                );

                let result = sqlx::query(&query)
                    .bind(Self::grain_type(grain_id))
                    .bind(Self::grain_key(grain_id))
                    .bind(state_name)
                    .bind(etag)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "failed to clear grain state");
                        StorageError::Io(e.to_string())
                    })?;

                if result.rows_affected() == 0 {
                    // Check if record exists
                    let exists_query = format!(
                        r#"
                        SELECT etag FROM {schema}.grain_state
                        WHERE grain_type = $1 AND grain_key = $2 AND state_name = $3
                        "#,
                        schema = self.schema
                    );

                    let current = sqlx::query(&exists_query)
                        .bind(Self::grain_type(grain_id))
                        .bind(Self::grain_key(grain_id))
                        .bind(state_name)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(|e| StorageError::Io(e.to_string()))?;

                    if let Some(row) = current {
                        let stored_etag: String = row.get("etag");
                        warn!(stored = %stored_etag, expected = %etag, "etag mismatch on clear");
                        return Err(StorageError::EtagMismatch {
                            stored: stored_etag,
                            expected: etag.to_string(),
                        });
                    }
                    // Record doesn't exist, which is fine for clear
                }

                info!("grain state cleared with etag check");
            }
            _ => {
                // Delete without ETag check (None or "*")
                let query = format!(
                    r#"
                    DELETE FROM {schema}.grain_state
                    WHERE grain_type = $1 AND grain_key = $2 AND state_name = $3
                    "#,
                    schema = self.schema
                );

                sqlx::query(&query)
                    .bind(Self::grain_type(grain_id))
                    .bind(Self::grain_key(grain_id))
                    .bind(state_name)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| {
                        error!(error = %e, "failed to clear grain state");
                        StorageError::Io(e.to_string())
                    })?;

                info!("grain state cleared");
            }
        }

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
    fn test_grain_key() {
        let grain_id = make_grain_id("my.grain", "test-key");
        let key = PostgresGrainStorage::grain_key(&grain_id);
        assert_eq!(key, "test-key");
    }

    #[test]
    fn test_grain_type() {
        let grain_id = make_grain_id("my.grain", "test-key");
        let grain_type = PostgresGrainStorage::grain_type(&grain_id);
        assert_eq!(grain_type, "my.grain");
    }
}
