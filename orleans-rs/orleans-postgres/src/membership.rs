//! PostgreSQL implementation of the membership table.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use orleans_clustering::{
    IMembershipTable, MembershipEntry, MembershipError, MembershipResult, MembershipTableData,
    SiloStatus, TableVersion,
};
use orleans_core::SiloAddress;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::net::SocketAddr;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::options::PostgresOptions;
use crate::PostgresResult;

/// PostgreSQL implementation of the membership table.
///
/// This implementation stores cluster membership data in PostgreSQL with
/// optimistic concurrency control via version numbers and ETags.
#[derive(Clone)]
pub struct PostgresMembershipTable {
    pool: PgPool,
    schema: String,
    cluster_id: String,
}

impl PostgresMembershipTable {
    /// Creates a new PostgreSQL membership table with the given options.
    #[instrument(skip(options), fields(schema = %options.schema, cluster_id = %options.cluster_id))]
    pub async fn new(options: &PostgresOptions) -> PostgresResult<Self> {
        options.validate().map_err(|e| {
            error!(error = %e, "invalid PostgreSQL options");
            crate::PostgresError::Configuration(e)
        })?;

        info!("connecting to PostgreSQL for membership table");

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
            cluster_id: options.cluster_id.clone(),
        };

        if options.run_migrations {
            table.run_migrations().await?;
        }

        info!("PostgreSQL membership table initialized");
        Ok(table)
    }

    /// Creates a new PostgreSQL membership table from an existing pool.
    pub fn from_pool(pool: PgPool, schema: String, cluster_id: String) -> Self {
        Self {
            pool,
            schema,
            cluster_id,
        }
    }

    /// Runs the database migrations to create the required tables.
    #[instrument(skip(self))]
    async fn run_migrations(&self) -> PostgresResult<()> {
        info!(schema = %self.schema, "running membership table migrations");

        // Create schema if not exists
        let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", self.schema);
        sqlx::query(&create_schema)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create schema");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create membership table
        let create_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.membership (
                cluster_id VARCHAR(255) NOT NULL,
                silo_address VARCHAR(255) NOT NULL,
                silo_ip VARCHAR(45) NOT NULL,
                silo_port INTEGER NOT NULL,
                silo_generation BIGINT NOT NULL,
                status INTEGER NOT NULL,
                silo_name VARCHAR(255) NOT NULL DEFAULT '',
                host_name VARCHAR(255) NOT NULL DEFAULT '',
                proxy_port INTEGER,
                role_name VARCHAR(255),
                update_zone INTEGER NOT NULL DEFAULT 0,
                fault_zone INTEGER NOT NULL DEFAULT 0,
                start_time TIMESTAMPTZ NOT NULL,
                i_am_alive_time TIMESTAMPTZ NOT NULL,
                suspect_times JSONB NOT NULL DEFAULT '[]',
                etag VARCHAR(64) NOT NULL,
                PRIMARY KEY (cluster_id, silo_address)
            )
            "#,
            schema = self.schema
        );
        sqlx::query(&create_table)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create membership table");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create table version table
        let create_version_table = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {schema}.membership_version (
                cluster_id VARCHAR(255) NOT NULL PRIMARY KEY,
                version BIGINT NOT NULL DEFAULT 0,
                version_etag VARCHAR(64) NOT NULL
            )
            "#,
            schema = self.schema
        );
        sqlx::query(&create_version_table)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to create membership_version table");
                crate::PostgresError::Migration(e.to_string())
            })?;

        // Create indexes
        let create_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_membership_status
            ON {schema}.membership (cluster_id, status)
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

        let create_time_index = format!(
            r#"
            CREATE INDEX IF NOT EXISTS idx_membership_alive_time
            ON {schema}.membership (cluster_id, i_am_alive_time)
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

        info!("membership table migrations completed");
        Ok(())
    }

    /// Converts a database row to a MembershipEntry.
    fn row_to_entry(row: &sqlx::postgres::PgRow) -> MembershipResult<(MembershipEntry, String)> {
        let silo_ip: String = row.get("silo_ip");
        let silo_port: i32 = row.get("silo_port");
        let silo_generation: i64 = row.get("silo_generation");

        let socket_addr: SocketAddr = format!("{}:{}", silo_ip, silo_port)
            .parse()
            .map_err(|e| MembershipError::Internal(format!("invalid socket address: {}", e)))?;

        let silo_address = SiloAddress::new(socket_addr, silo_generation);

        let status_int: i32 = row.get("status");
        let status = match status_int {
            0 => SiloStatus::Created,
            2 => SiloStatus::Joining,
            3 => SiloStatus::Active,
            4 => SiloStatus::ShuttingDown,
            5 => SiloStatus::Stopping,
            6 => SiloStatus::Dead,
            _ => {
                return Err(MembershipError::Internal(format!(
                    "invalid silo status: {}",
                    status_int
                )))
            }
        };

        let suspect_times_json: serde_json::Value = row.get("suspect_times");
        let suspect_times: Vec<(SiloAddress, DateTime<Utc>)> =
            serde_json::from_value(suspect_times_json).unwrap_or_default();

        let entry = MembershipEntry {
            silo_address,
            status,
            silo_name: row.get("silo_name"),
            host_name: row.get("host_name"),
            proxy_port: row.get::<Option<i32>, _>("proxy_port").map(|p| p as u16),
            role_name: row.get("role_name"),
            update_zone: row.get::<i32, _>("update_zone") as u32,
            fault_zone: row.get::<i32, _>("fault_zone") as u32,
            start_time: row.get("start_time"),
            i_am_alive_time: row.get("i_am_alive_time"),
            suspect_times,
        };

        let etag: String = row.get("etag");

        Ok((entry, etag))
    }

    /// Gets the current table version.
    async fn get_table_version(&self) -> MembershipResult<TableVersion> {
        let query = format!(
            r#"
            SELECT version, version_etag FROM {schema}.membership_version
            WHERE cluster_id = $1
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(&self.cluster_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| MembershipError::Storage(e.to_string()))?;

        match result {
            Some(row) => {
                let version: i64 = row.get("version");
                let version_etag: String = row.get("version_etag");
                Ok(TableVersion::with_etag(version, version_etag))
            }
            None => Ok(TableVersion::new()),
        }
    }

    /// Increments the table version atomically.
    async fn increment_table_version(
        &self,
        expected_version: &TableVersion,
    ) -> MembershipResult<TableVersion> {
        let new_etag = Uuid::new_v4().to_string();
        let new_version = expected_version.version + 1;

        let query = format!(
            r#"
            INSERT INTO {schema}.membership_version (cluster_id, version, version_etag)
            VALUES ($1, $2, $3)
            ON CONFLICT (cluster_id) DO UPDATE
            SET version = $2, version_etag = $3
            WHERE {schema}.membership_version.version = $4
            RETURNING version
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(&self.cluster_id)
            .bind(new_version)
            .bind(&new_etag)
            .bind(expected_version.version)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| MembershipError::Storage(e.to_string()))?;

        match result {
            Some(_) => Ok(TableVersion::with_etag(new_version, new_etag)),
            None => {
                let current = self.get_table_version().await?;
                Err(MembershipError::VersionMismatch {
                    expected: expected_version.version,
                    actual: current.version,
                })
            }
        }
    }

    /// Formats a silo address as a string key.
    fn silo_address_key(addr: &SiloAddress) -> String {
        format!("{}:{}/{}", addr.endpoint().ip(), addr.endpoint().port(), addr.generation())
    }
}

#[async_trait]
impl IMembershipTable for PostgresMembershipTable {
    #[instrument(skip(self), fields(silo = %silo_address))]
    async fn read_row(
        &self,
        silo_address: &SiloAddress,
    ) -> MembershipResult<Option<(MembershipEntry, String)>> {
        debug!("reading membership row");

        let query = format!(
            r#"
            SELECT silo_ip, silo_port, silo_generation, status, silo_name, host_name,
                   proxy_port, role_name, update_zone, fault_zone, start_time,
                   i_am_alive_time, suspect_times, etag
            FROM {schema}.membership
            WHERE cluster_id = $1 AND silo_address = $2
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(&self.cluster_id)
            .bind(Self::silo_address_key(silo_address))
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read membership row");
                MembershipError::Storage(e.to_string())
            })?;

        match result {
            Some(row) => {
                let entry = Self::row_to_entry(&row)?;
                debug!(status = ?entry.0.status, "found membership row");
                Ok(Some(entry))
            }
            None => {
                debug!("membership row not found");
                Ok(None)
            }
        }
    }

    #[instrument(skip(self))]
    async fn read_all(&self) -> MembershipResult<MembershipTableData> {
        debug!("reading all membership rows");

        let query = format!(
            r#"
            SELECT silo_ip, silo_port, silo_generation, status, silo_name, host_name,
                   proxy_port, role_name, update_zone, fault_zone, start_time,
                   i_am_alive_time, suspect_times, etag
            FROM {schema}.membership
            WHERE cluster_id = $1
            ORDER BY start_time
            "#,
            schema = self.schema
        );

        let rows = sqlx::query(&query)
            .bind(&self.cluster_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to read all membership rows");
                MembershipError::Storage(e.to_string())
            })?;

        let mut entries = Vec::with_capacity(rows.len());
        for row in &rows {
            entries.push(Self::row_to_entry(row)?);
        }

        let version = self.get_table_version().await?;

        debug!(count = entries.len(), version = version.version, "read all membership rows");
        Ok(MembershipTableData { entries, version })
    }

    #[instrument(skip(self, entry), fields(silo = %entry.silo_address, status = ?entry.status))]
    async fn insert_row(
        &self,
        entry: MembershipEntry,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        info!("inserting membership row");

        // Check and increment version first
        let _new_version = self.increment_table_version(&table_version).await?;

        let etag = Uuid::new_v4().to_string();
        let suspect_times_json =
            serde_json::to_value(&entry.suspect_times).unwrap_or(serde_json::json!([]));

        let query = format!(
            r#"
            INSERT INTO {schema}.membership (
                cluster_id, silo_address, silo_ip, silo_port, silo_generation,
                status, silo_name, host_name, proxy_port, role_name,
                update_zone, fault_zone, start_time, i_am_alive_time, suspect_times, etag
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            ON CONFLICT (cluster_id, silo_address) DO NOTHING
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(&self.cluster_id)
            .bind(Self::silo_address_key(&entry.silo_address))
            .bind(entry.silo_address.endpoint().ip().to_string())
            .bind(entry.silo_address.endpoint().port() as i32)
            .bind(entry.silo_address.generation())
            .bind(entry.status as i32)
            .bind(&entry.silo_name)
            .bind(&entry.host_name)
            .bind(entry.proxy_port.map(|p| p as i32))
            .bind(&entry.role_name)
            .bind(entry.update_zone as i32)
            .bind(entry.fault_zone as i32)
            .bind(entry.start_time)
            .bind(entry.i_am_alive_time)
            .bind(suspect_times_json)
            .bind(&etag)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to insert membership row");
                MembershipError::Storage(e.to_string())
            })?;

        let inserted = result.rows_affected() > 0;
        if inserted {
            info!("membership row inserted successfully");
        } else {
            warn!("membership row already exists");
        }

        Ok(inserted)
    }

    #[instrument(skip(self, entry), fields(silo = %entry.silo_address, status = ?entry.status))]
    async fn update_row(
        &self,
        entry: MembershipEntry,
        etag: &str,
        table_version: TableVersion,
    ) -> MembershipResult<bool> {
        info!("updating membership row");

        // Check and increment version first
        let _new_version = self.increment_table_version(&table_version).await?;

        let new_etag = Uuid::new_v4().to_string();
        let suspect_times_json =
            serde_json::to_value(&entry.suspect_times).unwrap_or(serde_json::json!([]));

        let query = format!(
            r#"
            UPDATE {schema}.membership
            SET status = $1, silo_name = $2, host_name = $3, proxy_port = $4,
                role_name = $5, update_zone = $6, fault_zone = $7, start_time = $8,
                i_am_alive_time = $9, suspect_times = $10, etag = $11
            WHERE cluster_id = $12 AND silo_address = $13 AND etag = $14
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(entry.status as i32)
            .bind(&entry.silo_name)
            .bind(&entry.host_name)
            .bind(entry.proxy_port.map(|p| p as i32))
            .bind(&entry.role_name)
            .bind(entry.update_zone as i32)
            .bind(entry.fault_zone as i32)
            .bind(entry.start_time)
            .bind(entry.i_am_alive_time)
            .bind(suspect_times_json)
            .bind(&new_etag)
            .bind(&self.cluster_id)
            .bind(Self::silo_address_key(&entry.silo_address))
            .bind(etag)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to update membership row");
                MembershipError::Storage(e.to_string())
            })?;

        let updated = result.rows_affected() > 0;
        if updated {
            info!("membership row updated successfully");
        } else {
            warn!("membership row not updated (etag mismatch or not found)");
        }

        Ok(updated)
    }

    #[instrument(skip(self, entry), fields(silo = %entry.silo_address))]
    async fn update_i_am_alive(&self, entry: &MembershipEntry) -> MembershipResult<()> {
        debug!("updating I-am-alive timestamp");

        let query = format!(
            r#"
            UPDATE {schema}.membership
            SET i_am_alive_time = $1
            WHERE cluster_id = $2 AND silo_address = $3
            "#,
            schema = self.schema
        );

        sqlx::query(&query)
            .bind(entry.i_am_alive_time)
            .bind(&self.cluster_id)
            .bind(Self::silo_address_key(&entry.silo_address))
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to update I-am-alive");
                MembershipError::Storage(e.to_string())
            })?;

        debug!("I-am-alive timestamp updated");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn delete_membership_table_entries(&self, cluster_id: &str) -> MembershipResult<()> {
        info!(cluster_id = %cluster_id, "deleting membership table entries");

        let query = format!(
            "DELETE FROM {schema}.membership WHERE cluster_id = $1",
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(cluster_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to delete membership entries");
                MembershipError::Storage(e.to_string())
            })?;

        info!(deleted = result.rows_affected(), "deleted membership entries");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn cleanup_defunct_silo_entries(&self, before: DateTime<Utc>) -> MembershipResult<()> {
        info!(before = %before, "cleaning up defunct silo entries");

        let query = format!(
            r#"
            DELETE FROM {schema}.membership
            WHERE cluster_id = $1
              AND status = $2
              AND i_am_alive_time < $3
            "#,
            schema = self.schema
        );

        let result = sqlx::query(&query)
            .bind(&self.cluster_id)
            .bind(SiloStatus::Dead as i32)
            .bind(before)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                error!(error = %e, "failed to cleanup defunct silo entries");
                MembershipError::Storage(e.to_string())
            })?;

        info!(deleted = result.rows_affected(), "cleaned up defunct silo entries");
        Ok(())
    }

    #[instrument(skip(self))]
    async fn initialize_membership_table(&self, try_init_table_version: bool) -> MembershipResult<()> {
        info!(try_init_version = try_init_table_version, "initializing membership table");

        if try_init_table_version {
            let etag = Uuid::new_v4().to_string();
            let query = format!(
                r#"
                INSERT INTO {schema}.membership_version (cluster_id, version, version_etag)
                VALUES ($1, 0, $2)
                ON CONFLICT (cluster_id) DO NOTHING
                "#,
                schema = self.schema
            );

            sqlx::query(&query)
                .bind(&self.cluster_id)
                .bind(&etag)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    error!(error = %e, "failed to initialize table version");
                    MembershipError::Storage(e.to_string())
                })?;
        }

        info!("membership table initialized");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_silo_address_key() {
        let addr: SocketAddr = "127.0.0.1:11111".parse().unwrap();
        let silo = SiloAddress::new(addr, 12345);
        let key = PostgresMembershipTable::silo_address_key(&silo);
        assert_eq!(key, "127.0.0.1:11111/12345");
    }

    #[test]
    fn test_silo_address_key_ipv6() {
        let addr: SocketAddr = "[::1]:11111".parse().unwrap();
        let silo = SiloAddress::new(addr, 1);
        let key = PostgresMembershipTable::silo_address_key(&silo);
        assert_eq!(key, "::1:11111/1");
    }
}
