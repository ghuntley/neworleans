//! Configuration options for PostgreSQL storage providers.

use std::time::Duration;

/// Configuration options for PostgreSQL connection pool.
#[derive(Debug, Clone)]
pub struct PostgresOptions {
    /// PostgreSQL connection string.
    pub connection_string: String,

    /// Minimum number of connections in the pool.
    pub min_connections: u32,

    /// Maximum number of connections in the pool.
    pub max_connections: u32,

    /// Connection timeout.
    pub connect_timeout: Duration,

    /// Idle connection timeout.
    pub idle_timeout: Duration,

    /// Maximum connection lifetime.
    pub max_lifetime: Duration,

    /// Schema name to use (default: "orleans").
    pub schema: String,

    /// Whether to run schema migrations on startup.
    pub run_migrations: bool,

    /// Cluster ID for multi-tenant deployments.
    pub cluster_id: String,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            connection_string: String::new(),
            min_connections: 1,
            max_connections: 10,
            connect_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(600),
            max_lifetime: Duration::from_secs(1800),
            schema: "orleans".to_string(),
            run_migrations: true,
            cluster_id: "default".to_string(),
        }
    }
}

impl PostgresOptions {
    /// Creates new options with the given connection string.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            ..Default::default()
        }
    }

    /// Creates options suitable for testing with minimal pool size.
    pub fn for_testing(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            min_connections: 1,
            max_connections: 5,
            connect_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            max_lifetime: Duration::from_secs(300),
            schema: "orleans_test".to_string(),
            run_migrations: true,
            cluster_id: "test".to_string(),
        }
    }

    /// Sets the minimum number of connections.
    pub fn with_min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Sets the maximum number of connections.
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Sets the connection timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sets the idle timeout.
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Sets the maximum connection lifetime.
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = lifetime;
        self
    }

    /// Sets the schema name.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Sets whether to run migrations on startup.
    pub fn with_run_migrations(mut self, run: bool) -> Self {
        self.run_migrations = run;
        self
    }

    /// Sets the cluster ID.
    pub fn with_cluster_id(mut self, id: impl Into<String>) -> Self {
        self.cluster_id = id.into();
        self
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.connection_string.is_empty() {
            return Err("connection_string is required".to_string());
        }
        if self.max_connections < self.min_connections {
            return Err("max_connections must be >= min_connections".to_string());
        }
        if self.schema.is_empty() {
            return Err("schema cannot be empty".to_string());
        }
        if self.cluster_id.is_empty() {
            return Err("cluster_id cannot be empty".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_options() {
        let opts = PostgresOptions::default();
        assert!(opts.connection_string.is_empty());
        assert_eq!(opts.min_connections, 1);
        assert_eq!(opts.max_connections, 10);
        assert_eq!(opts.schema, "orleans");
        assert!(opts.run_migrations);
    }

    #[test]
    fn test_new_with_connection_string() {
        let opts = PostgresOptions::new("postgres://localhost/orleans");
        assert_eq!(opts.connection_string, "postgres://localhost/orleans");
    }

    #[test]
    fn test_for_testing() {
        let opts = PostgresOptions::for_testing("postgres://localhost/test");
        assert_eq!(opts.schema, "orleans_test");
        assert_eq!(opts.cluster_id, "test");
        assert_eq!(opts.max_connections, 5);
    }

    #[test]
    fn test_builder_pattern() {
        let opts = PostgresOptions::new("postgres://localhost/db")
            .with_min_connections(2)
            .with_max_connections(20)
            .with_schema("custom")
            .with_cluster_id("my-cluster");

        assert_eq!(opts.min_connections, 2);
        assert_eq!(opts.max_connections, 20);
        assert_eq!(opts.schema, "custom");
        assert_eq!(opts.cluster_id, "my-cluster");
    }

    #[test]
    fn test_validation() {
        let opts = PostgresOptions::default();
        assert!(opts.validate().is_err());

        let opts = PostgresOptions::new("postgres://localhost/db");
        assert!(opts.validate().is_ok());

        let opts = PostgresOptions::new("postgres://localhost/db").with_schema("");
        assert!(opts.validate().is_err());

        let opts = PostgresOptions::new("postgres://localhost/db")
            .with_min_connections(10)
            .with_max_connections(5);
        assert!(opts.validate().is_err());
    }
}
