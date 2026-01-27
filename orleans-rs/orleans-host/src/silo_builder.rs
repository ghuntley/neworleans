//! Builder for configuring and creating silos.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use orleans_clustering::{ClusterMembershipOptions, IMembershipTable, InMemoryMembershipTable};
use orleans_runtime::GrainTypeData;

use crate::config::SiloConfig;
use crate::error::{SiloError, SiloResult};
use crate::silo::Silo;

/// Builder for creating and configuring silos.
pub struct SiloBuilder {
    config: SiloConfig,
    grain_types: Vec<Arc<GrainTypeData>>,
    membership_table: Option<Arc<dyn IMembershipTable>>,
}

impl Default for SiloBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SiloBuilder {
    /// Create a new silo builder with default configuration.
    pub fn new() -> Self {
        Self {
            config: SiloConfig::default(),
            grain_types: Vec::new(),
            membership_table: None,
        }
    }

    /// Create a silo builder with development configuration.
    pub fn development() -> Self {
        Self {
            config: SiloConfig::development(),
            grain_types: Vec::new(),
            membership_table: None,
        }
    }

    /// Create a silo builder with test configuration.
    pub fn test() -> Self {
        Self {
            config: SiloConfig::test(),
            grain_types: Vec::new(),
            membership_table: None,
        }
    }

    /// Set the listen address for the silo.
    pub fn listen_address(mut self, address: SocketAddr) -> Self {
        self.config.listen_address = address;
        self
    }

    /// Set the generation number for this silo instance.
    pub fn generation(mut self, generation: i64) -> Self {
        self.config.generation = generation;
        self
    }

    /// Set the entire silo configuration.
    pub fn with_config(mut self, config: SiloConfig) -> Self {
        self.config = config;
        self
    }

    /// Set the cluster membership options.
    pub fn with_membership_options(mut self, options: ClusterMembershipOptions) -> Self {
        self.config.membership = options;
        self
    }

    /// Set the membership table to use.
    ///
    /// If not set, a new in-memory table will be created.
    /// For a multi-silo cluster, all silos must share the same membership table.
    pub fn with_membership_table(mut self, table: Arc<dyn IMembershipTable>) -> Self {
        self.membership_table = Some(table);
        self
    }

    /// Register a grain type with the silo.
    pub fn register_grain_type(mut self, grain_type_data: Arc<GrainTypeData>) -> Self {
        self.grain_types.push(grain_type_data);
        self
    }

    /// Register multiple grain types with the silo.
    pub fn register_grain_types(mut self, grain_types: Vec<Arc<GrainTypeData>>) -> Self {
        self.grain_types.extend(grain_types);
        self
    }

    /// Set the startup timeout.
    pub fn startup_timeout(mut self, timeout: Duration) -> Self {
        self.config.startup_timeout = timeout;
        self
    }

    /// Set the shutdown timeout.
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.config.shutdown_timeout = timeout;
        self
    }

    /// Build the silo.
    ///
    /// This creates the silo but does not start it.
    /// Call `start()` on the returned silo to begin operation.
    pub async fn build(self) -> SiloResult<Silo> {
        // Validate configuration
        if self.grain_types.is_empty() {
            return Err(SiloError::NoGrainTypesRegistered);
        }

        // Create or use provided membership table
        let membership_table = match self.membership_table {
            Some(table) => table,
            None => {
                let table = Arc::new(InMemoryMembershipTable::new("orleans-cluster"));
                table.initialize_membership_table(true).await?;
                table
            }
        };

        Silo::new(self.config, self.grain_types, membership_table).await
    }
}
