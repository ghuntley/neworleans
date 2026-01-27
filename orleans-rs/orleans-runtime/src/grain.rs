//! Core grain traits and types.
//!
//! This module defines the fundamental interfaces that all grains must implement
//! to participate in the Orleans actor model.

use async_trait::async_trait;
use orleans_core::{GrainId, GrainType};

use crate::error::RuntimeResult;
use crate::grain_context::IGrainContext;

/// Marker trait that identifies a type as an Orleans grain.
///
/// All grains must implement this trait. The trait provides access to
/// the grain's identity and lifecycle hooks.
///
/// # Example
///
/// ```ignore
/// #[grain]
/// pub struct HelloGrain {
///     greeting_count: u32,
/// }
///
/// impl IGrain for HelloGrain {
///     fn grain_type() -> GrainType {
///         GrainType::create("HelloGrain")
///     }
/// }
/// ```
#[async_trait]
pub trait IGrain: Send + Sync + 'static {
    /// Returns the grain type for this grain implementation.
    ///
    /// This is used to identify the grain type when routing messages
    /// and creating activations.
    fn grain_type() -> GrainType
    where
        Self: Sized;

    /// Called when the grain is being activated.
    ///
    /// Override this to perform initialization logic, such as loading
    /// state from storage or setting up resources.
    ///
    /// # Errors
    ///
    /// If this returns an error, the activation will fail and the
    /// grain will not be available to process requests.
    async fn on_activate(&mut self, _context: &dyn IGrainContext) -> RuntimeResult<()> {
        Ok(())
    }

    /// Called when the grain is being deactivated.
    ///
    /// Override this to perform cleanup logic, such as saving state
    /// to storage or releasing resources.
    ///
    /// # Errors
    ///
    /// Errors during deactivation are logged but do not prevent
    /// the deactivation from completing.
    async fn on_deactivate(&mut self, _context: &dyn IGrainContext) -> RuntimeResult<()> {
        Ok(())
    }
}

/// A grain invoker that dispatches method calls to grain instances.
///
/// This trait is implemented by code generation for each grain interface.
/// It deserializes the request body, calls the appropriate method,
/// and serializes the response.
#[async_trait]
pub trait IGrainMethodInvoker: Send + Sync {
    /// Invoke a method on the grain.
    ///
    /// # Arguments
    ///
    /// * `grain` - The grain instance (as a type-erased box).
    /// * `context` - The grain context.
    /// * `method_id` - The method to invoke.
    /// * `request_body` - The serialized request arguments.
    ///
    /// # Returns
    ///
    /// The serialized response body.
    async fn invoke(
        &self,
        grain: &mut dyn std::any::Any,
        context: &dyn IGrainContext,
        method_id: u32,
        request_body: &[u8],
    ) -> RuntimeResult<Vec<u8>>;

    /// Returns the interface type name.
    fn interface_type(&self) -> &str;

    /// Returns the list of method IDs supported by this invoker.
    fn method_ids(&self) -> &[u32];
}

/// Factory for creating grain instances.
///
/// This trait is implemented by code generation for each grain type.
/// It creates new grain instances with default state.
pub trait IGrainActivator: Send + Sync {
    /// Create a new grain instance.
    ///
    /// # Arguments
    ///
    /// * `grain_id` - The identity of the grain being created.
    ///
    /// # Returns
    ///
    /// A boxed grain instance.
    fn create(&self, grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync>;

    /// Returns the grain type this activator creates.
    fn grain_type(&self) -> GrainType;
}

/// Metadata about a registered grain type.
#[derive(Clone)]
pub struct GrainTypeData {
    /// The grain type identifier.
    pub grain_type: GrainType,

    /// The activator for creating grain instances.
    pub activator: std::sync::Arc<dyn IGrainActivator>,

    /// The invokers for this grain type, keyed by interface type.
    pub invokers: std::collections::HashMap<String, std::sync::Arc<dyn IGrainMethodInvoker>>,
}

impl GrainTypeData {
    /// Create new grain type metadata.
    pub fn new(grain_type: GrainType, activator: std::sync::Arc<dyn IGrainActivator>) -> Self {
        Self {
            grain_type,
            activator,
            invokers: std::collections::HashMap::new(),
        }
    }

    /// Add an invoker for an interface.
    pub fn with_invoker(
        mut self,
        interface_type: &str,
        invoker: std::sync::Arc<dyn IGrainMethodInvoker>,
    ) -> Self {
        self.invokers.insert(interface_type.to_string(), invoker);
        self
    }
}

/// Grain placement hint for the directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementHint {
    /// Use the default placement strategy (consistent hashing).
    Default,

    /// Prefer placing on a specific silo.
    PreferSilo(orleans_core::SiloAddress),

    /// Prefer placing locally on the current silo.
    PreferLocal,

    /// Place randomly across the cluster.
    Random,
}

impl Default for PlacementHint {
    fn default() -> Self {
        PlacementHint::Default
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::GrainType;

    // Test helper grain
    struct TestGrain {
        value: i32,
    }

    #[async_trait]
    impl IGrain for TestGrain {
        fn grain_type() -> GrainType {
            GrainType::create("TestGrain")
        }
    }

    #[test]
    fn test_grain_type_creation() {
        let grain_type = TestGrain::grain_type();
        assert_eq!(grain_type.as_str(), Some("TestGrain"));
    }

    #[test]
    fn test_placement_hint_default() {
        let hint = PlacementHint::default();
        assert_eq!(hint, PlacementHint::Default);
    }

    #[test]
    fn test_grain_type_data_creation() {
        struct TestActivator;

        impl IGrainActivator for TestActivator {
            fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(TestGrain { value: 0 })
            }

            fn grain_type(&self) -> GrainType {
                TestGrain::grain_type()
            }
        }

        let activator = std::sync::Arc::new(TestActivator);
        let data = GrainTypeData::new(TestGrain::grain_type(), activator);

        assert_eq!(data.grain_type.as_str(), Some("TestGrain"));
        assert!(data.invokers.is_empty());
    }
}
