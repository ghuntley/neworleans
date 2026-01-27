//! Grain activation context.
//!
//! The grain context provides access to runtime services and identity
//! information for a grain activation.

use orleans_core::{ActivationId, GrainId, GrainType, SiloAddress};
use std::sync::Arc;

use crate::grain_factory::IGrainFactory;

/// The context available to a grain during its lifecycle.
///
/// This trait provides access to:
/// - The grain's identity (GrainId, ActivationId)
/// - The hosting silo's address
/// - Runtime services (grain factory, etc.)
/// - Lifecycle control (deactivation request)
pub trait IGrainContext: Send + Sync {
    /// Returns the grain's identity.
    fn grain_id(&self) -> &GrainId;

    /// Returns the grain's type.
    fn grain_type(&self) -> &GrainType;

    /// Returns the activation's unique ID.
    fn activation_id(&self) -> &ActivationId;

    /// Returns the silo hosting this activation.
    fn silo_address(&self) -> &SiloAddress;

    /// Returns the grain factory for creating grain references.
    fn grain_factory(&self) -> Arc<dyn IGrainFactory>;

    /// Requests deactivation of this grain.
    ///
    /// The grain will be deactivated after the current method completes.
    fn deactivate_on_idle(&self);

    /// Delays the grain's idle deactivation.
    ///
    /// Call this to reset the idle timer and prevent the grain from
    /// being garbage collected due to inactivity.
    fn delay_deactivation(&self);
}

/// A concrete implementation of the grain context.
pub struct GrainContext {
    grain_id: GrainId,
    grain_type: GrainType,
    activation_id: ActivationId,
    silo_address: SiloAddress,
    grain_factory: Arc<dyn IGrainFactory>,
    deactivate_requested: std::sync::atomic::AtomicBool,
    last_activity: parking_lot::RwLock<std::time::Instant>,
}

impl GrainContext {
    /// Create a new grain context.
    pub fn new(
        grain_id: GrainId,
        grain_type: GrainType,
        activation_id: ActivationId,
        silo_address: SiloAddress,
        grain_factory: Arc<dyn IGrainFactory>,
    ) -> Self {
        Self {
            grain_id,
            grain_type,
            activation_id,
            silo_address,
            grain_factory,
            deactivate_requested: std::sync::atomic::AtomicBool::new(false),
            last_activity: parking_lot::RwLock::new(std::time::Instant::now()),
        }
    }

    /// Returns true if deactivation has been requested.
    pub fn is_deactivate_requested(&self) -> bool {
        self.deactivate_requested
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the last activity time.
    pub fn last_activity(&self) -> std::time::Instant {
        *self.last_activity.read()
    }

    /// Updates the last activity time to now.
    pub fn touch(&self) {
        *self.last_activity.write() = std::time::Instant::now();
    }
}

impl IGrainContext for GrainContext {
    fn grain_id(&self) -> &GrainId {
        &self.grain_id
    }

    fn grain_type(&self) -> &GrainType {
        &self.grain_type
    }

    fn activation_id(&self) -> &ActivationId {
        &self.activation_id
    }

    fn silo_address(&self) -> &SiloAddress {
        &self.silo_address
    }

    fn grain_factory(&self) -> Arc<dyn IGrainFactory> {
        self.grain_factory.clone()
    }

    fn deactivate_on_idle(&self) {
        self.deactivate_requested
            .store(true, std::sync::atomic::Ordering::Release);
    }

    fn delay_deactivation(&self) {
        self.touch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orleans_core::{GrainType, IdSpan};
    use std::net::SocketAddr;

    // Mock grain factory for testing
    struct MockGrainFactory;

    impl IGrainFactory for MockGrainFactory {
        fn get_grain_reference(
            &self,
            _grain_type: GrainType,
            _key: IdSpan,
        ) -> Arc<dyn crate::grain_reference::IGrainReference> {
            unimplemented!()
        }
    }

    fn create_test_context() -> GrainContext {
        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("test-key"));
        let activation_id = ActivationId::new();
        let silo_address =
            SiloAddress::new("127.0.0.1:11111".parse::<SocketAddr>().unwrap(), 1234);
        let grain_factory = Arc::new(MockGrainFactory);

        GrainContext::new(
            grain_id,
            grain_type,
            activation_id,
            silo_address,
            grain_factory,
        )
    }

    #[test]
    fn test_context_grain_id() {
        let context = create_test_context();
        assert_eq!(context.grain_id().grain_type().as_str(), Some("TestGrain"));
    }

    #[test]
    fn test_context_grain_type() {
        let context = create_test_context();
        assert_eq!(context.grain_type().as_str(), Some("TestGrain"));
    }

    #[test]
    fn test_context_activation_id() {
        let context = create_test_context();
        // Activation ID should be valid
        assert!(!context.activation_id().is_default());
    }

    #[test]
    fn test_context_silo_address() {
        let context = create_test_context();
        assert_eq!(context.silo_address().generation(), 1234);
    }

    #[test]
    fn test_deactivate_on_idle() {
        let context = create_test_context();
        assert!(!context.is_deactivate_requested());

        context.deactivate_on_idle();
        assert!(context.is_deactivate_requested());
    }

    #[test]
    fn test_delay_deactivation() {
        let context = create_test_context();
        let initial_activity = context.last_activity();

        std::thread::sleep(std::time::Duration::from_millis(10));
        context.delay_deactivation();

        assert!(context.last_activity() > initial_activity);
    }

    #[test]
    fn test_touch() {
        let context = create_test_context();
        let initial_activity = context.last_activity();

        std::thread::sleep(std::time::Duration::from_millis(10));
        context.touch();

        assert!(context.last_activity() > initial_activity);
    }
}
