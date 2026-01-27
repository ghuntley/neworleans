//! Grain factory for creating grain references.
//!
//! The grain factory is the primary API for obtaining grain references.
//! It creates lightweight proxy objects that can be used to invoke
//! methods on grains, regardless of where they are activated.

use orleans_core::{GrainId, GrainType, IdSpan};
use orleans_messaging::GrainInterfaceType;
use std::sync::Arc;

use crate::grain_reference::{GrainReference, IGrainReference, MessageSender};

/// Factory for creating grain references.
///
/// The grain factory provides a type-safe way to obtain references to grains.
/// It handles the mapping between grain types and their network locations.
pub trait IGrainFactory: Send + Sync {
    /// Get a reference to a grain by type and key.
    ///
    /// # Arguments
    ///
    /// * `grain_type` - The type of grain to get.
    /// * `key` - The unique key identifying the grain instance.
    ///
    /// # Returns
    ///
    /// A grain reference that can be used to invoke methods.
    fn get_grain_reference(
        &self,
        grain_type: GrainType,
        key: IdSpan,
    ) -> Arc<dyn IGrainReference>;
}

/// Default implementation of the grain factory.
pub struct GrainFactory {
    /// The message sender for grain invocations.
    message_sender: Arc<dyn MessageSender>,

    /// The default interface type (used when not explicitly specified).
    default_interface_resolver: Arc<dyn InterfaceResolver>,
}

/// Resolves grain types to their primary interface types.
pub trait InterfaceResolver: Send + Sync {
    /// Get the primary interface type for a grain type.
    fn resolve(&self, grain_type: &GrainType) -> GrainInterfaceType;
}

/// Simple interface resolver that uses a naming convention.
///
/// Converts grain type names to interface names by prepending "I".
/// For example: "HelloGrain" -> "IHelloGrain"
pub struct ConventionInterfaceResolver;

impl InterfaceResolver for ConventionInterfaceResolver {
    fn resolve(&self, grain_type: &GrainType) -> GrainInterfaceType {
        let type_name = grain_type.as_str().unwrap_or("Unknown");
        // If it ends with "Grain", replace with the interface convention
        let interface_name = if type_name.ends_with("Grain") {
            format!("I{}", type_name)
        } else {
            format!("I{}", type_name)
        };
        GrainInterfaceType::create(&interface_name)
    }
}

/// Interface resolver backed by a map.
pub struct MapInterfaceResolver {
    mappings: dashmap::DashMap<String, String>,
    fallback: ConventionInterfaceResolver,
}

impl MapInterfaceResolver {
    /// Create a new map-based interface resolver.
    pub fn new() -> Self {
        Self {
            mappings: dashmap::DashMap::new(),
            fallback: ConventionInterfaceResolver,
        }
    }

    /// Register a mapping from grain type to interface type.
    pub fn register(&self, grain_type: &str, interface_type: &str) {
        self.mappings
            .insert(grain_type.to_string(), interface_type.to_string());
    }
}

impl Default for MapInterfaceResolver {
    fn default() -> Self {
        Self::new()
    }
}

impl InterfaceResolver for MapInterfaceResolver {
    fn resolve(&self, grain_type: &GrainType) -> GrainInterfaceType {
        let type_str = grain_type.as_str().unwrap_or("Unknown");
        if let Some(interface_name) = self.mappings.get(type_str) {
            GrainInterfaceType::create(&interface_name)
        } else {
            self.fallback.resolve(grain_type)
        }
    }
}

impl GrainFactory {
    /// Create a new grain factory.
    pub fn new(
        message_sender: Arc<dyn MessageSender>,
        interface_resolver: Arc<dyn InterfaceResolver>,
    ) -> Self {
        Self {
            message_sender,
            default_interface_resolver: interface_resolver,
        }
    }

    /// Create a grain factory with the default convention-based resolver.
    pub fn with_convention_resolver(message_sender: Arc<dyn MessageSender>) -> Self {
        Self::new(message_sender, Arc::new(ConventionInterfaceResolver))
    }

    /// Get a reference to a grain with a specific interface type.
    pub fn get_grain_reference_with_interface(
        &self,
        grain_type: GrainType,
        key: IdSpan,
        interface_type: GrainInterfaceType,
    ) -> Arc<dyn IGrainReference> {
        Arc::new(GrainReference::from_key(
            grain_type,
            key,
            interface_type,
            self.message_sender.clone(),
        ))
    }

    /// Get a grain reference by GrainId.
    pub fn get_grain_reference_by_id(
        &self,
        grain_id: GrainId,
        interface_type: GrainInterfaceType,
    ) -> Arc<dyn IGrainReference> {
        Arc::new(GrainReference::new(
            grain_id.clone(),
            grain_id.grain_type().clone(),
            interface_type,
            self.message_sender.clone(),
        ))
    }
}

impl IGrainFactory for GrainFactory {
    fn get_grain_reference(
        &self,
        grain_type: GrainType,
        key: IdSpan,
    ) -> Arc<dyn IGrainReference> {
        let interface_type = self.default_interface_resolver.resolve(&grain_type);
        self.get_grain_reference_with_interface(grain_type, key, interface_type)
    }
}

/// Extension trait for typed grain access.
pub trait GrainFactoryExt: IGrainFactory {
    /// Get a typed grain reference.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The grain interface type marker.
    fn get_grain<T: GrainInterfaceMarker>(
        &self,
        key: &str,
    ) -> crate::grain_reference::TypedGrainReference<T> {
        let grain_type = T::grain_type();
        let interface_type = T::interface_type();
        let key = IdSpan::from_str(key);

        // We need to cast self to get the grain reference
        // This is a bit awkward but works for the trait extension pattern
        let inner = self.get_grain_reference(grain_type.clone(), key.clone());
        // Cast to the correct interface
        let casted = inner.cast(interface_type);
        crate::grain_reference::TypedGrainReference::new(casted)
    }

    /// Get a typed grain reference with a GUID key.
    fn get_grain_by_guid<T: GrainInterfaceMarker>(
        &self,
        key: uuid::Uuid,
    ) -> crate::grain_reference::TypedGrainReference<T> {
        self.get_grain::<T>(&key.to_string())
    }

    /// Get a typed grain reference with an integer key.
    fn get_grain_by_int<T: GrainInterfaceMarker>(
        &self,
        key: i64,
    ) -> crate::grain_reference::TypedGrainReference<T> {
        self.get_grain::<T>(&key.to_string())
    }
}

/// Marker trait for grain interface types.
///
/// This is implemented by generated code for each grain interface.
pub trait GrainInterfaceMarker {
    /// Returns the grain type for grains implementing this interface.
    fn grain_type() -> GrainType;

    /// Returns the interface type identifier.
    fn interface_type() -> GrainInterfaceType;
}

// Blanket implementation for all grain factories
impl<T: IGrainFactory + ?Sized> GrainFactoryExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use orleans_messaging::Message;
    use std::time::Duration;
    use crate::error::RuntimeResult;

    // Mock message sender for testing
    struct MockMessageSender;

    impl MessageSender for MockMessageSender {
        fn send_request(
            &self,
            _message: Message,
            _timeout: Option<Duration>,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = RuntimeResult<Message>> + Send + '_>,
        > {
            Box::pin(async move {
                Ok(Message::new_request(
                    GrainId::new(
                        GrainType::create("Test"),
                        IdSpan::from_str("key"),
                    ),
                    GrainInterfaceType::create("ITest"),
                    1,
                    Bytes::new(),
                    orleans_core::SiloAddress::new("127.0.0.1:11111".parse().unwrap(), 1),
                ))
            })
        }

        fn send_one_way(&self, _message: Message) -> RuntimeResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_convention_interface_resolver() {
        let resolver = ConventionInterfaceResolver;

        let grain_type = GrainType::create("HelloGrain");
        let interface = resolver.resolve(&grain_type);
        assert_eq!(interface.as_str(), Some("IHelloGrain"));
    }

    #[test]
    fn test_map_interface_resolver() {
        let resolver = MapInterfaceResolver::new();
        resolver.register("CustomGrain", "ICustomInterface");

        let custom_type = GrainType::create("CustomGrain");
        let interface = resolver.resolve(&custom_type);
        assert_eq!(interface.as_str(), Some("ICustomInterface"));

        // Fallback to convention
        let other_type = GrainType::create("OtherGrain");
        let interface = resolver.resolve(&other_type);
        assert_eq!(interface.as_str(), Some("IOtherGrain"));
    }

    #[test]
    fn test_grain_factory_creation() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let grain_type = GrainType::create("TestGrain");
        let key = IdSpan::from_str("key1");

        let reference = factory.get_grain_reference(grain_type.clone(), key.clone());

        assert_eq!(reference.grain_type(), &grain_type);
        assert_eq!(reference.grain_id().key(), &key);
        assert_eq!(reference.interface_type().as_str(), Some("ITestGrain"));
    }

    #[test]
    fn test_grain_factory_with_interface() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let grain_type = GrainType::create("TestGrain");
        let key = IdSpan::from_str("key1");
        let interface = GrainInterfaceType::create("ICustomInterface");

        let reference = factory.get_grain_reference_with_interface(
            grain_type.clone(),
            key.clone(),
            interface.clone(),
        );

        assert_eq!(reference.interface_type(), &interface);
    }

    #[test]
    fn test_grain_factory_by_id() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let grain_type = GrainType::create("TestGrain");
        let grain_id = GrainId::new(grain_type.clone(), IdSpan::from_str("key1"));
        let interface = GrainInterfaceType::create("ITestGrain");

        let reference = factory.get_grain_reference_by_id(grain_id.clone(), interface.clone());

        assert_eq!(reference.grain_id(), &grain_id);
        assert_eq!(reference.interface_type(), &interface);
    }

    // Test typed grain access
    struct ITestGrain;

    impl GrainInterfaceMarker for ITestGrain {
        fn grain_type() -> GrainType {
            GrainType::create("TestGrain")
        }

        fn interface_type() -> GrainInterfaceType {
            GrainInterfaceType::create("ITestGrain")
        }
    }

    #[test]
    fn test_get_grain_typed() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let reference = factory.get_grain::<ITestGrain>("my-key");

        assert_eq!(
            reference.as_untyped().grain_type().as_str(),
            Some("TestGrain")
        );
        assert_eq!(
            reference.as_untyped().interface_type().as_str(),
            Some("ITestGrain")
        );
    }

    #[test]
    fn test_get_grain_by_guid() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let guid = uuid::Uuid::new_v4();
        let reference = factory.get_grain_by_guid::<ITestGrain>(guid);

        assert_eq!(
            reference.as_untyped().grain_id().key().as_str(),
            Some(guid.to_string().as_str())
        );
    }

    #[test]
    fn test_get_grain_by_int() {
        let sender = Arc::new(MockMessageSender);
        let factory = GrainFactory::with_convention_resolver(sender);

        let reference = factory.get_grain_by_int::<ITestGrain>(12345);

        assert_eq!(
            reference.as_untyped().grain_id().key().as_str(),
            Some("12345")
        );
    }
}
