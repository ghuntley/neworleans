//! Tests for the Orleans code generation macros.
//!
//! Note: Testing proc macros requires the generated code to compile,
//! so these tests use compile-time expansion via cargo expand or
//! runtime verification of the generated types.

use std::sync::Arc;

// Re-export test dependencies
use async_trait::async_trait;
use orleans_core::{GrainId, GrainType, IdSpan};
use orleans_runtime::{
    GrainSerialize, GrainTypeData, IGrain, IGrainActivator, IGrainContext, IGrainMethodInvoker,
    RuntimeResult,
};

/// Test that GrainSerialize trait works for u32.
#[test]
fn test_grain_serialize_u32() {
    let value: u32 = 12345;
    let bytes = value.serialize();
    assert_eq!(bytes, value.to_le_bytes().to_vec());
}

/// Test that GrainSerialize trait works for String.
#[test]
fn test_grain_serialize_string() {
    let value = String::from("Hello");
    let bytes = value.serialize();
    // First 4 bytes are the length, then the string data
    assert_eq!(bytes.len(), 4 + 5);
    assert_eq!(&bytes[0..4], &5u32.to_le_bytes());
    assert_eq!(&bytes[4..], b"Hello");
}

// ============================================================================
// Manual grain implementation for testing codegen patterns
// ============================================================================

/// A test grain that manually implements all the traits that codegen should generate.
/// This serves as the "golden" implementation to compare against generated code.
struct TestGrain {
    counter: u32,
}

impl Default for TestGrain {
    fn default() -> Self {
        Self { counter: 0 }
    }
}

impl TestGrain {
    fn increment(&mut self) -> u32 {
        self.counter += 1;
        self.counter
    }

    fn get_value(&self) -> u32 {
        self.counter
    }
}

#[async_trait]
impl IGrain for TestGrain {
    fn grain_type() -> GrainType
    where
        Self: Sized,
    {
        GrainType::create("TestGrain")
    }
}

/// Activator for TestGrain - matches what `#[grain]` should generate.
struct TestGrainActivator;

impl IGrainActivator for TestGrainActivator {
    fn create(&self, _grain_id: &GrainId) -> Box<dyn std::any::Any + Send + Sync> {
        Box::new(TestGrain::default())
    }

    fn grain_type(&self) -> GrainType {
        TestGrain::grain_type()
    }
}

/// Invoker for TestGrain - matches what `#[grain_impl]` should generate.
struct TestGrainITestGrainInvoker;

impl TestGrainITestGrainInvoker {
    const INTERFACE_TYPE: &'static str = "ITestGrain";
    const METHOD_IDS: &'static [u32] = &[1, 2];
}

impl IGrainMethodInvoker for TestGrainITestGrainInvoker {
    fn interface_type(&self) -> &str {
        Self::INTERFACE_TYPE
    }

    fn method_ids(&self) -> &[u32] {
        Self::METHOD_IDS
    }

    // Manually expand async_trait to avoid Send bound issues
    fn invoke<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        grain: &'life1 mut dyn std::any::Any,
        _context: &'life2 dyn IGrainContext,
        method_id: u32,
        body: &'life3 [u8],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = RuntimeResult<Vec<u8>>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        // The grain is Box<dyn Any + Send + Sync>, so we need to downcast it
        let grain = grain
            .downcast_mut::<TestGrain>()
            .expect("Failed to downcast grain");

        let _ = body; // Suppress unused warning
        let result = match method_id {
            1 => {
                // increment() -> u32
                let value = grain.increment();
                Ok(GrainSerialize::serialize(&value))
            }
            2 => {
                // get_value() -> u32
                let value = grain.get_value();
                Ok(GrainSerialize::serialize(&value))
            }
            _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                interface_type: Self::INTERFACE_TYPE.to_string(),
                method_id,
            }),
        };

        Box::pin(std::future::ready(result))
    }
}

/// Helper function to create GrainTypeData - matches what codegen should generate.
fn create_test_grain_type() -> Arc<GrainTypeData> {
    let activator = Arc::new(TestGrainActivator);
    let invoker: Arc<dyn IGrainMethodInvoker> = Arc::new(TestGrainITestGrainInvoker);

    let data = GrainTypeData::new(TestGrain::grain_type(), activator)
        .with_invoker("ITestGrain", invoker);

    Arc::new(data)
}

// ============================================================================
// Tests for the manually implemented grain (verify the pattern works)
// ============================================================================

#[test]
fn test_grain_type_creation() {
    let grain_type = TestGrain::grain_type();
    assert_eq!(grain_type.as_str(), Some("TestGrain"));
}

#[test]
fn test_activator_creates_grain() {
    let activator = TestGrainActivator;
    let grain_id = GrainId::new(TestGrain::grain_type(), IdSpan::from_str("test-1"));
    let boxed = activator.create(&grain_id);

    // Verify we can downcast it back
    let grain = boxed.downcast_ref::<TestGrain>();
    assert!(grain.is_some());
    assert_eq!(grain.unwrap().counter, 0);
}

#[test]
fn test_grain_type_data_creation() {
    let grain_type_data = create_test_grain_type();

    assert_eq!(grain_type_data.grain_type.as_str(), Some("TestGrain"));
    assert!(grain_type_data.invokers.contains_key("ITestGrain"));
}

// ============================================================================
// Interface type constant tests (matches what #[grain_interface] should generate)
// ============================================================================

/// Interface type constant - what `#[grain_interface]` should generate
const I_TEST_GRAIN_INTERFACE_TYPE: &str = "ITestGrain";

/// Method IDs module - what `#[grain_interface]` should generate
mod i_test_grain_methods {
    pub const INCREMENT: u32 = 1;
    pub const GET_VALUE: u32 = 2;
    pub const ALL: &[u32] = &[1, 2];
}

#[test]
fn test_interface_type_constant() {
    assert_eq!(I_TEST_GRAIN_INTERFACE_TYPE, "ITestGrain");
}

#[test]
fn test_method_id_constants() {
    assert_eq!(i_test_grain_methods::INCREMENT, 1);
    assert_eq!(i_test_grain_methods::GET_VALUE, 2);
    assert_eq!(i_test_grain_methods::ALL, &[1, 2]);
}

#[test]
fn test_invoker_interface_type() {
    let invoker = TestGrainITestGrainInvoker;
    assert_eq!(invoker.interface_type(), "ITestGrain");
}

#[test]
fn test_invoker_method_ids() {
    let invoker = TestGrainITestGrainInvoker;
    assert_eq!(invoker.method_ids(), &[1, 2]);
}
