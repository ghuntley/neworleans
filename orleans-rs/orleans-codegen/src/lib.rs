//! Procedural macros for Orleans grain code generation.
//!
//! This crate provides two main attribute macros:
//!
//! - `#[grain_interface]` - Applied to trait definitions to generate grain interface metadata,
//!   method ID constants, and client proxy implementations.
//!
//! - `#[grain]` - Applied to struct definitions to generate grain activators, method invokers,
//!   and type registration helpers.
//!
//! # Example
//!
//! ```rust,ignore
//! use orleans_codegen::{grain_interface, grain};
//!
//! #[grain_interface]
//! pub trait ICounterGrain {
//!     async fn increment(&self) -> u32;
//!     async fn get_value(&self) -> u32;
//! }
//!
//! #[grain]
//! pub struct CounterGrain {
//!     counter: u32,
//! }
//!
//! impl ICounterGrain for CounterGrain {
//!     async fn increment(&mut self) -> u32 {
//!         self.counter += 1;
//!         self.counter
//!     }
//!
//!     async fn get_value(&self) -> u32 {
//!         self.counter
//!     }
//! }
//! ```

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{parse_macro_input, ItemImpl, ItemStruct, ItemTrait};

mod grain;
mod grain_interface;

/// Marks a trait as an Orleans grain interface.
///
/// This macro generates:
/// - A `GrainInterfaceType` constant for the interface
/// - Method ID constants for each method
/// - A proxy struct that implements the trait for remote grain invocation
/// - A marker trait implementation for typed grain factory access
///
/// # Method ID Generation
///
/// Method IDs are generated using a stable hash of the method name and signature.
/// Methods are assigned IDs starting from 1 in declaration order.
///
/// # Example
///
/// ```rust,ignore
/// use orleans_codegen::grain_interface;
///
/// #[grain_interface]
/// pub trait IHelloGrain {
///     /// Says hello to the given name.
///     async fn say_hello(&self, name: String) -> String;
///
///     /// Gets the greeting count.
///     async fn get_count(&self) -> u32;
/// }
/// ```
///
/// This generates:
///
/// ```rust,ignore
/// // Interface type constant
/// pub const IHELLO_GRAIN_INTERFACE_TYPE: &str = "IHelloGrain";
///
/// // Method ID constants
/// pub mod ihello_grain_methods {
///     pub const SAY_HELLO: u32 = 1;
///     pub const GET_COUNT: u32 = 2;
/// }
///
/// // Proxy implementation for remote calls
/// pub struct IHelloGrainProxy { ... }
///
/// impl IHelloGrain for IHelloGrainProxy {
///     async fn say_hello(&self, name: String) -> String { ... }
///     async fn get_count(&self) -> u32 { ... }
/// }
/// ```
#[proc_macro_attribute]
pub fn grain_interface(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let trait_def = parse_macro_input!(item as ItemTrait);
    grain_interface::generate(trait_def)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Marks a struct as an Orleans grain.
///
/// This macro generates:
/// - An `IGrain` trait implementation
/// - An activator struct implementing `IGrainActivator`
/// - A helper function to create `GrainTypeData`
///
/// The struct must also have an `impl` block for a grain interface (marked with
/// `#[grain_interface]`). Use `#[grain_impl]` on that impl block to generate
/// the method invoker.
///
/// # Attributes
///
/// - `#[grain(type_name = "CustomName")]` - Override the grain type name (defaults to struct name)
///
/// # Example
///
/// ```rust,ignore
/// use orleans_codegen::{grain, grain_impl, grain_interface};
///
/// #[grain_interface]
/// pub trait ICounterGrain {
///     async fn increment(&mut self) -> u32;
///     async fn get_value(&self) -> u32;
/// }
///
/// #[grain]
/// pub struct CounterGrain {
///     counter: u32,
/// }
///
/// impl CounterGrain {
///     pub fn new() -> Self {
///         Self { counter: 0 }
///     }
/// }
///
/// #[grain_impl(ICounterGrain)]
/// impl ICounterGrain for CounterGrain {
///     async fn increment(&mut self) -> u32 {
///         self.counter += 1;
///         self.counter
///     }
///
///     async fn get_value(&self) -> u32 {
///         self.counter
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn grain(attr: TokenStream, item: TokenStream) -> TokenStream {
    let struct_def = parse_macro_input!(item as ItemStruct);
    let attr_tokens = TokenStream2::from(attr);
    grain::generate_grain(attr_tokens, struct_def)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

/// Marks an impl block as implementing a grain interface.
///
/// This generates the method invoker that dispatches method calls to the grain.
///
/// # Arguments
///
/// - The first argument is the interface trait name (required)
///
/// # Example
///
/// ```rust,ignore
/// #[grain_impl(ICounterGrain)]
/// impl ICounterGrain for CounterGrain {
///     async fn increment(&mut self) -> u32 {
///         self.counter += 1;
///         self.counter
///     }
///
///     async fn get_value(&self) -> u32 {
///         self.counter
///     }
/// }
/// ```
#[proc_macro_attribute]
pub fn grain_impl(attr: TokenStream, item: TokenStream) -> TokenStream {
    let impl_block = parse_macro_input!(item as ItemImpl);
    let attr_tokens = TokenStream2::from(attr);
    grain::generate_grain_impl(attr_tokens, impl_block)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}
