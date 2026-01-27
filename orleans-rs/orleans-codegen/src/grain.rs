//! Code generation for `#[grain]` and `#[grain_impl]` attribute macros.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse2, FnArg, Ident, ImplItem, ImplItemFn, ItemImpl, ItemStruct, Pat, ReturnType, Type,
    Visibility,
};

/// Parse grain attributes.
struct GrainAttrs {
    type_name: Option<String>,
}

impl GrainAttrs {
    fn parse(_tokens: TokenStream) -> syn::Result<Self> {
        // For MVP, just support empty attributes or type_name = "..."
        // TODO: Parse more complex attributes
        Ok(GrainAttrs { type_name: None })
    }
}

/// Generate code for a grain struct definition.
pub fn generate_grain(attrs: TokenStream, struct_def: ItemStruct) -> syn::Result<TokenStream> {
    let attrs = GrainAttrs::parse(attrs)?;
    let struct_name = &struct_def.ident;
    let struct_vis = &struct_def.vis;

    // Use custom type name or default to struct name
    let grain_type_name = attrs
        .type_name
        .unwrap_or_else(|| struct_name.to_string());

    // Generate IGrain implementation
    let igrain_impl = generate_igrain_impl(struct_name, &grain_type_name);

    // Generate activator struct
    let activator = generate_activator(struct_name, struct_vis, &grain_type_name);

    // Generate helper function to create grain type data
    let create_grain_type_fn = generate_create_grain_type_fn(struct_name, struct_vis);

    Ok(quote! {
        // Original struct definition
        #struct_def

        #igrain_impl

        #activator

        #create_grain_type_fn
    })
}

/// Generate IGrain trait implementation.
fn generate_igrain_impl(struct_name: &Ident, grain_type_name: &str) -> TokenStream {
    quote! {
        #[async_trait::async_trait]
        impl orleans_runtime::IGrain for #struct_name {
            fn grain_type() -> orleans_core::GrainType
            where
                Self: Sized,
            {
                orleans_core::GrainType::create(#grain_type_name)
            }
        }
    }
}

/// Generate activator struct and IGrainActivator implementation.
fn generate_activator(
    struct_name: &Ident,
    vis: &Visibility,
    grain_type_name: &str,
) -> TokenStream {
    let activator_name = format_ident!("{}Activator", struct_name);

    quote! {
        /// Activator for creating instances of `#struct_name`.
        #vis struct #activator_name;

        impl orleans_runtime::IGrainActivator for #activator_name {
            fn create(&self, _grain_id: &orleans_core::GrainId) -> Box<dyn std::any::Any + Send + Sync> {
                Box::new(#struct_name::default())
            }

            fn grain_type(&self) -> orleans_core::GrainType {
                orleans_core::GrainType::create(#grain_type_name)
            }
        }
    }
}

/// Generate helper function to create GrainTypeData.
fn generate_create_grain_type_fn(struct_name: &Ident, vis: &Visibility) -> TokenStream {
    let fn_name = format_ident!("create_{}_type", to_snake_case(&struct_name.to_string()));
    let activator_name = format_ident!("{}Activator", struct_name);

    quote! {
        /// Create the grain type data for `#struct_name`.
        ///
        /// This function creates an `Arc<GrainTypeData>` with the activator
        /// but without any invokers. Use `.with_invoker()` to add interface
        /// invokers before registering with a silo.
        #vis fn #fn_name() -> std::sync::Arc<orleans_runtime::GrainTypeData> {
            let activator = std::sync::Arc::new(#activator_name);
            std::sync::Arc::new(orleans_runtime::GrainTypeData::new(
                <#struct_name as orleans_runtime::IGrain>::grain_type(),
                activator,
            ))
        }
    }
}

/// Generate code for a grain impl block.
pub fn generate_grain_impl(attrs: TokenStream, impl_block: ItemImpl) -> syn::Result<TokenStream> {
    // Parse the interface name from attributes
    let interface_name: Ident = parse2(attrs)?;

    // Get the implementing struct name
    let struct_type = &impl_block.self_ty;
    let struct_name = match struct_type.as_ref() {
        Type::Path(type_path) => type_path
            .path
            .get_ident()
            .cloned()
            .ok_or_else(|| syn::Error::new_spanned(struct_type, "Expected a simple type name"))?,
        _ => {
            return Err(syn::Error::new_spanned(
                struct_type,
                "Expected a simple type name",
            ))
        }
    };

    // Collect methods from the impl block
    let methods: Vec<&ImplItemFn> = impl_block
        .items
        .iter()
        .filter_map(|item| match item {
            ImplItem::Fn(method) => Some(method),
            _ => None,
        })
        .collect();

    // Generate invoker struct
    let invoker = generate_invoker(&struct_name, &interface_name, &methods)?;

    // Generate helper to create grain type data with this invoker
    let with_invoker_fn = generate_with_invoker_fn(&struct_name, &interface_name);

    Ok(quote! {
        // Original impl block
        #impl_block

        #invoker

        #with_invoker_fn
    })
}

/// Generate invoker struct and IGrainMethodInvoker implementation.
fn generate_invoker(
    struct_name: &Ident,
    interface_name: &Ident,
    methods: &[&ImplItemFn],
) -> syn::Result<TokenStream> {
    let invoker_name = format_ident!("{}{}Invoker", struct_name, interface_name);
    let interface_type = interface_name.to_string();
    let _methods_module = format_ident!("{}_methods", to_snake_case(&interface_name.to_string()));

    // Generate method dispatch arms
    let dispatch_arms: Vec<TokenStream> = methods
        .iter()
        .enumerate()
        .map(|(idx, method)| generate_dispatch_arm(struct_name, method, idx + 1))
        .collect::<syn::Result<Vec<_>>>()?;

    // Generate method IDs array
    let method_ids: Vec<u32> = (1..=methods.len() as u32).collect();

    Ok(quote! {
        /// Method invoker for `#struct_name` implementing `#interface_name`.
        pub struct #invoker_name;

        impl #invoker_name {
            /// The interface type this invoker handles.
            pub const INTERFACE_TYPE: &'static str = #interface_type;

            /// All method IDs this invoker handles.
            pub const METHOD_IDS: &'static [u32] = &[#(#method_ids),*];
        }

        #[async_trait::async_trait]
        impl orleans_runtime::IGrainMethodInvoker for #invoker_name {
            fn interface_type(&self) -> &str {
                Self::INTERFACE_TYPE
            }

            fn method_ids(&self) -> &[u32] {
                Self::METHOD_IDS
            }

            async fn invoke(
                &self,
                grain: &mut dyn std::any::Any,
                _context: &dyn orleans_runtime::IGrainContext,
                method_id: u32,
                body: &[u8],
            ) -> orleans_runtime::RuntimeResult<Vec<u8>> {
                let grain = grain
                    .downcast_mut::<#struct_name>()
                    .ok_or_else(|| orleans_runtime::RuntimeError::Internal(
                        format!("Failed to downcast grain to {}", stringify!(#struct_name))
                    ))?;

                match method_id {
                    #(#dispatch_arms)*
                    _ => Err(orleans_runtime::RuntimeError::MethodNotFound {
                        interface_type: Self::INTERFACE_TYPE.to_string(),
                        method_id,
                    }),
                }
            }
        }
    })
}

/// Generate a single dispatch arm for a method.
fn generate_dispatch_arm(
    _struct_name: &Ident,
    method: &ImplItemFn,
    method_id: usize,
) -> syn::Result<TokenStream> {
    let method_name = &method.sig.ident;
    let method_id_u32 = method_id as u32;

    // Extract parameters (skip &self/&mut self)
    let params: Vec<(&Ident, &Type)> = method
        .sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Receiver(_) => None,
            FnArg::Typed(pat_type) => {
                if let Pat::Ident(pat_ident) = pat_type.pat.as_ref() {
                    Some((&pat_ident.ident, pat_type.ty.as_ref()))
                } else {
                    None
                }
            }
        })
        .collect();

    let param_names: Vec<&Ident> = params.iter().map(|(name, _)| *name).collect();

    // Check if method is async
    let is_async = method.sig.asyncness.is_some();

    // For MVP, generate simple code that works with no-arg methods
    // and primitive return types. More complex serialization should
    // use the orleans-serialization crate.
    let call = if is_async {
        quote! { grain.#method_name(#(#param_names),*).await }
    } else {
        quote! { grain.#method_name(#(#param_names),*) }
    };

    // Get return type
    let has_return = !matches!(method.sig.output, ReturnType::Default);

    // Generate parameter deserialization if needed
    // For MVP, we expect body to be empty for no-arg methods
    // TODO: Add proper parameter deserialization using orleans-serialization
    let param_setup = if params.is_empty() {
        quote! { let _ = body; } // Suppress unused warning
    } else {
        // For MVP, skip parameter deserialization
        // Users should provide custom invokers for methods with parameters
        quote! {
            // TODO: Deserialize parameters from body
            // For MVP, methods with parameters require manual invoker implementation
            let _ = body;
            compile_error!("Methods with parameters are not yet supported by codegen. Please write a manual invoker.");
        }
    };

    let serialize_result = if has_return {
        quote! {
            let result = #call;
            // Use GrainSerialize trait for result serialization
            Ok(orleans_runtime::GrainSerialize::serialize(&result))
        }
    } else {
        quote! {
            #call;
            Ok(Vec::new())
        }
    };

    Ok(quote! {
        #method_id_u32 => {
            #param_setup
            #serialize_result
        }
    })
}

/// Generate helper function to create grain type data with an invoker.
fn generate_with_invoker_fn(struct_name: &Ident, interface_name: &Ident) -> TokenStream {
    let fn_name = format_ident!(
        "create_{}_type_with_{}",
        to_snake_case(&struct_name.to_string()),
        to_snake_case(&interface_name.to_string())
    );
    let invoker_name = format_ident!("{}{}Invoker", struct_name, interface_name);
    let activator_name = format_ident!("{}Activator", struct_name);
    let interface_type = interface_name.to_string();

    quote! {
        /// Create the grain type data for `#struct_name` with the `#interface_name` invoker.
        pub fn #fn_name() -> std::sync::Arc<orleans_runtime::GrainTypeData> {
            let invoker: std::sync::Arc<dyn orleans_runtime::IGrainMethodInvoker> =
                std::sync::Arc::new(#invoker_name);

            let activator = std::sync::Arc::new(#activator_name);
            let mut data = orleans_runtime::GrainTypeData::new(
                <#struct_name as orleans_runtime::IGrain>::grain_type(),
                activator,
            );
            data = data.with_invoker(#interface_type, invoker);
            std::sync::Arc::new(data)
        }
    }
}

/// Convert a string to snake_case.
fn to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_ascii_lowercase());
    }
    result
}
