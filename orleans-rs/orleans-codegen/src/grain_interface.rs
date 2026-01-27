//! Code generation for `#[grain_interface]` attribute macro.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, Ident, ItemTrait, Pat, ReturnType, Signature, TraitItem, Type, Visibility,
};

/// Generate code for a grain interface trait.
pub fn generate(trait_def: ItemTrait) -> syn::Result<TokenStream> {
    let trait_name = &trait_def.ident;
    let trait_vis = &trait_def.vis;

    // Collect methods from the trait
    let methods: Vec<&Signature> = trait_def
        .items
        .iter()
        .filter_map(|item| match item {
            TraitItem::Fn(method) => Some(&method.sig),
            _ => None,
        })
        .collect();

    // Generate interface type constant
    let interface_type_const = generate_interface_type_const(trait_name, trait_vis);

    // Generate method ID module
    let method_ids_module = generate_method_ids_module(trait_name, trait_vis, &methods);

    // Generate proxy struct and implementation
    let proxy_code = generate_proxy(trait_name, trait_vis, &methods)?;

    // Generate marker trait for typed grain factory access
    let marker_trait = generate_marker_trait(trait_name, trait_vis);

    Ok(quote! {
        // Original trait definition
        #trait_def

        #interface_type_const

        #method_ids_module

        #proxy_code

        #marker_trait
    })
}

/// Generate the interface type constant.
fn generate_interface_type_const(trait_name: &Ident, vis: &Visibility) -> TokenStream {
    let const_name = format_ident!(
        "{}_INTERFACE_TYPE",
        to_screaming_snake_case(&trait_name.to_string())
    );
    let interface_type = trait_name.to_string();

    quote! {
        /// The interface type constant for this grain interface.
        #vis const #const_name: &str = #interface_type;
    }
}

/// Generate the module containing method ID constants.
fn generate_method_ids_module(
    trait_name: &Ident,
    vis: &Visibility,
    methods: &[&Signature],
) -> TokenStream {
    let module_name = format_ident!("{}_methods", to_snake_case(&trait_name.to_string()));

    let method_consts: Vec<TokenStream> = methods
        .iter()
        .enumerate()
        .map(|(idx, sig)| {
            let method_name = &sig.ident;
            let const_name = format_ident!("{}", to_screaming_snake_case(&method_name.to_string()));
            let method_id = (idx + 1) as u32;

            quote! {
                /// Method ID for `#method_name`.
                pub const #const_name: u32 = #method_id;
            }
        })
        .collect();

    let all_ids: Vec<u32> = (1..=methods.len() as u32).collect();

    quote! {
        /// Method ID constants for the grain interface.
        #vis mod #module_name {
            #(#method_consts)*

            /// All method IDs for this interface.
            pub const ALL: &[u32] = &[#(#all_ids),*];
        }
    }
}

/// Generate the proxy struct and its trait implementation.
fn generate_proxy(
    trait_name: &Ident,
    vis: &Visibility,
    methods: &[&Signature],
) -> syn::Result<TokenStream> {
    let proxy_name = format_ident!("{}Proxy", trait_name);
    let methods_module = format_ident!("{}_methods", to_snake_case(&trait_name.to_string()));

    // Generate method implementations for the proxy
    let method_impls: Vec<TokenStream> = methods
        .iter()
        .enumerate()
        .map(|(idx, sig)| generate_proxy_method(sig, idx + 1, &methods_module))
        .collect::<syn::Result<Vec<_>>>()?;

    Ok(quote! {
        /// Client proxy for invoking methods on a remote grain.
        #vis struct #proxy_name {
            grain_reference: std::sync::Arc<dyn orleans_runtime::IGrainReference>,
        }

        impl #proxy_name {
            /// Create a new proxy from a grain reference.
            pub fn new(grain_reference: std::sync::Arc<dyn orleans_runtime::IGrainReference>) -> Self {
                Self { grain_reference }
            }

            /// Get the underlying grain reference.
            pub fn as_grain_reference(&self) -> &std::sync::Arc<dyn orleans_runtime::IGrainReference> {
                &self.grain_reference
            }

            /// Get the grain ID.
            pub fn grain_id(&self) -> &orleans_core::GrainId {
                self.grain_reference.grain_id()
            }
        }

        #[async_trait::async_trait]
        impl #trait_name for #proxy_name {
            #(#method_impls)*
        }

        impl From<std::sync::Arc<dyn orleans_runtime::IGrainReference>> for #proxy_name {
            fn from(grain_reference: std::sync::Arc<dyn orleans_runtime::IGrainReference>) -> Self {
                Self::new(grain_reference)
            }
        }

        impl From<orleans_runtime::TypedGrainReference<dyn #trait_name>> for #proxy_name {
            fn from(typed_ref: orleans_runtime::TypedGrainReference<dyn #trait_name>) -> Self {
                Self::new(typed_ref.as_untyped().clone())
            }
        }
    })
}

/// Generate a single method implementation for the proxy.
fn generate_proxy_method(
    sig: &Signature,
    _method_id: usize,
    methods_module: &Ident,
) -> syn::Result<TokenStream> {
    let method_name = &sig.ident;
    let const_name = format_ident!("{}", to_screaming_snake_case(&method_name.to_string()));

    // Extract parameters (skip &self)
    let params: Vec<(&Ident, &Type)> = sig
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

    // Get return type
    let return_type = match &sig.output {
        ReturnType::Default => quote! { () },
        ReturnType::Type(_, ty) => quote! { #ty },
    };

    // Generate serialization of parameters
    let serialize_params = if params.is_empty() {
        quote! { bytes::Bytes::new() }
    } else {
        // For MVP, we'll use a simple serialization approach
        // TODO: Use proper Orleans serialization when available
        quote! {
            {
                let mut buffer = Vec::new();
                #(
                    // Serialize each parameter
                    // For MVP, use simple byte representation
                    let param_bytes = serialize_param(&#param_names);
                    buffer.extend_from_slice(&param_bytes);
                )*
                bytes::Bytes::from(buffer)
            }
        }
    };

    // Generate deserialization of return value
    let deserialize_result = quote! {
        deserialize_result::<#return_type>(&response_body)
    };

    // Regenerate signature for impl
    let asyncness = &sig.asyncness;
    let inputs = &sig.inputs;
    let output = &sig.output;

    Ok(quote! {
        #asyncness fn #method_name(#inputs) #output {
            let method_id = #methods_module::#const_name;
            let request_body = #serialize_params;

            let response_body = self.grain_reference
                .invoke(method_id, request_body, None)
                .await
                .expect("Grain invocation failed");

            #deserialize_result
        }
    })
}

/// Generate marker trait for typed grain factory access.
fn generate_marker_trait(trait_name: &Ident, _vis: &Visibility) -> TokenStream {
    let interface_type = trait_name.to_string();

    quote! {
        impl orleans_runtime::GrainInterfaceMarker for dyn #trait_name {
            fn interface_type() -> orleans_messaging::GrainInterfaceType {
                orleans_messaging::GrainInterfaceType::create(#interface_type)
            }
        }
    }
}

/// Convert a string to SCREAMING_SNAKE_CASE.
fn to_screaming_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(c.to_ascii_uppercase());
    }
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_screaming_snake_case() {
        assert_eq!(to_screaming_snake_case("ICounterGrain"), "I_COUNTER_GRAIN");
        assert_eq!(to_screaming_snake_case("HelloWorld"), "HELLO_WORLD");
        assert_eq!(to_screaming_snake_case("increment"), "INCREMENT");
    }

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("ICounterGrain"), "i_counter_grain");
        assert_eq!(to_snake_case("HelloWorld"), "hello_world");
        assert_eq!(to_snake_case("getValue"), "get_value");
    }
}
