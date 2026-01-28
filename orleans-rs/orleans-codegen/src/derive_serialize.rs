//! Derive macros for Orleans serialization.
//!
//! This module provides derive macros to automatically implement
//! `FieldSerialize` and `FieldDeserialize` traits for structs.

use proc_macro2::TokenStream;
use quote::{quote, format_ident};
use syn::{Data, DeriveInput, Error, Fields, Ident, Lit, Result};

/// Information about a field to be serialized.
struct FieldInfo {
    /// The field name (for named structs) or index (for tuple structs).
    ident: FieldIdent,
    /// The field ID for serialization (from #[id(n)] attribute).
    field_id: u32,
    /// The field type.
    ty: syn::Type,
}

/// Field identifier - either a name or a tuple index.
enum FieldIdent {
    Named(Ident),
    Unnamed(syn::Index),
}

impl FieldIdent {
    fn to_token_stream(&self) -> TokenStream {
        match self {
            FieldIdent::Named(ident) => quote!(#ident),
            FieldIdent::Unnamed(index) => quote!(#index),
        }
    }
}

/// Extract field information including the #[id(n)] attribute.
fn extract_field_info(
    field: &syn::Field,
    index: usize,
    next_auto_id: &mut u32,
) -> Result<FieldInfo> {
    let ident = match &field.ident {
        Some(name) => FieldIdent::Named(name.clone()),
        None => FieldIdent::Unnamed(syn::Index::from(index)),
    };

    // Look for #[id(n)] attribute
    let mut field_id = None;
    for attr in &field.attrs {
        if attr.path().is_ident("id") {
            let meta = attr.parse_args::<Lit>()?;
            if let Lit::Int(lit_int) = meta {
                field_id = Some(lit_int.base10_parse::<u32>()?);
            } else {
                return Err(Error::new_spanned(
                    meta,
                    "Expected integer literal in #[id(n)] attribute",
                ));
            }
        }
    }

    // Auto-assign field ID if not specified
    let field_id = match field_id {
        Some(id) => id,
        None => {
            let id = *next_auto_id;
            *next_auto_id += 1;
            id
        }
    };

    Ok(FieldInfo {
        ident,
        field_id,
        ty: field.ty.clone(),
    })
}

/// Generate implementation of `FieldSerialize` for a struct.
pub fn generate_field_serialize(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Collect field info
    let fields = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => &fields_named.named,
            Fields::Unnamed(fields_unnamed) => &fields_unnamed.unnamed,
            Fields::Unit => {
                // Unit structs have no fields to serialize
                return Ok(quote! {
                    impl #impl_generics ::orleans_serialization::codecs::FieldSerialize for #name #ty_generics #where_clause {
                        fn serialize_field(&self, writer: &mut ::orleans_serialization::Writer, field_id: u32) {
                            writer.begin_tag_delimited_field(field_id);
                            writer.write_end_tag();
                            writer.set_field_id(field_id);
                        }
                    }
                });
            }
        },
        Data::Enum(_) => {
            return Err(Error::new_spanned(
                input,
                "OrleansSerialize can only be derived for structs",
            ))
        }
        Data::Union(_) => {
            return Err(Error::new_spanned(
                input,
                "OrleansSerialize cannot be derived for unions",
            ))
        }
    };

    let mut field_infos = Vec::new();
    let mut next_auto_id = 0u32;
    for (index, field) in fields.iter().enumerate() {
        field_infos.push(extract_field_info(field, index, &mut next_auto_id)?);
    }

    // Generate serialization code for each field
    let serialize_fields: Vec<TokenStream> = field_infos
        .iter()
        .map(|fi| {
            let ident = fi.ident.to_token_stream();
            let field_id = fi.field_id;
            quote! {
                ::orleans_serialization::codecs::FieldSerialize::serialize_field(&self.#ident, writer, #field_id);
            }
        })
        .collect();

    // Also generate Serialize trait implementation
    let serialize_impl = quote! {
        impl #impl_generics ::orleans_serialization::codecs::Serialize for #name #ty_generics #where_clause {
            fn serialize(&self, writer: &mut ::orleans_serialization::Writer) {
                #(#serialize_fields)*
            }
        }
    };

    let expanded = quote! {
        impl #impl_generics ::orleans_serialization::codecs::FieldSerialize for #name #ty_generics #where_clause {
            fn serialize_field(&self, writer: &mut ::orleans_serialization::Writer, field_id: u32) {
                use ::orleans_serialization::codecs::FieldSerialize;
                writer.begin_tag_delimited_field(field_id);
                #(#serialize_fields)*
                writer.write_end_tag();
                writer.set_field_id(field_id);
            }
        }

        #serialize_impl
    };

    Ok(expanded)
}

/// Generate implementation of `FieldDeserialize` for a struct.
pub fn generate_field_deserialize(input: DeriveInput) -> Result<TokenStream> {
    let name = &input.ident;
    let generics = &input.generics;
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    // Collect field info
    let (fields, is_named) = match &input.data {
        Data::Struct(data_struct) => match &data_struct.fields {
            Fields::Named(fields_named) => (&fields_named.named, true),
            Fields::Unnamed(fields_unnamed) => (&fields_unnamed.unnamed, false),
            Fields::Unit => {
                // Unit structs just skip to end tag
                return Ok(quote! {
                    impl #impl_generics ::orleans_serialization::codecs::FieldDeserialize for #name #ty_generics #where_clause {
                        fn deserialize_field(reader: &mut ::orleans_serialization::Reader) -> ::orleans_serialization::Result<Self> {
                            let saved_field_id = reader.current_field_id();
                            reader.reset_field_id();

                            // Skip any fields until end tag
                            loop {
                                let field = reader.read_field_header()?;
                                if field.is_end_tag() {
                                    break;
                                }
                                reader.skip_field(&field)?;
                            }

                            reader.set_field_id(saved_field_id);
                            Ok(#name)
                        }
                    }

                    impl #impl_generics ::orleans_serialization::codecs::Deserialize for #name #ty_generics #where_clause {
                        fn deserialize(reader: &mut ::orleans_serialization::Reader) -> ::orleans_serialization::Result<Self> {
                            // Skip any fields until end tag
                            loop {
                                let field = reader.read_field_header()?;
                                if field.is_end_tag() {
                                    break;
                                }
                                reader.skip_field(&field)?;
                            }

                            Ok(#name)
                        }
                    }
                });
            }
        },
        Data::Enum(_) => {
            return Err(Error::new_spanned(
                input,
                "OrleansDeserialize can only be derived for structs",
            ))
        }
        Data::Union(_) => {
            return Err(Error::new_spanned(
                input,
                "OrleansDeserialize cannot be derived for unions",
            ))
        }
    };

    let mut field_infos = Vec::new();
    let mut next_auto_id = 0u32;
    for (index, field) in fields.iter().enumerate() {
        field_infos.push(extract_field_info(field, index, &mut next_auto_id)?);
    }

    // Generate variable declarations for each field (using Default trait)
    let field_declarations: Vec<TokenStream> = field_infos
        .iter()
        .map(|fi| {
            let var_name = match &fi.ident {
                FieldIdent::Named(ident) => format_ident!("field_{}", ident),
                FieldIdent::Unnamed(index) => format_ident!("field_{}", index.index),
            };
            let ty = &fi.ty;
            quote! {
                let mut #var_name: #ty = ::std::default::Default::default();
            }
        })
        .collect();

    // Generate match arms for field deserialization
    let match_arms: Vec<TokenStream> = field_infos
        .iter()
        .map(|fi| {
            let var_name = match &fi.ident {
                FieldIdent::Named(ident) => format_ident!("field_{}", ident),
                FieldIdent::Unnamed(index) => format_ident!("field_{}", index.index),
            };
            let field_id = fi.field_id;
            quote! {
                #field_id => #var_name = ::orleans_serialization::codecs::FieldDeserialize::deserialize_field(reader)?,
            }
        })
        .collect();

    // Generate struct construction
    let struct_construction = if is_named {
        let field_assignments: Vec<TokenStream> = field_infos
            .iter()
            .map(|fi| {
                let field_name = match &fi.ident {
                    FieldIdent::Named(ident) => ident.clone(),
                    FieldIdent::Unnamed(_) => unreachable!(),
                };
                let var_name = format_ident!("field_{}", field_name);
                quote! {
                    #field_name: #var_name,
                }
            })
            .collect();
        quote! {
            #name {
                #(#field_assignments)*
            }
        }
    } else {
        let field_values: Vec<TokenStream> = field_infos
            .iter()
            .map(|fi| {
                let var_name = match &fi.ident {
                    FieldIdent::Named(_) => unreachable!(),
                    FieldIdent::Unnamed(index) => format_ident!("field_{}", index.index),
                };
                quote!(#var_name)
            })
            .collect();
        quote! {
            #name(#(#field_values),*)
        }
    };

    let expanded = quote! {
        impl #impl_generics ::orleans_serialization::codecs::FieldDeserialize for #name #ty_generics #where_clause {
            fn deserialize_field(reader: &mut ::orleans_serialization::Reader) -> ::orleans_serialization::Result<Self> {
                use ::orleans_serialization::codecs::FieldDeserialize;

                let saved_field_id = reader.current_field_id();
                reader.reset_field_id();

                #(#field_declarations)*

                loop {
                    let field = reader.read_field_header()?;
                    if field.is_end_tag() {
                        break;
                    }

                    match reader.current_field_id() {
                        #(#match_arms)*
                        _ => reader.skip_field(&field)?,
                    }
                }

                reader.set_field_id(saved_field_id);
                Ok(#struct_construction)
            }
        }

        impl #impl_generics ::orleans_serialization::codecs::Deserialize for #name #ty_generics #where_clause {
            fn deserialize(reader: &mut ::orleans_serialization::Reader) -> ::orleans_serialization::Result<Self> {
                use ::orleans_serialization::codecs::FieldDeserialize;

                #(#field_declarations)*

                loop {
                    let field = reader.read_field_header()?;
                    if field.is_end_tag() {
                        break;
                    }

                    match reader.current_field_id() {
                        #(#match_arms)*
                        _ => reader.skip_field(&field)?,
                    }
                }

                Ok(#struct_construction)
            }
        }
    };

    Ok(expanded)
}
