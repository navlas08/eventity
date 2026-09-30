//! Derive macros for Eventity requests and integration events.
//!
//! Re-exported from the `eventity` crate as [`Request`] and [`Event`].
#![warn(missing_docs)]

extern crate proc_macro;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use quote::quote;
use syn::{Data::Struct, parse_macro_input};
use syn::{DeriveInput, Error, TypeTraitObject};
use syn::{Fields, Type};

#[derive(FromDeriveInput, Clone)]
#[darling(attributes(request))]
struct RequestArgs {
    output: Option<Type>,
    error: Option<TypeTraitObject>,
}

/// Implements `eventity::Request` for a request struct.
///
/// Provide both associated types with the `request` helper attribute:
/// ```rust
/// #[derive(Request)]
/// #[request(output = u32, error = MyError)]
/// struct GetCount;
/// ```
///
/// The derive only accepts structs. It adds the `Request` implementation; the
/// handler is registered separately with `MessageBusBuilder::request_handler`.
#[proc_macro_derive(Request, attributes(request))]
pub fn derive_request(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    if let Struct(_) = &input.data {
        let args = match RequestArgs::from_derive_input(&input) {
            Ok(v) => v,
            Err(e) => return TokenStream::from(e.write_errors()),
        };

        let name = &input.ident;
        let (impl_generics, ty_generics, where_clause) = &input.generics.split_for_impl();

        let Some(output) = args.output else {
            return Error::new(name.span(), "missing #[request(output = ...)]")
                .to_compile_error()
                .into();
        };
        let Some(error) = args.error else {
            return Error::new(name.span(), "missing #[request(error = ...)]")
                .to_compile_error()
                .into();
        };

        quote! {
            impl #impl_generics Request for #name #ty_generics #where_clause {
                type Output = #output;
                type Error = #error;
            }
        }
        .into()
    } else {
        Error::new(
            input.ident.span(),
            "Request can only be derived for structs",
        )
        .to_compile_error()
        .into()
    }
}

#[derive(FromDeriveInput, Clone)]
#[darling(attributes(event))]
struct EventArgs {
    error: Option<Type>,
}

/// Implements `eventity::IntegrationEvent` for an event struct.
///
/// The event must derive Serde `Serialize` and `Deserialize`, and define its
/// handler error with `#[event(error = MyError)]`. Routing defaults are derived
/// from names: the queue is the struct name, the exchange is the package name,
/// and the aggregate name is the prefix before the first uppercase letter after
/// the first character. Override aggregate ID selection by marking a named
/// field `#[aggregate_id]`; otherwise a named `id` or `aggregate_id` field is
/// required. For tuple structs or custom routing conventions, implement
/// `IntegrationEvent` manually.
///
/// ```rust,ignore
/// #[derive(Serialize, Deserialize, Event)]
/// #[event(error = MyError)]
/// struct OrderCreated {
///     #[aggregate_id]
///     order_id: Uuid,
///     customer_id: Uuid,
/// }
/// ```
#[proc_macro_derive(Event, attributes(event, aggregate_id))]
pub fn derive_event(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);

    if let Struct(ref data_struct) = input.data {
        let args = match EventArgs::from_derive_input(&input) {
            Ok(v) => v,
            Err(e) => return TokenStream::from(e.write_errors()),
        };

        let queue_name = &input.ident;
        let queue_name_str = queue_name.to_string();

        let split_index = queue_name_str
            .char_indices()
            .skip(1)
            .find(|&(_, c)| c.is_uppercase())
            .map(|(i, _)| i)
            .unwrap_or(queue_name_str.len());

        let aggregate_name = queue_name_str[..split_index].to_string();

        let mut agg_id_field = None;

        if let Fields::Named(ref fields) = data_struct.fields {
            for field in &fields.named {
                if field
                    .attrs
                    .iter()
                    .any(|attr| attr.path().is_ident("aggregate_id"))
                {
                    agg_id_field = field.ident.clone();
                    break;
                }
                if field
                    .ident
                    .as_ref()
                    .is_some_and(|ident| ident == "id" || ident == "aggregate_id")
                {
                    agg_id_field = field.ident.clone();
                }
            }
        }

        let Some(agg_id_field) = agg_id_field else {
            return Error::new(
                input.ident.span(),
                "Event requires a named `id` or `aggregate_id` field; implement IntegrationEvent manually for other layouts",
            )
            .to_compile_error()
            .into();
        };
        let aggregate_id_impl = quote! {
            fn aggregate_id(&self) -> String {
                self.#agg_id_field.to_string()
            }
        };

        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

        let Some(error) = args.error else {
            return Error::new(queue_name.span(), "missing #[event(error = ...)]")
                .to_compile_error()
                .into();
        };

        let crate_name = std::env::var("CARGO_PKG_NAME")
            .unwrap_or_else(|_| "unknown_crate".to_string())
            .replace("-", "_");

        quote! {
            impl #impl_generics IntegrationEvent for #queue_name #ty_generics #where_clause {
                type Error = #error;

                fn exchange() -> &'static str {
                    #crate_name
                }

                fn queue() -> &'static str {
                    #queue_name_str
                }

                fn aggregate() -> &'static str {
                    #aggregate_name
                }

                #aggregate_id_impl
            }
        }
        .into()
    } else {
        Error::new(input.ident.span(), "Event can only be derived for structs")
            .to_compile_error()
            .into()
    }
}
