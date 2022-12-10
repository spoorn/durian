//! Macros for the [durian](https://docs.rs/durian/latest/durian/) crate
//!
//! Contains macros for easily annotating structs as [`Packets`](https://docs.rs/durian/latest/durian/trait.Packet.html) and automatically
//! implementing [`PacketBuilders`](https://docs.rs/durian/latest/durian/trait.PacketBuilder.html). The only
//! requirement is the struct must be de/serializable, meaning all nested fields also need to be
//! de/serializable.
//!
//! `#[bincode_packet]` will de/serialize your Packet using [`bincode`] and applies necessary derive
//! macros automatically for you.
//!
//! ```rust
//! use durian::bincode_packet;
//!
//! // Automatically implements Packet, and generates a PositionPacketBuilder that implements
//! // PacketBuilder.  You can also add other macros such as derive macros so long s they don't
//! // conflict with what #[bincode_packet] adds (See bincode_packet documentation).
//! #[bincode_packet]
//! #[derive(Debug)]
//! struct Position {
//!     x: i32,
//!     y: i32
//! }
//!
//! // Works for Unit (empty) structs as well
//! #[bincode_packet]
//! struct Ack;
//! ```
//!
//! You can also use the derive macros (`BinPacket` and `UnitPacket`) manually:
//!
//! ```rust
//! use durian::serde::{Deserialize, Serialize};
//! use durian::{BinPacket, UnitPacket};
//!
//! #[derive(Serialize, Deserialize, BinPacket)]
//! #[serde(crate = "durian::serde")]
//! struct Position { x: i32, y: i32 }
//!
//! #[derive(UnitPacket)]
//! struct Ack;
//! ```

use proc_macro::{self, TokenStream};
use proc_macro2::{Ident, Span};
use quote::quote;
use syn::{parse_macro_input, Data, DataStruct, DeriveInput, Fields};

/// Macros for easy creation of [`Packets`](https://docs.rs/durian/latest/durian/trait.Packet.html) and [`PacketBuilders`](https://docs.rs/durian/latest/durian/trait.PacketBuilder.html)
///
/// For example:
///
/// ```rust
/// #[bincode_packet]
/// struct Position {
///     x: i32,
///     y: i32
/// }
/// ```
///
/// Generates this code:
///
/// ```rust
/// #[derive(Serialize, Deserialize, BinPacket)]
/// #[serde(crate = "durian::serde")]
/// struct Position {
///     x: i32,
///     y: i32
/// }
/// ```
#[proc_macro_attribute]
pub fn bincode_packet(_attr: TokenStream, tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);

    match &input.data {
        Data::Struct(data_struct) => {
            match &data_struct.fields {
                Fields::Named(fields) => {
                    if fields.named.is_empty() {
                        panic!("Struct annotated with #[bincode_packet] and has named fields cannot have empty number of fields")
                    }
                    // See https://github.com/serde-rs/serde/issues/1465 for why serde macro is needed
                    return quote! {
                        #[derive(durian::serde::Serialize, durian::serde::Deserialize, durian::BinPacket)]
                        #[serde(crate = "durian::serde")]
                        #input
                    }
                    .into();
                }
                Fields::Unit => {
                    return quote! {
                        #[derive(durian::UnitPacket)]
                        #input
                    }
                    .into()
                }
                _ => panic!(
                    "Only Structs with Named fields and Empty Unit structs can be annotated with #[bincode_packet]"
                ),
            }
        }
        _ => panic!("Only structs with named fields can be annotated with #[bincode_packet]"),
    };
}

/// derive macro to automatically implement [`Packet`](https://docs.rs/durian/latest/durian/trait.Packet.html) and [`PacketBuilder`](https://docs.rs/durian/latest/durian/trait.PacketBuilder.html)
/// for a struct.
///
/// For example:
///
/// ```rust
/// #[derive(BinPacket)]
/// struct Position {
///     x: i32,
///     y: i32
/// }
/// ```
///
/// Generates this code:
///
/// ```rust
/// impl durian::Packet for Position {
///     fn as_bytes(&self) -> durian::bytes::Bytes {
///         durian::bytes::Bytes::from(durian::bincode::serialize(self).unwrap())
///     }
/// }
///
/// #[derive(Copy, Clone)]
/// pub struct PositionPacketBuilder;
/// impl durian::PacketBuilder<Position> for PositionPacketBuilder {
///
///     fn read(&self, bytes: durian::bytes::Bytes) -> Result<Position, Box<dyn std::error::Error>> {
///         Ok(durian::bincode::deserialize(bytes.as_ref()).unwrap())
///     }
/// }
/// ```
#[proc_macro_derive(BinPacket)]
pub fn bin_packet(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;
    let packet_builder_name = Ident::new((name.to_string() + "PacketBuilder").as_str(), Span::call_site());

    return quote! {
        impl durian::Packet for #name {
            fn as_bytes(&self) -> durian::bytes::Bytes {
                durian::bytes::Bytes::from(durian::bincode::serialize(self).unwrap())
            }
        }

        #[derive(Copy, Clone)]
        pub struct #packet_builder_name;
        impl durian::PacketBuilder<#name> for #packet_builder_name {

            fn read(&self, bytes: durian::bytes::Bytes) -> Result<#name, Box<dyn std::error::Error>> {
                Ok(durian::bincode::deserialize(bytes.as_ref()).unwrap())
            }
        }
    }
    .into();
}

/// Same as [`BinPacket`] but for empty or Unit structs
///
/// For example:
///
/// ```rust
/// #[derive(UnitPacket)]
/// struct Ack;
/// ```
///
/// Generates this code:
///
/// ```rust
/// impl durian::Packet for Ack {
///     fn as_bytes(&self) -> durian::bytes::Bytes {
///         durian::bytes::Bytes::from("Ack")
///     }
/// }
///
/// #[derive(Copy, Clone)]
/// pub struct AckPacketBuilder;
/// impl durian::PacketBuilder<Ack> for AckPacketBuilder {
///
///     fn read(&self, bytes: durian::bytes::Bytes) -> Result<Ack, Box<dyn std::error::Error>> {
///         Ok(Ack)
///     }
/// }
/// ```
#[proc_macro_derive(UnitPacket)]
pub fn unit_packet(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;
    let name_str = format!("\"{}\"", name);
    let packet_builder_name = Ident::new((name.to_string() + "PacketBuilder").as_str(), Span::call_site());

    return quote! {
        impl durian::Packet for #name {
            fn as_bytes(&self) -> durian::bytes::Bytes {
                durian::bytes::Bytes::from(#name_str)
            }
        }

        #[derive(Copy, Clone)]
        pub struct #packet_builder_name;
        impl durian::PacketBuilder<#name> for #packet_builder_name {

            fn read(&self, bytes: durian::bytes::Bytes) -> Result<#name, Box<dyn std::error::Error>> {
                Ok(#name)
            }
        }
    }
    .into();
}

/// Implements a `new()` function for a struct that contains only a `message: String` field
///
/// Not really for public use.  Used internally for errors in the `durian` crate.
#[proc_macro_derive(ErrorOnlyMessage)]
pub fn error_only_message(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;

    let fields_punct = match input.data {
        Data::Struct(DataStruct {
            fields: Fields::Named(fields),
            ..
        }) => fields.named,
        _ => panic!("Only structs with named fields can be annotated with ErrorMessageNew"),
    };

    let mut has_message = false;
    for field in fields_punct.iter() {
        if field.ident.as_ref().unwrap().to_string() == "message".to_string() {
            has_message = true;
            break;
        }
    }

    if !has_message {
        panic!("Only structs with a named field 'message: String' can be annotated with ErrorMessageNew")
    }

    let modified = quote! {
        impl #name {
            pub fn new<S: Into<String>>(message: S) -> Self {
                #name {
                    message: message.into()
                }
            }
        }

        impl std::error::Error for #name {}
    };
    modified.into()
}
