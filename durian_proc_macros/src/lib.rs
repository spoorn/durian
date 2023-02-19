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
                    #[allow(clippy::needless_return)]
                    return quote! {
                        #[derive(durian::serde::Serialize, durian::serde::Deserialize, durian::BinPacket)]
                        #[serde(crate = "durian::serde")]
                        #input
                    }
                        .into();
                }
                Fields::Unit =>
                    {
                        #[allow(clippy::needless_return)]
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

    #[allow(clippy::needless_return)]
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

    #[allow(clippy::needless_return)]
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

/// Convenience derive macro that implements [`Deref`] and [`DerefMut`] for a struct that contains a
/// `manager: PacketManager` field.
///
/// This is especially useful in situations where you must wrap a PacketManager behind a struct, such as in Bevy where
/// you want to insert the PacketManager as a Resource, but can't annotate the PacketManager struct itself.
///
/// For example:
///
/// ```rust
/// #[derive(DerefPacketManager)]
/// struct ClientPacketManager {
///     manager: PacketManager
/// }
/// ```
///
/// Generates this code:
///
/// ```rust
/// struct ClientPacketManager {
///     manager: PacketManager
/// }
///
/// impl std::ops::Deref for ClientPacketManager {
///     type Target = (durian::PacketManager);
///
///     fn deref(&self) -> &Self::Target {
///         &self.manager
///     }
/// }
///
/// impl std::ops::DerefMut for ClientPacketManager {
///     fn deref_mut(&mut self) -> &mut Self::Target {
///         &mut self.manager
///     }
/// }
/// ```
///
/// You can then automatically access `PacketManager` functions through a `ClientPacketManager` like this:
///
/// ```rust
/// let manager = ClientPacketManager { manager: PacketManager::new() };
/// // Directly call PacketManager functions through our manager variable without accessing the underlying field
/// manager.init_client(ClientConfig::new("127.0.0.1:5001", "127.0.0.1:5000", 2, 2);
/// ```
#[proc_macro_derive(DerefPacketManager)]
pub fn deref_packet_manager(tokens: TokenStream) -> TokenStream {
    let input = parse_macro_input!(tokens as DeriveInput);
    let name = input.ident;

    match input.data {
        Data::Struct(DataStruct {
                         fields: Fields::Named(fields),
                         ..
                     }) => {
            let mut has_manager = false;
            for field in fields.named.iter() {
                if *field.ident.as_ref().unwrap() == *"manager" {
                    has_manager = true;
                    break;
                }
            }

            if !has_manager {
                panic!("DerefPacketManager can only be used on a struct with a 'manager: PacketManager' field")
            }
        }
        _ => panic!("Only structs with named fields can be annotated with ErrorMessageNew"),
    };

    #[allow(clippy::needless_return)]
    return quote! {
        impl std::ops::Deref for #name {
            type Target = (durian::PacketManager);

            fn deref(&self) -> &Self::Target {
                &self.manager
            }
        }

        impl std::ops::DerefMut for #name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.manager
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
        if *field.ident.as_ref().unwrap() == *"message" {
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
                    message: message.into(),
                    error_type: durian::ErrorType::Unexpected
                }
            }

            pub fn new_with_type<S: Into<String>>(message: S, error_type: durian::ErrorType) -> Self {
                #name {
                    message: message.into(),
                    error_type
                }
            }
        }

        impl std::error::Error for #name {}
    };
    modified.into()
}
