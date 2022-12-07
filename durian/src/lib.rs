//! _"You either love it, hate it, or never heard of it.  There is no in between"_
//! 
//! `durian` is a client-server networking library built on top of the QUIC protocol, implemented
//! in Rust by [quinn](https://github.com/quinn-rs/quinn).
//! 
//! It provides a thin abstraction layer above the lower-level details of byte management, 
//! framing, and more, to make writing netcode easier and allow the user to focus on the messaging
//! contents instead.
//! 
//! # Cargo.toml Dependency
//! 
//! Add `durian` to your Cargo.toml via `cargo add durian` or manually:
//! 
//! ```toml
//! [dependencies]
//! durian = "a.b.c"
//! 
//! // The macros are included by default.  To disable, use
//! // durian = { version = "a.b.c", features = ["no-macros"] }
//! ```
//! 
//! # Packet/PacketBuilder
//! 
//! `durian` allows for structuring [`Packets`] as simple structs.  The structs must implement
//! Trait [`Packet`], which has a single function [`Packet::as_bytes()`] for serializing the [`Packet`]
//! into bytes to be sent over the wire between client and server.
//!
//! 
//! # PacketManager
//! 
//! and sending them between clients and
//! servers asynchronously or synchronously via a [`PacketManager`](PacketManager) which can represent
//! either a client connected to a single server, or a server connected to multiple clients.
//! 
//! # Examples
//! 
//! For beginners, creating packets to be sent between clients/server should be extremely straight-forward
//! and the [Example](#Example) below covers most of what you'd need.  For more complex scenarios, such as
//! serializing/deserializing packets in a custom way, can be done by implementing the various Traits
//! yourself, or through extra configurations in the [`PacketManager`](PacketManager).

mod quinn_helpers;
mod packet;

pub use packet::*;

#[cfg(feature = "durian_macros")]
#[allow(unused_imports)]
pub use durian_macros;

#[cfg(feature = "durian_macros")]
#[allow(unused_imports)]
pub use durian_macros::*;

#[cfg(feature = "durian_macros")]
#[allow(unused_imports)]
pub use bytes;

#[cfg(feature = "durian_macros")]
#[allow(unused_imports)]
pub use serde;

#[cfg(feature = "durian_macros")]
#[allow(unused_imports)]
pub use bincode;