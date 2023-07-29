//! _"This library stinks!" ... "Unless you like durian"_
//!
//! `durian` is a client-server networking library built on top of the [QUIC](https://en.wikipedia.org/wiki/QUIC) protocol which is
//! implemented in Rust by [quinn](https://github.com/quinn-rs/quinn).
//!
//! It provides a thin abstraction layer above the lower-level details of connection management, byte management,
//! framing, and more, to make writing netcode easier and allow the user to focus on the messaging
//! contents instead.  Serialization and deserialization are built into the APIs so you can send and receive exact
//! Packets as structs for ease of development.
//!
//! ### Disclaimer
//! This library is in very early (but very active!) development, meaning a LOT of it will change rapidly.  
//! In its current state, it's usable to create a quick multiplayer demo.  I use it myself to learn [game
//! development](https://github.com/spoorn/multisnakegame).
//!
//! This __is not production ready__, and is missing a lot of features to make it production ready.
//!
//! However, if you are trying to build something and want to avoid a lot of the headache of lower-level netcode details,
//! and don't need the "production" features, such as a multiplayer game demo, LAN sandbox applications, etc., then feel
//! free to try it out!  
//!
//! `durian`'s goal is to make it as simple as possible to setup netcode (See the examples below).
//!
//! # Cargo.toml Dependency
//!
//! Add `durian` to your Cargo.toml via `cargo add durian` or manually:
//!
//! ```toml
//! [dependencies]
//! durian = "0.3"
//! ```
//!
//! # Packet/PacketBuilder
//!
//! There are 2 steps needed to create a `Packet` to be used with `durian`:
//!
//! 1. `durian` allows for structuring `Packets` as simple structs.  The structs must implement
//! Trait [`Packet`], which has a single function [`Packet::as_bytes()`] which will be called for
//! serializing the [`Packet`] into bytes to be sent over the wire between client and server.
//!
//! 2. There also needs to be a struct that implements [`PacketBuilder`] which is used to
//! deserialize from bytes back into your [`Packet`] struct via the [`PacketBuilder::read()`]
//! function.
//!
//! For your convenience, `durian` is bundled with [`durian_macros`] which contains a few macros that
//! help autogenerate Impl blocks for a struct for both [`Packet`] and [`PacketBuilder`].  The only
//! requirement is the struct must be de/serializable, meaning all nested fields also need to be
//! de/serializable.
//!
//! [`bincode_packet`] will de/serialize your Packet using [`bincode`] and applies necessary derive
//! macros automatically for you.
//! ```rust
//! use durian::bincode_packet;
//!
//! // Automatically implements Packet, and generates a PositionPacketBuilder that implements
//! // PacketBuilder.  You can also add other macros such as derive macros so long as they don't
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
//! You can also use the derive macros ([`BinPacket`] and [`UnitPacket`]) manually:
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
//!
//!
//! # PacketManager
//!
//! [`PacketManager`] is what you will use to initiate connections between clients/servers, and
//! send/receive `Packets`.  
//!
//! A [`PacketManager`] would be created on each client to connect to a
//! single server, and one created on the server to connect to multiple clients. It contains both
//! synchronous and asynchronous APIs, so you can call the functions both from a synchronous
//! context, or within an async runtime (_Note: the synchronous path will create a separate
//! isolated async runtime context per `PacketManager` instance._)
//!
//! There are 4 basic steps to using the [`PacketManager`], which would be done on both the client
//! and server side:
//!
//! 1. Create a [`PacketManager`] via [`new()`](`PacketManager::new()`) or, if calling from an async context, [`new_for_async()`](`PacketManager::new_for_async()`)
//!
//! 2. Register the [`Packets`](`Packet`) and [`PacketBuilders`](`PacketBuilder`) that the [`PacketManager`] will __receive__
//! and __send__ using [`register_receive_packet()`](`PacketManager::register_receive_packet()`) and [`register_send_packet()`](`PacketManager::register_send_packet()`).  
//! The ordering of `Packet` registration matters for the `receive` channel and
//! `send` channel each - the client and server must register the same packets in the same order,
//! for the opposite channels.  
//!     - In other words, the client must register `receive` packets in the
//! same order the server registers the same as `send` packets, and vice versa, the client must
//! register `send` packets in the same order the server registers the same as `receive` packets.
//! This helps to ensure the client and servers are in sync on what Packets to send/receive, almost
//! like ensuring they are on the same "version" so to speak, and is used to properly identify
//! Packets.
//!     - For your convenience, `durian_macros` comes bundled with 2 macros that simplify registration of send and
//! receive packets: [`register_receive!()`](https://docs.rs/durian_macros/latest/durian_macros/#declarative-macros), and [`register_send!()`](https://docs.rs/durian_macros/latest/durian_macros/#declarative-macros)
//!
//! 3. Initiate connection(s) with [`init_client()`](`PacketManager::init_client()`) (or the async variant [`async_init_client()`](`PacketManager::async_init_client()`)
//! if on the client side, else use [`init_server()`](`PacketManager::init_server()`) (or the async variant [`async_init_server)`](`PacketManager::async_init_server()`)
//! if on the server side.
//!
//! 4. Send packets using any of [`broadcast()`](`PacketManager::broadcast()`), [`send()`](`PacketManager::send()`), [`send_to()`](`PacketManager::send_to()`)
//! or the respective `async` variants if calling from an async context already.  Receive packets
//! using any of [`received_all()`](`PacketManager::received_all()`) , [`received()`](`PacketManager::received()`), or the respective
//! `async` variants.
//!
//! Putting these together:
//!
//! ```rust
//! use durian::{ClientConfig, PacketManager, bincode_packet, register_receive, register_send};
//!
//! #[bincode_packet]
//! struct Position { x: i32, y: i32 }
//! #[bincode_packet]
//! struct ServerAck;
//! #[bincode_packet]
//! struct ClientAck;
//! #[bincode_packet]
//! struct InputMovement { direction: String }
//!
//! fn packet_manager_example() {
//!     // Create PacketManager
//!     let mut manager = PacketManager::new();
//!
//!     // Register send and receive packets
//!     // Using macros
//!     let receive_results = register_receive!(
//!         manager,
//!         (Position, PositionPacketBuilder),
//!         (ServerAck, ServerAckPacketBuilder)
//!     );
//!     let send_results = register_send!(manager, ClientAck, InputMovement);
//!     // Validate registrations succeeded
//!     assert!(receive_results.iter().all(|r| r.is_ok()));  
//!     assert!(send_results.iter().all(|r| r.is_ok()));
//!
//!     // Or equivalently with manual registrations:
//!     // manager.register_receive_packet::<Position>(PositionPacketBuilder).unwrap();
//!     // manager.register_receive_packet::<ServerAck>(ServerAckPacketBuilder).unwrap();
//!     // manager.register_send_packet::<ClientAck>().unwrap();
//!     // manager.register_send_packet::<InputMovement>().unwrap();
//!
//!     // Initialize a client
//!     let client_config = ClientConfig::new("127.0.0.1:5001", "127.0.0.1:5000", 2, 2);
//!     manager.init_client(client_config).unwrap();
//!
//!     // Send and receive packets
//!     manager.broadcast(InputMovement { direction: "North".to_string() }).unwrap();
//!     manager.received_all::<Position, PositionPacketBuilder>(false).unwrap();
//!
//!     // The above PacketManager is for the client.  Server side is similar except the packets
//!     // are swapped between receive vs send channels:
//!
//!     // Create PacketManager
//!     let mut server_manager = PacketManager::new();
//!
//!     // Register send and receive packets
//!     let server_receive_results = register_receive!(
//!         server_manager,
//!         (ClientAck, ClientAckPacketBuilder),
//!         (InputMovement, InputMovementPacketBuilder)
//!     );
//!     let server_send_results = register_send!(server_manager, Position, ServerAck);
//!     // Validate registrations succeeded
//!     assert!(server_receive_results.iter().all(|r| r.is_ok()));  
//!     assert!(server_send_results.iter().all(|r| r.is_ok()));
//!
//!     // Initialize a client
//!     let client_config = ClientConfig::new("127.0.0.1:5001", "127.0.0.1:5000", 2, 2);
//!     server_manager.init_client(client_config).unwrap();
//!
//!     // Send and receive packets
//!     server_manager.broadcast(Position { x: 1, y: 3 }).unwrap();
//!     server_manager.received_all::<InputMovement, InputMovementPacketBuilder>(false).unwrap();
//! }
//! ```
//!
//! # durian_macros
//!
//! Rust docs don't properly link documentation of re-exported crates.  If you want to learn more about how the
//! [`bincode_packet`] and other macros used above work, see the [`durian_macros`] docs.
//!
//! # Examples
//!
//! For beginners, creating packets to be sent between clients/server should be extremely straight-forward
//! and the above examples should covers most of what you'd need.  For more complex scenarios, such as
//! serializing/deserializing packets in a custom way, can be done by implementing the various Traits
//! yourself, or through extra configurations in the [`PacketManager`](PacketManager).
//!
//! For a comprehensive minimal example, see the [`example crate`](https://github.com/spoorn/durian/tree/main/example).
//!
//! I also use this library myself for simple game development.  See the [`multisnakegame repo`](https://github.com/spoorn/multisnakegame).
//!
//!
//! # Debugging
//!
//! `durian` uses the `log` API with debug and trace logs.  Enable debug logging to see update logs
//! from `durian`, and enable trace logging to see packet byte transmissions.

mod error;
mod packet;
mod quinn_helpers;

pub use error::*;
pub use packet::*;

#[doc(inline)]
#[allow(unused_imports)]
pub use durian_macros;

#[allow(unused_imports)]
pub use durian_macros::*;

#[allow(unused_imports)]
pub use bytes;

#[allow(unused_imports)]
pub use serde;

#[allow(unused_imports)]
pub use bincode;

/// For [`PacketManager::get_source()`]
#[allow(unused_imports)]
pub use quinn::Endpoint;
