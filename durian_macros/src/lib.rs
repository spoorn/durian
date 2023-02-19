//! Macros for the [durian](https://docs.rs/durian/latest/durian/) crate
//! 
//! # Procedural Macros
//!
//! Procedural macros for easily annotating structs as [`Packets`](https://docs.rs/durian/latest/durian/trait.Packet.html) and automatically
//! implementing [`PacketBuilders`](https://docs.rs/durian/latest/durian/trait.PacketBuilder.html). The only
//! requirement is the struct must be de/serializable, meaning all nested fields also need to be
//! de/serializable.
//!
//! `#[bincode_packet]` will de/serialize your Packet using [`bincode`](https://docs.rs/bincode/latest/bincode/) and applies necessary derive
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
//! 
//! # Declarative Macros
//! 
//! Regular macros for easy and concise calls to the [`PacketManager`](https://docs.rs/durian/latest/durian/struct.PacketManager.html).
//! 
//! These include macros for registering all your `send` and `receive` packets:
//! 
//! - [`register_send!(packet_manager, <send packets>...)`](`register_send`)
//! - [`register_receive!(packet_manager, <receive packets>...)`](`register_receive`)
//! 
//! Where the `<send packets>` a sequence of your `send` packet types, or a slice of those types, and 
//! `<receive packets>` are a sequence of tuples containing (your `receive` packet type, the associated packet builder),
//! or a slice of those tuples.
//! 
//! ### Example:
//! 
//! ```rust
//! use durian::{bincode_packet, register_send, register_receive, PacketManager};
//! 
//! // Send packets
//! #[bincode_packet]
//! struct Position { x: i32, y: i32 }
//! #[bincode_packet]
//! struct Ack;
//! 
//! // Receive packets
//! #[bincode_packet]
//! struct UpdatePosition { x: i32, y: i32 }
//! #[bincode_packet]
//! struct NewMessage { message: String }
//! 
//! fn main() {
//!     let mut manager = PacketManager::new();
//!     let register_receive_results = register_receive!(
//!         manager, 
//!         (UpdatePosition, UpdatePositionPacketBuilder), 
//!         (NewMessage, NewMessagePacketBuilder)
//!     );
//!     // Or equivalently in a slice, 
//!     // register_receive_results!(manager, 
//!     //      [(UpdatePosition, UpdatePositionPacketBuilder), (NewMessage, NewMessagePacketBuilder)]
//!     // );`
//! 
//!     let register_send_results = register_send!(manager, Position, Ack);
//!     // Or equivalently in a slice, `register_send!(manager, [Position, Ack]);`
//! 
//!     // You can then validate that all the registrations were successful:
//!     assert!(register_receive_results.iter().all(|r| r.is_ok()));
//!     assert!(register_send_results.iter().all(|r| r.is_ok()));
//! 
//!     // The macros used above are equivalent to the following manual registration:
//!     //
//!     // manager.register_receive_packet::<UpdatePosition>(UpdatePositionPacketBuilder);
//!     // manager.register_receive_packet::<NewMessage>(NewMessagePacketBuilder);
//!     // manager.register_send_packet::<Position>();
//!     // manager.register_send_packet::<Ack>();
//! }
//! ```

#[allow(unused_imports)]
pub use durian_proc_macros::*;

#[macro_export]
macro_rules! register_send {
    ($manager:ident , [$($packet:ty),*]) => {
        {
            let mut results = std::vec::Vec::new();
            $(
                results.push($manager.register_send_packet::<$packet>());
            )*
            results
        }
    };
    ($manager:ident , $($packet:ty),*) => {
        {
            let mut results = std::vec::Vec::new();
            $(
                results.push($manager.register_send_packet::<$packet>());
            )*
            results
        }
    };
}

#[macro_export]
macro_rules! register_receive {
     ($manager:ident , [$(($packet:ty, $builder:expr)),*]) => {
        {
            let mut results = std::vec::Vec::new();
            $(
                results.push($manager.register_receive_packet::<$packet>($builder));
            )*
            results
        }
    };
    ($manager:ident , $(($packet:ty, $builder:expr)),*) => {
        {
            let mut results = std::vec::Vec::new();
            $(
                results.push($manager.register_receive_packet::<$packet>($builder));
            )*
            results
        }
    };
}