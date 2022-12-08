# durian_macros
Macros for the `durian` crate

These should not be used alone!  The macros depend on Traits and paths defined in `durian`

Contains a few macros that help autogenerate Impl blocks for a struct for both `Packet` and `PacketBuilder`.  The only
requirement is the struct must be de/serializable, meaning all nested fields also need to be
de/serializable.

`#[bincode_packet]` will de/serialize your Packet using [`bincode`] and applies necessary derive
macros automatically for you.

```rust
use durian::bincode_packet;

// Automatically implements Packet, and generates a PositionPacketBuilder that implements 
// PacketBuilder.  You can also add other macros such as derive macros so long s they don't
// conflict with what #[bincode_packet] adds (See bincode_packet documentation).
#[bincode_packet]
#[derive(Debug)]
struct Position {
    x: i32,
    y: i32
}

// Works for Unit (empty) structs as well
#[bincode_packet]
struct Ack;
```

You can also use the derive macros (`BinPacket` and `UnitPacket`) manually:

```rust
use durian::serde::{Deserialize, Serialize};
use durian::{BinPacket, UnitPacket};

#[derive(Serialize, Deserialize, BinPacket)]
#[serde(crate = "durian::serde")]
struct Position { x: i32, y: i32 }

#[derive(UnitPacket)]
struct Ack;
```