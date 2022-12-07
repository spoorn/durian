use std::error::Error;
use durian::{bincode_packet, Packet, PacketBuilder};
use durian::bytes::Bytes;

#[derive(Debug)]
struct Identifier {
    name: String
}

impl Packet for Identifier {
    fn as_bytes(&self) -> Bytes {
        Bytes::from(self.name.to_string())
    }
}

impl PacketBuilder<Identifier> for Identifier {
    fn read(&self, bytes: Bytes) -> Result<Identifier, Box<dyn Error>> {
        let name = std::str::from_utf8(&bytes)?.to_string();
        Ok(Identifier { name })
    }
}

#[derive(Debug)]
#[bincode_packet]
struct Position {
    x: i32,
    y: i32
}

#[bincode_packet]
struct Ack;

fn main() {
    println!("Hello, world!");
}
