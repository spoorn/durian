use durian::bytes::Bytes;
use durian::serde::{Deserialize, Serialize};
use durian::{bincode_packet, BinPacket, ClientConfig, Packet, PacketBuilder, PacketManager, ServerConfig, UnitPacket};
use std::error::Error;

#[bincode_packet]
#[derive(Debug)]
struct Position {
    x: i32,
    y: i32,
}

#[bincode_packet]
struct ClientAck;

// Using BinPacket and UnitPacket

#[derive(Debug, Serialize, Deserialize, BinPacket)]
#[serde(crate = "durian::serde")]
struct OtherPosition {
    x: i32,
    y: i32,
}

#[derive(Debug, UnitPacket)]
struct ServerAck;

// Manually implementing Packet and PacketBuilder

#[derive(Debug)]
struct Identifier {
    name: String,
}

impl Packet for Identifier {
    fn as_bytes(&self) -> Bytes {
        Bytes::from(self.name.to_string())
    }
}

struct IdentifierPacketBuilder;
impl PacketBuilder<Identifier> for IdentifierPacketBuilder {
    fn read(&self, bytes: Bytes) -> Result<Identifier, Box<dyn Error>> {
        let name = std::str::from_utf8(&bytes)?.to_string();
        Ok(Identifier { name })
    }
}

pub fn setup(num_clients: u32, start_port: u32) -> (Vec<PacketManager>, PacketManager) {
    let server_addr = "127.0.0.1:5000";

    // Server example
    let mut server_manager = PacketManager::new();
    // Register `receive` and `send` packets
    server_manager.register_receive_packet::<Position>(PositionPacketBuilder).unwrap();
    server_manager.register_receive_packet::<ClientAck>(ClientAckPacketBuilder).unwrap();
    server_manager.register_receive_packet::<Identifier>(IdentifierPacketBuilder).unwrap();
    server_manager.register_send_packet::<OtherPosition>().unwrap();
    server_manager.register_send_packet::<ServerAck>().unwrap();
    // init_connections() takes in number of incoming/outgoing streams to spin up for the
    // connection, and validates against the number of registered packets.
    // If this is the server-side, you can configure whether it blocks on waiting for a number of
    // clients, as well as the total number of expected clients (or None if server can accept any
    // number of clients).  A thread will be spun up to wait for extra clients beyond the number
    // to block on.
    let server_config = ServerConfig::new(server_addr, 0, Some(num_clients), 3, 2);
    server_manager.init_server(server_config).unwrap();

    let mut client_managers = Vec::new();

    for i in 0..num_clients {
        let client_addr = format!("127.0.0.1:{}", start_port + i);

        // Client example
        let mut client_manager = PacketManager::new();
        // Register `receive` and `send` packets.
        // Note: these must be in the same order for opposite channels as the server.
        client_manager.register_receive_packet::<OtherPosition>(OtherPositionPacketBuilder).unwrap();
        client_manager.register_receive_packet::<ServerAck>(ServerAckPacketBuilder).unwrap();
        client_manager.register_send_packet::<Position>().unwrap();
        client_manager.register_send_packet::<ClientAck>().unwrap();
        client_manager.register_send_packet::<Identifier>().unwrap();
        // init_connections() takes in number of incoming/outgoing streams to spin up for the
        // connection, and validates against the number of registered packets.
        // If this is the client-side, this is a blocking call that waits until the connection is
        // established.
        let client_config = ClientConfig::new(client_addr.as_str(), server_addr, 2, 3);
        client_manager.init_client(client_config).unwrap();
        client_managers.push(client_manager);
    }

    (client_managers, server_manager)
}

pub fn sync_example_multiclient_server(client_managers: &mut [PacketManager], server_manager: &mut PacketManager) {
    let server_addr = "127.0.0.1:5000";

    // broadcast packets to all recipients, and receive all packets from sender
    server_manager.broadcast(OtherPosition { x: 0, y: 1 }).unwrap();
    // Or you can send to a specific recipient via the address
    server_manager.broadcast(ServerAck).unwrap();
    // received() variants can be a blocking call based on the boolean flag passed in.
    // WARNING: be careful with blocking calls in your actual application, as it can cause your app
    // to freeze if packets aren't sent exactly in the order you expect!

    for client_manager in client_managers.iter_mut() {
        let _other_position_packets = loop {
            // received_all() returns a vector of packets received from each sender address
            // The boolean flag is to set whether it's a blocking call or not
            let mut queue = client_manager.received_all::<OtherPosition, OtherPositionPacketBuilder>(false).unwrap();
            // In this case, there's only one sender: the server
            let queue_packets = queue.pop().unwrap();
            if queue_packets.0 == server_addr {
                if let Some(packets) = queue_packets.1 {
                    break packets;
                }
            }
        };
        let _server_ack_packets = loop {
            let mut queue = client_manager.received_all::<ServerAck, ServerAckPacketBuilder>(false).unwrap();
            let queue_packets = queue.pop().unwrap();
            if queue_packets.0 == server_addr {
                if let Some(packets) = queue_packets.1 {
                    break packets;
                }
            }
        };

        client_manager.send(Position { x: 5, y: 6 }).unwrap();
        client_manager.send(ClientAck).unwrap();
    }

    // wait for all packets from clients
    server_manager.received_all::<Position, PositionPacketBuilder>(true).unwrap();
    server_manager.received_all::<ClientAck, ClientAckPacketBuilder>(true).unwrap();
}

pub fn sync_example_single_client_server(client_manager: &mut PacketManager, server_manager: &mut PacketManager) {
    let client_addr = "127.0.0.1:5001";
    let server_addr = "127.0.0.1:5000";

    // Below we show different ways to send/receive packets

    // broadcast packets to all recipients, and receive all packets from sender
    server_manager.broadcast(OtherPosition { x: 0, y: 1 }).unwrap();
    // Or you can send to a specific recipient via the address
    server_manager.send_to(client_addr, ServerAck).unwrap();
    // received() variants can be a blocking call based on the boolean flag passed in.
    // WARNING: be careful with blocking calls in your actual application, as it can cause your app
    // to freeze if packets aren't sent exactly in the order you expect!
    let _other_position_packets = loop {
        // received_all() returns a vector of packets received from each sender address
        // The boolean flag is to set whether it's a blocking call or not
        let mut queue = client_manager.received_all::<OtherPosition, OtherPositionPacketBuilder>(false).unwrap();
        // In this case, there's only one sender: the server
        let queue_packets = queue.pop().unwrap();
        if queue_packets.0 == server_addr {
            if let Some(packets) = queue_packets.1 {
                break packets;
            }
        }
    };
    //println!("{:?}", other_position_packets);
    let _server_ack_packets = loop {
        let mut queue = client_manager.received_all::<ServerAck, ServerAckPacketBuilder>(false).unwrap();
        let queue_packets = queue.pop().unwrap();
        if queue_packets.0 == server_addr {
            if let Some(packets) = queue_packets.1 {
                break packets;
            }
        }
    };
    //println!("{:?}", server_ack_packets);

    // Single client-server relationship when you know there is only 1 sender and 1 recipient

    // Send packets using send() and received(), which should only be used if there is only a single
    // recipient and transmitter
    server_manager.send(OtherPosition { x: 0, y: 1 }).unwrap();
    server_manager.send(ServerAck).unwrap();
    client_manager.received::<OtherPosition, OtherPositionPacketBuilder>(true).unwrap();
    client_manager.received::<ServerAck, ServerAckPacketBuilder>(true).unwrap();

    // println!("{:?}", client_manager.received::<OtherPosition, OtherPositionPacketBuilder>(true).unwrap());
    // println!("{:?}", client_manager.received::<ServerAck, ServerAckPacketBuilder>(true).unwrap());
}

pub async fn async_sync_example() {
    let client_addr = "127.0.0.1:5001";
    let server_addr = "127.0.0.1:5000";

    // Server example
    let mut server_manager = PacketManager::new_for_async();
    // Register `receive` and `send` packets
    server_manager.register_receive_packet::<Position>(PositionPacketBuilder).unwrap();
    server_manager.register_receive_packet::<ClientAck>(ClientAckPacketBuilder).unwrap();
    server_manager.register_receive_packet::<Identifier>(IdentifierPacketBuilder).unwrap();
    server_manager.register_send_packet::<OtherPosition>().unwrap();
    server_manager.register_send_packet::<ServerAck>().unwrap();
    // init_connections() takes in number of incoming/outgoing streams to spin up for the
    // connection, and validates against the number of registered packets.
    // If this is the server-side, you can configure whether it blocks on waiting for a number of
    // clients, as well as the total number of expected clients (or None if server can accept any
    // number of clients).  A thread will be spun up to wait for extra clients beyond the number
    // to block on.
    let server_config = ServerConfig::new(server_addr, 0, Some(1), 3, 2);
    server_manager.async_init_server(server_config).await.unwrap();

    // Client example
    let mut client_manager = PacketManager::new_for_async();
    // Register `receive` and `send` packets.
    // Note: these must be in the same order for opposite channels as the server.
    client_manager.register_receive_packet::<OtherPosition>(OtherPositionPacketBuilder).unwrap();
    client_manager.register_receive_packet::<ServerAck>(ServerAckPacketBuilder).unwrap();
    client_manager.register_send_packet::<Position>().unwrap();
    client_manager.register_send_packet::<ClientAck>().unwrap();
    client_manager.register_send_packet::<Identifier>().unwrap();
    // init_connections() takes in number of incoming/outgoing streams to spin up for the
    // connection, and validates against the number of registered packets.
    // If this is the client-side, this is a blocking call that waits until the connection is
    // established.
    let client_config = ClientConfig::new(client_addr, server_addr, 2, 3);
    client_manager.async_init_client(client_config).await.unwrap();

    // Below we show different ways to send/receive packets

    // broadcast packets to all recipients, and receive all packets from sender
    server_manager.async_broadcast(OtherPosition { x: 0, y: 1 }).await.unwrap();
    // Or you can send to a specific recipient via the address
    server_manager.async_send_to(client_addr, ServerAck).await.unwrap();
    // received() variants can be a blocking call based on the boolean flag passed in.
    // WARNING: be careful with blocking calls in your actual application, as it can cause your app
    // to freeze if packets aren't sent exactly in the order you expect!
    let _other_position_packets = loop {
        // received_all() returns a vector of packets received from each sender address
        // The boolean flag is to set whether it's a blocking call or not
        let mut queue =
            client_manager.async_received_all::<OtherPosition, OtherPositionPacketBuilder>(false).await.unwrap();
        // In this case, there's only one sender: the server
        let queue_packets = queue.pop().unwrap();
        if queue_packets.0 == server_addr {
            if let Some(packets) = queue_packets.1 {
                break packets;
            }
        }
    };
    //println!("{:?}", other_position_packets);
    let _server_ack_packets = loop {
        let mut queue = client_manager.async_received_all::<ServerAck, ServerAckPacketBuilder>(false).await.unwrap();
        let queue_packets = queue.pop().unwrap();
        if queue_packets.0 == server_addr {
            if let Some(packets) = queue_packets.1 {
                break packets;
            }
        }
    };
    //println!("{:?}", server_ack_packets);

    // Single client-server relationship when you know there is only 1 sender and 1 recipient

    // Send packets using send() and received(), which should only be used if there is only a single
    // recipient and transmitter
    server_manager.async_send(OtherPosition { x: 0, y: 1 }).await.unwrap();
    server_manager.async_send(ServerAck).await.unwrap();

    //println!("{:?}", client_manager.async_received::<OtherPosition, OtherPositionPacketBuilder>(true).await.unwrap());
    //println!("{:?}", client_manager.async_received::<ServerAck, ServerAckPacketBuilder>(true).await.unwrap());
}
