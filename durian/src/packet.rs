use std::any::{Any, type_name, TypeId};
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;

use bimap::BiMap;
use bytes::Bytes;
use derive_more::Display;
use durian_macros::ErrorOnlyMessage;
use hashbrown::HashMap;
use log::{debug, error, trace};
use quinn::{Connection, RecvStream, SendStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::quinn_helpers::{make_client_endpoint, make_server_endpoint};

const FRAME_BOUNDARY: &[u8] = b"AAAAAA031320050421";

/// Packet trait that allows a struct to be sent through [`PacketManager`], for serializing
/// 
/// This is automatically implemented if using the macros [`bincode_packet`](`durian_macros::bincode_packet`), 
/// [`BinPacket`](`durian_macros::BinPacket`), or [`UnitPacket`](`durian_macros::UnitPacket`).
pub trait Packet {
    
    /// Return a serialized form of the Packet as [`Bytes`]
    fn as_bytes(&self) -> Bytes;
    
    // https://stackoverflow.com/questions/33687447/how-to-get-a-reference-to-a-concrete-type-from-a-trait-object
    // fn as_any(self: Self) -> Box<dyn Any>;
}

/// PacketBuilder is the deserializer for [`Packet`] and used when [`PacketManager`] receives bytes
///
/// This is automatically implemented if using the macros [`bincode_packet`](`durian_macros::bincode_packet`), 
/// [`BinPacket`](`durian_macros::BinPacket`), or [`UnitPacket`](`durian_macros::UnitPacket`).
pub trait PacketBuilder<T: Packet> {
    
    /// Deserializes [`Bytes`] into the [`Packet`] this PacketBuilder is implemented for
    /// 
    /// # Error
    /// Returns an error if deserializing fails
    fn read(&self, bytes: Bytes) -> Result<T, Box<dyn Error>>;
}

/// Error when calling [`PacketManager::init_connections()`] or [`PacketManager::async_init_connections()`]
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct ConnectionError {
    /// Error message
    pub message: String
}

/// Error when calling [`PacketManager::register_receive_packet()`], [`PacketManager::received_all()`],
/// [`PacketManager::async_received_all()`], [`PacketManager::received()`], [`PacketManager::async_received()`]
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct ReceiveError {
    /// Error message
    pub message: String
}

/// Error when calling [`PacketManager::register_send_packet()`], [`PacketManager::broadcast()`],
/// [`PacketManager::async_broadcast()`], [`PacketManager::send()`], [`PacketManager::async_send()`],
/// [`PacketManager::send_to()`], [`PacketManager::async_send_to()`]
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct SendError {
    /// Error message
    pub message: String
}

/// The core of `durian` that is the central struct containing all the necessary APIs for initiating and managing
/// connections, creating streams, sending [`Packets`](`Packet`), receiving, broadcasting, etc.
/// 
/// A `PacketManager` would be created on each client to connect to a
/// single server, and one created on the server to connect to multiple clients. It contains both 
/// synchronous and asynchronous APIs, so you can call the functions both from a synchronous 
/// context, or within an async runtime (_Note: the synchronous path will create a separate 
/// isolated async runtime context per `PacketManager` instance._)
///
/// There are 4 basic steps to using the `PacketManager`, which would be done on both the client
/// and server side:
///
/// 1. Create a `PacketManager` via [`new()`](`PacketManager::new()`) or, if calling from an async context, [`new_for_async()`](`PacketManager::new_for_async()`)
///
/// 2. Register the [`Packets`](`Packet`) and [`PacketBuilders`](`PacketBuilder`) that the `PacketManager` will __receive__
/// and __send__ using [`register_receive_packet()`](`PacketManager::register_receive_packet()`) and [`register_send_packet()`](`PacketManager::register_send_packet()`).  
/// The ordering of `Packet` registration matters for the `receive` channel and
/// `send` channel each - the client and server must register the same packets in the same order,
/// for the opposite channels.  
///     - In other words, the client must register `receive` packets in the 
/// same order the server registers the same as `send` packets, and vice versa, the client must
/// register `send` packets in the same order the server registers the same as `receive` packets.
/// This helps to ensure the client and servers are in sync on what Packets to send/receive, almost
/// like ensuring they are on the same "version" so to speak, and is used to properly identify
/// Packets.
///
/// 3. Initiate a connection with [`init_connections()`](`PacketManager::init_connections()`) or the async variant
/// [`async_init_connections()`](`PacketManager::async_init_connections()`)
///
/// 4. Send packets using any of [`broadcast()`](`PacketManager::broadcast()`), [`send()`](`PacketManager::send()`), [`send_to()`](`PacketManager::send_to()`)
/// or the respective `async` variants if calling from an async context already.  Receive packets
/// using any of [`received_all()`](`PacketManager::received_all()`) , [`received()`](`PacketManager::received()`), or the respective
/// `async` variants.
///
/// # Example
///
/// ```rust
/// use durian::PacketManager;
/// use durian_macros::bincode_packet;
///
/// #[bincode_packet]
/// struct Position { x: i32, y: i32 }
/// #[bincode_packet]
/// struct ServerAck;
/// #[bincode_packet]
/// struct ClientAck;
/// #[bincode_packet]
/// struct InputMovement { direction: String }
///
/// fn packet_manager_example() {
///     // Create PacketManager
///     let mut manager = PacketManager::new();
///
///     // Register send and receive packets
///     manager.register_receive_packet::<Position>(PositionPacketBuilder).unwrap();
///     manager.register_receive_packet::<ServerAck>(ServerAckPacketBuilder).unwrap();
///     manager.register_send_packet::<ClientAck>().unwrap();
///     manager.register_send_packet::<InputMovement>().unwrap();
///
///     // Initialize connection to an address
///     manager.init_connections(true, 2, 2, "127.0.0.1:5000", Some("127.0.0.1:5001"), 0, None).unwrap();
///
///     // Send and receive packets
///     manager.broadcast(InputMovement { direction: "North".to_string() }).unwrap();
///     manager.received_all::<Position, PositionPacketBuilder>(false).unwrap();
///
///     // The above PacketManager is for the client.  Server side is similar except the packets
///     // are swapped between receive vs send channels.
/// }
#[derive(Debug)]
pub struct PacketManager {
    receive_packets: BiMap<u32, TypeId>,
    send_packets: BiMap<u32, TypeId>,
    recv_packet_builders: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    send_streams: Vec<(String, HashMap<u32, RwLock<SendStream>>)>,
    rx: Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>,
    new_send_streams: Arc<RwLock<Vec<(String, HashMap<u32, RwLock<SendStream>>)>>>,
    new_rxs: Arc<RwLock<Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
    // Endpoint and Connection structs moved to the struct fields to prevent closing connections
    // by dropping.
    // Client addr to (index in above Vecs AKA a client ID, Connection)
    client_connections: Arc<RwLock<HashMap<String, (u32, Connection)>>>,
    server_connection: Option<Connection>,
    source_addr: String,
    next_receive_id: u32,
    next_send_id: u32,
    // We construct a single Tokio Runtime to be used by each PacketManger instance, so that
    // methods can be synchronous.  There is also an async version of each API if the user wants
    // to use their own runtime.
    runtime: Arc<Option<Runtime>>
}

impl PacketManager {
    
    /// Create a new `PacketManager`
    /// 
    /// If calling from an asynchronous context/runtime, use the [`new_for_async()`](`PacketManager::new_for_async()`) variant.
    /// This constructs a [`tokio Runtime`](`Runtime`) for the `PacketManager` instance.
    /// 
    /// # Panic
    /// If the [`Runtime`] could not be created.  Usually happens if you call `new()` from an existing async runtime.
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build();
        match runtime {
            Ok(runtime) => {
                PacketManager {
                    receive_packets: BiMap::new(),
                    send_packets: BiMap::new(),
                    recv_packet_builders: HashMap::new(),
                    send_streams: Vec::new(),
                    rx: Vec::new(),
                    new_send_streams: Arc::new(RwLock::new(vec![])),
                    new_rxs: Arc::new(RwLock::new(Vec::new())),
                    client_connections: Arc::new(RwLock::new(HashMap::new())),
                    server_connection: None,
                    source_addr: "".to_string(),
                    next_receive_id: 0,
                    next_send_id: 0,
                    runtime: Arc::new(Some(runtime))
                }
            }
            Err(e) => {
                panic!("Could not create a Tokio runtime for PacketManager.  If you are calling new() from code that already has an async runtime available, use PacketManager.new_async(), and respective async_*() versions of APIs.  --  {}", e);
            }
        }
    }

    /// Create a new `PacketManager`
    ///
    /// If calling from a synchronous context, use [`new()`](`PacketManager::new()`)
    pub fn new_for_async() -> Self {
        PacketManager {
            receive_packets: BiMap::new(),
            send_packets: BiMap::new(),
            recv_packet_builders: HashMap::new(),
            send_streams: Vec::new(),
            rx: Vec::new(),
            new_send_streams: Arc::new(RwLock::new(vec![])),
            new_rxs: Arc::new(Default::default()),
            client_connections: Arc::new(RwLock::new(HashMap::new())),
            server_connection: None,
            source_addr: "".to_string(),
            next_receive_id: 0,
            next_send_id: 0,
            runtime: Arc::new(None)
        }
    }

    pub fn init_connections<S: Into<String>>(&mut self, is_server: bool, num_incoming_streams: u32, num_outgoing_streams: u32, server_addr: S, client_addr: Option<S>, wait_for_clients: u32, expected_num_clients: Option<u32>) -> Result<(), Box<dyn Error>> {
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_init_connection()?");
            }
            Some(runtime) => {
                self.validate_connection_prereqs(is_server, &client_addr, num_incoming_streams, num_outgoing_streams)?;
                let server_addr = server_addr.into();
                let mut client_addr = if let Some(s) = client_addr {
                    Some(s.into())
                } else {
                    None
                };
                if is_server {
                    self.source_addr = server_addr.clone();
                } else {
                    self.source_addr = client_addr.unwrap();
                    client_addr = Some(self.source_addr.clone());
                }
                runtime.block_on(PacketManager::async_init_connections_helper(is_server, num_incoming_streams, num_outgoing_streams, server_addr, client_addr,
                                                                                        wait_for_clients, expected_num_clients, &self.runtime, &self.new_send_streams, &self.new_rxs,
                                                                                        &self.client_connections, &mut self.server_connection))
            }
        }
    }

    pub async fn async_init_connections<S: Into<String>>(&mut self, is_server: bool, num_incoming_streams: u32, num_outgoing_streams: u32, server_addr: S, client_addr: Option<S>, wait_for_clients: u32, expected_num_clients: Option<u32>) -> Result<(), Box<dyn Error>> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_init_connections(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.validate_connection_prereqs(is_server, &client_addr, num_incoming_streams, num_outgoing_streams)?;
        let server_addr = server_addr.into();
        let mut client_addr = if let Some(s) = client_addr {
            Some(s.into())
        } else {
            None
        };
        if is_server {
            self.source_addr = server_addr.clone();
        } else {
            self.source_addr = client_addr.unwrap();
            client_addr = Some(self.source_addr.clone());
        }
        PacketManager::async_init_connections_helper(is_server, num_incoming_streams, num_outgoing_streams, server_addr, client_addr, 
                                                     wait_for_clients, expected_num_clients, &self.runtime, &self.new_send_streams, &self.new_rxs, 
                                                     &self.client_connections, &mut self.server_connection).await
    }
    
    fn validate_connection_prereqs<S: Into<String>>(&self, is_server: bool, client_addr: &Option<S>, num_incoming_streams: u32, num_outgoing_streams: u32) -> Result<(), ConnectionError> {
        if !is_server && client_addr.is_none() {
            return Err(ConnectionError::new("PacketManager is for client-side, but client_addr is None"));
        }
        let num_receive_packets = self.receive_packets.len() as u32;
        if num_receive_packets != num_incoming_streams {
            return Err(ConnectionError::new(format!("num_incoming_streams={} does not match number of registered receive packets={}.  Did you forget to call register_receive_packet()?", num_incoming_streams, num_receive_packets)));
        }
        let num_send_packets = self.send_packets.len() as u32;
        if num_send_packets != num_outgoing_streams {
            return Err(ConnectionError::new(format!("num_outgoing_streams={} does not match number of registered send packets={}.  Did you forget to call register_send_packet()?", num_incoming_streams, num_send_packets)));
        }
        Ok(())
    }

    // TODO: validate number of streams against registered packets 
    async fn async_init_connections_helper<S: Into<String>>(is_server: bool, num_incoming_streams: u32, num_outgoing_streams: u32, 
                                                            server_addr: S, mut client_addr: Option<S>, wait_for_clients: u32, 
                                                            expected_num_clients: Option<u32>, runtime: &Arc<Option<Runtime>>,
                                                            new_send_streams: &Arc<RwLock<Vec<(String, HashMap<u32, RwLock<SendStream>>)>>>,
                                                            new_rxs: &Arc<RwLock<Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
                                                            client_connections: &Arc<RwLock<HashMap<String, (u32, Connection)>>>,
                                                            server_connection: &mut Option<Connection>) -> Result<(), Box<dyn Error>> {
        // if expected_num_accepts_uni != self.next_receive_id {
        //     return Err(Box::new(ConnectionError::new("expected_num_accepts_uni does not match number of registered receive packets")));
        // }
        
        let client_addr = match client_addr.take() {
            None => { "None".to_string() }
            Some(s) => { s.into() }
        };
        let server_addr = server_addr.into();
        debug!("Initiating connection with is_server={}, num_incoming_streams={}, num_outgoing_streams={}, server_addr={}, client_addr={}", is_server, num_incoming_streams, num_outgoing_streams, server_addr, client_addr);
        // TODO: assert num streams equals registered
        let server_addr = server_addr.parse().unwrap();
        
        if is_server {
            let (endpoint, server_cert) = make_server_endpoint(server_addr)?;

            // TODO: use synchronous blocks during read and write of client_connections
            // Single connection
            for i in 0..wait_for_clients {
                let incoming_conn = endpoint.accept().await.unwrap();
                let conn = incoming_conn.await.unwrap();
                let addr = conn.remote_address();
                if client_connections.read().await.contains_key(&addr.to_string()) {
                    panic!("Client with addr={} was already connected", addr);
                }
                debug!("[server] connection accepted: addr={}", conn.remote_address());
                let (server_send_streams, recv_streams) = PacketManager::open_streams_for_connection(addr.to_string(), &conn, num_incoming_streams, num_outgoing_streams).await?;
                let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, runtime.as_ref()).unwrap();
                new_rxs.write().await.push((addr.to_string(), res));
                new_send_streams.write().await.push((addr.to_string(), server_send_streams));
                client_connections.write().await.insert(addr.to_string(), (i, conn));
            }
            
            if expected_num_clients.is_none() || expected_num_clients.unwrap() > wait_for_clients {
                let client_connections = client_connections.clone();
                let arc_send_streams = new_send_streams.clone();
                let arc_rx = new_rxs.clone();
                let arc_runtime = Arc::clone(runtime);
                let mut client_id = wait_for_clients;
                let accept_client_task = async move {
                    match expected_num_clients {
                        None => {
                            loop {
                                debug!("[server] Waiting for more clients...");
                                let incoming_conn = endpoint.accept().await.unwrap();
                                let conn = incoming_conn.await.unwrap();
                                let addr = conn.remote_address();
                                if client_connections.read().await.contains_key(&addr.to_string()) {
                                    panic!("Client with addr={} was already connected", addr);
                                }
                                debug!("[server] connection accepted: addr={}", conn.remote_address());
                                let (send_streams, recv_streams) = PacketManager::open_streams_for_connection(addr.to_string(), &conn, num_incoming_streams, num_outgoing_streams).await.unwrap();
                                let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, arc_runtime.as_ref()).unwrap();
                                arc_rx.write().await.push((addr.to_string(), res));
                                arc_send_streams.write().await.push((addr.to_string(), send_streams));
                                client_connections.write().await.insert(addr.to_string(), (client_id, conn));
                                client_id += 1;
                            }
                        }
                        Some(expected_num_clients) => {
                            for i in 0..(expected_num_clients - wait_for_clients) {
                                debug!("[server] Waiting for client #{}", i + wait_for_clients);
                                let incoming_conn = endpoint.accept().await.unwrap();
                                let conn = incoming_conn.await.unwrap();
                                let addr = conn.remote_address();
                                if client_connections.read().await.contains_key(&addr.to_string()) {
                                    panic!("Client with addr={} was already connected", addr);
                                }
                                debug!("[server] connection accepted: addr={}", conn.remote_address());
                                let (send_streams, recv_streams) = PacketManager::open_streams_for_connection(addr.to_string(), &conn, num_incoming_streams, num_outgoing_streams).await.unwrap();
                                let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, arc_runtime.as_ref()).unwrap();
                                arc_rx.write().await.push((addr.to_string(), res));
                                arc_send_streams.write().await.push((addr.to_string(), send_streams));
                                client_connections.write().await.insert(addr.to_string(), (client_id, conn));
                                client_id += 1;
                            }
                        }
                    }
                    Ok::<(), Box<ConnectionError>>(())
                };

                // TODO: save this value
                let accept_client_thread = match runtime.as_ref() {
                    None => {
                        tokio::spawn(accept_client_task)
                    }
                    Some(runtime) => {
                        runtime.spawn(accept_client_task)
                    }
                };
            }
        } else {
            // Bind this endpoint to a UDP socket on the given client address.
            let endpoint = make_client_endpoint(client_addr.parse().unwrap(), &[])?;

            // Connect to the server passing in the server name which is supposed to be in the server certificate.
            let conn = endpoint.connect(server_addr, "hostname")?.await?;
            let addr = conn.remote_address();
            debug!("[client] connected: addr={}", addr);
            let (client_send_streams, recv_streams) = PacketManager::open_streams_for_connection(addr.to_string(), &conn, num_incoming_streams, num_outgoing_streams).await?;
            let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, runtime.as_ref()).unwrap();
            new_rxs.write().await.push((addr.to_string(), res));
            new_send_streams.write().await.push((addr.to_string(), client_send_streams));
            let _ = server_connection.insert(conn);
        }
            
        Ok(())
    }
    
    async fn open_streams_for_connection(addr: String, conn: &Connection, num_incoming_streams: u32, num_outgoing_streams: u32) -> Result<(HashMap<u32, RwLock<SendStream>>, HashMap<u32, RecvStream>), Box<dyn Error>> {
        let mut send_streams = HashMap::new();
        let mut recv_streams = HashMap::new();
        // Note: Packets are not sent immediately upon the write.  The thread needs to be kept
        // open so that the packets can actually be sent over the wire to the client.
        for i in 0..num_outgoing_streams {
            trace!("Opening outgoing stream for addr={} packet id={}", addr, i);
            let mut send_stream = conn
                .open_uni()
                .await?;
            trace!("Writing packet to {} for packet id {}", addr, i);
            send_stream.write_u32(i).await?;
            send_streams.insert(i, RwLock::new(send_stream));
        }

        for i in 0..num_incoming_streams {
            trace!("Accepting incoming stream from {} for packet id {}", addr, i);
            let mut recv = conn.accept_uni().await?;
            trace!("Validating incoming packet from {} id {}", addr, i);
            let id = recv.read_u32().await?;
            trace!("Received incoming packet from {} with packet id {}", addr, id);
            // if id >= self.next_receive_id {
            //     return Err(Box::new(ConnectionError::new(format!("Received unexpected packet ID {} from server", id))));
            // }

            recv_streams.insert(i, recv);
        }

        Ok((send_streams, recv_streams))
    }
    
    fn spawn_receive_thread<S: Into<String>>(addr: S, recv_streams: HashMap<u32, RecvStream>, runtime: &Option<Runtime>) -> Result<HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>, Box<dyn Error>> {
        let mut rxs = HashMap::new();
        trace!("Spawning receive thread for addr={} for {} ids", addr.into(), recv_streams.len());
        for (id, mut recv_stream) in recv_streams.into_iter() {
            let (tx, rx) = mpsc::channel(100);

            let task = async move {
                let mut partial_chunk: Option<Bytes> = None;
                loop {
                    // TODO: relay error message
                    // TODO: configurable size limit
                    let chunk = recv_stream.read_chunk(usize::MAX, true).await.unwrap();
                    match chunk {
                        None => {
                            // TODO: Error
                            trace!("Receive stream closed, got None when reading chunks");
                            break;
                        }
                        Some(chunk) => {
                            trace!("Received chunked packets for id={}, length={}", id, chunk.bytes.len());
                            let bytes;
                            match partial_chunk.take() {
                                None => {
                                    bytes = chunk.bytes;
                                }
                                Some(part) => {
                                    bytes = Bytes::from([part, chunk.bytes].concat());
                                    trace!("Concatenated saved part and chunked packet: {:?}", bytes);
                                }
                            }

                            // TODO: Make trace log
                            trace!("Received bytes: {:?}", bytes);
                            let boundaries: Vec<usize> = bytes.windows(FRAME_BOUNDARY.len()).enumerate().filter(|(_, w)| matches!(*w, FRAME_BOUNDARY)).map(|(i, _)| i).collect();
                            let mut offset = 0;
                            for i in boundaries.iter() {
                                // Reached end of bytes
                                if offset >= bytes.len() {
                                    break;
                                }
                                let frame = bytes.slice(offset..*i);
                                match partial_chunk.take() {
                                    None => {
                                        if matches!(frame.as_ref(), FRAME_BOUNDARY) {
                                            error!("Found a dangling FRAME_BOUNDARY in packet frame.  This most likely is a bug in durian")
                                        } else {
                                            trace!("Transmitting received bytes of length {}", frame.len());
                                            tx.send(frame).await.unwrap();
                                        }
                                    },
                                    Some(part) => {
                                        let reconstructed_frame = Bytes::from([part, frame].concat());
                                        if matches!(reconstructed_frame.as_ref(), FRAME_BOUNDARY) {
                                            error!("Found a dangling FRAME_BOUNDARY in packet frame.  This most likely is a bug in durian")
                                        } else {
                                            trace!("Transmitting reconstructed received bytes of length {}", reconstructed_frame.len());
                                            tx.send(reconstructed_frame).await.unwrap();
                                        }
                                    }
                                }
                                offset = i + FRAME_BOUNDARY.len();
                            }

                            if boundaries.is_empty() || (offset + FRAME_BOUNDARY.len() != bytes.len() - 1) {
                                let prefix_part = bytes.slice(offset..bytes.len());
                                match partial_chunk.take() {
                                    None => {
                                        partial_chunk = Some(prefix_part);
                                    }
                                    Some(part) => {
                                        partial_chunk = Some(Bytes::from([part, prefix_part].concat()))
                                    }
                                }
                            }
                        }
                    }
                }
            };

            let receive_thread: JoinHandle<()> = match runtime.as_ref() {
                None => {
                    tokio::spawn(task)
                }
                Some(runtime) => {
                    runtime.spawn(task)
                }
            };

            rxs.insert(id, (RwLock::new(rx), receive_thread));
        }
        
        Ok(rxs)
    }
    
    /// Returns the source address of this PacketManager as a String
    /// 
    /// # Returns
    /// Source address is the server address if this PacketManager is for a server, else the client address.
    pub fn get_source_addr(&self) -> String {
        self.source_addr.to_string()
    }
    
    /// Register a [`Packet`] on a `receive` stream/channel, and its associated [`PacketBuilder`]
    /// 
    /// # Error
    /// Returns a [`ReceiveError`] if registration failed.
    pub fn register_receive_packet<T: Packet + 'static>(&mut self, packet_builder: impl PacketBuilder<T> + 'static + Sync + Send) -> Result<(), ReceiveError> {
        if self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!("Type '{}' was already registered as a Receive packet", type_name::<T>())))
        }
        
        let packet_type_id = TypeId::of::<T>();
        self.receive_packets.insert(self.next_receive_id, packet_type_id);
        self.recv_packet_builders.insert(packet_type_id, Box::new(packet_builder));
        debug!("Registered Receive packet with id={}, type={}", self.next_receive_id, type_name::<T>());
        self.next_receive_id += 1;
        Ok(())
    }

    /// Register a [`Packet`] on a `send` stream/channel
    ///
    /// # Error
    /// Returns a [`SendError`] if registration failed.
    pub fn register_send_packet<T: Packet + 'static>(&mut self) -> Result<(), SendError> {
        if self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!("Type '{}' was already registered as a Send packet", type_name::<T>())))
        }

        self.send_packets.insert(self.next_send_id, TypeId::of::<T>());
        debug!("Registered Send packet with id={}, type={}", self.next_send_id, type_name::<T>());
        self.next_send_id += 1;
        Ok(())
    }
    
    pub fn received_all<T: Packet + 'static, U: PacketBuilder<T> + 'static>(&mut self, blocking: bool) -> Result<Vec<(String, Option<Vec<T>>)>, ReceiveError> {
        self.validate_for_received::<T>(true)?;
        self.update_new_receivers();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                runtime.block_on(async {
                    let mut res = vec![];
                    for (recv_index, (addr, _rx_map)) in self.rx.iter().enumerate() {
                        res.push((addr.to_string(), PacketManager::async_received_helper::<T, U>(blocking, recv_index, &self.receive_packets, &self.recv_packet_builders, &self.rx).await?));
                    }
                    Ok(res)
                })
            }
        }
    }

    // blocking blocks on ALL receivers
    pub async fn async_received_all<T: Packet + 'static, U: PacketBuilder<T> + 'static>(&mut self, blocking: bool) -> Result<Vec<(String, Option<Vec<T>>)>, ReceiveError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_received(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_received::<T>(true).await?;
        self.async_update_new_receivers().await;
        let mut res = vec![];
        for (recv_index, (addr, _rx_map)) in self.rx.iter().enumerate() {
            res.push((addr.to_string(), PacketManager::async_received_helper::<T, U>(blocking, recv_index, &self.receive_packets, &self.recv_packet_builders, &self.rx).await?));
        }
        Ok(res)
    }

    pub fn received<T: Packet + 'static, U: PacketBuilder<T> + 'static>(&mut self, blocking: bool) -> Result<Option<Vec<T>>, ReceiveError> {
        self.validate_for_received::<T>(false)?;
        self.update_new_receivers();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                runtime.block_on(PacketManager::async_received_helper::<T, U>(blocking, 0, &self.receive_packets, &self.recv_packet_builders, &self.rx))
            }
        }
    }

    pub async fn async_received<T: Packet + 'static, U: PacketBuilder<T> + 'static>(&mut self, blocking: bool) -> Result<Option<Vec<T>>, ReceiveError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_received(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_received::<T>(false).await?;
        self.async_update_new_receivers().await;
        PacketManager::async_received_helper::<T, U>(blocking, 0, &self.receive_packets, &self.recv_packet_builders, &self.rx).await
    }
    
    // Assumes does not have more than one client to send to, should be checked by callers
    // TODO: WARN ABOUT BLOCKING, can hang server if you get packets in order you don't expect!
    async fn async_received_helper<T: Packet + 'static, U: PacketBuilder<T> + 'static>(blocking: bool, recv_index: usize,
                                                                                       receive_packets: &BiMap<u32, TypeId>,
                                                                                       recv_packet_builders: &HashMap<TypeId, Box<dyn Any + Send + Sync>>,
                                                                                       rxs: &Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        if rxs.len() <= recv_index {
            return Err(ReceiveError::new(format!("Could not find receiver stream for recv index={}.  Did you forget to call init_connections()?  Or there might be no clients connected yet.", recv_index)));
        }
        let packet_type_id = TypeId::of::<T>();
        let id = receive_packets.get_by_right(&packet_type_id).unwrap();
        let (addr, rx_map) = rxs.get(recv_index).unwrap();
        let (rx_lock, _receive_thread) = rx_map.get(id).unwrap();
        let mut rx = rx_lock.write().await;
        let mut res: Vec<T> = Vec::new();
        let packet_builder: &U = recv_packet_builders.get(&TypeId::of::<T>()).unwrap().downcast_ref::<U>().unwrap();
        
        // If blocking, wait for the first packet
        if blocking {
            match rx.recv().await {
                None => { return Err(ReceiveError::new(format!("Channel for packet type {} closed unexpectedly!", type_name::<T>()))); }
                Some(bytes) => {
                    PacketManager::receive_bytes::<T, U>(bytes, packet_builder, &mut res)?;
                }
            }
        }
        
        // Loop for any subsequent packets
        loop {
            match rx.try_recv() {
                Ok(bytes) => {
                    PacketManager::receive_bytes::<T, U>(bytes, packet_builder, &mut res)?;
                }
                Err(e) => {
                    match e {
                        TryRecvError::Empty => { break; }
                        TryRecvError::Disconnected => {
                            return Err(ReceiveError::new(format!("Receiver channel for type {} was disconnected", type_name::<T>())));
                        }
                    }
                }
            }
        }

        if res.is_empty() {
            return Ok(None);
        }
        debug!("Fetched {} received packets of type={}, id={}, from addr={}", res.len(), type_name::<T>(), id, addr);
        Ok(Some(res))
    }
    
    fn validate_for_received<T: Packet + 'static>(&self, for_all: bool) -> Result<Option<Vec<T>>, ReceiveError> {
        if !for_all && self.has_more_than_one_remote() {
            return Err(ReceiveError::new(format!("async_received()/received() was called for packet {}, but there is more than one client.  Did you mean to call async_received_all()/received_all()?", type_name::<T>())))
        }
        
        if !self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!("Type '{}' was never registered!  Did you forget to call register_receive_packet()?", type_name::<T>())))
        }
        Ok(None)
    }

    async fn async_validate_for_received<T: Packet + 'static>(&self, for_all: bool) -> Result<Option<Vec<T>>, ReceiveError> {
        if !for_all && self.async_has_more_than_one_remote().await {
            return Err(ReceiveError::new(format!("async_received()/received() was called for packet {}, but there is more than one client.  Did you mean to call async_received_all()/received_all()?", type_name::<T>())))
        }
        
        if !self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!("Type '{}' was never registered!  Did you forget to call register_receive_packet()?", type_name::<T>())))
        }
        Ok(None)
    }
    
    fn update_new_receivers(&mut self) {
        let mut new_rx_lock = self.new_rxs.blocking_write();
        if !new_rx_lock.is_empty() {
            let mut new_rx_vec = std::mem::take(&mut *new_rx_lock);
            self.rx.append(&mut new_rx_vec);
        }
    }

    async fn async_update_new_receivers(&mut self) {
        let mut new_rx_lock = self.new_rxs.write().await;
        if !new_rx_lock.is_empty() {
            let mut new_rx_vec = std::mem::take(&mut *new_rx_lock);
            self.rx.append(&mut new_rx_vec);
        }
    }
    
    pub fn broadcast<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(true)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                runtime.block_on(async {
                    let send_streams_len = self.send_streams.len();
                    for send_index in 0..send_streams_len {
                        PacketManager::async_send_helper::<T>(&packet, send_index, &self.send_packets, &self.send_streams).await?;
                    }
                    Ok(())
                })
            }
        }
    }

    pub async fn async_broadcast<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let send_streams_len = self.send_streams.len();
        for send_index in 0..send_streams_len {
            PacketManager::async_send_helper::<T>(&packet, send_index, &self.send_packets, &self.send_streams).await?;
        }
        Ok(())
    }

    pub fn send<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(false)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                runtime.block_on(PacketManager::async_send_helper::<T>(&packet, 0, &self.send_packets, &self.send_streams))
            }
        }
    }

    pub async fn async_send<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(false).await?;
        self.async_update_new_senders().await;
        PacketManager::async_send_helper::<T>(&packet, 0, &self.send_packets, &self.send_streams).await
    }

    pub fn send_to<T: Packet + 'static>(&mut self, addr: impl Into<String>, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(true)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                let send_index = if self.server_connection.is_some() {
                    // We are the client sending to a single server
                    0
                } else {
                    // Else, we're a server and we need to fetch the client id or index
                    self.client_connections.blocking_read().get(&addr.into()).unwrap().0
                };
                runtime.block_on(PacketManager::async_send_helper::<T>(&packet, send_index as usize, &self.send_packets, &self.send_streams))
            }
        }
    }

    pub async fn async_send_to<T: Packet + 'static>(&mut self, addr: impl Into<String>, packet: T) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let send_index = if self.server_connection.is_some() {
            // We are the client sending to a single server
            0
        } else {
            // Else, we're a server and we need to fetch the client id or index
            self.client_connections.read().await.get(&addr.into()).unwrap().0
        };
        PacketManager::async_send_helper::<T>(&packet, send_index as usize, &self.send_packets, &self.send_streams).await
    }

    fn update_new_senders(&mut self) {
        let mut new_send_stream_lock = self.new_send_streams.blocking_write();
        if !new_send_stream_lock.is_empty() {
            let mut new_send_streams_vec = std::mem::take(&mut *new_send_stream_lock);
            self.send_streams.append(&mut new_send_streams_vec);
        }
    }

    async fn async_update_new_senders(&mut self) {
        let mut new_send_stream_lock = self.new_send_streams.write().await;
        if !new_send_stream_lock.is_empty() {
            let mut new_send_streams_vec = std::mem::take(&mut *new_send_stream_lock);
            self.send_streams.append(&mut new_send_streams_vec);
        }
    }
    
    async fn async_send_helper<T: Packet + 'static>(packet: &T, send_index: usize, send_packets: &BiMap<u32, TypeId>,
                                                    send_streams: &Vec<(String, HashMap<u32, RwLock<SendStream>>)>,) -> Result<(), SendError> {
        if send_streams.len() <= send_index {
            return Err(SendError::new(format!("Could not find send stream for client index={}.  Did you forget to call init_connections()?  Or there might be no clients connected yet.", send_index)));
        }
        let bytes = packet.as_bytes();
        let packet_type_id = TypeId::of::<T>();
        let id = send_packets.get_by_right(&packet_type_id).unwrap();
        let (addr, send_streams) = &send_streams[send_index];
        let mut send_stream = send_streams.get(id).unwrap().write().await;
        debug!("Sending bytes to {} with len {}", addr, bytes.len());
        trace!("Sending bytes {:?}", bytes);
        send_stream.write_chunk(bytes).await.unwrap();
        trace!("Sending FRAME_BOUNDARY");
        send_stream.write_all(FRAME_BOUNDARY).await.unwrap();
        debug!("Sent packet to {} with id={}, type={}", addr, id, type_name::<T>());
        Ok(())
    }
    
    pub fn get_num_clients(&self) -> u32 {
        self.client_connections.blocking_read().len() as u32
    }
    
    pub fn get_client_id<S: Into<String>>(&self, addr: S) -> u32 {
        self.client_connections.blocking_read().get(&addr.into()).unwrap().0
    }
    
    fn validate_for_send<T: Packet + 'static>(&self, for_all: bool) -> Result<(), SendError> {
        if !for_all && self.has_more_than_one_remote() {
            return Err(SendError::new(format!("send() was called for packet {}, but there is more than one client.  Did you mean to call broadcast()?", type_name::<T>())))
        }

        if !self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!("Type '{}' was never registered!  Did you forget to call register_send_packet()?", type_name::<T>())))
        }
        
        Ok(())
    }

    async fn async_validate_for_send<T: Packet + 'static>(&self, for_all: bool) -> Result<(), SendError> {
        if !for_all && self.async_has_more_than_one_remote().await {
            return Err(SendError::new(format!("async_send() was called for packet {}, but there is more than one client.  Did you mean to call async_broadcast()?", type_name::<T>())))
        }

        if !self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!("Type '{}' was never registered!  Did you forget to call register_send_packet()?", type_name::<T>())))
        }

        Ok(())
    }
    
    #[inline]
    fn receive_bytes<T: Packet + 'static, U: PacketBuilder<T> + 'static>(bytes: Bytes, packet_builder: &U, res: &mut Vec<T>) -> Result<(), ReceiveError> {
        if bytes.is_empty() {
            return Err(ReceiveError::new(format!("Received empty bytes for packet type={}!", type_name::<T>())));
        }
        debug!("Received packet with id={} for type={}", res.len(), type_name::<T>());
        let packet = packet_builder.read(bytes).unwrap();
        res.push(packet);
        Ok(())
    }

    // Either we are a single client talking to a single server, or a server talking to potentially multiple clients
    #[inline]
    fn has_more_than_one_remote(&self) -> bool {
        self.server_connection.is_none() && self.client_connections.blocking_read().len() > 1
    }

    #[inline]
    async fn async_has_more_than_one_remote(&self) -> bool {
        self.server_connection.is_none() && self.client_connections.read().await.len() > 1
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use durian_macros::bincode_packet;
    use tokio::sync::mpsc;
    use tokio::time::sleep;

    use durian::packet::PacketManager;

    use crate as durian;

    #[bincode_packet]
    #[derive(Debug, PartialEq, Eq)]
    struct Test {
        id: i32
    }

    #[bincode_packet]
    #[derive(Debug, PartialEq, Eq)]
    struct Other {
        name: String,
        id: i32
    }

    // TODO: Test sync versions
    // TODO: flaky, need to validate entire packet sequence like tests below or race condition can happen and validations fail
    #[tokio::test]
    async fn receive_packet_e2e_async() {
        let mut manager = PacketManager::new_for_async();
        
        let (tx, mut rx) = mpsc::channel(100);
        let server_addr = "127.0.0.1:5000";
        let client_addr = "127.0.0.1:5001";
        
        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            assert!(m.register_send_packet::<Test>().is_ok());
            assert!(m.register_send_packet::<Other>().is_ok());
            assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            assert!(m.async_init_connections(true, 2, 2, server_addr, None, 1, None).await.is_ok());
            
            for _ in 0..100 {
                assert!(m.async_send::<Test>(Test { id: 5 }).await.is_ok());
                assert!(m.async_send::<Test>(Test { id: 8 }).await.is_ok());
                assert!(m.async_send::<Other>(Other { name: "spoorn".to_string(), id: 4 }).await.is_ok());
                assert!(m.async_send::<Other>(Other { name: "kiko".to_string(), id: 6 }).await.is_ok());
                
                let test_res = m.async_received::<Test, TestPacketBuilder>(true).await;
                assert!(test_res.is_ok());
                let unwrapped = test_res.unwrap();
                assert!(unwrapped.is_some());
                assert_eq!(unwrapped.unwrap(), vec![Test { id: 6 }, Test { id: 9 }]);
                let other_res = m.async_received::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let unwrapped = other_res.unwrap();
                assert!(unwrapped.is_some());
                assert_eq!(unwrapped.unwrap(), vec![Other { name: "mango".to_string(), id: 1 }, Other { name: "luna".to_string(), id: 3 }]);
            }
            
            rx.recv().await;
            // loop {
            //     // Have to use tokio's sleep so it can yield to the tokio executor
            //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
            //     //sleep(Duration::from_millis(100)).await;
            // }
        });
        
        // Client
        assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        assert!(manager.register_send_packet::<Test>().is_ok());
        assert!(manager.register_send_packet::<Other>().is_ok());
        let client = manager.async_init_connections(false, 2, 2, server_addr, Some(client_addr), 0, None).await;
        println!("{:#?}", client);
        
        assert!(client.is_ok());
        
        for _ in 0..100 {
            // Send packets
            assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager.async_send::<Test>(Test { id: 9 }).await.is_ok());
            assert!(manager.async_send::<Other>(Other { name: "mango".to_string(), id: 1 }).await.is_ok());
            assert!(manager.async_send::<Other>(Other { name: "luna".to_string(), id: 3 }).await.is_ok());
            
            let test_res = manager.async_received::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let unwrapped = test_res.unwrap();
            assert!(unwrapped.is_some());
            assert_eq!(unwrapped.unwrap(), vec![Test { id: 5 }, Test { id: 8 }]);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            assert_eq!(unwrapped.unwrap(), vec![Other { name: "spoorn".to_string(), id: 4 }, Other { name: "kiko".to_string(), id: 6 }]);
        }
        
        tx.send(0).await.unwrap();
        assert!(server.await.is_ok());
    }

    #[tokio::test]
    async fn receive_packet_e2e_async_broadcast() {
        let mut manager = PacketManager::new_for_async();

        let (tx, mut rx) = mpsc::channel(100);
        let server_addr = "127.0.0.1:6000";
        let client_addr = "127.0.0.1:6001";
        let client2_addr = "127.0.0.1:6002";
        
        let num_receive = 8;
        let num_send = 8;

        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            assert!(m.register_send_packet::<Test>().is_ok());
            assert!(m.register_send_packet::<Other>().is_ok());
            assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            assert!(m.async_init_connections(true, 2, 2, server_addr, None, 2, None).await.is_ok());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("server sent {}, received {}", sent_packets, recv_packets);
                println!("all_test_packets len {}", all_test_packets.len());
                println!("all_other_packets len {}", all_other_packets.len());
                if sent_packets < num_send {
                    assert!(m.async_broadcast::<Test>(Test { id: 5 }).await.is_ok());
                    assert!(m.async_broadcast::<Test>(Test { id: 8 }).await.is_ok());
                    assert!(m.async_broadcast::<Other>(Other { name: "spoorn".to_string(), id: 4 }).await.is_ok());
                    assert!(m.async_broadcast::<Other>(Other { name: "kiko".to_string(), id: 6 }).await.is_ok());
                    sent_packets += 4;
                }

                if recv_packets == num_receive * 2 {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let test_res = m.async_received_all::<Test, TestPacketBuilder>(false).await;
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                assert_eq!(received_all.len(), 2);
                let (addr, unwrapped) = received_all.remove(0);
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
                if unwrapped2.is_some() {
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
                
                let other_res = m.async_received_all::<Other, OtherPacketBuilder>(false).await;
                assert!(other_res.is_ok());
                let mut received_all = other_res.unwrap();
                assert_eq!(received_all.len(), 2);
                let (addr, unwrapped) = received_all.remove(0);
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
                if unwrapped2.is_some() {
                    assert!(unwrapped2.is_some());
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
            }
            
            assert_eq!(all_test_packets.len(), num_receive);
            assert_eq!(all_other_packets.len(), num_receive);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 6 });
                } else {
                    assert_eq!(packet, Test { id: 9 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Other { name: "mango".to_string(), id: 1 });
                } else {
                    assert_eq!(packet, Other { name: "luna".to_string(), id: 3 });
                }
            }

            rx.recv().await;
            // loop {
            //     // Have to use tokio's sleep so it can yield to the tokio executor
            //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
            //     //sleep(Duration::from_millis(100)).await;
            // }
            println!("server exit");
        });

        let (client2_tx, mut client2_rx) = mpsc::channel(100);
        let client2 = tokio::spawn(async move {
            let mut manager = PacketManager::new_for_async();
            assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            assert!(manager.register_send_packet::<Test>().is_ok());
            assert!(manager.register_send_packet::<Other>().is_ok());
            assert!(manager.async_init_connections(false, 2, 2, server_addr, Some(client2_addr), 0, None).await.is_ok());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("client2 sent {}, received {}", sent_packets, recv_packets);
                if sent_packets < num_send {
                    assert!(manager.async_broadcast::<Test>(Test { id: 6 }).await.is_ok());
                    assert!(manager.async_broadcast::<Test>(Test { id: 9 }).await.is_ok());
                    assert!(manager.async_broadcast::<Other>(Other { name: "mango".to_string(), id: 1 }).await.is_ok());
                    assert!(manager.async_broadcast::<Other>(Other { name: "luna".to_string(), id: 3 }).await.is_ok());
                    sent_packets += 4;
                }

                if recv_packets == num_receive {
                    println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                    sleep(Duration::from_secs(4)).await;
                    break;
                }
                
                let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                assert!(!received_all.is_empty());
                let (addr, unwrapped) = received_all.remove(0);
                assert_eq!(addr, server_addr);
                assert!(unwrapped.is_some());
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_test_packets.append(&mut packets);
                let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let unwrapped = other_res.unwrap();
                assert!(unwrapped.is_some());
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_other_packets.append(&mut packets);
            }

            assert_eq!(all_test_packets.len(), num_receive/2);
            assert_eq!(all_other_packets.len(), num_receive/2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 5 });
                } else {
                    assert_eq!(packet, Test { id: 8 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
                } else {
                    assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
                }
            }

            client2_rx.recv().await;
            println!("client2 exit");
        });

        // Client
        assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        assert!(manager.register_send_packet::<Test>().is_ok());
        assert!(manager.register_send_packet::<Other>().is_ok());
        let client = manager.async_init_connections(false, 2, 2, server_addr, Some(client_addr), 0, None).await;
        println!("client1: {:#?}", client);

        assert!(client.is_ok());

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        loop {
            println!("client1 sent {}, received {}", sent_packets, recv_packets);
            // Send packets
            if sent_packets < num_send {
                assert!(manager.async_broadcast::<Test>(Test { id: 6 }).await.is_ok());
                assert!(manager.async_broadcast::<Test>(Test { id: 9 }).await.is_ok());
                assert!(manager.async_broadcast::<Other>(Other { name: "mango".to_string(), id: 1 }).await.is_ok());
                assert!(manager.async_broadcast::<Other>(Other { name: "luna".to_string(), id: 3 }).await.is_ok());
                sent_packets += 4;
            }

            if recv_packets == num_receive {
                println!("client1 done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            assert!(!received_all.is_empty());
            let (addr, unwrapped) = received_all.remove(0);
            assert_eq!(addr, server_addr);
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_test_packets.append(&mut packets);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_other_packets.append(&mut packets);
        }

        assert_eq!(all_test_packets.len(), num_receive/2);
        assert_eq!(all_other_packets.len(), num_receive/2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
            } else {
                assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
            }
        }
        
        println!("client1 exit");
        
        println!("send server exit");
        tx.send(0).await.unwrap();
        assert!(server.await.is_ok());
        println!("send client2 exit");
        client2_tx.send(0).await.unwrap();
        assert!(client2.await.is_ok());
    }

    #[tokio::test]
    async fn receive_packet_e2e_async_send_to() {
        let mut manager = PacketManager::new_for_async();

        let (tx, mut rx) = mpsc::channel(100);
        let server_addr = "127.0.0.1:7000";
        let client_addr = "127.0.0.1:7001";
        let client2_addr = "127.0.0.1:7002";

        let num_receive = 8;
        let num_send = 8;

        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            assert!(m.register_send_packet::<Test>().is_ok());
            assert!(m.register_send_packet::<Other>().is_ok());
            assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            assert!(m.async_init_connections(true, 2, 2, server_addr, None, 2, None).await.is_ok());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("server sent {}, received {}", sent_packets, recv_packets);
                println!("all_test_packets len {}", all_test_packets.len());
                println!("all_other_packets len {}", all_other_packets.len());
                if sent_packets < num_send {
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 5 }).await.is_ok());
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 8 }).await.is_ok());
                    assert!(m.async_send_to::<Other>(client_addr.to_string(), Other { name: "spoorn".to_string(), id: 4 }).await.is_ok());
                    assert!(m.async_send_to::<Other>(client_addr.to_string(), Other { name: "kiko".to_string(), id: 6 }).await.is_ok());
                    assert!(m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 5 }).await.is_ok());
                    assert!(m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 8 }).await.is_ok());
                    assert!(m.async_send_to::<Other>(client2_addr.to_string(), Other { name: "spoorn".to_string(), id: 4 }).await.is_ok());
                    assert!(m.async_send_to::<Other>(client2_addr.to_string(), Other { name: "kiko".to_string(), id: 6 }).await.is_ok());
                    sent_packets += 4;  // to each client
                }

                if recv_packets == num_receive * 2 {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let test_res = m.async_received_all::<Test, TestPacketBuilder>(false).await;
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                assert_eq!(received_all.len(), 2);
                let (addr, unwrapped) = received_all.remove(0);
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
                if unwrapped2.is_some() {
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }

                let other_res = m.async_received_all::<Other, OtherPacketBuilder>(false).await;
                assert!(other_res.is_ok());
                let mut received_all = other_res.unwrap();
                assert_eq!(received_all.len(), 2);
                let (addr, unwrapped) = received_all.remove(0);
                let (addr2, unwrapped2) = received_all.remove(0);
                assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
                if unwrapped2.is_some() {
                    assert!(unwrapped2.is_some());
                    let mut packets = unwrapped2.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }
            }

            assert_eq!(all_test_packets.len(), num_receive);
            assert_eq!(all_other_packets.len(), num_receive);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 6 });
                } else {
                    assert_eq!(packet, Test { id: 9 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Other { name: "mango".to_string(), id: 1 });
                } else {
                    assert_eq!(packet, Other { name: "luna".to_string(), id: 3 });
                }
            }

            rx.recv().await;
            // loop {
            //     // Have to use tokio's sleep so it can yield to the tokio executor
            //     // https://stackoverflow.com/questions/70798841/why-does-a-tokio-thread-wait-for-a-blocking-thread-before-continuing?rq=1
            //     //sleep(Duration::from_millis(100)).await;
            // }
            println!("server exit");
        });

        let (client2_tx, mut client2_rx) = mpsc::channel(100);
        let client2 = tokio::spawn(async move {
            let mut manager = PacketManager::new_for_async();
            assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            assert!(manager.register_send_packet::<Test>().is_ok());
            assert!(manager.register_send_packet::<Other>().is_ok());
            assert!(manager.async_init_connections(false, 2, 2, server_addr, Some(client2_addr), 0, None).await.is_ok());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("client2 sent {}, received {}", sent_packets, recv_packets);
                if sent_packets < num_send {
                    // Either send or send_to should work since there is only one recipient
                    assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                    assert!(manager.async_send_to::<Test>(server_addr.to_string(), Test { id: 9 }).await.is_ok());
                    assert!(manager.async_send::<Other>(Other { name: "mango".to_string(), id: 1 }).await.is_ok());
                    assert!(manager.async_send_to::<Other>(server_addr.to_string(), Other { name: "luna".to_string(), id: 3 }).await.is_ok());
                    sent_packets += 4;
                }

                if recv_packets == num_receive {
                    println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                    sleep(Duration::from_secs(4)).await;
                    break;
                }

                let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                assert!(!received_all.is_empty());
                let (addr, unwrapped) = received_all.remove(0);
                assert_eq!(addr, server_addr);
                assert!(unwrapped.is_some());
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_test_packets.append(&mut packets);
                let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let unwrapped = other_res.unwrap();
                assert!(unwrapped.is_some());
                let mut packets = unwrapped.unwrap();
                recv_packets += packets.len();
                all_other_packets.append(&mut packets);
            }

            assert_eq!(all_test_packets.len(), num_receive/2);
            assert_eq!(all_other_packets.len(), num_receive/2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 5 });
                } else {
                    assert_eq!(packet, Test { id: 8 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
                } else {
                    assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
                }
            }

            client2_rx.recv().await;
            println!("client2 exit");
        });

        // Client
        assert!(manager.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
        assert!(manager.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
        assert!(manager.register_send_packet::<Test>().is_ok());
        assert!(manager.register_send_packet::<Other>().is_ok());
        let client = manager.async_init_connections(false, 2, 2, server_addr, Some(client_addr), 0, None).await;
        println!("client1: {:#?}", client);

        assert!(client.is_ok());

        let mut sent_packets = 0;
        let mut recv_packets = 0;
        let mut all_test_packets = Vec::new();
        let mut all_other_packets = Vec::new();
        loop {
            println!("client1 sent {}, received {}", sent_packets, recv_packets);
            // Send packets
            if sent_packets < num_send {
                // Either send or send_to should work since there is only one recipient
                assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                assert!(manager.async_send_to::<Test>(server_addr.to_string(), Test { id: 9 }).await.is_ok());
                assert!(manager.async_send::<Other>(Other { name: "mango".to_string(), id: 1 }).await.is_ok());
                assert!(manager.async_send_to::<Other>(server_addr.to_string(), Other { name: "luna".to_string(), id: 3 }).await.is_ok());
                sent_packets += 4;
            }

            if recv_packets == num_receive {
                println!("client1 done, sent {}, received {}", sent_packets, recv_packets);
                break;
            }

            let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let mut received_all = test_res.unwrap();
            assert!(!received_all.is_empty());
            let (addr, unwrapped) = received_all.remove(0);
            assert_eq!(addr, server_addr);
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_test_packets.append(&mut packets);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            let mut packets = unwrapped.unwrap();
            recv_packets += packets.len();
            all_other_packets.append(&mut packets);
        }

        assert_eq!(all_test_packets.len(), num_receive/2);
        assert_eq!(all_other_packets.len(), num_receive/2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Other { name: "spoorn".to_string(), id: 4 });
            } else {
                assert_eq!(packet, Other { name: "kiko".to_string(), id: 6 });
            }
        }

        println!("client1 exit");

        println!("send server exit");
        tx.send(0).await.unwrap();
        assert!(server.await.is_ok());
        println!("send client2 exit");
        client2_tx.send(0).await.unwrap();
        assert!(client2.await.is_ok());
    }
}