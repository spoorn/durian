#![allow(clippy::type_complexity)]

use std::any::{type_name, Any, TypeId};
use std::cmp::max;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use bimap::BiMap;
use bytes::Bytes;
use hashbrown::HashMap;
use indexmap::IndexMap;
use log::{debug, error, trace, warn};
use quinn::{Connection, Endpoint, ReadError, RecvStream, SendStream, VarInt, WriteError};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::task::JoinHandle;

use crate::quinn_helpers::{make_client_endpoint, make_server_endpoint};
use crate::{CloseError, ConnectionError, ErrorType, ReceiveError, SendError};

#[cfg(test)]
#[path = "./packet_tests.rs"]
mod packet_tests;

// Sent at the end of frames
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
/// 3. Initiate connection(s) with [`init_client()`](`PacketManager::init_client()`) (or the async variant [`async_init_client()`](`PacketManager::async_init_client()`)
/// if on the client side, else use [`init_server()`](`PacketManager::init_server()`) (or the async variant [`async_init_server)`](`PacketManager::async_init_server()`)
/// if on the server side.
///
/// 4. Send packets using any of [`broadcast()`](`PacketManager::broadcast()`), [`send()`](`PacketManager::send()`), [`send_to()`](`PacketManager::send_to()`)
/// or the respective `async` variants if calling from an async context already.  Receive packets
/// using any of [`received_all()`](`PacketManager::received_all()`) , [`received()`](`PacketManager::received()`), or the respective
/// `async` variants.
///
/// # Example
///
/// ```rust
/// use durian::{ClientConfig, PacketManager};
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
///     // Initialize a client
///     let client_config = ClientConfig::new("127.0.0.1:5001", "127.0.0.1:5000", 2, 2);
///     manager.init_client(client_config).unwrap();
///
///     // Send and receive packets
///     manager.broadcast(InputMovement { direction: "North".to_string() }).unwrap();
///     manager.received_all::<Position, PositionPacketBuilder>(false).unwrap();
///
///     // The above PacketManager is for the client.  Server side is similar except the packets
///     // are swapped between receive vs send channels:
///
///     // Create PacketManager
///     let mut server_manager = PacketManager::new();
///
///     // Register send and receive packets
///     server_manager.register_receive_packet::<ClientAck>(ClientAckPacketBuilder).unwrap();
///     server_manager.register_receive_packet::<InputMovement>(InputMovementPacketBuilder).unwrap();
///     server_manager.register_send_packet::<Position>().unwrap();
///     server_manager.register_send_packet::<ServerAck>().unwrap();
///
///     // Initialize a client
///     let client_config = ClientConfig::new("127.0.0.1:5001", "127.0.0.1:5000", 2, 2);
///     server_manager.init_client(client_config).unwrap();
///
///     // Send and receive packets
///     server_manager.broadcast(Position { x: 1, y: 3 }).unwrap();
///     server_manager.received_all::<InputMovement, InputMovementPacketBuilder>(false).unwrap();
/// }
/// ```
#[derive(Debug)]
pub struct PacketManager {
    /// Packet Id <-> Packet TypeId
    receive_packets: BiMap<u32, TypeId>,
    /// Packet Id <-> Packet TypeId
    send_packets: BiMap<u32, TypeId>,
    recv_packet_builders: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    // DenseSlotMap for the below so we can iterate fast, while not degrading insert/remove much
    /// Send streams RemoteId -> Packet Id -> SendStream
    send_streams: IndexMap<u32, HashMap<u32, RwLock<SendStream>>>,
    /// Receive channels, to be filled by separate threads
    /// RemoteId -> PacketId -> (Receiver channel, Thread JoinHandle)
    rx: IndexMap<u32, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>>,
    /// Track new send streams to be put into the send_streams IndexMap
    /// This is so new send streams can be created after PacketManager initialization
    new_send_streams: Arc<RwLock<Vec<(u32, HashMap<u32, RwLock<SendStream>>)>>>,
    /// Track new receive streams to be put into the rx IndexMap
    /// This is so new receive streams can be created after PacketManager initialization
    new_rxs: Arc<RwLock<Vec<(u32, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
    // Endpoint and Connection structs moved to the struct fields to prevent closing connections
    // by dropping.
    // This is also used to count the number of clients when broadcasting
    /// Remote Id (index in IndexMaps) -> (addr, Connection)
    remote_connections: Arc<RwLock<HashMap<u32, (String, Connection)>>>,
    // (Socket Address, Endpoint)
    source: (String, Option<Arc<Endpoint>>),
    /// For tracking receive packet Ids
    next_receive_id: u32,
    /// For tracking send packet Ids
    next_send_id: u32,
    // We construct a single Tokio Runtime to be used by each PacketManger instance, so that
    // methods can be synchronous.  There is also an async version of each API if the user wants
    // to use their own runtime.
    runtime: Arc<Option<Runtime>>,
}

#[derive(Clone, PartialEq, Eq, Debug)]
/// Client configuration for initiating a connection from a client to a server via [`PacketManager::init_client()`]
pub struct ClientConfig {
    /// Socket address of the client (`hostname:port`) (e.g. "127.0.0.1:5001")
    pub addr: String,
    /// Socket address of the server to connect to (`hostname:port`) (e.g. "127.0.0.1:5000")
    pub server_addr: String,
    /// Number of `receive` streams to accept from server to client.  Must be equal to number of receive packets
    /// registered through [`PacketManager::register_receive_packet()`]
    pub num_receive_streams: u32,
    /// Number of `send` streams to open from client to server.  Must be equal to number of send packets registered
    /// through [`PacketManager::register_send_packet()`]
    pub num_send_streams: u32,
    /// Period of inactivity before sending a keep-alive packet
    ///
    /// Keep-alive packets prevent an inactive but otherwise healthy connection from timing out.
    ///
    /// None to disable, which is the default. Only one side of any given connection needs keep-alive enabled for
    /// the connection to be preserved. Must be set lower than the idle_timeout of both peers to be effective.
    pub keep_alive_interval: Option<Duration>,
    /// Maximum duration of inactivity to accept before timing out the connection.
    /// The true idle timeout is the minimum of this and the peer's own max idle timeout. Defaults to 60 seconds.
    /// None represents an infinite timeout.
    ///
    /// __IMPORTANT: In the case of clients disconnecting abruptly, i.e. your application cannot call
    /// [`PacketManager::close_connection()`] or [`PacketManager::finish_connection()`] gracefully, the *true idle timeout* will help to remove
    /// disconnected clients from the connection queue, thus allowing them to reconnect after that timeout frame.__
    ///
    /// __WARNING: If a peer or its network path malfunctions or acts maliciously, an infinite idle timeout can result
    /// in permanently hung futures!__
    pub idle_timeout: Option<Duration>,

    /// Protocols to send to server if applicable.
    ///
    /// ## Example:
    ///
    /// ```
    /// use durian::ServerConfig;
    ///
    /// let mut config = ServerConfig::new("127.0.0.1:5000", 0, None, 2, 2);
    /// config.with_alpn_protocols(&[b"hq-29"]);
    /// ```
    pub alpn_protocols: Option<Vec<Vec<u8>>>,
}

impl ClientConfig {
    /// Construct and return a new [`ClientConfig`]
    pub fn new<S: Into<String>>(
        addr: S,
        server_addr: S,
        num_receive_streams: u32,
        num_send_streams: u32,
    ) -> Self {
        ClientConfig {
            addr: addr.into(),
            server_addr: server_addr.into(),
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: Some(Duration::from_secs(60)),
            alpn_protocols: None,
        }
    }

    /// Set the keep alive interval
    pub fn with_keep_alive_interval(&mut self, duration: Duration) -> &mut Self {
        self.keep_alive_interval = Some(duration);
        self
    }

    /// Set the idle timeout
    pub fn with_idle_timeout(&mut self, duration: Duration) -> &mut Self {
        self.idle_timeout = Some(duration);
        self
    }

    /// Set the ALPN protocols
    pub fn with_alpn_protocols(&mut self, protocols: &[&[u8]]) -> &mut Self {
        self.alpn_protocols = Some(protocols.iter().map(|&x| x.into()).collect());
        self
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
/// Server configuration for spinning up a server on a socket address via [`PacketManager::init_server()`]
pub struct ServerConfig {
    /// Socket address of the server (`hostname:port`) (e.g. "127.0.0.1:5001")
    pub addr: String,
    /// Number of clients to block waiting for incoming connections
    pub wait_for_clients: u32,
    /// [Optional] Total number of clients the server expects to connect with.  The server will spin up a thread that
    /// waits for incoming client connections until `total_expected_clients` is reached.  Set to `None` to allow any
    /// number of clients connections.  If `wait_for_clients > total_expected_clients`, the server will still wait
    /// for `wait_for_clients` number of client connections.
    pub total_expected_clients: Option<u32>,
    /// Set the max number of concurrent connection accepts to process at any given time.
    ///
    /// A thread will be spawned for each, allowing `max_concurrent_accepts` connections to come in at the same time.
    /// Default to `wait_for_clients + total_expected_clients(if set)`, else number of cores available.
    pub max_concurrent_accepts: u32,
    /// Number of `receive` streams to accept from server to client.  Must be equal to number of receive packets
    /// registered through [`PacketManager::register_receive_packet()`]
    pub num_receive_streams: u32,
    /// Number of `send` streams to open from client to server.  Must be equal to number of send packets registered
    /// through [`PacketManager::register_send_packet()`]
    pub num_send_streams: u32,
    /// Period of inactivity before sending a keep-alive packet
    ///
    /// Keep-alive packets prevent an inactive but otherwise healthy connection from timing out.
    ///
    /// None to disable, which is the default. Only one side of any given connection needs keep-alive enabled for
    /// the connection to be preserved. Must be set lower than the idle_timeout of both peers to be effective.
    pub keep_alive_interval: Option<Duration>,
    /// Maximum duration of inactivity to accept before timing out the connection.
    /// The true idle timeout is the minimum of this and the peer's own max idle timeout. Defaults to 60 seconds.
    /// None represents an infinite timeout.
    ///
    /// __IMPORTANT: In the case of clients disconnecting abruptly, i.e. your application cannot call
    /// [`PacketManager::close_connection()`] or [`PacketManager::finish_connection()`] gracefully, the *true idle timeout* will help to remove
    /// disconnected clients from the connection queue, thus allowing them to reconnect after that timeout frame.__
    ///
    /// __WARNING: If a peer or its network path malfunctions or acts maliciously, an infinite idle timeout can result
    /// in permanently hung futures!__
    pub idle_timeout: Option<Duration>,

    /// Protocols to send to server if applicable.
    ///
    /// ## Example:
    ///
    /// ```
    /// use durian::ServerConfig;
    ///
    /// let mut config = ServerConfig::new("127.0.0.1:5000", 0, None, 2, 2);
    /// config.with_alpn_protocols(&[b"hq-29"]);
    /// ```
    pub alpn_protocols: Option<Vec<Vec<u8>>>,
}

impl ServerConfig {
    /// Construct and return a new [`ServerConfig`]
    pub fn new<S: Into<String>>(
        addr: S,
        wait_for_clients: u32,
        total_expected_clients: Option<u32>,
        num_receive_streams: u32,
        num_send_streams: u32,
    ) -> Self {
        // If total_expected_clients is erroneously set to lower than wait_for_clients, just set it to 0
        let mut expected_clients =
            if let Some(expected) = total_expected_clients { expected } else { 0 };
        expected_clients = if expected_clients > wait_for_clients {
            expected_clients - wait_for_clients
        } else {
            0
        };
        ServerConfig {
            addr: addr.into(),
            wait_for_clients,
            total_expected_clients,
            max_concurrent_accepts: max(num_cpus::get() as u32, expected_clients),
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: Some(Duration::from_secs(60)),
            alpn_protocols: None,
        }
    }

    /// Construct and return a new [`ServerConfig`] that only allows up to `wait_for_clients` number of client connections
    pub fn new_with_max_clients<S: Into<String>>(
        addr: S,
        wait_for_clients: u32,
        num_receive_streams: u32,
        num_send_streams: u32,
    ) -> Self {
        ServerConfig {
            addr: addr.into(),
            wait_for_clients,
            total_expected_clients: Some(wait_for_clients),
            max_concurrent_accepts: num_cpus::get() as u32,
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: Some(Duration::from_secs(60)),
            alpn_protocols: None,
        }
    }

    /// Construct and return a new [`ServerConfig`], with [`total_expected_clients`](`ServerConfig::total_expected_clients`) set to `None` so the server
    /// continuously accepts new client connections
    pub fn new_listening<S: Into<String>>(
        addr: S,
        wait_for_clients: u32,
        num_receive_streams: u32,
        num_send_streams: u32,
    ) -> Self {
        ServerConfig {
            addr: addr.into(),
            wait_for_clients,
            total_expected_clients: None,
            max_concurrent_accepts: num_cpus::get() as u32,
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: Some(Duration::from_secs(60)),
            alpn_protocols: None,
        }
    }

    /// Set the keep alive interval
    pub fn with_keep_alive_interval(&mut self, duration: Duration) -> &mut Self {
        self.keep_alive_interval = Some(duration);
        self
    }

    /// Set the max idle timeout
    pub fn with_idle_timeout(&mut self, duration: Option<Duration>) -> &mut Self {
        self.idle_timeout = duration;
        self
    }

    /// Set the ALPN protocols
    pub fn with_alpn_protocols(&mut self, protocols: &[&[u8]]) -> &mut Self {
        self.alpn_protocols = Some(protocols.iter().map(|&x| x.into()).collect());
        self
    }

    /// Set the max concurrent accept connections
    pub fn with_max_concurrent_accepts(&mut self, max_concurrent_accepts: u32) -> &mut Self {
        self.max_concurrent_accepts = max_concurrent_accepts;
        self
    }
}

// TODO: Allow closing Endpoint directly, along with its threads
impl PacketManager {
    /// Create a new `PacketManager`
    ///
    /// If calling from an asynchronous context/runtime, use the [`new_for_async()`](`PacketManager::new_for_async()`) variant.
    /// This constructs a [`tokio Runtime`](`Runtime`) for the `PacketManager` instance.
    ///
    /// # Panic
    /// If the [`Runtime`] could not be created.  Usually happens if you call `new()` from an existing async runtime.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build();
        match runtime {
            Ok(runtime) => PacketManager {
                receive_packets: BiMap::new(),
                send_packets: BiMap::new(),
                recv_packet_builders: HashMap::new(),
                send_streams: IndexMap::new(),
                rx: IndexMap::new(),
                new_send_streams: Arc::new(RwLock::new(vec![])),
                new_rxs: Arc::new(RwLock::new(Vec::new())),
                remote_connections: Arc::new(RwLock::new(HashMap::new())),
                source: ("".to_string(), None),
                next_receive_id: 0,
                next_send_id: 0,
                runtime: Arc::new(Some(runtime)),
            },
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
            send_streams: IndexMap::new(),
            rx: IndexMap::new(),
            new_send_streams: Arc::new(RwLock::new(vec![])),
            new_rxs: Arc::new(Default::default()),
            remote_connections: Arc::new(RwLock::new(HashMap::new())),
            source: ("".to_string(), None),
            next_receive_id: 0,
            next_send_id: 0,
            runtime: Arc::new(None),
        }
    }

    /// Initialize a client side `PacketManager`
    ///
    /// # Arguments
    ///
    /// * `client_config` - Client configuration
    ///
    /// # Returns
    /// A [`Result`] containing `()` if successful, else a [`ConnectionError`] on error
    ///
    /// # Panics
    /// When the `PacketManager` does not have a runtime instance associated with it, which can happen if you created
    /// the `PacketManager` using [`new_for_async()`](`PacketManager::new_for_async()`) instead of [`new()`](`PacketManager::new()`).
    pub fn init_client(&mut self, client_config: ClientConfig) -> Result<(), ConnectionError> {
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_init_client()?");
            }
            Some(runtime) => {
                self.validate_connection_prereqs(
                    client_config.num_receive_streams,
                    client_config.num_send_streams,
                )?;
                self.source.0 = client_config.addr.clone();
                runtime.block_on(PacketManager::init_client_helper(
                    client_config,
                    &self.runtime,
                    &self.new_rxs,
                    &self.new_send_streams,
                    &self.remote_connections,
                    &mut self.source.1,
                ))
            }
        }
    }

    /// Initialize a client side `PacketManager`, to be used if calling from an async context
    ///
    /// # Arguments
    ///
    /// * `client_config` - Client configuration
    ///
    /// # Returns
    /// A `Future` that returns a [`Result`] containing `()` if successful, else a [`ConnectionError`] on error
    ///
    /// # Panics
    /// When the `PacketManager` has a runtime instance associated with it, which can happen if you created
    /// the `PacketManager` using [`new()`](`PacketManager::new()`) instead of [`new_for_async()`](`PacketManager::new_for_async()`).
    pub async fn async_init_client(
        &mut self,
        client_config: ClientConfig,
    ) -> Result<(), ConnectionError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_init_client(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.validate_connection_prereqs(
            client_config.num_receive_streams,
            client_config.num_send_streams,
        )?;
        self.source.0 = client_config.addr.clone();
        PacketManager::init_client_helper(
            client_config,
            &self.runtime,
            &self.new_rxs,
            &self.new_send_streams,
            &self.remote_connections,
            &mut self.source.1,
        )
        .await
    }

    /// Initialize a server side `PacketManager`
    ///
    /// # Arguments
    ///
    /// * `server_config` - Server configuration
    ///
    /// # Returns
    /// A [`Result`] containing `()` if successful, else a [`ConnectionError`] on error
    ///
    /// # Panics
    /// When the `PacketManager` does not have a runtime instance associated with it, which can happen if you created
    /// the `PacketManager` using [`new_for_async()`](`PacketManager::new_for_async()`) instead of [`new()`](`PacketManager::new()`).
    pub fn init_server(&mut self, server_config: ServerConfig) -> Result<(), ConnectionError> {
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_init_server()?");
            }
            Some(runtime) => {
                self.validate_connection_prereqs(
                    server_config.num_receive_streams,
                    server_config.num_send_streams,
                )?;
                self.source.0 = server_config.addr.clone();
                runtime.block_on(PacketManager::init_server_helper(
                    server_config,
                    &self.runtime,
                    &self.new_rxs,
                    &self.new_send_streams,
                    &self.remote_connections,
                    &mut self.source.1,
                ))
            }
        }
    }

    /// Initialize a server side `PacketManager`, to be used if calling from an async context
    ///
    /// # Arguments
    ///
    /// * `server_config` - Server configuration
    ///
    /// # Returns
    /// A `Future` that returns a [`Result`] containing `()` if successful, else a [`ConnectionError`] on error
    ///
    /// # Panics
    /// When the `PacketManager` has a runtime instance associated with it, which can happen if you created
    /// the `PacketManager` using [`new()`](`PacketManager::new()`) instead of [`new_for_async()`](`PacketManager::new_for_async()`).
    pub async fn async_init_server(
        &mut self,
        server_config: ServerConfig,
    ) -> Result<(), ConnectionError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_init_server(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.validate_connection_prereqs(
            server_config.num_receive_streams,
            server_config.num_send_streams,
        )?;
        self.source.0 = server_config.addr.clone();
        PacketManager::init_server_helper(
            server_config,
            &self.runtime,
            &self.new_rxs,
            &self.new_send_streams,
            &self.remote_connections,
            &mut self.source.1,
        )
        .await
    }

    fn validate_connection_prereqs(
        &self,
        num_incoming_streams: u32,
        num_outgoing_streams: u32,
    ) -> Result<(), ConnectionError> {
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

    async fn init_server_helper(
        server_config: ServerConfig,
        runtime: &Arc<Option<Runtime>>,
        new_rxs: &Arc<RwLock<Vec<(u32, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
        new_send_streams: &Arc<RwLock<Vec<(u32, HashMap<u32, RwLock<SendStream>>)>>>,
        client_connections: &Arc<RwLock<HashMap<u32, (String, Connection)>>>,
        source_endpoint: &mut Option<Arc<Endpoint>>,
    ) -> Result<(), ConnectionError> {
        debug!("Initiating server with {:?}", server_config);

        let (endpoint, server_cert) = make_server_endpoint(
            server_config.addr.parse()?,
            server_config.keep_alive_interval,
            server_config.idle_timeout,
            server_config.alpn_protocols,
        )?;
        let endpoint = Arc::new(endpoint);
        let _ = source_endpoint.insert(Arc::clone(&endpoint));
        let num_receive_streams = server_config.num_receive_streams;
        let num_send_streams = server_config.num_send_streams;

        // TODO: use synchronous blocks during read and write of client_connections
        // Single connection
        for i in 0..server_config.wait_for_clients {
            let incoming_conn = endpoint.accept().await.unwrap();
            let conn = incoming_conn.await?;
            let addr = conn.remote_address();
            if client_connections.read().await.contains_key(&i) {
                panic!(
                    "[server] Client with addr={} was already connected as remote_id={}",
                    addr, i
                );
            }
            println!(
                "[server] connection accepted: addr={}, remote_id={}",
                conn.remote_address(),
                i
            );
            let (server_send_streams, recv_streams) = PacketManager::open_streams_for_connection(
                i,
                &conn,
                num_receive_streams,
                num_send_streams,
            )
            .await?;
            let res = PacketManager::spawn_receive_thread(i, recv_streams, runtime.as_ref())?;
            new_rxs.write().await.push((i, res));
            new_send_streams.write().await.push((i, server_send_streams));
            client_connections.write().await.insert(i, (addr.to_string(), conn));
        }

        if server_config.total_expected_clients.is_none()
            || server_config.total_expected_clients.unwrap() > server_config.wait_for_clients
        {
            let remote_id = Arc::new(Mutex::new(server_config.wait_for_clients));

            // TODO: save this value
            for i in 0..server_config.max_concurrent_accepts {
                debug!("Spinning up client connection accept thread #{}", i);

                let client_connections = client_connections.clone();
                let arc_send_streams = new_send_streams.clone();
                let arc_rx = new_rxs.clone();
                let arc_runtime = Arc::clone(runtime);
                let endpoint = Arc::clone(&endpoint);
                let remote_id_clone = Arc::clone(&remote_id);

                // TODO: refactor
                let accept_client_task = async move {
                    match server_config.total_expected_clients {
                        None => loop {
                            debug!("[server] Waiting for more clients...");
                            let incoming_conn = endpoint.accept().await.unwrap();
                            let conn = incoming_conn.await?;
                            let addr = conn.remote_address();
                            let mut id = remote_id_clone.lock().await;
                            if client_connections.read().await.contains_key(&*id) {
                                panic!("[server] Client with addr={} was already connected as remote_id={}", addr, id);
                            }
                            debug!("[server] connection accepted: addr={}", conn.remote_address());
                            let (send_streams, recv_streams) =
                                PacketManager::open_streams_for_connection(
                                    *id,
                                    &conn,
                                    num_receive_streams,
                                    num_send_streams,
                                )
                                .await?;
                            let res = PacketManager::spawn_receive_thread(
                                *id,
                                recv_streams,
                                arc_runtime.as_ref(),
                            )?;
                            arc_rx.write().await.push((*id, res));
                            arc_send_streams.write().await.push((*id, send_streams));
                            client_connections.write().await.insert(*id, (addr.to_string(), conn));
                            *id += 1;
                        },
                        Some(expected_num_clients) => {
                            for i in 0..(expected_num_clients - server_config.wait_for_clients) {
                                debug!(
                                    "[server] Waiting for client #{}",
                                    i + server_config.wait_for_clients
                                );
                                let incoming_conn = endpoint.accept().await.unwrap();
                                let conn = incoming_conn.await?;
                                let addr = conn.remote_address();
                                let mut id = remote_id_clone.lock().await;
                                if client_connections.read().await.contains_key(&*id) {
                                    panic!(
                                        "[server] Client with addr={} was already connected as remote_id={}",
                                        addr, id
                                    );
                                }
                                debug!(
                                    "[server] connection accepted: addr={}",
                                    conn.remote_address()
                                );
                                let (send_streams, recv_streams) =
                                    PacketManager::open_streams_for_connection(
                                        *id,
                                        &conn,
                                        num_receive_streams,
                                        num_send_streams,
                                    )
                                    .await?;
                                let res = PacketManager::spawn_receive_thread(
                                    *id,
                                    recv_streams,
                                    arc_runtime.as_ref(),
                                )?;
                                arc_rx.write().await.push((*id, res));
                                arc_send_streams.write().await.push((*id, send_streams));
                                client_connections
                                    .write()
                                    .await
                                    .insert(*id, (addr.to_string(), conn));
                                *id += 1;
                            }
                        }
                    }
                    Ok::<(), Box<ConnectionError>>(())
                };

                let accept_client_thread = match runtime.as_ref() {
                    None => tokio::spawn(accept_client_task),
                    Some(runtime) => runtime.spawn(accept_client_task),
                };
            }
        }

        Ok(())
    }

    // TODO: Add support for creating a client PacketManager using an existing endpoint
    async fn init_client_helper(
        client_config: ClientConfig,
        runtime: &Arc<Option<Runtime>>,
        new_rxs: &Arc<RwLock<Vec<(u32, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
        new_send_streams: &Arc<RwLock<Vec<(u32, HashMap<u32, RwLock<SendStream>>)>>>,
        // For bookkeeping, but technically the "client_connection" will be the server only
        client_connections: &Arc<RwLock<HashMap<u32, (String, Connection)>>>,
        source_endpoint: &mut Option<Arc<Endpoint>>,
    ) -> Result<(), ConnectionError> {
        debug!("Initiating client with {:?}", client_config);
        // Bind this endpoint to a UDP socket on the given client address.
        let endpoint = make_client_endpoint(
            client_config.addr.parse()?,
            &[],
            client_config.keep_alive_interval,
            client_config.idle_timeout,
            client_config.alpn_protocols,
        )?;
        let endpoint = Arc::new(endpoint);
        let _ = source_endpoint.insert(Arc::clone(&endpoint));

        // Connect to the server passing in the server name which is supposed to be in the server certificate.
        let conn = endpoint.connect(client_config.server_addr.parse()?, "server")?.await?;
        let addr = conn.remote_address();
        debug!("[client] connected: addr={}", addr);
        let (client_send_streams, recv_streams) = PacketManager::open_streams_for_connection(
            0,
            &conn,
            client_config.num_receive_streams,
            client_config.num_send_streams,
        )
        .await?;
        let res = PacketManager::spawn_receive_thread(0, recv_streams, runtime.as_ref())?;
        // Client side defaults to only having the 1 server at "remote_id" 0
        new_rxs.write().await.push((0, res));
        new_send_streams.write().await.push((0, client_send_streams));
        client_connections.write().await.insert(0, (addr.to_string(), conn));
        Ok(())
    }

    async fn open_streams_for_connection(
        remote_id: u32,
        conn: &Connection,
        num_incoming_streams: u32,
        num_outgoing_streams: u32,
    ) -> Result<(HashMap<u32, RwLock<SendStream>>, HashMap<u32, RecvStream>), Box<dyn Error>> {
        let mut send_streams = HashMap::new();
        let mut recv_streams = HashMap::new();
        // Note: Packets are not sent immediately upon the write.  The thread needs to be kept
        // open so that the packets can actually be sent over the wire to the client.
        for i in 0..num_outgoing_streams {
            trace!("Opening outgoing stream for remote_id={} packet id={}", remote_id, i);
            let mut send_stream = conn.open_uni().await?;
            trace!("Writing packet to {} for packet id {}", remote_id, i);
            send_stream.write_u32(i).await?;
            send_streams.insert(i, RwLock::new(send_stream));
        }

        for i in 0..num_incoming_streams {
            trace!("Accepting incoming stream from {} for packet id {}", remote_id, i);
            let mut recv = conn.accept_uni().await?;
            trace!("Validating incoming packet from {} id {}", remote_id, i);
            let id = recv.read_u32().await?;
            trace!("Received incoming packet from {} with packet id {}", remote_id, id);
            // if id >= self.next_receive_id {
            //     return Err(Box::new(ConnectionError::new(format!("Received unexpected packet ID {} from server", id))));
            // }

            recv_streams.insert(i, recv);
        }

        Ok((send_streams, recv_streams))
    }

    fn spawn_receive_thread(
        remote_id: u32,
        recv_streams: HashMap<u32, RecvStream>,
        runtime: &Option<Runtime>,
    ) -> Result<HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>, Box<dyn Error>> {
        let mut rxs = HashMap::new();
        trace!(
            "Spawning receive thread for remote_id={} for {} ids",
            remote_id,
            recv_streams.len()
        );
        for (id, mut recv_stream) in recv_streams.into_iter() {
            let (tx, rx) = mpsc::channel(100);

            let task = async move {
                let mut partial_chunk: Option<Bytes> = None;
                loop {
                    // TODO: relay error message
                    // TODO: configurable size limit
                    let chunk = recv_stream.read_chunk(usize::MAX, true).await;
                    if let Err(e) = &chunk {
                        match e {
                            ReadError::Reset(_) => {}
                            ReadError::ConnectionLost(_) => {
                                warn!("Receive stream for remote_id={}, packet id={} errored.  This may mean the connection was closed prematurely: {:?}", remote_id, id, e);
                                break;
                            }
                            ReadError::UnknownStream => {}
                            ReadError::IllegalOrderedRead => {}
                            ReadError::ZeroRttRejected => {}
                        }
                    }
                    match chunk.unwrap() {
                        None => {
                            // TODO: Error
                            debug!(
                                "Receive stream for remote_id={}, packet id={} is finished, got None when reading chunks",
                                remote_id, id
                            );
                            break;
                        }
                        Some(chunk) => {
                            trace!(
                                "Received chunked packets for id={}, length={}",
                                id,
                                chunk.bytes.len()
                            );
                            let bytes;
                            match partial_chunk.take() {
                                None => {
                                    bytes = chunk.bytes;
                                }
                                Some(part) => {
                                    bytes = Bytes::from([part, chunk.bytes].concat());
                                    trace!(
                                        "Concatenated saved part and chunked packet: {:?}",
                                        bytes
                                    );
                                }
                            }

                            // TODO: Make trace log
                            trace!("Received bytes: {:?}", bytes);
                            let boundaries: Vec<usize> = bytes
                                .windows(FRAME_BOUNDARY.len())
                                .enumerate()
                                .filter(|(_, w)| matches!(*w, FRAME_BOUNDARY))
                                .map(|(i, _)| i)
                                .collect();
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
                                            trace!(
                                                "Transmitting received bytes of length {}",
                                                frame.len()
                                            );
                                            // Should never have FRAME_BOUNDARY at this point
                                            tx.send(frame).await.unwrap();
                                        }
                                    }
                                    Some(part) => {
                                        let reconstructed_frame =
                                            Bytes::from([part, frame].concat());
                                        if matches!(reconstructed_frame.as_ref(), FRAME_BOUNDARY) {
                                            error!("Found a dangling FRAME_BOUNDARY in packet frame.  This most likely is a bug in durian")
                                        } else {
                                            trace!(
                                                "Transmitting reconstructed received bytes of length {}",
                                                reconstructed_frame.len()
                                            );
                                            // Remove boundary if at beginning of reconstructed frame
                                            if reconstructed_frame.starts_with(FRAME_BOUNDARY) {
                                                tx.send(reconstructed_frame.slice(
                                                    FRAME_BOUNDARY.len() - 1
                                                        ..reconstructed_frame.len(),
                                                ))
                                                .await
                                                .unwrap();
                                            } else {
                                                tx.send(reconstructed_frame).await.unwrap();
                                            }
                                        }
                                    }
                                }
                                offset = i + FRAME_BOUNDARY.len();
                            }

                            // We got a partial chunk if there were no boundaries found, so the chunk couldn't be
                            // split to be sent to tx, or there is a leftover slice at the end that doesn't have the
                            // ending frame signaling end of a send()
                            if boundaries.is_empty()
                                || (offset + FRAME_BOUNDARY.len() != bytes.len() - 1
                                    || !bytes.ends_with(FRAME_BOUNDARY))
                            {
                                let prefix_part = bytes.slice(offset..bytes.len());
                                match partial_chunk.take() {
                                    None => {
                                        partial_chunk = Some(prefix_part);
                                    }
                                    Some(part) => {
                                        partial_chunk =
                                            Some(Bytes::from([part, prefix_part].concat()))
                                    }
                                }
                            }
                        }
                    }
                }
            };

            let receive_thread: JoinHandle<()> = match runtime.as_ref() {
                None => tokio::spawn(task),
                Some(runtime) => runtime.spawn(task),
            };

            rxs.insert(id, (RwLock::new(rx), receive_thread));
        }

        Ok(rxs)
    }

    /// Returns the source address and Endpoint of this PacketManager as a Tuple (String, Option<Endpoint>)
    ///
    /// # Returns
    /// Source address is the server address if this PacketManager is for a server, else the client address.
    /// Endpoint is listening Endpoint at the source Socket Address
    pub fn get_source(&self) -> &(String, Option<Arc<Endpoint>>) {
        &self.source
    }

    /// Register a [`Packet`] on a `receive` stream/channel, and its associated [`PacketBuilder`]
    ///
    /// # Returns
    /// A [`Result`] containing `()` for success, [`ReceiveError`] if registration failed.
    pub fn register_receive_packet<T: Packet + 'static>(
        &mut self,
        packet_builder: impl PacketBuilder<T> + 'static + Sync + Send,
    ) -> Result<(), ReceiveError> {
        if self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!(
                "Type '{}' was already registered as a Receive packet",
                type_name::<T>()
            )));
        }

        let packet_type_id = TypeId::of::<T>();
        self.receive_packets.insert(self.next_receive_id, packet_type_id);
        self.recv_packet_builders.insert(packet_type_id, Box::new(packet_builder));
        debug!(
            "Registered Receive packet with id={}, type={}",
            self.next_receive_id,
            type_name::<T>()
        );
        self.next_receive_id += 1;
        Ok(())
    }

    /// Register a [`Packet`] on a `send` stream/channel
    ///
    /// # Returns
    /// A [`Result`] containing `()` for success, [`SendError`] if registration failed.
    pub fn register_send_packet<T: Packet + 'static>(&mut self) -> Result<(), SendError> {
        if self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!(
                "Type '{}' was already registered as a Send packet",
                type_name::<T>()
            )));
        }

        self.send_packets.insert(self.next_send_id, TypeId::of::<T>());
        debug!("Registered Send packet with id={}, type={}", self.next_send_id, type_name::<T>());
        self.next_send_id += 1;
        Ok(())
    }

    /// Fetches all received packets from all destination addresses
    ///
    /// This reads from all `receive` channels and deserializes the [`Packets`](`Packet`) requested.  Any channel that
    /// is found disconnected will be removed from the stream queue and a warning will be logged.  If a reading from a
    /// receive channel ran into an unexpected error, this function will stop reading from other channels and return
    /// the error.
    ///
    /// # Type Arguments
    /// * `T: Packet + 'static` - The [`Packet`] type to request
    /// * `U: PacketBuilder<T> + 'static` - The [`PacketBuilder`] for this packet, used to deserialize bytes into
    ///     the [`Packet`] type requested
    ///
    /// # Arguments
    /// * `blocking` - `true` to make this a blocking call, waiting for __ALL__ destination addresses to send at least
    ///     one Packet of type `T`.  `false` will make this non-blocking, and any destination addresses that did not
    ///     send the Packet will return [`None`] paired with it.  __Warning: be careful about making this a blocking
    ///     call, as if any of the Packets don't come from any destination address, it could hang your application__
    ///
    /// # Returns
    /// A [`Result`] containing a [`Vec`] of pair tuples with the first element being the destination socket address,
    /// and the second element will have a `None` if `blocking` was set to `false` and the associated destination address
    /// did not send the Packet type when this call queried for it, or [`Some`] containing a [`Vec`] of the Packets
    /// type `T` that was requested from the associated destination address.  
    ///
    /// [`ReceiveError`] if there was an error fetching received packets.  If a channel was found disconnected, no
    /// Error will be returned with it, but instead it will be removed from the stream queue and output.
    ///
    /// # Panics
    /// If the `PacketManager` was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn received_all<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        &mut self,
        blocking: bool,
    ) -> Result<Vec<(u32, Option<Vec<T>>)>, ReceiveError> {
        self.validate_for_received::<T>(true)?;
        self.update_new_receivers();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => {
                let res = runtime.block_on(async {
                    let mut res = vec![];
                    let mut err: Option<ReceiveError> = None;
                    // If we find any streams are closed, save them to cleanup and close the connections after iterating
                    let mut remote_id_to_close = Vec::new();
                    for (remote_id, rxs) in self.rx.iter() {
                        let received = PacketManager::async_received_helper::<T, U>(
                            blocking,
                            *remote_id,
                            &self.receive_packets,
                            &self.recv_packet_builders,
                            rxs,
                        )
                        .await;

                        match received {
                            Ok(received) => {
                                res.push((*remote_id, received));
                            }
                            Err(e) => match e.error_type {
                                ErrorType::Unexpected => {
                                    err = Some(e);
                                    break;
                                }
                                ErrorType::Disconnected => {
                                    remote_id_to_close.push(*remote_id);
                                }
                            },
                        }
                    }

                    if let Some(e) = err {
                        return (remote_id_to_close, Err(e));
                    }
                    (remote_id_to_close, Ok(res))
                });

                for remote_id in res.0.iter() {
                    warn!("Receive stream for remote_id={} disconnected.  Removing it from the receive queue and continuing as normal.", remote_id);
                    self.close_connection(*remote_id).unwrap_or_else(|_| {
                        panic!("Could not close connection for remote_id={}", remote_id)
                    });
                }

                res.1
            }
        }
    }

    /// Fetches all received packets from all destination addresses
    ///
    /// Same as [`received_all()`](`PacketManager::received_all()`), except it returns a `Future` and can be called
    /// from an async context.
    ///
    /// # Panics
    /// If the `PacketManager` was created via [`new()`](`PacketManager::new()`)
    pub async fn async_received_all<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        &mut self,
        blocking: bool,
    ) -> Result<Vec<(u32, Option<Vec<T>>)>, ReceiveError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_received(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.async_validate_for_received::<T>(true).await?;
        self.async_update_new_receivers().await;
        let mut res = vec![];
        let mut err: Option<ReceiveError> = None;
        // If we find any streams are closed, save them to cleanup and close the connections after iterating
        let mut remote_id_to_close = Vec::new();
        for (remote_id, rxs) in self.rx.iter() {
            let received = PacketManager::async_received_helper::<T, U>(
                blocking,
                *remote_id,
                &self.receive_packets,
                &self.recv_packet_builders,
                rxs,
            )
            .await;

            match received {
                Ok(received) => {
                    res.push((*remote_id, received));
                }
                Err(e) => match e.error_type {
                    ErrorType::Unexpected => {
                        err = Some(e);
                        break;
                    }
                    ErrorType::Disconnected => {
                        remote_id_to_close.push(*remote_id);
                    }
                },
            }
        }

        for remote_id in remote_id_to_close.iter() {
            warn!("Receive stream for remote_id={} disconnected.  Removing it from the receive queue and continuing as normal.", remote_id);
            self.async_close_connection(*remote_id).await.unwrap_or_else(|_| {
                panic!("Could not close connection for remote_id={}", remote_id)
            });
        }

        if let Some(e) = err {
            return Err(e);
        }
        Ok(res)
    }

    /// Fetches all received packets from a single destination address.  This should only be called if there is __only__
    /// one destination address, particularly convenient if this is for a client which connects to a single server.
    ///
    /// This reads from the single `receive` channel and deserializes the [`Packets`](`Packet`) requested.
    ///
    /// # Type Arguments
    /// * `T: Packet + 'static` - The [`Packet`] type to request
    /// * `U: PacketBuilder<T> + 'static` - The [`PacketBuilder`] for this packet, used to deserialize bytes into
    ///     the [`Packet`] type requested
    ///
    /// # Arguments
    /// * `blocking` - `true` to make this a blocking call, waiting for the destination address to send at least
    ///     one Packet of type `T`.  `false` will make this non-blocking, and if destination address did not
    ///     send the Packet, it will return [`None`].  __Warning: be careful about making this a blocking
    ///     call, as if Packets don't arrive exactly as you expect, it could hang your application__
    ///
    /// # Returns
    /// A [`Result`] containing [`None`] if the destination address did not send any Packets of this type, else [`Some`]
    /// containing a [`Vec`] of those received packets.
    ///
    /// [`ReceiveError`] if error occurred fetching received packets.
    ///
    /// # Panics
    /// If the [`PacketManager`] was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn received<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        &mut self,
        blocking: bool,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        self.validate_for_received::<T>(false)?;
        self.update_new_receivers();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_received()?");
            }
            Some(runtime) => runtime.block_on({
                let rxs = self.rx.first().unwrap();
                PacketManager::async_received_helper::<T, U>(
                    blocking,
                    *rxs.0,
                    &self.receive_packets,
                    &self.recv_packet_builders,
                    rxs.1,
                )
            }),
        }
    }

    /// Fetches all received packets from a single destination address.  This should only be called if there is __only__
    /// one destination address, particularly convenient if this is for a client which connects to a single server.
    ///
    /// Same as [`received()`](`PacketManager::received()`), except it returns a `Future` and can be called
    /// from an async context.
    ///
    /// # Panics
    /// If the `PacketManager` was created via [`new()`](`PacketManager::new()`)
    pub async fn async_received<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        &mut self,
        blocking: bool,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_received(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.async_validate_for_received::<T>(false).await?;
        self.async_update_new_receivers().await;
        let rxs = self.rx.first().unwrap();
        PacketManager::async_received_helper::<T, U>(
            blocking,
            *rxs.0,
            &self.receive_packets,
            &self.recv_packet_builders,
            rxs.1,
        )
        .await
    }

    // Assumes does not have more than one client to send to, should be checked by callers
    // TODO: Handle connections dropped, if joinhandle failed, close the connection and return error, etc.
    async fn async_received_helper<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        blocking: bool,
        remote_id: u32,
        receive_packets: &BiMap<u32, TypeId>,
        recv_packet_builders: &HashMap<TypeId, Box<dyn Any + Send + Sync>>,
        rxs: &HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        let packet_type_id = TypeId::of::<T>();
        let id = receive_packets.get_by_right(&packet_type_id).unwrap();
        let (rx_lock, _receive_thread) = rxs.get(id).unwrap();
        let mut rx = rx_lock.write().await;
        let mut res: Vec<T> = Vec::new();
        let packet_builder: &U =
            recv_packet_builders.get(&TypeId::of::<T>()).unwrap().downcast_ref::<U>().unwrap();

        // If blocking, wait for the first packet
        if blocking {
            match rx.recv().await {
                None => {
                    return Err(ReceiveError::new_with_type(
                        format!(
                            "Receiver channel for packet type {} was disconnected",
                            type_name::<T>()
                        ),
                        ErrorType::Disconnected,
                    ));
                }
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
                Err(e) => match e {
                    TryRecvError::Empty => {
                        break;
                    }
                    TryRecvError::Disconnected => {
                        return Err(ReceiveError::new_with_type(
                            format!(
                                "Receiver channel for packet type {} was disconnected",
                                type_name::<T>()
                            ),
                            ErrorType::Disconnected,
                        ));
                    }
                },
            }
        }

        if res.is_empty() {
            return Ok(None);
        }
        debug!(
            "Fetched {} received packets of type={}, id={}, from remote_id={}",
            res.len(),
            type_name::<T>(),
            id,
            remote_id
        );
        Ok(Some(res))
    }

    fn validate_for_received<T: Packet + 'static>(
        &self,
        for_all: bool,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        if !for_all && self.has_more_than_one_remote() {
            return Err(ReceiveError::new(format!("async_received()/received() was called for packet {}, but there is more than one client.  Did you mean to call async_received_all()/received_all()?", type_name::<T>())));
        }

        if !self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!(
                "Type '{}' was never registered!  Did you forget to call register_receive_packet()?",
                type_name::<T>()
            )));
        }
        Ok(None)
    }

    async fn async_validate_for_received<T: Packet + 'static>(
        &self,
        for_all: bool,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        if !for_all && self.async_has_more_than_one_remote().await {
            return Err(ReceiveError::new(format!("async_received()/received() was called for packet {}, but there is more than one client.  Did you mean to call async_received_all()/received_all()?", type_name::<T>())));
        }

        if !self.receive_packets.contains_right(&TypeId::of::<T>()) {
            return Err(ReceiveError::new(format!(
                "Type '{}' was never registered!  Did you forget to call register_receive_packet()?",
                type_name::<T>()
            )));
        }
        Ok(None)
    }

    fn update_new_receivers(&mut self) {
        let mut new_rx_lock = self.new_rxs.blocking_write();
        if !new_rx_lock.is_empty() {
            let new_rx_vec = std::mem::take(&mut *new_rx_lock);
            for (remote_id, val) in new_rx_vec.into_iter() {
                if self.rx.contains_key(&remote_id) {
                    panic!("Receive stream for remote_id={} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", remote_id);
                }
                self.rx.insert(remote_id, val);
            }
        }
    }

    async fn async_update_new_receivers(&mut self) {
        let mut new_rx_lock = self.new_rxs.write().await;
        if !new_rx_lock.is_empty() {
            let new_rx_vec = std::mem::take(&mut *new_rx_lock);
            for (remote_id, val) in new_rx_vec.into_iter() {
                if self.rx.contains_key(&remote_id) {
                    panic!("Receive stream for remote_id={} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", remote_id);
                }
                self.rx.insert(remote_id, val);
            }
        }
    }

    #[inline]
    fn receive_bytes<T: Packet + 'static, U: PacketBuilder<T> + 'static>(
        bytes: Bytes,
        packet_builder: &U,
        res: &mut Vec<T>,
    ) -> Result<(), ReceiveError> {
        if bytes.is_empty() {
            return Err(ReceiveError::new(format!(
                "Received empty bytes for packet type={}!",
                type_name::<T>()
            )));
        }
        debug!("Received packet with id={} for type={}", res.len(), type_name::<T>());
        let packet = match packet_builder.read(bytes) {
            Ok(p) => p,
            Err(e) => {
                let err_msg = format!(
                    "Could not build packet of type={} from bytes: {:?}",
                    type_name::<T>(),
                    e
                );
                error!("{}", err_msg);
                return Err(ReceiveError::new_with_type(err_msg, ErrorType::Unexpected));
            }
        };
        res.push(packet);
        Ok(())
    }

    /// Broadcast a Packet to all destination addresses
    ///
    /// If any `send` channels are disconnected, they will be removed from the send stream queue and a warning will be
    /// logged, and no error will be indicated from this function.  If there was an unexpected error, no further packets
    /// will be sent, and the function will stop to return an error.
    ///
    /// # Arguments
    /// * `packet` - The [`Packet`] to broadcast
    ///
    /// # Returns
    /// A [`Result`] containing `()` if Packet was sent, else [`SendError`].  If a channel was disconnected, it will be
    /// removed from the send stream queue and __no__ error will be returned.
    ///
    /// # Panics
    /// If the [`PacketManager`] was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn broadcast<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(true)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_broadcast()?");
            }
            Some(runtime) => {
                let res = runtime.block_on(async {
                    let mut err: Option<SendError> = None;
                    // If we find any streams are closed, save them to cleanup and close the connections after iterating
                    let mut remote_id_to_close = Vec::new();
                    for (remote_id, send_streams) in self.send_streams.iter() {
                        let sent = PacketManager::async_send_helper::<T>(
                            &packet,
                            *remote_id,
                            &self.send_packets,
                            send_streams,
                        )
                        .await;

                        if let Err(e) = sent {
                            warn!("Ran into error during broadcast(): {}", e);
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    err = Some(e);
                                    break;
                                }
                                ErrorType::Disconnected => {
                                    remote_id_to_close.push(*remote_id);
                                }
                            }
                        }
                    }

                    if let Some(e) = err {
                        return (remote_id_to_close, Err(e));
                    }

                    (remote_id_to_close, Ok(()))
                });

                for remote_id in res.0.iter() {
                    warn!("Send stream for remote_id={} disconnected.  Removing it from the send queue and continuing as normal.", remote_id);
                    self.close_connection(*remote_id).unwrap_or_else(|_| {
                        panic!("Could not close connection for remote_id={}", remote_id)
                    });
                }

                res.1
            }
        }
    }

    /// Broadcast a Packet to all destination addresses
    ///
    /// Same as [`broadcast()`](`PacketManager::broadcast()`), except it returns a `Future` and can be called from
    /// an async context.
    ///
    /// # Panics
    /// If the `PacketManager` was created via [`new()`](`PacketManager::new()`)
    pub async fn async_broadcast<T: Packet + 'static>(
        &mut self,
        packet: T,
    ) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let mut err: Option<SendError> = None;
        // If we find any streams are closed, save them to cleanup and close the connections after iterating
        let mut remote_id_to_close = Vec::new();
        for (remote_id, send_streams) in self.send_streams.iter() {
            let sent = PacketManager::async_send_helper::<T>(
                &packet,
                *remote_id,
                &self.send_packets,
                send_streams,
            )
            .await;

            if let Err(e) = sent {
                warn!("Ran into error during async_broadcast(): {}", e);
                match e.error_type {
                    ErrorType::Unexpected => {
                        err = Some(e);
                        break;
                    }
                    ErrorType::Disconnected => {
                        remote_id_to_close.push(*remote_id);
                    }
                }
            }
        }

        for remote_id in remote_id_to_close.iter() {
            warn!(
                "Send stream for remote_id={} disconnected.  Removing it from the send queue and continuing as normal.",
                remote_id
            );
            self.async_close_connection(*remote_id).await.unwrap_or_else(|_| {
                panic!("Could not close connection for remote_id={}", remote_id)
            });
        }

        if let Some(e) = err {
            return Err(e);
        }

        Ok(())
    }

    /// Send a Packet to the single destination address.  This should __only__ be used if there is __only__ one
    /// destination address, particularly convenient if this is for a client which connects to a single server.
    ///
    /// # Arguments
    /// * `packet` - The [`Packet`] to broadcast
    ///
    /// # Returns
    /// A [`Result`] containing `()` if Packet was sent, else [`SendError`]
    ///
    /// # Panics
    /// If the [`PacketManager`] was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn send<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(false)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_send()?");
            }
            Some(runtime) => {
                let res = runtime.block_on({
                    let first = self.send_streams.first().unwrap();
                    PacketManager::async_send_helper::<T>(
                        &packet,
                        *first.0,
                        &self.send_packets,
                        first.1,
                    )
                });
                if let Err(e) = &res {
                    warn!("Ran into error during async_send(): {}", e);
                }
                res
            }
        }
    }

    /// Send a Packet to the single destination address.  This should __only__ be used if there is __only__ one
    /// destination address, particularly convenient if this is for a client which connects to a single server.
    ///
    /// Same as [`send()`](`PacketManager::send()`)
    ///
    /// # Panics
    /// If the `PacketManager` was created via [`new()`](`PacketManager::new()`)
    pub async fn async_send<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.async_validate_for_send::<T>(false).await?;
        self.async_update_new_senders().await;
        let first = self.send_streams.first().unwrap();
        let res =
            PacketManager::async_send_helper::<T>(&packet, *first.0, &self.send_packets, first.1)
                .await;
        if let Err(e) = &res {
            warn!("Ran into error during async_send(): {}", e);
        }
        res
    }

    /// Send a Packet to a specified destination address.
    ///
    /// # Arguments
    /// * `remote_id` - The destination Remote Id to send the Packet to
    /// * `packet` - The [`Packet`] to broadcast
    ///
    /// # Returns
    /// A [`Result`] containing `()` if Packet was sent, else [`SendError`].  In contrast to [`broadcast()`](`PacketManager::broadcast()`),
    /// if the `send` channel is disconnected, this will return a [`SendError`] with `error_type == [`ErrorType::Disconnect`]`
    ///
    /// # Panics
    /// If the [`PacketManager`] was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn send_to<T: Packet + 'static>(
        &mut self,
        remote_id: u32,
        packet: T,
    ) -> Result<(), SendError> {
        self.validate_for_send::<T>(true)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_send_to()?");
            }
            Some(runtime) => {
                let res = match self.send_streams.get(&remote_id) {
                    None => Err(SendError::new(format!(
                        "Could not find Send stream for remote_id={}",
                        remote_id
                    ))),
                    Some(send_streams) => runtime.block_on(PacketManager::async_send_helper::<T>(
                        &packet,
                        remote_id,
                        &self.send_packets,
                        send_streams,
                    )),
                };

                match res {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        warn!("Ran into error during send_to(): {}", e);
                        match e.error_type {
                            ErrorType::Unexpected => Err(e),
                            ErrorType::Disconnected => {
                                warn!("Send stream for remote_id={} disconnected.  Removing it from the send queue and returning error.", remote_id);
                                self.close_connection(remote_id).unwrap_or_else(|_| {
                                    panic!("Could not close connection for remote_id={}", remote_id)
                                });
                                Err(e)
                            }
                        }
                    }
                }
            }
        }
    }

    /// Send a Packet to a specified destination address.
    ///
    /// Same as [`send_to()`](`PacketManager::send_to()`)
    ///
    /// # Panics
    /// - If the `PacketManager` was created via [`new()`](`PacketManager::new()`)
    pub async fn async_send_to<T: Packet + 'static>(
        &mut self,
        remote_id: u32,
        packet: T,
    ) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let res = match self.send_streams.get(&remote_id) {
            None => Err(SendError::new(format!(
                "Could not find Send stream for remote_id={}",
                remote_id
            ))),
            Some(send_streams) => {
                PacketManager::async_send_helper::<T>(
                    &packet,
                    remote_id,
                    &self.send_packets,
                    send_streams,
                )
                .await
            }
        };

        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Ran into error during async_send_to(): {}", e);
                match e.error_type {
                    ErrorType::Unexpected => Err(e),
                    ErrorType::Disconnected => {
                        warn!("Send stream for remote_id={} disconnected.  Removing it from the send queue and returning error.", remote_id);
                        self.async_close_connection(remote_id).await.unwrap_or_else(|_| {
                            panic!("Could not close connection for remote_id={}", remote_id)
                        });
                        Err(e)
                    }
                }
            }
        }
    }

    fn update_new_senders(&mut self) {
        let mut new_send_stream_lock = self.new_send_streams.blocking_write();
        if !new_send_stream_lock.is_empty() {
            let new_send_streams_vec = std::mem::take(&mut *new_send_stream_lock);
            for (remote_id, val) in new_send_streams_vec.into_iter() {
                if self.send_streams.contains_key(&remote_id) {
                    panic!("Send stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", remote_id);
                }
                self.send_streams.insert(remote_id, val);
            }
        }
    }

    async fn async_update_new_senders(&mut self) {
        let mut new_send_stream_lock = self.new_send_streams.write().await;
        if !new_send_stream_lock.is_empty() {
            let new_send_streams_vec = std::mem::take(&mut *new_send_stream_lock);
            for (remote_id, val) in new_send_streams_vec.into_iter() {
                if self.send_streams.contains_key(&remote_id) {
                    panic!("Send stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", remote_id);
                }
                self.send_streams.insert(remote_id, val);
            }
        }
    }

    // TODO: handle connection dropped
    async fn async_send_helper<T: Packet + 'static>(
        packet: &T,
        remote_id: u32,
        send_packets: &BiMap<u32, TypeId>,
        send_streams: &HashMap<u32, RwLock<SendStream>>,
    ) -> Result<(), SendError> {
        let bytes = packet.as_bytes();
        let packet_type_id = TypeId::of::<T>();
        let id = send_packets.get_by_right(&packet_type_id).unwrap();
        let mut send_stream = send_streams.get(id).unwrap().write().await;
        debug!("Sending bytes to remote_id={} with len {}", remote_id, bytes.len());
        trace!("Sending bytes {:?}", bytes);
        if let Err(e) = send_stream.write_chunk(bytes).await {
            return match e {
                WriteError::ConnectionLost(e) => Err(SendError::new_with_type(
                    format!(
                        "Send stream for remote_id={}, packet id={} is disconnected: {:?}",
                        remote_id, id, e
                    ),
                    ErrorType::Disconnected,
                )),
                _ => Err(SendError::new_with_type(
                    format!(
                        "Send stream for remote_id={}, packet id={} ran into unexpected error: {:?}",
                        remote_id, id, e
                    ),
                    ErrorType::Unexpected,
                )),
            };
        }
        trace!("Sending FRAME_BOUNDARY");
        if let Err(e) = send_stream.write_all(FRAME_BOUNDARY).await {
            return match e {
                WriteError::ConnectionLost(e) => {
                    Err(SendError::new_with_type(
                        format!("Send stream for remote_id={}, packet id={} is disconnected: {:?}", remote_id, id, e),
                        ErrorType::Disconnected,
                    ))
                },
                _ => { Err(SendError::new_with_type(format!("Send stream for remote_id={}, packet id={} ran into unexpected error when writing frame boundary: {:?}", remote_id, id, e), ErrorType::Unexpected)) }
            };
        }
        debug!("Sent packet to remote_id={} with id={}, type={}", remote_id, id, type_name::<T>());
        Ok(())
    }

    /// Returns the number of clients attached to this server.  This will always return `0` if on a client side.
    pub fn get_num_clients(&self) -> u32 {
        self.remote_connections.blocking_read().len() as u32
    }

    /// Returns the number of clients attached to this server.  This will always return `0` if on a client side.
    pub async fn async_get_num_clients(&self) -> u32 {
        self.remote_connections.read().await.len() as u32
    }

    /// Returns the client connections in tuples of (remote Id, Socket Address)
    pub fn get_remote_connections(&self) -> Vec<(u32, String)> {
        self.remote_connections
            .blocking_read()
            .iter()
            .map(|(remote_id, con)| (*remote_id, con.0.clone()))
            .collect()
    }

    /// Returns the client connections in tuples of (remote Id, Socket Address)
    pub async fn async_get_remote_connections(&self) -> Vec<(u32, String)> {
        self.remote_connections
            .read()
            .await
            .iter()
            .map(|(remote_id, con)| (*remote_id, con.0.clone()))
            .collect()
    }

    /// Returns the Socket IP Address for a remote Id.  Remote Ids start from 0 and are incremented in the order each
    /// remote client is connected to the current connection.  This can be helpful to associated some sort of numerical ID with clients.
    ///
    /// # Returns
    /// [`None`] if the remote Id was not a valid Id in remote connections.  [`Some`] containing the Remote address
    /// for the requested remote Id.
    pub fn get_remote_address(&self, remote_id: u32) -> Option<String> {
        let remote_connections = self.remote_connections.blocking_read();
        if !remote_connections.contains_key(&remote_id) {
            return None;
        }
        Some(remote_connections.get(&remote_id).unwrap().0.clone())
    }

    /// Returns the Socket IP Address for a remote Id.  Remote Ids start from 0 and are incremented in the order each
    /// remote client is connected to the current connection.  This can be helpful to associated some sort of numerical ID with clients.
    ///
    /// Same as [`get_remote_id`](`PacketManager::get_remote_id()`), except returns a `Future` and can be called from
    /// an async context.
    pub async fn async_get_remote_address(&self, remote_id: u32) -> Option<String> {
        let client_connections = self.remote_connections.read().await;
        if !client_connections.contains_key(&remote_id) {
            return None;
        }
        Some(client_connections.get(&remote_id).unwrap().0.clone())
    }

    /// Close the connection with a destination remote Id
    ///
    /// __Warning: this will forcefully close the connection, causing any Packets in stream queues that haven't already
    /// sent over the wire to be dropped.__
    ///
    /// Cleans up resources associated with the connection internal to [`PacketManager`].  A Packet will be sent to the
    /// destination address detailing the reason of connection closure.
    ///
    /// # Returns
    /// A [`Result`] containing `()` on success, [`CloseError`] if error occurred while closing the connection.  Note:
    /// connection resources may be partially closed.
    pub fn close_connection(&mut self, remote_id: u32) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.update_new_senders();
        self.update_new_receivers();
        let mut client_connections = self.remote_connections.blocking_write();
        if let Some((addr, conn)) = client_connections.get(&remote_id) {
            debug!(
                "Forcefully closing connection for remote addr={}, remote_id={}",
                addr, remote_id
            );
            conn.close(
                VarInt::from(1_u8),
                "PacketManager::close_connection() called for this connection".as_bytes(),
            );
        }
        client_connections.remove(&remote_id);
        // Note: We don't gracefully shut down the streams individually, as they should shut down on their own eventually
        self.send_streams.remove(&remote_id);
        self.rx.remove(&remote_id);
        Ok(())
    }

    /// Close the connection with a destination Remote Id
    ///
    /// __Warning: this will forcefully close the connection, causing any Packets in stream queues that haven't already
    /// sent over the wire to be dropped.__
    ///
    /// Same as [`close_connection()`](`PacketManager::close_connection()`), except returns a Future and can be called
    /// from an async context.
    pub async fn async_close_connection(&mut self, remote_id: u32) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.async_update_new_senders().await;
        self.async_update_new_receivers().await;
        let mut client_connections = self.remote_connections.write().await;
        if let Some((addr, conn)) = client_connections.get(&remote_id) {
            debug!(
                "Forcefully closing connection for remote addr={}, remote_id={}",
                addr, remote_id
            );
            conn.close(
                VarInt::from(1_u8),
                "PacketManager::close_connection() called for this connection".as_bytes(),
            );
        }
        client_connections.remove(&remote_id);
        // Note: We don't gracefully shut down the streams individually, as they should shut down on their own eventually
        self.send_streams.remove(&remote_id);
        self.rx.remove(&remote_id);
        Ok(())
    }

    /// Closes the connection with a destination Remote Id after flushing all send streams gracefully.
    ///
    /// This differs from [`close_connection()`](`PacketManager::close_connection()`) in that it gracefully flushes all
    /// send streams, and waits for acks on each before closing the connection.
    ///
    /// Cleans up resources associated with the connection internal to [`PacketManager`].  A Packet will be sent to the
    /// destination address detailing the reason of connection closure.
    ///
    /// # Returns
    /// A [`Result`] containing `()` on success, [`CloseError`] if error occurred while closing the connection.  Note:
    /// connection resources may be partially closed.
    pub fn finish_connection(&mut self, remote_id: u32) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.update_new_senders();
        self.update_new_receivers();
        // Finish all send streams
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_finish_connection()?");
            }
            Some(runtime) => runtime.block_on(async {
                if let Some(send_streams) = self.send_streams.get(&remote_id) {
                    for send_stream in send_streams.values() {
                        if let Err(e) = send_stream.write().await.finish().await {
                            debug!(
                                "Could not finish send stream for remote_id={}.  Continuing to close connection: {:?}",
                                remote_id, e
                            );
                        }
                    }
                }
            }),
        };
        self.send_streams.remove(&remote_id);
        let mut client_connections = self.remote_connections.blocking_write();
        if let Some((addr, conn)) = client_connections.get(&remote_id) {
            debug!("Finishing connection for remote addr={}, remote_id={}", addr, remote_id);
            conn.close(
                VarInt::from(1_u8),
                "PacketManager::finish_connection() called for this connection".as_bytes(),
            );
        }
        client_connections.remove(&remote_id);
        self.rx.remove(&remote_id);
        Ok(())
    }

    /// Closes the connection with a destination Remote Id after flushing all send streams gracefully.
    ///
    /// Same as [`finish_connection()`](`PacketManager::finish_connection()`), except returns a Future and can be called
    /// from an async context.
    pub async fn async_finish_connection(&mut self, remote_id: u32) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.async_update_new_senders().await;
        self.async_update_new_receivers().await;
        // Finish all send streams
        if let Some(send_streams) = self.send_streams.get(&remote_id) {
            for send_stream in send_streams.values() {
                if let Err(e) = send_stream.write().await.finish().await {
                    debug!("Could not finish send stream for remote_id={}.  Continuing to close connection: {:?}", remote_id, e);
                }
            }
        }
        self.send_streams.remove(&remote_id);
        let mut client_connections = self.remote_connections.write().await;
        if let Some((addr, conn)) = client_connections.get(&remote_id) {
            debug!("Finishing connection for remote addr={}, remote_id={}", addr, remote_id);
            conn.close(
                VarInt::from(1_u8),
                "PacketManager::finish_connection() called for this connection".as_bytes(),
            );
        }
        client_connections.remove(&remote_id);
        self.rx.remove(&remote_id);
        Ok(())
    }

    fn validate_for_send<T: Packet + 'static>(&self, for_all: bool) -> Result<(), SendError> {
        if !for_all && self.has_more_than_one_remote() {
            return Err(SendError::new(format!("send() was called for packet {}, but there is more than one client.  Did you mean to call broadcast()?", type_name::<T>())));
        }

        if !self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!(
                "Type '{}' was never registered!  Did you forget to call register_send_packet()?",
                type_name::<T>()
            )));
        }

        Ok(())
    }

    async fn async_validate_for_send<T: Packet + 'static>(
        &self,
        for_all: bool,
    ) -> Result<(), SendError> {
        if !for_all && self.async_has_more_than_one_remote().await {
            return Err(SendError::new(format!("async_send() was called for packet {}, but there is more than one client.  Did you mean to call async_broadcast()?", type_name::<T>())));
        }

        if !self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!(
                "Type '{}' was never registered!  Did you forget to call register_send_packet()?",
                type_name::<T>()
            )));
        }

        Ok(())
    }

    // Either we are a single client talking to a single server, or a server talking to potentially multiple clients
    #[inline]
    fn has_more_than_one_remote(&self) -> bool {
        self.remote_connections.blocking_read().len() > 1
    }

    #[inline]
    async fn async_has_more_than_one_remote(&self) -> bool {
        self.remote_connections.read().await.len() > 1
    }
}
