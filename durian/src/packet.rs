#![allow(clippy::type_complexity)]

use std::any::{Any, type_name, TypeId};
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
use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;

use crate::{CloseError, ConnectionError, ErrorType, ReceiveError, SendError};
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
    receive_packets: BiMap<u32, TypeId>,
    send_packets: BiMap<u32, TypeId>,
    recv_packet_builders: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
    // DenseSlotMap for the below so we can iterate fast, while not degrading insert/remove much
    send_streams: IndexMap<String, HashMap<u32, RwLock<SendStream>>>,
    rx: IndexMap<String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>>,
    new_send_streams: Arc<RwLock<Vec<(String, HashMap<u32, RwLock<SendStream>>)>>>,
    new_rxs: Arc<RwLock<Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
    // Endpoint and Connection structs moved to the struct fields to prevent closing connections
    // by dropping.
    // This is also used to count the number of clients when broadcasting
    // Client addr to (index in above Vecs AKA a client ID, Connection)
    client_connections: Arc<RwLock<HashMap<String, (u32, Connection)>>>,
    server_connection: Option<(String, Connection)>,
    // (Socket Address, Endpoint)
    source: (String, Option<Arc<Endpoint>>),
    next_receive_id: u32,
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
    /// The true idle timeout is the minimum of this and the peer's own max idle timeout. Defaults to None which
    /// represents an infinite timeout.
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
    pub fn new<S: Into<String>>(addr: S, server_addr: S, num_receive_streams: u32, num_send_streams: u32) -> Self {
        ClientConfig {
            addr: addr.into(),
            server_addr: server_addr.into(),
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: None,
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
    /// The true idle timeout is the minimum of this and the peer's own max idle timeout. Defaults to None which
    /// represents an infinite timeout.
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
        let mut expected_clients = if let Some(expected) = total_expected_clients { expected } else { 0 };
        expected_clients = if expected_clients > wait_for_clients {
            expected_clients - wait_for_clients
        } else {
            0
        };
        ServerConfig {
            addr: addr.into(),
            wait_for_clients,
            total_expected_clients,
            max_concurrent_accepts: max(
                num_cpus::get() as u32,
                expected_clients,
            ),
            num_receive_streams,
            num_send_streams,
            keep_alive_interval: None,
            idle_timeout: None,
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
            idle_timeout: None,
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
            idle_timeout: None,
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
                client_connections: Arc::new(RwLock::new(HashMap::new())),
                server_connection: None,
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
            client_connections: Arc::new(RwLock::new(HashMap::new())),
            server_connection: None,
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
                self.validate_connection_prereqs(client_config.num_receive_streams, client_config.num_send_streams)?;
                self.source.0 = client_config.addr.clone();
                runtime.block_on(PacketManager::init_client_helper(
                    client_config,
                    &self.runtime,
                    &self.new_rxs,
                    &self.new_send_streams,
                    &mut self.server_connection,
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
    pub async fn async_init_client(&mut self, client_config: ClientConfig) -> Result<(), ConnectionError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_init_client(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.validate_connection_prereqs(client_config.num_receive_streams, client_config.num_send_streams)?;
        self.source.0 = client_config.addr.clone();
        PacketManager::init_client_helper(
            client_config,
            &self.runtime,
            &self.new_rxs,
            &self.new_send_streams,
            &mut self.server_connection,
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
                self.validate_connection_prereqs(server_config.num_receive_streams, server_config.num_send_streams)?;
                self.source.0 = server_config.addr.clone();
                runtime.block_on(PacketManager::init_server_helper(
                    server_config,
                    &self.runtime,
                    &self.new_rxs,
                    &self.new_send_streams,
                    &self.client_connections,
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
    pub async fn async_init_server(&mut self, server_config: ServerConfig) -> Result<(), ConnectionError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_init_server(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.validate_connection_prereqs(server_config.num_receive_streams, server_config.num_send_streams)?;
        self.source.0 = server_config.addr.clone();
        PacketManager::init_server_helper(
            server_config,
            &self.runtime,
            &self.new_rxs,
            &self.new_send_streams,
            &self.client_connections,
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
        new_rxs: &Arc<RwLock<Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
        new_send_streams: &Arc<RwLock<Vec<(String, HashMap<u32, RwLock<SendStream>>)>>>,
        client_connections: &Arc<RwLock<HashMap<String, (u32, Connection)>>>,
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
            if client_connections.read().await.contains_key(&addr.to_string()) {
                panic!("[server] Client with addr={} was already connected", addr);
            }
            println!("[server] connection accepted: addr={}", conn.remote_address());
            let (server_send_streams, recv_streams) = PacketManager::open_streams_for_connection(
                addr.to_string(),
                &conn,
                num_receive_streams,
                num_send_streams,
            )
            .await?;
            let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, runtime.as_ref())?;
            new_rxs.write().await.push((addr.to_string(), res));
            new_send_streams.write().await.push((addr.to_string(), server_send_streams));
            client_connections.write().await.insert(addr.to_string(), (i, conn));
        }

        // TODO: There can be any number of client connections at the same time.  This current blocks on accepting a
        // single connection at a time, which can become a huge bottleneck
        if server_config.total_expected_clients.is_none()
            || server_config.total_expected_clients.unwrap() > server_config.wait_for_clients
        {
            let mut client_id = server_config.wait_for_clients;

            // TODO: save this value
            for i in 0..server_config.max_concurrent_accepts {
                debug!("Spinning up client connection accept thread #{}", i);

                let client_connections = client_connections.clone();
                let arc_send_streams = new_send_streams.clone();
                let arc_rx = new_rxs.clone();
                let arc_runtime = Arc::clone(runtime);
                let endpoint = Arc::clone(&endpoint);

                let accept_client_task = async move {
                    match server_config.total_expected_clients {
                        None => loop {
                            debug!("[server] Waiting for more clients...");
                            let incoming_conn = endpoint.accept().await.unwrap();
                            let conn = incoming_conn.await?;
                            let addr = conn.remote_address();
                            if client_connections.read().await.contains_key(&addr.to_string()) {
                                panic!("[server] Client with addr={} was already connected", addr);
                            }
                            debug!("[server] connection accepted: addr={}", conn.remote_address());
                            let (send_streams, recv_streams) = PacketManager::open_streams_for_connection(
                                addr.to_string(),
                                &conn,
                                num_receive_streams,
                                num_send_streams,
                            )
                            .await?;
                            let res = PacketManager::spawn_receive_thread(
                                &addr.to_string(),
                                recv_streams,
                                arc_runtime.as_ref(),
                            )?;
                            arc_rx.write().await.push((addr.to_string(), res));
                            arc_send_streams.write().await.push((addr.to_string(), send_streams));
                            let mut client_connections = client_connections.write().await;
                            client_connections.insert(addr.to_string(), (client_id, conn));
                            client_id += 1;
                        },
                        Some(expected_num_clients) => {
                            for i in 0..(expected_num_clients - server_config.wait_for_clients) {
                                debug!("[server] Waiting for client #{}", i + server_config.wait_for_clients);
                                let incoming_conn = endpoint.accept().await.unwrap();
                                let conn = incoming_conn.await?;
                                let addr = conn.remote_address();
                                if client_connections.read().await.contains_key(&addr.to_string()) {
                                    panic!("[server] Client with addr={} was already connected", addr);
                                }
                                debug!("[server] connection accepted: addr={}", conn.remote_address());
                                let (send_streams, recv_streams) = PacketManager::open_streams_for_connection(
                                    addr.to_string(),
                                    &conn,
                                    num_receive_streams,
                                    num_send_streams,
                                )
                                .await?;
                                let res = PacketManager::spawn_receive_thread(
                                    &addr.to_string(),
                                    recv_streams,
                                    arc_runtime.as_ref(),
                                )?;
                                arc_rx.write().await.push((addr.to_string(), res));
                                arc_send_streams.write().await.push((addr.to_string(), send_streams));
                                let mut client_connections = client_connections.write().await;
                                client_connections.insert(addr.to_string(), (client_id, conn));
                                client_id += 1;
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
        new_rxs: &Arc<RwLock<Vec<(String, HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>)>>>,
        new_send_streams: &Arc<RwLock<Vec<(String, HashMap<u32, RwLock<SendStream>>)>>>,
        server_connection: &mut Option<(String, Connection)>,
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
            addr.to_string(),
            &conn,
            client_config.num_receive_streams,
            client_config.num_send_streams,
        )
        .await?;
        let res = PacketManager::spawn_receive_thread(&addr.to_string(), recv_streams, runtime.as_ref())?;
        new_rxs.write().await.push((addr.to_string(), res));
        new_send_streams.write().await.push((addr.to_string(), client_send_streams));
        let _ = server_connection.insert((addr.to_string(), conn));
        Ok(())
    }

    async fn open_streams_for_connection(
        addr: String,
        conn: &Connection,
        num_incoming_streams: u32,
        num_outgoing_streams: u32,
    ) -> Result<(HashMap<u32, RwLock<SendStream>>, HashMap<u32, RecvStream>), Box<dyn Error>> {
        let mut send_streams = HashMap::new();
        let mut recv_streams = HashMap::new();
        // Note: Packets are not sent immediately upon the write.  The thread needs to be kept
        // open so that the packets can actually be sent over the wire to the client.
        for i in 0..num_outgoing_streams {
            trace!("Opening outgoing stream for addr={} packet id={}", addr, i);
            let mut send_stream = conn.open_uni().await?;
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

    fn spawn_receive_thread<S: Into<String>>(
        addr: S,
        recv_streams: HashMap<u32, RecvStream>,
        runtime: &Option<Runtime>,
    ) -> Result<HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>, Box<dyn Error>> {
        let mut rxs = HashMap::new();
        let addr = addr.into();
        trace!("Spawning receive thread for addr={} for {} ids", addr, recv_streams.len());
        for (id, mut recv_stream) in recv_streams.into_iter() {
            let (tx, rx) = mpsc::channel(100);

            let addr_clone = addr.clone();
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
                                warn!("Receive stream for addr={}, packet id={} errored.  This may mean the connection was closed prematurely: {:?}", addr_clone, id, e);
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
                                "Receive stream for addr={}, packet id={} is finished, got None when reading chunks",
                                addr_clone, id
                            );
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
                                            trace!("Transmitting received bytes of length {}", frame.len());
                                            tx.send(frame).await.unwrap();
                                        }
                                    }
                                    Some(part) => {
                                        let reconstructed_frame = Bytes::from([part, frame].concat());
                                        if matches!(reconstructed_frame.as_ref(), FRAME_BOUNDARY) {
                                            error!("Found a dangling FRAME_BOUNDARY in packet frame.  This most likely is a bug in durian")
                                        } else {
                                            trace!(
                                                "Transmitting reconstructed received bytes of length {}",
                                                reconstructed_frame.len()
                                            );
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
                                    Some(part) => partial_chunk = Some(Bytes::from([part, prefix_part].concat())),
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
        debug!("Registered Receive packet with id={}, type={}", self.next_receive_id, type_name::<T>());
        self.next_receive_id += 1;
        Ok(())
    }

    /// Register a [`Packet`] on a `send` stream/channel
    ///
    /// # Returns
    /// A [`Result`] containing `()` for success, [`SendError`] if registration failed.
    pub fn register_send_packet<T: Packet + 'static>(&mut self) -> Result<(), SendError> {
        if self.send_packets.contains_right(&TypeId::of::<T>()) {
            return Err(SendError::new(format!("Type '{}' was already registered as a Send packet", type_name::<T>())));
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
    ) -> Result<Vec<(String, Option<Vec<T>>)>, ReceiveError> {
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
                    let mut addr_to_close = Vec::new();
                    for (addr, rxs) in self.rx.iter() {
                        let received = PacketManager::async_received_helper::<T, U>(
                            blocking,
                            addr,
                            &self.receive_packets,
                            &self.recv_packet_builders,
                            rxs,
                        )
                        .await;

                        match received {
                            Ok(received) => {
                                res.push((addr.clone(), received));
                            }
                            Err(e) => match e.error_type {
                                ErrorType::Unexpected => {
                                    err = Some(e);
                                    break;
                                }
                                ErrorType::Disconnected => {
                                    addr_to_close.push(addr.clone());
                                }
                            },
                        }
                    }

                    if let Some(e) = err {
                        return (addr_to_close, Err(e));
                    }
                    (addr_to_close, Ok(res))
                });

                for addr in res.0.iter() {
                    warn!("Receive stream for addr={} disconnected.  Removing it from the receive queue and continuing as normal.", addr);
                    self.close_connection(addr)
                        .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr));
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
    ) -> Result<Vec<(String, Option<Vec<T>>)>, ReceiveError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_received(), make sure you create the PacketManager using new_for_async(), not new()");
        }
        self.async_validate_for_received::<T>(true).await?;
        self.async_update_new_receivers().await;
        let mut res = vec![];
        let mut err: Option<ReceiveError> = None;
        // If we find any streams are closed, save them to cleanup and close the connections after iterating
        let mut addr_to_close = Vec::new();
        for (addr, rxs) in self.rx.iter() {
            let received = PacketManager::async_received_helper::<T, U>(
                blocking,
                addr,
                &self.receive_packets,
                &self.recv_packet_builders,
                rxs,
            )
            .await;

            match received {
                Ok(received) => {
                    res.push((addr.clone(), received));
                }
                Err(e) => match e.error_type {
                    ErrorType::Unexpected => {
                        err = Some(e);
                        break;
                    }
                    ErrorType::Disconnected => {
                        addr_to_close.push(addr.clone());
                    }
                },
            }
        }

        for addr in addr_to_close.iter() {
            warn!("Receive stream for addr={} disconnected.  Removing it from the receive queue and continuing as normal.", addr);
            self.async_close_connection(addr)
                .await
                .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr));
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
                    rxs.0,
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
            rxs.0,
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
        addr: &String,
        receive_packets: &BiMap<u32, TypeId>,
        recv_packet_builders: &HashMap<TypeId, Box<dyn Any + Send + Sync>>,
        rxs: &HashMap<u32, (RwLock<Receiver<Bytes>>, JoinHandle<()>)>,
    ) -> Result<Option<Vec<T>>, ReceiveError> {
        let packet_type_id = TypeId::of::<T>();
        let id = receive_packets.get_by_right(&packet_type_id).unwrap();
        let (rx_lock, _receive_thread) = rxs.get(id).unwrap();
        let mut rx = rx_lock.write().await;
        let mut res: Vec<T> = Vec::new();
        let packet_builder: &U = recv_packet_builders.get(&TypeId::of::<T>()).unwrap().downcast_ref::<U>().unwrap();

        // If blocking, wait for the first packet
        if blocking {
            match rx.recv().await {
                None => {
                    return Err(ReceiveError::new_with_type(
                        format!("Receiver channel for packet type {} was disconnected", type_name::<T>()),
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
                            format!("Receiver channel for packet type {} was disconnected", type_name::<T>()),
                            ErrorType::Disconnected,
                        ));
                    }
                },
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
            for (addr, val) in new_rx_vec.into_iter() {
                if self.rx.contains_key(&addr) {
                    panic!("Receive stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", addr);
                }
                self.rx.insert(addr, val);
            }
        }
    }

    async fn async_update_new_receivers(&mut self) {
        let mut new_rx_lock = self.new_rxs.write().await;
        if !new_rx_lock.is_empty() {
            let new_rx_vec = std::mem::take(&mut *new_rx_lock);
            for (addr, val) in new_rx_vec.into_iter() {
                if self.rx.contains_key(&addr) {
                    panic!("Receive stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", addr);
                }
                self.rx.insert(addr, val);
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
            return Err(ReceiveError::new(format!("Received empty bytes for packet type={}!", type_name::<T>())));
        }
        debug!("Received packet with id={} for type={}", res.len(), type_name::<T>());
        let packet = match packet_builder.read(bytes) {
            Ok(p) => p,
            Err(e) => {
                let err_msg = format!("Could not build packet of type={} from bytes: {:?}", type_name::<T>(), e);
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
                    let mut addr_to_close = Vec::new();
                    for (addr, send_streams) in self.send_streams.iter() {
                        let sent =
                            PacketManager::async_send_helper::<T>(&packet, addr, &self.send_packets, send_streams)
                                .await;

                        if let Err(e) = sent {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    err = Some(e);
                                    break;
                                }
                                ErrorType::Disconnected => {
                                    addr_to_close.push(addr.clone());
                                }
                            }
                        }
                    }

                    if let Some(e) = err {
                        return (addr_to_close, Err(e));
                    }

                    (addr_to_close, Ok(()))
                });

                for addr in res.0.iter() {
                    warn!("Send stream for addr={} disconnected.  Removing it from the send queue and continuing as normal.", addr);
                    self.close_connection(addr)
                        .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr));
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
    pub async fn async_broadcast<T: Packet + 'static>(&mut self, packet: T) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let mut err: Option<SendError> = None;
        // If we find any streams are closed, save them to cleanup and close the connections after iterating
        let mut addr_to_close = Vec::new();
        for (addr, send_streams) in self.send_streams.iter() {
            let sent = PacketManager::async_send_helper::<T>(&packet, addr, &self.send_packets, send_streams).await;

            if let Err(e) = sent {
                match e.error_type {
                    ErrorType::Unexpected => {
                        err = Some(e);
                        break;
                    }
                    ErrorType::Disconnected => {
                        addr_to_close.push(addr.clone());
                    }
                }
            }
        }

        for addr in addr_to_close.iter() {
            warn!(
                "Send stream for addr={} disconnected.  Removing it from the send queue and continuing as normal.",
                addr
            );
            self.async_close_connection(addr)
                .await
                .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr));
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
            Some(runtime) => runtime.block_on({
                let first = self.send_streams.first().unwrap();
                PacketManager::async_send_helper::<T>(&packet, first.0, &self.send_packets, first.1)
            }),
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
        PacketManager::async_send_helper::<T>(&packet, first.0, &self.send_packets, first.1).await
    }

    /// Send a Packet to a specified destination address.
    ///
    /// # Arguments
    /// * `addr` - The destination Socket address to send the Packet to
    /// * `packet` - The [`Packet`] to broadcast
    ///
    /// # Returns
    /// A [`Result`] containing `()` if Packet was sent, else [`SendError`].  In contrast to [`broadcast()`](`PacketManager::broadcast()`),
    /// if the `send` channel is disconnected, this will return a [`SendError`] with `error_type == [`ErrorType::Disconnect`]`
    ///
    /// # Panics
    /// If the [`PacketManager`] was created via [`new_for_async()`](`PacketManager::new_for_async()`)
    pub fn send_to<T: Packet + 'static>(&mut self, addr: impl Into<String>, packet: T) -> Result<(), SendError> {
        self.validate_for_send::<T>(true)?;
        self.update_new_senders();
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_send_to()?");
            }
            Some(runtime) => {
                let addr = addr.into();
                let res = match self.send_streams.get(&addr) {
                    None => Err(SendError::new(format!("Could not find Send stream for address {}", addr))),
                    Some(send_streams) => runtime.block_on(PacketManager::async_send_helper::<T>(
                        &packet,
                        &addr,
                        &self.send_packets,
                        send_streams,
                    )),
                };

                match res {
                    Ok(_) => Ok(()),
                    Err(e) => match e.error_type {
                        ErrorType::Unexpected => Err(e),
                        ErrorType::Disconnected => {
                            let addr_clone = addr.clone();
                            warn!("Send stream for addr={} disconnected.  Removing it from the send queue and returning error.", addr);
                            self.close_connection(addr)
                                .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr_clone));
                            Err(e)
                        }
                    },
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
        addr: impl Into<String>,
        packet: T,
    ) -> Result<(), SendError> {
        if self.runtime.is_some() {
            panic!("PacketManager has a runtime instance associated with it.  If you are using async_send(), make sure you create the PacketManager using new_async(), not new()");
        }
        self.async_validate_for_send::<T>(true).await?;
        self.async_update_new_senders().await;
        let addr = addr.into();
        let res = match self.send_streams.get(&addr) {
            None => Err(SendError::new(format!("Could not find Send stream for address {}", addr))),
            Some(send_streams) => {
                PacketManager::async_send_helper::<T>(&packet, &addr, &self.send_packets, send_streams).await
            }
        };

        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                match e.error_type {
                    ErrorType::Unexpected => Err(e),
                    ErrorType::Disconnected => {
                        let addr_clone = addr.clone();
                        warn!("Send stream for addr={} disconnected.  Removing it from the send queue and returning error.", addr);
                        self.async_close_connection(addr)
                            .await
                            .unwrap_or_else(|_| panic!("Could not close connection for addr={}", addr_clone));
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
            for (addr, val) in new_send_streams_vec.into_iter() {
                if self.send_streams.contains_key(&addr) {
                    panic!("Send stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", addr);
                }
                self.send_streams.insert(addr, val);
            }
        }
    }

    async fn async_update_new_senders(&mut self) {
        let mut new_send_stream_lock = self.new_send_streams.write().await;
        if !new_send_stream_lock.is_empty() {
            let new_send_streams_vec = std::mem::take(&mut *new_send_stream_lock);
            for (addr, val) in new_send_streams_vec.into_iter() {
                if self.send_streams.contains_key(&addr) {
                    panic!("Send stream for {} already existed!  There cannot be multiple connections between the same Socket addresses for the same protocol.", addr);
                }
                self.send_streams.insert(addr, val);
            }
        }
    }

    // TODO: handle connection dropped
    async fn async_send_helper<T: Packet + 'static>(
        packet: &T,
        addr: &String,
        send_packets: &BiMap<u32, TypeId>,
        send_streams: &HashMap<u32, RwLock<SendStream>>,
    ) -> Result<(), SendError> {
        let bytes = packet.as_bytes();
        let packet_type_id = TypeId::of::<T>();
        let id = send_packets.get_by_right(&packet_type_id).unwrap();
        let mut send_stream = send_streams.get(id).unwrap().write().await;
        debug!("Sending bytes to {} with len {}", addr, bytes.len());
        trace!("Sending bytes {:?}", bytes);
        if let Err(e) = send_stream.write_chunk(bytes).await {
            return match e {
                WriteError::ConnectionLost(e) => Err(SendError::new_with_type(
                    format!("Send stream for addr={}, packet id={} is disconnected: {:?}", addr, id, e),
                    ErrorType::Disconnected,
                )),
                _ => Err(SendError::new_with_type(
                    format!("Send stream for addr={}, packet id={} ran into unexpected error: {:?}", addr, id, e),
                    ErrorType::Unexpected,
                )),
            };
        }
        trace!("Sending FRAME_BOUNDARY");
        if let Err(e) = send_stream.write_all(FRAME_BOUNDARY).await {
            return match e {
                WriteError::ConnectionLost(e) => {
                    Err(SendError::new_with_type(
                        format!("Send stream for addr={}, packet id={} is disconnected: {:?}", addr, id, e),
                        ErrorType::Disconnected,
                    ))
                },
                _ => { Err(SendError::new_with_type(format!("Send stream for addr={}, packet id={} ran into unexpected error when writing frame boundary: {:?}", addr, id, e), ErrorType::Unexpected)) }
            };
        }
        debug!("Sent packet to {} with id={}, type={}", addr, id, type_name::<T>());
        Ok(())
    }

    /// Returns the number of clients attached to this server.  This will always return `0` if on a client side.
    pub fn get_num_clients(&self) -> u32 {
        self.client_connections.blocking_read().len() as u32
    }

    /// Returns the number of clients attached to this server.  This will always return `0` if on a client side.
    pub async fn async_get_num_clients(&self) -> u32 {
        self.client_connections.read().await.len() as u32
    }
    
    /// Returns the client connections in tuples of (client SocketAddress, client ID)
    pub fn get_client_connections(&self) -> Vec<(String, u32)> {
        self.client_connections.blocking_read().iter().map(|(addr, con)| (addr.clone(), con.0)).collect()
    }

    /// Returns the client connections in tuples of (client SocketAddress, client ID)
    pub async fn async_get_client_connections(&self) -> Vec<(String, u32)> {
        self.client_connections.read().await.iter().map(|(addr, con)| (addr.clone(), con.0)).collect()
    }

    /// Returns the `Client ID` for a Socket address.  Client IDs start from 0 and are incremented in the order each
    /// client is connected to the server.  This can be helpful to associated some sort of numerical ID with clients.
    ///
    /// # Returns
    /// [`None`] if the client address was not a valid address in client connections.  [`Some`] containing the Client ID
    /// for the requested client Socket address.
    ///
    /// # Panics
    /// If called from a client side
    pub fn get_client_id<S: Into<String>>(&self, addr: S) -> Option<u32> {
        let client_connections = self.client_connections.blocking_read();
        let addr = addr.into();
        if client_connections.is_empty() {
            panic!("get_client_id() should only be called on the server side for a valid client Socket address");
        }
        if !client_connections.contains_key(&addr) {
            return None;
        }
        Some(client_connections.get(&addr).unwrap().0)
    }

    /// Returns the `Client ID` for a Socket address.  Client IDs start from 0 and are incremented in the order each
    /// client is connected to the server.  This can be helpful to associated some sort of numerical ID with clients.
    ///
    /// Same as [`get_client_id`](`PacketManager::get_client_id()`), except returns a `Future` and can be called from
    /// an async context.
    pub async fn async_get_client_id<S: Into<String>>(&self, addr: S) -> Option<u32> {
        let client_connections = self.client_connections.read().await;
        let addr = addr.into();
        if client_connections.is_empty() {
            panic!("get_client_id() should only be called on the server side for a valid client Socket address");
        }
        if !client_connections.contains_key(&addr) {
            return None;
        }
        Some(client_connections.get(&addr).unwrap().0)
    }

    /// Close the connection with a destination Socket address
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
    pub fn close_connection<S: Into<String>>(&mut self, addr: S) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.update_new_senders();
        self.update_new_receivers();
        let addr = addr.into();
        let mut client_connections = self.client_connections.blocking_write();
        if let Some((id, conn)) = client_connections.get(&addr) {
            debug!("Forcefully closing connection for client addr={}, id={}", addr, id);
            conn.close(VarInt::from(1_u8), "PacketManager::close_connection() called for this connection".as_bytes());
        }
        client_connections.remove(&addr);
        // Note: We don't gracefully shut down the streams individually, as they should shut down on their own eventually
        self.send_streams.remove(&addr);
        self.rx.remove(&addr);
        Ok(())
    }

    /// Close the connection with a destination Socket address
    ///
    /// __Warning: this will forcefully close the connection, causing any Packets in stream queues that haven't already
    /// sent over the wire to be dropped.__
    ///
    /// Same as [`close_connection()`](`PacketManager::close_connection()`), except returns a Future and can be called
    /// from an async context.
    pub async fn async_close_connection<S: Into<String>>(&mut self, addr: S) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.async_update_new_senders().await;
        self.async_update_new_receivers().await;
        let addr = addr.into();
        let mut client_connections = self.client_connections.write().await;
        if let Some((id, conn)) = client_connections.get(&addr) {
            debug!("Forcefully closing connection for client addr={}, id={}", addr, id);
            conn.close(VarInt::from(1_u8), "PacketManager::close_connection() called for this connection".as_bytes());
        }
        client_connections.remove(&addr);
        // Note: We don't gracefully shut down the streams individually, as they should shut down on their own eventually
        self.send_streams.remove(&addr);
        self.rx.remove(&addr);
        Ok(())
    }

    /// Closes the connection with a destination Socket address after flushing all send streams gracefully.
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
    pub fn finish_connection<S: Into<String>>(&mut self, addr: S) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.update_new_senders();
        self.update_new_receivers();
        let addr = addr.into();
        // Finish all send streams
        match self.runtime.as_ref() {
            None => {
                panic!("PacketManager does not have a runtime instance associated with it.  Did you mean to call async_finish_connection()?");
            }
            Some(runtime) => runtime.block_on(async {
                if let Some(send_streams) = self.send_streams.get(&addr) {
                    for send_stream in send_streams.values() {
                        if let Err(e) = send_stream.write().await.finish().await {
                            debug!(
                                "Could not finish send stream for addr={}.  Continuing to close connection: {:?}",
                                addr, e
                            );
                        }
                    }
                }
            }),
        };
        self.send_streams.remove(&addr);
        let mut client_connections = self.client_connections.blocking_write();
        if let Some((id, conn)) = client_connections.get(&addr) {
            debug!("Finishing connection for client addr={}, id={}", addr, id);
            conn.close(VarInt::from(1_u8), "PacketManager::finish_connection() called for this connection".as_bytes());
        }
        client_connections.remove(&addr);
        self.rx.remove(&addr);
        Ok(())
    }

    /// Closes the connection with a destination Socket address after flushing all send streams gracefully.
    ///
    /// Same as [`finish_connection()`](`PacketManager::finish_connection()`), except returns a Future and can be called
    /// from an async context.
    pub async fn async_finish_connection<S: Into<String>>(&mut self, addr: S) -> Result<(), CloseError> {
        // Update senders and receivers before checking what to remove
        self.async_update_new_senders().await;
        self.async_update_new_receivers().await;
        let addr = addr.into();
        // Finish all send streams
        if let Some(send_streams) = self.send_streams.get(&addr) {
            for send_stream in send_streams.values() {
                if let Err(e) = send_stream.write().await.finish().await {
                    debug!("Could not finish send stream for addr={}.  Continuing to close connection: {:?}", addr, e);
                }
            }
        }
        self.send_streams.remove(&addr);
        let mut client_connections = self.client_connections.write().await;
        if let Some((id, conn)) = client_connections.get(&addr) {
            debug!("Finishing connection for client addr={}, id={}", addr, id);
            conn.close(VarInt::from(1_u8), "PacketManager::finish_connection() called for this connection".as_bytes());
        }
        client_connections.remove(&addr);
        self.rx.remove(&addr);
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

    async fn async_validate_for_send<T: Packet + 'static>(&self, for_all: bool) -> Result<(), SendError> {
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

    use tokio::sync::mpsc;
    use tokio::time::sleep;

    use durian::packet::PacketManager;
    use durian_macros::{bincode_packet};

    use crate as durian;
    use crate::{ClientConfig, ErrorType, register_receive, register_send, ServerConfig};

    #[bincode_packet]
    #[derive(Debug, PartialEq, Eq)]
    struct Test {
        id: i32,
    }

    #[bincode_packet]
    #[derive(Debug, PartialEq, Eq)]
    struct Other {
        name: String,
        id: i32,
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
            let server_config = ServerConfig::new_listening(server_addr, 1, 2, 2);
            assert!(m.async_init_server(server_config).await.is_ok());
            let client_connections = m.async_get_client_connections().await;
            assert_eq!(client_connections.len(), 1);
            assert_eq!(client_connections[0], (client_addr.to_string(), 0u32));

            for _ in 0..100 {
                assert!(m.async_send::<Test>(Test { id: 5 }).await.is_ok());
                assert!(m.async_send::<Test>(Test { id: 8 }).await.is_ok());
                assert!(m
                    .async_send::<Other>(Other {
                        name: "spoorn".to_string(),
                        id: 4,
                    })
                    .await
                    .is_ok());
                assert!(m
                    .async_send::<Other>(Other {
                        name: "kiko".to_string(),
                        id: 6,
                    })
                    .await
                    .is_ok());

                let test_res = m.async_received::<Test, TestPacketBuilder>(true).await;
                assert!(test_res.is_ok());
                let unwrapped = test_res.unwrap();
                assert!(unwrapped.is_some());
                assert_eq!(unwrapped.unwrap(), vec![Test { id: 6 }, Test { id: 9 }]);
                let other_res = m.async_received::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let unwrapped = other_res.unwrap();
                assert!(unwrapped.is_some());
                assert_eq!(
                    unwrapped.unwrap(),
                    vec![
                        Other {
                            name: "mango".to_string(),
                            id: 1,
                        },
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        },
                    ]
                );
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
        let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
        let client = manager.async_init_client(client_config).await;
        assert!(manager.async_get_client_connections().await.is_empty());
        println!("{:#?}", client);

        assert!(client.is_ok());

        for _ in 0..100 {
            // Send packets
            assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
            assert!(manager.async_send::<Test>(Test { id: 9 }).await.is_ok());
            assert!(manager
                .async_send::<Other>(Other {
                    name: "mango".to_string(),
                    id: 1,
                })
                .await
                .is_ok());
            assert!(manager
                .async_send::<Other>(Other {
                    name: "luna".to_string(),
                    id: 3,
                })
                .await
                .is_ok());

            let test_res = manager.async_received::<Test, TestPacketBuilder>(true).await;
            assert!(test_res.is_ok());
            let unwrapped = test_res.unwrap();
            assert!(unwrapped.is_some());
            assert_eq!(unwrapped.unwrap(), vec![Test { id: 5 }, Test { id: 8 }]);
            let other_res = manager.async_received::<Other, OtherPacketBuilder>(true).await;
            assert!(other_res.is_ok());
            let unwrapped = other_res.unwrap();
            assert!(unwrapped.is_some());
            assert_eq!(
                unwrapped.unwrap(),
                vec![
                    Other {
                        name: "spoorn".to_string(),
                        id: 4,
                    },
                    Other {
                        name: "kiko".to_string(),
                        id: 6,
                    },
                ]
            );
        }

        tx.send(0).await.unwrap();
        assert!(server.await.is_ok());
    }

    // Run with
    // $env:RUST_LOG="debug"; cargo test receive_packet_e2e_async_broadcast -- --nocapture
    #[test_log::test(tokio::test)]
    async fn receive_packet_e2e_async_broadcast() {
        let mut manager = PacketManager::new_for_async();

        let (tx, mut rx) = mpsc::channel(100);
        let server_addr = "127.0.0.1:6000";
        let client_addr = "127.0.0.1:6001";
        let client2_addr = "127.0.0.1:6002";

        let num_receive = 100;
        let client2_max_receive = 40;
        let num_send = 100;

        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            assert!(m.register_send_packet::<Test>().is_ok());
            assert!(m.register_send_packet::<Other>().is_ok());
            assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            let server_config = ServerConfig::new_listening(server_addr, 2, 2, 2);
            assert!(m.async_init_server(server_config).await.is_ok());
            let client_connections = m.async_get_client_connections().await;
            assert_eq!(client_connections.len(), 2);
            let client1_is_first = client_connections[0].0 == client_addr.clone() && client_connections[0].1 == 0u32
                || client_connections[1].0 == client_addr.clone() && client_connections[1].1 == 0u32;
            if client1_is_first {
                assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
            } else {
                assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
            }

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut client2_recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            let mut closed_client2 = false;
            loop {
                //println!("server sent {}, received {}", sent_packets, recv_packets);
                //println!("all_test_packets len {}", all_test_packets.len());
                //println!("all_other_packets len {}", all_other_packets.len());
                if sent_packets < num_send {
                    assert!(m.async_broadcast::<Test>(Test { id: 5 }).await.is_ok());
                    assert!(m.async_broadcast::<Test>(Test { id: 8 }).await.is_ok());
                    assert!(m
                        .async_broadcast::<Other>(Other {
                            name: "spoorn".to_string(),
                            id: 4,
                        })
                        .await
                        .is_ok());
                    assert!(m
                        .async_broadcast::<Other>(Other {
                            name: "kiko".to_string(),
                            id: 6,
                        })
                        .await
                        .is_ok());
                    sent_packets += 4;
                }

                // When halfway, try disconnecting one of the clients
                if client2_recv_packets >= client2_max_receive && !closed_client2 {
                    m.async_close_connection(client2_addr).await.unwrap();
                    closed_client2 = true;
                }

                if recv_packets >= num_receive + client2_max_receive && sent_packets >= num_send {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
                println!("{:?}", test_res);
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                assert_eq!(received_all.len(), if closed_client2 { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                assert!(m.async_get_client_id(addr.clone()).await.is_some());
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    client2_recv_packets += packets.len() / 2;
                    all_test_packets.append(&mut packets);
                }

                if !closed_client2 {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    assert!(m.async_get_client_id(addr2).await.is_some());
                    if unwrapped2.is_some() {
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        client2_recv_packets += packets.len() / 2;
                        all_test_packets.append(&mut packets);
                    }
                }

                let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let mut received_all = other_res.unwrap();
                assert_eq!(received_all.len(), if closed_client2 { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                assert!(m.async_get_client_id(addr.clone()).await.is_some());
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    client2_recv_packets += packets.len() / 2;
                    all_other_packets.append(&mut packets);
                }

                if !closed_client2 {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    assert!(m.async_get_client_id(addr2).await.is_some());
                    if unwrapped2.is_some() {
                        assert!(unwrapped2.is_some());
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        client2_recv_packets += packets.len() / 2;
                        all_other_packets.append(&mut packets);
                    }
                }
            }

            assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
            assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 6 });
                } else {
                    assert_eq!(packet, Test { id: 9 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "mango".to_string(),
                            id: 1,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        }
                    );
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
            let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
            assert!(manager.async_init_client(client_config).await.is_ok());
            assert!(manager.async_get_client_connections().await.is_empty());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("client2 sent {}, received {}", sent_packets, recv_packets);
                if sent_packets < num_send {
                    assert!(manager.async_broadcast::<Test>(Test { id: 6 }).await.is_ok());
                    assert!(manager.async_broadcast::<Test>(Test { id: 9 }).await.is_ok());
                    assert!(manager
                        .async_broadcast::<Other>(Other {
                            name: "mango".to_string(),
                            id: 1,
                        })
                        .await
                        .is_ok());
                    assert!(manager
                        .async_broadcast::<Other>(Other {
                            name: "luna".to_string(),
                            id: 3,
                        })
                        .await
                        .is_ok());
                    sent_packets += 4;
                }

                if recv_packets == client2_max_receive {
                    println!("client2 done, sent {}, received {}", sent_packets, recv_packets);
                    sleep(Duration::from_secs(4)).await;
                    break;
                }

                let test_res = manager.async_received_all::<Test, TestPacketBuilder>(true).await;
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                if received_all.is_empty() {
                    println!("client2 got empty packets.  Exiting loop");
                    break;
                }
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

            assert_eq!(all_test_packets.len(), client2_max_receive / 2);
            assert_eq!(all_other_packets.len(), client2_max_receive / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 5 });
                } else {
                    assert_eq!(packet, Test { id: 8 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "spoorn".to_string(),
                            id: 4,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "kiko".to_string(),
                            id: 6,
                        }
                    );
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
        let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
        let client = manager.async_init_client(client_config).await;
        assert!(manager.async_get_client_connections().await.is_empty());
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
                assert!(manager
                    .async_broadcast::<Other>(Other {
                        name: "mango".to_string(),
                        id: 1,
                    })
                    .await
                    .is_ok());
                assert!(manager
                    .async_broadcast::<Other>(Other {
                        name: "luna".to_string(),
                        id: 3,
                    })
                    .await
                    .is_ok());
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

        assert_eq!(all_test_packets.len(), num_receive / 2);
        assert_eq!(all_other_packets.len(), num_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(
                    packet,
                    Other {
                        name: "spoorn".to_string(),
                        id: 4,
                    }
                );
            } else {
                assert_eq!(
                    packet,
                    Other {
                        name: "kiko".to_string(),
                        id: 6,
                    }
                );
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

        let num_receive = 100;
        let client2_max_receive = 40;
        let num_send = 100;

        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            assert!(m.register_send_packet::<Test>().is_ok());
            assert!(m.register_send_packet::<Other>().is_ok());
            assert!(m.register_receive_packet::<Test>(TestPacketBuilder).is_ok());
            assert!(m.register_receive_packet::<Other>(OtherPacketBuilder).is_ok());
            let server_config = ServerConfig::new_with_max_clients(server_addr, 2, 2, 2);
            assert!(m.async_init_server(server_config).await.is_ok());
            let client_connections = m.async_get_client_connections().await;
            assert_eq!(client_connections.len(), 2);
            let client1_is_first = client_connections[0].0 == client_addr.clone() && client_connections[0].1 == 0u32
                || client_connections[1].0 == client_addr.clone() && client_connections[1].1 == 0u32;
            if client1_is_first {
                assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
            } else {
                assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
            }

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            let mut client2_closed = false;
            loop {
                println!("server sent {}, received {}", sent_packets, recv_packets);
                println!("all_test_packets len {}", all_test_packets.len());
                println!("all_other_packets len {}", all_other_packets.len());
                if sent_packets < num_send {
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 5 }).await.is_ok());
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 8 }).await.is_ok());
                    assert!(m
                        .async_send_to::<Other>(
                            client_addr.to_string(),
                            Other {
                                name: "spoorn".to_string(),
                                id: 4,
                            },
                        )
                        .await
                        .is_ok());
                    assert!(m
                        .async_send_to::<Other>(
                            client_addr.to_string(),
                            Other {
                                name: "kiko".to_string(),
                                id: 6,
                            },
                        )
                        .await
                        .is_ok());
                    if !client2_closed {
                        if let Err(e) = m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 5 }).await {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 8 }).await {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m
                            .async_send_to::<Other>(
                                client2_addr.to_string(),
                                Other {
                                    name: "spoorn".to_string(),
                                    id: 4,
                                },
                            )
                            .await
                        {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m
                            .async_send_to::<Other>(
                                client2_addr.to_string(),
                                Other {
                                    name: "kiko".to_string(),
                                    id: 6,
                                },
                            )
                            .await
                        {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                    }
                    sent_packets += 4; // to each client
                }

                if recv_packets >= num_receive + client2_max_receive {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
                println!("{:?}", test_res);
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                if received_all.len() == 1 {
                    client2_closed = true;
                }
                assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                assert!(m.async_get_client_id(addr.clone()).await.is_some());
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
                if !client2_closed {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    assert!(m.async_get_client_id(addr2.clone()).await.is_some());
                    if unwrapped2.is_some() {
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        all_test_packets.append(&mut packets);
                    }
                }

                let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let mut received_all = other_res.unwrap();
                if received_all.len() == 1 {
                    client2_closed = true;
                }
                assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }

                if !client2_closed {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    if unwrapped2.is_some() {
                        assert!(unwrapped2.is_some());
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        all_other_packets.append(&mut packets);
                    }
                }
            }

            assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
            assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 6 });
                } else {
                    assert_eq!(packet, Test { id: 9 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "mango".to_string(),
                            id: 1,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        }
                    );
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
            let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
            assert!(manager.async_init_client(client_config).await.is_ok());
            assert!(manager.async_get_client_connections().await.is_empty());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("client2 sent {}, received {}", sent_packets, recv_packets);
                if sent_packets < client2_max_receive {
                    // Either send or send_to should work since there is only one recipient
                    assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                    assert!(manager.async_send_to::<Test>(server_addr.to_string(), Test { id: 9 }).await.is_ok());
                    assert!(manager
                        .async_send::<Other>(Other {
                            name: "mango".to_string(),
                            id: 1,
                        })
                        .await
                        .is_ok());
                    assert!(manager
                        .async_send_to::<Other>(
                            server_addr.to_string(),
                            Other {
                                name: "luna".to_string(),
                                id: 3,
                            },
                        )
                        .await
                        .is_ok());
                    sent_packets += 4;
                }

                // Close connection early for testing
                if recv_packets >= client2_max_receive {
                    manager.async_close_connection(server_addr).await.unwrap();
                    println!("client2 closed connection");
                    break;
                }

                // Fallback to exit loop, but should error
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

            assert_eq!(all_test_packets.len(), client2_max_receive / 2);
            assert_eq!(all_other_packets.len(), client2_max_receive / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 5 });
                } else {
                    assert_eq!(packet, Test { id: 8 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "spoorn".to_string(),
                            id: 4,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "kiko".to_string(),
                            id: 6,
                        }
                    );
                }
            }

            client2_rx.recv().await;
            println!("client2 exit");
        });

        // Client
        let receive_results = register_receive!(manager, (Test, TestPacketBuilder), (Other, OtherPacketBuilder));
        assert!(receive_results.iter().all(|r| r.is_ok()));
        let send_results = register_send!(manager, Test, Other);
        assert!(send_results.iter().all(|r| r.is_ok()));
        let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
        let client = manager.async_init_client(client_config).await;
        assert!(manager.async_get_client_connections().await.is_empty());
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
                assert!(manager
                    .async_send::<Other>(Other {
                        name: "mango".to_string(),
                        id: 1,
                    })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send_to::<Other>(
                        server_addr.to_string(),
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        },
                    )
                    .await
                    .is_ok());
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

        assert_eq!(all_test_packets.len(), num_receive / 2);
        assert_eq!(all_other_packets.len(), num_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(
                    packet,
                    Other {
                        name: "spoorn".to_string(),
                        id: 4,
                    }
                );
            } else {
                assert_eq!(
                    packet,
                    Other {
                        name: "kiko".to_string(),
                        id: 6,
                    }
                );
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
    async fn receive_packet_e2e_async_send_to_with_finish_connection() {
        let mut manager = PacketManager::new_for_async();

        let (tx, mut rx) = mpsc::channel(100);
        let server_addr = "127.0.0.1:8000";
        let client_addr = "127.0.0.1:8001";
        let client2_addr = "127.0.0.1:8002";

        let num_receive = 100;
        let client2_max_receive = 40;
        let num_send = 100;

        // Server
        let server = tokio::spawn(async move {
            let mut m = PacketManager::new_for_async();
            let send_results = register_send!(m, Test, Other);
            assert!(send_results.iter().all(|r| r.is_ok()));
            let receive_results = register_receive!(m, [(Test, TestPacketBuilder), (Other, OtherPacketBuilder)]);
            assert!(receive_results.iter().all(|r| r.is_ok()));
            let server_config = ServerConfig::new_with_max_clients(server_addr, 2, 2, 2);
            assert!(m.async_init_server(server_config).await.is_ok());
            let client_connections = m.async_get_client_connections().await;
            assert_eq!(client_connections.len(), 2);
            let client1_is_first = client_connections[0].0 == client_addr.clone() && client_connections[0].1 == 0u32
                || client_connections[1].0 == client_addr.clone() && client_connections[1].1 == 0u32;
            if client1_is_first {
                assert!(client_connections.contains(&(client_addr.to_string(), 0u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 1u32)));
            } else {
                assert!(client_connections.contains(&(client_addr.to_string(), 1u32)));
                assert!(client_connections.contains(&(client2_addr.to_string(), 0u32)));
            }

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            let mut client2_closed = false;
            loop {
                println!("server sent {}, received {}", sent_packets, recv_packets);
                println!("all_test_packets len {}", all_test_packets.len());
                println!("all_other_packets len {}", all_other_packets.len());
                if sent_packets < num_send {
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 5 }).await.is_ok());
                    assert!(m.async_send_to::<Test>(client_addr.to_string(), Test { id: 8 }).await.is_ok());
                    assert!(m
                        .async_send_to::<Other>(
                            client_addr.to_string(),
                            Other {
                                name: "spoorn".to_string(),
                                id: 4,
                            },
                        )
                        .await
                        .is_ok());
                    assert!(m
                        .async_send_to::<Other>(
                            client_addr.to_string(),
                            Other {
                                name: "kiko".to_string(),
                                id: 6,
                            },
                        )
                        .await
                        .is_ok());
                    if !client2_closed {
                        if let Err(e) = m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 5 }).await {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m.async_send_to::<Test>(client2_addr.to_string(), Test { id: 8 }).await {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m
                            .async_send_to::<Other>(
                                client2_addr.to_string(),
                                Other {
                                    name: "spoorn".to_string(),
                                    id: 4,
                                },
                            )
                            .await
                        {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                        if let Err(e) = m
                            .async_send_to::<Other>(
                                client2_addr.to_string(),
                                Other {
                                    name: "kiko".to_string(),
                                    id: 6,
                                },
                            )
                            .await
                        {
                            match e.error_type {
                                ErrorType::Unexpected => {
                                    panic!("Couldn't send Test to client 2 {:?}", e);
                                }
                                ErrorType::Disconnected => {}
                            }
                        }
                    }
                    sent_packets += 4; // to each client
                }

                if recv_packets >= num_receive + client2_max_receive {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let test_res = m.async_received_all::<Test, TestPacketBuilder>(true).await;
                println!("{:?}", test_res);
                assert!(test_res.is_ok());
                let mut received_all = test_res.unwrap();
                if received_all.len() == 1 {
                    client2_closed = true;
                }
                assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                assert!(m.async_get_client_id(addr.clone()).await.is_some());
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_test_packets.append(&mut packets);
                }
                if !client2_closed {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    assert!(m.async_get_client_id(addr2.clone()).await.is_some());
                    if unwrapped2.is_some() {
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        all_test_packets.append(&mut packets);
                    }
                }

                // When a client calls finish_connection(), it's possible the send streams are flushed in different
                // order than invoked, so it's possible we already read all packets at this point in between
                // reading the 2 packet types.  i.e. last Other packets are already flushed and read by server, and
                // then the last Test packets are read on subsequent iteration.
                if recv_packets >= num_receive + client2_max_receive {
                    println!("server done, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                let other_res = m.async_received_all::<Other, OtherPacketBuilder>(true).await;
                assert!(other_res.is_ok());
                let mut received_all = other_res.unwrap();
                if received_all.len() == 1 {
                    client2_closed = true;
                }
                assert_eq!(received_all.len(), if client2_closed { 1 } else { 2 });
                let (addr, unwrapped) = received_all.remove(0);
                if unwrapped.is_some() {
                    let mut packets = unwrapped.unwrap();
                    recv_packets += packets.len();
                    all_other_packets.append(&mut packets);
                }

                if !client2_closed {
                    let (addr2, unwrapped2) = received_all.remove(0);
                    assert!((addr == client_addr || addr == client2_addr) && (addr != addr2));
                    if unwrapped2.is_some() {
                        assert!(unwrapped2.is_some());
                        let mut packets = unwrapped2.unwrap();
                        recv_packets += packets.len();
                        all_other_packets.append(&mut packets);
                    }
                }
            }

            assert_eq!(all_test_packets.len(), (num_receive + client2_max_receive) / 2);
            assert_eq!(all_other_packets.len(), (num_receive + client2_max_receive) / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 6 });
                } else {
                    assert_eq!(packet, Test { id: 9 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "mango".to_string(),
                            id: 1,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        }
                    );
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
            let receive_results = register_receive!(manager, [(Test, TestPacketBuilder), (Other, OtherPacketBuilder)]);
            assert!(receive_results.iter().all(|r| r.is_ok()));
            let send_results = register_send!(manager, [Test, Other]);
            assert!(send_results.iter().all(|r| r.is_ok()));
            let client_config = ClientConfig::new(client2_addr, server_addr, 2, 2);
            assert!(manager.async_init_client(client_config).await.is_ok());
            assert!(manager.async_get_client_connections().await.is_empty());

            let mut sent_packets = 0;
            let mut recv_packets = 0;
            let mut all_test_packets = Vec::new();
            let mut all_other_packets = Vec::new();
            loop {
                println!("client2 sent {}, received {}", sent_packets, recv_packets);
                if sent_packets < client2_max_receive {
                    // Either send or send_to should work since there is only one recipient
                    assert!(manager.async_send::<Test>(Test { id: 6 }).await.is_ok());
                    assert!(manager.async_send_to::<Test>(server_addr.to_string(), Test { id: 9 }).await.is_ok());
                    assert!(manager
                        .async_send::<Other>(Other {
                            name: "mango".to_string(),
                            id: 1,
                        })
                        .await
                        .is_ok());
                    assert!(manager
                        .async_send_to::<Other>(
                            server_addr.to_string(),
                            Other {
                                name: "luna".to_string(),
                                id: 3,
                            },
                        )
                        .await
                        .is_ok());
                    sent_packets += 4;
                }

                // Close connection early for testing
                if recv_packets >= client2_max_receive {
                    manager.async_finish_connection(server_addr).await.unwrap();
                    println!("client2 finished connection, sent {}, received {}", sent_packets, recv_packets);
                    break;
                }

                // Fallback to exit loop, but should error
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

            assert_eq!(all_test_packets.len(), client2_max_receive / 2);
            assert_eq!(all_other_packets.len(), client2_max_receive / 2);
            for (i, packet) in all_test_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(packet, Test { id: 5 });
                } else {
                    assert_eq!(packet, Test { id: 8 });
                }
            }
            for (i, packet) in all_other_packets.into_iter().enumerate() {
                if i % 2 == 0 {
                    assert_eq!(
                        packet,
                        Other {
                            name: "spoorn".to_string(),
                            id: 4,
                        }
                    );
                } else {
                    assert_eq!(
                        packet,
                        Other {
                            name: "kiko".to_string(),
                            id: 6,
                        }
                    );
                }
            }

            client2_rx.recv().await;
            println!("client2 exit");
        });

        // Client
        let receive_results = register_receive!(manager, (Test, TestPacketBuilder), (Other, OtherPacketBuilder));
        assert!(receive_results.iter().all(|r| r.is_ok()));
        let send_results = register_send!(manager, Test, Other);
        assert!(send_results.iter().all(|r| r.is_ok()));
        let client_config = ClientConfig::new(client_addr, server_addr, 2, 2);
        let client = manager.async_init_client(client_config).await;
        assert!(manager.async_get_client_connections().await.is_empty());
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
                assert!(manager
                    .async_send::<Other>(Other {
                        name: "mango".to_string(),
                        id: 1,
                    })
                    .await
                    .is_ok());
                assert!(manager
                    .async_send_to::<Other>(
                        server_addr.to_string(),
                        Other {
                            name: "luna".to_string(),
                            id: 3,
                        },
                    )
                    .await
                    .is_ok());
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

        assert_eq!(all_test_packets.len(), num_receive / 2);
        assert_eq!(all_other_packets.len(), num_receive / 2);
        for (i, packet) in all_test_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(packet, Test { id: 5 });
            } else {
                assert_eq!(packet, Test { id: 8 });
            }
        }
        for (i, packet) in all_other_packets.into_iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(
                    packet,
                    Other {
                        name: "spoorn".to_string(),
                        id: 4,
                    }
                );
            } else {
                assert_eq!(
                    packet,
                    Other {
                        name: "kiko".to_string(),
                        id: 6,
                    }
                );
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
