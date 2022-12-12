use derive_more::Display;
use durian_macros::ErrorOnlyMessage;
use quinn::ConnectError;
use std::error::Error;
use std::net::AddrParseError;

/// Error when calling [`PacketManager::register_receive_packet()`](`crate::PacketManager::register_receive_packet()`), [`PacketManager::received_all()`](`crate::PacketManager::received_all()`),
/// [`PacketManager::async_received_all()`](`crate::PacketManager::async_received_all()`), [`PacketManager::received()`](`crate::PacketManager::received()`), [`PacketManager::async_received()`](`crate::PacketManager::async_received()`)
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct ReceiveError {
    /// Error message
    pub message: String,
}

/// Error when calling [`PacketManager::register_send_packet()`](`crate::PacketManager::register_send_packet()`), [`PacketManager::broadcast()`](`crate::PacketManager::broadcast()`),
/// [`PacketManager::async_broadcast()`](`crate::PacketManager::async_broadcast()`), [`PacketManager::send()`](`crate::PacketManager::send()`), [`PacketManager::async_send()`](`crate::PacketManager::async_send()`),
/// [`PacketManager::send_to()`](`crate::PacketManager::send_to()`), [`PacketManager::async_send_to()`](`crate::PacketManager::async_send_to()`)
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct SendError {
    /// Error message
    pub message: String,
}

/// Error when calling [`PacketManager::init_client()`](`crate::PacketManager::init_client()`), [`PacketManager::async_init_client()`](`crate::PacketManager::async_init_client()`)
/// or [`PacketManager::init_server()`](`crate::PacketManager::init_server()`), [`PacketManager::async_init_server()`](`crate::PacketManager::async_init_server()`)
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct ConnectionError {
    /// Error message
    pub message: String,
}

impl From<quinn::ConnectionError> for ConnectionError {
    fn from(e: quinn::ConnectionError) -> Self {
        ConnectionError::new(format!("ConnectionError: {:?}", e))
    }
}

impl From<Box<dyn Error>> for ConnectionError {
    fn from(e: Box<dyn Error>) -> Self {
        ConnectionError::new(format!("ConnectionError: {:?}", e))
    }
}

impl From<quinn::ConnectionError> for Box<ConnectionError> {
    fn from(e: quinn::ConnectionError) -> Self {
        Box::new(ConnectionError::new(format!("ConnectionError: {:?}", e)))
    }
}

impl From<Box<dyn Error>> for Box<ConnectionError> {
    fn from(e: Box<dyn Error>) -> Self {
        Box::new(ConnectionError::new(format!("ConnectionError: {:?}", e)))
    }
}

impl From<AddrParseError> for ConnectionError {
    fn from(e: AddrParseError) -> Self {
        ConnectionError::new(format!("ConnectionError: {:?}", e))
    }
}

impl From<ConnectError> for ConnectionError {
    fn from(e: ConnectError) -> Self {
        ConnectionError::new(format!("ConnectionError: {:?}", e))
    }
}

/// Error when calling [`PacketManager::close_connection()`](`crate::PacketManager::close_connection()`)
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct CloseError {
    /// Error message
    pub message: String,
}