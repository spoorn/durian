use std::error::Error;
use derive_more::Display;
use durian_macros::ErrorOnlyMessage;

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

/// Error when calling [`PacketManager::init_connections()`] or [`PacketManager::async_init_connections()`]
#[derive(Debug, Clone, Display, ErrorOnlyMessage)]
pub struct ConnectionError {
    /// Error message
    pub message: String
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