use std::error::Error;
use std::fmt::{Display, Formatter};
use std::net::AddrParseError;

use quinn::ConnectError;

use crate as durian;
use durian_macros::ErrorOnlyMessage;

/// Error types when receiving an error on one of the [`PacketManager`](`crate::PacketManager`) APIs
#[derive(Default, PartialEq, Eq, Copy, Clone, Debug)]
pub enum ErrorType {
    /// This was an unexpected error and should be treated as such
    #[default]
    Unexpected,
    /// The stream or connection was disconnected.  The [`PacketManager`](`crate::PacketManager`) would have cleaned up the connection so
    /// subsequent calls should not run into this again for the same address.  Depending on the application, this may
    /// be perfectly normal/expected, or an error.
    Disconnected,
}

/// Error when calling [`PacketManager::register_receive_packet()`](`crate::PacketManager::register_receive_packet()`), [`PacketManager::received_all()`](`crate::PacketManager::received_all()`),
/// [`PacketManager::async_received_all()`](`crate::PacketManager::async_received_all()`), [`PacketManager::received()`](`crate::PacketManager::received()`), [`PacketManager::async_received()`](`crate::PacketManager::async_received()`)
#[derive(Debug, Clone, ErrorOnlyMessage)]
pub struct ReceiveError {
    /// Error message
    pub message: String,
    /// Error type
    pub error_type: ErrorType,
}

impl Display for ReceiveError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReceiveError(message: {}, error_type: {:?})", self.message, self.error_type)
    }
}

/// Error when calling [`PacketManager::register_send_packet()`](`crate::PacketManager::register_send_packet()`), [`PacketManager::broadcast()`](`crate::PacketManager::broadcast()`),
/// [`PacketManager::async_broadcast()`](`crate::PacketManager::async_broadcast()`), [`PacketManager::send()`](`crate::PacketManager::send()`), [`PacketManager::async_send()`](`crate::PacketManager::async_send()`),
/// [`PacketManager::send_to()`](`crate::PacketManager::send_to()`), [`PacketManager::async_send_to()`](`crate::PacketManager::async_send_to()`)
#[derive(Debug, Clone, ErrorOnlyMessage)]
pub struct SendError {
    /// Error message
    pub message: String,
    /// Error type
    pub error_type: ErrorType,
}

impl Display for SendError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SendError(message: {}, error_type: {:?})", self.message, self.error_type)
    }
}

/// Error when calling [`PacketManager::init_client()`](`crate::PacketManager::init_client()`), [`PacketManager::async_init_client()`](`crate::PacketManager::async_init_client()`)
/// or [`PacketManager::init_server()`](`crate::PacketManager::init_server()`), [`PacketManager::async_init_server()`](`crate::PacketManager::async_init_server()`)
#[derive(Debug, Clone, ErrorOnlyMessage)]
pub struct ConnectionError {
    /// Error message
    pub message: String,
    /// Error type
    pub error_type: ErrorType,
}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ConnectionError(message: {}, error_type: {:?})", self.message, self.error_type)
    }
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
#[derive(Debug, Clone, ErrorOnlyMessage)]
pub struct CloseError {
    /// Error message
    pub message: String,
    /// Error type
    pub error_type: ErrorType,
}

impl Display for CloseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CloseError(message: {}, error_type: {:?})", self.message, self.error_type)
    }
}
