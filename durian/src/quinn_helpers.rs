use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use quinn::{ClientConfig, Endpoint, IdleTimeout, ServerConfig, TransportConfig};

// Implementation of `ServerCertVerifier` that verifies everything as trustworthy.
struct SkipServerVerification;

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self)
    }
}

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

/// Constructs a QUIC endpoint configured for use a client only.
///
/// ## Args
///
/// - server_certs: list of trusted certificates.
#[allow(unused)]
pub fn make_client_endpoint(
    bind_addr: SocketAddr,
    server_certs: &[&[u8]],
    keep_alive_interval: Option<Duration>,
    idle_timeout: Option<Duration>
) -> Result<Endpoint, Box<dyn Error>> {
    let client_cfg = configure_client(server_certs, keep_alive_interval, idle_timeout)?;
    let mut endpoint = Endpoint::client(bind_addr)?;
    endpoint.set_default_client_config(client_cfg);
    Ok(endpoint)
}

/// Constructs a QUIC endpoint configured to listen for incoming connections on a certain address
/// and port.
///
/// ## Returns
///
/// - a stream of incoming QUIC connections
/// - server certificate serialized into DER format
#[allow(unused)]
pub fn make_server_endpoint(bind_addr: SocketAddr, keep_alive_interval: Option<Duration>,
                            idle_timeout: Option<Duration>) -> Result<(Endpoint, Vec<u8>), Box<dyn Error>> {
    let (server_config, server_cert) = configure_server(keep_alive_interval, idle_timeout)?;
    let endpoint = Endpoint::server(server_config, bind_addr)?;
    Ok((endpoint, server_cert))
}

/// Builds default quinn client config and trusts given certificates.
///
/// ## Args
///
/// - server_certs: a list of trusted certificates in DER format.
fn configure_client(server_certs: &[&[u8]], keep_alive_interval: Option<Duration>,
                    idle_timeout: Option<Duration>) -> Result<ClientConfig, Box<dyn Error>> {
    // let mut certs = rustls::RootCertStore::empty();
    // for cert in server_certs {
    //     certs.add(&rustls::Certificate(cert.to_vec()))?;
    // }
    // 
    // Ok(ClientConfig::with_root_certificates(certs))
    let crypto = rustls::ClientConfig::builder()
        .with_safe_defaults()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    let mut client_config = ClientConfig::new(Arc::new(crypto));
    let mut transport_config = TransportConfig::default();
    transport_config.keep_alive_interval(keep_alive_interval);
    if let Some(idle_timeout) = idle_timeout {
        transport_config.max_idle_timeout(Some(IdleTimeout::try_from(idle_timeout)?));
    }
    client_config.transport_config(Arc::new(transport_config));

    Ok(client_config)
}

/// Returns default server configuration along with its certificate.
fn configure_server(keep_alive_interval: Option<Duration>,
                    idle_timeout: Option<Duration>) -> Result<(ServerConfig, Vec<u8>), Box<dyn Error>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_der = cert.serialize_der().unwrap();
    let priv_key = cert.serialize_private_key_der();
    let cert_chain = vec![rustls::Certificate(cert_der.clone())];
    let priv_key = rustls::PrivateKey(priv_key);

    let mut server_config = ServerConfig::with_single_cert(cert_chain, priv_key)?;
    let transport_config = Arc::get_mut(&mut server_config.transport)
        .unwrap()
        .keep_alive_interval(keep_alive_interval);
    if let Some(idle_timeout) = idle_timeout {
        transport_config.max_idle_timeout(Some(IdleTimeout::try_from(idle_timeout)?));
    }

    Ok((server_config, cert_der))
}