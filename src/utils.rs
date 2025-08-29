use num::{FromPrimitive, traits::WrappingAdd};
use pnet::datalink;
use socket2::{Domain, Protocol, Type};
use std::{
    env,
    io::{self},
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::net::UdpSocket;
use tracing::{debug, warn};

pub(crate) fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Increments a mutable reference in place, and returns the original value
pub(crate) fn wrapping_inplace_add<T: WrappingAdd + FromPrimitive + Copy>(value: &mut T) -> T {
    let id = *value;
    *value = value.wrapping_add(&T::from_u8(1).unwrap());
    id
}

/// Get the server listen port, either from environment or default 5064
pub fn get_default_server_port() -> u16 {
    env::var("EPICS_CA_SERVER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(5064u16)
        .max(5000u16)
}

/// Get the beacon broadcast port, either from environment or default 5065
pub fn get_default_beacon_port() -> u16 {
    env::var("EPICS_CA_REPEATER_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(5065u16)
        .max(5000u16)
}

/// Get the target list of broadcast IPs, by reading the environment and interfaces
///
/// Hostnames are resolved if in the environment setting, so this will re-resolve
pub fn get_target_broadcast_ips(default_search_port: u16) -> Vec<SocketAddr> {
    // let interfaces = ;
    let mut ips = Vec::new();
    // Work out if we want to automatically include all local broadcast
    let use_auto_address = env::var("EPICS_CA_AUTO_ADDR_LIST")
        .map(|v| !v.eq_ignore_ascii_case("no"))
        .unwrap_or(true);
    if use_auto_address {
        ips.extend(
            datalink::interfaces()
                .into_iter()
                .filter(|i| !i.is_loopback())
                .flat_map(|i| i.ips.into_iter())
                .filter(|i| i.is_ipv4())
                .flat_map(|f| (f.broadcast(), default_search_port).to_socket_addrs())
                .flatten(),
        );
    }
    if let Ok(addr_list) = env::var("EPICS_CA_ADDR_LIST") {
        for add in addr_list.split_ascii_whitespace() {
            if add.contains(":") {
                match add.to_socket_addrs() {
                    Ok(addr) => {
                        debug!("Adding search IP: {add} => {addr:?}");
                        ips.extend(addr);
                    }
                    Err(e) => {
                        warn!("Failed to convert '{add}' to address: {e}");
                        continue;
                    }
                }
            } else {
                match (add, default_search_port).to_socket_addrs() {
                    Ok(addr) => {
                        debug!("Adding search IP: {add} => {addr:?}");
                        ips.extend(addr);
                    }
                    Err(e) => {
                        warn!("Failed to convert '{add}' to address: {e}");
                        continue;
                    }
                }
            }
        }
    }
    // The user might have explicitly requested some
    ips
}

pub fn get_default_connection_timeout() -> f32 {
    env::var("EPICS_CA_CONN_TMO")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30.0f32)
        .max(0.1f32)
}

pub fn get_default_beacon_period() -> f32 {
    env::var("EPICS_CA_BEACON_PERIOD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(15.0f32)
        .max(0.1f32)
}

pub fn get_default_max_search_interval() -> f32 {
    env::var("EPICS_CA_MAX_SEARCH_PERIOD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300.0f32)
        .max(60f32)
}
