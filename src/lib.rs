use std::{io, net::ToSocketAddrs};

use socket2::{Domain, Protocol, Type};

pub mod client;
pub mod database;
pub mod messages;
pub mod provider;
pub mod server;

pub fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    Ok(socket.into())
}
