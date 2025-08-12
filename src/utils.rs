use num::{FromPrimitive, traits::WrappingAdd};
use socket2::{Domain, Protocol, Type};
use std::{
    io::{self},
    net::ToSocketAddrs,
};
use tokio::net::UdpSocket;

pub fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    UdpSocket::from_std(std::net::UdpSocket::from(socket))
}

/// Increments a mutable reference in place, and returns the original value
pub fn wrapping_inplace_add<T: WrappingAdd + FromPrimitive + Copy>(value: &mut T) -> T {
    let id = *value;
    *value = value.wrapping_add(&T::from_u8(1).unwrap());
    id
}
