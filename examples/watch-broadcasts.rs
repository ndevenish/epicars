use std::{
    io::{self, ErrorKind},
    net::ToSocketAddrs,
};

use epicars::messages::{self, RawMessage, parse_search_packet};
use log::{error, info, warn};
use socket2::{Domain, Protocol, Type};
use tokio::{net::UdpSocket, task::yield_now};

pub fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    colog::init();

    tokio::spawn(async {
        let std_sock = match new_reusable_udp_socket("0.0.0.0:5065") {
            Ok(x) => x,
            Err(err) => match err.kind() {
                ErrorKind::AddrInUse => {
                    panic!(
                        "Error: Port 5065 already in use, without reuse flag. Is a caRepeater running?"
                    );
                }
                x => panic!("IO Error: {x}"),
            },
        };

        let socket_beacon = UdpSocket::from_std(std_sock).unwrap();

        loop {
            read_socket(&socket_beacon).await;
        }
    });

    tokio::spawn(async {
        let socket_search =
            tokio::net::UdpSocket::from_std(new_reusable_udp_socket("0.0.0.0:5064").unwrap())
                .unwrap();
        loop {
            read_socket(&socket_search).await;
        }
    });
    info!("Waiting for packets on 0.0.0.0:5065, 0.0.0.0:5064",);
    loop {
        yield_now().await;
    }
}

async fn read_socket(socket: &UdpSocket) {
    let mut buf: Vec<u8> = vec![0; 0xFFFF];

    while let Ok((size, sender)) = socket.recv_from(&mut buf).await {
        let msg_buf = &buf[..size];

        if let Ok((leftover, raw)) = RawMessage::parse(msg_buf) {
            match raw.command {
                0 => {
                    if let Ok(searches) = parse_search_packet(msg_buf) {
                        info!(
                            "Received SEARCH for {} names from {sender}: {}",
                            searches.len(),
                            searches
                                .iter()
                                .map(|x| x.channel_name.clone())
                                .collect::<Vec<String>>()
                                .join(" ")
                        );
                    } else {
                        error!(
                            "Receied code 0 CA_PROTO_VERSION packet from {sender} but we don't understand the contents"
                        );
                    }
                }
                13 => {
                    let beacon: messages::RsrvIsUp = raw.try_into().unwrap();
                    info!(
                        "Received BEACON {}:{} ({}) from {sender}",
                        beacon.server_ip.map(|f| f.into()).unwrap_or(sender.ip()),
                        beacon.server_port,
                        beacon.beacon_id,
                    );
                    if !leftover.is_empty() {
                        warn!(
                            "Warning: After parsing BEACON, {} bytes were remaining",
                            leftover.len()
                        )
                    }
                }
                _ => warn!("Unexpected broadcast packet received!"),
            }
        } else {
            error!("Got an error parsing a raw message; was it a real CA message? {msg_buf:x?}");
            break;
        }
    }
}
