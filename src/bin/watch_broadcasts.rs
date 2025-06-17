use std::io::ErrorKind;

use epics::{
    messages::{self, parse_search_packet, RawMessage},
    new_reusable_udp_socket,
};
use tokio::{net::UdpSocket, task::yield_now};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // const BEACON: Token = Token(0);
    // const SEARCH: Token = Token(1);

    // let mut poll = mio::Poll::new().unwrap();
    // let mut events = mio::Events::with_capacity(128);
    tokio::spawn(async {
        let std_sock = match new_reusable_udp_socket("0.0.0.0:5065") {
            Ok(x) => x,
            Err(err) => match err.kind() {
                ErrorKind::AddrInUse => {
                    panic!("Error: Port 5065 already in use, without reuse flag. Is a caRepeater running?");
                }
                x => panic!("IO Error: {}", x),
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
    println!("Waiting for packets on 0.0.0.0:5065, 0.0.0.0:5064",);
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
                        println!(
                            "Received SEARCH for {} names from {sender}: {}",
                            searches.len(),
                            searches
                                .iter()
                                .map(|x| x.channel_name.clone())
                                .collect::<Vec<String>>()
                                .join(" ")
                        );
                    } else {
                        println!("Receied code 0 CA_PROTO_VERSION packet from {sender} but we don't understand the contents");
                    }
                }
                13 => {
                    let beacon: messages::RsrvIsUp = raw.try_into().unwrap();
                    println!(
                        "Received BEACON {}:{} ({}) from {sender}",
                        beacon.server_ip.map(|f| f.into()).unwrap_or(sender.ip()),
                        beacon.server_port,
                        beacon.beacon_id,
                    );
                    if !leftover.is_empty() {
                        println!(
                            "Warning: After parsing BEACON, {} bytes were remaining",
                            leftover.len()
                        )
                    }
                }
                _ => {
                    println!()
                }
            }
        } else {
            println!(
                "Got an error parsing a raw message; was it a real CA message? {:x?}",
                msg_buf
            );
            break;
        }
    }
}
