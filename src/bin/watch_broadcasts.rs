use nom::error::Error;
use nom::Finish;

use epics::{
    messages::{parse_search_packet, CAMessage, RsrvIsUp},
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
        let socket_beacon = UdpSocket::bind("0.0.0.0:5065").await.unwrap();
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

        if let Ok((_, command)) = nom::number::complete::be_u16::<_, Error<_>>(msg_buf).finish() {
            // Process the command here
            if command == 13 {
                if let Ok((_, beacon)) = RsrvIsUp::parse(msg_buf) {
                    println!(
                        "Received BEACON {}:{} from {sender}",
                        beacon.server_ip.map(|f| f.into()).unwrap_or(sender.ip()),
                        beacon.server_port
                    );
                } else {
                    println!("Received INVALID Beacon from {sender}: {:?}", msg_buf);
                }
            } else if command == 0 && size > 16 {
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
                // println!("Received code: {command} from {sender} in {size} bytes");
            }
        } else {
            println!("Receieved {size} byte packet from {sender}");
        }
    }
}
