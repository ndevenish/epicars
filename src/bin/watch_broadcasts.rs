use mio::{Interest, Token};
use nom::error::Error;
use nom::Finish;

use epics::{
    messages::{parse_search_packet, CAMessage, RsrvIsUp},
    new_reusable_udp_socket,
};

fn main() {
    const BEACON: Token = Token(0);
    const SEARCH: Token = Token(1);

    let mut poll = mio::Poll::new().unwrap();
    let mut events = mio::Events::with_capacity(128);

    let mut socket_beacon = mio::net::UdpSocket::bind("0.0.0.0:5065".parse().unwrap()).unwrap();
    let mut socket_search =
        mio::net::UdpSocket::from_std(new_reusable_udp_socket("0.0.0.0:5064").unwrap());

    poll.registry()
        .register(&mut socket_beacon, BEACON, Interest::READABLE)
        .unwrap();
    poll.registry()
        .register(&mut socket_search, SEARCH, Interest::READABLE)
        .unwrap();

    println!(
        "Waiting for packets on {:?}, {:?}",
        socket_beacon.local_addr().unwrap(),
        socket_search.local_addr().unwrap()
    );
    loop {
        poll.poll(&mut events, None).unwrap();
        for event in events.iter() {
            match event.token() {
                BEACON => read_socket(&mut socket_beacon),
                SEARCH => read_socket(&mut socket_search),
                _ => unreachable!(),
            }
        }
    }
}

fn read_socket(socket: &mut mio::net::UdpSocket) {
    let mut buf: Vec<u8> = vec![0; 0xFFFF];

    while let Ok((size, sender)) = socket.recv_from(&mut buf) {
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
