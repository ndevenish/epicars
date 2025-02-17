use std::net::UdpSocket;

use nom::error::Error;
use nom::Finish;

use epics::messages::{CAMessage, RsrvIsUp};

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:5065").unwrap();
    let mut buf: Vec<u8> = vec![0; 0xFFFF];
    println!("Waiting for packets on {:?}", socket.local_addr().unwrap());
    while let Ok((size, sender)) = socket.recv_from(&mut buf) {
        if let Ok((_, command)) =
            nom::number::complete::be_u16::<_, Error<_>>(buf.as_slice()).finish()
        {
            // Process the command here
            if command == 13 {
                if let Ok((_, beacon)) = RsrvIsUp::parse(buf.as_slice()) {
                    println!("Received Beacon {beacon:?} from {sender}");
                } else {
                    println!("Received INVALID Beacon from {sender}: {:?}", &buf[..size]);
                }
            } else {
                println!("Received code: {command} from {sender}");
            }
        } else {
            println!("Receieved {size} byte packet from {sender}");
        }
    }
}
