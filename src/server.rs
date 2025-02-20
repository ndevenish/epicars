use pnet::datalink;
use std::{
    collections::HashMap,
    fmt::format,
    hash::Hash,
    io::Cursor,
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};
use tokio::net::{TcpListener, UdpSocket};

use crate::messages::{self, parse_search_packet, CAMessage};

#[derive(Default)]
struct Limits<T> {
    upper: Option<T>,
    lower: Option<T>,
}

#[derive(Default)]
struct LimitSet<T> {
    display_limits: Limits<T>,
    warning_limits: Limits<T>,
    alarm_limits: Limits<T>,
}

struct NumericDBR<T> {
    status: i16,
    severity: i16,
    /// Only makes sense for FLOAT/DOUBLE, here to try and avoid duplication
    precision: Option<u16>,
    units: String,
    limits: LimitSet<T>,
    count: usize,
    value: Vec<T>,
    last_updated: Instant,
}

struct StringDBR {
    status: i16,
    severity: i16,
    value: String,
}
struct EnumDBR {
    status: i16,
    severity: i16,
    strings: HashMap<u16, String>,
    value: u16,
}

/// Basic DBR Data types, independent of category
enum DBRID {
    String = 0,
    Int = 1,
    Short = 2,
    Float = 3,
    Enum = 4,
    Char = 5,
    Long = 6,
}

/// Mapping of DBR categories
enum DBRCategory {
    Basic = 0,
    Status = 1,
    Time = 2,
    Graphics = 3,
    Control = 4,
}

enum DBR {
    Enum(EnumDBR),
    String(StringDBR),
    Char(NumericDBR<i8>),
    Int(NumericDBR<i16>),
    Long(NumericDBR<i32>),
    Float(NumericDBR<f32>),
    Double(NumericDBR<f64>),
}

struct Circuit {
    last_message: Instant,
    /// We must have this for the circuit to count as ready
    client_version: Option<i16>,
    client_host_name: Option<String>,
    client_user_name: Option<String>,
    client_events_on: bool,
}

impl Circuit {
    fn new() -> Self {
        Circuit {
            last_message: Instant::now(),
            client_version: None,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
        }
    }
}

struct LibraryRecord(usize);

struct Library {
    /// Records are addressed purely
    records: HashMap<LibraryRecord, DBR>,
    /// Keeps track of externally exposed names for each record
    names: HashMap<String, LibraryRecord>,
}

pub struct Server {
    /// Broadcast port to sent beacons
    beacon_port: u16,
    /// Port to receive search queries on
    search_port: u16,
    /// Port to receive connections on
    connection_port: u16,
    /// Time that last beacon was sent
    last_beacon: Instant,
    /// The beacon ID of the last beacon broadcast
    beacon_id: u32,
    circuits: Vec<Circuit>,
    // library: ChannelLibrary,
}

impl Default for Server {
    fn default() -> Self {
        Server {
            beacon_port: 5065,
            search_port: 5064,
            connection_port: 5064,
            last_beacon: Instant::now(),
            beacon_id: 0,
            circuits: Vec::new(),
        }
    }
}

fn get_broadcast_ips() -> Vec<Ipv4Addr> {
    datalink::interfaces()
        .into_iter()
        .filter(|i| !i.is_loopback())
        .flat_map(|i| i.ips.into_iter())
        .filter_map(|i| match i.broadcast() {
            IpAddr::V4(broadcast_ip) => Some(broadcast_ip),
            _ => None,
        })
        .collect()
}

impl Server {
    pub fn new(beacon_port: u16) -> Self {
        Server {
            beacon_port,
            ..Default::default()
        }
    }

    fn listen_for_searches(&self) {
        let search_port = self.search_port.clone();
        tokio::spawn(async move {
            let listener = UdpSocket::bind(format!("0.0.0.0:{}", search_port))
                .await
                .unwrap();
            let mut buf: Vec<u8> = vec![0; 0xFFFF];
            loop {
                let (size, origin) = listener.recv_from(&mut buf).await.unwrap();
                let msg_buf = &buf[..size];
                if let Ok(searches) = parse_search_packet(msg_buf) {
                    println!("Got search message from {}", origin);
                } else {
                    println!("Got unparseable search message from {}", origin);
                }
            }
        });
    }
    fn broadcast_beacons(&self) {
        let beacon_port = self.beacon_port.clone();
        let connection_port = self.connection_port.clone();
        tokio::spawn(async move {
            println!("Starting to broadcast");
            let broadcast = UdpSocket::bind(format!("0.0.0.0:{}", beacon_port))
                .await
                .unwrap();
            broadcast.set_broadcast(true);
            let mut message = messages::RsrvIsUp {
                server_port: connection_port,
                beacon_id: 0,
                ..Default::default()
            };
            loop {
                let mut writer = Cursor::new(Vec::new());
                message.write(&mut writer).unwrap();
                let message_bytes = writer.into_inner();
                let broadcast_ips = get_broadcast_ips();
                for i in broadcast_ips.iter() {
                    broadcast.send_to(message_bytes.as_slice(), (*i, beacon_port));
                }
                message.beacon_id = message.beacon_id.wrapping_add(1);
                println!("Broadcast beacon to {} interfaces", broadcast_ips.len());
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
    }
    pub async fn listen(&self) -> ! {
        self.listen_for_searches();
        self.broadcast_beacons();
        loop {}
    }
}
