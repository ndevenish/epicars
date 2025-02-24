#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::HashMap,
    io::{self, Cursor},
    net::{IpAddr, Ipv4Addr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UdpSocket},
};
use tokio_util::sync::CancellationToken;

use crate::{
    messages::{self, parse_search_packet, AsBytes, CAMessage, Message, MessageError},
    new_reusable_udp_socket,
};

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

enum SingleOrVec<T> {
    Single(T),
    Vector(Vec<T>),
}

struct NumericDBR<T> {
    status: i16,
    severity: i16,
    /// Only makes sense for FLOAT/DOUBLE, here to try and avoid duplication
    precision: Option<u16>,
    units: String,
    limits: LimitSet<T>,
    value: SingleOrVec<T>,
    last_updated: Instant,
}

impl<T> Default for NumericDBR<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            status: Default::default(),
            severity: Default::default(),
            precision: Default::default(),
            units: Default::default(),
            limits: Default::default(),
            value: SingleOrVec::Single(T::default()),
            last_updated: Instant::now(),
        }
    }
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
enum Dbrid {
    String = 0,
    Int = 1,
    Float = 2,
    Enum = 3,
    Char = 4,
    Long = 5,
    Double = 6,
}

/// Mapping of DBR categories
enum DBRCategory {
    Basic = 0,
    Status = 1,
    Time = 2,
    Graphics = 3,
    Control = 4,
}

enum Dbr {
    Enum(EnumDBR),
    String(StringDBR),
    Char(NumericDBR<i8>),
    Int(NumericDBR<i16>),
    Long(NumericDBR<i32>),
    Float(NumericDBR<f32>),
    Double(NumericDBR<f64>),
}

type ChannelLibrary = Arc<Mutex<HashMap<String, Dbr>>>;

pub struct Server {
    /// Broadcast port to sent beacons
    beacon_port: u16,
    /// Port to receive search queries on
    search_port: u16,
    /// Port to receive connections on, if specified
    connection_port: Option<u16>,
    /// Time that last beacon was sent
    last_beacon: Instant,
    /// The beacon ID of the last beacon broadcast
    beacon_id: u32,
    circuits: Vec<Circuit>,
    library: ChannelLibrary,
    shutdown: CancellationToken,
}

impl Default for Server {
    fn default() -> Self {
        Server {
            beacon_port: 5065,
            search_port: 5064,
            connection_port: None,
            last_beacon: Instant::now(),
            beacon_id: 0,
            circuits: Vec::new(),
            library: ChannelLibrary::default(),
            shutdown: CancellationToken::new(),
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
    pub async fn new(beacon_port: u16) -> io::Result<Self> {
        let server = Server {
            beacon_port,
            ..Default::default()
        };
        server
            .library
            .lock()
            .unwrap()
            .insert("something".to_string(), Dbr::Double(NumericDBR::default()));

        server.listen().await?;
        Ok(server)
    }

    pub async fn listen(&self) -> Result<(), std::io::Error> {
        // Create the TCP listener first so we know what port to advertise
        let request_port = self.connection_port.unwrap_or(40000);

        let connection_socket =
            tokio::net::TcpListener::bind(format!("0.0.0.0:{}", request_port)).await?;

        let listen_port = connection_socket.local_addr().unwrap().port();

        self.listen_for_searches(listen_port);
        // self.broadcast_beacons(listen_port).await?;
        self.handle_tcp_connections(connection_socket);

        Ok(())
    }

    async fn broadcast_beacons(&self, connection_port: u16) -> io::Result<()> {
        let beacon_port = self.beacon_port;
        let broadcast = UdpSocket::bind("0.0.0.0:0").await?;
        tokio::spawn(async move {
            broadcast.set_broadcast(true).unwrap();
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
                    broadcast
                        .send_to(message_bytes.as_slice(), (*i, beacon_port))
                        .await
                        .unwrap();
                }
                message.beacon_id = message.beacon_id.wrapping_add(1);
                println!(
                    "Broadcast beacon to {} interfaces: {:?}",
                    broadcast_ips.len(),
                    broadcast_ips,
                );
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
        Ok(())
    }

    fn listen_for_searches(&self, connection_port: u16) {
        let search_port = self.search_port;
        let server_names = self.library.clone();
        tokio::spawn(async move {
            let mut buf: Vec<u8> = vec![0; 0xFFFF];
            let listener = UdpSocket::from_std(
                new_reusable_udp_socket(format!("0.0.0.0:{}", search_port)).unwrap(),
            )
            .unwrap();

            println!(
                "Listening for searches on {:?}",
                listener.local_addr().unwrap()
            );
            loop {
                let (size, origin) = listener.recv_from(&mut buf).await.unwrap();
                let msg_buf = &buf[..size];
                // println!("Got search from {}", origin);
                if let Ok(searches) = parse_search_packet(msg_buf) {
                    let mut replies = Vec::new();
                    {
                        let server_names = server_names.lock().unwrap();
                        for search in searches {
                            if server_names.contains_key(&search.channel_name) {
                                // println!("Request match! Can provide {}", search.channel_name);
                                replies.push(search.respond(None, connection_port, true));
                            }
                        }
                    }
                    if !replies.is_empty() {
                        let mut reply_buf = Cursor::new(Vec::new());
                        messages::Version::default().write(&mut reply_buf).unwrap();
                        for reply in &replies {
                            reply.write(&mut reply_buf).unwrap();
                        }
                        listener
                            .send_to(&reply_buf.into_inner(), origin)
                            .await
                            .unwrap();
                        println!("Sending {} search results", replies.len());
                    }
                } else {
                    println!("Got unparseable search message from {}", origin);
                }
            }
        });
    }

    fn handle_tcp_connections(&self, listener: TcpListener) {
        let library = self.library.clone();
        tokio::spawn(async move {
            loop {
                println!(
                    "Waiting to accept TCP connections on {}",
                    listener.local_addr().unwrap()
                );
                let (connection, client) = listener.accept().await.unwrap();
                println!("  Got new stream from {}", client);
                let circuit_library = library.clone();
                tokio::spawn(async move {
                    Circuit::start(connection, circuit_library).await;
                });
            }
        });
    }
}

struct Circuit {
    last_message: Instant,
    client_version: u16,
    client_host_name: Option<String>,
    client_user_name: Option<String>,
    client_events_on: bool,
    library: ChannelLibrary,
    stream: TcpStream,
}

struct Channel {
    name: String,
    client_id: u32,
    server_id: u32,
}

impl Circuit {
    async fn do_version_exchange(stream: &mut TcpStream) -> Result<u16, MessageError> {
        // Send our Version
        stream
            .write_all(messages::Version::default().as_bytes().as_ref())
            .await?;
        // Immediately receive a Version message back from the client
        Ok(match Message::read_server_message(stream).await? {
            Message::Version(v) => v.protocol_version,
            err => {
                // This is an error, we cannot receive anything until we get this
                return Err(MessageError::UnexpectedMessage(err));
            }
        })
    }
    async fn start(mut stream: TcpStream, library: ChannelLibrary) {
        println!("Starting circuit with {:?}", stream.peer_addr());
        let client_version = Circuit::do_version_exchange(&mut stream).await.unwrap();
        println!("Got client version: {}", client_version);
        // Client version is the bare minimum we need to establish a valid circuit
        let mut circuit = Circuit {
            last_message: Instant::now(),
            client_version,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
            library,
            stream,
        };
        // Now, everything else is based on responding to events
        loop {
            let message = match Message::read_server_message(&mut circuit.stream).await {
                Ok(message) => message,
                Err(err) => match err {
                    // Handle various cases that could lead to this failing
                    MessageError::IO(io) => {
                        // Just fail the circuit completely, could inspect io.kind later
                        println!("IO Error reading server message: {}", io);
                        break;
                    }
                    MessageError::UnknownCommandId(id) => {
                        println!("Error: Receieved unknown command id: {id}");
                        continue;
                    }
                    MessageError::ParsingError(msg) => {
                        println!("Error: Incoming message parse error: {}", msg);
                        continue;
                    }
                    MessageError::UnexpectedMessage(msg) => {
                        println!("Error: Got valid but unexpected message from client: {msg:?}");
                        continue;
                    }
                },
            };
            circuit.handle_message(message).await;
        }
        // If out here, we are closing the channel
        println!("Closing channel");
        let _ = circuit.stream.shutdown().await;
    }

    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::Echo => {
                let mut reply_buf = Cursor::new(Vec::new());
                messages::Echo.write(&mut reply_buf).unwrap();
                self.stream
                    .write_all(&reply_buf.into_inner())
                    .await
                    .unwrap();
            }
            Message::ClientName(name) if self.client_user_name.is_none() => {
                println!("Got client username: {}", name.name);
                self.client_user_name = Some(name.name);
            }
            Message::HostName(name) if self.client_host_name.is_none() => {
                println!("Got client hostname: {}", name.name);
                self.client_host_name = Some(name.name);
            }
            Message::CreateChannel(chan) => {
                println!("Got request to create channel to: {}", chan.channel_name);
            }
            msg => panic!("Unexpected message: {:?}", msg),
        };
    }
}
