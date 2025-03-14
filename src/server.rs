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
    sync::{broadcast, mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;

use crate::{
    database::{Dbr, DbrValue, NumericDBR, SingleOrVec},
    messages::{
        self, parse_search_packet, AccessRights, AsBytes, CAMessage, CreateChannel,
        CreateChannelResponse, ECAError, ErrorCondition, Message, MessageError, ReadNotify,
        ReadNotifyResponse,
    },
    new_reusable_udp_socket,
};

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
    next_circuit_id: u64,
    circuits: Vec<Circuit>,
    library: Arc<Mutex<PVLibrary>>,
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
            library: Default::default(),
            shutdown: CancellationToken::new(),
            next_circuit_id: 0,
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

async fn try_bind_ports(
    request_port: Option<u16>,
) -> Result<tokio::net::TcpListener, std::io::Error> {
    match request_port {
        None => {
            // Try binding TCP 5064
            if let Ok(socket) = tokio::net::TcpListener::bind("0.0.0.0:5064").await {
                return Ok(socket);
            }
            tokio::net::TcpListener::bind("0.0.0.0:0").await
        }
        Some(port) => tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await,
    }
}

impl Server {
    async fn listen(&mut self) -> Result<(), std::io::Error> {
        // Create the TCP listener first so we know what port to advertise
        let request_port = self.connection_port;

        // Try to bind 5064
        let connection_socket = try_bind_ports(request_port).await?;

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
                if let Ok(searches) = parse_search_packet(msg_buf) {
                    // println!("Got search from {}", origin);
                    let mut replies = Vec::new();
                    {
                        let server_names = server_names.lock().unwrap();
                        // println!("Known pvs: {:?}", server_names);
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

    fn handle_tcp_connections(&mut self, listener: TcpListener) {
        let library = self.library.clone();
        tokio::spawn(async move {
            let mut id = 0;
            loop {
                println!(
                    "Waiting to accept TCP connections on {}",
                    listener.local_addr().unwrap()
                );
                let (connection, client) = listener.accept().await.unwrap();
                println!("  Got new stream from {}", client);
                let circuit_library = library.clone();
                tokio::spawn(async move {
                    Circuit::start(id, connection, circuit_library).await;
                });
                id += 1;
            }
        });
    }
}

struct Circuit {
    id: u64,
    last_message: Instant,
    client_version: u16,
    client_host_name: Option<String>,
    client_user_name: Option<String>,
    client_events_on: bool,
    library: Arc<Mutex<PVLibrary>>,
    stream: TcpStream,
    channels: HashMap<u32, Channel>,
    next_channel_id: u32,
}

struct Channel {
    name: String,
    client_id: u32,
    server_id: u32,
    pv: Arc<Mutex<PV>>,
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
    async fn start(id: u64, mut stream: TcpStream, library: Arc<Mutex<PVLibrary>>) {
        println!("{id}: Starting circuit with {:?}", stream.peer_addr());
        let client_version = Circuit::do_version_exchange(&mut stream).await.unwrap();
        println!("{id}: Got client version: {}", client_version);
        // Client version is the bare minimum we need to establish a valid circuit
        let mut circuit = Circuit {
            id,
            last_message: Instant::now(),
            client_version,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
            library,
            stream,
            channels: HashMap::new(),
            next_channel_id: 0,
        };
        // Now, everything else is based on responding to events
        loop {
            let message = match Message::read_server_message(&mut circuit.stream).await {
                Ok(message) => message,
                Err(err) => {
                    match err {
                        // Handle various cases that could lead to this failing
                        MessageError::IO(io) => {
                            // Just fail the circuit completely, could inspect io.kind later
                            println!("{id}: IO Error reading server message: {}", io);
                            break;
                        }
                        MessageError::UnknownCommandId(command_id) => {
                            println!("{id}: Error: Receieved unknown command id: {command_id}");
                            continue;
                        }
                        MessageError::ParsingError(msg) => {
                            println!("{id}: Error: Incoming message parse error: {}", msg);
                            continue;
                        }
                        MessageError::UnexpectedMessage(msg) => {
                            println!("{id}: Error: Got message from client that is invalid to receive on a server: {msg:?}");
                            continue;
                        }
                        MessageError::IncorrectCommandId(msg, expect) => {
                            panic!("{id}: Fatal error: Got {msg} instead of {expect}")
                        }
                        MessageError::InvalidField(message) => {
                            println!("{id}: Got invalid message field: {message}");
                            continue;
                        }
                        MessageError::ErrorResponse(message) => {
                            println!("{id}: Got reading server messages generated error response: {message}");
                            continue;
                        }
                    }
                }
            };
            if let Err(MessageError::UnexpectedMessage(msg)) = circuit.handle_message(message).await
            {
                println!("{id}: Error: Unexpected message: {:?}", msg);
                continue;
            }
        }
        // If out here, we are closing the channel
        println!("{id}: Closing circuit");
        let _ = circuit.stream.shutdown().await;
    }

    async fn handle_message(&mut self, message: Message) -> Result<(), MessageError> {
        let id = self.id;
        match message {
            Message::Echo => {
                let mut reply_buf = Cursor::new(Vec::new());
                messages::Echo.write(&mut reply_buf).unwrap();
                self.stream.write_all(&reply_buf.into_inner()).await?
            }
            Message::ClientName(name) if self.client_user_name.is_none() => {
                println!("{id}: Got client username: {}", name.name);
                self.client_user_name = Some(name.name);
            }
            Message::HostName(name) if self.client_host_name.is_none() => {
                println!("{id}: Got client hostname: {}", name.name);
                self.client_host_name = Some(name.name);
            }
            Message::CreateChannel(message) => {
                println!(
                    "{id}: Got request to create channel to: {}",
                    message.channel_name
                );
                let mut buffer = Vec::new();
                let (messages, channel) = self.create_channel(message);
                for out_message in messages {
                    buffer.extend(out_message.as_bytes());
                }
                // Write the results of channel creation
                if !buffer.is_empty() {
                    self.stream.write_all(&buffer).await?
                }
                if let Ok(channel) = channel {
                    self.channels.insert(channel.server_id, channel);
                }
            }
            Message::ClearChannel(message) => {
                println!("{id}:{}: Request to clear channel", message.server_id);
                self.channels.remove(&message.server_id);
            }
            Message::ReadNotify(msg) => {
                println!("{id}:{}: ReadNotify request: {:?}", msg.server_id, msg);
                match self.do_read(&msg) {
                    Ok(r) => {
                        println!("Sending response: {r:?}");
                        self.stream.write_all(&r.as_bytes()).await?
                    }

                    Err(e) => {
                        // Send an error in response to the read
                        let err = ECAError::new(e, msg.client_ioid, Message::ReadNotify(msg));
                        println!("Returning error: {err:?}");
                        self.stream.write_all(&err.as_bytes()).await?
                    }
                }
            }
            msg => return Err(MessageError::UnexpectedMessage(msg)),
        }
        Ok(())
    }

    fn do_read(&self, request: &ReadNotify) -> Result<ReadNotifyResponse, ErrorCondition> {
        let pv = self.channels[&request.server_id].pv.lock().unwrap();
        // Read the data into a Vec<u8>
        let (data_count, data) = pv
            .record
            .encode_value(request.data_type, request.data_count as usize)?;
        // let data_count = 1;
        // let data = 42u32.to_be_bytes().to_vec();
        Ok(request.respond(data_count, data))
    }

    fn create_channel(&mut self, message: CreateChannel) -> (Vec<Message>, Result<Channel, ()>) {
        let library = self.library.lock().unwrap();
        if !library.contains_key(&message.channel_name) {
            println!(
                "Got a request for channel to '{}', which we do not have.",
                message.channel_name
            );
            return (
                vec![Message::CreateChannelFailure(message.respond_failure())],
                Err(()),
            );
        }
        let pv_arc = &library[&message.channel_name];
        let pv = pv_arc.lock().unwrap();
        let access_rights = AccessRights {
            client_id: message.client_id,
            access_rights: pv.get_access_right(),
        };
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        let createchan = CreateChannelResponse {
            data_count: pv.record.get_count() as u32,
            data_type: pv.record.get_native_type(),
            client_id: message.client_id,
            server_id: id,
        };
        println!(
            "{}:{}: Opening {:?} channel to {}",
            self.id, id, access_rights.access_rights, message.channel_name
        );
        // We have this channel, send the initial
        (
            vec![
                Message::AccessRights(access_rights),
                Message::CreateChannelResponse(createchan),
            ],
            Ok(Channel {
                name: message.channel_name,
                server_id: id,
                client_id: message.client_id,
                pv: pv_arc.clone(),
            }),
        )
    }
}

pub struct ServerBuilder {
    beacon_port: u16,
    search_port: u16,
    connection_port: Option<u16>,
    library: HashMap<String, PV>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl ServerBuilder {
    pub fn new() -> ServerBuilder {
        ServerBuilder {
            beacon_port: 5065,
            search_port: 5064,
            connection_port: None,
            library: Default::default(),
        }
    }
    pub fn beacon_port(mut self, port: u16) -> ServerBuilder {
        self.beacon_port = port;
        self
    }
    pub fn search_port(mut self, port: u16) -> ServerBuilder {
        self.search_port = port;
        self
    }
    pub fn connection_port(mut self, port: u16) -> ServerBuilder {
        self.connection_port = Some(port);
        self
    }
    pub async fn start(self) -> io::Result<Server> {
        let mut server = Server {
            beacon_port: self.beacon_port,
            search_port: self.search_port,
            connection_port: self.connection_port,
            library: Arc::new(Mutex::new(
                self.library
                    .into_iter()
                    .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
                    .collect(),
            )),
            ..Default::default()
        };
        server.listen().await?;
        Ok(server)
    }
}

type PVLibrary = HashMap<String, Arc<Mutex<PV>>>;

#[derive(Debug)]
struct PV {
    name: String,
    record: Dbr,
    // Writing new values - sender to write, receiver to pick them up on the server
    write_tx: mpsc::Sender<(DbrValue, Option<oneshot::Receiver<()>>)>,
    write_rx: mpsc::Receiver<(DbrValue, Option<oneshot::Receiver<()>>)>,
    watch_tx: watch::Sender<DbrValue>,
    broadcast_tx: broadcast::Sender<DbrValue>,
}

impl PV {
    fn get_access_right(&self) -> messages::AccessRight {
        messages::AccessRight::ReadWrite
    }

    fn new(name: &str, record: Dbr) -> Self {
        let (write_tx, write_rx) = mpsc::channel(256);
        PV {
            name: name.to_owned(),
            write_tx,
            write_rx,
            watch_tx: watch::channel(record.get_value()).0,
            broadcast_tx: broadcast::channel(256).0,
            record,
        }
    }
}

pub trait AddBuilderPV<T> {
    fn add_pv(self, name: &str, initial_value: T) -> Self;
}
impl AddBuilderPV<i32> for ServerBuilder {
    fn add_pv(mut self, name: &str, initial_value: i32) -> Self {
        let pv = PV::new(
            name,
            Dbr::Long(NumericDBR {
                value: SingleOrVec::Single(initial_value),
                ..Default::default()
            }),
        );
        self.library.insert(name.to_owned(), pv);
        self
    }
}
