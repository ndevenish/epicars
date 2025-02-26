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
    messages::{
        self, parse_search_packet, AccessRights, AsBytes, CAMessage, CreateChannel,
        CreateChannelResponse, DBRBasicType, DBRCategory, DBRType, Message, MessageError,
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
            library: Arc::<Mutex<PVLibrary>>::default(),
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

impl Server {
    async fn listen(&mut self) -> Result<(), std::io::Error> {
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
        let id = self.next_circuit_id;
        self.next_circuit_id += 1;

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
                    Circuit::start(id, connection, circuit_library).await;
                });
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
    cancelled: CancellationToken,
    next_channel_id: u32,
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
            cancelled: CancellationToken::new(),
            next_channel_id: 0,
        };
        // Now, everything else is based on responding to events
        loop {
            let message = match Message::read_server_message(&mut circuit.stream).await {
                Ok(message) => message,
                Err(err) => match err {
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
                    MessageError::InvalidField(key, value) => {
                        println!("{id}: Got invalid message field: {key}={value}");
                        continue;
                    }
                },
            };
            if let Err(MessageError::UnexpectedMessage(msg)) = circuit.handle_message(message).await
            {
                println!("{id}: Error: Unexpected message: {:?}", msg);
                continue;
            }
        }
        // If out here, we are closing the channel
        println!("{id}: Closing circuit");
        circuit.cancelled.cancel();
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
                for out_message in self.create_channel(message) {
                    buffer.extend(out_message.as_bytes());
                }
                // Write the results of channel creation
                if !buffer.is_empty() {
                    self.stream.write_all(&buffer).await?
                }
            }
            msg => return Err(MessageError::UnexpectedMessage(msg)),
        };
        Ok(())
    }

    fn create_channel(&mut self, message: CreateChannel) -> Vec<Message> {
        let library = self.library.lock().unwrap();
        if !library.contains_key(&message.channel_name) {
            println!(
                "Got a request for channel to '{}', which we do not have.",
                message.channel_name
            );
            return vec![Message::CreateChannelFailure(message.respond_failure())];
        }
        let pv = &library[&message.channel_name];
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
        vec![
            Message::AccessRights(access_rights),
            Message::CreateChannelResponse(createchan),
        ]
    }
}

#[derive(Default, Debug)]
struct Limits<T> {
    upper: Option<T>,
    lower: Option<T>,
}

#[derive(Default, Debug)]
struct LimitSet<T> {
    display_limits: Limits<T>,
    warning_limits: Limits<T>,
    alarm_limits: Limits<T>,
}

#[derive(Clone, Debug)]
enum SingleOrVec<T> {
    Single(T),
    Vector(Vec<T>),
}

#[derive(Debug)]
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
impl<T> NumericDBR<T> {
    fn get_count(&self) -> usize {
        match &self.value {
            &SingleOrVec::Single(_) => 1,
            SingleOrVec::Vector(v) => v.len(),
        }
    }
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
#[derive(Debug)]
struct StringDBR {
    status: i16,
    severity: i16,
    value: String,
}
#[derive(Debug)]
struct EnumDBR {
    status: i16,
    severity: i16,
    strings: HashMap<u16, String>,
    value: u16,
}
#[derive(Debug)]
enum Dbr {
    Enum(EnumDBR),
    String(StringDBR),
    Char(NumericDBR<i8>),
    Int(NumericDBR<i16>),
    Long(NumericDBR<i32>),
    Float(NumericDBR<f32>),
    Double(NumericDBR<f64>),
}
impl Dbr {
    // fn value_default(&self) -> DbrValue {
    //     match self {
    //         Dbr::Enum(_) => DbrValue::Enum(0),
    //         Dbr::String(_) => DbrValue::String(String::new()),
    //         Dbr::Char(_) => DbrValue::Char(0),
    //         Dbr::Int(_) => DbrValue::Int(0),
    //         Dbr::Long(_) => DbrValue::Long(0),
    //         Dbr::Float(_) => DbrValue::Float(0.0),
    //         Dbr::Double(_) => DbrValue::Double(0.0),
    //     }
    // }
    fn get_count(&self) -> usize {
        match self {
            Dbr::Enum(_) => 1,
            Dbr::String(_) => 1,
            Dbr::Char(dbr) => dbr.get_count(),
            Dbr::Int(dbr) => dbr.get_count(),
            Dbr::Long(dbr) => dbr.get_count(),
            Dbr::Float(dbr) => dbr.get_count(),
            Dbr::Double(dbr) => dbr.get_count(),
        }
    }
    fn get_value(&self) -> DbrValue {
        match self {
            Dbr::Enum(dbr) => DbrValue::Enum(dbr.value),
            Dbr::String(dbr) => DbrValue::String(dbr.value.clone()),
            Dbr::Char(dbr) => DbrValue::Char(dbr.value.clone()),
            Dbr::Int(dbr) => DbrValue::Int(dbr.value.clone()),
            Dbr::Long(dbr) => DbrValue::Long(dbr.value.clone()),
            Dbr::Float(dbr) => DbrValue::Float(dbr.value.clone()),
            Dbr::Double(dbr) => DbrValue::Double(dbr.value.clone()),
        }
    }
    fn get_native_type(&self) -> DBRType {
        DBRType {
            basic_type: match self {
                Dbr::Enum(_) => DBRBasicType::Enum,
                Dbr::String(_) => DBRBasicType::String,
                Dbr::Char(_) => DBRBasicType::Char,
                Dbr::Int(_) => DBRBasicType::Int,
                Dbr::Long(_) => DBRBasicType::Long,
                Dbr::Float(_) => DBRBasicType::Float,
                Dbr::Double(_) => DBRBasicType::Double,
            },
            category: DBRCategory::Basic,
        }
    }
}

#[derive(Clone, Debug)]
enum DbrValue {
    Enum(u16),
    String(String),
    Char(SingleOrVec<i8>),
    Int(SingleOrVec<i16>),
    Long(SingleOrVec<i32>),
    Float(SingleOrVec<f32>),
    Double(SingleOrVec<f64>),
}

type PVLibrary = HashMap<String, PV>;

pub struct ServerBuilder {
    beacon_port: u16,
    search_port: u16,
    connection_port: Option<u16>,
    library: PVLibrary,
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
    pub async fn start(&self) -> io::Result<Server> {
        let mut server = Server {
            beacon_port: self.beacon_port,
            search_port: self.search_port,
            connection_port: self.connection_port,
            ..Default::default()
        };
        server.library.lock().unwrap().insert(
            "something".to_string(),
            PV::new("something", Dbr::Double(NumericDBR::default())),
        );

        server.listen().await?;
        Ok(server)
    }
}

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
