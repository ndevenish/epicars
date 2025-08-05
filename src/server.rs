#![allow(dead_code)]

use core::str;
use pnet::datalink;
use socket2::{Domain, Protocol, Type};
use std::{
    collections::HashMap,
    io::{self, Cursor},
    net::{IpAddr, Ipv4Addr, ToSocketAddrs},
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    dbr::{DBR_BASIC_STRING, Dbr, DbrType},
    messages::{
        self, AccessRights, AsBytes, CAMessage, CreateChannel, CreateChannelResponse, ECAError,
        ErrorCondition, EventAddResponse, Message, MessageError, MonitorMask, ReadNotify,
        ReadNotifyResponse, Write, parse_search_packet,
    },
    providers::Provider,
};

#[doc(hidden)]
pub fn new_reusable_udp_socket<T: ToSocketAddrs>(address: T) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_reuse_port(true)?;
    socket.set_nonblocking(true)?;
    let addr = address.to_socket_addrs()?.next().unwrap();
    socket.bind(&addr.into())?;
    Ok(socket.into())
}

pub struct Server<L: Provider> {
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
    circuits: Vec<Circuit<L>>,
    shutdown: CancellationToken,
    library_provider: L,
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

/// Try to bind a specific port, or if not supplied the default, or a random port if that fails.
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
        Some(port) => tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await,
    }
}

impl<L: Provider> Server<L> {
    async fn listen(&mut self) -> Result<(), std::io::Error> {
        // Create the TCP listener first so we know what port to advertise
        let request_port = self.connection_port;
        let connection_socket = try_bind_ports(request_port).await?;

        // Whatever we ended up with, we want to advertise
        let listen_port = connection_socket.local_addr().unwrap().port();

        self.listen_for_searches(listen_port);
        self.handle_tcp_connections(connection_socket);
        self.broadcast_beacons(listen_port).await?;

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
                info!(
                    "Broadcasting beacon {} to {} interfaces: {:?}",
                    message.beacon_id,
                    broadcast_ips.len(),
                    broadcast_ips,
                );
                message.beacon_id = message.beacon_id.wrapping_add(1);
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
        Ok(())
    }

    fn listen_for_searches(&self, connection_port: u16) {
        let search_port = self.search_port;
        let library_provider = self.library_provider.clone();
        tokio::spawn(async move {
            let mut buf: Vec<u8> = vec![0; 0xFFFF];
            let listener = UdpSocket::from_std(
                new_reusable_udp_socket(format!("0.0.0.0:{search_port}")).unwrap(),
            )
            .unwrap();

            info!(
                "Listening for searches on {:?}",
                listener.local_addr().unwrap()
            );
            // Keep track of recent searches and source ports, to help reject duplicate
            // messages coming in on multiple network ports.
            let mut recent_searches: Vec<(Instant, u16, Vec<messages::Search>)> = Vec::new();

            loop {
                let (size, origin) = listener.recv_from(&mut buf).await.unwrap();
                let msg_buf = &buf[..size];
                if let Ok(searches) = parse_search_packet(msg_buf) {
                    // Handle rejection window:
                    // Drop all requests for identical PVs from the same source port
                    let rejection_window_start = Instant::now()
                        .checked_sub(Duration::from_micros(500))
                        .unwrap();
                    let mut is_duplicate = false;
                    recent_searches.retain(|(i, port, msgs)| {
                        let keep = i > &rejection_window_start;
                        if *port == origin.port() && msgs == &searches && keep {
                            is_duplicate = true;
                        }
                        keep
                    });
                    if is_duplicate {
                        continue;
                    }
                    // This isn't a duplicate search, record it in case it comes in again
                    recent_searches.push((Instant::now(), origin.port(), searches.to_vec()));

                    let mut replies = Vec::new();
                    {
                        for search in searches {
                            if library_provider.provides(&search.channel_name) {
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
                        debug!("Sending {} search results", replies.len());
                    }
                } else {
                    error!("Got unparseable search message from {origin}");
                }
            }
        });
    }

    fn handle_tcp_connections(&mut self, listener: TcpListener) {
        let library = self.library_provider.clone();
        tokio::spawn(async move {
            let mut id = 0;
            loop {
                info!(
                    "Waiting to accept TCP connections on {}",
                    listener.local_addr().unwrap()
                );
                let (connection, client) = listener.accept().await.unwrap();
                debug!("  Got new stream from {client}");
                let circuit_library = library.clone();
                tokio::spawn(async move {
                    Circuit::start(id, connection, circuit_library).await;
                });
                id += 1;
            }
        });
    }
}

struct Circuit<L: Provider> {
    id: u64,
    last_message: Instant,
    client_version: u16,
    client_host_name: Option<String>,
    client_user_name: Option<String>,
    client_events_on: bool,
    library: L,
    channels: HashMap<u32, Channel>,
    next_channel_id: u32,
    monitor_value_available: mpsc::Sender<String>,
}

#[derive(Debug)]
struct Channel {
    name: String,
    client_id: u32,
    server_id: u32,
    subscription: Option<PVSubscription>,
}

#[derive(Debug)]
struct PVSubscription {
    data_type: DbrType,
    data_count: usize,
    subscription_id: u32,
    mask: MonitorMask,
    receiver: broadcast::Receiver<Dbr>,
}

impl<L: Provider> Circuit<L> {
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
    async fn start(id: u64, mut stream: TcpStream, library: L) {
        info!("{id}: Starting circuit with {:?}", stream.peer_addr());
        let client_version = Circuit::<L>::do_version_exchange(&mut stream)
            .await
            .unwrap();
        // Client version is the bare minimum we need to establish a valid circuit
        debug!("{id}: Got client version: {client_version}");
        let (monitor_value_available, mut monitor_updates) = mpsc::channel::<String>(32);
        let mut circuit = Circuit {
            id,
            last_message: Instant::now(),
            client_version,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
            library,
            channels: HashMap::new(),
            next_channel_id: 0,
            monitor_value_available,
        };

        // Now, everything else is based on responding to events
        loop {
            tokio::select! {
                pv_name = monitor_updates.recv() => match pv_name {
                    Some(pv_name) => match circuit.handle_monitor_update(&pv_name).await {
                        Ok(messages) => {
                            for msg in messages {
                                debug!("{id}: Writing subscription update: {msg:?}");
                                stream.write_all(&msg.as_bytes()).await.unwrap()
                            }
                        },
                        Err(msg) => {
                            error!("{id} Error: Unexpected Error message {msg:?}");
                            continue;
                        },
                    }
                    None => {
                        panic!("Update monitor got all endpoints closed");
                    }
                },
                message = Message::read_server_message(&mut stream) => {
                    let message = match message {
                        Ok(message) => message,
                        Err(MessageError::IO(io)) => {
                            if io.kind() != io::ErrorKind::UnexpectedEof {
                                error!("{id}: IO Error reading server message: {io}");
                            }
                            break;
                        }
                        Err(MessageError::UnknownCommandId(command_id)) => {
                            error!("{id}: Error: Receieved unknown command id: {command_id}");
                            continue;
                        }
                        Err(MessageError::ParsingError(msg)) => {
                            error!("{id}: Error: Incoming message parse error: {msg}");
                            continue;
                        }
                        Err(MessageError::UnexpectedMessage(msg)) => {
                            error!("{id}: Error: Got message from client that is invalid to receive on a server: {msg:?}");
                            continue;
                        }
                        Err(MessageError::IncorrectCommandId(msg, expect)) => {
                            panic!("{id}: Fatal error: Got {msg} instead of {expect}")
                        }
                        Err(MessageError::InvalidField(message)) => {
                            error!("{id}: Got invalid message field: {message}");
                            continue;
                        }
                        Err(MessageError::ErrorResponse(message)) => {
                            error!("{id}: Got reading server messages generated error response: {message}");
                            continue;
                        }
                    };
                    match circuit.handle_message(message).await {
                        Ok(messages) => {
                            for msg in messages {
                                stream.write_all(&msg.as_bytes()).await.unwrap()
                            }
                        }
                        Err(MessageError::UnexpectedMessage(msg)) => {
                            error!("{id}: Error: Unexpected message: {msg:?}");
                            continue;
                        }
                        Err(msg) => {
                            error!("{id} Error: Unexpected Error message {msg:?}");
                            continue;
                        }
                    };
                }
            }
        }

        // If out here, we are closing the channel
        info!("{id}: Closing circuit");
        let _ = stream.shutdown().await;
    }

    async fn handle_monitor_update(&mut self, pv_name: &str) -> Result<Vec<Message>, MessageError> {
        let c = self
            .channels
            .values_mut()
            .find(|v| v.name == pv_name)
            .unwrap();
        debug!(
            "{}: {}: Got update notification for PV {}",
            self.id, c.server_id, c.name
        );
        let subscription = c.subscription.as_mut().unwrap();
        let dbr = subscription.receiver.recv().await.unwrap();

        info!("Circuit got update notification: {dbr:?}");
        let (item_count, data) = dbr
            .convert_to(subscription.data_type)
            .unwrap()
            .to_bytes(NonZeroUsize::new(subscription.data_count));
        Ok(vec![Message::EventAddResponse(EventAddResponse {
            data_type: subscription.data_type,
            data_count: item_count as u32,
            subscription_id: subscription.subscription_id,
            status_code: ErrorCondition::Normal,
            data,
        })])
    }

    async fn handle_message(&mut self, message: Message) -> Result<Vec<Message>, MessageError> {
        let id = self.id;
        match message {
            Message::Echo => Ok(vec![Message::Echo]),
            Message::EventAdd(msg) => {
                debug!("{id}: {}: Got {:?}", msg.server_id, msg);
                let channel = &mut self.channels.get_mut(&msg.server_id).unwrap();

                let receiver = self
                    .library
                    .monitor_value(
                        &channel.name,
                        msg.data_type,
                        msg.data_count as usize,
                        msg.mask,
                        self.monitor_value_available.clone(),
                    )
                    .map_err(MessageError::ErrorResponse)?;

                channel.subscription = Some(PVSubscription {
                    data_type: msg.data_type,
                    data_count: msg.data_count as usize,
                    mask: msg.mask,
                    subscription_id: msg.subscription_id,
                    receiver,
                });
                Ok(Vec::new())
            }
            Message::ClientName(name) if self.client_user_name.is_none() => {
                info!("{id}: Got client username: {}", name.name);
                self.client_user_name = Some(name.name);
                Ok(Vec::default())
            }
            Message::HostName(name) if self.client_host_name.is_none() => {
                info!("{id}: Got client hostname: {}", name.name);
                self.client_host_name = Some(name.name);
                Ok(Vec::default())
            }
            Message::CreateChannel(message) => {
                info!(
                    "{id}: Got request to create channel to: {}",
                    message.channel_name
                );
                let (messages, channel) = self.create_channel(message);
                if let Ok(channel) = channel {
                    self.channels.insert(channel.server_id, channel);
                }
                Ok(messages)
            }
            Message::ClearChannel(message) => {
                info!("{id}:{}: Request to clear channel", message.server_id);
                self.channels.remove(&message.server_id);
                Ok(Vec::default())
            }
            Message::ReadNotify(msg) => {
                info!("{id}:{}: ReadNotify request: {:?}", msg.server_id, msg);
                match self.do_read(&msg) {
                    Ok(r) => {
                        debug!("Sending response: {r:?}");
                        // self.stream.write_all(&r.as_bytes()).await?
                        Ok(vec![Message::ReadNotifyResponse(r)])
                    }

                    Err(e) => {
                        // Send an error in response to the read
                        let err = ECAError::new(e, msg.client_ioid, Message::ReadNotify(msg));
                        error!("Returning error: {err:?}");
                        // self.stream.write_all(&err.as_bytes()).await?
                        Ok(vec![Message::ECAError(err)])
                    }
                }
            }
            Message::Write(msg) => {
                debug!("{id}:{}: Write request: {:?}", msg.server_id, msg);
                if !self.do_write(&msg) {
                    Ok(vec![Message::ECAError(ECAError::new(
                        ErrorCondition::PutFail,
                        msg.client_ioid,
                        Message::Write(msg),
                    ))])
                } else {
                    Ok(Vec::default())
                }
            }
            msg => Err(MessageError::UnexpectedMessage(msg)),
        }
    }

    fn do_read(&self, request: &ReadNotify) -> Result<ReadNotifyResponse, ErrorCondition> {
        let channel = &self.channels[&request.server_id];
        let pv = self
            .library
            .read_value(&channel.name, Some(request.data_type))
            .unwrap();

        // Read the data into a Vec<u8>
        let (data_count, data) = pv
            .convert_to(request.data_type)?
            .to_bytes(NonZeroUsize::new(request.data_count as usize));
        Ok(request.respond(data_count, data))
    }

    fn do_write(&mut self, request: &Write) -> bool {
        assert!(request.data_type == DBR_BASIC_STRING);
        let channel = self.channels.get(&request.server_id).unwrap();

        let Ok(dbr) = Dbr::from_bytes(
            request.data_type,
            request.data_count as usize,
            &request.data,
        ) else {
            return false;
        };
        debug!("Got write request: {dbr:?}");
        self.library.write_value(&channel.name, dbr).is_ok()
    }

    fn create_channel(&mut self, message: CreateChannel) -> (Vec<Message>, Result<Channel, ()>) {
        let Ok(pv) = self.library.read_value(&message.channel_name, None) else {
            warn!(
                "Got a request for channel to '{}', which we do not appear to have.",
                message.channel_name
            );
            return (
                vec![Message::CreateChannelFailure(message.respond_failure())],
                Err(()),
            );
        };
        let access_rights = AccessRights {
            client_id: message.client_id,
            access_rights: self.library.get_access_right(
                &message.channel_name,
                self.client_user_name.as_deref(),
                self.client_host_name.as_deref(),
            ),
        };
        let id = self.next_channel_id;
        self.next_channel_id += 1;
        let createchan = CreateChannelResponse {
            data_count: pv.value().get_count() as u32,
            data_type: pv.data_type().basic_type,
            client_id: message.client_id,
            server_id: id,
        };
        info!(
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
                subscription: None,
            }),
        )
    }
}

pub struct ServerBuilder<L: Provider> {
    beacon_port: u16,
    search_port: u16,
    connection_port: Option<u16>,
    provider: L,
}

impl<L: Provider> ServerBuilder<L> {
    pub fn new(provider: L) -> ServerBuilder<L> {
        ServerBuilder {
            beacon_port: 5065,
            search_port: 5064,
            connection_port: None,
            provider,
        }
    }
    pub fn beacon_port(mut self, port: u16) -> ServerBuilder<L> {
        self.beacon_port = port;
        self
    }
    pub fn search_port(mut self, port: u16) -> ServerBuilder<L> {
        self.search_port = port;
        self
    }
    pub fn connection_port(mut self, port: u16) -> ServerBuilder<L> {
        self.connection_port = Some(port);
        self
    }
    pub async fn start(self) -> io::Result<Server<L>> {
        let mut server = Server {
            beacon_port: self.beacon_port,
            search_port: self.search_port,
            connection_port: self.connection_port,
            library_provider: self.provider,
            last_beacon: Instant::now(),
            beacon_id: 0,
            next_circuit_id: 0,
            circuits: Vec::new(),
            shutdown: CancellationToken::new(),
        };
        server.listen().await?;
        Ok(server)
    }
}
