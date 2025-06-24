#![allow(dead_code)]

use core::str;
use pnet::datalink;
use std::{
    collections::HashMap,
    io::{self, Cursor},
    net::{IpAddr, Ipv4Addr},
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UdpSocket},
    sync::{broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;

use crate::{
    database::{DBRType, Dbr, DBR_BASIC_STRING},
    messages::{
        self, parse_search_packet, AccessRights, AsBytes, CAMessage, CreateChannel,
        CreateChannelResponse, ECAError, ErrorCondition, EventAddResponse, Message, MessageError,
        MonitorMask, ReadNotify, ReadNotifyResponse, Write,
    },
    new_reusable_udp_socket,
    provider::Provider,
};

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
        Some(port) => tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await,
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
                println!(
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
                        println!("Sending {} search results", replies.len());
                    }
                } else {
                    println!("Got unparseable search message from {}", origin);
                }
            }
        });
    }

    fn handle_tcp_connections(&mut self, listener: TcpListener) {
        let library = self.library_provider.clone();
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
    data_type: DBRType,
    data_count: usize,
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
        println!("{id}: Starting circuit with {:?}", stream.peer_addr());
        let client_version = Circuit::<L>::do_version_exchange(&mut stream)
            .await
            .unwrap();
        // Client version is the bare minimum we need to establish a valid circuit
        println!("{id}: Got client version: {}", client_version);
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
                    Some(pv_name) => circuit.handle_monitor_update(&pv_name).await,
                    None => {
                        panic!("Update monitor got all endpoints closed");
                    }
                },
                message = Message::read_server_message(&mut stream) => {
                    let message = match message {
                        Ok(message) => message,
                        Err(MessageError::IO(io)) => {
                            if io.kind() != io::ErrorKind::UnexpectedEof {
                                println!("{id}: IO Error reading server message: {}", io);
                            }
                            break;
                        }
                        Err(MessageError::UnknownCommandId(command_id)) => {
                            println!("{id}: Error: Receieved unknown command id: {command_id}");
                            continue;
                        }
                        Err(MessageError::ParsingError(msg)) => {
                            println!("{id}: Error: Incoming message parse error: {}", msg);
                            continue;
                        }
                        Err(MessageError::UnexpectedMessage(msg)) => {
                            println!("{id}: Error: Got message from client that is invalid to receive on a server: {msg:?}");
                            continue;
                        }
                        Err(MessageError::IncorrectCommandId(msg, expect)) => {
                            panic!("{id}: Fatal error: Got {msg} instead of {expect}")
                        }
                        Err(MessageError::InvalidField(message)) => {
                            println!("{id}: Got invalid message field: {message}");
                            continue;
                        }
                        Err(MessageError::ErrorResponse(message)) => {
                            println!("{id}: Got reading server messages generated error response: {message}");
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
                            println!("{id}: Error: Unexpected message: {:?}", msg);
                            continue;
                        }
                        Err(msg) => {
                            println!("{id} Error: Unexpected Error message {:?}", msg);
                            continue;
                        }
                    };
                }
            }
        }

        // If out here, we are closing the channel
        println!("{id}: Closing circuit");
        let _ = stream.shutdown().await;
    }

    async fn handle_monitor_update(&mut self, pv_name: &str) {
        let c = self
            .channels
            .values_mut()
            .find(|v| v.name == pv_name)
            .unwrap();
        println!(
            "{}: {}: Got update notification for PV {}",
            self.id, c.server_id, c.name
        );
        let sub = c
            .subscription
            .as_mut()
            .unwrap()
            .receiver
            .recv()
            .await
            .unwrap();
        // .try_recv()
        // .unwrap();

        println!("Circuit got update notification: {:?}", sub);
        // let sub = c.subscription.as_ref().unwrap().receiver.recv().await.unwrap();
        // let sub = c.subscription.as_
    }

    async fn handle_message(&mut self, message: Message) -> Result<Vec<Message>, MessageError> {
        let id = self.id;
        match message {
            Message::Echo => Ok(vec![Message::Echo]),
            Message::EventAdd(msg) => {
                println!("{id}: {}: Got {:?}", msg.server_id, msg);
                let channel = &mut self.channels.get_mut(&msg.server_id).unwrap();

                let mut receiver = self
                    .library
                    .monitor_value(
                        &channel.name,
                        msg.mask,
                        self.monitor_value_available.clone(),
                    )
                    .map_err(MessageError::ErrorResponse)?;
                // Calling monitor_value inserts the first message into the receiver
                let first_value = &receiver.recv().await.unwrap();

                channel.subscription = Some(PVSubscription {
                    data_type: msg.data_type,
                    data_count: msg.data_count as usize,
                    mask: msg.mask,
                    receiver,
                });
                Ok(vec![Message::EventAddResponse(EventAddResponse {
                    data_type: msg.data_type,
                    data_count: msg.data_count,
                    subscription_id: msg.subscription_id,
                    status_code: ErrorCondition::Normal,
                    data: first_value
                        .convert_to(msg.data_type.basic_type)
                        .unwrap()
                        .encode_value(msg.data_type, msg.data_count as usize)
                        .unwrap()
                        .1,
                })])
            }
            Message::ClientName(name) if self.client_user_name.is_none() => {
                println!("{id}: Got client username: {}", name.name);
                self.client_user_name = Some(name.name);
                Ok(Vec::default())
            }
            Message::HostName(name) if self.client_host_name.is_none() => {
                println!("{id}: Got client hostname: {}", name.name);
                self.client_host_name = Some(name.name);
                Ok(Vec::default())
            }
            Message::CreateChannel(message) => {
                println!(
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
                println!("{id}:{}: Request to clear channel", message.server_id);
                self.channels.remove(&message.server_id);
                Ok(Vec::default())
            }
            Message::ReadNotify(msg) => {
                println!("{id}:{}: ReadNotify request: {:?}", msg.server_id, msg);
                match self.do_read(&msg) {
                    Ok(r) => {
                        println!("Sending response: {r:?}");
                        // self.stream.write_all(&r.as_bytes()).await?
                        Ok(vec![Message::ReadNotifyResponse(r)])
                    }

                    Err(e) => {
                        // Send an error in response to the read
                        let err = ECAError::new(e, msg.client_ioid, Message::ReadNotify(msg));
                        println!("Returning error: {err:?}");
                        // self.stream.write_all(&err.as_bytes()).await?
                        Ok(vec![Message::ECAError(err)])
                    }
                }
            }
            Message::Write(msg) => {
                println!("{id}:{}: Write request: {:?}", msg.server_id, msg);
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
        let (data_count, data) = pv.encode_value(request.data_type, request.data_count as usize)?;
        Ok(request.respond(data_count, data))
    }

    fn do_write(&mut self, request: &Write) -> bool {
        assert!(request.data_type == DBR_BASIC_STRING);
        let channel = self.channels.get(&request.server_id).unwrap();

        // Slightly unsure of formats here... so let's manually divide into strings here
        let data: Vec<&str> = request
            .data
            .chunks(40)
            .map(|d| {
                let strlen = d.iter().position(|&c| c == 0x00).unwrap();
                str::from_utf8(&d[0..strlen]).unwrap()
            })
            .collect();
        // println!("Got write request: {:?}", data);
        self.library.write_value(&channel.name, &data).is_ok()
    }

    fn create_channel(&mut self, message: CreateChannel) -> (Vec<Message>, Result<Channel, ()>) {
        let Ok(pv) = self.library.read_value(&message.channel_name, None) else {
            println!(
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
            data_count: pv.get_count() as u32,
            data_type: pv.get_native_type(),
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
