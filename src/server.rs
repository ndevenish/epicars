#![allow(dead_code)]

use core::str;
use pnet::datalink;
use std::{
    collections::HashMap,
    io::{self, Cursor},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream, UdpSocket},
    select,
    sync::{broadcast, mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};

use crate::{
    dbr::{Dbr, DbrType},
    messages::{
        self, AccessRights, AsBytes, CAMessage, CreateChannel, CreateChannelResponse, ECAError,
        ErrorCondition, EventAddResponse, Message, MessageError, MonitorMask, ReadNotify,
        ReadNotifyResponse, WriteNotify, parse_search_packet,
    },
    providers::Provider,
    utils::{
        get_default_beacon_period, get_default_beacon_port, get_default_server_port,
        new_reusable_udp_socket,
    },
};

/// Serve data to CA clients by managing the Circuit/Channel lifecycles and interfacing with [`Provider`].
pub struct Server<L: Provider> {
    /// Broadcast port to sent beacons
    beacon_port: u16,
    /// Port to receive search queries on
    search_port: u16,
    /// Port to receive connections on, if specified
    connection_port: Option<u16>,
    /// The beacon ID of the last beacon broadcast
    beacon_id: u32,
    next_circuit_id: u64,
    circuits: Vec<Circuit<L>>,
    shutdown: CancellationToken,
    library_provider: L,
    tasks: JoinSet<Result<(), io::Error>>,
    lifecycle_events: broadcast::Sender<ServerEvent>,
}

impl<L: Provider> Default for Server<L> {
    fn default() -> Self {
        Self {
            beacon_port: Default::default(),
            search_port: Default::default(),
            connection_port: Default::default(),
            beacon_id: Default::default(),
            next_circuit_id: Default::default(),
            circuits: Default::default(),
            shutdown: Default::default(),
            library_provider: Default::default(),
            tasks: Default::default(),
            lifecycle_events: broadcast::Sender::new(1024),
        }
    }
}
pub struct ServerHandle {
    cancel: CancellationToken,
    handle: JoinHandle<Result<(), io::Error>>,
    events: broadcast::Receiver<ServerEvent>,
    /// Connection and Search ports this server has launched with
    ports: (u16, u16),
}

impl ServerHandle {
    pub async fn join(&mut self) -> Result<(), io::Error> {
        (&mut self.handle).await.unwrap()
    }

    pub async fn stop(mut self) -> Result<(), io::Error> {
        self.cancel.cancel();
        self.join().await
    }

    /// Get a subscriber to server events
    pub fn listen_to_events(&self) -> broadcast::Receiver<ServerEvent> {
        self.events.resubscribe()
    }
    /// Get the actual port this server accepts connections on
    pub fn connection_port(&self) -> u16 {
        self.ports.0
    }
    /// Get the actual port this server accepts UDP searches on
    pub fn search_port(&self) -> u16 {
        self.ports.1
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// Lifecycle events for things that happen on the server
#[derive(Clone)]
pub enum ServerEvent {
    /// A client has connected
    CircuitOpened {
        id: u64,
        peer: SocketAddr,
    },
    /// A client has disconnected, or been disconnected
    CircuitClose {
        id: u64,
    },
    /// The client has fully identified themselves
    ClientIdentified {
        circuit_id: u64,
        client_hostname: String,
        client_username: String,
    },
    /// Client has opened a channel to a specific PV
    CreateChannel {
        circuit_id: u64,
        channel_id: u32,
        channel_name: String,
    },
    ClearChannel {
        circuit_id: u64,
        channel_id: u32,
    },
    /// A Client has attempted to read a PV
    Read {
        circuit_id: u64,
        channel_id: u32,
        success: bool,
    },
    /// A client has attempted to write to a PV
    Write {
        circuit_id: u64,
        channel_id: u32,
        success: bool,
    },
    /// A client has subscribed to events
    Subscribe {
        circuit_id: u64,
        channel_id: u32,
    },
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
            // Try binding the default port on TCP (usually 5064)
            if let Ok(socket) =
                tokio::net::TcpListener::bind(("0.0.0.0", get_default_server_port())).await
            {
                return Ok(socket);
            }
            tokio::net::TcpListener::bind("0.0.0.0:0").await
        }
        Some(port) => tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await,
    }
}

impl<L: Provider> Server<L> {
    async fn listen(
        mut self,
        conn_info: oneshot::Sender<(u16, u16)>,
        broadcast_beacons: bool,
    ) -> Result<(), std::io::Error> {
        // Create the TCP listener first so we know what port to advertise
        let request_port = self.connection_port;
        let connection_socket = try_bind_ports(request_port).await?;

        // Whatever we ended up with, we want to advertise
        let listen_port = connection_socket.local_addr().unwrap().port();

        let search_port = self
            .listen_for_searches(listen_port)
            .await
            .map_err(|_| std::io::Error::other("Error: Search startup failed to get port"))?;
        let _ = conn_info.send((listen_port, search_port));
        self.handle_tcp_connections(connection_socket);

        if broadcast_beacons {
            self.broadcast_beacons(listen_port).await?;
        }

        // Join all tasks, and take the first io::Error as the error to return
        let results: Vec<_> = self
            .tasks
            .join_all()
            .await
            .into_iter()
            .filter_map(Result::err)
            .collect();
        if let Some(e) = results.into_iter().next() {
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn broadcast_beacons(&mut self, connection_port: u16) -> io::Result<()> {
        let beacon_port = self.beacon_port;
        let broadcast = UdpSocket::bind("0.0.0.0:0").await?;
        let cancel = self.shutdown.clone();
        let beacon_period = Duration::from_secs_f32(get_default_beacon_period());
        self.tasks.spawn(async move {
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
                debug!(
                    "Broadcasting beacon {} to {} interfaces: {:?}",
                    message.beacon_id,
                    broadcast_ips.len(),
                    broadcast_ips,
                );
                message.beacon_id = message.beacon_id.wrapping_add(1);
                select! {
                    _ = tokio::time::sleep(beacon_period) => (),
                    _ = cancel.cancelled() => break,
                };
            }
            Ok(())
        });
        Ok(())
    }

    /// Start listening for searches. Return the port we ended up binding.
    async fn listen_for_searches(
        &mut self,
        connection_port: u16,
    ) -> Result<u16, oneshot::error::RecvError> {
        let search_port = self.search_port;
        let library_provider = self.library_provider.clone();
        let cancel = self.shutdown.clone();

        let (tx, rx) = oneshot::channel();

        self.tasks.spawn(async move {
            let mut buf: Vec<u8> = vec![0; 0xFFFF];
            let listener = match new_reusable_udp_socket(format!("0.0.0.0:{search_port}")) {
                Ok(x) => x,
                Err(e) => {
                    panic!("Failed to create reusable UDP socket 0.0.0.0:{search_port}: {e}");
                }
            };
            let _ = tx.send(listener.local_addr().unwrap().port());
            debug!(
                "Listening for searches on {:?}",
                listener.local_addr().unwrap()
            );
            // Keep track of recent searches and source ports, to help reject duplicate
            // messages coming in on multiple network ports.
            let mut recent_searches: Vec<(Instant, u16, Vec<messages::Search>)> = Vec::new();

            loop {
                // Receieve a message, or cancel
                let (size, origin) = select! {
                    r = listener.recv_from(&mut buf) => r,
                    _ = cancel.cancelled() => break,
                }
                .unwrap();
                let msg_buf = &buf[..size];
                if let Ok(searches) = parse_search_packet(msg_buf) {
                    trace!("Gotten searches: {searches:?}");
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
            Ok(())
        });
        rx.await
    }

    fn handle_tcp_connections(&mut self, listener: TcpListener) {
        let library = self.library_provider.clone();
        let cancel_inner = self.shutdown.clone();
        let lifecycle = self.lifecycle_events.clone();
        self.tasks.spawn(async move {
            let mut id = 0;
            let mut tasks = JoinSet::new();
            loop {
                debug!(
                    "Waiting to accept TCP connections on {}",
                    listener.local_addr().unwrap()
                );
                let (connection, client) = match select! {
                    _ = cancel_inner.cancelled() => break,
                    x = listener.accept() => x,
                } {
                    Ok(x) => x,
                    Err(e) => {
                        warn!("Failed to accept incoming connection: {e}");
                        continue;
                    }
                };
                debug!("  Got new stream from {client}");
                let circuit_library = library.clone();
                let cancel = cancel_inner.clone();
                let inner_lifecycle = lifecycle.clone();
                tasks.spawn(async move {
                    Circuit::start(id, connection, circuit_library, cancel, inner_lifecycle).await;
                });
                id += 1;
            }
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result
                    && e.is_panic()
                {
                    debug!("Circuit Panicked! Resuming unwind");
                    std::panic::resume_unwind(e.into_panic());
                }
            }
            tasks.join_all().await;
            Ok(())
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
    client_addr: SocketAddr,
    library: L,
    channels: HashMap<u32, Channel>,
    next_channel_id: u32,
    monitor_value_available: mpsc::Sender<String>,
    /// For broadcasting lifecycle events
    lifecycle_events: broadcast::Sender<ServerEvent>,
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
    async fn start(
        id: u64,
        mut stream: TcpStream,
        library: L,
        cancel: CancellationToken,
        lifecycle: broadcast::Sender<ServerEvent>,
    ) {
        let Ok(peer_addr) = stream.peer_addr() else {
            debug!("Peer disconnected before circuit could be started!");
            return;
        };
        debug!("{id}: Starting circuit with {:?}", stream.peer_addr());
        let client_version = Circuit::<L>::do_version_exchange(&mut stream)
            .await
            .unwrap();
        // Client version is the bare minimum we need to establish a valid circuit
        debug!("{id}: Got client version: {client_version}");
        let _ = lifecycle.send(ServerEvent::CircuitOpened {
            id,
            peer: peer_addr,
        });

        let (monitor_value_available, mut monitor_updates) = mpsc::channel::<String>(32);
        let mut circuit = Circuit {
            id,
            last_message: Instant::now(),
            client_version,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
            client_addr: peer_addr,
            library,
            channels: HashMap::new(),
            next_channel_id: 0,
            monitor_value_available,
            lifecycle_events: lifecycle,
        };

        // Now, everything else is based on responding to events
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
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
                                trace!("Writing response message: {msg:?}");
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
        debug!("{id}: Closing circuit");
        let _ = stream.shutdown().await;
        let _ = circuit
            .lifecycle_events
            .send(ServerEvent::CircuitClose { id });
    }

    async fn handle_monitor_update(&mut self, pv_name: &str) -> Result<Vec<Message>, MessageError> {
        let Some(c) = self.channels.values_mut().find(|v| v.name == pv_name) else {
            error!(
                "Could not find pv named {pv_name} in channels: {:?}",
                self.channels
            );
            return Err(MessageError::ErrorResponse(ErrorCondition::Internal));
        };
        debug!(
            "{}: {}: Got update notification for PV {}",
            self.id, c.server_id, c.name
        );
        let Some(subscription) = c.subscription.as_mut() else {
            // This was probably sent before closing
            trace!("Got monitor update for closed subscription!!");
            return Ok(Vec::new());
        };
        let dbr = subscription.receiver.recv().await.unwrap();

        debug!("Circuit got update notification: {dbr:?}");
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
                        (self.id << 32) | channel.server_id as u64,
                        msg.data_type,
                        msg.data_count as usize,
                        msg.mask,
                        self.monitor_value_available.clone(),
                    )
                    .map_err(MessageError::ErrorResponse)?;

                let _ = self.lifecycle_events.send(ServerEvent::Subscribe {
                    circuit_id: self.id,
                    channel_id: msg.server_id,
                });
                channel.subscription = Some(PVSubscription {
                    data_type: msg.data_type,
                    data_count: msg.data_count as usize,
                    mask: msg.mask,
                    subscription_id: msg.subscription_id,
                    receiver,
                });
                // Send back an initial value\
                let name = channel.name.clone();
                let value = self.do_read_dbr(&name, msg.data_type);
                Ok(vec![
                    msg.respond(&value)
                        .map_err(MessageError::ErrorResponse)?
                        .into(),
                ])
            }
            Message::EventCancel(msg) => {
                debug!("{id}: {}: Got {:?}", msg.server_id, msg);
                let channel = &mut self.channels.get_mut(&msg.server_id).unwrap();
                self.library.cancel_monitor_value(
                    &channel.name,
                    (self.id << 32) | channel.server_id as u64,
                    msg.data_type,
                    msg.data_count as usize,
                );
                channel.subscription = None;
                Ok(vec![msg.response().into()])
            }
            Message::ClientName(name) if self.client_user_name.is_none() => {
                debug!("{id}: Got client username: {}", name.name);
                self.client_user_name = Some(name.name);
                if self.client_host_name.is_some() {
                    let _ = self.lifecycle_events.send(ServerEvent::ClientIdentified {
                        circuit_id: self.id,
                        client_hostname: self.client_host_name.clone().unwrap(),
                        client_username: self.client_user_name.clone().unwrap(),
                    });
                }
                Ok(Vec::default())
            }
            Message::HostName(name) if self.client_host_name.is_none() => {
                debug!("{id}: Got client hostname: {}", name.name);
                self.client_host_name = Some(name.name);
                if self.client_user_name.is_some() {
                    let _ = self.lifecycle_events.send(ServerEvent::ClientIdentified {
                        circuit_id: self.id,
                        client_hostname: self.client_host_name.clone().unwrap(),
                        client_username: self.client_user_name.clone().unwrap(),
                    });
                }
                Ok(Vec::default())
            }
            Message::CreateChannel(message) => {
                debug!(
                    "{id}: Got request to create channel to: {}",
                    message.channel_name
                );
                let (messages, channel) = self.create_channel(message);
                if let Ok(channel) = channel {
                    let _ = self.lifecycle_events.send(ServerEvent::CreateChannel {
                        circuit_id: self.id,
                        channel_id: channel.server_id,
                        channel_name: channel.name.clone(),
                    });
                    self.channels.insert(channel.server_id, channel);
                }
                Ok(messages)
            }
            Message::ClearChannel(message) => {
                debug!("{id}:{}: Request to clear channel", message.server_id);
                self.channels.remove(&message.server_id);
                let _ = self.lifecycle_events.send(ServerEvent::ClearChannel {
                    circuit_id: self.id,
                    channel_id: message.server_id,
                });
                Ok(Vec::default())
            }
            Message::ReadNotify(msg) => {
                debug!("{id}:{}: ReadNotify request: {:?}", msg.server_id, msg);
                match self.do_read(&msg) {
                    Ok(r) => {
                        debug!("Sending response: {r:?}");
                        let _ = self.lifecycle_events.send(ServerEvent::Read {
                            circuit_id: self.id,
                            channel_id: msg.server_id,
                            success: true,
                        });
                        Ok(vec![Message::ReadNotifyResponse(r)])
                    }

                    Err(e) => {
                        let _ = self.lifecycle_events.send(ServerEvent::Read {
                            circuit_id: self.id,
                            channel_id: msg.server_id,
                            success: false,
                        });
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
                if !self.do_write(&msg.clone().into()) {
                    Ok(vec![Message::ECAError(ECAError::new(
                        ErrorCondition::PutFail,
                        msg.client_ioid,
                        Message::Write(msg),
                    ))])
                } else {
                    Ok(Vec::default())
                }
            }
            Message::WriteNotify(msg) => {
                debug!("{id}:{}: Write request: {:?}", msg.server_id, msg);
                if !self.do_write(&msg) {
                    Ok(vec![msg.respond(0).into()])
                } else {
                    Ok(vec![msg.respond(1).into()])
                }
            }
            msg => Err(MessageError::UnexpectedMessage(msg)),
        }
    }

    fn do_read_dbr(&self, name: &str, data_type: DbrType) -> Dbr {
        self.library.read_value(name, Some(data_type)).unwrap()
    }

    fn do_read(&self, request: &ReadNotify) -> Result<ReadNotifyResponse, ErrorCondition> {
        let channel = &self.channels[&request.server_id];
        let pv = self.do_read_dbr(&channel.name, request.data_type);

        // Read the data into a Vec<u8>
        let (data_count, data) = pv
            .convert_to(request.data_type)?
            .to_bytes(NonZeroUsize::new(request.data_count as usize));
        Ok(request.respond(data_count, data))
    }

    fn do_write(&mut self, request: &WriteNotify) -> bool {
        let channel = self.channels.get(&request.server_id).unwrap();
        let Ok(dbr) = Dbr::from_bytes(
            request.data_type,
            request.data_count as usize,
            &request.data,
        ) else {
            let _ = self.lifecycle_events.send(ServerEvent::Write {
                circuit_id: self.id,
                channel_id: request.server_id,
                success: false,
            });
            return false;
        };
        debug!("Got write request: {dbr:?}");
        let success = self.library.write_value(&channel.name, dbr).is_ok();
        let _ = self.lifecycle_events.send(ServerEvent::Write {
            circuit_id: self.id,
            channel_id: request.server_id,
            success,
        });
        success
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
        debug!(
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

/// Construct a [Server] object by setting up multiple aspects before running
pub struct ServerBuilder<L: Provider> {
    beacon_port: u16,
    search_port: u16,
    connection_port: Option<u16>,
    provider: L,
    cancellation_token: CancellationToken,
    beacons: bool,
}

impl<L: Provider> ServerBuilder<L> {
    pub fn new(provider: L) -> ServerBuilder<L> {
        ServerBuilder {
            beacon_port: get_default_beacon_port(),
            search_port: get_default_server_port(),
            connection_port: None,
            provider,
            cancellation_token: CancellationToken::new(),
            beacons: true,
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
    pub fn beacons(mut self, active: bool) -> ServerBuilder<L> {
        self.beacons = active;
        self
    }
    pub fn connection_port(mut self, port: u16) -> ServerBuilder<L> {
        self.connection_port = Some(port);
        self
    }
    pub fn cancellation_token(mut self, cancel: CancellationToken) -> ServerBuilder<L> {
        self.cancellation_token = cancel;
        self
    }

    pub async fn start(self) -> Result<ServerHandle, ()> {
        let shutdown = self.cancellation_token.clone();
        let server = Server {
            beacon_port: self.beacon_port,
            search_port: self.search_port,
            connection_port: self.connection_port,
            library_provider: self.provider,
            shutdown,
            ..Default::default()
        };
        let events = server.lifecycle_events.subscribe();
        let (tx, rx) = oneshot::channel();
        let handle = tokio::spawn(async move { server.listen(tx, self.beacons).await });
        let ports = rx.await.map_err(|_| ())?;

        Ok(ServerHandle {
            cancel: self.cancellation_token,
            events,
            handle,
            ports,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Provider, ServerBuilder,
        dbr::{Dbr, DbrType},
        messages::ErrorCondition,
    };

    #[derive(Default, Clone, Debug)]
    struct BlankProvider {}
    impl Provider for BlankProvider {
        fn provides(&self, _pv_name: &str) -> bool {
            false
        }

        fn read_value(
            &self,
            _pv_name: &str,
            _requested_type: Option<DbrType>,
        ) -> Result<Dbr, ErrorCondition> {
            Err(ErrorCondition::UnavailInServ)
        }
    }

    #[tokio::test]
    async fn test_random_bind() {
        // BlankProvider{}
        let handle = ServerBuilder::new(BlankProvider {})
            .connection_port(0)
            .search_port(0)
            .start()
            .await
            .unwrap();
        assert_ne!(handle.connection_port(), 0, "Connection port");
        assert_ne!(handle.search_port(), 0, "Search port");
    }
}
