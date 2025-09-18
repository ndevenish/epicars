#![allow(dead_code)]

use std::{
    cmp::max,
    collections::{HashMap, hash_map::Entry},
    io::ErrorKind,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, split},
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_stream::StreamExt;
use tokio_util::{codec::FramedRead, sync::CancellationToken};
use tracing::{debug, debug_span, error, trace, warn};

use crate::{
    client::{Searcher, SearcherBuilder, searcher::CouldNotFindError},
    dbr::{Dbr, DbrBasicType, DbrCategory, DbrType, DbrValue},
    messages::{self, Access, CAMessage, ClientMessage, Message, MonitorMask, RsrvIsUp},
    utils::{
        get_default_beacon_port, get_default_connection_timeout, get_default_server_port,
        new_reusable_udp_socket, wrapping_inplace_add,
    },
};

enum CircuitRequest {
    GetChannel(String, oneshot::Sender<Result<ChannelInfo, ClientError>>),
    /// Read a single value from the server
    Read {
        channel: u32,
        length: usize,
        category: DbrCategory,
        reply: oneshot::Sender<Result<Dbr, ClientError>>,
    },
    /// Start a subscription to a PV on the server
    Subscribe {
        channel: u32,
        length: usize,
        dbr_type: DbrType,
        reply: oneshot::Sender<Result<broadcast::Receiver<Dbr>, ClientError>>,
    },
    Unsubscribe {
        channel: u32,
    },
}

struct Circuit {
    address: SocketAddr,
    cancel: CancellationToken,
    requests_tx: mpsc::Sender<CircuitRequest>,
}

impl Circuit {
    async fn connect(
        address: &SocketAddr,
        client_name: Option<&str>,
        host_name: Option<&str>,
    ) -> Result<Self, ClientError> {
        debug!("Connecting new Circuit to {address}");
        let mut tcp = TcpStream::connect(address).await?;
        // Work out what to call ourselves
        let client_name = client_name
            .map(|u| u.to_string())
            .unwrap_or_else(whoami::username);
        let host_name = host_name
            .map(|h| h.to_string())
            .or_else(|| whoami::fallible::hostname().ok())
            .unwrap_or_else(|| client_name.clone());

        // Exchange version messages
        Message::write_all_messages(&[messages::Version::default().into()], &mut tcp).await?;
        Self::do_read_check_version(&mut tcp).await?;
        debug!("Done version exchange, sending identification messages");
        // Send the identification messages
        Message::write_all_messages(
            &[
                messages::ClientName { name: client_name }.into(),
                messages::HostName { name: host_name }.into(),
            ],
            &mut tcp,
        )
        .await?;

        let (requests_tx, requests_rx) = mpsc::channel(8);
        // Now we have a connected circuit, ready for lifecycle!
        let cancel = CancellationToken::new();

        // Make the internal object
        let inner_cancel = cancel.clone();
        let inner_address = *address;
        tokio::spawn(async move {
            CircuitInternal {
                address: inner_address,
                requests_rx,
                cancel: inner_cancel,
                next_cid: 0,
                channel_lookup: Default::default(),
                channels: Default::default(),
                pending_reads: Default::default(),
                last_echo_sent_at: Instant::now(),
                last_received_message_at: Instant::now(),
                pending_broadcasts: Default::default(),
                broadcast_receivers: Default::default(),
                broadcast_channels: Default::default(),
            }
            .circuit_lifecycle(tcp)
            .await;
        });

        debug!("Circuit Ready.");

        Ok(Circuit {
            address: *address,
            cancel,
            requests_tx,
        })
    }

    // async fn circuit_lifecycle(&mut self)
    /// Handle reading the Version packet from the stream, and checking we can handle it
    async fn do_read_check_version(socket: &mut TcpStream) -> Result<(), ClientError> {
        // Read the
        let mut ver_buf = [0u8; 16];
        socket.read_exact(&mut ver_buf).await?;
        let (_, server_version) = messages::Version::parse(&ver_buf)
            .map_err(|_| ClientError::ServerSentInvalidMessage)?;
        if !server_version.is_compatible() {
            Err(ClientError::ServerVersionMismatch(
                server_version.protocol_version,
            ))
        } else {
            Ok(())
        }
    }
    // async fn get_channel(&mut self, name : &str) -> Result<u32, ClientError> {
    //     self.
    // }

    async fn get_channel(&self, name: String) -> Result<ChannelInfo, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(CircuitRequest::GetChannel(name, tx))
            .await
            .map_err(|_| ClientError::ClientClosed)?;
        rx.await.map_err(|_| ClientError::ClientClosed)?
    }

    /// Request a PV from the circuit
    async fn read_pv(&self, name: &str) -> Result<Dbr, ClientError> {
        let channel = self.get_channel(name.to_owned()).await?;
        debug!("Circuit read_pv got channel: {channel:?}");
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(CircuitRequest::Read {
                channel: channel.cid,
                length: 0usize,
                category: DbrCategory::Time,
                reply: tx,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;
        rx.await.map_err(|_| ClientError::ClientClosed)?
    }

    async fn subscribe(&self, name: &str) -> Result<broadcast::Receiver<Dbr>, ClientError> {
        let channel = self.get_channel(name.to_string()).await?;
        debug!("Circuit subscribe got channel: {channel:?}");
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(CircuitRequest::Subscribe {
                channel: channel.cid,
                length: 0,
                dbr_type: DbrType {
                    basic_type: channel.native_type,
                    category: DbrCategory::Time,
                },
                reply: tx,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;
        rx.await.unwrap()
    }
}

#[derive(Debug, Default, Copy, Clone)]
enum ChannelState {
    #[default]
    Closed,
    SentCreate,
    Ready,
}

/// Summary of channel information
#[derive(Debug, Clone, Copy)]
struct ChannelInfo {
    state: ChannelState,
    native_type: DbrBasicType,
    native_count: u32,
    cid: u32,
    permissions: Access,
}

#[derive(Debug, Default)]
struct Channel {
    name: String,
    state: ChannelState,
    native_type: Option<DbrBasicType>,
    native_count: u32,
    cid: u32,
    sid: u32,
    permissions: Access,
    /// Watchers waiting for this channel to be open
    pending_open: Vec<oneshot::Sender<Result<ChannelInfo, ClientError>>>,
    next_ioid: u32,
    broadcast_receivers: Vec<u32>,
}

impl Channel {
    fn info(&self) -> ChannelInfo {
        ChannelInfo {
            state: self.state,
            native_type: self.native_type.unwrap(),
            native_count: self.native_count,
            cid: self.cid,
            permissions: self.permissions,
        }
    }
}

// Inner circuit state, used to hold async management data
struct CircuitInternal {
    /// A copy of the address we are connected to
    address: SocketAddr,
    /// When the last message was received. Used to calculate Echo timing.
    last_received_message_at: Instant,
    last_echo_sent_at: Instant,
    // requests_tx: mpsc::Sender<CircuitRequest>,
    requests_rx: mpsc::Receiver<CircuitRequest>,
    cancel: CancellationToken,
    next_cid: u32,
    channels: HashMap<u32, Channel>,
    channel_lookup: HashMap<String, u32>,
    /// Watchers waiting for specific reads
    pending_reads: HashMap<u32, (Instant, oneshot::Sender<Result<Dbr, ClientError>>)>,
    /// Broadcast subscriptions we have not had confirmed yet
    #[allow(clippy::type_complexity)] // TODO: Actually follow Clippy's advice here
    pending_broadcasts: HashMap<
        u32,
        (
            Instant,
            (
                usize,
                DbrType,
                oneshot::Sender<Result<broadcast::Receiver<Dbr>, ClientError>>,
            ),
        ),
    >,
    broadcast_receivers: HashMap<u32, (usize, DbrType, broadcast::Sender<Dbr>)>,
    broadcast_channels: HashMap<u32, u32>,
}

impl CircuitInternal {
    async fn circuit_lifecycle(&mut self, tcp: TcpStream) {
        debug!("Started circuit to {}", self.address);
        let (tcp_rx, mut tcp_tx) = split(tcp);
        let mut framed = FramedRead::with_capacity(tcp_rx, ClientMessage::default(), 16384usize);
        let activity_period = Duration::from_secs_f32(get_default_connection_timeout() / 2.0);
        loop {
            let next_timing_stop =
                max(self.last_echo_sent_at, self.last_received_message_at) + activity_period;
            let messages_out = select! {
                _ = self.cancel.cancelled() => break,
                incoming = framed.next() => match incoming {
                    Some(message) => match message {
                        Ok(message) => Some(self.handle_message(message)),
                        Err(e) => {
                            error!("Got error processing server message: {e}");
                            continue;
                        }
                    },
                    None => break,
                },
                request = self.requests_rx.recv() => match request {
                    None => break,
                    Some(req) => Some(self.handle_request(req).await)
                },
                _ = tokio::time::sleep_until(next_timing_stop.into()) => {
                    if self.last_echo_sent_at < self.last_received_message_at {
                        self.last_echo_sent_at = Instant::now();
                        Some(vec![Message::Echo])
                    } else {
                        // We sent an echo already, this is the termination time
                        error!("Received no reply from server, assuming connection dead");
                        break
                    }
                },
            };

            // Send any messages out
            if let Some(messages) = messages_out {
                for message in &messages {
                    trace!("Sending {message:?}");
                }
                if Message::write_all_messages(&messages, &mut tcp_tx)
                    .await
                    .is_err()
                {
                    error!("Failed to write messages to io stream, aborting");
                }
            }
        }
        self.cancel.cancel();
        let _ = tcp_tx.shutdown().await;
    }

    fn create_channel(&mut self, name: String) -> (&mut Channel, Vec<Message>) {
        // We need to open a new channel
        let cid = self.next_cid;
        self.next_cid = self.next_cid.wrapping_add(1);
        let channel = Channel {
            cid,
            state: ChannelState::SentCreate, // Or, about to, anyway
            next_ioid: 4242,
            ..Default::default()
        };
        let _span = debug_span!("create_channel", cid = cid).entered();
        debug!("Creating channel '{name}' cid: {cid}");
        self.channel_lookup.insert(name.clone(), cid);
        self.channels.insert(cid, channel);
        (
            self.channels.get_mut(&cid).unwrap(),
            vec![
                messages::CreateChannel {
                    client_id: cid,
                    channel_name: name,
                    ..Default::default()
                }
                .into(),
            ],
        )
    }

    async fn handle_request(&mut self, request: CircuitRequest) -> Vec<Message> {
        match request {
            CircuitRequest::GetChannel(name, sender) => {
                if let Some(id) = self.channel_lookup.get(&name) {
                    // We already have this channel.. let's check if it is open
                    let channel = self.channels.get_mut(id).unwrap();
                    match channel.state {
                        ChannelState::Closed => panic!("We should never see a closed channel?"),
                        ChannelState::SentCreate => channel.pending_open.push(sender),
                        ChannelState::Ready => {
                            // Already ready, just send it out
                            let _ = sender.send(Ok(channel.info()));
                        }
                    }
                    Vec::new()
                } else {
                    let (channel, messages) = self.create_channel(name);
                    channel.pending_open.push(sender);
                    // Pass the channel create messages back
                    messages
                }
            }
            CircuitRequest::Read {
                channel: cid,
                length,
                category,
                reply,
            } => {
                let Some(channel) = self.channels.get_mut(&cid) else {
                    let _ = reply.send(Err(ClientError::ChannelClosed));
                    return Vec::new();
                };
                let _span = debug_span!("handle_request", cid = cid).entered();
                // Send the read request
                let ioid = wrapping_inplace_add(&mut channel.next_ioid);
                debug!(
                    "Sending read request {ioid} for channel {cid} ({})",
                    channel.name
                );
                self.pending_reads.insert(ioid, (Instant::now(), reply));
                vec![
                    messages::ReadNotify {
                        data_type: DbrType {
                            basic_type: channel.native_type.unwrap(),
                            category,
                        },
                        data_count: length as u32,
                        server_id: channel.sid,
                        client_ioid: ioid,
                    }
                    .into(),
                ]
            }
            CircuitRequest::Subscribe {
                channel: cid,
                length,
                dbr_type,
                reply,
            } => {
                let Some(channel) = self.channels.get_mut(&cid) else {
                    let _ = reply.send(Err(ClientError::ChannelClosed));
                    return Vec::new();
                };
                let _span = debug_span!("handle_request", cid = cid).entered();
                let ioid = wrapping_inplace_add(&mut channel.next_ioid);
                self.pending_broadcasts
                    .insert(ioid, (Instant::now(), (length, dbr_type, reply)));
                self.broadcast_channels.insert(ioid, channel.cid);
                channel.broadcast_receivers.push(ioid);
                vec![
                    messages::EventAdd {
                        data_type: dbr_type,
                        data_count: length as u32,
                        server_id: channel.sid,
                        subscription_id: ioid,
                        mask: MonitorMask::default(),
                    }
                    .into(),
                ]
            }
            CircuitRequest::Unsubscribe { channel: _channel } => {
                todo!();
            }
        }
    }
    fn handle_message(&mut self, message: ClientMessage) -> Vec<Message> {
        self.last_received_message_at = Instant::now();
        trace!("Received message: {message:?}");
        match message {
            ClientMessage::AccessRights(msg) => {
                let _span = debug_span!("handle_message", cid = &msg.client_id).entered();
                let Some(channel) = self.channels.get_mut(&msg.client_id) else {
                    debug!("Got message for closed/uncreated channel");
                    return Vec::new();
                };
                debug!("Got AccessRights update: {}", msg.access_rights);
                channel.permissions = msg.access_rights;
                Vec::new()
            }
            ClientMessage::CreateChannelResponse(msg) => {
                let _span = debug_span!("handle_message", cid = &msg.client_id).entered();
                let Some(channel) = self.channels.get_mut(&msg.client_id) else {
                    debug!("Got message for closed/uncreated channel: {msg:?}");
                    return Vec::new();
                };
                channel.native_count = msg.data_count;
                channel.native_type = Some(msg.data_type);
                channel.state = ChannelState::Ready;
                channel.sid = msg.server_id;
                let info = channel.info();

                for sender in channel.pending_open.drain(..) {
                    let _ = sender.send(Ok(info));
                }
                Vec::new()
            }
            ClientMessage::CreateChannelFailure(msg) => {
                let Some(mut channel) = self.channels.remove(&msg.client_id) else {
                    warn!(
                        "Got channel failure message for a nonexistent channel {}",
                        msg.client_id
                    );
                    return Vec::new();
                };
                for sender in channel.pending_open.drain(..) {
                    let _ = sender.send(Err(ClientError::ChannelCreateFailed));
                }
                Vec::new()
            }
            ClientMessage::ReadNotifyResponse(msg) => {
                let Some((_, reply_tx)) = self.pending_reads.remove(&msg.client_ioid) else {
                    warn!("Got ReadNotifyResponse for apparently unknown read request?! {msg:?}");
                    return Vec::new();
                };
                debug!("Processing message {msg:?}");
                match Dbr::from_bytes(msg.data_type, msg.data_count as usize, &msg.data) {
                    Ok(dbr) => {
                        let _ = reply_tx.send(Ok(dbr));
                    }
                    Err(_) => {
                        let _ = reply_tx.send(Err(ClientError::ServerSentInvalidMessage));
                    }
                };
                Vec::new()
            }
            ClientMessage::Echo => Vec::new(), // Echo just bumps our last_received message counter
            ClientMessage::Version(_msg) => {
                warn!("Got unexpected VERSION message in normal circuit lifecycle.");
                Vec::new()
            }
            ClientMessage::EventAddResponse(msg) => {
                let Some(channel_id) = self.broadcast_channels.get(&msg.subscription_id) else {
                    warn!(
                        "Got subscription message without associated channel: {}",
                        msg.subscription_id
                    );
                    return Vec::new();
                };
                let _span = debug_span!("handle_message", cid = channel_id).entered();
                if msg.data.is_empty() {
                    debug!(
                        "Got empty EventAddResponse: Purging subscription {}",
                        msg.subscription_id
                    );
                    // This is a special case: The server is requesting termination
                    // of the subscription (possibly because we asked it to). Shut down
                    // the channel monitors.
                    if let Some(cid) = self.broadcast_channels.get(&msg.subscription_id) {
                        self.channels
                            .get_mut(cid)
                            .unwrap()
                            .broadcast_receivers
                            .retain(|s| *s != msg.subscription_id);
                    }
                    self.broadcast_channels.remove(&msg.subscription_id);
                    self.broadcast_receivers.remove(&msg.subscription_id);
                    self.pending_broadcasts.remove(&msg.subscription_id);
                    return Vec::new();
                }
                let Ok(dbr) = Dbr::from_bytes(msg.data_type, msg.data_count as usize, &msg.data)
                else {
                    error!("Got invalid subscription response from server: {msg:?}");
                    return Vec::new();
                };
                debug!(
                    "Got subscription {} response: {:?}",
                    msg.subscription_id, dbr
                );
                // Check - this might be the first
                if let Some((_, (length, dbrtype, reply))) =
                    self.pending_broadcasts.remove(&msg.subscription_id)
                {
                    // This is the first EventAdd response, tell waiting clients that opening was successful
                    // TODO: Make this capacity configurable.
                    let (tx, rx) = broadcast::channel(32);
                    self.broadcast_receivers
                        .insert(msg.subscription_id, (length, dbrtype, tx));
                    // Send the receiver to the waiting client
                    let _ = reply.send(Ok(rx));
                }
                let transmitter = &self
                    .broadcast_receivers
                    .get(&msg.subscription_id)
                    .expect("Should have just created this")
                    .2;
                match transmitter.send(dbr) {
                    Ok(0) | Err(_) => {
                        // We have no receivers left; cancel this subscription
                        debug!("No more receivers for {}: Cancelling", msg.subscription_id);

                        let sid = self.channels.get(channel_id).unwrap().sid;
                        return vec![msg.cancel(sid).into()];
                    }
                    Ok(_) => (),
                };
                Vec::new()
            }
            msg => {
                debug!("Got unhandled message from server: {msg:?}");
                Vec::new()
            } // ClientMessage::SearchResponse(msg) => todo!(),
              // ClientMessage::ServerDisconnect(msg) => todo!(),
              // ClientMessage::WriteNotifyResponse(msg) => todo!(),
              // ClientMessage::ECAError(msg) => todo!(),
        }
    }
}

pub struct Client {
    /// Port to listen for server beacon messages
    beacon_port: u16,
    /// Multicast port on which to send searches
    search_port: u16,
    /// Servers we have seen broadcasting.
    /// This can be used to trigger e.g. re-searching on the appearance
    /// of a new beacon server or the case of one restarting (at which
    /// point the beacon ID resets).
    observed_beacons: Arc<Mutex<HashMap<SocketAddr, (u32, Instant)>>>,
    /// Active name searches and how long ago we sent them
    // name_searches: HashMap<u32, (String, Instant, oneshot::Sender<SocketAddr>)>,
    /// Active connections to different servers
    circuits: HashMap<SocketAddr, Circuit>,
    /// The cancellation token
    cancellation: CancellationToken,
    searcher: Searcher,
}

#[derive(thiserror::Error, Debug)]
pub enum ClientError {
    #[error("{0}")]
    IO(#[from] io::Error),
    #[error("{0}")]
    PVNotFoundError(#[from] CouldNotFindError),
    #[error("Failed to parse message ƒrom server")]
    ServerSentInvalidMessage,
    #[error("The server version ({0}) was incompatible")]
    ServerVersionMismatch(u16),
    #[error("The Client is closing or has closed")]
    ClientClosed,
    #[error("The channel does not exist or is already closed")]
    ChannelClosed,
    #[error("Channel creation failed")]
    ChannelCreateFailed,
}

impl Client {
    pub async fn new() -> Result<Client, io::Error> {
        Self::new_with(get_default_server_port(), None).await
    }
    pub async fn new_with(
        search_port: u16,
        broadcast_addresses: Option<Vec<SocketAddr>>,
    ) -> Result<Client, io::Error> {
        let beacon_port = get_default_beacon_port();
        let cancel = CancellationToken::new();
        let searcher_b = SearcherBuilder::new()
            .search_port(search_port)
            .stop_token(cancel.clone());
        let searcher = if let Some(addr) = broadcast_addresses {
            searcher_b.broadcast_to(addr)
        } else {
            searcher_b
        }
        .start()
        .await
        .unwrap();

        let mut client = Client {
            beacon_port,
            search_port,
            observed_beacons: Default::default(),
            circuits: Default::default(),
            cancellation: CancellationToken::new(),
            searcher,
        };
        client.start().await?;
        Ok(client)
    }

    async fn start(&mut self) -> Result<(), io::Error> {
        if let Err(err) = self.watch_broadcasts(self.cancellation.clone()).await {
            warn!(
                "Failed to create broadcast watcher on port {}, will run without: {err:?}",
                self.beacon_port
            );
        }
        Ok(())
    }

    async fn get_or_create_circuit(&mut self, addr: SocketAddr) -> Result<&Circuit, ClientError> {
        Ok(match self.circuits.entry(addr) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let circuit = Circuit::connect(&addr, None, None).await?;
                entry.insert(circuit)
            }
        })
    }

    pub async fn read_pv(&mut self, name: &str) -> Result<DbrValue, ClientError> {
        // First, find the server that holds this name
        let ioc = self.searcher.search_for(name).await?;
        let circuit = self.get_or_create_circuit(ioc).await?;
        // let channel = circuit
        circuit.read_pv(name).await.map(|d| d.take_value())
    }
    pub async fn subscribe(&mut self, name: &str) -> Result<broadcast::Receiver<Dbr>, ClientError> {
        let ioc = self.searcher.search_for(name).await?;
        let circuit = self.get_or_create_circuit(ioc).await?;
        circuit.subscribe(name).await
    }

    /// Watch for broadcast beacons, and record their ID and timestamp into the client map
    async fn watch_broadcasts(&self, stop: CancellationToken) -> Result<(), io::Error> {
        let port = self.beacon_port;
        let beacon_map = self.observed_beacons.clone();
        // Bind the socket first, so that we know early if it fails
        let broadcast_socket = new_reusable_udp_socket(SocketAddr::new([0, 0, 0, 0].into(), port))?;

        tokio::spawn(async move {
            let mut buf: Vec<u8> = vec![0; 0xFFFF];

            loop {
                select! {
                    _ = stop.cancelled() => break,
                    r = broadcast_socket.recv_from(&mut buf) => match r {
                    Ok((size, addr)) => {
                        if let Ok((_, beacon)) = RsrvIsUp::parse(&buf[..size]) {
                            trace!("Observed beacon: {beacon:?}");
                            let send_ip =
                                beacon.server_ip.map(IpAddr::V4).unwrap_or(addr.ip());
                            let mut beacons = beacon_map.lock().unwrap();
                            beacons.insert(
                                (send_ip, beacon.server_port).into(),
                                (beacon.beacon_id, Instant::now()),
                            );
                        }
                    }
                    Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => {
                        warn!("Got unresumable error whilst watching broadcasts: {e:?}");
                        break;
                    }
                }
                }
            }
        });
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
