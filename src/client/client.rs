#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::{HashMap, hash_map::Entry},
    io::ErrorKind,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, split},
    net::TcpStream,
    select,
    sync::{mpsc, oneshot},
};
use tokio_stream::StreamExt;
use tokio_util::{codec::FramedRead, sync::CancellationToken};
use tracing::{debug, debug_span, error, trace, warn};

use crate::{
    client::{Searcher, searcher::CouldNotFindError},
    dbr::{Dbr, DbrBasicType, DbrCategory, DbrType, DbrValue},
    messages::{self, Access, CAMessage, ClientMessage, Message, RsrvIsUp},
    utils::new_reusable_udp_socket,
};

fn get_default_broadcast_ips() -> Vec<IpAddr> {
    let interfaces = datalink::interfaces();
    interfaces
        .into_iter()
        .filter(|i| !i.is_loopback())
        .flat_map(|i| i.ips.into_iter())
        .filter(|i| i.is_ipv4())
        .map(|f| f.broadcast())
        .collect()
}

enum CircuitRequest {
    GetChannel(String, oneshot::Sender<Result<ChannelInfo, ClientError>>),
    /// Read a single value from the server
    Read {
        channel: u32,
        length: usize,
        category: DbrCategory,
        reply: oneshot::Sender<Result<Dbr, ClientError>>,
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
                last_sent_at: Instant::now(),
                requests_rx,
                cancel: inner_cancel,
                next_cid: 0,
                channel_lookup: Default::default(),
                channels: Default::default(),
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
        println!("Circuit read_pv got channel: {channel:?}");
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
    /// Watchers waiting for reads specifically
    pending_read: Vec<oneshot::Sender<Result<Dbr, ClientError>>>,
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
    /// When the last message was sent. Used to calculate Echo timing.
    last_sent_at: Instant,
    // requests_tx: mpsc::Sender<CircuitRequest>,
    requests_rx: mpsc::Receiver<CircuitRequest>,
    cancel: CancellationToken,
    next_cid: u32,
    channels: HashMap<u32, Channel>,
    channel_lookup: HashMap<String, u32>,
}
impl CircuitInternal {
    async fn circuit_lifecycle(&mut self, tcp: TcpStream) {
        debug!("Started circuit to {}", self.address);
        let (tcp_rx, mut tcp_tx) = split(tcp);
        let mut framed = FramedRead::with_capacity(tcp_rx, ClientMessage {}, 16384usize);
        loop {
            let messages_out = select! {
                _ = self.cancel.cancelled() => break,
                Some(message) = framed.next() => match message {
                    Ok(message) => {self.handle_message(message); None},
                    Err(e) => {
                        error!("Got error processing server message: {e}");
                        continue;
                    }
                },
                request = self.requests_rx.recv() => match request {
                    None => break,
                    Some(req) => Some(self.handle_request(req).await)
                },
            };
            // Send any messages out
            if let Some(messages) = messages_out {
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
                // let channel = self.get_channel(&name);
                let Some(channel) = self.channels.get_mut(&cid) else {
                    let _ = reply.send(Err(ClientError::ChannelClosed));
                    return Vec::new();
                };
                // Send the read request
                channel.pending_read.push(reply);
                debug!("Sending read request for: {cid}");
                vec![
                    messages::ReadNotify {
                        data_type: DbrType {
                            basic_type: channel.native_type.unwrap(),
                            category,
                        },
                        data_count: length as u32,
                        server_id: channel.sid,
                        client_ioid: channel.cid,
                    }
                    .into(),
                ]
                // todo!();
            }
        }
    }
    fn handle_message(&mut self, message: Message) {
        match message {
            Message::AccessRights(msg) => {
                let _span = debug_span!("handle_message", cid = &msg.client_id).entered();
                let Some(channel) = self.channels.get_mut(&msg.client_id) else {
                    debug!("Got message for closed/uncreated channel");
                    return;
                };
                debug!("Got AccessRights update: {}", msg.access_rights);
                channel.permissions = msg.access_rights;
            }
            Message::CreateChannelResponse(msg) => {
                let _span = debug_span!("handle_message", cid = &msg.client_id).entered();
                let Some(channel) = self.channels.get_mut(&msg.client_id) else {
                    debug!("Got message for closed/uncreated channel: {msg:?}");
                    return;
                };
                channel.native_count = msg.data_count;
                channel.native_type = Some(msg.data_type);
                channel.state = ChannelState::Ready;
                channel.sid = msg.server_id;
                let info = channel.info();

                for sender in channel.pending_open.drain(..) {
                    let _ = sender.send(Ok(info));
                }
            }
            msg => println!("Ignoring unhandled message from server: {msg:?}"),
        }
    }
}

pub struct Client {
    /// Port to listen for server beacon messages
    beacon_port: u16,
    /// Multicast port on which to send searches
    search_port: u16,
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<IpAddr>,
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
}

impl Client {
    pub async fn new() -> Result<Client, io::Error> {
        let mut client = Client {
            beacon_port: 5065,
            search_port: 5064,
            broadcast_addresses: get_default_broadcast_ips(),
            observed_beacons: Default::default(),
            circuits: Default::default(),
            cancellation: CancellationToken::new(),
            searcher: Searcher::start().await.unwrap(),
        };
        client.start().await?;
        Ok(client)
    }

    async fn start(&mut self) -> Result<(), io::Error> {
        if let Err(err) = self.watch_broadcasts(self.cancellation.clone()).await {
            warn!("Failed to create broadcast watcher, will run without: {err:?}");
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
