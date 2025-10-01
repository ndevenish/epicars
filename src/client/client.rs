#![allow(dead_code)]

use core::panic;
use std::{
    cmp::max,
    collections::HashMap,
    io::ErrorKind,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt, split},
    net::TcpStream,
    select,
    sync::{broadcast, mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
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

#[derive(Debug)]
pub struct SubscriptionToken {
    circuit: SocketAddr,
    ioid: u32,
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
    Write {
        channel: u32,
        reply: oneshot::Sender<Result<(), ClientError>>,
        value: DbrValue,
        notify: bool,
    },
    /// Start a subscription to a PV on the server
    Subscribe {
        channel: u32,
        length: usize,
        dbr_type: DbrType,
        reply: oneshot::Sender<Result<(broadcast::Receiver<Dbr>, u32), ClientError>>,
    },
    Unsubscribe(u32),
}

#[derive(Debug)]
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
                pending_writes: Default::default(),
                last_echo_sent_at: Instant::now(),
                last_received_message_at: Instant::now(),
                pending_monitors: Default::default(),
                monitor_receivers: Default::default(),
                monitor_channels: Default::default(),
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
    async fn read_pv(&self, name: &str, kind: DbrCategory) -> Result<Dbr, ClientError> {
        let channel = self.get_channel(name.to_owned()).await?;
        debug!("Circuit read_pv got channel: {channel:?}");
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(CircuitRequest::Read {
                channel: channel.cid,
                length: 0usize,
                category: kind,
                reply: tx,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;
        rx.await.map_err(|_| ClientError::ClientClosed)?
    }
    /// Do a Circuit read, by spawning a task and responding via channel
    fn read_spawn(
        &self,
        name: &str,
        kind: DbrCategory,
        length: Option<usize>,
    ) -> oneshot::Receiver<Result<Dbr, ClientError>> {
        let (read_tx, read_rx) = oneshot::channel();
        let name = name.to_string();
        let requests_tx = self.requests_tx.clone();
        // Note: Because spawning, we cannot do this whilst holding &self. So do it by
        // communicating via completely cloneable channels.
        tokio::spawn(async move {
            // Get the channel first
            let (tx, rx) = oneshot::channel();
            // Send the GetChannel request, with a reply oneshot
            if requests_tx
                .send(CircuitRequest::GetChannel(name, tx))
                .await
                .is_err()
            {
                let _ = read_tx.send(Err(ClientError::ClientClosed));
                return;
            }
            // Wait for a response on this oneshot
            let Ok(channel_result) = rx.await else {
                let _ = read_tx.send(Err(ClientError::ClientClosed));
                return;
            };
            // Pull the result, or error, out of this oneshot
            let channel = match channel_result {
                Ok(c) => c,
                Err(e) => {
                    let _ = read_tx.send(Err(e));
                    return;
                }
            };

            // We have a channel! Request the read...
            trace!("Circuit read_pv got channel: {channel:?}");
            let (tx, rx) = oneshot::channel();
            let _ = requests_tx
                .send(CircuitRequest::Read {
                    channel: channel.cid,
                    length: length.unwrap_or(0usize),
                    category: kind,
                    reply: tx,
                })
                .await;
            let Ok(res) = rx.await else {
                let _ = read_tx.send(Err(ClientError::ClientClosed));
                return;
            };
            trace!("Circuit read got result: {res:?}");
            let _ = read_tx.send(res);
        });
        read_rx
    }

    async fn write_pv(&self, name: &str, value: impl Into<DbrValue>) -> Result<(), ClientError> {
        let channel = self.get_channel(name.to_owned()).await?;
        if matches!(channel.permissions, Access::None | Access::Read) {
            return Err(ClientError::ChannelReadOnly);
        };
        let (tx, rx) = oneshot::channel();
        self.requests_tx
            .send(CircuitRequest::Write {
                channel: channel.cid,
                reply: tx,
                value: value.into(),
                notify: true,
            })
            .await
            .map_err(|_| ClientError::ClientClosed)?;
        rx.await.unwrap_or(Err(ClientError::ChannelClosed))
    }

    async fn subscribe(&self, name: &str) -> Result<(broadcast::Receiver<Dbr>, u32), ClientError> {
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
        rx.await.map_err(|_| ClientError::ChannelCreateFailed)?
    }
    async fn unsubscribe(&self, subscription_id: u32) {
        let _ = self
            .requests_tx
            .send(CircuitRequest::Unsubscribe(subscription_id))
            .await;
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

type Ioid = u32;

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
    /// Watchers waiting for write notifications
    pending_writes: HashMap<u32, (Instant, oneshot::Sender<Result<(), ClientError>>)>,
    /// Broadcast subscriptions we have not had confirmed yet
    #[allow(clippy::type_complexity)] // TODO: Actually follow Clippy's advice here
    pending_monitors: HashMap<
        u32,
        (
            Instant,
            (
                usize,
                DbrType,
                oneshot::Sender<Result<(broadcast::Receiver<Dbr>, u32), ClientError>>,
            ),
        ),
    >,
    /// Lookup subscription info from an IOID to the subscription info§
    monitor_receivers: HashMap<u32, (usize, DbrType, broadcast::Sender<Dbr>)>,
    /// The ioid->channel ID lookup table. IOID is unique across the whole circuit.
    monitor_channels: HashMap<u32, u32>,
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
            CircuitRequest::Write {
                channel: cid,
                reply,
                value,
                notify,
            } => {
                let Some(channel) = self.channels.get_mut(&cid) else {
                    let _ = reply.send(Err(ClientError::ChannelClosed));
                    return Vec::new();
                };
                let _span = debug_span!("handle_request", cid = cid).entered();
                let ioid = wrapping_inplace_add(&mut channel.next_ioid);
                debug!(
                    "Sending write request {ioid} for channel {cid} ({})",
                    channel.name
                );
                let (_, data) = value.to_bytes(None);
                if notify {
                    self.pending_writes.insert(ioid, (Instant::now(), reply));
                    vec![
                        messages::WriteNotify {
                            data_type: DbrType {
                                basic_type: value.get_type(),
                                category: DbrCategory::Basic,
                            },
                            data_count: value.get_count() as u32,
                            server_id: channel.sid,
                            client_ioid: ioid,
                            data,
                        }
                        .into(),
                    ]
                } else {
                    vec![
                        messages::Write {
                            data_type: DbrType {
                                basic_type: value.get_type(),
                                category: DbrCategory::Basic,
                            },
                            data_count: value.get_count() as u32,
                            server_id: channel.sid,
                            client_ioid: ioid,
                            data,
                        }
                        .into(),
                    ]
                }
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
                self.pending_monitors
                    .insert(ioid, (Instant::now(), (length, dbr_type, reply)));
                self.monitor_channels.insert(ioid, channel.cid);
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
            CircuitRequest::Unsubscribe(ioid) => {
                debug!("Got unsubscribe request for: {ioid}");
                // Any pending should just be removed now
                self.pending_monitors.remove(&ioid);

                // Remove from lookup tables if not already partially
                // cleared. Only if we have all tables present can we
                // send a cancel message, the otherwise assumption is
                // that e.g. we have a partially closed circuit.
                if let (Some(cid), Some((count, dbr_type, _))) = (
                    self.monitor_channels.remove(&ioid),
                    self.monitor_receivers.remove(&ioid),
                ) && let Some(channel) = self.channels.get(&cid)
                {
                    return vec![
                        messages::EventCancel {
                            data_type: dbr_type,
                            data_count: count as u32,
                            server_id: channel.sid,
                            subscription_id: ioid,
                        }
                        .into(),
                    ];
                }
                Vec::new()
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
                let Some(channel_id) = self.monitor_channels.get(&msg.subscription_id) else {
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
                    if let Some(cid) = self.monitor_channels.get(&msg.subscription_id) {
                        self.channels
                            .get_mut(cid)
                            .unwrap()
                            .broadcast_receivers
                            .retain(|s| *s != msg.subscription_id);
                    }
                    self.monitor_channels.remove(&msg.subscription_id);
                    self.monitor_receivers.remove(&msg.subscription_id);
                    self.pending_monitors.remove(&msg.subscription_id);
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
                    self.pending_monitors.remove(&msg.subscription_id)
                {
                    // This is the first EventAdd response, tell waiting clients that opening was successful
                    // TODO: Make this capacity configurable.
                    let (tx, rx) = broadcast::channel(32);
                    self.monitor_receivers
                        .insert(msg.subscription_id, (length, dbrtype, tx));
                    // Send the receiver to the waiting client
                    let _ = reply.send(Ok((rx, msg.subscription_id)));
                }
                let transmitter = &self
                    .monitor_receivers
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
            ClientMessage::WriteNotifyResponse(msg) => {
                let Some((_, tx)) = self.pending_writes.remove(&msg.client_ioid) else {
                    debug!("Got WriteNotifyResponse for unknown write ioid!");
                    return Vec::new();
                };
                if msg.status_code == 1 {
                    let _ = tx.send(Ok(()));
                } else {
                    let _ = tx.send(Err(ClientError::WriteFailed(msg.status_code)));
                }
                Vec::new()
            }
            msg => {
                debug!("Got unhandled message from server: {msg:?}");
                Vec::new()
            } // ClientMessage::SearchResponse(msg) => todo!(),
              // ClientMessage::ServerDisconnect(msg) => todo!(),
              // ClientMessage::ECAError(msg) => todo!(),
        }
    }
}

/// Keep track of active subscriptions, so we can carry service over disconnects
struct SubscriptionInfo {
    /// The PV name this subscription is for
    name: String,
    /// The sender, so that we can hand it out to new circuits
    sender: broadcast::Sender<Dbr>,
    /// The watcher, so that we can hand it out to new circuits
    watcher: watch::Sender<Dbr>,
    /// The last known IOC that supplied this subscription
    circuit: Option<SocketAddr>,
}

/// Internal requests to manage state within the CircuitInternal
#[derive(Debug)]
enum ClientInternalRequest {
    /// A search for a PV has concluded, either positively or negatively
    DeterminedPV(String, Option<SocketAddr>),
    OpenedCircuit(Circuit),
    CircuitOpenFailed(SocketAddr),
    ReadAvailable(String, Result<Dbr, ClientError>),
}

#[derive(Debug)]
enum CircuitState {
    Pending,
    Open(Circuit),
}
/// Internal state for the Client async task
///
/// Because we want to manage state outside of a direct user async call
/// (e.g. managing reconnection), we need to have a separate task loop to
/// communicate with.
///
/// Anything related to managing reconnections should go in here.
struct ClientInternal {
    cancellation: CancellationToken,
    /// Record of all open subscriptions, for reconnection
    subscriptions: Vec<SubscriptionInfo>,
    /// Currently active connections
    circuits: HashMap<SocketAddr, CircuitState>,
    /// Known and trusted resolutions to specific PV requests
    known_ioc: HashMap<String, SocketAddr>,
    /// Servers we have seen broadcasting.
    /// This can be used to trigger e.g. re-searching on the appearance
    /// of a new beacon server or the case of one restarting (at which
    /// point the beacon ID resets).
    observed_beacons: Arc<Mutex<HashMap<SocketAddr, (u32, Instant)>>>,
    /// The searcher object
    searcher: Searcher,
    /// Requests coming from the Client user
    requests: mpsc::Receiver<ClientRequest>,
    internal_requests: mpsc::UnboundedSender<ClientInternalRequest>,
    subtasks: JoinSet<()>,
    read_requests: Vec<ReadRequest>,
}

#[derive(Debug)]
struct ReadRequest {
    name: String,
    kind: DbrCategory,
    length: usize,
    response: oneshot::Sender<Result<Dbr, GetError>>,
}

impl ClientInternal {
    async fn start(
        cancel_token: CancellationToken,
        search_port: u16,
        beacon_port: u16,
        broadcast_addresses: Option<Vec<SocketAddr>>,
        started: oneshot::Sender<Result<mpsc::Sender<ClientRequest>, io::Error>>,
    ) {
        let searcher = match Self::start_searcher(
            search_port,
            broadcast_addresses,
            cancel_token.clone(),
        )
        .await
        {
            Ok(x) => x,
            Err(e) => {
                let _ = started.send(Err(e));
                return;
            }
        };
        let (tx, rx) = mpsc::channel(32);
        let (tx_i, mut rx_i) = mpsc::unbounded_channel();
        let mut internal = ClientInternal {
            cancellation: cancel_token,
            subscriptions: Vec::new(),
            circuits: HashMap::new(),
            observed_beacons: Default::default(),
            searcher,
            requests: rx,
            subtasks: Default::default(),
            known_ioc: Default::default(),
            internal_requests: tx_i,
            read_requests: Default::default(),
        };
        if let Err(err) = internal.watch_broadcasts(beacon_port).await {
            warn!(
                "Failed to create broadcast watcher on port {}, will run without: {err:?}",
                beacon_port
            );
        }

        let _ = started.send(Ok(tx));
        loop {
            select! {
                _ = internal.cancellation.cancelled() => break,
                request = internal.requests.recv() => match request {
                    Some(r) => internal.handle_request(r),
                    None => break,
                },
                request = rx_i.recv() => internal.handle_internal_request(request.unwrap()),
            }
        }
    }

    async fn start_searcher(
        search_port: u16,
        broadcast_addresses: Option<Vec<SocketAddr>>,
        cancel_token: CancellationToken,
    ) -> Result<Searcher, io::Error> {
        // let cancel = CancellationToken::new();
        let builder = SearcherBuilder::new()
            .search_port(search_port)
            .stop_token(cancel_token);
        let searcher = if let Some(addr) = broadcast_addresses {
            builder.broadcast_to(addr)
        } else {
            builder
        }
        .start()
        .await
        .unwrap();
        Ok(searcher)
    }

    /// Watch for broadcast beacons, and record their ID and timestamp into the client map
    async fn watch_broadcasts(&mut self, port: u16) -> Result<(), io::Error> {
        // Bind the socket first, so that we know early if it fails
        let broadcast_socket = new_reusable_udp_socket(SocketAddr::new([0, 0, 0, 0].into(), port))?;
        let beacon_map = self.observed_beacons.clone();
        let stop = self.cancellation.clone();
        self.subtasks.spawn(async move {
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

    fn handle_request(&mut self, request: ClientRequest) {
        match request {
            ClientRequest::Get {
                name,
                kind,
                length,
                result,
            } => {
                debug!("Got CA read request for {name}");
                self.read_requests.push(ReadRequest {
                    name: name.clone(),
                    kind,
                    length,
                    response: result,
                });
                if let Some(addr) = self.known_ioc.get(&name) {
                    debug!("  Known IOC: {addr}");
                    // We already have it, send ourselves a message to proceed
                    let _ = self
                        .internal_requests
                        .send(ClientInternalRequest::DeterminedPV(name, Some(*addr)));
                } else {
                    debug!("  Unknown IOC, searching");
                    // We don't know this IOC, we have to search for it.
                    let handle = self.searcher.search_spawn(name.clone());
                    let req = self.internal_requests.clone();
                    self.subtasks.spawn(async move {
                        let _ = req.send(ClientInternalRequest::DeterminedPV(
                            name,
                            handle.await.ok().flatten(),
                        ));
                    });
                };
            }
            ClientRequest::Put {
                name: _,
                value: _,
                result: _,
            } => todo!(),
            ClientRequest::Subscribe {
                name: _,
                kind: _,
                length: _,
                monitor: _,
                result: _,
            } => todo!(),
        }
    }
    /// Handle internal operations messages
    ///
    /// We need this because we don't want to block, especially while
    /// holding a mutable self-reference - in the main request handling
    /// function.
    fn handle_internal_request(&mut self, request: ClientInternalRequest) {
        trace!("Handling internal request: {request:?}");
        match request {
            ClientInternalRequest::DeterminedPV(name, socket_addr) => {
                debug!("Got notification of PV {name} determined: {socket_addr:?}");

                trace!("Outstanding requests: {:?}", self.read_requests);
                let Some(addr) = socket_addr else {
                    // Send responses to anyone waiting for this to say it couldn't be found
                    for req in self.read_requests.extract_if(.., |k| k.name == name) {
                        let _ = req.response.send(Err(GetError::CouldNotFindIOC));
                    }
                    return;
                };
                debug!("Known circuits: {:?}", self.circuits);
                self.known_ioc.insert(name, addr);

                // Temporarily remove the circuit to avoid mutable borrow of self
                let circuit = self.circuits.remove(&addr);

                // Now we know what server a given PV is served by, check if we already have a connection
                match circuit {
                    Some(CircuitState::Open(circuit)) => {
                        // We already have this circuit open! Send any read requests to it
                        self.send_outstanding_requests_for(&circuit);
                        self.circuits.insert(addr, CircuitState::Open(circuit));
                    }
                    Some(CircuitState::Pending) => {
                        // Nothing to do, this will open and handle requests when ready
                        // So, put the circuit back.
                        self.circuits.insert(addr, CircuitState::Pending);
                    }
                    None => {
                        // We need to open this circuit
                        self.circuits.insert(addr, CircuitState::Pending);
                        let req = self.internal_requests.clone();
                        self.subtasks.spawn(async move {
                            match Circuit::connect(&addr, None, None).await {
                                Ok(circuit) => {
                                    let _ = req.send(ClientInternalRequest::OpenedCircuit(circuit));
                                }
                                Err(e) => {
                                    warn!("Failed to connect to Circuit {addr}: {e}");
                                    let _ =
                                        req.send(ClientInternalRequest::CircuitOpenFailed(addr));
                                }
                            }
                        });
                    }
                }
            }
            ClientInternalRequest::OpenedCircuit(circuit) => {
                debug!("Circuit to {} opened", circuit.address);
                self.send_outstanding_requests_for(&circuit);
                self.circuits
                    .insert(circuit.address, CircuitState::Open(circuit));
            }
            ClientInternalRequest::CircuitOpenFailed(socket_addr) => {
                warn!("Circuit {socket_addr} open failed, failing reads");
                for request in self.read_requests.extract_if(.., |r| {
                    self.known_ioc
                        .get(&r.name)
                        .map(|addr| *addr == socket_addr)
                        .is_some()
                }) {
                    let _ = request.response.send(Err(GetError::InternalClientError));
                }
            }
            ClientInternalRequest::ReadAvailable(name, dbr) => {
                debug!("Got read of {name} from circuit");
                let dbr = dbr.map_err(|_| GetError::InternalClientError);
                for request in self.read_requests.extract_if(.., |r| r.name == name) {
                    let _ = request.response.send(dbr.clone());
                }
            }
        }
    }

    fn send_outstanding_requests_for(&mut self, circuit: &Circuit) {
        // Go through all open read requests, then dispatch the request if for
        // this circuit ... at the moment this is rather indirect but wary about
        // introducing more state caches
        for request in self.read_requests.iter().filter(|r| {
            self.known_ioc
                .get(&r.name)
                .map(|addr| *addr == circuit.address)
                .is_some()
        }) {
            let result = circuit.read_spawn(&request.name, request.kind, Some(request.length));
            let mq = self.internal_requests.clone();
            let name = request.name.clone();
            self.subtasks.spawn(async move {
                // request.response.send()
                if let Ok(response) = result.await {
                    let _ = mq.send(ClientInternalRequest::ReadAvailable(name, response));
                }
            });
        }
    }
    // async fn get_or_create_circuit(
    //     circuits: &mut HashMap<SocketAddr, Circuit>,
    //     addr: SocketAddr,
    // ) -> Result<&Circuit, ClientError> {
    //     Ok(match circuits.entry(addr) {
    //         Entry::Occupied(entry) => entry.into_mut(),
    //         Entry::Vacant(entry) => {
    //             let circuit = Circuit::connect(&addr, None, None).await?;
    //             entry.insert(circuit)
    //         }
    //     })
    // }

    // // fn get_known_ioc(&self, pv_name: &str) -> Option<SocketAddr> {}
    // async fn find_ioc(&mut self, pv_name: &str) -> Result<SocketAddr, CouldNotFindError> {
    //     let addr = if let Some(addr) = self.known_ioc.get(pv_name) {
    //         *addr
    //     } else {
    //         // We need to search for this
    //         let addr = self.searcher.search_for(pv_name).await?;
    //         self.known_ioc.insert(pv_name.to_string(), addr);
    //         addr
    //     };
    //     Ok(addr)
    // }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum GetError {
    #[error("The internal client has closed")]
    Closed,
    #[error("Could not convert data type to requested")]
    NoConvert,
    #[error("Could not find a source IOC for this PV name")]
    CouldNotFindIOC,
    #[error("Internal client error")]
    InternalClientError,
}
impl From<CouldNotFindError> for GetError {
    fn from(_: CouldNotFindError) -> Self {
        GetError::CouldNotFindIOC
    }
}

#[derive(thiserror::Error, Debug)]
enum PutError {}
#[derive(thiserror::Error, Debug)]
enum SubscribeError {}

type SubscriptionObjects = (broadcast::Receiver<Dbr>, watch::Receiver<Dbr>);

/// Requests to send to the client internal
#[derive(Debug)]
enum ClientRequest {
    /// Get a PV value, once
    Get {
        name: String,
        kind: DbrCategory,
        length: usize,
        result: oneshot::Sender<Result<Dbr, GetError>>,
    },
    /// Write a value to a PV, once
    Put {
        name: String,
        value: DbrValue,
        result: oneshot::Sender<Result<(), PutError>>,
    },
    /// Request a subscription
    Subscribe {
        name: String,
        kind: DbrType,
        length: usize,
        monitor: u8,
        result: oneshot::Sender<Result<SubscriptionObjects, SubscribeError>>,
    },
}
pub struct Client {
    /// The central cancellation token, to completely shut down the client
    cancellation: CancellationToken,
    /// Communication with the internal processing loop
    internal_requests: mpsc::Sender<ClientRequest>,
    /// Handle to know when the internal loop has finished
    handle: Option<JoinHandle<()>>,
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
    #[error("PV is read-only")]
    ChannelReadOnly,
    #[error["Write to PV failed, code: {0}"]]
    WriteFailed(u32),
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
        // A way to get the "Launched OK" message back
        let (result_tx, result_rx) = oneshot::channel();

        let internal_cancel = cancel.clone();
        let handle = tokio::spawn(async move {
            ClientInternal::start(
                internal_cancel,
                search_port,
                beacon_port,
                broadcast_addresses,
                result_tx,
            )
            .await;
            debug!("ClientInternal terminated");
        });
        // Wait for this to start
        let internal_tx = result_rx.await.unwrap()?;

        Ok(Client {
            cancellation: cancel,
            internal_requests: internal_tx,
            handle: Some(handle),
        })
    }

    /// Cleanly shut down
    pub async fn stop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }

    /// Read a PV directly from name into a specific data type
    pub async fn get<T>(&self, name: &str) -> Result<T, GetError>
    where
        T: TryFrom<DbrValue>,
    {
        let result = self.get_kind(name, DbrCategory::Basic).await?;
        let Ok(v) = T::try_from(result.take_value()) else {
            return Err(GetError::NoConvert);
        };
        Ok(v)
    }

    /// Read a named PV, with a specific metadata payload
    pub async fn get_kind(&self, name: &str, kind: DbrCategory) -> Result<Dbr, GetError> {
        let (tx, rx) = oneshot::channel();
        if self
            .internal_requests
            .send(ClientRequest::Get {
                name: name.to_string(),
                kind,
                length: 0,
                result: tx,
            })
            .await
            .is_err()
        {
            // The other end of the oneshot was closed
            return Err(GetError::Closed);
        }
        // Wait for some response
        let Ok(result) = rx.await else {
            // The other end of the oneshot was closed
            return Err(GetError::Closed);
        };
        result
    }

    // async fn get_or_create_circuit(
    //     circuits: &mut HashMap<SocketAddr, Circuit>,
    //     addr: SocketAddr,
    // ) -> Result<&Circuit, ClientError> {
    //     Ok(match circuits.entry(addr) {
    //         Entry::Occupied(entry) => entry.into_mut(),
    //         Entry::Vacant(entry) => {
    //             let circuit = Circuit::connect(&addr, None, None).await?;
    //             entry.insert(circuit)
    //         }
    //     })
    // }

    // pub async fn read_pv(&mut self, name: &str) -> Result<DbrValue, ClientError> {
    //     // First, find the server that holds this name
    //     let ioc = self.searcher.search_for(name).await?;
    //     let circuit = Self::get_or_create_circuit(&mut self.circuits, ioc).await?;
    //     // let channel = circuit
    //     circuit.read_pv(name).await.map(|d| d.take_value())
    // }
    // /// Subscribe to changes for a particular PV
    // ///
    // /// Returns the [broadcast::Receiver] for the PV, but also a token
    // /// that can be used to later cancel the subscription.
    // pub async fn subscribe(
    //     &mut self,
    //     name: &str,
    // ) -> Result<(broadcast::Receiver<Dbr>, SubscriptionToken), ClientError> {
    //     let ioc = self.searcher.search_for(name).await?;
    //     let circuit = Self::get_or_create_circuit(&mut self.circuits, ioc).await?;
    //     let (receiver, ioid) = circuit.subscribe(name).await?;
    //     // self.subscriptions.insert(name.to_string(), ioc);
    //     Ok((receiver, SubscriptionToken { circuit: ioc, ioid }))
    // }

    // pub async fn write_pv(
    //     &mut self,
    //     name: &str,
    //     value: impl Into<DbrValue>,
    // ) -> Result<(), ClientError> {
    //     let ioc = self.searcher.search_for(name).await?;
    //     let circuit = Self::get_or_create_circuit(&mut self.circuits, ioc).await?;
    //     circuit.write_pv(name, value.into()).await
    // }

    // /// Stop receiving messages for a specific PV
    // ///
    // /// > [!NOTE]
    // /// > It is still possible to get updated messages returned after
    // /// > calling this function, because there may have been messages
    // /// > in-flight before the termination request gets through.
    // pub async fn unsubscribe(&mut self, subscription: SubscriptionToken) {
    //     // let Some(ioc) = self.subscriptions.remove(name) else {
    //     //     return;
    //     // };
    //     let Some(circuit) = self.circuits.get(&subscription.circuit) else {
    //         return;
    //     };
    //     circuit.unsubscribe(subscription.ioid).await;
    // }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use tracing::level_filters::LevelFilter;

    use crate::Client;

    #[tokio::test]
    async fn test_stop() {
        let t_ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let mut client = Client::new().await.unwrap();
        client.stop().await;
    }
}
