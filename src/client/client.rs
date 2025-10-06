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
    sync::{
        broadcast,
        mpsc::{self, UnboundedSender},
        oneshot, watch,
    },
    task::{JoinHandle, JoinSet},
};
use tokio_stream::StreamExt;
use tokio_util::{codec::FramedRead, sync::CancellationToken};
use tracing::{debug, debug_span, error, trace, warn};

use crate::{
    client::{
        Searcher, SearcherBuilder, Subscription,
        searcher::CouldNotFindError,
        subscription::{SenderPair, SubscriptionKeeper},
    },
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
        basic_type: Option<DbrBasicType>,
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
        mask: MonitorMask,
        senders: SenderPair<Dbr>,
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
        client_messages: UnboundedSender<ClientInternalRequest>,
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
                monitors: Default::default(),
                client_messages,
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
                basic_type: None,
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
            let channel = match Self::get_channel_from(&requests_tx, &name).await {
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
                    basic_type: None,
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

    /// Async-handle fetching a channel, given only a handle to the circuit request queue
    ///
    /// This means that we can run channel-request flows without holding a mutable handle
    /// to self.
    async fn get_channel_from(
        requests: &mpsc::Sender<CircuitRequest>,
        name: &str,
    ) -> Result<ChannelInfo, ClientError> {
        // Get the channel first
        let (tx, rx) = oneshot::channel();
        // Send the GetChannel request, with a reply oneshot
        requests
            .send(CircuitRequest::GetChannel(name.to_string(), tx))
            .await
            .map_err(|_| ClientError::ClientClosed)?;

        // Wait for a response on this oneshot
        rx.await.map_err(|_| ClientError::ClientClosed)?
    }

    fn write_spawn(
        &self,
        name: &str,
        value: DbrValue,
    ) -> oneshot::Receiver<Result<(), ClientError>> {
        let (write_tx, write_rx) = oneshot::channel();
        let name = name.to_string();
        let requests_tx = self.requests_tx.clone();
        // Note: Because spawning, we cannot do this whilst holding &self. So do it by
        // communicating via completely cloneable channels.
        tokio::spawn(async move {
            let channel = match Self::get_channel_from(&requests_tx, &name).await {
                Ok(c) => c,
                Err(e) => {
                    let _ = write_tx.send(Err(e));
                    return;
                }
            };

            // We have a channel! Request the write...
            trace!("Circuit write_pv got channel: {channel:?}");
            let (tx, rx) = oneshot::channel();
            let _ = requests_tx
                .send(CircuitRequest::Write {
                    channel: channel.cid,
                    reply: tx,
                    value,
                    notify: true,
                })
                .await;
            let Ok(res) = rx.await else {
                let _ = write_tx.send(Err(ClientError::ClientClosed));
                return;
            };
            trace!("Circuit write got result: {res:?}");
            let _ = write_tx.send(res);
        });
        write_rx
    }

    async fn unsubscribe(&self, subscription_id: u32) {
        let _ = self
            .requests_tx
            .send(CircuitRequest::Unsubscribe(subscription_id))
            .await;
    }
    fn subscribe_spawn(&self, request: SubscriptionRequest, senders: SenderPair<Dbr>) {
        let name = request.name.to_string();
        let requests_tx = self.requests_tx.clone();
        tokio::spawn(async move {
            let channel = Self::get_channel_from(&requests_tx, &name)
                .await
                .expect("Don't currently handle circuits refusing knowledge of subscribing PV");
            trace!("Circuit subscription got channel: {channel:?}");
            let _ = requests_tx
                .send(CircuitRequest::Subscribe {
                    channel: channel.cid,
                    length: request.length,
                    dbr_type: DbrType {
                        basic_type: request.basic_type.unwrap_or(channel.native_type),
                        category: request.kind,
                    },
                    mask: request.monitor,
                    senders,
                })
                .await;
        });
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
    /// A list of all Ioid of open subscriptions
    subscribers: Vec<Ioid>,
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
type ChannelId = u32;

struct Monitor {
    channel_id: ChannelId,
    last_update: Instant,
    length: u32,
    dbr_type: DbrType,
    senders: SenderPair<Dbr>,
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
    next_cid: ChannelId,
    channels: HashMap<ChannelId, Channel>,
    channel_lookup: HashMap<String, ChannelId>,
    /// Watchers waiting for specific reads
    pending_reads: HashMap<u32, (Instant, oneshot::Sender<Result<Dbr, ClientError>>)>,
    /// Watchers waiting for write notifications
    pending_writes: HashMap<u32, (Instant, oneshot::Sender<Result<(), ClientError>>)>,
    /// Active subscriptions to updates
    monitors: HashMap<Ioid, Monitor>,
    /// For sending messages back to the client
    client_messages: UnboundedSender<ClientInternalRequest>,
}

impl CircuitInternal {
    async fn circuit_lifecycle(&mut self, tcp: TcpStream) {
        debug!("Started circuit to {}", self.address);
        let (tcp_rx, mut tcp_tx) = split(tcp);
        let mut framed = FramedRead::with_capacity(tcp_rx, ClientMessage::default(), 16384usize);
        let activity_period = Duration::from_secs_f32(get_default_connection_timeout() / 2.0);
        let remote_exit = loop {
            let next_timing_stop =
                max(self.last_echo_sent_at, self.last_received_message_at) + activity_period;
            let messages_out = select! {
                _ = self.cancel.cancelled() => break false,
                incoming = framed.next() => match incoming {
                    Some(message) => match message {
                        Ok(message) => Some(self.handle_message(message)),
                        Err(e) => {
                            error!("Got error processing server message: {e}");
                            continue;
                        }
                    },
                    None => break true,
                },
                request = self.requests_rx.recv() => match request {
                    Some(req) => Some(self.handle_request(req).await),
                    // The channel letting the client control this circuit was closed
                    None => break false,
                },
                _ = tokio::time::sleep_until(next_timing_stop.into()) => {
                    if self.last_echo_sent_at < self.last_received_message_at {
                        self.last_echo_sent_at = Instant::now();
                        Some(vec![Message::Echo])
                    } else {
                        // We sent an echo already, this is the termination time
                        error!("Received no reply from server, assuming connection dead");
                        break true;
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
        };
        trace!("Circuit closing");
        let _ = self
            .client_messages
            .send(ClientInternalRequest::CircuitClosed(
                self.address,
                remote_exit,
            ));
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
                basic_type,
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
                            basic_type: basic_type.unwrap_or(channel.native_type.unwrap()),
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
                senders,
                mask,
            } => {
                // Since the caller should have verified that the channel was open,
                // it is an error to get here and not have one.
                let Some(channel) = self.channels.get_mut(&cid) else {
                    return Vec::new();
                };
                let _span = debug_span!("handle_request", cid = cid).entered();
                let ioid = wrapping_inplace_add(&mut channel.next_ioid);
                self.monitors.insert(
                    ioid,
                    Monitor {
                        last_update: Instant::now(),
                        length: length as u32,
                        dbr_type,
                        senders,
                        channel_id: cid,
                    },
                );
                channel.subscribers.push(ioid);
                vec![
                    messages::EventAdd {
                        data_type: dbr_type,
                        data_count: length as u32,
                        server_id: channel.sid,
                        subscription_id: ioid,
                        mask,
                    }
                    .into(),
                ]
            }
            CircuitRequest::Unsubscribe(ioid) => {
                debug!("Got unsubscribe request for: {ioid}");
                self.create_unsubscribe_message(ioid).into_iter().collect()
            }
        }
    }
    fn create_unsubscribe_message(&mut self, ioid: Ioid) -> Option<Message> {
        // Remove entries for this subscription. If it isn't present
        // in all of the tracking tables, then assume we're in the middle
        // of shutdown and don't send an explicit EventCancel.
        if let Some(monitor) = self.monitors.remove(&ioid)
            && let Some(channel) = self.channels.get(&monitor.channel_id)
        {
            Some(
                messages::EventCancel {
                    data_type: monitor.dbr_type,
                    data_count: monitor.length,
                    server_id: channel.sid,
                    subscription_id: ioid,
                }
                .into(),
            )
        } else {
            None
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
                let Some(monitor) = self.monitors.get(&msg.subscription_id) else {
                    warn!(
                        "Got subscription message without associated channel: {}",
                        msg.subscription_id
                    );
                    return Vec::new();
                };
                let _span = debug_span!("handle_message", cid = monitor.channel_id).entered();
                if msg.data.is_empty() {
                    // This is a special case: The server is requesting termination
                    // of the subscription (possibly because we asked it to). Shut down
                    // the channel monitors.
                    debug!(
                        "Got empty EventAddResponse: Purging subscription {}",
                        msg.subscription_id
                    );
                    self.discard_subscription(msg.subscription_id);
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
                let transmitter = &mut self
                    .monitors
                    .get_mut(&msg.subscription_id)
                    .expect("Should not have been removed anywhere except here");
                // Send the update... don't check for failure as the client should
                // manage subscription lifecycles itself
                if transmitter.senders.send(dbr).is_none() {
                    debug!(
                        "Got subscription message but all listeners have dropped. Unsubscribing."
                    );
                    return self
                        .create_unsubscribe_message(msg.subscription_id)
                        .into_iter()
                        .collect();
                }
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
            ClientMessage::EventCancelResponse(_) => {
                trace!("Got confirmation of EventCancel from server");
                Vec::new()
            }
            msg => {
                debug!("Got unhandled message from server: {msg:?}");
                Vec::new()
            }
        }
    }

    /// Discard any internal state tracking subscriptions
    ///
    /// Used when the event stream has already been closed (or you are about to close it)
    fn discard_subscription(&mut self, ioid: Ioid) {
        if let Some(monitor) = self.monitors.remove(&ioid) {
            // Remove this Ioid from the channel subscriptions
            if let Some(channel) = self.channels.get_mut(&monitor.channel_id)
                && let Some(position) = channel.subscribers.iter().position(|v| v == &ioid)
            {
                channel.subscribers.swap_remove(position);
            }
        }
    }
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
    /// Keep track of open subscriptions, independently of circuit, for reconnection
    subscriptions: Arc<Mutex<SubscriptionKeeper>>,
    /// The queue to handle notifications about discovered PV IOC
    pv_search_results: mpsc::UnboundedReceiver<(String, Option<SocketAddr>)>,
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
    requests: mpsc::UnboundedReceiver<ClientRequest>,
    /// Internal lifecycle messages
    internal_requests: mpsc::UnboundedSender<ClientInternalRequest>,
    /// Tasks we've launched
    subtasks: JoinSet<()>,

    pending_reads: Requests<ReadRequest>,
    pending_writes: Requests<WriteRequest>,
    pending_subs: Requests<SubscriptionRequest>,
    // Keep track of which active subscriptions are associated with each IOC
    active_subs: HashMap<SocketAddr, Vec<Ioid>>,
    next_internal_id: Ioid,
}

trait RequestInfo {
    type Error: Clone;
    fn name(&self) -> &str;
}

#[derive(Debug)]
struct ReadRequest {
    name: String,
    kind: DbrCategory,
    length: usize,
    response: oneshot::Sender<Result<Dbr, GetError>>,
    ioid: Option<Ioid>,
}
impl RequestInfo for ReadRequest {
    type Error = GetError;

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
struct WriteRequest {
    name: String,
    value: DbrValue,
    response: oneshot::Sender<Result<(), PutError>>,
    ioid: Option<Ioid>,
}
impl RequestInfo for WriteRequest {
    type Error = PutError;
    fn name(&self) -> &str {
        &self.name
    }
}
#[derive(Debug, Clone)]
struct SubscriptionRequest {
    name: String,
    kind: DbrCategory,
    basic_type: Option<DbrBasicType>,
    length: usize,
    monitor: MonitorMask,
}
impl RequestInfo for SubscriptionRequest {
    type Error = ();
    fn name(&self) -> &str {
        &self.name
    }
}
#[derive(Debug)]
struct Requests<T> {
    /// Requests that we don't even know the IOC for
    searching_for_pv: Vec<T>,
    /// Requests that we've determined IOC, and are waiting for connection
    waiting_for_circuit: HashMap<SocketAddr, Vec<T>>,
    /// Requests that have been sent to the IOC
    dispatched: HashMap<Ioid, T>,
}
impl<T> Default for Requests<T> {
    fn default() -> Self {
        Self {
            // pending: Vec::new(),
            searching_for_pv: Vec::new(),
            waiting_for_circuit: HashMap::new(),
            dispatched: HashMap::new(),
        }
    }
}
impl<T> Requests<T>
where
    T: RequestInfo,
{
    fn add(&mut self, request: T, known_ioc: Option<SocketAddr>) {
        if let Some(ioc) = known_ioc {
            self.waiting_for_circuit
                .entry(ioc)
                .or_default()
                .push(request);
        } else {
            self.searching_for_pv.push(request);
        }
    }
    fn get_waiting_for(&mut self, address: SocketAddr) -> Vec<T> {
        self.waiting_for_circuit
            .remove(&address)
            .unwrap_or_default()
    }
    fn dispatched(&mut self, ioid: Ioid, request: T) {
        self.dispatched.insert(ioid, request);
    }
    /// We have found an IOC serving a specific PV name
    fn found_pv(&mut self, name: &str, address: SocketAddr) {
        self.waiting_for_circuit
            .entry(address)
            .or_default()
            .extend(self.searching_for_pv.extract_if(.., |r| r.name() == name));
    }
}

impl Requests<ReadRequest> {
    fn mark_no_pv_found(&mut self, name: &str) {
        for req in self.searching_for_pv.extract_if(.., |k| k.name == name) {
            let _ = req.response.send(Err(GetError::CouldNotFindIOC));
        }
    }
    fn mark_circuit_failed(&mut self, ioc: SocketAddr, error: GetError) {
        if let Some(tasks) = self.waiting_for_circuit.remove(&ioc) {
            for task in tasks {
                let _ = task.response.send(Err(error.clone()));
            }
        }
    }
    fn fulfill(&mut self, ioid: Ioid, result: Result<Dbr, ClientError>) {
        if let Some(req) = self.dispatched.remove(&ioid) {
            let dbr = result.map_err(|_| GetError::InternalClientError);
            let _ = req.response.send(dbr);
        }
    }
}
impl Requests<WriteRequest> {
    fn mark_no_pv_found(&mut self, name: &str) {
        for req in self.searching_for_pv.extract_if(.., |k| k.name == name) {
            let _ = req.response.send(Err(PutError::CouldNotFindIOC));
        }
    }
    fn mark_circuit_failed(&mut self, ioc: SocketAddr, error: PutError) {
        if let Some(tasks) = self.waiting_for_circuit.remove(&ioc) {
            for task in tasks {
                let _ = task.response.send(Err(error.clone()));
            }
        }
    }
    fn fulfill(&mut self, ioid: Ioid, result: Result<(), ClientError>) {
        if let Some(req) = self.dispatched.remove(&ioid) {
            let dbr = result.map_err(|_| PutError::InternalClientError);
            let _ = req.response.send(dbr);
        }
    }
}
impl Requests<SubscriptionRequest> {
    fn mark_circuit_failed(&mut self, ioc: SocketAddr) {
        if let Some(tasks) = self.waiting_for_circuit.remove(&ioc) {
            self.searching_for_pv.extend(tasks);
        }
    }
}
impl ClientInternal {
    async fn start(
        cancel_token: CancellationToken,
        search_port: u16,
        beacon_port: u16,
        broadcast_addresses: Option<Vec<SocketAddr>>,
        subscriptions: Arc<Mutex<SubscriptionKeeper>>,
        started: oneshot::Sender<Result<mpsc::UnboundedSender<ClientRequest>, io::Error>>,
    ) {
        // Make a channel for search results
        let (search_tx, search_rx) = mpsc::unbounded_channel();

        let searcher = match Self::start_searcher(
            search_port,
            broadcast_addresses,
            cancel_token.clone(),
            search_tx,
        )
        .await
        {
            Ok(x) => x,
            Err(e) => {
                let _ = started.send(Err(e));
                return;
            }
        };
        let (tx, rx) = mpsc::unbounded_channel();
        let (tx_i, mut rx_i) = mpsc::unbounded_channel();

        let mut internal = ClientInternal {
            cancellation: cancel_token,
            subscriptions,
            pv_search_results: search_rx,
            circuits: HashMap::new(),
            observed_beacons: Default::default(),
            searcher,
            requests: rx,
            subtasks: Default::default(),
            known_ioc: Default::default(),
            internal_requests: tx_i,
            next_internal_id: 0,
            pending_reads: Default::default(),
            pending_writes: Default::default(),
            pending_subs: Default::default(),
            active_subs: Default::default(),
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
                Some((pv, addr)) = internal.pv_search_results.recv() => internal.handle_pv_result(&pv, addr),
            }
        }
    }

    async fn start_searcher(
        search_port: u16,
        broadcast_addresses: Option<Vec<SocketAddr>>,
        cancel_token: CancellationToken,
        results_sender: mpsc::UnboundedSender<(String, Option<SocketAddr>)>,
    ) -> Result<Searcher, io::Error> {
        // let cancel = CancellationToken::new();
        let builder = SearcherBuilder::new()
            .search_port(search_port)
            .stop_token(cancel_token)
            .report_to(results_sender);
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
                let ioc: Option<SocketAddr> = self.queue_ioc_search(&name, false);
                self.pending_reads.add(
                    ReadRequest {
                        name: name.clone(),
                        kind,
                        length,
                        response: result,
                        ioid: None,
                    },
                    ioc,
                );
            }
            ClientRequest::Put {
                name,
                value,
                result,
            } => {
                debug!("Got CA write request for {name}");
                let ioc = self.queue_ioc_search(&name, false);
                self.pending_writes.add(
                    WriteRequest {
                        name,
                        value,
                        response: result,
                        ioid: None,
                    },
                    ioc,
                );
            }
            ClientRequest::Subscribe(request) => {
                let ioc = self.queue_ioc_search(&request.name, true);
                self.pending_subs.add(request, ioc);
            }
        }
    }

    /// Do logic for searching for IOC
    ///
    /// If the IOC is already known, then it is returned (although the internal
    /// message saying that it is available is also posted).
    ///
    /// If the IOC is not known, then a search will be started.
    fn queue_ioc_search(&mut self, name: &str, eternal: bool) -> Option<SocketAddr> {
        if let Some(addr) = self.known_ioc.get(name) {
            debug!("  Known IOC: {addr}");
            // We already have it, send ourselves a message to proceed
            let _ = self
                .internal_requests
                .send(ClientInternalRequest::DeterminedPV(
                    name.to_string(),
                    Some(*addr),
                ));
            Some(*addr)
        } else {
            debug!("Doing IOC search for unrecognised PV: {name}");
            // We don't know this IOC, we have to search for it.
            if eternal {
                self.searcher.queue_search_until_found(name.to_string());
            } else {
                self.searcher.queue_search(name);
            }
            None
        }
    }

    fn handle_pv_result(&mut self, name: &str, address: Option<SocketAddr>) {
        debug!("Got notification of PV {name} search conclusion: {address:?}");
        // If this search failed, notify reads and writes
        let Some(addr) = address else {
            self.pending_reads.mark_no_pv_found(name);
            self.pending_writes.mark_no_pv_found(name);
            return;
        };
        // Mark anything waiting for this as now waiting for the circuit
        self.pending_reads.found_pv(name, addr);
        self.pending_writes.found_pv(name, addr);
        self.pending_subs.found_pv(name, addr);
        self.known_ioc.insert(name.to_string(), addr);

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
                    match Circuit::connect(&addr, None, None, req.clone()).await {
                        Ok(circuit) => {
                            let _ = req.send(ClientInternalRequest::OpenedCircuit(circuit));
                        }
                        Err(e) => {
                            warn!("Failed to connect to Circuit {addr}: {e}");
                            let _ = req.send(ClientInternalRequest::CircuitOpenFailed(addr));
                        }
                    }
                });
            }
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
            ClientInternalRequest::DeterminedPV(name, address) => {
                self.handle_pv_result(&name, address)
            }
            ClientInternalRequest::OpenedCircuit(circuit) => {
                debug!("Circuit to {} opened", circuit.address);
                self.send_outstanding_requests_for(&circuit);
                self.circuits
                    .insert(circuit.address, CircuitState::Open(circuit));
            }
            ClientInternalRequest::CircuitOpenFailed(socket_addr) => {
                warn!("Circuit {socket_addr} open failed, failing reads");
                self.pending_reads
                    .mark_circuit_failed(socket_addr, GetError::InternalClientError);
                self.pending_writes
                    .mark_circuit_failed(socket_addr, PutError::InternalClientError);
                self.pending_subs.mark_circuit_failed(socket_addr);
            }
            ClientInternalRequest::ReadAvailable(ioid, result) => {
                trace!("Got read {ioid} from circuit");
                self.pending_reads.fulfill(ioid, result);
            }
            ClientInternalRequest::WriteComplete(ioid, result) => {
                trace!("Got write {ioid} from circuit");
                self.pending_writes.fulfill(ioid, result);
            }
            ClientInternalRequest::CircuitClosed(socket_addr, remote_exit) => {
                // If we didn't initiate the closure, then drop any associated PV
                if remote_exit {
                    trace!(
                        "Remote {socket_addr} closed connection, dropping any known PV associations"
                    );
                    self.known_ioc.retain(|_, v| *v != socket_addr);
                }
                self.circuits.remove(&socket_addr);
                // Handle any outstanding interactions
                // Easy: mark any in-flight reads or writes as failed
                self.pending_reads
                    .mark_circuit_failed(socket_addr, GetError::Closed);
                self.pending_writes
                    .mark_circuit_failed(socket_addr, PutError::Closed);
                // Subscriptions go back to searching for PV
                if let Some(active_subs) = self.active_subs.remove(&socket_addr) {
                    for ioid in active_subs {
                        if let Some(request) = self.pending_subs.dispatched.remove(&ioid) {
                            // Retry this subscription
                            debug!("Resubmitting subscription to {}", request.name);
                            let ioc = self.queue_ioc_search(&request.name, true);
                            self.pending_subs.add(request, ioc);
                        }
                    }
                };
            }
        }
    }

    fn send_outstanding_requests_for(&mut self, circuit: &Circuit) {
        let mut internal_ioid = self.next_internal_id;

        // Dispatch any read requests waiting for this
        for request in self.pending_reads.get_waiting_for(circuit.address) {
            let ioid = wrapping_inplace_add(&mut internal_ioid);

            let result = circuit.read_spawn(&request.name, request.kind, Some(request.length));
            let mq = self.internal_requests.clone();
            self.subtasks.spawn(async move {
                if let Ok(response) = result.await {
                    let _ = mq.send(ClientInternalRequest::ReadAvailable(ioid, response));
                }
            });
            self.pending_reads.dispatched(ioid, request);
        }

        // Go through all open write requests
        for request in self.pending_writes.get_waiting_for(circuit.address) {
            let result = circuit.write_spawn(&request.name, request.value.clone());
            let ioid = wrapping_inplace_add(&mut internal_ioid);
            // request.ioid = Some(ioid);
            let mq = self.internal_requests.clone();
            self.subtasks.spawn(async move {
                if let Ok(response) = result.await {
                    let _ = mq.send(ClientInternalRequest::WriteComplete(ioid, response));
                }
            });
            self.pending_writes.dispatched(ioid, request);
        }

        trace!("Pending subs: {:?}", self.pending_subs);
        // Go through all open subscription requests
        for request in self.pending_subs.get_waiting_for(circuit.address) {
            trace!("Sending outstanding request {request:?}");
            let ioid = wrapping_inplace_add(&mut internal_ioid);
            // Get the senders for this
            let senders = self
                .subscriptions
                .lock()
                .unwrap()
                .get_senders(&request.name);

            circuit.subscribe_spawn(request.clone(), senders);
            self.pending_subs.dispatched(ioid, request);
            self.active_subs
                .entry(circuit.address)
                .or_default()
                .push(ioid);
        }
        self.next_internal_id = internal_ioid;
    }

    /// Get an internal IOID for matching responses
    fn get_next_id(&mut self) -> Ioid {
        let id = self.next_internal_id;
        self.next_internal_id = self.next_internal_id.wrapping_add(1);
        id
    }
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

#[derive(thiserror::Error, Debug, Clone)]
pub enum PutError {
    #[error("The internal client has closed")]
    Closed,
    #[error("Could not find a source IOC for this PV name")]
    CouldNotFindIOC,
    #[error("Internal client error")]
    InternalClientError,
}

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
    /// Request a subscription.
    ///
    /// This assumes you have already obtained the subscription Receiver.
    Subscribe(SubscriptionRequest),
}

/// Internal requests to manage state within the CircuitInternal
#[derive(Debug)]
enum ClientInternalRequest {
    /// A search for a PV has concluded, either positively or negatively
    DeterminedPV(String, Option<SocketAddr>),
    OpenedCircuit(Circuit),
    /// Sent to mark a circuit as closed
    CircuitClosed(SocketAddr, bool),
    CircuitOpenFailed(SocketAddr),
    ReadAvailable(Ioid, Result<Dbr, ClientError>),
    /// A write was completed, along with the result
    WriteComplete(Ioid, Result<(), ClientError>),
}

pub struct Client {
    /// The central cancellation token, to completely shut down the client
    cancellation: CancellationToken,
    /// Communication with the internal processing loop
    internal_requests: mpsc::UnboundedSender<ClientRequest>,
    /// Handle to know when the internal loop has finished
    handle: Option<JoinHandle<()>>,
    /// Access and modify the open subscriptions objects
    subscriptions: Arc<Mutex<SubscriptionKeeper>>,
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

        let subscriptions = Arc::new(Mutex::new(SubscriptionKeeper::new()));

        let internal_cancel = cancel.clone();
        let inner_subscriptions = subscriptions.clone();
        let handle = tokio::spawn(async move {
            ClientInternal::start(
                internal_cancel,
                search_port,
                beacon_port,
                broadcast_addresses,
                inner_subscriptions,
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
            subscriptions,
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
        self.internal_requests
            .send(ClientRequest::Get {
                name: name.to_string(),
                kind,
                length: 0,
                result: tx,
            })
            .map_err(|_| GetError::Closed)?;

        // Wait for some response
        let Ok(result) = rx.await else {
            // The other end of the oneshot was closed
            return Err(GetError::Closed);
        };
        result
    }

    pub async fn put(&self, name: &str, value: impl Into<DbrValue>) -> Result<(), PutError> {
        let (tx, rx) = oneshot::channel();
        self.internal_requests
            .send(ClientRequest::Put {
                name: name.to_string(),
                value: value.into(),
                result: tx,
            })
            .map_err(|_| PutError::Closed)?;
        rx.await.unwrap_or(Err(PutError::Closed))
    }

    /// Request subscription to a specific PV by name
    ///
    /// This will return immediately with the receiver that events will
    /// arrive on. This is because connection will be retried until a
    /// suitable connection has been found, and if connection is lost
    /// then reconnect will be attempted.
    ///
    /// Events will arrive once a connection is made.
    ///
    /// If the connection is closed, then a message with the inner
    /// [Option<Dbr>] of `None` will be sent. No further messages will
    /// be sent until reconnection, unless the client object itself is
    /// closed (at which point the outer [broadcast::Receiver] will
    /// return it's [broadcast::error::RecvError]).
    ///
    /// To unsubscribe, drop the receiver.
    ///
    pub fn subscribe<T>(&self, name: &str) -> Subscription<T>
    where
        T: for<'a> TryFrom<&'a DbrValue>,
    {
        let (rec, _) = self.subscriptions.lock().unwrap().get_receivers(name);
        let _ = self
            .internal_requests
            .send(ClientRequest::Subscribe(SubscriptionRequest {
                name: name.to_string(),
                kind: DbrCategory::Basic,
                basic_type: None,
                length: 0,
                monitor: MonitorMask::VALUE,
            }));
        Subscription::new(rec)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::providers::IntercomProvider;
    use crate::utils::connected_client_server;
    use crate::{Client, Server, ServerBuilder};
    use tracing::info;
    use tracing::level_filters::LevelFilter;
    use tracing_subscriber::fmt::TestWriter;

    #[tokio::test]
    async fn test_stop() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::DEBUG)
            .try_init();

        let mut client = Client::new().await.unwrap();
        client.stop().await;
    }

    #[tokio::test]
    async fn test_basic_get() {
        let mut provider = IntercomProvider::new();
        let _pv = provider.add_pv("TEST", 10i8).unwrap();
        let (client, _server) = connected_client_server(provider).await;
        assert_eq!(client.get::<i8>("TEST").await.unwrap(), 10i8);
    }
    #[tokio::test]
    async fn test_monitor() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(LevelFilter::TRACE)
            .with_writer(TestWriter::new())
            .try_init();
        let mut provider = IntercomProvider::new();
        let pv = provider.add_pv("TEST", 10i8).unwrap();
        let (client, server) = connected_client_server(provider).await;
        let mut monitor = client.subscribe::<i16>("TEST");
        assert_eq!(monitor.recv().await.unwrap(), 10i16);
        pv.store(42);
        assert_eq!(monitor.recv().await.unwrap(), 42);
        // Make the server disconnect
        let search_port = server.search_port();
        server.stop().await.unwrap();
        info!("Starting new server");
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut provider = IntercomProvider::new();
        provider.add_pv("TEST", 99i8).unwrap();
        let server = ServerBuilder::new(provider)
            .beacons(false)
            .search_port(search_port)
            .start()
            .await
            .unwrap();
        assert_eq!(monitor.recv().await.unwrap(), 99);
        let _ = server.stop().await;
    }
}
