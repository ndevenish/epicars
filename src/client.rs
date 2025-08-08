#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::HashMap,
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    io,
    net::UdpSocket,
    select,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::messages::{self, AsBytes, CAMessage, Message, RsrvIsUp};

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

struct Circuit {
    address: Ipv4Addr,
    port: u16,
}

pub struct SearcherBuilder {
    search_port: u16,
    stop_token: CancellationToken,
    broadcast_addresses: Option<Vec<IpAddr>>,
}

impl Default for SearcherBuilder {
    fn default() -> Self {
        SearcherBuilder {
            search_port: 5064,
            stop_token: CancellationToken::new(),
            broadcast_addresses: None,
        }
    }
}
impl SearcherBuilder {
    pub fn new() -> Self {
        SearcherBuilder::default()
    }
    pub async fn start(self) -> Result<Searcher, io::Error> {
        let (send, request_recv) = mpsc::channel(32);
        let mut searcher = Searcher {
            pending_requests: send,
            search_port: self.search_port,
            stop_token: CancellationToken::new(),
            broadcast_addresses: self
                .broadcast_addresses
                .unwrap_or_else(get_default_broadcast_ips),
        };
        searcher
            .start_searching(request_recv)
            .await
            .and(Ok(searcher))
    }
}

#[derive(Debug)]
pub struct Searcher {
    /// Submit requests to search for new PVs
    pending_requests: mpsc::Sender<(
        String,
        oneshot::Sender<broadcast::Receiver<Option<SocketAddr>>>,
    )>,
    /// The port to send request broadcasts to
    search_port: u16,
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<IpAddr>,
    stop_token: CancellationToken,
}

impl Searcher {
    async fn start_searching(
        &mut self,
        mut incoming_requests: mpsc::Receiver<(
            String,
            oneshot::Sender<broadcast::Receiver<Option<SocketAddr>>>,
        )>,
    ) -> Result<(), io::Error> {
        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        send_socket.set_broadcast(true).unwrap();

        let mut state = SearcherInternal {
            search_port: self.search_port,
            broadcast_addresses: self.broadcast_addresses.clone(),
            stop_token: self.stop_token.clone(),
            ..Default::default()
        };

        tokio::spawn(async move {
            loop {
                let mut requests = Vec::new();
                let mut buffer = vec![0u8; 0xFFFF];

                select! {
                    _ = state.stop_token.cancelled() => break,
                    _ = incoming_requests.recv_many(&mut requests, 32) => if requests.is_empty() { break; } else {
                        state.handle_new_requests(&send_socket, requests).await
                    },
                    result = send_socket.recv_from(&mut buffer) => match result {
                        Ok((size, sender)) => state.handle_response(&buffer[..size], sender).await,
                        Err(e) => {
                            error!("Error waiting for search responses: {e}");
                        },
                    }
                };
            }
        });
        Ok(())
    }

    /// Get the SocketAddr for the server serving a specific PV
    pub async fn search_for(&self, name: &str) -> Result<SocketAddr, CouldNotFindError> {
        let (ret_send, ret_recv) = oneshot::channel::<broadcast::Receiver<Option<SocketAddr>>>();
        // Send the request into our async search loop
        self.pending_requests
            .send((name.to_string(), ret_send))
            .await
            .map_err(|_| CouldNotFindError)?;
        // Get the receiver back from here
        let mut result_receiver = ret_recv.await.map_err(|_| CouldNotFindError)?;
        // Now, wait on this
        result_receiver
            .recv()
            .await
            .unwrap_or(None)
            .ok_or(CouldNotFindError)
    }
}

#[derive(Debug)]
pub struct CouldNotFindError;

struct SearchAttempt {
    name: String,
    attempt: u8,
    timestamp: Instant,
}
impl SearchAttempt {
    /// Get the time that we should try to search again
    fn get_next_search_timestamp(&self) -> Option<Instant> {
        None
    }
}

/// Handle searcher internal state, inside a single Async context
#[derive(Default)]
struct SearcherInternal {
    /// The port to send request broadcasts to
    search_port: u16,
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<IpAddr>,
    /// Keep track of outstanding requests
    in_flight: HashMap<u32, SearchAttempt>,
    /// Per-PV data for in-flight requests
    per_pv_info: HashMap<String, (broadcast::Sender<Option<SocketAddr>>, Vec<u32>)>,
    stop_token: CancellationToken,
    search_id: u32,
}
impl SearcherInternal {
    async fn handle_new_requests(
        &mut self,
        socket: &UdpSocket,
        requests: Vec<(
            String,
            oneshot::Sender<broadcast::Receiver<Option<SocketAddr>>>,
        )>,
    ) {
        // We have received messages on the buffer
        debug_assert!(!requests.is_empty());

        let mut messages = vec![Message::Version(messages::Version::default())];
        for (name, waiter_reply) in requests {
            // Get or create an entry in our per-PV map to keep track of everything
            let (update_sender, search_ids) = self
                .per_pv_info
                .entry(name.clone())
                .or_insert_with(|| (broadcast::Sender::new(1), Vec::new()));
            let _ = waiter_reply.send(update_sender.subscribe());
            // Register this search attempt
            self.in_flight.insert(
                self.search_id,
                SearchAttempt {
                    name: name.clone(),
                    attempt: 0,
                    timestamp: Instant::now(),
                },
            );
            search_ids.push(self.search_id);
            // Build the search message for this
            messages.push(Message::Search(messages::Search {
                search_id: self.search_id,
                channel_name: name.clone(),
                ..Default::default()
            }));
            debug!("Sending search for {name}");
            // Increment our search counter
            self.search_id = self.search_id.wrapping_add(1);
        }

        // Build a single search packet for all of these
        let buffer: Vec<_> = messages.into_iter().flat_map(|m| m.as_bytes()).collect();
        // Send it to all of our broadcast IPs
        for ip in &self.broadcast_addresses {
            let target_addr = (*ip, self.search_port).into();
            debug!("Sending to: {target_addr}");
            socket
                .send_to::<SocketAddr>(&buffer, target_addr)
                .await
                .expect("Socket sending failed");
        }
    }

    async fn handle_response(&mut self, response: &[u8], sender: SocketAddr) {
        let Ok(messages) = Message::parse_many_client_messages(response) else {
            warn!("Received unparseable search response");
            return;
        };
        for message in messages {
            let response = match message {
                Message::SearchResponse(search_response) => search_response,
                Message::Version(_) => continue,
                m => {
                    warn!("Received unexpected search response: {m:?}");
                    continue;
                }
            };
            // What was this a response to?
            let Some(search) = self.in_flight.remove(&response.search_id) else {
                warn!("Received unrequested or duplicate search response");
                continue;
            };
            // Now we know we have a response to an actual request - clear out any past
            // requests for this and send the notification up to the caller
            let (success_sender, searchers) = self.per_pv_info.remove(&search.name).unwrap();
            // Get rid of any other in-flight searches for this
            for search_id in searchers {
                self.in_flight.remove(&search_id);
            }
            let server_origin = (
                response.server_ip.map(|i| i.into()).unwrap_or(sender.ip()),
                response.port_number,
            )
                .into();
            // Report this to all the listeners
            debug!("Found server for {}: {server_origin:?}", search.name);
            let _ = success_sender.send(Some(server_origin));
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
    circuits: Vec<Circuit>,
    /// The cancellation token
    cancellation: CancellationToken,
}

impl Client {
    pub fn new(beacon_port: u16, search_port: u16, broadcast_addresses: Vec<IpAddr>) -> Client {
        Client {
            beacon_port,
            search_port,
            broadcast_addresses,
            ..Default::default()
        }
    }

    pub async fn start(&mut self) {
        // Open a UDP socket to listen for broadcast replies
        let _search_reply_socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

        self.watch_broadcasts(self.cancellation.clone())
            .await
            .unwrap();
    }

    /// Watch for broadcast beacons, and record their ID and timestamp into the client map
    async fn watch_broadcasts(&self, stop: CancellationToken) -> Result<(), io::Error> {
        let port = self.beacon_port;
        let beacon_map = self.observed_beacons.clone();
        // Bind the socket first, so that we know early if it fails
        let broadcast_socket = UdpSocket::bind(SocketAddr::new([0, 0, 0, 0].into(), port)).await?;
        tokio::spawn(async move {
            let mut buf: Vec<u8> = vec![0; 0xFFFF];

            loop {
                select! {
                    _ = stop.cancelled() => break,
                    r = broadcast_socket.recv_from(&mut buf) => match r {
                    Ok((size, addr)) => {
                        if let Ok((_, beacon)) = RsrvIsUp::parse(&buf[..size]) {
                            debug!("Observed beacon: {beacon:?}");
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

impl Default for Client {
    fn default() -> Self {
        Client {
            beacon_port: 5065,
            search_port: 5064,
            broadcast_addresses: Vec::new(),
            observed_beacons: Arc::new(Mutex::new(HashMap::new())),
            circuits: Vec::new(),
            cancellation: CancellationToken::new(),
        }
    }
}
