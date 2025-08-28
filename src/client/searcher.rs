use num::{FromPrimitive, traits::WrappingAdd};
use std::{
    cmp::min,
    collections::HashMap,
    fmt::Display,
    future,
    net::SocketAddr,
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::{
    io,
    net::UdpSocket,
    select,
    sync::{broadcast, mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::{
    messages::{self, AsBytes, Message},
    utils::{get_default_server_port, get_target_broadcast_ips},
};

/// Increments a mutable reference in place, and returns the original value
fn wrapping_add<T: WrappingAdd + FromPrimitive + Copy>(value: &mut T) -> T {
    let id = *value;
    *value = value.wrapping_add(&T::from_u8(1).unwrap());
    id
}

pub struct SearcherBuilder {
    search_port: u16,
    stop_token: CancellationToken,
    broadcast_addresses: Option<Vec<SocketAddr>>,
    timeout: Option<Duration>,
    /// The socket that is UDP bound to receive replies
    bind_address: SocketAddr,
}

impl Default for SearcherBuilder {
    fn default() -> Self {
        SearcherBuilder {
            search_port: get_default_server_port(),
            stop_token: CancellationToken::new(),
            broadcast_addresses: None,
            timeout: Some(Duration::from_secs(1)),
            bind_address: "0.0.0.0:0".parse().unwrap(),
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
            timeout: self.timeout,
            pending_requests: send,
            stop_token: CancellationToken::new(),
            broadcast_addresses: self
                .broadcast_addresses
                .unwrap_or_else(|| get_target_broadcast_ips(get_default_server_port())),
            bind_address: self.bind_address,
        };
        searcher
            .start_searching(request_recv)
            .await
            .and(Ok(searcher))
    }
    pub fn stop_token(mut self, token: CancellationToken) -> Self {
        self.stop_token = token.child_token();
        self
    }
    pub fn search_port(mut self, port: u16) -> Self {
        self.search_port = port;
        self
    }
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }
    pub fn broadcast_to(mut self, addresses: Vec<SocketAddr>) -> Self {
        self.broadcast_addresses = Some(addresses);
        self
    }
}

#[derive(Debug)]
pub struct Searcher {
    timeout: Option<Duration>,
    /// Submit requests to search for new PVs
    pending_requests: mpsc::Sender<(
        String,
        oneshot::Sender<broadcast::Receiver<Option<SocketAddr>>>,
    )>,
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<SocketAddr>,
    stop_token: CancellationToken,
    bind_address: SocketAddr,
}

impl Searcher {
    pub async fn start() -> Result<Searcher, io::Error> {
        SearcherBuilder::new().start().await
    }
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    async fn start_searching(
        &mut self,
        mut incoming_requests: mpsc::Receiver<(
            String,
            oneshot::Sender<broadcast::Receiver<Option<SocketAddr>>>,
        )>,
    ) -> Result<(), io::Error> {
        let send_socket = UdpSocket::bind(self.bind_address).await?;
        send_socket.set_broadcast(true).unwrap();

        let mut state = SearcherInternal {
            broadcast_addresses: self.broadcast_addresses.clone(),
            stop_token: self.stop_token.clone(),
            timeout: self.timeout,
            ..Default::default()
        };

        tokio::spawn(async move {
            let mut buffer = vec![0u8; 0xFFFF];
            loop {
                let mut requests = Vec::new();
                // Work out when the next time-based check should occue

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
                    },
                    _ = state.next_attempt() => if let Some(buf) = state.handle_retries_and_timeouts() {
                        for addr in &state.broadcast_addresses {
                            debug!("Sending retry to: {addr}");
                            send_socket
                                .send_to::<SocketAddr>(&buf, *addr)
                                .await
                                .expect("Socket sending failed");
                        }
                    },
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

    pub fn stop(&self) {
        self.stop_token.cancel();
    }
    pub fn is_cancelled(&self) -> bool {
        self.stop_token.is_cancelled()
    }
}

impl Drop for Searcher {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Debug)]
pub struct CouldNotFindError;

impl Display for CouldNotFindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not find the requested PV")
    }
}

impl std::error::Error for CouldNotFindError {}

#[derive(Debug)]
struct SearchAttempt {
    name: String,
    // attempts: u8,
    search_expires_at: Option<Instant>,
    active_searches: Vec<u32>,
    next_search_at: Instant,
    /// How are results reported back to the requesters?
    reporter: broadcast::Sender<Option<SocketAddr>>,
}

impl SearchAttempt {
    /// Recalculate timings and return a new search message
    fn new_search(&mut self, search_id: u32) -> messages::Search {
        let backoff =
            Duration::from_millis(32 * 2u64.pow(min(self.active_searches.len(), 11) as u32));
        self.active_searches.push(search_id);
        self.next_search_at = Instant::now() + backoff;
        messages::Search {
            search_id,
            channel_name: self.name.clone(),
            ..Default::default()
        }
    }
}
impl Default for SearchAttempt {
    fn default() -> Self {
        SearchAttempt {
            name: String::new(),
            search_expires_at: None,
            active_searches: Vec::new(),
            next_search_at: Instant::now(),
            reporter: broadcast::Sender::new(1),
        }
    }
}

/// Handle searcher internal state, inside a single Async context
#[derive(Default)]
struct SearcherInternal {
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<SocketAddr>,
    /// Search IDs of outstanding requests to the PV name
    in_flight: HashMap<u32, String>,
    /// Data about all the PVs we are searching for
    per_pv_info: HashMap<String, SearchAttempt>,
    stop_token: CancellationToken,
    /// The next search ID to send
    search_id: u32,
    timeout: Option<Duration>,
}
impl SearcherInternal {
    /// Wait until it's time for the next tracked attempt
    fn next_attempt(&self) -> Pin<Box<dyn Future<Output = ()> + Send + 'static>> {
        let next_wake = self
            .per_pv_info
            .values()
            .flat_map(|v| [Some(v.next_search_at), v.search_expires_at])
            .flatten()
            .min();
        match next_wake {
            None => Box::pin(future::pending()),
            Some(instant) => {
                if instant < Instant::now() {
                    Box::pin(future::ready(()))
                } else {
                    Box::pin(tokio::time::sleep_until(tokio::time::Instant::from_std(
                        instant,
                    )))
                }
            }
        }
    }

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
            let info = self
                .per_pv_info
                .entry(name.clone())
                .or_insert_with(|| SearchAttempt {
                    name: name.clone(),
                    search_expires_at: self.timeout.map(|t| Instant::now() + t),
                    ..Default::default()
                });
            // Give the requester a place to wait for replies
            let _ = waiter_reply.send(info.reporter.subscribe());
            let search_id = wrapping_add(&mut self.search_id);
            // Register this search attempt
            self.in_flight.insert(search_id, name.clone());
            // Build the search message for this
            messages.push(Message::Search(info.new_search(search_id)));
            debug!("Sending search for {name}");
        }

        // Build a single search packet for all of these
        let buffer: Vec<_> = messages.into_iter().flat_map(|m| m.as_bytes()).collect();
        // Send it to all of our broadcast IPs
        for addr in &self.broadcast_addresses {
            debug!("Sending search packet to: {addr}");
            socket
                .send_to::<SocketAddr>(&buffer, *addr)
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
            let Some(pv_name) = self.in_flight.remove(&response.search_id) else {
                debug!("Received unrequested or duplicate search response");
                continue;
            };
            // Now we know we have a response to an actual request - clear out any past
            // requests for this and send the notification up to the caller
            let info = self.per_pv_info.remove(&pv_name).unwrap();
            // Get rid of any other in-flight searches for this
            for search_id in info.active_searches {
                self.in_flight.remove(&search_id);
            }
            let server_origin = (
                response.server_ip.map(|i| i.into()).unwrap_or(sender.ip()),
                response.port_number,
            )
                .into();
            // Report this to all the listeners
            debug!("Found server for {pv_name}: {server_origin:?}");
            let _ = info.reporter.send(Some(server_origin));
        }
    }

    fn handle_retries_and_timeouts(&mut self) -> Option<Vec<u8>> {
        let now = Instant::now();

        // discard any expired searches
        self.per_pv_info.retain(|_, v| match v.search_expires_at {
            None => true,
            Some(time) => {
                if time < now {
                    // We are discarding this. Send the termination signal,
                    let _ = v.reporter.send(None);
                    // And then remove from the in-flight register
                    for id in v.active_searches.iter() {
                        let _ = self.in_flight.remove(id);
                    }
                    debug!(
                        "Dropping search for {} as reached search timeout {:.2} ms ago",
                        v.name,
                        (now - time).as_secs_f32() * 1000.0
                    );
                    false
                } else {
                    true
                }
            }
        });

        let mut search_messages = self
            .per_pv_info
            .values_mut()
            .filter(|s| s.next_search_at < now)
            .map(|s| {
                debug!("Sending retry search for: {}", s.name);
                Message::Search(s.new_search(wrapping_add(&mut self.search_id)))
            })
            .peekable();

        if search_messages.peek().is_some() {
            Some(
                vec![Message::Version(messages::Version::default())]
                    .into_iter()
                    .chain(search_messages)
                    .flat_map(|m| m.as_bytes())
                    .collect(),
            )
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use std::net::IpAddr;

    use tokio::net::UdpSocket;

    use crate::{
        client::{SearcherBuilder, searcher::wrapping_add},
        messages::{AsBytes, Message},
    };

    #[test]
    fn test_wrapping_add() {
        let mut i = 3u32;
        assert_eq!(wrapping_add(&mut i), 3);
        assert_eq!(i, 4);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_search() {
        // Set up a receiver
        let incoming = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let port = incoming.local_addr().unwrap().port();

        let subtask = tokio::spawn(async move {
            let s = SearcherBuilder::new()
                .search_port(port)
                .broadcast_to(vec![IpAddr::V4([127, 0, 0, 1].into())])
                .start()
                .await
                .unwrap();
            assert_eq!(
                s.search_for("TEST").await.unwrap(),
                "127.0.0.1:6464".to_string().parse().unwrap()
            );
        });

        // Receive and validate this request
        let mut buffer = [0u8; 16384];
        let (size, source) = incoming.recv_from(&mut buffer).await.unwrap();
        let messages = Message::parse_many_server_messages(&buffer[..size]).unwrap();
        println!("{messages:?}");
        assert_eq!(messages.len(), 2);
        assert!(matches!(messages[0], Message::Version(_)));
        let Message::Search(search_msg) = &messages[1] else {
            panic!("Didn't get a search message");
        };
        assert_eq!(search_msg.channel_name, "TEST");

        // Send a message back
        incoming
            .send_to(&search_msg.respond(None, 6464, true).as_bytes(), source)
            .await
            .unwrap();

        subtask.await.unwrap();
    }
}
