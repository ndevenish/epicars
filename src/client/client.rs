#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::{HashMap, hash_map::Entry},
    io::ErrorKind,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{self, AsyncReadExt},
    net::TcpStream,
    select,
    sync::{mpsc, oneshot},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    client::{Searcher, searcher::CouldNotFindError},
    dbr::{Dbr, DbrType, DbrValue},
    messages::{self, CAMessage, Message, RsrvIsUp},
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
    /// Read a single value from the server
    Read {
        name: String,
        length: usize,
        kind: DbrType,
        reply: oneshot::Sender<Dbr>,
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
            }
            .circuit_lifecycle()
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
}

// Inner circuit state, used to hold async management data
struct CircuitInternal {
    /// A copy of the address we are connected to
    address: SocketAddr,
    /// When the last message was sent. Used to calculate Echo timing.
    last_sent_at: Instant,
    requests_rx: mpsc::Receiver<CircuitRequest>,
    cancel: CancellationToken,
}
impl CircuitInternal {
    async fn circuit_lifecycle(&mut self) {
        debug!("Started circuit to {}", self.address);
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
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

    pub async fn read_pv(&mut self, name: &str) -> Result<DbrValue, ClientError> {
        // First, find the server that holds this name
        let ioc = self.searcher.search_for(name).await?;
        // Get or create the circuit to this IOC
        let _circuit = match self.circuits.entry(ioc) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let circuit = Circuit::connect(&ioc, None, None).await?;
                entry.insert(circuit)
            }
        };

        panic!();
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
