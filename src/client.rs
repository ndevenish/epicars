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
    sync::{mpsc, oneshot, watch},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::messages::{CAMessage, RsrvIsUp};

struct Circuit {
    address: Ipv4Addr,
    port: u16,
}

pub struct Client {
    /// Port to listen for server beacon messages
    beacon_port: u16,
    /// Multicast port on which to send searches
    search_port: u16,
    /// Interfaces to broadcast onto
    broadcast_addresses: Vec<Ipv4Addr>,
    /// Servers we have seen broadcasting.
    /// This can be used to trigger e.g. re-searching on the appearance
    /// of a new beacon server or the case of one restarting (at which
    /// point the beacon ID resets).
    observed_beacons: Arc<Mutex<HashMap<(IpAddr, u16), (u32, Instant)>>>,
    /// Active name searches and how long ago we sent them
    name_searches: HashMap<String, (u32, Instant, oneshot::Sender<SocketAddr>)>,
    search_request_queue: Option<mpsc::Sender<(String, oneshot::Sender<SocketAddr>)>>,
    /// Active connections to different servers
    circuits: Vec<Circuit>,
    /// The last search ID sent out
    search_id: u32,
    /// The cancellation token
    cancellation: CancellationToken,
}

impl Client {
    pub fn new(beacon_port: u16, search_port: u16, broadcast_addresses: Vec<Ipv4Addr>) -> Client {
        Client {
            beacon_port,
            search_port,
            broadcast_addresses,
            ..Default::default()
        }
    }

    pub async fn start(&mut self) {
        // Open a UDP socket to listen for broadcast replies
        let search_reply_socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

        self.watch_broadcasts(self.cancellation.clone())
            .await
            .unwrap();
        self.manage_search_lifecycle().await.unwrap();
    }

    async fn manage_search_lifecycle(&mut self) -> Result<(), io::Error> {
        let (send, recv) = mpsc::channel(32);
        self.search_request_queue = Some(send);
        let send_socket = UdpSocket::bind("0.0.0.0:0").await?;
        tokio::spawn(async move {});
        Ok(())
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
                                beacon.server_ip.map(|f| IpAddr::V4(f)).unwrap_or(addr.ip());
                            let mut beacons = beacon_map.lock().unwrap();
                            beacons.insert(
                                (send_ip, beacon.server_port),
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
        let interfaces = datalink::interfaces();
        let broadcast_ips: Vec<Ipv4Addr> = interfaces
            .into_iter()
            .filter(|i| !i.is_loopback())
            .flat_map(|i| i.ips.into_iter())
            .filter_map(|i| match i.broadcast() {
                IpAddr::V4(broadcast_ip) => Some(broadcast_ip),
                _ => None,
            })
            .collect();
        Client {
            beacon_port: 5065,
            search_port: 5064,
            broadcast_addresses: Vec::new(),
            observed_beacons: Arc::new(Mutex::new(HashMap::new())),
            name_searches: HashMap::new(),
            circuits: Vec::new(),
            search_id: 0,
            cancellation: CancellationToken::new(),
            search_request_queue: None,
        }
    }
}
