#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::HashMap,
    io::ErrorKind,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{io, net::UdpSocket, select};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    client::Searcher,
    messages::{CAMessage, RsrvIsUp},
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
    searcher: Searcher,
}

impl Client {
    pub async fn new() -> Result<Client, io::Error> {
        let mut client = Client {
            beacon_port: 5065,
            search_port: 5064,
            broadcast_addresses: get_default_broadcast_ips(),
            observed_beacons: Arc::new(Mutex::new(HashMap::new())),
            circuits: Vec::new(),
            cancellation: CancellationToken::new(),
            searcher: Searcher::start().await.unwrap(),
        };
        client.start().await?;
        Ok(client)
    }

    async fn start(&mut self) -> Result<(), io::Error> {
        self.watch_broadcasts(self.cancellation.clone()).await?;
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
