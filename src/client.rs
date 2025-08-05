#![allow(dead_code)]

use pnet::datalink;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr},
    time::Instant,
};
use tracing::debug;

struct Circuit {
    address: Ipv4Addr,
    port: u16,
}

pub struct Client {
    /// Port to listen for server beacon messages
    beacon_port: u16,
    /// Multicast port on which to send searches
    search_port: u16,
    /// Interfaces to listen on
    broadcast_addresses: Vec<Ipv4Addr>,
    /// Servers we have seen broadcasting.
    /// This can be used to trigger e.g. re-searching on the appearance
    /// of a new beacon server or the case of one restarting (at which
    /// point the beacon ID resets).
    observed_beacons: HashMap<(Ipv4Addr, u16), u32>,
    /// Active name searches and how long ago we sent them
    name_searches: HashMap<String, (u32, Instant)>,
    /// Active connections to different servers
    circuits: Vec<Circuit>,
    /// The last search ID sent out
    search_id: u32,
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

    pub fn start(&mut self) {}
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
        debug!("Addresses: {broadcast_ips:?}");
        Client {
            beacon_port: 5065,
            search_port: 5064,
            broadcast_addresses: Vec::new(),
            observed_beacons: HashMap::new(),
            name_searches: HashMap::new(),
            circuits: Vec::new(),
            search_id: 0,
        }
    }
}
