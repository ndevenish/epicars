use std::{collections::HashMap, net::Ipv4Addr};

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
}

impl Default for Client {
    fn default() -> Self {
        Client {
            beacon_port: 5064,
            search_port: 5064,
            broadcast_addresses: Vec::new(),
            observed_beacons: HashMap::new(),
        }
    }
}
