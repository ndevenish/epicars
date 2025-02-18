use std::time::Instant;

struct Circuit {
    last_message: Instant,
    /// We must have this for the circuit to count as ready
    client_version: Option<i16>,
    client_host_name: Option<String>,
    client_user_name: Option<String>,
    client_events_on: bool,
}

impl Circuit {
    fn new() -> Self {
        Circuit {
            last_message: Instant::now(),
            client_version: None,
            client_host_name: None,
            client_user_name: None,
            client_events_on: true,
        }
    }
}

pub struct Server {
    /// Broadcast port to sent beacons
    beacon_port: u16,
    /// Port to receive search queries on
    search_port: u16,
    /// Time that last beacon was sent
    last_beacon: Instant,
    /// The beacon ID of the last beacon broadcast
    beacon_id: u32,
    circuits: Vec<Circuit>,
}

impl Default for Server {
    fn default() -> Self {
        Server {
            beacon_port: 5065,
            search_port: 5064,
            last_beacon: Instant::now(),
            beacon_id: 0,
            circuits: Vec::new(),
        }
    }
}
