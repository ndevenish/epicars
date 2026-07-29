use std::collections::HashMap;

use tokio::sync::{broadcast, watch};
use tracing::trace;

use crate::dbr::Dbr;

/// Generate and hold subscription connections
#[derive(Default)]
pub(crate) struct SubscriptionKeeper {
    subscriptions: HashMap<String, SenderPair<Dbr>>,
}

#[derive(Debug, Clone)]
pub struct SenderPair<T>
where
    T: Clone,
{
    pub broadcast: broadcast::Sender<T>,
    pub watch: watch::Sender<Option<T>>,
}

type ReceiverPair<T> = (broadcast::Receiver<T>, watch::Receiver<Option<T>>);

impl<T> SenderPair<T>
where
    T: Clone,
{
    /// Attempt to send a value to all receivers, and return the number of listeners
    ///
    /// A successful send is one where at least one Receiver was active.
    /// An unsuccessful send was one where all receivers were closed.
    pub fn send(&mut self, value: T) -> Option<usize> {
        match (
            self.broadcast.send(value.clone()),
            self.watch.send(Some(value.clone())),
        ) {
            (Ok(a), Ok(_)) => Some(a + self.watch.receiver_count()),
            (Ok(a), Err(_)) => Some(a),
            (Err(_), Ok(_)) => Some(self.watch.receiver_count()),
            (Err(_), Err(_)) => None,
        }
    }

    fn new(capacity: usize) -> Self {
        Self {
            broadcast: broadcast::Sender::new(capacity),
            watch: watch::Sender::new(None),
        }
    }
    fn subscribe(&mut self) -> ReceiverPair<T> {
        (self.broadcast.subscribe(), self.watch.subscribe())
    }
}

impl SubscriptionKeeper {
    pub fn new() -> Self {
        SubscriptionKeeper::default()
    }
    pub fn get_receivers(&mut self, name: &str) -> ReceiverPair<Dbr> {
        self.get_internal_senders(name).subscribe()
    }

    pub fn get_senders(&mut self, name: &str) -> SenderPair<Dbr> {
        self.get_internal_senders(name).clone()
    }

    fn get_internal_senders(&mut self, name: &str) -> &mut SenderPair<Dbr> {
        let r = self
            .subscriptions
            .entry(name.to_string())
            .or_insert_with(|| SenderPair::new(32));
        trace!(
            "Broadcast senders in get_internal: {}",
            r.broadcast.receiver_count()
        );
        r
    }
}
