use std::collections::HashMap;

use tokio::sync::{broadcast, watch};

use crate::dbr::Dbr;

type SenderPair = (broadcast::Sender<Option<Dbr>>, watch::Sender<Option<Dbr>>);
/// Generate and hold subscription connections
#[derive(Default)]
pub(crate) struct SubscriptionKeeper {
    subscriptions: HashMap<String, SenderPair>,
}

impl SubscriptionKeeper {
    pub fn new() -> Self {
        SubscriptionKeeper::default()
    }
    pub fn get_receivers(
        &mut self,
        name: &str,
    ) -> (
        broadcast::Receiver<Option<Dbr>>,
        watch::Receiver<Option<Dbr>>,
    ) {
        let (send_b, send_w) = self.get_internal_senders(name);
        (send_b.subscribe(), send_w.subscribe())
    }

    pub fn get_senders(
        &mut self,
        name: &str,
    ) -> (broadcast::Sender<Option<Dbr>>, watch::Sender<Option<Dbr>>) {
        let (send_b, send_w) = self.get_internal_senders(name);
        (send_b.clone(), send_w.clone())
    }

    fn get_internal_senders(
        &mut self,
        name: &str,
    ) -> &(broadcast::Sender<Option<Dbr>>, watch::Sender<Option<Dbr>>) {
        self.subscriptions
            .entry(name.to_string())
            .or_insert_with(|| (broadcast::Sender::new(32), watch::Sender::new(None)))
    }
}
