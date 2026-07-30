//! Interface between the CA Server and rust code

pub mod intercom;
pub use intercom::IntercomProvider;

use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{
    broadcast::{self},
    mpsc::{self},
};

use crate::{
    dbr::{Dbr, DbrType},
    messages::{self, ErrorCondition, MonitorMask},
    value::{Value, meta::Meta},
};

/// An opaque handle to one subscription, allocated by the [`Provider`] that owns it.
///
/// Allocated by the provider rather than derived by the server, because the provider is
/// the only party that can guarantee uniqueness: a `Provider` is `Clone` and one clone per
/// circuit is the *point*, so several servers can share the same underlying state. The
/// server used to compute this as `(circuit_id << 32) | channel_id`, with the circuit
/// counter restarting at 0 for each `Server` - so two servers over one provider both asked
/// for key 0 and one silently overwrote the other's trigger.
///
/// The value carries no meaning; do not attempt to unpack one.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubscriberId(u64);

impl SubscriberId {
    /// Allocate an ID that no other `SubscriberId` in this process will equal.
    ///
    /// Process-wide rather than per-provider so that two providers cannot collide either,
    /// which costs one atomic increment per subscription.
    pub fn new() -> SubscriberId {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        SubscriberId(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

impl Default for SubscriberId {
    fn default() -> SubscriberId {
        SubscriberId::new()
    }
}

/// Provides PV values for a CAServer
pub trait Provider: Sync + Send + Clone + Default + 'static {
    /// Does this provider control the given PV name?
    fn provides(&self, pv_name: &str) -> bool;

    /// Fetch a single PV value.
    ///
    /// The type requested by the caller is provided, but this is only
    /// a request - you can return any type you wish from this function,
    /// and it will be automatically converted to the target type (if
    /// such a safe conversion exists).
    ///
    /// The record that you return with no requested_type is used for
    /// the native type and data count that is reported to new subscribers.
    fn read_value(
        &self,
        pv_name: &str,
        requested_type: Option<DbrType>,
    ) -> Result<Dbr, ErrorCondition>;

    #[allow(unused_variables)]
    fn get_access_right(
        &self,
        pv_name: &str,
        client_user_name: Option<&str>,
        client_host_name: Option<&str>,
    ) -> messages::Access {
        messages::Access::Read
    }

    /// Write a value sent by a client to a PV
    ///
    /// There is no type information - data sent from caput appears to
    /// always be as a string?
    #[allow(unused_variables)]
    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        Err(ErrorCondition::NoWtAccess)
    }

    /// Request setting up a subscription to a PV
    ///
    /// Half of the two-channel pull model: this returns the channel updates *arrive* on,
    /// while `trigger` is how the provider says "there is something to read" - it carries
    /// the PV name, and the server only reads the broadcast receiver once woken by it.
    /// The indirection is deliberate; it is what lets the transport decide when to
    /// consume, which pvAccess's pipelined monitor flow control requires.
    ///
    /// The payload is protocol-neutral. Each server projects `(Value, Meta)` onto its own
    /// wire types, so one provider can feed a CA and a pvAccess server at once.
    ///
    /// Returns the [`SubscriberId`] the implementation allocated for this subscription;
    /// the caller must hand that back to [`Provider::cancel_monitor_value`].
    #[allow(unused_variables)]
    fn monitor_value(
        &mut self,
        pv_name: &str,
        data_type: DbrType,
        data_count: usize,
        mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<(SubscriberId, broadcast::Receiver<(Value, Meta)>), ErrorCondition> {
        Err(ErrorCondition::UnavailInServ)
    }

    /// Tear down a subscription previously returned by [`Provider::monitor_value`].
    #[allow(unused_variables)]
    fn cancel_monitor_value(
        &mut self,
        pv_name: &str,
        subscriber: SubscriberId,
        data_type: DbrType,
        data_count: usize,
    ) {
    }
}
