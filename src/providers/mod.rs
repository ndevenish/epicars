//! Interface between the CA Server and rust code

pub mod intercom;
pub use intercom::IntercomProvider;

use tokio::sync::{
    broadcast::{self},
    mpsc::{self},
};

use crate::{
    dbr::{Dbr, DbrType},
    messages::{self, ErrorCondition, MonitorMask},
};

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
    ///
    #[allow(unused_variables)]
    fn monitor_value(
        &mut self,
        pv_name: &str,
        unique_subscriber_id: u64,
        data_type: DbrType,
        data_count: usize,
        mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Dbr>, ErrorCondition> {
        Err(ErrorCondition::UnavailInServ)
    }

    #[allow(unused_variables)]
    fn cancel_monitor_value(
        &mut self,
        pv_name: &str,
        unique_subscriber_id: u64,
        data_type: DbrType,
        data_count: usize,
    ) {
    }
}
