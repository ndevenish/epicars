// use tokio::sync::{self, mpsc};

use tokio::sync::{broadcast, mpsc};

use crate::{
    database::{DBRType, Record},
    messages::{self, ErrorCondition, MonitorMask},
};

/// Provides PV values for a CAServer
pub trait Provider: Sync + Send + Clone + 'static {
    /// Does this provider control the given PV name?
    fn provides(&self, pv_name: &str) -> bool;

    /// Fetch a single PV value.
    ///
    /// The type requested by the caller is provided, but this is only
    /// a request - you can return any type you wish from this function,
    /// and it will be automatically converted to the target type (if
    /// possible).
    ///
    /// The record that you return with no requested_type is used for
    /// the native type and data count that is reported to new subscribers.
    fn read_value(
        &self,
        pv_name: &str,
        requested_type: Option<DBRType>,
    ) -> Result<Record, ErrorCondition>;

    #[allow(unused_variables)]
    fn get_access_right(
        &self,
        pv_name: &str,
        client_user_name: Option<&str>,
        client_host_name: Option<&str>,
    ) -> messages::AccessRight {
        messages::AccessRight::Read
    }

    /// Write a value to a PV
    #[allow(unused_variables)]
    fn write_value(&mut self, pv_name: &str, value: &[&str]) -> Result<(), ErrorCondition> {
        Err(ErrorCondition::NoWtAccess)
    }

    #[allow(unused_variables)]
    fn monitor_value(
        &mut self,
        pv_name: &str,
        mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Record>, ErrorCondition> {
        Err(ErrorCondition::UnavailInServ)
    }
}
