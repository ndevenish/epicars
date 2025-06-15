// use tokio::sync::{self, mpsc};

use crate::database::{DBRType, Dbr};

/// Provides PV values for a CAServer
pub trait Provider {
    /// Does this provider control the given PV name?
    fn provides(&self, pv_name: String) -> bool;

    /// Fetch a single PV value.
    ///
    /// The type requested by the caller is provided, but this is only
    /// a request - you can return any type you wish from this function,
    /// and it will be automatically converted to the target type (if
    /// possible).
    fn get_value(&self, pv_name: String, requested_type: DBRType) -> Result<Dbr, ()>;

    // /// Request the start of
    // fn monitor_value(pv_name : String, watcher : mpsc::Sender<Dbr>, requested_type : DBRType);

    // fn set_value(pv_name : String, value : Dbr) ->
}
