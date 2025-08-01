use std::{
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use tokio::sync::{
    broadcast::{self, error::TryRecvError},
    mpsc,
};

use crate::{
    database::{DBRType, Dbr, DbrValue, IntoDBRBasicType},
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
    /// such a safe conversion exists).
    ///
    /// The record that you return with no requested_type is used for
    /// the native type and data count that is reported to new subscribers.
    fn read_value(
        &self,
        pv_name: &str,
        requested_type: Option<DBRType>,
    ) -> Result<Dbr, ErrorCondition>;

    #[allow(unused_variables)]
    fn get_access_right(
        &self,
        pv_name: &str,
        client_user_name: Option<&str>,
        client_host_name: Option<&str>,
    ) -> messages::AccessRight {
        messages::AccessRight::Read
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
        data_type: DBRType,
        data_count: usize,
        mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Dbr>, ErrorCondition> {
        Err(ErrorCondition::UnavailInServ)
    }
}

struct PV {
    name: String,
    value: Arc<Mutex<DbrValue>>,
    /// Channel to send updates to EPIC clients
    sender: broadcast::Sender<(String, DbrValue)>,
    /// Place where updates get sent to this object
    receiver: broadcast::Receiver<DbrValue>,
}

impl Clone for PV {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            value: self.value.clone(),
            sender: self.sender.clone(),
            receiver: self.receiver.resubscribe(),
        }
    }
}

impl PV {
    pub fn load(&mut self) -> DbrValue {
        let mut value = self.value.lock().unwrap();
        loop {
            let new_value = match self.receiver.try_recv() {
                Ok(v) => v,
                Err(TryRecvError::Lagged(_)) => continue,
                _ => break,
            };
            *value = new_value;
        }
        value.clone()
    }

    pub fn store(&mut self, value: &DbrValue) -> Result<(), ErrorCondition> {
        // Ensure we "discard" any messages for this instance
        self.receiver = self.receiver.resubscribe();
        // Not update the shared value
        let stored_value = &mut *self.value.lock().unwrap();
        // *stored_value = (&vec![value.clone()]).into();
        *stored_value = value.convert_to(stored_value.get_type())?;
        // Now send off any notifications for this
        let _ = self.sender.send((self.name.clone(), stored_value.clone()));
        Ok(())
    }
}

/// Typed interface to reading single values to/from a PV
#[derive(Clone)]
pub struct Intercom<T>
where
    T: IntoDBRBasicType,
{
    pv: Arc<Mutex<PV>>,
    _marker: PhantomData<T>,
}

impl<T> Intercom<T>
where
    T: IntoDBRBasicType + Clone + Default,
    for<'a> Vec<T>: TryFrom<&'a DbrValue>,
    for<'a> DbrValue: From<&'a Vec<T>>,
{
    pub fn load(&mut self) -> T {
        let value = self.pv.lock().unwrap().load();
        // Convert the DbrValue into T
        let ex: Vec<T> = match (&value).try_into() {
            Ok(v) => v,
            _ => panic!("Provider logic should ensure this conversion never fails!"),
        };
        // Extract the zeroth value from this vector
        ex.first().unwrap_or(&T::default()).clone()
    }

    pub fn store(&mut self, value: &T) {
        self.pv
            .lock()
            .unwrap()
            .store(&(&vec![value.clone()]).into())
            .expect("Provider logic should ensure this never fails");
    }
}

// pub struct IntercomProvider {
//     pvs : Arc<Mutex<HashMap<String, Inter>
// }
