use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use tokio::sync::{
    broadcast::{self},
    mpsc::{self, error::TrySendError},
};

use crate::{
    database::{DBRType, Dbr, DbrValue, IntoDBRBasicType, Status},
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

#[derive(Clone, Debug)]
struct PV {
    name: String,
    value: Arc<Mutex<DbrValue>>,
    timestamp: SystemTime,
    /// Channel to send updates to EPIC clients
    sender: broadcast::Sender<Dbr>,
    triggers: Vec<mpsc::Sender<String>>,
}

// impl Clone for PV {
//     fn clone(&self) -> Self {
//         Self {
//             name: self.name.clone(),
//             value: self.value.clone(),
//             timestamp: self.timestamp.clone(),
//             sender: self.sender.clone(),
//         }
//     }
// }

impl PV {
    pub fn load(&self) -> DbrValue {
        let value = self.value.lock().unwrap();
        value.clone()
    }

    pub fn store(&mut self, value: &DbrValue) -> Result<(), ErrorCondition> {
        // Not update the shared value
        let stored_value = &mut *self.value.lock().unwrap();
        *stored_value = value.convert_to(stored_value.get_type())?;
        self.timestamp = SystemTime::now();
        // Now send off the new value to any listeners
        let _ = self.sender.send(Dbr::Time {
            status: Status::default(),
            timestamp: self.timestamp,
            value: value.clone(),
        });
        // Send the "please look at" triggers, filtering out any that are dead
        self.triggers = self
            .triggers
            .iter()
            .filter_map(|t| match t.try_send(self.name.clone()) {
                Ok(_) => Some(t.clone()),
                Err(TrySendError::Full(_)) => Some(t.clone()),
                Err(TrySendError::Closed(_)) => None,
            })
            .collect();
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
    DbrValue: From<Vec<T>>,
{
    fn new(pv: Arc<Mutex<PV>>) -> Self {
        Self {
            pv,
            _marker: PhantomData,
        }
    }

    pub fn load(&self) -> T {
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
            .store(&(vec![value.clone()]).into())
            .expect("Provider logic should ensure this never fails");
    }
}

#[derive(Debug)]
pub struct PVAlreadyExists;

#[derive(Clone, Default)]
pub struct IntercomProvider {
    pvs: Arc<Mutex<HashMap<String, Arc<Mutex<PV>>>>>,
}

impl IntercomProvider {
    pub fn new() -> IntercomProvider {
        IntercomProvider {
            pvs: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_pv<T>(
        &mut self,
        name: &str,
        initial_value: T,
    ) -> Result<Intercom<T>, PVAlreadyExists>
    where
        T: IntoDBRBasicType + Clone + Default,
        for<'a> Vec<T>: TryFrom<&'a DbrValue>,
        DbrValue: From<Vec<T>>,
    {
        let pv = Arc::new(Mutex::new(PV {
            name: name.to_owned(),
            value: Arc::new(Mutex::new(DbrValue::from(vec![initial_value.clone()]))),
            timestamp: SystemTime::now(),
            sender: broadcast::channel(16).0,
            triggers: Vec::new(),
        }));
        let mut pvmap = self.pvs.lock().unwrap();
        if pvmap.contains_key(name) {
            return Err(PVAlreadyExists);
        }
        let _ = pvmap.insert(name.to_string(), pv.clone());
        Ok(Intercom::<T>::new(pv))
    }
}

impl Provider for IntercomProvider {
    fn provides(&self, pv_name: &str) -> bool {
        self.pvs.lock().unwrap().contains_key(pv_name)
    }

    fn read_value(
        &self,
        pv_name: &str,
        _requested_type: Option<DBRType>,
    ) -> Result<Dbr, ErrorCondition> {
        let pvmap = self.pvs.lock().unwrap();
        let pv = pvmap
            .get(pv_name)
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        Ok(Dbr::Time {
            status: Status::default(),
            timestamp: pv.timestamp,
            value: pv.load(),
        })
    }

    fn get_access_right(
        &self,
        _pv_name: &str,
        _client_user_name: Option<&str>,
        _client_host_name: Option<&str>,
    ) -> messages::AccessRight {
        messages::AccessRight::ReadWrite
    }

    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(pv_name)
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        println!("Provider: Processing write: {value:?}");
        if let Err(e) = pv.store(value.value()) {
            println!("    Error: {e:?}");
            Err(e)
        } else {
            Ok(())
        }
    }

    fn monitor_value(
        &mut self,
        pv_name: &str,
        _data_type: DBRType,
        _data_count: usize,
        _mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Dbr>, ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(pv_name)
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        pv.triggers.push(trigger);
        Ok(pv.sender.subscribe())
    }
}
