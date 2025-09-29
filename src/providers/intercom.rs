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
use tracing::{debug, error};

use crate::{
    Provider,
    dbr::{DBR_CLASS_NAME, Dbr, DbrBasicType, DbrType, DbrValue, Status},
    messages::{self, ErrorCondition, MonitorMask},
};

#[derive(Clone, Debug)]
struct PV {
    name: String,
    value: Arc<Mutex<DbrValue>>,
    /// Minimum array length. If set, at least this many array items will
    /// be sent to subscribers, and if a longer value is assigned then this
    /// minimum length will be increased. If None, then only the current
    /// array length items will be sent.
    minimum_length: Option<usize>,
    /// The last time this value was written
    timestamp: SystemTime,
    /// Channel to send updates to any interested listeners
    sender: broadcast::Sender<Dbr>,
    /// Trigger channel, to notify the server there is a new broadcast available
    triggers: HashMap<u64, mpsc::Sender<String>>,
    /// The EPICS record type, for CLASS_NAME responses
    epics_record_type: Option<String>,
}

impl PV {
    pub fn load(&self) -> DbrValue {
        let value = self.value.lock().unwrap();
        value.clone()
    }

    /// Load the value to a Dbr ready to send to an CA client
    ///
    /// This includes adjustments for minimum size, and encoding (e.g.
    /// sending a string as a Char array instead of restricting to 40-chars)
    pub fn load_for_ca(&self, requested_type: Option<DbrType>) -> Dbr {
        let mut value = self.value.lock().unwrap().clone();
        if requested_type == Some(DBR_CLASS_NAME) {
            return Dbr::ClassName(DbrValue::String(vec![
                self.epics_record_type
                    .clone()
                    .unwrap_or_else(|| value.get_default_record_type()),
            ]));
        }
        // Handle minimum length
        if let Some(size) = self.minimum_length
            && value.get_count() < size
        {
            let _ = value.resize(size);
        }
        Dbr::Time {
            status: Status::default(),
            timestamp: self.timestamp,
            value,
        }
    }
    /// Store a value from the CA protocol to the PV
    ///
    /// In this case, there are special behaviour like e.g. parsing
    /// numbers out of string data type
    fn store_from_ca(&mut self, value: &DbrValue) -> Result<(), ErrorCondition> {
        let native_type = self.value.lock().unwrap().get_type();
        let value = if value.get_type() == DbrBasicType::String {
            value
                .parse_into(native_type)
                .map_err(|_| ErrorCondition::NoConvert)?
        } else {
            value.clone()
        };
        self.store(&value)
    }

    pub fn store(&mut self, value: &DbrValue) -> Result<(), ErrorCondition> {
        // Now update the shared value
        {
            let stored_value = &mut *self.value.lock().unwrap();
            *stored_value = value.convert_to(stored_value.get_type())?;
            // Update the minimum length, if we are now longer
            if let Some(size) = self.minimum_length
                && stored_value.get_count() > size
            {
                self.minimum_length = Some(stored_value.get_count());
            }
            // Ensure lock is dropped
        }
        self.timestamp = SystemTime::now();
        // Now send off the new value to any listeners
        let _ = self.sender.send(self.load_for_ca(None));
        // Send the "please look at" triggers, filtering out any that are dead
        self.triggers = self
            .triggers
            .iter() // TODO: Should this be into_iter?
            .filter_map(|(k, t)| match t.try_send(self.name.clone()) {
                Ok(_) => Some((*k, t.clone())),
                Err(TrySendError::Full(_)) => Some((*k, t.clone())),
                Err(TrySendError::Closed(_)) => None,
            })
            .collect();
        Ok(())
    }
}

impl Default for PV {
    fn default() -> Self {
        PV {
            name: String::new(),
            value: Arc::new(Mutex::new(DbrValue::Int(vec![0]))),
            minimum_length: None,
            timestamp: SystemTime::now(),
            sender: broadcast::Sender::new(16),
            triggers: Default::default(),
            epics_record_type: None,
        }
    }
}

/// Typed interface to reading single values to/from a PV
#[derive(Clone, Debug)]
pub struct Intercom<T>
where
    T: TryFrom<DbrValue>,
    DbrValue: From<T>,
{
    pv: Arc<Mutex<PV>>,
    _marker: PhantomData<T>,
}

impl<T> Intercom<T>
where
    T: TryFrom<DbrValue>,
    DbrValue: From<T>,
{
    fn new(pv: Arc<Mutex<PV>>) -> Self {
        if cfg!(debug_assertions) {
            // Ensure that this pv can be converted into our static type..
            // the library user should not be able to do this, so this
            // indicates an error in our logic
            let Ok(_) = TryInto::<T>::try_into(pv.lock().unwrap().load()) else {
                panic!("Failed to convert PV to static type");
            };
        }
        Self {
            pv,
            _marker: PhantomData,
        }
    }

    pub fn load(&self) -> T {
        let value = self.pv.lock().unwrap().load();
        match value.try_into() {
            Ok(v) => v,
            _ => panic!("Provider logic should ensure this conversion never fails!"),
        }
    }

    pub fn store(&self, value: T) {
        self.pv
            .lock()
            .unwrap()
            .store(&(value).into())
            .expect("Provider logic should ensure this never fails");
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Dbr> {
        self.pv.lock().unwrap().sender.subscribe()
    }
}

#[derive(Debug)]
pub struct PVAlreadyExists;

#[derive(Clone, Default)]
pub struct IntercomProvider {
    pvs: Arc<Mutex<HashMap<String, Arc<Mutex<PV>>>>>,
    /// A Prefix that is inserted in front of any PV name
    pub prefix: String,
    /// Automatically map PV alternative names with a "_RBV" suffix
    pub rbv: bool,
}

impl IntercomProvider {
    pub fn new() -> IntercomProvider {
        IntercomProvider {
            pvs: Arc::new(Mutex::new(HashMap::new())),
            prefix: String::new(),
            rbv: false,
        }
    }

    fn register_pv<T>(&mut self, pv: Arc<Mutex<PV>>) -> Result<Intercom<T>, PVAlreadyExists>
    where
        T: TryFrom<DbrValue>,
        DbrValue: From<T>,
    {
        let name = pv.lock().unwrap().name.clone();
        let mut pvmap = self.pvs.lock().unwrap();
        if pvmap.contains_key(&name) {
            return Err(PVAlreadyExists);
        }
        let _ = pvmap.insert(name, pv.clone());
        Ok(Intercom::<T>::new(pv))
    }

    pub fn add_pv<T>(
        &mut self,
        name: &str,
        initial_value: T,
    ) -> Result<Intercom<T>, PVAlreadyExists>
    where
        T: TryFrom<DbrValue> + Clone + Default,
        DbrValue: From<T>,
    {
        let pv = Arc::new(Mutex::new(PV {
            name: name.to_owned(),
            value: Arc::new(Mutex::new(DbrValue::from(initial_value))),
            ..Default::default()
        }));
        self.register_pv(pv.clone())?;
        Ok(Intercom::<T>::new(pv))
    }

    /// Normalize a PV name by stripping prefix/suffix
    fn normalize_pv_name<'a>(&self, pv_name: &'a str) -> &'a str {
        let mut name = pv_name;
        if pv_name.starts_with(&self.prefix) {
            name = &name[self.prefix.len()..];
        }
        if self.rbv && name.ends_with("_RBV") {
            name = &name[..name.len() - 4]
        }
        name
    }
}

impl Provider for IntercomProvider {
    fn provides(&self, pv_name: &str) -> bool {
        if !pv_name.starts_with(&self.prefix) {
            return false;
        }
        self.pvs
            .lock()
            .unwrap()
            .contains_key(self.normalize_pv_name(pv_name))
    }

    fn read_value(
        &self,
        pv_name: &str,
        requested_type: Option<DbrType>,
    ) -> Result<Dbr, ErrorCondition> {
        let pv = {
            let pvmap = self.pvs.lock().unwrap();
            pvmap
                .get(self.normalize_pv_name(pv_name))
                .ok_or(ErrorCondition::UnavailInServ)?
                .clone()
        };
        let pv = pv.lock().unwrap();
        Ok(pv.load_for_ca(requested_type))
    }

    fn get_access_right(
        &self,
        pv_name: &str,
        _client_user_name: Option<&str>,
        _client_host_name: Option<&str>,
    ) -> messages::Access {
        if self.rbv && pv_name.ends_with("_RBV") {
            messages::Access::Read
        } else {
            messages::Access::ReadWrite
        }
    }

    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        // Don't allow writing of the implicit RBV
        if self.rbv && pv_name.ends_with("_RBV") {
            return Err(ErrorCondition::NoWtAccess);
        }
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(self.normalize_pv_name(pv_name))
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        debug!("Provider: Processing write: {value:?}");
        if let Err(e) = pv.store_from_ca(value.value()) {
            error!("    Error: {e:?}");
            Err(e)
        } else {
            Ok(())
        }
    }

    fn monitor_value(
        &mut self,
        pv_name: &str,
        unique_subscriber_id: u64,
        _data_type: DbrType,
        _data_count: usize,
        _mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Dbr>, ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(self.normalize_pv_name(pv_name))
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        pv.triggers.insert(unique_subscriber_id, trigger);
        Ok(pv.sender.subscribe())
    }

    fn cancel_monitor_value(
        &mut self,
        pv_name: &str,
        unique_subscriber_id: u64,
        _data_type: DbrType,
        _data_count: usize,
    ) {
        let mut pvmap = self.pvs.lock().unwrap();
        let Some(mut pv) = pvmap
            .get_mut(self.normalize_pv_name(pv_name))
            .and_then(|f| f.lock().ok())
        else {
            debug!("Got remove subscription for nonexistent subsription!");
            return;
        };
        pv.triggers.remove(&unique_subscriber_id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        dbr::DbrBasicType,
        providers::intercom::{Intercom, PV},
    };

    #[test]
    fn test_string_intercom() {
        let pv = Arc::new(Mutex::new(PV {
            name: "TEST".to_owned(),
            value: Arc::new(Mutex::new("Test String".to_string().into())),
            ..Default::default()
        }));
        let si = Intercom::<String>::new(pv.clone());
        // let si = StringIntercom::new(pv.clone());
        assert_eq!(si.load(), "Test String");
        assert_eq!(
            pv.lock().unwrap().load_for_ca(None).data_type().basic_type,
            DbrBasicType::Char
        );
    }
}
