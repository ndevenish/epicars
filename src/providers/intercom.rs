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
use tracing::{error, info};

use crate::{
    Provider,
    dbr::{Dbr, DbrBasicType, DbrType, DbrValue, IntoDbrBasicType, Status},
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
    /// The optional type to serialize as. This is useful for forcing an
    /// otherwise String DbrValue to be DbrValue::Char when sending off.
    force_dbr_type: Option<DbrBasicType>,
    /// The last time this value was written
    timestamp: SystemTime,
    /// Channel to send updates to EPIC clients
    sender: broadcast::Sender<Dbr>,
    /// Trigger channel, to notify the server there is a new broadcast available
    triggers: Vec<mpsc::Sender<String>>,
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
    pub fn load_for_ca(&self) -> Dbr {
        let mut value = self.value.lock().unwrap().clone();
        if let Some(to_type) = self.force_dbr_type
            && value.get_type() != to_type
        {
            value = value
                .convert_to(to_type)
                .expect("PV Logic should ensure stored value can always be converted")
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
        let _ = self.sender.send(self.load_for_ca());
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

impl Default for PV {
    fn default() -> Self {
        PV {
            name: String::new(),
            value: Arc::new(Mutex::new(DbrValue::Int(vec![0]))),
            minimum_length: None,
            force_dbr_type: None,
            timestamp: SystemTime::now(),
            sender: broadcast::Sender::new(16),
            triggers: Vec::new(),
        }
    }
}

/// Typed interface to reading single values to/from a PV
#[derive(Clone)]
pub struct Intercom<T>
where
    T: IntoDbrBasicType,
{
    pv: Arc<Mutex<PV>>,
    _marker: PhantomData<T>,
}

impl<T> Intercom<T>
where
    T: IntoDbrBasicType + Clone + Default,
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

#[derive(Clone)]
pub struct VecIntercom<T>
where
    T: IntoDbrBasicType,
{
    pv: Arc<Mutex<PV>>,
    _marker: PhantomData<T>,
}

impl<T> VecIntercom<T>
where
    T: IntoDbrBasicType + Clone + Default,
    for<'a> Vec<T>: TryFrom<&'a DbrValue>,
    DbrValue: From<Vec<T>>,
{
    fn new(pv: Arc<Mutex<PV>>) -> Self {
        Self {
            pv,
            _marker: PhantomData,
        }
    }

    pub fn load(&self) -> Vec<T> {
        match (&self.pv.lock().unwrap().load()).try_into() {
            Ok(v) => v,
            _ => panic!("Provider logic should ensure this conversion never fails!"),
        }
    }

    pub fn store(&mut self, value: &[T]) {
        self.pv
            .lock()
            .unwrap()
            .store(&value.to_vec().into())
            .expect("Provider logic should ensure this never fails");
    }
}

#[derive(Debug)]
pub struct StringIntercom {
    pv: Arc<Mutex<PV>>,
}

impl StringIntercom {
    fn new(pv: Arc<Mutex<PV>>) -> Self {
        assert!(pv.lock().unwrap().value.lock().unwrap().get_type() == DbrBasicType::String);
        Self { pv }
    }
    pub fn load(&self) -> String {
        let DbrValue::String(value) = self.pv.lock().unwrap().load() else {
            panic!("StringIntercom PV is not of string type!");
        };
        match value.as_slice() {
            [] => String::new(),
            [value] => value.clone(),
            _ => panic!("Got multi-value string DbrValue in StringIntercom!"),
        }
    }
    pub fn store(&mut self, value: &str) {
        self.pv
            .lock()
            .unwrap()
            .store(&vec![value.to_owned()].into())
            .expect("Provider logic should ensure this never fails");
    }
}

#[derive(Debug)]
pub struct PVAlreadyExists;

#[derive(Clone, Default)]
pub struct IntercomProvider {
    pvs: Arc<Mutex<HashMap<String, Arc<Mutex<PV>>>>>,
    /// A Prefix that is inserted in front of any PV name
    pub prefix: String,
}

impl IntercomProvider {
    pub fn new() -> IntercomProvider {
        IntercomProvider {
            pvs: Arc::new(Mutex::new(HashMap::new())),
            prefix: String::new(),
        }
    }

    fn register_pv(&mut self, pv: Arc<Mutex<PV>>) -> Result<(), PVAlreadyExists> {
        let name = &pv.lock().unwrap().name;
        let mut pvmap = self.pvs.lock().unwrap();
        if pvmap.contains_key(name) {
            return Err(PVAlreadyExists);
        }
        let _ = pvmap.insert(name.to_owned(), pv.clone());
        Ok(())
    }

    pub fn add_pv<T>(
        &mut self,
        name: &str,
        initial_value: T,
    ) -> Result<Intercom<T>, PVAlreadyExists>
    where
        T: IntoDbrBasicType + Clone + Default,
        for<'a> Vec<T>: TryFrom<&'a DbrValue>,
        DbrValue: From<Vec<T>>,
    {
        let pv = Arc::new(Mutex::new(PV {
            name: name.to_owned(),
            value: Arc::new(Mutex::new(DbrValue::from(vec![initial_value.clone()]))),
            ..Default::default()
        }));
        self.register_pv(pv.clone())?;
        Ok(Intercom::<T>::new(pv))
    }

    pub fn add_vec_pv<T>(
        &mut self,
        name: &str,
        initial_value: Vec<T>,
        minimum_length: Option<usize>,
    ) -> Result<VecIntercom<T>, PVAlreadyExists>
    where
        T: IntoDbrBasicType + Clone + Default,
        for<'a> Vec<T>: TryFrom<&'a DbrValue>,
        DbrValue: From<Vec<T>>,
    {
        // let pv = self.create_pv(name, DbrValue::from(initial_value.clone()), minimum_length)?;
        let pv = Arc::new(Mutex::new(PV {
            name: name.to_owned(),
            value: Arc::new(Mutex::new(DbrValue::from(initial_value.clone()))),
            minimum_length,
            ..Default::default()
        }));
        self.register_pv(pv.clone())?;
        Ok(VecIntercom::<T>::new(pv))
    }

    pub fn add_string_pv(
        &mut self,
        name: &str,
        initial_value: &str,
        minimum_u8_len: Option<usize>,
    ) -> Result<StringIntercom, PVAlreadyExists> {
        let pv = Arc::new(Mutex::new(PV {
            name: name.to_owned(),
            minimum_length: minimum_u8_len,
            force_dbr_type: Some(DbrBasicType::Char),
            value: Arc::new(Mutex::new(DbrValue::String(vec![initial_value.to_owned()]))),
            ..Default::default()
        }));
        self.register_pv(pv.clone())?;
        Ok(StringIntercom::new(pv))
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
            .contains_key(&pv_name[self.prefix.len()..])
    }

    fn read_value(
        &self,
        pv_name: &str,
        _requested_type: Option<DbrType>,
    ) -> Result<Dbr, ErrorCondition> {
        let pv = {
            let pvmap = self.pvs.lock().unwrap();
            pvmap
                .get(&pv_name[self.prefix.len()..])
                .ok_or(ErrorCondition::UnavailInServ)?
                .clone()
        };
        let pv = pv.lock().unwrap();
        Ok(pv.load_for_ca())
    }

    fn get_access_right(
        &self,
        _pv_name: &str,
        _client_user_name: Option<&str>,
        _client_host_name: Option<&str>,
    ) -> messages::Access {
        messages::Access::ReadWrite
    }

    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(&pv_name[self.prefix.len()..])
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        info!("Provider: Processing write: {value:?}");
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
        _data_type: DbrType,
        _data_count: usize,
        _mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<broadcast::Receiver<Dbr>, ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(&pv_name[self.prefix.len()..])
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        pv.triggers.push(trigger);
        Ok(pv.sender.subscribe())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::{
        dbr::DbrBasicType,
        providers::intercom::{PV, StringIntercom},
    };

    #[test]
    fn test_string_intercom() {
        let pv = Arc::new(Mutex::new(PV {
            name: "TEST".to_owned(),
            value: Arc::new(Mutex::new(vec!["Test String".to_owned()].into())),
            force_dbr_type: Some(DbrBasicType::Char),
            ..Default::default()
        }));
        let si = StringIntercom::new(pv.clone());
        assert_eq!(si.load(), "Test String");
        assert_eq!(
            pv.lock().unwrap().load_for_ca().data_type().basic_type,
            DbrBasicType::Char
        );
    }
}
