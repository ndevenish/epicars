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
    dbr::{DBR_CLASS_NAME, Dbr, DbrBasicType, DbrType, DbrValue, DefaultEpicsClass, Status},
    messages::{self, ErrorCondition, MonitorMask},
    providers::SubscriberId,
    value::{
        Value,
        meta::{Meta, TimeStamp},
    },
};

/// The stored value in CA's representation.
///
/// Infallible for anything held by a [`PV`]: values only ever enter through a
/// [`DbrValue`] - `add_pv` and [`PVBuilder`] both require `DbrValue: From<T>`, and
/// [`PV::store`] coerces every write to the type already stored - so the neutral value is
/// always inside the CA subset. Storage is nonetheless a [`Value`], so that a pvAccess
/// server can be handed the value without a round trip through CA's type system; the
/// neutral *write* path that could break this invariant arrives with the trait rework in
/// Phase 3.
fn value_as_dbr(value: &Value) -> DbrValue {
    DbrValue::try_from(value).expect("Provider logic should ensure this conversion never fails")
}

pub struct PVBuilder<'a, T>
where
    T: TryFrom<DbrValue> + Clone,
    DbrValue: From<T>,
{
    name: String,
    value: T,
    read_only: bool,
    class_name: Option<String>,
    automatic_rbv: bool,
    minimum_length: Option<usize>,
    provider: &'a mut IntercomProvider,
}

impl<'a, T> PVBuilder<'a, T>
where
    T: TryFrom<DbrValue> + Clone + DefaultEpicsClass,
    DbrValue: From<T>,
{
    pub fn new(provider: &'a mut IntercomProvider, name: &str, initial_value: T) -> Self {
        PVBuilder::<T> {
            name: name.to_string(),
            value: initial_value,
            read_only: false,
            class_name: None,
            automatic_rbv: false,
            minimum_length: None,
            provider,
        }
    }
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }
    /// Set the EPICS class name
    pub fn class_name(mut self, name: &str) -> Self {
        self.class_name = Some(name.to_string());
        self
    }
    /// If a read-only RBV-suffix value should automatically be created for this
    pub fn rbv(mut self, auto_rb: bool) -> Self {
        self.automatic_rbv = auto_rb;
        self
    }
    pub fn minimum_length(mut self, length: usize) -> Self {
        self.minimum_length = Some(length);
        self
    }
    /// Instantiate the PV, and return an Intercom to talk to it
    pub fn build(self) -> Result<Intercom<T>, PVAlreadyExists> {
        // Via DbrValue deliberately: it is the CA mapping that decides a lone String is a
        // Char array rather than a DBR_STRING, and that has to keep holding
        let value = Arc::new(Mutex::new(Value::from(DbrValue::from(self.value))));
        let classname = self
            .class_name
            .or(T::get_default_record_type().map(|v| v.to_string()));

        // If we requested an automatic RBV, make an entry for that
        if self.automatic_rbv {
            let pv_rbv = PV {
                name: format!("{}_RBV", self.name),
                value: value.clone(),
                minimum_length: self.minimum_length,
                epics_record_type: classname.clone(),
                read_only: true,
                ..Default::default()
            };
            self.provider.register_pv(Arc::new(Mutex::new(pv_rbv)))?;
        }
        let pv = PV {
            name: self.name,
            value,
            minimum_length: self.minimum_length,
            epics_record_type: classname,
            read_only: self.read_only,
            ..Default::default()
        };
        self.provider.register_pv(Arc::new(Mutex::new(pv)))
    }
}

#[derive(Clone, Debug)]
struct PV {
    name: String,
    /// The value, in the protocol-neutral representation.
    ///
    /// Neutral rather than [`DbrValue`] so that neither protocol's server has to go
    /// through the other's type system to read it - see [`value_as_dbr`] for the
    /// invariant this currently holds to.
    value: Arc<Mutex<Value>>,
    /// Minimum array length. If set, at least this many array items will
    /// be sent to subscribers, and if a longer value is assigned then this
    /// minimum length will be increased. If None, then only the current
    /// array length items will be sent.
    minimum_length: Option<usize>,
    /// The last time this value was written
    timestamp: SystemTime,
    /// Channel to send updates to any interested listeners
    ///
    /// Protocol-neutral: the CA server projects this onto a [`Dbr`] at the wire boundary
    /// and a pvAccess server will project the same payload onto an NTScalar.
    sender: broadcast::Sender<(Value, Meta)>,
    /// Trigger channel, to notify the server there is a new broadcast available
    ///
    /// Keyed by an ID this provider allocated, so that two servers sharing this PV cannot
    /// evict each other - see [`SubscriberId`].
    triggers: HashMap<SubscriberId, mpsc::Sender<String>>,
    /// The EPICS record type, for CLASS_NAME responses
    epics_record_type: Option<String>,
    /// Whether this PV can be written via EPICS
    read_only: bool,
}

impl PV {
    pub fn load(&self) -> DbrValue {
        value_as_dbr(&self.value.lock().unwrap())
    }

    /// The stored value, padded out to `minimum_length`, in CA's representation
    ///
    /// The padding is applied here rather than at the wire boundary because it is a
    /// provider policy - "always send subscribers at least this many elements" - so both
    /// protocols should see it.
    fn load_padded(&self) -> DbrValue {
        let mut value = value_as_dbr(&self.value.lock().unwrap());
        if let Some(size) = self.minimum_length
            && value.get_count() < size
        {
            let _ = value.resize(size);
        }
        value
    }

    /// Load the value and its metadata in protocol-neutral form
    ///
    /// This is what subscribers receive; each protocol's server projects it onto its own
    /// wire representation. `load_for_ca` is the CA projection of exactly this.
    pub fn load_neutral(&self) -> (Value, Meta) {
        (
            Value::from(self.load_padded()),
            Meta::timestamped(TimeStamp::from(self.timestamp)),
        )
    }

    /// Load the value to a Dbr ready to send to an CA client
    ///
    /// This includes adjustments for minimum size, and encoding (e.g.
    /// sending a string as a Char array instead of restricting to 40-chars)
    pub fn load_for_ca(&self, requested_type: Option<DbrType>) -> Dbr {
        if requested_type == Some(DBR_CLASS_NAME) {
            return Dbr::ClassName(DbrValue::String(vec![
                self.epics_record_type.clone().unwrap_or_else(|| {
                    value_as_dbr(&self.value.lock().unwrap()).get_default_record_type()
                }),
            ]));
        }
        Dbr::Time {
            status: Status::default(),
            timestamp: self.timestamp,
            value: self.load_padded(),
        }
    }
    /// Store a value from the CA protocol to the PV
    ///
    /// In this case, there are special behaviour like e.g. parsing
    /// numbers out of string data type
    fn store_from_ca(&mut self, value: &DbrValue) -> Result<(), ErrorCondition> {
        let native_type = self
            .value
            .lock()
            .unwrap()
            .ca_basic_type()
            .map_err(|_| ErrorCondition::NoConvert)?;
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
            let native_type = stored_value
                .ca_basic_type()
                .map_err(|_| ErrorCondition::NoConvert)?;
            let converted = value.convert_to(native_type)?;
            // Update the minimum length, if we are now longer
            if let Some(size) = self.minimum_length
                && converted.get_count() > size
            {
                self.minimum_length = Some(converted.get_count());
            }
            *stored_value = Value::from(converted);
            // Ensure lock is dropped
        }
        self.timestamp = SystemTime::now();
        // Now send off the new value to any listeners
        let _ = self.sender.send(self.load_neutral());
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
            value: Arc::new(Mutex::new(Value::from(DbrValue::Int(vec![0])))),
            minimum_length: None,
            timestamp: SystemTime::now(),
            sender: broadcast::Sender::new(256),
            triggers: Default::default(),
            epics_record_type: None,
            read_only: false,
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

    pub fn subscribe(&self) -> ConverterReceiver<T> {
        ConverterReceiver {
            receiver: self.pv.lock().unwrap().sender.subscribe(),
            _phantom: PhantomData,
        }
    }
}

/// Wrap a value broadcast receiver into a receiver that converts to a specific type
#[derive(Debug)]
pub struct ConverterReceiver<T>
where
    T: TryFrom<DbrValue>,
    DbrValue: From<T>,
{
    receiver: broadcast::Receiver<(Value, Meta)>,
    _phantom: PhantomData<T>,
}

pub enum ConverterRecvError {
    /// There are no more active senders implying no further messages will ever
    /// be sent.
    Closed,

    /// The receiver lagged too far behind. Attempting to receive again will
    /// return the oldest message still retained by the channel.
    ///
    /// Includes the number of skipped messages.
    Lagged(u64),
    ConversionError,
}
pub enum ConverterTryRecvError {
    /// The channel is currently empty. There are still active Sender
    /// handles, so data may yet become available.
    Empty,
    /// There are no more active senders implying no further messages
    /// will ever be sent.
    Closed,

    /// The receiver lagged too far behind. Attempting to receive again
    /// will return the oldest message still retained by the channel.
    ///
    /// Includes the number of skipped messages.
    Lagged(u64),
    ConversionError,
}
impl From<broadcast::error::RecvError> for ConverterRecvError {
    fn from(value: broadcast::error::RecvError) -> Self {
        match value {
            broadcast::error::RecvError::Closed => Self::Closed,
            broadcast::error::RecvError::Lagged(n) => Self::Lagged(n),
        }
    }
}
impl From<broadcast::error::TryRecvError> for ConverterTryRecvError {
    fn from(value: broadcast::error::TryRecvError) -> Self {
        match value {
            broadcast::error::TryRecvError::Closed => Self::Closed,
            broadcast::error::TryRecvError::Lagged(n) => Self::Lagged(n),
            broadcast::error::TryRecvError::Empty => Self::Empty,
        }
    }
}
/// A broadcast update, in a receiver's static type
///
/// Goes via [`DbrValue`] because that is where the numeric coercion lives - `T`'s bound
/// is `TryFrom<DbrValue>`, and generalising that to the neutral model is plan item 3.5.
fn convert_update<T>(value: &Value) -> Option<T>
where
    T: TryFrom<DbrValue>,
{
    T::try_from(DbrValue::try_from(value).ok()?).ok()
}

impl<T> ConverterReceiver<T>
where
    T: TryFrom<DbrValue>,
    DbrValue: From<T>,
{
    pub async fn recv(&mut self) -> Result<T, ConverterRecvError> {
        self.receiver
            .recv()
            .await
            .map_err(|e| e.into())
            .and_then(|(value, _meta)| {
                convert_update(&value).ok_or(ConverterRecvError::ConversionError)
            })
    }
    pub fn try_recv(&mut self) -> Result<T, ConverterTryRecvError> {
        self.receiver
            .try_recv()
            .map_err(|e| e.into())
            .and_then(|(value, _meta)| {
                convert_update(&value).ok_or(ConverterTryRecvError::ConversionError)
            })
    }
    pub fn resubscribe(&self) -> Self {
        Self {
            receiver: self.receiver.resubscribe(),
            _phantom: PhantomData,
        }
    }
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }
    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
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

    /// Add a PV with default configuration
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
            value: Arc::new(Mutex::new(Value::from(DbrValue::from(initial_value)))),
            ..Default::default()
        }));
        self.register_pv(pv.clone())?;
        Ok(Intercom::<T>::new(pv))
    }

    /// Create a [PVBuilder] for customisation
    pub fn build_pv<T>(&mut self, name: &str, initial_value: T) -> PVBuilder<'_, T>
    where
        T: TryFrom<DbrValue> + Clone + Default,
        DbrValue: From<T>,
    {
        PVBuilder {
            name: name.to_string(),
            value: initial_value,
            read_only: false,
            class_name: None,
            automatic_rbv: false,
            minimum_length: None,
            provider: self,
        }
    }
    /// Normalize a PV name by stripping prefix/suffix
    fn normalize_pv_name<'a>(&self, pv_name: &'a str) -> &'a str {
        let mut name = pv_name;
        if pv_name.starts_with(&self.prefix) {
            name = &name[self.prefix.len()..];
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
        let pvs = self.pvs.lock().unwrap();
        let Some(pv) = pvs.get(self.normalize_pv_name(pv_name)) else {
            return messages::Access::None;
        };
        if pv.lock().unwrap().read_only {
            messages::Access::Read
        } else {
            messages::Access::ReadWrite
        }
    }

    fn write_value(&mut self, pv_name: &str, value: Dbr) -> Result<(), ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(self.normalize_pv_name(pv_name))
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        if pv.read_only {
            return Err(ErrorCondition::NoWtAccess);
        }
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
        _data_type: DbrType,
        _data_count: usize,
        _mask: MonitorMask,
        trigger: mpsc::Sender<String>,
    ) -> Result<(SubscriberId, broadcast::Receiver<(Value, Meta)>), ErrorCondition> {
        let mut pvmap = self.pvs.lock().unwrap();
        let mut pv = pvmap
            .get_mut(self.normalize_pv_name(pv_name))
            .ok_or(ErrorCondition::UnavailInServ)?
            .lock()
            .unwrap();
        let subscriber = SubscriberId::new();
        pv.triggers.insert(subscriber, trigger);
        Ok((subscriber, pv.sender.subscribe()))
    }

    fn cancel_monitor_value(
        &mut self,
        pv_name: &str,
        subscriber: SubscriberId,
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
        pv.triggers.remove(&subscriber);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc;

    use super::{PV, value_as_dbr};
    use crate::{
        Provider,
        dbr::{DBR_BASIC_INT, DbrBasicType, DbrValue},
        messages::MonitorMask,
        providers::{IntercomProvider, SubscriberId, intercom::Intercom},
        value::{
            Value,
            meta::{Alarm, Meta},
        },
    };

    /// Register a monitor the way the server does, and hand back both ends.
    fn monitor(
        provider: &mut IntercomProvider,
        pv_name: &str,
        trigger_depth: usize,
    ) -> (
        SubscriberId,
        mpsc::Receiver<String>,
        tokio::sync::broadcast::Receiver<(Value, Meta)>,
    ) {
        let (trigger, triggers) = mpsc::channel(trigger_depth);
        let (subscriber, updates) = provider
            .monitor_value(pv_name, DBR_BASIC_INT, 1, MonitorMask::default(), trigger)
            .unwrap();
        (subscriber, triggers, updates)
    }

    /// How many triggers the provider still holds for a PV.
    fn trigger_count(provider: &IntercomProvider, pv_name: &str) -> usize {
        provider.pvs.lock().unwrap()[pv_name]
            .lock()
            .unwrap()
            .triggers
            .len()
    }

    #[test]
    fn test_string_intercom() {
        let pv = Arc::new(Mutex::new(PV {
            name: "TEST".to_owned(),
            value: Arc::new(Mutex::new(Value::from(DbrValue::from(
                "Test String".to_string(),
            )))),
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

    /// Load-bearing property 1: `store()` works from ordinary synchronous code, which is
    /// the entire point of `Intercom<T>`, and keeps working while the server runs on
    /// tokio.
    #[test]
    fn store_is_callable_from_non_async_code() {
        let mut provider = IntercomProvider::new();
        let value = provider.add_pv("COUNT", 0i32).unwrap();

        // With no tokio runtime anywhere in sight
        value.store(7);
        assert_eq!(value.load(), 7);

        // ... and with an async subscriber on a runtime this thread never enters
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let mut updates = value.subscribe();
        let received = runtime.spawn(async move { updates.recv().await.ok() });
        value.store(8);
        assert_eq!(runtime.block_on(received).unwrap(), Some(8));

        // ... and from a plain OS thread, which has no runtime context at all
        let elsewhere = value.clone();
        std::thread::spawn(move || elsewhere.store(9))
            .join()
            .unwrap();
        assert_eq!(value.load(), 9);
    }

    /// Load-bearing property 2: a full trigger queue is success, not an error.
    ///
    /// The trigger only says "look at the broadcast channel", so a trigger that could not
    /// be enqueued has lost nothing - the value is still in the broadcast buffer and the
    /// subscriber will pick it up when it drains. `store()` must not become an `await` to
    /// "fix" this, because property 1 forbids it.
    #[test]
    fn store_tolerates_a_full_trigger_queue() {
        let mut provider = IntercomProvider::new();
        let value = provider.add_pv("COUNT", 0i32).unwrap();
        // Depth 1, so only the first of these three stores can enqueue a trigger
        let (_subscriber, mut triggers, mut updates) = monitor(&mut provider, "COUNT", 1);

        value.store(1);
        value.store(2);
        value.store(3);

        assert_eq!(triggers.try_recv().unwrap(), "COUNT");
        assert!(
            triggers.try_recv().is_err(),
            "the queue was full, so no further triggers got in"
        );
        assert_eq!(
            trigger_count(&provider, "COUNT"),
            1,
            "a full queue must not evict the trigger"
        );
        // Nothing was lost: all three values are queued for the subscriber
        assert_eq!(updates.len(), 3);
        for expected in [1i32, 2, 3] {
            let (value, _) = updates.try_recv().unwrap();
            assert_eq!(value, Value::from(vec![expected]));
        }
    }

    /// Subscription IDs come from the provider, so two subscriptions to one PV never
    /// collide - including two servers doing it through their own clone of the provider,
    /// which is what used to break.
    #[test]
    fn subscriber_ids_are_unique_and_cancel_independently() {
        assert_ne!(SubscriberId::new(), SubscriberId::new());

        let mut provider = IntercomProvider::new();
        let value = provider.add_pv("COUNT", 0i32).unwrap();
        // A second handle on the same state, as each server's circuit holds
        let mut shared = provider.clone();

        let (first, mut first_triggers, _first_updates) = monitor(&mut provider, "COUNT", 4);
        let (second, mut second_triggers, _second_updates) = monitor(&mut shared, "COUNT", 4);
        assert_ne!(first, second);
        assert_eq!(trigger_count(&provider, "COUNT"), 2);

        value.store(1);
        assert_eq!(first_triggers.try_recv().unwrap(), "COUNT");
        assert_eq!(second_triggers.try_recv().unwrap(), "COUNT");

        // Cancelling one leaves the other running
        provider.cancel_monitor_value("COUNT", first, DBR_BASIC_INT, 1);
        assert_eq!(trigger_count(&provider, "COUNT"), 1);
        value.store(2);
        assert!(first_triggers.try_recv().is_err());
        assert_eq!(second_triggers.try_recv().unwrap(), "COUNT");

        shared.cancel_monitor_value("COUNT", second, DBR_BASIC_INT, 1);
        assert_eq!(trigger_count(&provider, "COUNT"), 0);
    }

    /// A *closed* trigger, unlike a full one, is dropped - that subscriber is gone.
    #[test]
    fn store_drops_closed_triggers() {
        let mut provider = IntercomProvider::new();
        let value = provider.add_pv("COUNT", 0i32).unwrap();
        let (_subscriber, triggers, _updates) = monitor(&mut provider, "COUNT", 4);
        assert_eq!(trigger_count(&provider, "COUNT"), 1);

        drop(triggers);
        value.store(1);
        assert_eq!(trigger_count(&provider, "COUNT"), 0);
    }

    /// The storage move is meant to be invisible from CA: the same values, types and
    /// padding as before.
    #[test]
    fn neutral_storage_keeps_the_ca_projection() {
        let mut provider = IntercomProvider::new();
        // A lone String is a Char array over CA rather than a DBR_STRING, and storage
        // being neutral must not quietly turn it into a DBR_STRING
        let text = provider
            .build_pv("TEXT", "abc".to_string())
            .minimum_length(8)
            .build()
            .unwrap();
        let dbr = provider.read_value("TEXT", None).unwrap();
        assert_eq!(dbr.data_type().basic_type, DbrBasicType::Char);
        assert_eq!(dbr.value().get_count(), 8, "padded to minimum_length");

        // A longer write ratchets the minimum up
        text.store("a longer string".to_string());
        assert_eq!(
            provider
                .read_value("TEXT", None)
                .unwrap()
                .value()
                .get_count(),
            15
        );

        // An enum-typed value still reads back as DBR_ENUM - the asymmetric u16 mapping
        // in `value::ca` exists for exactly this case
        let pv = PV {
            name: "MODE".to_owned(),
            value: Arc::new(Mutex::new(Value::from(DbrValue::Enum(2)))),
            ..Default::default()
        };
        assert_eq!(pv.load(), DbrValue::Enum(2));
        assert_eq!(
            pv.load_for_ca(None).data_type().basic_type,
            DbrBasicType::Enum
        );
        assert_eq!(
            value_as_dbr(&Value::from(DbrValue::Enum(2))),
            DbrValue::Enum(2)
        );
    }

    /// What subscribers now receive: a neutral value, already padded, with the metadata
    /// the CA `Time` category used to carry.
    #[test]
    fn subscribers_receive_neutral_padded_values() {
        let mut provider = IntercomProvider::new();
        let array = provider
            .build_pv("ARRAY", vec![1i32, 2])
            .minimum_length(4)
            .build()
            .unwrap();
        let (_subscriber, _triggers, mut updates) = monitor(&mut provider, "ARRAY", 4);

        array.store(vec![7i32, 8]);
        let (value, meta) = updates.try_recv().unwrap();
        assert_eq!(
            value,
            Value::from(vec![7i32, 8, 0, 0]),
            "minimum_length is a provider policy, so it applies to the neutral payload"
        );
        assert_eq!(meta.alarm, Some(Alarm::none()));
        assert!(meta.timestamp.is_some());
        assert!(meta.display.is_none() && meta.control.is_none());

        // ... and it agrees with what the CA read path reports
        let dbr = provider.read_value("ARRAY", None).unwrap();
        assert_eq!(Value::from(dbr.value()), value);
    }

    #[test]
    fn test_automatic_rbv() {
        let mut p = IntercomProvider::new();
        let ic = p.build_pv("COUNT", 32i8).rbv(true).build().unwrap();
        assert!(p.provides("COUNT_RBV"));
        assert!(!ic.pv.lock().unwrap().read_only);
    }

    #[test]
    fn test_bool_intercom() {
        let mut p = IntercomProvider::new();
        let ic = p.add_pv("FLAG", false).unwrap();
        ic.store(false);
        assert_eq!(ic.load(), false);
    }
}
