//! Protocol-neutral metadata: what travels alongside a [`Value`](crate::value::Value).
//!
//! The encouraging finding behind this module is that CA and pvAccess agree almost
//! exactly on *what* metadata is. CA's [`DbrCategory`](crate::dbr::DbrCategory) ladder -
//! Basic, Status, Time, Graphics, Control - is the same set of concepts as NTScalar's
//! optional `alarm`, `timeStamp`, `display` and `control` fields, differing only in that
//! CA makes them a cumulative enumeration and pvAccess makes them independent:
//!
//! | CA category | Neutral | NT field |
//! |---|---|---|
//! | `Status` | [`Alarm`] | `alarm_t` |
//! | `Time` | [`TimeStamp`] | `time_t` |
//! | `Graphics` | [`Display`] | `display_t` |
//! | `Control` | [`Control`] | `control_t` |
//!
//! [`Meta`] is the carrier, with every field optional - which is what lets a CA category
//! and an NT structure both project onto it.
//!
//! # The two protocols do not share an epoch
//!
//! CA timestamps count seconds from the **EPICS epoch, 1990-01-01**. pvAccess's
//! `time_t.secondsPastEpoch` counts from the **POSIX epoch, 1970-01-01**. The offset is
//! [`EPICS_EPOCH_OFFSET`] - 631 152 000 seconds, confirmed against a real IOC, where
//! `pvxget` on an unprocessed record reports `secondsPastEpoch = 631152000`: a zero
//! EPICS timestamp expressed in POSIX terms.
//!
//! ```
//! use epicars::value::meta::{EPICS_EPOCH_OFFSET, TimeStamp};
//!
//! // A zero CA timestamp is 631152000 seconds past the POSIX epoch
//! let zero_ca = TimeStamp::from_ca(0, 0).unwrap();
//! assert_eq!(zero_ca.to_posix(), (EPICS_EPOCH_OFFSET, 0));
//! ```
//!
//! [`TimeStamp`] is therefore **epoch-neutral internally** - it stores a
//! [`SystemTime`], as `PV.timestamp` already does - and converts at each wire boundary
//! rather than on the way in. Getting this wrong is a 20-year offset that every client
//! renders without complaint, so it would not surface as an obvious failure.
//!
//! Two further differences worth noting: pvAccess's `secondsPastEpoch` is a signed
//! **64-bit** value against CA's 32-bit `stamp`, and it carries a third field CA has no
//! analogue for, `userTag`.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Seconds between the POSIX epoch (1970-01-01) and the EPICS epoch (1990-01-01).
///
/// CA timestamps are relative to the latter, pvAccess timestamps to the former. See the
/// module documentation.
pub const EPICS_EPOCH_OFFSET: i64 = 631_152_000;

/// How serious an alarm is.
///
/// These five values, and their order, are shared by CA's `severity` field and NT's
/// `alarm_t.severity`, so unlike [`Alarm::status`] this one concept really is common to
/// both protocols.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AlarmSeverity {
    #[default]
    None = 0,
    Minor = 1,
    Major = 2,
    Invalid = 3,
    Undefined = 4,
}

impl AlarmSeverity {
    /// The name, as clients render it.
    pub fn name(&self) -> &'static str {
        match self {
            AlarmSeverity::None => "NONE",
            AlarmSeverity::Minor => "MINOR",
            AlarmSeverity::Major => "MAJOR",
            AlarmSeverity::Invalid => "INVALID",
            AlarmSeverity::Undefined => "UNDEFINED",
        }
    }

    /// The choices, in index order, for an `enum_t`-style representation.
    pub fn choices() -> [&'static str; 5] {
        ["NONE", "MINOR", "MAJOR", "INVALID", "UNDEFINED"]
    }
}

impl TryFrom<i32> for AlarmSeverity {
    type Error = UnknownAlarmSeverity;

    fn try_from(value: i32) -> Result<AlarmSeverity, UnknownAlarmSeverity> {
        match value {
            0 => Ok(AlarmSeverity::None),
            1 => Ok(AlarmSeverity::Minor),
            2 => Ok(AlarmSeverity::Major),
            3 => Ok(AlarmSeverity::Invalid),
            4 => Ok(AlarmSeverity::Undefined),
            other => Err(UnknownAlarmSeverity(other)),
        }
    }
}

impl From<AlarmSeverity> for i32 {
    fn from(value: AlarmSeverity) -> i32 {
        value as i32
    }
}

/// Returned when an integer severity is outside the five defined values.
#[derive(Debug, thiserror::Error)]
#[error("{0} is not a known alarm severity")]
pub struct UnknownAlarmSeverity(pub i32);

/// Alarm state: CA's `status`/`severity` pair, NT's `alarm_t`.
///
/// The `severity` field means the same thing in both protocols - see [`AlarmSeverity`].
/// **`status` does not.** CA's `status` is an `epicsAlarmCondition`, one of twenty-odd
/// specific causes (`HIHI`, `READ`, `LINK`, ...); NT's `alarm_t.status` is a much coarser
/// classification of *where* the alarm came from (device, driver, record, database,
/// conf, client). They are held in one field here because they occupy the same slot in
/// both wire formats, but a value carried over from one protocol should not be
/// interpreted using the other's table. `message` has no CA analogue at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Alarm {
    pub severity: AlarmSeverity,
    /// See the type documentation: this field's *meaning* is protocol-specific.
    pub status: i32,
    /// Human-readable detail. Always empty when the alarm came from CA, which has no
    /// field for it.
    pub message: String,
}

impl Alarm {
    /// No alarm: severity `None`, status 0, no message.
    pub fn none() -> Alarm {
        Alarm::default()
    }

    /// Whether this represents the absence of an alarm.
    pub fn is_ok(&self) -> bool {
        self.severity == AlarmSeverity::None
    }
}

/// A point in time, plus pvAccess's `userTag`.
///
/// Stored as a [`SystemTime`], deliberately: neither protocol's epoch is privileged, and
/// conversion happens at the wire boundary. See the module documentation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimeStamp {
    pub time: SystemTime,
    /// pvAccess's `time_t.userTag`. CA has no analogue, so this is 0 for anything
    /// arriving over CA and is dropped on the way out.
    pub user_tag: i32,
}

impl TimeStamp {
    /// The current time, with no user tag.
    pub fn now() -> TimeStamp {
        TimeStamp::from(SystemTime::now())
    }

    /// Builder form for [`TimeStamp::user_tag`].
    pub fn with_user_tag(mut self, user_tag: i32) -> TimeStamp {
        self.user_tag = user_tag;
        self
    }

    /// From a CA `stamp`: seconds and nanoseconds past the **EPICS** epoch.
    ///
    /// `None` if the result is not representable as a [`SystemTime`], which on a
    /// platform with a 64-bit `SystemTime` cannot happen for any `i32` input.
    pub fn from_ca(seconds: i32, nanoseconds: u32) -> Option<TimeStamp> {
        TimeStamp::from_posix(i64::from(seconds) + EPICS_EPOCH_OFFSET, nanoseconds)
    }

    /// To a CA `stamp`: seconds and nanoseconds past the **EPICS** epoch.
    ///
    /// CA's seconds field is only 32 bits, so times before 1921 or after 2058 saturate
    /// rather than wrapping. Both bounds are well outside anything a control system will
    /// legitimately timestamp.
    pub fn to_ca(&self) -> (i32, u32) {
        let (seconds, nanoseconds) = self.to_posix();
        let seconds = seconds - EPICS_EPOCH_OFFSET;
        (
            i32::try_from(seconds).unwrap_or(if seconds < 0 { i32::MIN } else { i32::MAX }),
            nanoseconds,
        )
    }

    /// From a pvAccess `time_t`: seconds and nanoseconds past the **POSIX** epoch.
    ///
    /// `None` if the result is not representable as a [`SystemTime`].
    pub fn from_posix(seconds: i64, nanoseconds: u32) -> Option<TimeStamp> {
        let magnitude = Duration::new(seconds.unsigned_abs(), 0);
        let time = if seconds < 0 {
            UNIX_EPOCH.checked_sub(magnitude)?
        } else {
            UNIX_EPOCH.checked_add(magnitude)?
        };
        Some(TimeStamp::from(
            time.checked_add(Duration::from_nanos(nanoseconds.into()))?,
        ))
    }

    /// To a pvAccess `time_t`: seconds and nanoseconds past the **POSIX** epoch.
    ///
    /// `nanoseconds` is always in `0..1_000_000_000`, including for times before 1970 -
    /// so a time half a second before the epoch is `(-1, 500_000_000)`, not
    /// `(0, -500_000_000)`, matching how both protocols encode it.
    pub fn to_posix(&self) -> (i64, u32) {
        match self.time.duration_since(UNIX_EPOCH) {
            Ok(since) => (since.as_secs() as i64, since.subsec_nanos()),
            Err(before) => {
                let before = before.duration();
                let seconds = before.as_secs() as i64;
                match before.subsec_nanos() {
                    0 => (-seconds, 0),
                    nanoseconds => (-seconds - 1, 1_000_000_000 - nanoseconds),
                }
            }
        }
    }
}

impl From<SystemTime> for TimeStamp {
    fn from(time: SystemTime) -> TimeStamp {
        TimeStamp { time, user_tag: 0 }
    }
}

/// How a client should render a value: NT's `display_t`.
///
/// The CA analogue is the `Graphics` category, which carries units, precision and
/// display limits, but has no `description` and no `form`. Note that CA's `GR_*`
/// structures also carry *alarm* and *warning* limits, which NT puts in a separate
/// `valueAlarm_t` that this module does not yet model.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Display {
    pub limit_low: f64,
    pub limit_high: f64,
    pub description: String,
    pub units: String,
    /// Digits after the decimal point. CA's `precision` field is an `i16`.
    pub precision: i32,
    pub form: DisplayForm,
}

impl Display {
    /// Whether the limits describe a real range, rather than being an unset `0.0`/`0.0`.
    pub fn has_limits(&self) -> bool {
        self.limit_low < self.limit_high
    }
}

/// The numeric presentation a client should use: NT's `display.form`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum DisplayForm {
    #[default]
    Default = 0,
    String = 1,
    Binary = 2,
    Decimal = 3,
    Hexadecimal = 4,
    Exponential = 5,
    Engineering = 6,
}

impl DisplayForm {
    pub fn name(&self) -> &'static str {
        DisplayForm::choices()[*self as usize]
    }

    /// The choices, in index order, as NT declares them for the `enum_t`.
    pub fn choices() -> [&'static str; 7] {
        [
            "Default",
            "String",
            "Binary",
            "Decimal",
            "Hexadecimal",
            "Exponential",
            "Engineering",
        ]
    }
}

impl TryFrom<i32> for DisplayForm {
    type Error = UnknownDisplayForm;

    fn try_from(value: i32) -> Result<DisplayForm, UnknownDisplayForm> {
        match value {
            0 => Ok(DisplayForm::Default),
            1 => Ok(DisplayForm::String),
            2 => Ok(DisplayForm::Binary),
            3 => Ok(DisplayForm::Decimal),
            4 => Ok(DisplayForm::Hexadecimal),
            5 => Ok(DisplayForm::Exponential),
            6 => Ok(DisplayForm::Engineering),
            other => Err(UnknownDisplayForm(other)),
        }
    }
}

impl From<DisplayForm> for i32 {
    fn from(value: DisplayForm) -> i32 {
        value as i32
    }
}

/// Returned when an integer form index is outside the seven defined values.
#[derive(Debug, thiserror::Error)]
#[error("{0} is not a known display form")]
pub struct UnknownDisplayForm(pub i32);

/// Bounds a client should enforce when writing: NT's `control_t`.
///
/// The CA analogue is the `Control` category's upper and lower control limits. CA has no
/// `min_step`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Control {
    pub limit_low: f64,
    pub limit_high: f64,
    /// Smallest change a client should send. 0 means unspecified.
    pub min_step: f64,
}

impl Control {
    /// Whether the limits describe a real range, rather than being an unset `0.0`/`0.0`.
    pub fn has_limits(&self) -> bool {
        self.limit_low < self.limit_high
    }
}

/// Everything that travels alongside a value, all of it optional.
///
/// Every field being independently optional is the point: a CA `DBR_STS_LONG` produces a
/// `Meta` with only `alarm`, an NTScalar carrying `alarm` and `timeStamp` but no
/// `display` produces the same shape, and neither has to pretend to be the other.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Meta {
    pub alarm: Option<Alarm>,
    pub timestamp: Option<TimeStamp>,
    pub display: Option<Display>,
    pub control: Option<Control>,
}

impl Meta {
    /// No metadata at all - the CA `Basic` category.
    pub fn new() -> Meta {
        Meta::default()
    }

    /// Alarm and timestamp, which is what a provider normally has to offer.
    ///
    /// This is the neutral form of CA's `Time` category, and of the `alarm`/`timeStamp`
    /// pair that every normative type carries.
    pub fn timestamped(timestamp: TimeStamp) -> Meta {
        Meta {
            alarm: Some(Alarm::none()),
            timestamp: Some(timestamp),
            ..Meta::default()
        }
    }

    pub fn with_alarm(mut self, alarm: Alarm) -> Meta {
        self.alarm = Some(alarm);
        self
    }

    pub fn with_timestamp(mut self, timestamp: TimeStamp) -> Meta {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn with_display(mut self, display: Display) -> Meta {
        self.display = Some(display);
        self
    }

    pub fn with_control(mut self, control: Control) -> Meta {
        self.control = Some(control);
        self
    }

    /// Whether no metadata is present at all.
    pub fn is_empty(&self) -> bool {
        self.alarm.is_none()
            && self.timestamp.is_none()
            && self.display.is_none()
            && self.control.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The 20-year trap: a time survives a round trip through *either* epoch, and the
    /// two disagree by exactly the offset.
    #[test]
    fn timestamps_round_trip_through_both_epochs() {
        let original = UNIX_EPOCH + Duration::new(1_753_000_000, 250_000_123);
        let stamp = TimeStamp::from(original);

        let (ca_seconds, ca_nanoseconds) = stamp.to_ca();
        assert_eq!(
            TimeStamp::from_ca(ca_seconds, ca_nanoseconds).unwrap().time,
            original
        );

        let (posix_seconds, posix_nanoseconds) = stamp.to_posix();
        assert_eq!(
            TimeStamp::from_posix(posix_seconds, posix_nanoseconds)
                .unwrap()
                .time,
            original
        );

        // ... and the two encodings of the same instant differ by the offset
        assert_eq!(i64::from(ca_seconds), posix_seconds - EPICS_EPOCH_OFFSET);
        assert_eq!(ca_nanoseconds, posix_nanoseconds);
        assert_eq!(posix_nanoseconds, 250_000_123);
    }

    /// The fixture from a real IOC: an unprocessed record reads back as EPICS zero,
    /// which `pvxget` renders as POSIX 631152000.
    #[test]
    fn zero_epics_timestamp_is_posix_631152000() {
        let zero = TimeStamp::from_ca(0, 0).unwrap();
        assert_eq!(zero.to_posix(), (631_152_000, 0));
        assert_eq!(zero.to_ca(), (0, 0));
        assert_eq!(
            zero.time,
            UNIX_EPOCH + Duration::from_secs(631_152_000),
            "a zero CA stamp is 1990-01-01, not 1970-01-01"
        );

        // The other direction: POSIX zero is 20 years before the EPICS epoch
        let posix_zero = TimeStamp::from(UNIX_EPOCH);
        assert_eq!(posix_zero.to_posix(), (0, 0));
        assert_eq!(posix_zero.to_ca(), (-631_152_000, 0));
    }

    /// Times before 1970 keep nanoseconds positive, as both wire formats require.
    #[test]
    fn pre_epoch_times_normalise_nanoseconds() {
        let before = UNIX_EPOCH - Duration::new(0, 500_000_000);
        let stamp = TimeStamp::from(before);
        assert_eq!(stamp.to_posix(), (-1, 500_000_000));
        assert_eq!(TimeStamp::from_posix(-1, 500_000_000).unwrap().time, before);

        // Exactly on a second boundary is not offset by one
        let whole = UNIX_EPOCH - Duration::from_secs(5);
        assert_eq!(TimeStamp::from(whole).to_posix(), (-5, 0));
        assert_eq!(TimeStamp::from_posix(-5, 0).unwrap().time, whole);
    }

    /// CA's 32-bit seconds field saturates rather than wrapping into a plausible-looking
    /// wrong answer.
    #[test]
    fn ca_seconds_saturate_outside_32_bits() {
        let far_future = TimeStamp::from_posix(i64::from(i32::MAX) + EPICS_EPOCH_OFFSET + 1, 0)
            .expect("representable as a SystemTime");
        assert_eq!(far_future.to_ca().0, i32::MAX);

        let far_past = TimeStamp::from_posix(i64::from(i32::MIN) + EPICS_EPOCH_OFFSET - 1, 0)
            .expect("representable as a SystemTime");
        assert_eq!(far_past.to_ca().0, i32::MIN);
    }

    #[test]
    fn user_tag_is_carried_but_has_no_ca_form() {
        let stamp = TimeStamp::now().with_user_tag(7);
        assert_eq!(stamp.user_tag, 7);
        // Round-tripping through CA drops it, there being no field to put it in
        let (seconds, nanoseconds) = stamp.to_ca();
        assert_eq!(
            TimeStamp::from_ca(seconds, nanoseconds).unwrap().user_tag,
            0
        );
    }

    #[test]
    fn alarm_severities_convert_both_ways() {
        for (index, name) in AlarmSeverity::choices().iter().enumerate() {
            let severity = AlarmSeverity::try_from(index as i32).unwrap();
            assert_eq!(severity.name(), *name);
            assert_eq!(i32::from(severity), index as i32);
        }
        assert!(AlarmSeverity::try_from(5).is_err());
        assert!(AlarmSeverity::try_from(-1).is_err());

        assert!(Alarm::none().is_ok());
        assert_eq!(AlarmSeverity::default(), AlarmSeverity::None);
        assert!(AlarmSeverity::Minor < AlarmSeverity::Major);
        assert!(
            !Alarm {
                severity: AlarmSeverity::Major,
                status: 3,
                message: "HIHI".to_string(),
            }
            .is_ok()
        );
    }

    #[test]
    fn display_forms_convert_both_ways() {
        for (index, name) in DisplayForm::choices().iter().enumerate() {
            let form = DisplayForm::try_from(index as i32).unwrap();
            assert_eq!(form.name(), *name);
            assert_eq!(i32::from(form), index as i32);
        }
        assert!(DisplayForm::try_from(7).is_err());
        assert_eq!(DisplayForm::default(), DisplayForm::Default);
    }

    #[test]
    fn meta_fields_are_independent() {
        assert!(Meta::new().is_empty());

        let alarm_only = Meta::new().with_alarm(Alarm::none());
        assert!(!alarm_only.is_empty());
        assert!(alarm_only.timestamp.is_none());
        assert!(alarm_only.display.is_none());

        let stamp = TimeStamp::now();
        let timed = Meta::timestamped(stamp);
        assert_eq!(timed.timestamp, Some(stamp));
        assert_eq!(timed.alarm, Some(Alarm::none()));
        assert!(timed.control.is_none());

        let full = timed
            .with_display(Display {
                units: "mm".to_string(),
                limit_low: -1.0,
                limit_high: 1.0,
                precision: 3,
                ..Display::default()
            })
            .with_control(Control {
                limit_low: -0.5,
                limit_high: 0.5,
                min_step: 0.01,
            });
        assert_eq!(full.display.as_ref().unwrap().units, "mm");
        assert!(full.display.as_ref().unwrap().has_limits());
        assert!(full.control.as_ref().unwrap().has_limits());
        assert!(!Display::default().has_limits());
        assert!(!Control::default().has_limits());
    }
}
