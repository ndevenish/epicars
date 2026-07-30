//! Conversions between CA's [`DbrValue`] and the neutral [`Value`].
//!
//! These are the adapters that let the CA wire format and the neutral model coexist,
//! and they are deliberately asymmetric:
//!
//! - `From<&DbrValue> for Value` is **total**. Every DBR value has a neutral form.
//! - `TryFrom<&Value> for DbrValue` is **partial**. CA has no structures, no unions,
//!   no 64-bit integers, and only one unsigned type.
//!
//! ## What the mapping does
//!
//! Every DBR value except `Enum` is a `Vec`, so it maps to a [`ScalarArray`] rather
//! than a [`Scalar`] - a one-element array, in the common scalar case. The reverse
//! direction accepts both.
//!
//! Unsigned values **widen to the next signed CA type** rather than being truncated or
//! rejected: `u8` becomes `DBR_SHORT`, `u16` and `u32` become `DBR_LONG`. That is
//! lossless, so a `u32` fails only when it genuinely exceeds `i32::MAX`.
//!
//! `bool` becomes `DBR_CHAR` `0`/`1`, CA having no boolean type.
//!
//! ## The `u16` / `Enum` special case
//!
//! CA's only unsigned type is the enum index, so a `u16` **scalar** maps back to
//! [`DbrValue::Enum`], while a `u16` **array** maps to `DBR_LONG` like any other
//! integer array. The asymmetry is what makes `Enum → Value → Enum` return the
//! original, which the provider storage move in plan item 0.4 depends on: an
//! enum-typed PV whose storage has become a `Value` must still read back over CA as
//! `DBR_ENUM`.
//!
//! ## `Dbr` as a projection
//!
//! [`Dbr`] keeps its variants, but gains a pair of adapters -
//! [`Dbr::to_value_and_meta`] and [`Dbr::from_value_and_meta`] - that make the whole
//! category enum a *projection* of `(Value, Meta)`. Keeping the variants is deliberate:
//! decomposing the category into composable optional fields would change the CA wire
//! path, and cannot be behaviour-neutral. That is Phase 3's job, once pvAccess has shown
//! what the metadata actually needs to do.

use crate::dbr::{Dbr, DbrBasicType, DbrCategory, DbrValue, Status};
use crate::value::meta::{Alarm, AlarmSeverity, Meta, TimeStamp};
use crate::value::{Scalar, ScalarArray, ScalarType, Value};

/// Why a [`Value`] could not be expressed as a [`DbrValue`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DbrConversionError {
    /// CA transfers flat arrays of primitives and nothing else.
    #[error("CA has no representation for a {0}")]
    Unrepresentable(&'static str),
    /// CA's widest integer is `i32`.
    #[error("CA integers are at most 32 bits, so a {} does not fit", .0.name())]
    IntegerTooWide(ScalarType),
    /// An unsigned value too large for the signed CA type it would widen into.
    #[error("{} value {value} is outside the range of any signed CA type", .source_type.name())]
    OutOfRange { source_type: ScalarType, value: u64 },
    /// A DBR category this crate does not yet build. `Graphics` and `Control` are stubs
    /// in [`crate::dbr`], several of whose arms are `todo!()`.
    #[error("Cannot build a DBR of category {0:?} from neutral metadata")]
    UnsupportedCategory(DbrCategory),
}

impl From<DbrValue> for Value {
    fn from(value: DbrValue) -> Value {
        match value {
            // See the module docs: a scalar u16 is CA's enum index
            DbrValue::Enum(v) => Value::Scalar(Scalar::UShort(v)),
            DbrValue::String(v) => Value::ScalarArray(ScalarArray::String(v)),
            DbrValue::Char(v) => Value::ScalarArray(ScalarArray::Byte(v)),
            DbrValue::Int(v) => Value::ScalarArray(ScalarArray::Short(v)),
            DbrValue::Long(v) => Value::ScalarArray(ScalarArray::Int(v)),
            DbrValue::Float(v) => Value::ScalarArray(ScalarArray::Float(v)),
            DbrValue::Double(v) => Value::ScalarArray(ScalarArray::Double(v)),
        }
    }
}

impl From<&DbrValue> for Value {
    fn from(value: &DbrValue) -> Value {
        Value::from(value.clone())
    }
}

impl TryFrom<&Value> for DbrValue {
    type Error = DbrConversionError;

    fn try_from(value: &Value) -> Result<DbrValue, DbrConversionError> {
        match value {
            Value::Scalar(scalar) => scalar_to_dbr(scalar),
            Value::ScalarArray(array) => array_to_dbr(array),
            Value::Structure(_) => Err(DbrConversionError::Unrepresentable("structure")),
            Value::StructureArray(_) => Err(DbrConversionError::Unrepresentable("structure array")),
            Value::Union(_) => Err(DbrConversionError::Unrepresentable("union")),
            Value::VariantUnion(_) => Err(DbrConversionError::Unrepresentable("variant union")),
        }
    }
}

impl TryFrom<Value> for DbrValue {
    type Error = DbrConversionError;

    fn try_from(value: Value) -> Result<DbrValue, DbrConversionError> {
        DbrValue::try_from(&value)
    }
}

impl Value {
    /// Which CA basic type this value would encode as, without encoding it.
    ///
    /// Useful for the "coerce a write to the type already stored" path, where converting
    /// the whole array just to read its type off would be wasteful. Kept next to
    /// [`DbrValue::try_from`] because the two must agree, which
    /// `ca_basic_type_agrees_with_conversion` checks.
    ///
    /// Reports the *type*, so a `u32` array answers `Long` even though converting it may
    /// still fail on an individual out-of-range element.
    pub fn ca_basic_type(&self) -> Result<DbrBasicType, DbrConversionError> {
        let element_type = match self {
            Value::Scalar(scalar) => scalar.scalar_type(),
            Value::ScalarArray(array) => array.element_type(),
            Value::Structure(_) => return Err(DbrConversionError::Unrepresentable("structure")),
            Value::StructureArray(_) => {
                return Err(DbrConversionError::Unrepresentable("structure array"));
            }
            Value::Union(_) => return Err(DbrConversionError::Unrepresentable("union")),
            Value::VariantUnion(_) => {
                return Err(DbrConversionError::Unrepresentable("variant union"));
            }
        };
        Ok(match element_type {
            ScalarType::Bool | ScalarType::Byte => DbrBasicType::Char,
            ScalarType::UByte | ScalarType::Short => DbrBasicType::Int,
            // See the module docs: a scalar u16 is CA's enum index, an array is data
            ScalarType::UShort => match self {
                Value::Scalar(_) => DbrBasicType::Enum,
                _ => DbrBasicType::Long,
            },
            ScalarType::Int | ScalarType::UInt => DbrBasicType::Long,
            ScalarType::Long => return Err(DbrConversionError::IntegerTooWide(ScalarType::Long)),
            ScalarType::ULong => return Err(DbrConversionError::IntegerTooWide(ScalarType::ULong)),
            ScalarType::Float => DbrBasicType::Float,
            ScalarType::Double => DbrBasicType::Double,
            ScalarType::String => DbrBasicType::String,
        })
    }
}

/// Widen an unsigned value into `i32`, the widest signed integer CA has.
fn widen_unsigned(value: u64, source_type: ScalarType) -> Result<i32, DbrConversionError> {
    i32::try_from(value).map_err(|_| DbrConversionError::OutOfRange { source_type, value })
}

fn scalar_to_dbr(scalar: &Scalar) -> Result<DbrValue, DbrConversionError> {
    Ok(match scalar {
        Scalar::Bool(v) => DbrValue::Char(vec![i8::from(*v)]),
        Scalar::Byte(v) => DbrValue::Char(vec![*v]),
        Scalar::UByte(v) => DbrValue::Int(vec![i16::from(*v)]),
        Scalar::Short(v) => DbrValue::Int(vec![*v]),
        // See the module docs: a scalar u16 is CA's enum index
        Scalar::UShort(v) => DbrValue::Enum(*v),
        Scalar::Int(v) => DbrValue::Long(vec![*v]),
        Scalar::UInt(v) => DbrValue::Long(vec![widen_unsigned((*v).into(), ScalarType::UInt)?]),
        Scalar::Long(_) => Err(DbrConversionError::IntegerTooWide(ScalarType::Long))?,
        Scalar::ULong(_) => Err(DbrConversionError::IntegerTooWide(ScalarType::ULong))?,
        Scalar::Float(v) => DbrValue::Float(vec![*v]),
        Scalar::Double(v) => DbrValue::Double(vec![*v]),
        Scalar::String(v) => DbrValue::String(vec![v.clone()]),
    })
}

fn array_to_dbr(array: &ScalarArray) -> Result<DbrValue, DbrConversionError> {
    Ok(match array {
        ScalarArray::Bool(v) => DbrValue::Char(v.iter().map(|b| i8::from(*b)).collect()),
        ScalarArray::Byte(v) => DbrValue::Char(v.clone()),
        ScalarArray::UByte(v) => DbrValue::Int(v.iter().map(|n| i16::from(*n)).collect()),
        ScalarArray::Short(v) => DbrValue::Int(v.clone()),
        ScalarArray::UShort(v) => DbrValue::Long(v.iter().map(|n| i32::from(*n)).collect()),
        ScalarArray::Int(v) => DbrValue::Long(v.clone()),
        ScalarArray::UInt(v) => DbrValue::Long(
            v.iter()
                .map(|n| widen_unsigned((*n).into(), ScalarType::UInt))
                .collect::<Result<Vec<i32>, _>>()?,
        ),
        ScalarArray::Long(_) => Err(DbrConversionError::IntegerTooWide(ScalarType::Long))?,
        ScalarArray::ULong(_) => Err(DbrConversionError::IntegerTooWide(ScalarType::ULong))?,
        ScalarArray::Float(v) => DbrValue::Float(v.clone()),
        ScalarArray::Double(v) => DbrValue::Double(v.clone()),
        ScalarArray::String(v) => DbrValue::String(v.clone()),
    })
}

/// Conversions between CA's [`Status`] pair and the neutral [`Alarm`].
///
/// CA has no `message` field, so that is always empty coming from CA and dropped going
/// back. A CA `severity` outside the five defined values - which would be a protocol
/// violation - clamps to [`AlarmSeverity::Undefined`], and a `status` outside `i16`
/// range - which only pvAccess could produce - saturates on the way back.
impl From<Status> for Alarm {
    fn from(status: Status) -> Alarm {
        Alarm {
            severity: AlarmSeverity::try_from(i32::from(status.severity))
                .unwrap_or(AlarmSeverity::Undefined),
            status: status.status.into(),
            message: String::new(),
        }
    }
}

impl From<&Alarm> for Status {
    fn from(alarm: &Alarm) -> Status {
        Status {
            status: alarm
                .status
                .clamp(i16::MIN.into(), i16::MAX.into())
                .try_into()
                .expect("clamped to i16 range"),
            severity: i32::from(alarm.severity) as i16,
        }
    }
}

impl Dbr {
    /// Split a DBR into the neutral value and metadata it is carrying.
    ///
    /// Total: every category has a neutral form. `Graphics` and `Control` yield their
    /// alarm but **not** their display or control metadata - `DbrGraphics`/`DbrControl`
    /// are stubs whose limits are unreachable and several of whose arms are `todo!()`.
    /// Plan item 1.10 is where those get filled in, driven by NTScalar's `display_t` and
    /// `control_t`.
    ///
    /// [`Dbr::ClassName`] is a CA-only RPC riding the value channel rather than a value,
    /// so it produces a bare string with no metadata. Plan item 3.4 replaces it with an
    /// explicit `record_type()` on the provider trait.
    pub fn to_value_and_meta(&self) -> (Value, Meta) {
        let value = Value::from(self.value());
        let meta = match self {
            Dbr::Basic(_) | Dbr::ClassName(_) => Meta::new(),
            Dbr::Status { status, .. } => Meta::new().with_alarm(Alarm::from(*status)),
            Dbr::Time {
                status, timestamp, ..
            } => Meta::new()
                .with_alarm(Alarm::from(*status))
                .with_timestamp(TimeStamp::from(*timestamp)),
            // See above: the graphics/control payloads have nowhere to go yet
            Dbr::Graphics { status, .. } | Dbr::Control { status, .. } => {
                Meta::new().with_alarm(Alarm::from(*status))
            }
        };
        (value, meta)
    }

    /// Project a neutral value and metadata onto a DBR of the requested category.
    ///
    /// Partial, in the two ways the neutral model is wider than CA:
    ///
    /// - the value may be unrepresentable, per [`DbrValue::try_from`];
    /// - `Graphics` and `Control` are rejected outright with
    ///   [`DbrConversionError::UnsupportedCategory`], rather than silently producing
    ///   defaults or reaching a `todo!()`.
    ///
    /// Metadata the category needs but `meta` does not carry is filled in with the same
    /// defaults [`Dbr::convert_to`] uses: no alarm, and the current time.
    pub fn from_value_and_meta(
        category: DbrCategory,
        value: &Value,
        meta: &Meta,
    ) -> Result<Dbr, DbrConversionError> {
        let value = DbrValue::try_from(value)?;
        let status = meta.alarm.as_ref().map(Status::from).unwrap_or_default();
        Ok(match category {
            DbrCategory::Basic => Dbr::Basic(value),
            DbrCategory::Status => Dbr::Status { status, value },
            DbrCategory::Time => Dbr::Time {
                status,
                timestamp: meta
                    .timestamp
                    .map(|t| t.time)
                    .unwrap_or_else(std::time::SystemTime::now),
                value,
            },
            DbrCategory::ClassName => Dbr::ClassName(value),
            category @ (DbrCategory::Graphics | DbrCategory::Control) => {
                return Err(DbrConversionError::UnsupportedCategory(category));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbr::{DbrGraphics, DbrType};
    use crate::value::meta::{Display, EPICS_EPOCH_OFFSET};
    use crate::value::{Structure, UnionField, UnionValue};
    use std::time::{Duration, UNIX_EPOCH};

    /// Every `DbrValue` variant survives the trip through `Value` unchanged.
    #[test]
    fn round_trip_every_dbrvalue_variant() {
        let originals = [
            DbrValue::Enum(3),
            DbrValue::Enum(u16::MAX),
            DbrValue::String(vec!["one".to_string(), "two".to_string()]),
            DbrValue::String(vec![]),
            DbrValue::Char(vec![-1, 0, 127]),
            DbrValue::Char(vec![]),
            DbrValue::Int(vec![i16::MIN, 0, i16::MAX]),
            DbrValue::Long(vec![i32::MIN, 0, i32::MAX]),
            DbrValue::Float(vec![-1.5, 0.0, f32::MAX]),
            DbrValue::Double(vec![-1.5, 0.0, f64::MAX]),
        ];
        for original in originals {
            let neutral = Value::from(&original);
            let back = DbrValue::try_from(&neutral)
                .unwrap_or_else(|e| panic!("{original:?} did not convert back: {e}"));
            assert_eq!(back, original, "round trip changed {original:?}");
            // The type is preserved, not just the contents
            assert_eq!(back.get_type(), original.get_type());
        }
    }

    /// Which neutral type each DBR variant lands on. The `Enum`/`u16` case is the one
    /// worth pinning down - see the module docs.
    #[test]
    fn dbr_maps_onto_the_expected_neutral_types() {
        assert_eq!(
            Value::from(DbrValue::Enum(3)),
            Value::Scalar(Scalar::UShort(3))
        );
        assert_eq!(
            Value::from(DbrValue::Char(vec![1, 2])),
            Value::ScalarArray(ScalarArray::Byte(vec![1, 2]))
        );
        assert_eq!(
            Value::from(DbrValue::Int(vec![1])).scalar_type(),
            Some(ScalarType::Short)
        );
        assert_eq!(
            Value::from(DbrValue::Long(vec![1])).scalar_type(),
            Some(ScalarType::Int)
        );
        assert_eq!(
            Value::from(DbrValue::Float(vec![1.0])).scalar_type(),
            Some(ScalarType::Float)
        );
        assert_eq!(
            Value::from(DbrValue::Double(vec![1.0])).scalar_type(),
            Some(ScalarType::Double)
        );
        assert_eq!(
            Value::from(DbrValue::String(vec!["a".to_string()])).scalar_type(),
            Some(ScalarType::String)
        );

        // A u16 array is data, not an enum index
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![1u16, 2])).unwrap(),
            DbrValue::Long(vec![1, 2])
        );
    }

    /// Scalars and one-element arrays are interchangeable coming back to CA.
    #[test]
    fn scalars_and_single_element_arrays_both_convert() {
        assert_eq!(
            DbrValue::try_from(&Value::from(7i32)).unwrap(),
            DbrValue::Long(vec![7])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![7i32])).unwrap(),
            DbrValue::Long(vec![7])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from("hello")).unwrap(),
            DbrValue::String(vec!["hello".to_string()])
        );
        // Owned and borrowed forms agree
        assert_eq!(
            DbrValue::try_from(Value::from(1.5f64)).unwrap(),
            DbrValue::Double(vec![1.5])
        );
    }

    /// Types CA lacks, but which widen losslessly into a type it has.
    #[test]
    fn unsigned_and_bool_widen_losslessly() {
        assert_eq!(
            DbrValue::try_from(&Value::from(200u8)).unwrap(),
            DbrValue::Int(vec![200])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![0u8, 255])).unwrap(),
            DbrValue::Int(vec![0, 255])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![u16::MAX])).unwrap(),
            DbrValue::Long(vec![65535])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(i32::MAX as u32)).unwrap(),
            DbrValue::Long(vec![i32::MAX])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(true)).unwrap(),
            DbrValue::Char(vec![1])
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![true, false])).unwrap(),
            DbrValue::Char(vec![1, 0])
        );
    }

    // One negative test per rejection reason.

    #[test]
    fn structures_are_rejected() {
        let structure =
            Value::from(Structure::with_id("epics:nt/NTScalar:1.0").with("value", 1i32));
        assert_eq!(
            DbrValue::try_from(&structure),
            Err(DbrConversionError::Unrepresentable("structure"))
        );
    }

    #[test]
    fn structure_arrays_are_rejected() {
        let array = Value::StructureArray(vec![Some(Structure::new().with("x", 1i32)), None]);
        assert_eq!(
            DbrValue::try_from(&array),
            Err(DbrConversionError::Unrepresentable("structure array"))
        );
    }

    #[test]
    fn unions_are_rejected() {
        let mut union = UnionValue::new(UnionField::new().with("intValue", ScalarType::Int));
        union.select("intValue", 1i32).unwrap();
        assert_eq!(
            DbrValue::try_from(&Value::from(union)),
            Err(DbrConversionError::Unrepresentable("union"))
        );
        assert_eq!(
            DbrValue::try_from(&Value::VariantUnion(Some(Box::new(Value::from(1i32))))),
            Err(DbrConversionError::Unrepresentable("variant union"))
        );
    }

    #[test]
    fn sixty_four_bit_integers_are_rejected() {
        // Even when the value itself would fit - the type is what CA cannot express
        assert_eq!(
            DbrValue::try_from(&Value::from(1i64)),
            Err(DbrConversionError::IntegerTooWide(ScalarType::Long))
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![1i64])),
            Err(DbrConversionError::IntegerTooWide(ScalarType::Long))
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(1u64)),
            Err(DbrConversionError::IntegerTooWide(ScalarType::ULong))
        );
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![1u64])),
            Err(DbrConversionError::IntegerTooWide(ScalarType::ULong))
        );
    }

    /// `Dbr` as a projection: the three categories a provider actually produces survive
    /// a round trip through `(Value, Meta)` unchanged.
    #[test]
    fn basic_status_and_time_round_trip_through_value_and_meta() {
        let timestamp = UNIX_EPOCH + Duration::new(1_753_000_000, 250_000_123);
        let status = Status {
            status: 3,
            severity: 2,
        };
        let originals = [
            Dbr::Basic(DbrValue::Long(vec![42])),
            Dbr::Status {
                status,
                value: DbrValue::Double(vec![1.5, -1.5]),
            },
            Dbr::Time {
                status,
                timestamp,
                value: DbrValue::Char(vec![-1, 0, 127]),
            },
            // The remaining variants, for completeness of the value path
            Dbr::Time {
                status: Status::default(),
                timestamp,
                value: DbrValue::Enum(7),
            },
            Dbr::ClassName(DbrValue::String(vec!["longout".to_string()])),
        ];

        for original in originals {
            let (value, meta) = original.to_value_and_meta();
            let back =
                Dbr::from_value_and_meta(original.data_type().category, &value, &meta).unwrap();

            assert_eq!(back.value(), original.value(), "value changed");
            assert_eq!(back.data_type(), original.data_type(), "type changed");
            if let (Dbr::Time { timestamp: a, .. }, Dbr::Time { timestamp: b, .. }) =
                (&back, &original)
            {
                assert_eq!(a, b, "timestamp changed");
            }
            match (back.status(), original.status()) {
                (Some(a), Some(b)) => {
                    assert_eq!((a.status, a.severity), (b.status, b.severity));
                }
                (None, None) => (),
                (a, b) => panic!("alarm presence changed: {a:?} vs {b:?}"),
            }
        }
    }

    /// Which metadata each category carries, and which it does not.
    #[test]
    fn categories_project_onto_the_expected_meta_fields() {
        let (_, basic) = Dbr::Basic(DbrValue::Long(vec![1])).to_value_and_meta();
        assert!(basic.is_empty(), "Basic carries no metadata at all");

        let (_, status) = Dbr::Status {
            status: Status {
                status: 5,
                severity: 1,
            },
            value: DbrValue::Long(vec![1]),
        }
        .to_value_and_meta();
        assert_eq!(
            status.alarm,
            Some(Alarm {
                severity: AlarmSeverity::Minor,
                status: 5,
                // CA has no message field
                message: String::new(),
            })
        );
        assert!(status.timestamp.is_none());

        let (value, time) = Dbr::Time {
            status: Status::default(),
            timestamp: UNIX_EPOCH + Duration::from_secs(631_152_000),
            value: DbrValue::Long(vec![1]),
        }
        .to_value_and_meta();
        assert_eq!(value, Value::from(vec![1i32]));
        assert_eq!(time.alarm, Some(Alarm::none()));
        // Stored epoch-neutrally, so it reads out differently per protocol
        let stamp = time.timestamp.unwrap();
        assert_eq!(stamp.to_ca(), (0, 0));
        assert_eq!(stamp.to_posix(), (EPICS_EPOCH_OFFSET, 0));
        assert!(time.display.is_none() && time.control.is_none());
    }

    /// The stub categories: alarm comes through, the graphics payload does not, and the
    /// reverse direction refuses rather than reaching a `todo!()`.
    #[test]
    fn graphics_and_control_are_one_way_for_now() {
        let graphics = Dbr::Graphics {
            status: Status {
                status: 1,
                severity: 3,
            },
            graphics: DbrGraphics::Long {
                units: "counts".to_string(),
                limits: Default::default(),
            },
            value: DbrValue::Long(vec![1]),
        };
        let (_, meta) = graphics.to_value_and_meta();
        assert_eq!(
            meta.alarm.as_ref().unwrap().severity,
            AlarmSeverity::Invalid
        );
        assert!(
            meta.display.is_none(),
            "DbrGraphics limits are unreachable, so nothing to project"
        );

        // Going the other way is refused, even with display metadata to hand
        let with_display = Meta::new().with_display(Display {
            units: "counts".to_string(),
            ..Display::default()
        });
        for category in [DbrCategory::Graphics, DbrCategory::Control] {
            assert_eq!(
                Dbr::from_value_and_meta(category, &Value::from(1i32), &with_display).unwrap_err(),
                DbrConversionError::UnsupportedCategory(category)
            );
        }
    }

    /// `ca_basic_type` is a shortcut for "the type `try_from` would produce", so the two
    /// must never disagree.
    #[test]
    fn ca_basic_type_agrees_with_conversion() {
        let values = [
            Value::from(true),
            Value::from(vec![true]),
            Value::from(1i8),
            Value::from(vec![1i8]),
            Value::from(1u8),
            Value::from(vec![1u8]),
            Value::from(1i16),
            Value::from(vec![1i16]),
            // The one asymmetric case
            Value::from(1u16),
            Value::from(vec![1u16]),
            Value::from(1i32),
            Value::from(vec![1i32]),
            Value::from(1u32),
            Value::from(vec![1u32]),
            Value::from(1.0f32),
            Value::from(vec![1.0f32]),
            Value::from(1.0f64),
            Value::from(vec![1.0f64]),
            Value::from("s"),
            Value::from(vec!["s".to_string()]),
        ];
        for value in &values {
            assert_eq!(
                value.ca_basic_type().unwrap(),
                DbrValue::try_from(value).unwrap().get_type(),
                "disagreement for {value:?}"
            );
        }
        assert_eq!(
            Value::from(1u16).ca_basic_type().unwrap(),
            DbrBasicType::Enum
        );
        assert_eq!(
            Value::from(vec![1u16]).ca_basic_type().unwrap(),
            DbrBasicType::Long
        );

        // ... and it rejects for the same reasons, without needing the data
        assert_eq!(
            Value::from(Structure::new()).ca_basic_type().unwrap_err(),
            DbrConversionError::Unrepresentable("structure")
        );
        assert_eq!(
            Value::StructureArray(Vec::new())
                .ca_basic_type()
                .unwrap_err(),
            DbrConversionError::Unrepresentable("structure array")
        );
        assert_eq!(
            Value::VariantUnion(None).ca_basic_type().unwrap_err(),
            DbrConversionError::Unrepresentable("variant union")
        );
        assert_eq!(
            Value::from(1i64).ca_basic_type().unwrap_err(),
            DbrConversionError::IntegerTooWide(ScalarType::Long)
        );
        assert_eq!(
            Value::from(vec![1u64]).ca_basic_type().unwrap_err(),
            DbrConversionError::IntegerTooWide(ScalarType::ULong)
        );

        // The type is known even where a particular value would not convert
        assert_eq!(
            Value::from(u32::MAX).ca_basic_type().unwrap(),
            DbrBasicType::Long
        );
        assert!(DbrValue::try_from(&Value::from(u32::MAX)).is_err());
    }

    /// Metadata the category needs but which is absent gets the same defaults
    /// `Dbr::convert_to` uses.
    #[test]
    fn missing_metadata_falls_back_to_defaults() {
        let before = std::time::SystemTime::now();
        let dbr =
            Dbr::from_value_and_meta(DbrCategory::Time, &Value::from(1i32), &Meta::new()).unwrap();
        let Dbr::Time {
            status, timestamp, ..
        } = &dbr
        else {
            panic!("not a Time DBR");
        };
        assert_eq!((status.status, status.severity), (0, 0));
        assert!(*timestamp >= before);

        // A value the neutral model can hold but CA cannot still fails, per variant
        assert_eq!(
            Dbr::from_value_and_meta(DbrCategory::Time, &Value::from(1i64), &Meta::new())
                .unwrap_err(),
            DbrConversionError::IntegerTooWide(ScalarType::Long)
        );
    }

    /// Alarms convert both ways, and out-of-range values are handled rather than
    /// panicking.
    #[test]
    fn alarms_convert_both_ways() {
        for severity in 0..=4i16 {
            let alarm = Alarm::from(Status {
                status: -7,
                severity,
            });
            assert_eq!(i32::from(alarm.severity), i32::from(severity));
            let back = Status::from(&alarm);
            assert_eq!((back.status, back.severity), (-7, severity));
        }

        // A severity CA should never send clamps rather than panicking
        assert_eq!(
            Alarm::from(Status {
                status: 0,
                severity: 99,
            })
            .severity,
            AlarmSeverity::Undefined
        );

        // A status only pvAccess could produce saturates into CA's i16
        assert_eq!(
            Status::from(&Alarm {
                severity: AlarmSeverity::None,
                status: i32::MAX,
                message: "dropped".to_string(),
            })
            .status,
            i16::MAX
        );
    }

    /// The projection does not disturb the existing wire path: bytes out are identical
    /// whether the DBR was built directly or via `(Value, Meta)`.
    #[test]
    fn projected_dbrs_encode_identically() {
        let timestamp = UNIX_EPOCH + Duration::from_secs(1_741_731_609);
        let direct = Dbr::Time {
            status: Status::default(),
            timestamp,
            value: DbrValue::Long(vec![42]),
        };
        let (value, meta) = direct.to_value_and_meta();
        let projected = Dbr::from_value_and_meta(DbrCategory::Time, &value, &meta).unwrap();

        let wire_type = DbrType {
            basic_type: crate::dbr::DbrBasicType::Long,
            category: DbrCategory::Time,
        };
        assert_eq!(
            projected.convert_to(wire_type).unwrap().to_bytes(None),
            direct.convert_to(wire_type).unwrap().to_bytes(None)
        );
    }

    #[test]
    fn unsigned_values_outside_the_signed_range_are_rejected() {
        let too_big = i32::MAX as u32 + 1;
        assert_eq!(
            DbrValue::try_from(&Value::from(too_big)),
            Err(DbrConversionError::OutOfRange {
                source_type: ScalarType::UInt,
                value: too_big.into(),
            })
        );
        // ... including one bad element in an otherwise convertible array
        assert_eq!(
            DbrValue::try_from(&Value::from(vec![1u32, u32::MAX])),
            Err(DbrConversionError::OutOfRange {
                source_type: ScalarType::UInt,
                value: u32::MAX.into(),
            })
        );
    }
}
