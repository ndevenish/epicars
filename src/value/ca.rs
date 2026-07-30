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

use crate::dbr::DbrValue;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{Structure, UnionField, UnionValue};

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
