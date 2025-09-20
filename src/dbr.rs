#![allow(dead_code)]

//! Represent CA DBR representations, for data interchange.
//!
//! CA defines thirty-five [DBR] kinds as special structures used to transfer data back
//! and forth. These can be broken down into seven basic array types, which define the
//! data, and five categories of attached metadata. This module models this, and
//! provides tools for handling generic data, converting between data types, and
//! serialization/deserialization for communication over CA.
//!
//! The basic types are enumerated in [`DbrBasicType`] and are represented in
//! [`DbrValue`] - all numeric data types in CA are signed, most can represent arrays
//! (in this crate, not necessarily in the epics-base implementation of CA). The
//! options, and the native type used to represent, are:
//! - [`DbrValue::Char`] ([`Vec<i8>`])
//! - [`DbrValue::Int`] ([`Vec<i16>`])
//! - [`DbrValue::Long`] ([`Vec<i32>`])
//! - [`DbrValue::Float`] ([`Vec<f32>`])
//! - [`DbrValue::Double`] ([`Vec<f64>`])
//! - [`DbrValue::Enum`] ([`u16`] by encoding) which is a special case - it reresents an
//!   index of an array of `[[u8; 26]; 16]` string options, normally stored on
//!   [`Dbr::Graphics`] but not yet implemented in this library.
//! - [`DbrValue::String`] - natively in CA this is a `[u8; 40]`, but for interchange
//!   here is represented by [`Vec<String>`], and is converted back and forth to
//!   fixed-length as required for communication. There is minimal support for
//!   manupulating this type, because mostly strings are implemented as Char arrays "in
//!   the wild" of our facility, and I am currently unsure whether this is practically
//!   ever manipulated as an array.
//!
//! The protocol also defines `SHORT` as an alias for `INT` - this is ignored here to
//! avoid excessive confusion.
//!
//! In CA, these seven data types can be sent with five kinds of metadata attached.
//! These are enumerated by [`DbrCategory`] and represented by [`Dbr`]. The five
//! categories are:
//! - [`Dbr::Basic`] - No extra metadata included, just the plain data value.
//! - [`Dbr::Status`] - Carries information about alarm status and severity in addition
//!   to the data.
//! - [`Dbr::Time`] - All of the information from [`Dbr::Status`], but with associated
//!   timestamp information.
//! - [`Dbr::Graphics`] - In CA, this controls information about the represented value
//!   e.g. units, limits. **This is currently unimplemented**.
//! - [`Dbr::Control`] - This can be used to fetch enum values, but further uses are
//!   unclear. **This is currently unimplemented**.
//!
//! In addition, there are four not-generically typed DBR Kinds:
//! - `Dbr::PutAckT` - Alert related, unimplemented
//! - `Dbr::PutAckS` - Alert related, unimplemented
//! - `Dbr::STSack_String` - Status related, unimplemented
//! - [`Dbr::ClassName`] - Returns the EPICS record type for the PV.
//!
//! Both [`DbrCategory`] and [`DbrBasicType`] are combined in the [`DbrType`] struct,
//! which provides interfaces to convert to/from the integer representation of types
//! used by the CA protocol.
//!
//! [DBR]:
//!     https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types
//!
use nom::{
    Parser,
    multi::count,
    number::complete::{be_f32, be_f64, be_i8, be_i16, be_i32, be_u16, be_u32},
};
use num::{Bounded, NumCast, cast::AsPrimitive, traits::ToBytes};
use std::{
    cmp,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Cursor},
    num::NonZeroUsize,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::messages::ErrorCondition;

// Constants from EPICS
const MAX_UNITS_SIZE: usize = 8;
const MAX_ENUM_STRING_SIZE: usize = 26;
const MAX_ENUM_STATES: usize = 16;

/// Encode a String to a fixed-maximum-length byte array
///
/// Problem: We want to convert a string to a byte sequence but never a length >
/// 40 (the fixed length of EPICS CA Strings). But we can't convert and truncate
/// because although we don't _expect_ to ever handle non-ASCII it technically
/// isn't guaranteed. So, convert one-character-at-a-time until the length would
/// go over.
fn string_to_fixed_length_bytes(value: &str, max_length: usize) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(max_length);
    for c in value.chars() {
        if buffer.len() + c.len_utf8() < max_length {
            let mut char_buffer = [0u8; 4];
            buffer.extend_from_slice(c.encode_utf8(&mut char_buffer).as_bytes());
        } else {
            break;
        }
    }
    buffer
}

/// Represent actual data transferred over CA
#[derive(Clone, Debug, PartialEq)]
pub enum DbrValue {
    Enum(u16),
    String(Vec<String>),
    Char(Vec<i8>),
    Int(Vec<i16>),
    Long(Vec<i32>),
    Float(Vec<f32>),
    Double(Vec<f64>),
}
/// Error returned when trying to resize a DBR but it's a data type that can't
#[derive(Debug)]
pub struct DbrValueIsEnumError;

/// Types of errors that can be returned from [`DbrValue::parse_into`]
#[derive(Debug)]
pub enum DbrParseError {
    SelfIsNotString,
    CannotParse(String),
}

impl DbrValue {
    pub fn get_default_record_type(&self) -> String {
        match self {
            DbrValue::Enum(_) => "mbbo".to_string(),
            DbrValue::String(_) => "waveform".to_string(),
            DbrValue::Char(_) => "longout".to_string(),
            DbrValue::Int(_) => "longout".to_string(),
            DbrValue::Long(_) => "longout".to_string(),
            DbrValue::Float(_) => "aao".to_string(),
            DbrValue::Double(_) => "aao".to_string(),
        }
    }
    pub fn get_count(&self) -> usize {
        match self {
            DbrValue::Enum(_) => 1,
            DbrValue::String(val) => val.len(),
            DbrValue::Char(val) => val.len(),
            DbrValue::Int(val) => val.len(),
            DbrValue::Long(val) => val.len(),
            DbrValue::Float(val) => val.len(),
            DbrValue::Double(val) => val.len(),
        }
    }
    pub fn get_type(&self) -> DbrBasicType {
        match self {
            DbrValue::Enum(_) => DbrBasicType::Enum,
            DbrValue::String(_) => DbrBasicType::String,
            DbrValue::Char(_) => DbrBasicType::Char,
            DbrValue::Int(_) => DbrBasicType::Int,
            DbrValue::Long(_) => DbrBasicType::Long,
            DbrValue::Float(_) => DbrBasicType::Float,
            DbrValue::Double(_) => DbrBasicType::Double,
        }
    }

    /// Convert a DbrValue::String to another data type by parsing the numeric string
    ///
    /// Fails if the DbrValue is not String or if the value cannot be parsed. Asking
    /// for a convertion from String->String just copies without doing any extra parsing.
    pub fn parse_into(&self, basic_type: DbrBasicType) -> Result<DbrValue, DbrParseError> {
        let DbrValue::String(val) = self else {
            return Err(DbrParseError::SelfIsNotString);
        };
        Ok(match basic_type {
            DbrBasicType::Enum => todo!(),
            DbrBasicType::String => self.clone(),
            DbrBasicType::Char => DbrValue::Char(
                val.iter()
                    .map(|s| s.parse().map_err(|_| DbrParseError::CannotParse(s.clone())))
                    .collect::<Result<Vec<_>, DbrParseError>>()?,
            ),
            DbrBasicType::Int => DbrValue::Int(
                val.iter()
                    .map(|s| s.parse().map_err(|_| DbrParseError::CannotParse(s.clone())))
                    .collect::<Result<Vec<_>, DbrParseError>>()?,
            ),
            DbrBasicType::Long => DbrValue::Long(
                val.iter()
                    .map(|s| s.parse().map_err(|_| DbrParseError::CannotParse(s.clone())))
                    .collect::<Result<Vec<_>, DbrParseError>>()?,
            ),
            DbrBasicType::Float => DbrValue::Float(
                val.iter()
                    .map(|s| s.parse().map_err(|_| DbrParseError::CannotParse(s.clone())))
                    .collect::<Result<Vec<_>, DbrParseError>>()?,
            ),
            DbrBasicType::Double => DbrValue::Double(
                val.iter()
                    .map(|s| s.parse().map_err(|_| DbrParseError::CannotParse(s.clone())))
                    .collect::<Result<Vec<_>, DbrParseError>>()?,
            ),
        })
    }

    pub fn convert_to(&self, basic_type: DbrBasicType) -> Result<DbrValue, ErrorCondition> {
        /// Utility function so that we don't have to repeat the map iter conversion
        fn _try_convert_vec<T, U>(from: &[T]) -> Result<Vec<U>, ErrorCondition>
        where
            T: Copy + NumCast,
            U: NumCast,
        {
            from.iter()
                .map(|n| NumCast::from(*n).ok_or(ErrorCondition::NoConvert))
                .collect()
        }
        /// Convert a single-item string to a numeric array
        fn _encode_string<T>(from: &Vec<String>) -> Result<Vec<T>, ErrorCondition>
        where
            T: Copy + 'static,
            u8: AsPrimitive<T>,
        {
            Ok(match from.as_slice() {
                [] => Vec::new(),
                [val] => val.as_bytes().iter().map(|c| c.as_()).collect(),
                _ => Err(ErrorCondition::NoConvert)?,
            })
        }

        Ok(match basic_type {
            DbrBasicType::Char => match self {
                DbrValue::Char(_val) => self.clone(),
                DbrValue::Int(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::String(val) => DbrValue::Char(_encode_string(val)?),
                DbrValue::Enum(val) => {
                    DbrValue::Char(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DbrBasicType::Int => match self {
                DbrValue::Char(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Int(_val) => self.clone(),
                DbrValue::Long(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::String(val) => DbrValue::Int(_encode_string(val)?),
                DbrValue::Enum(val) => {
                    DbrValue::Int(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DbrBasicType::Long => match self {
                DbrValue::Char(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Long(_val) => self.clone(),
                DbrValue::Float(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::String(val) => DbrValue::Long(_encode_string(val)?),
                DbrValue::Enum(val) => {
                    DbrValue::Long(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DbrBasicType::Float => match self {
                DbrValue::Char(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Float(_val) => self.clone(),
                DbrValue::Double(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::String(val) => DbrValue::Float(_encode_string(val)?),
                DbrValue::Enum(val) => {
                    DbrValue::Float(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DbrBasicType::Double => match self {
                DbrValue::Char(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Double(_val) => self.clone(),
                DbrValue::String(val) => DbrValue::Double(_encode_string(val)?),
                DbrValue::Enum(val) => {
                    DbrValue::Double(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DbrBasicType::String => match self {
                DbrValue::String(_) => self.clone(),
                DbrValue::Char(val) => DbrValue::String(vec![
                    String::from_utf8(val.iter().map(|c| *c as u8).collect())
                        .map_err(|_| ErrorCondition::NoConvert)?,
                ]),
                _ => return Err(ErrorCondition::UnavailInServ),
            },
            DbrBasicType::Enum => match self {
                DbrValue::Enum(_val) => self.clone(),
                _ => return Err(ErrorCondition::NoConvert),
            },
        })
    }

    /// Encode the value contents of a DBR into a byte vector
    ///
    /// If max_elems is `None`, then all elements available will be returned.
    ///
    /// Returns the number of elements along with the bytes
    pub fn to_bytes(&self, max_elems: Option<NonZeroUsize>) -> (usize, Vec<u8>) {
        let elements = if let Some(max_elem) = max_elems {
            cmp::min(max_elem.into(), self.get_count())
        } else {
            self.get_count()
        };

        (
            elements,
            match self {
                DbrValue::Enum(val) => val.to_be_bytes().to_vec(),
                DbrValue::String(val) => val
                    .iter()
                    .flat_map(|v| {
                        let mut buf = string_to_fixed_length_bytes(v, 39);
                        buf.resize(40, 0u8);
                        buf
                    })
                    .collect(),
                DbrValue::Char(val) => val
                    .iter()
                    .take(elements)
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
                DbrValue::Int(val) => val
                    .iter()
                    .take(elements)
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
                DbrValue::Long(val) => val
                    .iter()
                    .take(elements)
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
                DbrValue::Float(val) => val
                    .iter()
                    .take(elements)
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
                DbrValue::Double(val) => val
                    .iter()
                    .take(elements)
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
            },
        )
    }

    pub fn decode_value(
        data_type: DbrBasicType,
        item_count: usize,
        data: &[u8],
    ) -> Result<DbrValue, nom::Err<nom::error::Error<&[u8]>>> {
        match data_type {
            DbrBasicType::Enum => {
                assert!(
                    item_count == 1,
                    "Multiple item count makes no sense for enum"
                );
                Ok(DbrValue::Enum(be_u16.parse(data)?.1))
            }
            DbrBasicType::String => Ok(DbrValue::String(
                data.chunks(40)
                    .map(|d| {
                        let strlen = d.iter().position(|&c| c == 0x00).unwrap();
                        str::from_utf8(&d[0..strlen]).unwrap()
                    })
                    .take(item_count)
                    .map(|s| s.to_string())
                    .collect(),
            )),
            DbrBasicType::Char => Ok(DbrValue::Char(count(be_i8, item_count).parse(data)?.1)),
            DbrBasicType::Int => Ok(DbrValue::Int(count(be_i16, item_count).parse(data)?.1)),
            DbrBasicType::Long => Ok(DbrValue::Long(count(be_i32, item_count).parse(data)?.1)),
            DbrBasicType::Float => Ok(DbrValue::Float(count(be_f32, item_count).parse(data)?.1)),
            DbrBasicType::Double => Ok(DbrValue::Double(count(be_f64, item_count).parse(data)?.1)),
        }
    }

    pub fn resize(&mut self, to_size: usize) -> Result<(), DbrValueIsEnumError> {
        match self {
            DbrValue::Enum(_) => Err(DbrValueIsEnumError)?,
            DbrValue::String(items) => items.resize(to_size, String::new()),
            DbrValue::Char(items) => items.resize(to_size, 0),
            DbrValue::Int(items) => items.resize(to_size, 0),
            DbrValue::Long(items) => items.resize(to_size, 0),
            DbrValue::Float(items) => items.resize(to_size, 0.0),
            DbrValue::Double(items) => items.resize(to_size, 0.0),
        };
        Ok(())
    }
}

/// Implement a From<datatype> for a specific dbrvalue kind
macro_rules! impl_dbrvalue_conversions_between {
    ($variant:ident, $typ:ty) => {
        impl From<Vec<$typ>> for DbrValue {
            fn from(value: Vec<$typ>) -> Self {
                DbrValue::$variant(value)
            }
        }
        impl From<&$typ> for DbrValue {
            fn from(value: &$typ) -> Self {
                DbrValue::$variant(vec![value.clone()])
            }
        }
        impl TryFrom<&DbrValue> for Vec<$typ> {
            type Error = ErrorCondition;
            fn try_from(value: &DbrValue) -> Result<Self, Self::Error> {
                Ok(match value.convert_to(DbrBasicType::$variant)? {
                    DbrValue::$variant(v) => v,
                    _ => unreachable!(),
                })
            }
        }
    };
}
impl_dbrvalue_conversions_between!(Char, i8);
impl_dbrvalue_conversions_between!(Int, i16);
impl_dbrvalue_conversions_between!(Long, i32);
impl_dbrvalue_conversions_between!(Float, f32);
impl_dbrvalue_conversions_between!(Double, f64);
impl_dbrvalue_conversions_between!(String, String);

macro_rules! impl_dbrvalue_copy_conversions_between {
    ($variant:ident, $typ:ty) => {
        impl From<$typ> for DbrValue {
            fn from(value: $typ) -> Self {
                DbrValue::$variant(vec![value])
            }
        }
    };
}
impl_dbrvalue_copy_conversions_between!(Char, i8);
impl_dbrvalue_copy_conversions_between!(Int, i16);
impl_dbrvalue_copy_conversions_between!(Long, i32);
impl_dbrvalue_copy_conversions_between!(Float, f32);
impl_dbrvalue_copy_conversions_between!(Double, f64);

#[derive(Clone, Debug)]
pub struct Limits<T: num_traits::Bounded + ToBytes> {
    display_limits: (T, T),
    alarm_limits: (T, T),
    warning_limits: (T, T),
}
impl<T: Bounded + ToBytes> Limits<T> {
    fn to_be_bytes(&self) -> Vec<u8> {
        let (d_l, d_u) = &self.display_limits;
        let (a_l, a_u) = &self.alarm_limits;
        let (w_l, w_u) = &self.warning_limits;

        let values = [d_u, d_l, a_u, w_u, w_l, a_l];
        values
            .iter()
            .flat_map(|v| v.to_be_bytes().as_ref().to_vec())
            .collect()
    }
}
impl<T: Bounded + ToBytes> Default for Limits<T> {
    fn default() -> Self {
        Self {
            display_limits: (T::min_value(), T::max_value()),
            alarm_limits: (T::min_value(), T::max_value()),
            warning_limits: (T::min_value(), T::max_value()),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DbrGraphics {
    Enum,
    String,
    Char {
        units: String,
        limits: Limits<i8>,
    },
    Int {
        units: String,
        limits: Limits<i16>,
    },
    Long {
        units: String,
        limits: Limits<i32>,
    },
    Float {
        units: String,
        limits: Limits<f32>,
        precision: i16,
    },
    Double {
        units: String,
        limits: Limits<f64>,
        precision: i16,
    },
}

impl DbrGraphics {
    fn default_for(kind: DbrBasicType) -> Self {
        match kind {
            DbrBasicType::String => todo!(),
            DbrBasicType::Enum => todo!(),
            DbrBasicType::Int => DbrGraphics::Int {
                units: String::new(),
                limits: Limits::default(),
            },
            DbrBasicType::Char => DbrGraphics::Char {
                units: String::new(),
                limits: Limits::default(),
            },
            DbrBasicType::Long => DbrGraphics::Long {
                units: String::new(),
                limits: Limits::default(),
            },
            DbrBasicType::Float => DbrGraphics::Float {
                units: String::new(),
                limits: Limits::default(),
                precision: 0,
            },
            DbrBasicType::Double => DbrGraphics::Double {
                units: String::new(),
                limits: Limits::default(),
                precision: 0,
            },
        }
    }
    fn to_bytes(&self) -> Vec<u8> {
        match self {
            DbrGraphics::Enum => todo!(),
            DbrGraphics::String => todo!(),
            DbrGraphics::Char { units, limits } => {
                let mut units = string_to_fixed_length_bytes(units, 7);
                units.resize(MAX_UNITS_SIZE, 0u8);
                units.append(&mut limits.to_be_bytes());
                units
            }
            DbrGraphics::Int { units, limits } => {
                let mut units = string_to_fixed_length_bytes(units, 7);
                units.resize(MAX_UNITS_SIZE, 0u8);
                units.append(&mut limits.to_be_bytes());
                units
            }
            DbrGraphics::Long { units, limits } => {
                let mut units = string_to_fixed_length_bytes(units, 7);
                units.resize(MAX_UNITS_SIZE, 0u8);
                units.append(&mut limits.to_be_bytes());
                units
            }
            DbrGraphics::Float {
                units,
                limits,
                precision,
            } => {
                let mut units = string_to_fixed_length_bytes(units, 7);
                units.resize(MAX_UNITS_SIZE, 0u8);
                units.append(&mut limits.to_be_bytes());
                let mut out = precision.to_be_bytes().to_vec();
                out.append(&mut units);
                out
            }
            DbrGraphics::Double {
                units,
                limits,
                precision,
            } => {
                let mut units = string_to_fixed_length_bytes(units, 7);
                units.resize(MAX_UNITS_SIZE, 0u8);
                units.append(&mut limits.to_be_bytes());
                let mut out = precision.to_be_bytes().to_vec();
                out.append(&mut units);
                out
            }
        }
    }
}
#[derive(Clone, Debug)]
pub enum DbrControl {
    Enum,
    String,
    Char(i8, i8),
    Int(i16, i16),
    Long(i32, i32),
    Float(f32, f32),
    Double(f64, f64),
}

impl DbrControl {
    fn default_for(kind: DbrBasicType) -> Self {
        match kind {
            DbrBasicType::String => todo!(),
            DbrBasicType::Enum => todo!(),
            DbrBasicType::Int => DbrControl::Int(i16::MIN, i16::MAX),
            DbrBasicType::Float => DbrControl::Float(f32::MIN, f32::MAX),
            DbrBasicType::Char => DbrControl::Char(i8::MIN, i8::MAX),
            DbrBasicType::Long => DbrControl::Long(i32::MIN, i32::MAX),
            DbrBasicType::Double => DbrControl::Double(f64::MIN, f64::MAX),
        }
    }
    fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            DbrControl::Enum => todo!(),
            DbrControl::String => todo!(),
            DbrControl::Char(l, u) => [l, u].iter().flat_map(|v| v.to_be_bytes()).collect(),
            DbrControl::Int(l, u) => [l, u].iter().flat_map(|v| v.to_be_bytes()).collect(),
            DbrControl::Long(l, u) => [l, u].iter().flat_map(|v| v.to_be_bytes()).collect(),
            DbrControl::Float(l, u) => [l, u].iter().flat_map(|v| v.to_be_bytes()).collect(),
            DbrControl::Double(l, u) => [l, u].iter().flat_map(|v| v.to_be_bytes()).collect(),
        }
    }
}
/// Basic DBR Data types, independent of category
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DbrBasicType {
    String = 0,
    Int = 1,
    Float = 2,
    Enum = 3,
    Char = 4,
    Long = 5,
    Double = 6,
}
impl TryFrom<u16> for DbrBasicType {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::String as u16 => Ok(Self::String),
            x if x == Self::Int as u16 => Ok(Self::Int),
            x if x == Self::Float as u16 => Ok(Self::Float),
            x if x == Self::Enum as u16 => Ok(Self::Enum),
            x if x == Self::Char as u16 => Ok(Self::Char),
            x if x == Self::Long as u16 => Ok(Self::Long),
            x if x == Self::Double as u16 => Ok(Self::Double),
            _ => Err(()),
        }
    }
}

/// Marks a type as being convertible to a DBRValue representation
pub trait IntoDbrBasicType {
    fn get_dbr_basic_type() -> DbrBasicType;
}

macro_rules! impl_into_dbr_basic_type {
    ($t:ty, $variant:ident) => {
        impl IntoDbrBasicType for $t {
            fn get_dbr_basic_type() -> DbrBasicType {
                DbrBasicType::$variant
            }
        }
    };
}

impl_into_dbr_basic_type!(i8, Char);
impl_into_dbr_basic_type!(u8, Int);
impl_into_dbr_basic_type!(i16, Int);
impl_into_dbr_basic_type!(u16, Long);
impl_into_dbr_basic_type!(i32, Long);
impl_into_dbr_basic_type!(f32, Float);
impl_into_dbr_basic_type!(f64, Double);
impl_into_dbr_basic_type!(String, String);

/// Mapping of DBR categories
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DbrCategory {
    Basic = 0,
    Status = 1,
    Time = 2,
    Graphics = 3,
    Control = 4,
    /// The special single-valued DBR_CLASS_NAME
    ClassName = 8,
}
impl TryFrom<u16> for DbrCategory {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::Basic as u16 => Ok(Self::Basic),
            x if x == Self::Status as u16 => Ok(Self::Status),
            x if x == Self::Time as u16 => Ok(Self::Time),
            x if x == Self::Graphics as u16 => Ok(Self::Graphics),
            x if x == Self::Control as u16 => Ok(Self::Control),
            38 => Ok(Self::ClassName),
            _ => Err(()),
        }
    }
}

/// Represent and translate from ID every possible combination of `DBR_*_*`
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DbrType {
    pub basic_type: DbrBasicType,
    pub category: DbrCategory,
}

pub const DBR_BASIC_STRING: DbrType = DbrType {
    basic_type: DbrBasicType::String,
    category: DbrCategory::Basic,
};

pub const DBR_CLASS_NAME: DbrType = DbrType {
    basic_type: DbrBasicType::String,
    category: DbrCategory::ClassName,
};

pub const DBR_BASIC_INT: DbrType = DbrType {
    basic_type: DbrBasicType::Int,
    category: DbrCategory::Basic,
};

impl TryFrom<u16> for DbrType {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            38 => Ok(DBR_CLASS_NAME),
            value if value < 38 => Ok(Self {
                basic_type: (value % 7).try_into()?,
                category: (value / 7).try_into()?,
            }),
            _ => Err(()),
        }
    }
}

impl From<DbrType> for u16 {
    fn from(value: DbrType) -> Self {
        match value {
            DBR_CLASS_NAME => 38,
            value => value.category as u16 * 7 + value.basic_type as u16,
        }
    }
}

impl DbrType {
    /// Give the lookup for the padding for each DBR type
    ///
    /// When encoding a return packet, there is a datatype-specific
    /// padding to be inserted between the metadata about the value and
    /// the actual value itself. This is given as a lookup table rather
    /// than a calculations.
    ///
    /// See <https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types>
    pub fn get_metadata_padding(&self) -> usize {
        match (self.category, self.basic_type) {
            (DbrCategory::Status, DbrBasicType::Char) => 1,
            (DbrCategory::Status, DbrBasicType::Double) => 4,
            (DbrCategory::Time, DbrBasicType::Int) => 2,
            (DbrCategory::Time, DbrBasicType::Enum) => 2,
            (DbrCategory::Time, DbrBasicType::Char) => 3,
            (DbrCategory::Time, DbrBasicType::Double) => 4,
            (DbrCategory::Graphics, DbrBasicType::Float) => 2,
            (DbrCategory::Graphics, DbrBasicType::Char) => 1,
            (DbrCategory::Control, DbrBasicType::Char) => 1,
            _ => 0,
        }
    }
    fn new(basic_type: DbrBasicType, category: DbrCategory) -> Self {
        Self {
            basic_type,
            category,
        }
    }
}

impl FromStr for DbrType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.to_uppercase();
        let mut s: &str = &upper;
        if s.starts_with("DBR_") {
            s = &s[4..];
        };
        let category = if s.contains("_") {
            let cats = &s[..s.find("_").unwrap()];
            s = &s[s.find("_").unwrap() + 1..];
            match cats {
                "BASIC" => DbrCategory::Basic,
                "STS" => DbrCategory::Status,
                "TIME" => DbrCategory::Time,
                "GR" => DbrCategory::Graphics,
                "CTRL" => DbrCategory::Control,
                "CLASS" => DbrCategory::ClassName,
                _ => return Err(()),
            }
        } else {
            DbrCategory::Basic
        };
        let kind = match s {
            "STRING" => DbrBasicType::String,
            "INT" => DbrBasicType::Int,
            "SHORT" => DbrBasicType::Int,
            "FLOAT" => DbrBasicType::Float,
            "ENUM" => DbrBasicType::Enum,
            "CHAR" => DbrBasicType::Char,
            "LONG" => DbrBasicType::Long,
            "DOUBLE" => DbrBasicType::Double,
            "NAME" if category == DbrCategory::ClassName => DbrBasicType::String,
            _ => return Err(()),
        };
        if matches!(category, DbrCategory::ClassName) && !matches!(kind, DbrBasicType::String) {
            // Class name is _only_ CLASS_NAME
            return Err(());
        }
        Ok(DbrType {
            basic_type: kind,
            category,
        })
    }
}

/// Represent alarm status of the record
#[derive(Copy, Clone, Debug, Default)]
pub struct Status {
    pub status: i16,
    pub severity: i16,
}

/// Structured unit of exchange for records in the CA protocol
#[derive(Clone, Debug)]
pub enum Dbr {
    /// Value only, with no metadata
    Basic(DbrValue),
    /// Alarm status metadata alongside the record value
    Status {
        status: Status,
        value: DbrValue,
    },
    /// Timestamp, alarm status, and value
    Time {
        status: Status,
        timestamp: SystemTime,
        value: DbrValue,
    },
    Graphics {
        status: Status,
        graphics: DbrGraphics,
        value: DbrValue,
    },
    Control {
        status: Status,
        graphics: DbrGraphics,
        control: DbrControl,
        value: DbrValue,
    },
    ClassName(DbrValue),
}

impl Dbr {
    pub fn take_value(self) -> DbrValue {
        match self {
            Dbr::Basic(value) => value,
            Dbr::Status { value, .. } => value,
            Dbr::Time { value, .. } => value,
            Dbr::Graphics { value, .. } => value,
            Dbr::Control { value, .. } => value,
            Dbr::ClassName(value) => value,
        }
    }
    /// Retrieve the [`DbrValue`] contained by this DBR
    pub fn value(&self) -> &DbrValue {
        match self {
            Dbr::Basic(value) => value,
            Dbr::Status { status: _, value } => value,
            Dbr::Time {
                status: _,
                timestamp: _,
                value,
            } => value,
            Dbr::Graphics { value, .. } => value,
            Dbr::Control { value, .. } => value,
            Dbr::ClassName(value) => value,
        }
    }
    /// If a DBR type encoding alarm status, fetch that
    pub fn status(&self) -> Option<Status> {
        match self {
            Dbr::Basic(_) => None,
            Dbr::Status { status, .. } => Some(*status),
            Dbr::Time { status, .. } => Some(*status),
            Dbr::Graphics { status, .. } => Some(*status),
            Dbr::Control { status, .. } => Some(*status),
            Dbr::ClassName(_) => None,
        }
    }
    pub fn data_type(&self) -> DbrType {
        match self {
            Dbr::Basic(value) => DbrType {
                basic_type: value.get_type(),
                category: DbrCategory::Basic,
            },
            Dbr::Status { status: _, value } => DbrType {
                basic_type: value.get_type(),
                category: DbrCategory::Status,
            },
            Dbr::Time {
                status: _,
                timestamp: _,
                value,
            } => DbrType {
                basic_type: value.get_type(),
                category: DbrCategory::Time,
            },
            Dbr::Graphics { value, .. } => DbrType {
                basic_type: value.get_type(),
                category: DbrCategory::Graphics,
            },
            Dbr::Control { value, .. } => DbrType {
                basic_type: value.get_type(),
                category: DbrCategory::Control,
            },
            Dbr::ClassName(_) => DBR_CLASS_NAME,
        }
    }

    pub fn from_bytes(
        data_type: DbrType,
        data_count: usize,
        data: &[u8],
    ) -> Result<Dbr, nom::Err<nom::error::Error<&[u8]>>> {
        if data_type.category == DbrCategory::Control {
            todo!("Don't understand CTRL structure usage well enough to parse yet");
        }
        if data_type.category == DbrCategory::Graphics {
            todo!("Don't understand GR structure usage well enough to parse yet");
        }

        let (data, status) = if data_type.category != DbrCategory::Basic {
            let (d, (status, severity)) = (be_i16, be_i16).parse(data)?;
            (d, Some(Status { status, severity }))
        } else {
            (data, None)
        };

        let (data, timestamp) = if data_type.category == DbrCategory::Time {
            let (input, (time_s, time_ns)) = (be_i32, be_u32).parse(data)?;
            (
                input,
                Some(
                    UNIX_EPOCH
                        .checked_add(Duration::new(time_s as u64 + 631152000u64, time_ns))
                        .unwrap(),
                ),
            )
        } else {
            (data, None)
        };

        // Offset the read buffer to account for metadata padding
        let data = &data[data_type.get_metadata_padding()..];
        let value = DbrValue::decode_value(data_type.basic_type, data_count, data)?;

        Ok(match data_type.category {
            DbrCategory::Basic => Dbr::Basic(value),
            DbrCategory::Status => Dbr::Status {
                status: status.unwrap(),
                value,
            },
            DbrCategory::Time => Dbr::Time {
                status: status.unwrap(),
                timestamp: timestamp.unwrap(),
                value,
            },
            DbrCategory::Graphics => todo!(),
            DbrCategory::Control => todo!(),
            DbrCategory::ClassName => Dbr::ClassName(value),
        })
    }

    pub fn to_bytes(&self, max_elems: Option<NonZeroUsize>) -> (usize, Vec<u8>) {
        let mut buffer = Cursor::new(Vec::new());
        let real_count = self.write_be(&mut buffer, max_elems).unwrap();
        (real_count, buffer.into_inner())
    }

    /// Write a requested number of elements to a stream
    ///
    /// Return the actual number of elements written
    pub fn write_be<W: io::Write>(
        &self,
        writer: &mut W,
        max_elems: Option<NonZeroUsize>,
    ) -> io::Result<usize> {
        let (real_elems, data) = self.value().to_bytes(max_elems);
        // All except Basic write status/severity
        if let Some(status) = self.status() {
            writer.write_all(&status.status.to_be_bytes())?;
            writer.write_all(&status.severity.to_be_bytes())?;
        }
        match self {
            Dbr::Time { timestamp, .. } => {
                let unix_time = timestamp.duration_since(UNIX_EPOCH).unwrap();
                let time_s = unix_time.as_secs() as i32 - 631152000i32;
                let time_ns = unix_time.subsec_nanos();
                writer.write_all(&time_s.to_be_bytes())?;
                writer.write_all(&time_ns.to_be_bytes())?;
            }
            Dbr::Graphics { graphics, .. } => {
                writer.write_all(&graphics.to_bytes())?;
            }
            Dbr::Control {
                graphics, control, ..
            } => {
                writer.write_all(&graphics.to_bytes())?;
                writer.write_all(&control.to_be_bytes())?;
            }
            _ => (),
        }

        writer.write_all(&vec![0u8; self.data_type().get_metadata_padding()])?;
        writer.write_all(&data)?;
        Ok(real_elems)
    }

    pub fn convert_to(&self, dbr_type: DbrType) -> Result<Dbr, ErrorCondition> {
        let value = self.value().convert_to(dbr_type.basic_type)?;
        // First handle category changes - we can do this for some but not all
        Ok(match self {
            Dbr::Basic(_) => match dbr_type.category {
                DbrCategory::Basic => Dbr::Basic(value),
                DbrCategory::Status => Dbr::Status {
                    status: Status::default(),
                    value,
                },
                DbrCategory::Time => Dbr::Time {
                    status: Status::default(),
                    timestamp: SystemTime::now(),
                    value,
                },
                DbrCategory::Graphics => Dbr::Graphics {
                    status: Status::default(),
                    graphics: DbrGraphics::default_for(value.get_type()),
                    value,
                },
                DbrCategory::Control => Dbr::Control {
                    status: Status::default(),
                    graphics: DbrGraphics::default_for(value.get_type()),
                    control: DbrControl::default_for(value.get_type()),
                    value,
                },
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Status { status, .. } => match dbr_type.category {
                DbrCategory::Basic => Dbr::Basic(value),
                DbrCategory::Status => Dbr::Status {
                    status: *status,
                    value,
                },
                DbrCategory::Time => Dbr::Time {
                    status: *status,
                    timestamp: SystemTime::now(),
                    value,
                },
                DbrCategory::Graphics => Dbr::Graphics {
                    status: *status,
                    graphics: DbrGraphics::default_for(value.get_type()),
                    value,
                },
                DbrCategory::Control => Dbr::Control {
                    status: *status,
                    graphics: DbrGraphics::default_for(value.get_type()),
                    control: DbrControl::default_for(value.get_type()),
                    value,
                },
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Time {
                status,
                timestamp: ts,
                value: _,
            } => match dbr_type.category {
                DbrCategory::Basic => Dbr::Basic(value),
                DbrCategory::Status => Dbr::Status {
                    status: *status,
                    value,
                },
                DbrCategory::Time => Dbr::Time {
                    status: *status,
                    timestamp: *ts,
                    value,
                },
                DbrCategory::Graphics => Dbr::Graphics {
                    status: *status,
                    graphics: DbrGraphics::default_for(value.get_type()),
                    value,
                },
                DbrCategory::Control => Dbr::Control {
                    status: *status,
                    graphics: DbrGraphics::default_for(value.get_type()),
                    control: DbrControl::default_for(value.get_type()),
                    value,
                },
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Graphics { .. } => {
                todo!("Implemented Graphics but don't need to convert from, yet")
            }
            Dbr::Control { .. } => todo!("Implemented Control but don't need to convert from, yet"),
            // ClassName cannot be converted as it isn't a normal form of data
            Dbr::ClassName(_) => match dbr_type.category {
                DbrCategory::ClassName => Dbr::ClassName(value),
                _ => return Err(ErrorCondition::NoConvert),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    #[test]
    fn single_or_vec() {
        let v: DbrValue = vec![500i32].into();
        assert!(v.convert_to(DbrBasicType::Int).is_ok());
        assert!(v.convert_to(DbrBasicType::Char).is_err());
        assert_eq!(v.to_bytes(None).1, vec![0x00, 0x00, 0x01, 0xF4]);
        assert_eq!(
            v.convert_to(DbrBasicType::Int).unwrap().to_bytes(None).1,
            vec![0x01, 0xF4]
        );

        let data = vec![500.23f32, 12.7f32];
        let v: DbrValue = data.clone().into();
        assert_eq!(v.get_count(), 2);
        assert_eq!(
            v.to_bytes(None).1,
            data.iter()
                .flat_map(|v| v.to_be_bytes())
                .collect::<Vec<u8>>()
        );
        assert_eq!(
            v.to_bytes(NonZeroUsize::new(1)).1,
            data.iter()
                .take(1)
                .flat_map(|v| v.to_be_bytes())
                .collect::<Vec<u8>>()
        );
        // Try converting this to an int with truncation
        let v = v.convert_to(DbrBasicType::Int).unwrap();
        assert_eq!(v.to_bytes(None).1, vec![0x01, 0xf4, 0x00, 0x0c]);

        assert_eq!(
            DbrValue::Float(vec![455.9f32])
                .convert_to(DbrBasicType::Long)
                .unwrap()
                .to_bytes(NonZeroUsize::new(5))
                .1,
            vec![0x00, 0x00, 0x01, 0xc7]
        );
    }

    #[test]
    fn encode_dbr() {
        let example_packet = [
            0x0, 0x0, 0x0, 0x0, 0x42, 0x32, 0x19, 0x99, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2a,
        ];
        let dbr = Dbr::Time {
            status: Status::default(),
            timestamp: SystemTime::UNIX_EPOCH
                .checked_add(Duration::from_secs(1741731609))
                .unwrap(),
            value: vec![42i32].into(),
        };

        let (_size, out_data) = dbr
            .convert_to(DbrType {
                basic_type: DbrBasicType::Long,
                category: DbrCategory::Time,
            })
            .unwrap()
            .to_bytes(None);
        assert_eq!(out_data.len(), example_packet.len());
        assert_eq!(out_data, example_packet);
    }

    #[test]
    fn test_string_to_char() {
        let test_string = "a test string".to_string();
        let s = DbrValue::String(vec![test_string.clone()]);
        let as_char = s.convert_to(DbrBasicType::Char).unwrap();
        let re_s = as_char.convert_to(DbrBasicType::String).unwrap();

        assert_eq!(s, re_s);
    }

    #[test]
    fn test_dbr_string_conversions() {
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Basic),
            "INT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Status),
            "DBR_STS_INT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Time),
            "TIME_INT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Graphics),
            "DBR_GR_INT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Control),
            "DBR_CTRL_INT".parse().unwrap()
        );

        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Basic),
            "INT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::String, DbrCategory::Graphics),
            "GR_STRING".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Int, DbrCategory::Basic),
            "SHORT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Float, DbrCategory::Basic),
            "FLOAT".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Long, DbrCategory::Basic),
            "LONG".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Enum, DbrCategory::Basic),
            "ENUM".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Char, DbrCategory::Basic),
            "CHAR".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::Double, DbrCategory::Basic),
            "DOUBLE".parse().unwrap()
        );
        assert_eq!(
            DbrType::new(DbrBasicType::String, DbrCategory::ClassName),
            "DBR_CLASS_NAME".parse().unwrap()
        );
        assert!("DBR_CLASS_INT".parse::<DbrType>().is_err());
    }
}
