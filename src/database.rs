#![allow(dead_code)]

use nom::{
    multi::count,
    number::complete::{be_f32, be_f64, be_i16, be_i32, be_i8, be_u16, be_u32},
    Parser,
};
use num::{traits::ToBytes, NumCast};
use std::{
    cmp,
    convert::TryFrom,
    fmt::Debug,
    io::{self, Cursor},
    num::NonZeroUsize,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::messages::ErrorCondition;

#[derive(Clone, Debug)]
pub enum DbrValue {
    Enum(u16),
    String(String),
    Char(Vec<i8>),
    Int(Vec<i16>),
    Long(Vec<i32>),
    Float(Vec<f32>),
    Double(Vec<f64>),
}

impl DbrValue {
    pub fn get_count(&self) -> usize {
        match self {
            DbrValue::Enum(_) => 1,
            DbrValue::String(_) => unimplemented!("Don't know if string arrays are supported"),
            DbrValue::Char(val) => val.len(),
            DbrValue::Int(val) => val.len(),
            DbrValue::Long(val) => val.len(),
            DbrValue::Float(val) => val.len(),
            DbrValue::Double(val) => val.len(),
        }
    }
    pub fn get_type(&self) -> DBRBasicType {
        match self {
            DbrValue::Enum(_) => DBRBasicType::Enum,
            DbrValue::String(_) => DBRBasicType::String,
            DbrValue::Char(_) => DBRBasicType::Char,
            DbrValue::Int(_) => DBRBasicType::Int,
            DbrValue::Long(_) => DBRBasicType::Long,
            DbrValue::Float(_) => DBRBasicType::Float,
            DbrValue::Double(_) => DBRBasicType::Double,
        }
    }
    pub fn convert_to(&self, basic_type: DBRBasicType) -> Result<DbrValue, ErrorCondition> {
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

        Ok(match basic_type {
            DBRBasicType::Char => match self {
                DbrValue::Char(_val) => self.clone(),
                DbrValue::Int(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Char(_try_convert_vec(val)?),
                DbrValue::String(_) => return Err(ErrorCondition::NoConvert),
                DbrValue::Enum(val) => {
                    DbrValue::Char(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DBRBasicType::Int => match self {
                DbrValue::Char(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Int(_val) => self.clone(),
                DbrValue::Long(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Int(_try_convert_vec(val)?),
                DbrValue::String(_) => return Err(ErrorCondition::NoConvert),
                DbrValue::Enum(val) => {
                    DbrValue::Int(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DBRBasicType::Long => match self {
                DbrValue::Char(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Long(_val) => self.clone(),
                DbrValue::Float(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::Double(val) => DbrValue::Long(_try_convert_vec(val)?),
                DbrValue::String(_) => return Err(ErrorCondition::NoConvert),
                DbrValue::Enum(val) => {
                    DbrValue::Long(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DBRBasicType::Float => match self {
                DbrValue::Char(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::Float(_val) => self.clone(),
                DbrValue::Double(val) => DbrValue::Float(_try_convert_vec(val)?),
                DbrValue::String(_) => return Err(ErrorCondition::NoConvert),
                DbrValue::Enum(val) => {
                    DbrValue::Float(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DBRBasicType::Double => match self {
                DbrValue::Char(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Int(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Long(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Float(val) => DbrValue::Double(_try_convert_vec(val)?),
                DbrValue::Double(_val) => self.clone(),
                DbrValue::String(_) => return Err(ErrorCondition::NoConvert),
                DbrValue::Enum(val) => {
                    DbrValue::Double(vec![NumCast::from(*val).ok_or(ErrorCondition::NoConvert)?])
                }
            },
            DBRBasicType::String => match self {
                DbrValue::String(_) => self.clone(),
                _ => return Err(ErrorCondition::UnavailInServ),
            },
            DBRBasicType::Enum => match self {
                DbrValue::Enum(_val) => self.clone(),
                _ => return Err(ErrorCondition::NoConvert),
            },
        })
    }

    /// Encode the value contents of a DBR into a byte vector
    ///
    /// If max_elems is zero, then no data will be returned. If it is
    /// `None`, then all data will be returned.
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
                DbrValue::String(_) => unimplemented!(),
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
        data_type: DBRBasicType,
        item_count: usize,
        data: &[u8],
    ) -> Result<DbrValue, nom::Err<nom::error::Error<&[u8]>>> {
        match data_type {
            DBRBasicType::Enum => {
                assert!(
                    item_count == 1,
                    "Multiple item count makes no sense for enum"
                );
                Ok(DbrValue::Enum(be_u16.parse(data)?.1))
            }
            DBRBasicType::String => {
                let strings: Vec<&str> = data
                    .chunks(40)
                    .map(|d| {
                        let strlen = d.iter().position(|&c| c == 0x00).unwrap();
                        str::from_utf8(&d[0..strlen]).unwrap()
                    })
                    .collect();
                assert!(
                    strings.len() == 1,
                    "Got multi-instance string data type, thought this could not happen"
                );
                Ok(DbrValue::String(strings[0].to_owned()))
            }
            DBRBasicType::Char => Ok(DbrValue::Char(count(be_i8, item_count).parse(data)?.1)),
            DBRBasicType::Int => Ok(DbrValue::Int(count(be_i16, item_count).parse(data)?.1)),
            DBRBasicType::Long => Ok(DbrValue::Long(count(be_i32, item_count).parse(data)?.1)),
            DBRBasicType::Float => Ok(DbrValue::Float(count(be_f32, item_count).parse(data)?.1)),
            DBRBasicType::Double => Ok(DbrValue::Double(count(be_f64, item_count).parse(data)?.1)),
        }
    }
}

/// Implement a From<datatype> for a specific dbrvalue kind
macro_rules! impl_dbrvalue_from {
    ($variant:ident, $typ:ty) => {
        impl From<Vec<$typ>> for DbrValue {
            fn from(value: Vec<$typ>) -> Self {
                DbrValue::$variant(value)
            }
        }
    };
}
impl_dbrvalue_from!(Char, i8);
impl_dbrvalue_from!(Int, i16);
impl_dbrvalue_from!(Long, i32);
impl_dbrvalue_from!(Float, f32);
impl_dbrvalue_from!(Double, f64);

/// Basic DBR Data types, independent of category
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DBRBasicType {
    String = 0,
    Int = 1,
    Float = 2,
    Enum = 3,
    Char = 4,
    Long = 5,
    Double = 6,
}
impl TryFrom<u16> for DBRBasicType {
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
/// Mapping of DBR categories
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DBRCategory {
    Basic = 0,
    Status = 1,
    Time = 2,
    Graphics = 3,
    Control = 4,
}
impl TryFrom<u16> for DBRCategory {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            x if x == Self::Basic as u16 => Ok(Self::Basic),
            x if x == Self::Status as u16 => Ok(Self::Status),
            x if x == Self::Time as u16 => Ok(Self::Time),
            x if x == Self::Graphics as u16 => Ok(Self::Graphics),
            x if x == Self::Control as u16 => Ok(Self::Control),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DBRType {
    pub basic_type: DBRBasicType,
    pub category: DBRCategory,
}

pub const DBR_BASIC_STRING: DBRType = DBRType {
    basic_type: DBRBasicType::String,
    category: DBRCategory::Basic,
};

impl TryFrom<u16> for DBRType {
    type Error = ();
    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Ok(Self {
            basic_type: (value % 7).try_into()?,
            category: (value / 7).try_into()?,
        })
    }
}

impl From<DBRType> for u16 {
    fn from(value: DBRType) -> Self {
        value.category as u16 * 7 + value.basic_type as u16
    }
}

impl DBRType {
    /// Give the lookup for the padding for each DBR type
    ///
    /// When encoding a return packet, there is a datatype-specific
    /// padding to be inserted between the metadata about the value and
    /// the actual value itself. This is given as a lookup table rather
    /// than a calculations.
    ///
    /// See https://docs.epics-controls.org/en/latest/internal/ca_protocol.html#payload-data-types
    pub fn get_metadata_padding(&self) -> usize {
        match (self.category, self.basic_type) {
            (DBRCategory::Status, DBRBasicType::Char) => 1,
            (DBRCategory::Status, DBRBasicType::Double) => 4,
            (DBRCategory::Time, DBRBasicType::Int) => 2,
            (DBRCategory::Time, DBRBasicType::Enum) => 2,
            (DBRCategory::Time, DBRBasicType::Char) => 3,
            (DBRCategory::Time, DBRBasicType::Double) => 4,
            (DBRCategory::Graphics, DBRBasicType::Float) => 2,
            (DBRCategory::Graphics, DBRBasicType::Char) => 1,
            (DBRCategory::Control, DBRBasicType::Char) => 1,
            _ => 0,
        }
    }
}

#[derive(Copy, Clone, Debug, Default)]
pub struct Status {
    pub status: i16,
    pub severity: i16,
}

/// Structured unit of exchange for records in the CA protocol
#[derive(Clone, Debug)]
pub enum Dbr {
    Basic(DbrValue),
    Status {
        status: Status,
        value: DbrValue,
    },
    Time {
        status: Status,
        timestamp: SystemTime,
        value: DbrValue,
    },
    Graphics,
    Control,
}

impl Dbr {
    pub fn value(&self) -> &DbrValue {
        match self {
            Dbr::Basic(value) => value,
            Dbr::Status { status: _, value } => value,
            Dbr::Time {
                status: _,
                timestamp: _,
                value,
            } => value,
            Dbr::Graphics => todo!(),
            Dbr::Control => todo!(),
        }
    }
    pub fn status(&self) -> Option<Status> {
        match self {
            Dbr::Basic(_) => None,
            Dbr::Status { status, .. } => Some(*status),
            Dbr::Time { status, .. } => Some(*status),
            Dbr::Graphics => todo!(),
            Dbr::Control => todo!(),
        }
    }
    pub fn data_type(&self) -> DBRType {
        match self {
            Dbr::Basic(value) => DBRType {
                basic_type: value.get_type(),
                category: DBRCategory::Basic,
            },
            Dbr::Status { status: _, value } => DBRType {
                basic_type: value.get_type(),
                category: DBRCategory::Status,
            },
            Dbr::Time {
                status: _,
                timestamp: _,
                value,
            } => DBRType {
                basic_type: value.get_type(),
                category: DBRCategory::Time,
            },
            Dbr::Graphics => todo!(),
            Dbr::Control => todo!(),
        }
    }

    pub fn from_bytes(
        data_type: DBRType,
        data_count: usize,
        data: &[u8],
    ) -> Result<Dbr, nom::Err<nom::error::Error<&[u8]>>> {
        if data_type.category == DBRCategory::Control {
            todo!("Don't understand CTRL structure usage well enough to parse yet");
        }
        if data_type.category == DBRCategory::Graphics {
            todo!("Don't understand GR structure usage well enough to parse yet");
        }

        let (data, status) = if data_type.category != DBRCategory::Basic {
            let (d, (status, severity)) = (be_i16, be_i16).parse(data)?;
            (d, Some(Status { status, severity }))
        } else {
            (data, None)
        };

        let (data, timestamp) = if data_type.category == DBRCategory::Time {
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
            DBRCategory::Basic => Dbr::Basic(value),
            DBRCategory::Status => Dbr::Status {
                status: status.unwrap(),
                value,
            },
            DBRCategory::Time => Dbr::Time {
                status: status.unwrap(),
                timestamp: timestamp.unwrap(),
                value,
            },
            DBRCategory::Graphics => todo!(),
            DBRCategory::Control => todo!(),
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
            Dbr::Graphics => todo!(),
            Dbr::Control => todo!(),
            _ => (),
        }

        writer.write_all(&vec![0u8; self.data_type().get_metadata_padding()])?;
        writer.write_all(&data)?;
        Ok(real_elems)
    }

    pub fn convert_to(&self, dbr_type: DBRType) -> Result<Dbr, ErrorCondition> {
        let value = self.value().convert_to(dbr_type.basic_type)?;
        // First handle category changes - we can do this for some but not all
        Ok(match self {
            Dbr::Basic(_) => match dbr_type.category {
                DBRCategory::Basic => self.clone(),
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Status { .. } => match dbr_type.category {
                DBRCategory::Basic => Dbr::Basic(value),
                DBRCategory::Status => self.clone(),
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Time {
                status,
                timestamp: _,
                value,
            } => match dbr_type.category {
                DBRCategory::Basic => Dbr::Basic(value.clone()),
                DBRCategory::Status => Dbr::Status {
                    status: *status,
                    value: value.clone(),
                },
                DBRCategory::Time => self.clone(),
                _ => return Err(ErrorCondition::NoConvert),
            },
            Dbr::Graphics => todo!(),
            Dbr::Control => todo!(),
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
        assert!(v.convert_to(DBRBasicType::Int).is_ok());
        assert!(v.convert_to(DBRBasicType::Char).is_err());
        assert_eq!(v.to_bytes(None).1, vec![0x00, 0x00, 0x01, 0xF4]);
        assert_eq!(
            v.convert_to(DBRBasicType::Int).unwrap().to_bytes(None).1,
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
        let v = v.convert_to(DBRBasicType::Int).unwrap();
        assert_eq!(v.to_bytes(None).1, vec![0x01, 0xf4, 0x00, 0x0c]);

        assert_eq!(
            DbrValue::Float(vec![455.9f32])
                .convert_to(DBRBasicType::Long)
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
            .convert_to(DBRType {
                basic_type: DBRBasicType::Long,
                category: DBRCategory::Time,
            })
            .unwrap()
            .to_bytes(None);
        assert_eq!(out_data.len(), example_packet.len());
        assert_eq!(out_data, example_packet);
    }
}
