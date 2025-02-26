#![allow(dead_code)]

use std::{collections::HashMap, time::Instant};

#[derive(Default, Debug)]
pub struct Limits<T> {
    upper: Option<T>,
    lower: Option<T>,
}

#[derive(Default, Debug)]
pub struct LimitSet<T> {
    display_limits: Limits<T>,
    warning_limits: Limits<T>,
    alarm_limits: Limits<T>,
}

#[derive(Clone, Debug)]
pub enum SingleOrVec<T> {
    Single(T),
    Vector(Vec<T>),
}

#[derive(Debug)]
pub struct NumericDBR<T> {
    pub status: i16,
    pub severity: i16,
    /// Only makes sense for FLOAT/DOUBLE, here to try and avoid duplication
    pub precision: Option<u16>,
    pub units: String,
    pub limits: LimitSet<T>,
    pub value: SingleOrVec<T>,
    pub last_updated: Instant,
}
impl<T> NumericDBR<T> {
    fn get_count(&self) -> usize {
        match &self.value {
            &SingleOrVec::Single(_) => 1,
            SingleOrVec::Vector(v) => v.len(),
        }
    }
}
impl<T> Default for NumericDBR<T>
where
    T: Default,
{
    fn default() -> Self {
        Self {
            status: Default::default(),
            severity: Default::default(),
            precision: Default::default(),
            units: Default::default(),
            limits: Default::default(),
            value: SingleOrVec::Single(T::default()),
            last_updated: Instant::now(),
        }
    }
}
#[derive(Debug)]
pub struct StringDBR {
    status: i16,
    severity: i16,
    value: String,
}
#[derive(Debug)]
pub struct EnumDBR {
    status: i16,
    severity: i16,
    strings: HashMap<u16, String>,
    value: u16,
}
#[derive(Debug)]
pub enum Dbr {
    Enum(EnumDBR),
    String(StringDBR),
    Char(NumericDBR<i8>),
    Int(NumericDBR<i16>),
    Long(NumericDBR<i32>),
    Float(NumericDBR<f32>),
    Double(NumericDBR<f64>),
}
impl Dbr {
    // fn value_default(&self) -> DbrValue {
    //     match self {
    //         Dbr::Enum(_) => DbrValue::Enum(0),
    //         Dbr::String(_) => DbrValue::String(String::new()),
    //         Dbr::Char(_) => DbrValue::Char(0),
    //         Dbr::Int(_) => DbrValue::Int(0),
    //         Dbr::Long(_) => DbrValue::Long(0),
    //         Dbr::Float(_) => DbrValue::Float(0.0),
    //         Dbr::Double(_) => DbrValue::Double(0.0),
    //     }
    // }
    pub fn get_count(&self) -> usize {
        match self {
            Dbr::Enum(_) => 1,
            Dbr::String(_) => 1,
            Dbr::Char(dbr) => dbr.get_count(),
            Dbr::Int(dbr) => dbr.get_count(),
            Dbr::Long(dbr) => dbr.get_count(),
            Dbr::Float(dbr) => dbr.get_count(),
            Dbr::Double(dbr) => dbr.get_count(),
        }
    }
    pub fn get_value(&self) -> DbrValue {
        match self {
            Dbr::Enum(dbr) => DbrValue::Enum(dbr.value),
            Dbr::String(dbr) => DbrValue::String(dbr.value.clone()),
            Dbr::Char(dbr) => DbrValue::Char(dbr.value.clone()),
            Dbr::Int(dbr) => DbrValue::Int(dbr.value.clone()),
            Dbr::Long(dbr) => DbrValue::Long(dbr.value.clone()),
            Dbr::Float(dbr) => DbrValue::Float(dbr.value.clone()),
            Dbr::Double(dbr) => DbrValue::Double(dbr.value.clone()),
        }
    }
    pub fn get_native_type(&self) -> DBRType {
        DBRType {
            basic_type: match self {
                Dbr::Enum(_) => DBRBasicType::Enum,
                Dbr::String(_) => DBRBasicType::String,
                Dbr::Char(_) => DBRBasicType::Char,
                Dbr::Int(_) => DBRBasicType::Int,
                Dbr::Long(_) => DBRBasicType::Long,
                Dbr::Float(_) => DBRBasicType::Float,
                Dbr::Double(_) => DBRBasicType::Double,
            },
            category: DBRCategory::Basic,
        }
    }
}

#[derive(Clone, Debug)]
pub enum DbrValue {
    Enum(u16),
    String(String),
    Char(SingleOrVec<i8>),
    Int(SingleOrVec<i16>),
    Long(SingleOrVec<i32>),
    Float(SingleOrVec<f32>),
    Double(SingleOrVec<f64>),
}

/// Basic DBR Data types, independent of category
#[derive(Debug, Copy, Clone)]
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
#[derive(Debug, Copy, Clone)]
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

#[derive(Debug, Copy, Clone)]
pub struct DBRType {
    pub basic_type: DBRBasicType,
    pub category: DBRCategory,
}

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
