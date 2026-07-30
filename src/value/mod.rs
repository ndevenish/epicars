//! A protocol-neutral value model, and its separate introspection.
//!
//! [`crate::dbr`] is simultaneously the Channel Access wire format and this crate's
//! data model. That conflation cannot be carried into pvAccess, whose type system is
//! strictly larger: booleans, unsigned integers, 64-bit integers, nested structures,
//! unions and arrays of structures all have no CA representation. This module is the
//! neutral model that both protocols convert to and from - see [`ca`] for the CA
//! adapters.
//!
//! Two types, deliberately kept apart:
//!
//! - [`Value`] holds data, recursively.
//! - [`Field`] describes *shape* without carrying any data.
//!
//! Splitting them is not tidiness. pvAccess transmits the introspection (`FieldDesc`)
//! separately from the data and caches it per connection, so a type that welds the two
//! together - as [`crate::dbr::DbrType`] does for CA, where "introspection" is a single
//! `u16` computed by arithmetic - cannot express the protocol. Introspection can
//! usually be *derived* from a value ([`Value::derive_field`]) but that is a
//! convenience, not the relationship between the types.
//!
//! ```
//! use epicars::value::{Structure, Value};
//!
//! // A nested structure holding an unsigned 64-bit array - inexpressible in DBR.
//! let value = Value::from(
//!     Structure::with_id("epics:nt/NTScalarArray:1.0")
//!         .with("value", vec![1u64, 2, u64::MAX])
//!         .with("nested", Structure::new().with("count", 3i32)),
//! );
//!
//! assert_eq!(
//!     value.get_path("nested.count"),
//!     Some(&Value::from(3i32))
//! );
//! ```

pub mod ca;
pub mod meta;

/// The twelve primitive types a [`Scalar`] or [`ScalarArray`] can hold.
///
/// This is the pvData set. CA covers only `Byte`, `Short`, `Int`, `Float`, `Double`,
/// `String`, plus `UShort` as its enum index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ScalarType {
    Bool,
    Byte,
    UByte,
    Short,
    UShort,
    Int,
    UInt,
    Long,
    ULong,
    Float,
    Double,
    String,
}

impl ScalarType {
    /// The pvData name for this type, as used in introspection output.
    pub fn name(&self) -> &'static str {
        match self {
            ScalarType::Bool => "boolean",
            ScalarType::Byte => "byte",
            ScalarType::UByte => "ubyte",
            ScalarType::Short => "short",
            ScalarType::UShort => "ushort",
            ScalarType::Int => "int",
            ScalarType::UInt => "uint",
            ScalarType::Long => "long",
            ScalarType::ULong => "ulong",
            ScalarType::Float => "float",
            ScalarType::Double => "double",
            ScalarType::String => "string",
        }
    }

    /// Whether this is one of the eight integer types.
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            ScalarType::Byte
                | ScalarType::UByte
                | ScalarType::Short
                | ScalarType::UShort
                | ScalarType::Int
                | ScalarType::UInt
                | ScalarType::Long
                | ScalarType::ULong
        )
    }

    /// Whether this is an unsigned integer type.
    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            ScalarType::UByte | ScalarType::UShort | ScalarType::UInt | ScalarType::ULong
        )
    }

    /// Whether this is `Float` or `Double`.
    pub fn is_floating(&self) -> bool {
        matches!(self, ScalarType::Float | ScalarType::Double)
    }

    /// Width in bytes of the fixed-size encodings; `None` for `String`.
    pub fn size(&self) -> Option<usize> {
        Some(match self {
            ScalarType::Bool | ScalarType::Byte | ScalarType::UByte => 1,
            ScalarType::Short | ScalarType::UShort => 2,
            ScalarType::Int | ScalarType::UInt | ScalarType::Float => 4,
            ScalarType::Long | ScalarType::ULong | ScalarType::Double => 8,
            ScalarType::String => return None,
        })
    }
}

/// A single primitive value.
#[derive(Clone, Debug, PartialEq)]
pub enum Scalar {
    Bool(bool),
    Byte(i8),
    UByte(u8),
    Short(i16),
    UShort(u16),
    Int(i32),
    UInt(u32),
    Long(i64),
    ULong(u64),
    Float(f32),
    Double(f64),
    String(String),
}

impl Scalar {
    pub fn scalar_type(&self) -> ScalarType {
        match self {
            Scalar::Bool(_) => ScalarType::Bool,
            Scalar::Byte(_) => ScalarType::Byte,
            Scalar::UByte(_) => ScalarType::UByte,
            Scalar::Short(_) => ScalarType::Short,
            Scalar::UShort(_) => ScalarType::UShort,
            Scalar::Int(_) => ScalarType::Int,
            Scalar::UInt(_) => ScalarType::UInt,
            Scalar::Long(_) => ScalarType::Long,
            Scalar::ULong(_) => ScalarType::ULong,
            Scalar::Float(_) => ScalarType::Float,
            Scalar::Double(_) => ScalarType::Double,
            Scalar::String(_) => ScalarType::String,
        }
    }

    /// The value as an `f64`, for any numeric type. `None` for `Bool` and `String`.
    ///
    /// Lossy for large 64-bit integers; this is for display and comparison, not for
    /// conversion.
    pub fn as_f64(&self) -> Option<f64> {
        Some(match self {
            Scalar::Byte(v) => *v as f64,
            Scalar::UByte(v) => *v as f64,
            Scalar::Short(v) => *v as f64,
            Scalar::UShort(v) => *v as f64,
            Scalar::Int(v) => *v as f64,
            Scalar::UInt(v) => *v as f64,
            Scalar::Long(v) => *v as f64,
            Scalar::ULong(v) => *v as f64,
            Scalar::Float(v) => *v as f64,
            Scalar::Double(v) => *v,
            Scalar::Bool(_) | Scalar::String(_) => return None,
        })
    }

    /// The contents of a `Scalar::String`, or `None` for anything else.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Scalar::String(v) => Some(v),
            _ => None,
        }
    }
}

/// A homogeneous array of primitives.
///
/// One variant per [`ScalarType`]; the element type is therefore always known, even
/// when the array is empty.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarArray {
    Bool(Vec<bool>),
    Byte(Vec<i8>),
    UByte(Vec<u8>),
    Short(Vec<i16>),
    UShort(Vec<u16>),
    Int(Vec<i32>),
    UInt(Vec<u32>),
    Long(Vec<i64>),
    ULong(Vec<u64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    String(Vec<String>),
}

impl ScalarArray {
    pub fn element_type(&self) -> ScalarType {
        match self {
            ScalarArray::Bool(_) => ScalarType::Bool,
            ScalarArray::Byte(_) => ScalarType::Byte,
            ScalarArray::UByte(_) => ScalarType::UByte,
            ScalarArray::Short(_) => ScalarType::Short,
            ScalarArray::UShort(_) => ScalarType::UShort,
            ScalarArray::Int(_) => ScalarType::Int,
            ScalarArray::UInt(_) => ScalarType::UInt,
            ScalarArray::Long(_) => ScalarType::Long,
            ScalarArray::ULong(_) => ScalarType::ULong,
            ScalarArray::Float(_) => ScalarType::Float,
            ScalarArray::Double(_) => ScalarType::Double,
            ScalarArray::String(_) => ScalarType::String,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ScalarArray::Bool(v) => v.len(),
            ScalarArray::Byte(v) => v.len(),
            ScalarArray::UByte(v) => v.len(),
            ScalarArray::Short(v) => v.len(),
            ScalarArray::UShort(v) => v.len(),
            ScalarArray::Int(v) => v.len(),
            ScalarArray::UInt(v) => v.len(),
            ScalarArray::Long(v) => v.len(),
            ScalarArray::ULong(v) => v.len(),
            ScalarArray::Float(v) => v.len(),
            ScalarArray::Double(v) => v.len(),
            ScalarArray::String(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// An empty array of the given element type.
    pub fn empty_of(element_type: ScalarType) -> ScalarArray {
        match element_type {
            ScalarType::Bool => ScalarArray::Bool(Vec::new()),
            ScalarType::Byte => ScalarArray::Byte(Vec::new()),
            ScalarType::UByte => ScalarArray::UByte(Vec::new()),
            ScalarType::Short => ScalarArray::Short(Vec::new()),
            ScalarType::UShort => ScalarArray::UShort(Vec::new()),
            ScalarType::Int => ScalarArray::Int(Vec::new()),
            ScalarType::UInt => ScalarArray::UInt(Vec::new()),
            ScalarType::Long => ScalarArray::Long(Vec::new()),
            ScalarType::ULong => ScalarArray::ULong(Vec::new()),
            ScalarType::Float => ScalarArray::Float(Vec::new()),
            ScalarType::Double => ScalarArray::Double(Vec::new()),
            ScalarType::String => ScalarArray::String(Vec::new()),
        }
    }
}

/// An ordered set of named fields, with an optional type identifier.
///
/// Field order is significant - pvAccess encodes structure members positionally, in
/// the order the introspection declares them - so this is a `Vec`, not a map. Names
/// are unique: [`Structure::insert`] replaces in place rather than appending a
/// duplicate.
///
/// The `id` is the structure's type name, e.g. `epics:nt/NTScalar:1.0`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Structure {
    id: Option<String>,
    fields: Vec<(String, Value)>,
}

impl Structure {
    /// An empty, untyped structure.
    pub fn new() -> Structure {
        Structure::default()
    }

    /// An empty structure with a type identifier, e.g. `epics:nt/NTScalar:1.0`.
    pub fn with_id(id: impl Into<String>) -> Structure {
        Structure {
            id: Some(id.into()),
            fields: Vec::new(),
        }
    }

    /// Builder form of [`Structure::insert`].
    pub fn with(mut self, name: impl Into<String>, value: impl Into<Value>) -> Structure {
        self.insert(name, value);
        self
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn set_id(&mut self, id: Option<String>) {
        self.id = id;
    }

    /// Add a field, or replace an existing one of the same name *in place*.
    ///
    /// Returns the previous value, if the name was already present.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<Value>) -> Option<Value> {
        let name = name.into();
        let value = value.into();
        match self.fields.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => Some(std::mem::replace(&mut entry.1, value)),
            None => {
                self.fields.push((name, value));
                None
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.fields
            .iter_mut()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v)
    }

    /// Look up a field through nested structures, e.g. `"timeStamp.secondsPastEpoch"`.
    pub fn get_path(&self, path: &str) -> Option<&Value> {
        let (head, tail) = match path.split_once('.') {
            Some((head, tail)) => (head, Some(tail)),
            None => (path, None),
        };
        let value = self.get(head)?;
        match tail {
            None => Some(value),
            Some(tail) => value.as_structure()?.get_path(tail),
        }
    }

    pub fn remove(&mut self, name: &str) -> Option<Value> {
        let index = self.fields.iter().position(|(n, _)| n == name)?;
        Some(self.fields.remove(index).1)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The fields, in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.fields.iter().map(|(n, v)| (n.as_str(), v))
    }

    /// The introspection for this structure, derived from its contents.
    ///
    /// `None` if any field's introspection cannot be derived - see
    /// [`Value::derive_field`].
    pub fn derive_field(&self) -> Option<StructureField> {
        let mut fields = Vec::with_capacity(self.fields.len());
        for (name, value) in &self.fields {
            fields.push((name.clone(), value.derive_field()?));
        }
        Some(StructureField {
            id: self.id.clone(),
            fields,
        })
    }
}

/// A union value: one selected member out of a declared set.
///
/// Unlike every other [`Value`], this carries its own introspection. A union's wire
/// encoding is a selector index into the member list, so a value that had forgotten
/// which union it belonged to could not be encoded, and could not report which member
/// names were legal to select. For a union with no declared members, see
/// [`Value::VariantUnion`].
#[derive(Clone, Debug, PartialEq)]
pub struct UnionValue {
    union_type: UnionField,
    /// Index into `union_type`'s members.
    selected: Option<usize>,
    value: Option<Value>,
}

/// Returned when selecting a union member that the union does not declare.
#[derive(Debug, thiserror::Error)]
#[error("Union has no member named '{0}'")]
pub struct NoSuchUnionMember(pub String);

impl UnionValue {
    /// A union of the given type, with no member selected.
    pub fn new(union_type: UnionField) -> UnionValue {
        UnionValue {
            union_type,
            selected: None,
            value: None,
        }
    }

    pub fn union_type(&self) -> &UnionField {
        &self.union_type
    }

    /// Select a member by name and set its value.
    ///
    /// Fails if the union does not declare `name`. The value is *not* checked against
    /// the member's declared [`Field`]: that check belongs to the encoder, which is
    /// the only place a mismatch matters.
    pub fn select(&mut self, name: &str, value: impl Into<Value>) -> Result<(), NoSuchUnionMember> {
        let index = self
            .union_type
            .index_of(name)
            .ok_or_else(|| NoSuchUnionMember(name.to_string()))?;
        self.selected = Some(index);
        self.value = Some(value.into());
        Ok(())
    }

    /// Clear the selection.
    pub fn deselect(&mut self) {
        self.selected = None;
        self.value = None;
    }

    /// Index into the member list of the selected member, if any.
    pub fn selected_index(&self) -> Option<usize> {
        self.selected
    }

    /// Name of the selected member, if any.
    pub fn selected_name(&self) -> Option<&str> {
        self.union_type
            .members
            .get(self.selected?)
            .map(|(n, _)| n.as_str())
    }

    /// The selected member's value, if a member is selected.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }
}

/// How many elements an array field holds.
///
/// pvAccess distinguishes these three forms in the introspection type byte; only
/// `Variable` can be derived from a value, since the bound is a property of the type.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ArraySize {
    #[default]
    Variable,
    Bounded(u32),
    Fixed(u32),
}

/// Introspection for a [`Structure`]: names and shapes, no data.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StructureField {
    id: Option<String>,
    fields: Vec<(String, Field)>,
}

impl StructureField {
    pub fn new() -> StructureField {
        StructureField::default()
    }

    pub fn with_id(id: impl Into<String>) -> StructureField {
        StructureField {
            id: Some(id.into()),
            fields: Vec::new(),
        }
    }

    /// Builder form of [`StructureField::insert`].
    pub fn with(mut self, name: impl Into<String>, field: impl Into<Field>) -> StructureField {
        self.insert(name, field);
        self
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    pub fn set_id(&mut self, id: Option<String>) {
        self.id = id;
    }

    /// Add a member, or replace an existing one of the same name in place.
    pub fn insert(&mut self, name: impl Into<String>, field: impl Into<Field>) -> Option<Field> {
        let name = name.into();
        let field = field.into();
        match self.fields.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => Some(std::mem::replace(&mut entry.1, field)),
            None => {
                self.fields.push((name, field));
                None
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|(n, _)| n == name).map(|(_, f)| f)
    }

    /// Look up a member through nested structures, e.g. `"timeStamp.nanoseconds"`.
    pub fn get_path(&self, path: &str) -> Option<&Field> {
        let (head, tail) = match path.split_once('.') {
            Some((head, tail)) => (head, Some(tail)),
            None => (path, None),
        };
        let field = self.get(head)?;
        match (tail, field) {
            (None, field) => Some(field),
            (Some(tail), Field::Structure(inner)) => inner.get_path(tail),
            (Some(_), _) => None,
        }
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// The members, in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Field)> {
        self.fields.iter().map(|(n, f)| (n.as_str(), f))
    }
}

/// Introspection for a union: the members that may be selected.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct UnionField {
    id: Option<String>,
    members: Vec<(String, Field)>,
}

impl UnionField {
    pub fn new() -> UnionField {
        UnionField::default()
    }

    pub fn with_id(id: impl Into<String>) -> UnionField {
        UnionField {
            id: Some(id.into()),
            members: Vec::new(),
        }
    }

    /// Builder form of [`UnionField::insert`].
    pub fn with(mut self, name: impl Into<String>, field: impl Into<Field>) -> UnionField {
        self.insert(name, field);
        self
    }

    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Add a member, or replace an existing one of the same name in place.
    pub fn insert(&mut self, name: impl Into<String>, field: impl Into<Field>) -> Option<Field> {
        let name = name.into();
        let field = field.into();
        match self.members.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => Some(std::mem::replace(&mut entry.1, field)),
            None => {
                self.members.push((name, field));
                None
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&Field> {
        self.members.iter().find(|(n, _)| n == name).map(|(_, f)| f)
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.members.iter().position(|(n, _)| n == name)
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The members, in declaration order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Field)> {
        self.members.iter().map(|(n, f)| (n.as_str(), f))
    }
}

/// The shape of a [`Value`], carrying no data.
///
/// This is what pvAccess sends as `FieldDesc` and caches per connection. Keep it
/// separate from `Value` - see the module documentation.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Scalar(ScalarType),
    ScalarArray {
        element_type: ScalarType,
        size: ArraySize,
    },
    Structure(StructureField),
    StructureArray {
        element_type: Box<StructureField>,
        size: ArraySize,
    },
    Union(UnionField),
    /// A union with no declared members: any value at all, or none.
    VariantUnion,
}

impl Field {
    /// A variable-length array of `element_type`.
    pub fn array_of(element_type: ScalarType) -> Field {
        Field::ScalarArray {
            element_type,
            size: ArraySize::Variable,
        }
    }

    /// A variable-length array of structures.
    pub fn array_of_structure(element_type: StructureField) -> Field {
        Field::StructureArray {
            element_type: Box::new(element_type),
            size: ArraySize::Variable,
        }
    }

    /// The type identifier, for the two forms that can carry one.
    pub fn id(&self) -> Option<&str> {
        match self {
            Field::Structure(structure) => structure.id(),
            Field::StructureArray { element_type, .. } => element_type.id(),
            Field::Union(union) => union.id(),
            _ => None,
        }
    }

    /// Whether this describes any of the array forms.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            Field::ScalarArray { .. } | Field::StructureArray { .. }
        )
    }
}

impl From<ScalarType> for Field {
    fn from(value: ScalarType) -> Field {
        Field::Scalar(value)
    }
}

impl From<StructureField> for Field {
    fn from(value: StructureField) -> Field {
        Field::Structure(value)
    }
}

impl From<UnionField> for Field {
    fn from(value: UnionField) -> Field {
        Field::Union(value)
    }
}

/// A protocol-neutral value.
///
/// Recursive, and a strict superset of what [`crate::dbr::DbrValue`] can express.
/// [`ca`] holds the conversions in both directions.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Scalar(Scalar),
    ScalarArray(ScalarArray),
    Structure(Structure),
    /// An array of structures, any element of which may be null.
    StructureArray(Vec<Option<Structure>>),
    Union(Box<UnionValue>),
    /// A union with no declared members, holding any value at all, or none.
    VariantUnion(Option<Box<Value>>),
}

impl Value {
    /// The type identifier, if this is a structure or a union that carries one.
    pub fn id(&self) -> Option<&str> {
        match self {
            Value::Structure(structure) => structure.id(),
            Value::Union(union) => union.union_type().id(),
            _ => None,
        }
    }

    pub fn as_scalar(&self) -> Option<&Scalar> {
        match self {
            Value::Scalar(scalar) => Some(scalar),
            _ => None,
        }
    }

    pub fn as_scalar_array(&self) -> Option<&ScalarArray> {
        match self {
            Value::ScalarArray(array) => Some(array),
            _ => None,
        }
    }

    pub fn as_structure(&self) -> Option<&Structure> {
        match self {
            Value::Structure(structure) => Some(structure),
            _ => None,
        }
    }

    pub fn as_structure_mut(&mut self) -> Option<&mut Structure> {
        match self {
            Value::Structure(structure) => Some(structure),
            _ => None,
        }
    }

    pub fn as_union(&self) -> Option<&UnionValue> {
        match self {
            Value::Union(union) => Some(union),
            _ => None,
        }
    }

    /// The element type, for scalars and scalar arrays.
    pub fn scalar_type(&self) -> Option<ScalarType> {
        match self {
            Value::Scalar(scalar) => Some(scalar.scalar_type()),
            Value::ScalarArray(array) => Some(array.element_type()),
            _ => None,
        }
    }

    /// Number of elements: 1 for a scalar, the length for either array form, and the
    /// field count for a structure.
    pub fn len(&self) -> usize {
        match self {
            Value::Scalar(_) => 1,
            Value::ScalarArray(array) => array.len(),
            Value::Structure(structure) => structure.len(),
            Value::StructureArray(elements) => elements.len(),
            Value::Union(_) | Value::VariantUnion(_) => 1,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Look up a structure field by name; `None` for anything but a structure.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.as_structure()?.get(name)
    }

    /// Look up a nested structure field, e.g. `"timeStamp.secondsPastEpoch"`.
    pub fn get_path(&self, path: &str) -> Option<&Value> {
        self.as_structure()?.get_path(path)
    }

    /// Derive the introspection for this value, where the data determines it.
    ///
    /// `None` in the two cases where it does not:
    ///
    /// - a [`Value::StructureArray`] with no non-null element, whose element type is
    ///   therefore unknown;
    /// - a [`Value::VariantUnion`] holding one of those, transitively.
    ///
    /// Note that array sizes always derive as [`ArraySize::Variable`], since bounds are
    /// a property of the type and not of any particular value. This is a convenience
    /// for the common case of a value built in memory and then described on the wire;
    /// a `Field` obtained any other way - decoded from a peer, or declared by a
    /// normative type - is authoritative over this.
    pub fn derive_field(&self) -> Option<Field> {
        Some(match self {
            Value::Scalar(scalar) => Field::Scalar(scalar.scalar_type()),
            Value::ScalarArray(array) => Field::array_of(array.element_type()),
            Value::Structure(structure) => Field::Structure(structure.derive_field()?),
            Value::StructureArray(elements) => {
                let element = elements.iter().flatten().next()?;
                Field::array_of_structure(element.derive_field()?)
            }
            Value::Union(union) => Field::Union(union.union_type().clone()),
            Value::VariantUnion(_) => Field::VariantUnion,
        })
    }
}

impl From<Scalar> for Value {
    fn from(value: Scalar) -> Value {
        Value::Scalar(value)
    }
}

impl From<ScalarArray> for Value {
    fn from(value: ScalarArray) -> Value {
        Value::ScalarArray(value)
    }
}

impl From<Structure> for Value {
    fn from(value: Structure) -> Value {
        Value::Structure(value)
    }
}

impl From<UnionValue> for Value {
    fn from(value: UnionValue) -> Value {
        Value::Union(Box::new(value))
    }
}

/// `From<T>` and `From<Vec<T>>` for each primitive, plus `TryFrom` back out.
macro_rules! impl_value_conversions {
    ($variant:ident, $typ:ty) => {
        impl From<$typ> for Scalar {
            fn from(value: $typ) -> Scalar {
                Scalar::$variant(value)
            }
        }
        impl From<$typ> for Value {
            fn from(value: $typ) -> Value {
                Value::Scalar(Scalar::$variant(value))
            }
        }
        impl From<Vec<$typ>> for ScalarArray {
            fn from(value: Vec<$typ>) -> ScalarArray {
                ScalarArray::$variant(value)
            }
        }
        impl From<Vec<$typ>> for Value {
            fn from(value: Vec<$typ>) -> Value {
                Value::ScalarArray(ScalarArray::$variant(value))
            }
        }
        impl TryFrom<&Value> for $typ {
            type Error = WrongValueType;
            fn try_from(value: &Value) -> Result<$typ, WrongValueType> {
                match value {
                    Value::Scalar(Scalar::$variant(v)) => Ok(v.clone()),
                    // A single-element array reads as a scalar, as CA does
                    Value::ScalarArray(ScalarArray::$variant(v)) if v.len() == 1 => {
                        Ok(v[0].clone())
                    }
                    _ => Err(WrongValueType {
                        expected: ScalarType::$variant,
                        found: value.scalar_type(),
                    }),
                }
            }
        }
        impl TryFrom<&Value> for Vec<$typ> {
            type Error = WrongValueType;
            fn try_from(value: &Value) -> Result<Vec<$typ>, WrongValueType> {
                match value {
                    Value::ScalarArray(ScalarArray::$variant(v)) => Ok(v.clone()),
                    Value::Scalar(Scalar::$variant(v)) => Ok(vec![v.clone()]),
                    _ => Err(WrongValueType {
                        expected: ScalarType::$variant,
                        found: value.scalar_type(),
                    }),
                }
            }
        }
    };
}

/// Returned when extracting a native type from a [`Value`] of a different type.
///
/// Deliberately does not attempt any numeric coercion - that is
/// [`crate::dbr::DbrValue::convert_to`]'s job on the CA side, and a pvAccess client
/// asks for the type it wants.
#[derive(Debug, thiserror::Error)]
#[error("Expected a value of type {expected:?}, found {found:?}")]
pub struct WrongValueType {
    pub expected: ScalarType,
    pub found: Option<ScalarType>,
}

impl_value_conversions!(Bool, bool);
impl_value_conversions!(Byte, i8);
impl_value_conversions!(UByte, u8);
impl_value_conversions!(Short, i16);
impl_value_conversions!(UShort, u16);
impl_value_conversions!(Int, i32);
impl_value_conversions!(UInt, u32);
impl_value_conversions!(Long, i64);
impl_value_conversions!(ULong, u64);
impl_value_conversions!(Float, f32);
impl_value_conversions!(Double, f64);
impl_value_conversions!(String, String);

impl From<&str> for Scalar {
    fn from(value: &str) -> Scalar {
        Scalar::String(value.to_string())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Value {
        Value::Scalar(Scalar::String(value.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The thing DbrValue cannot express: nesting, and unsigned 64-bit arrays.
    #[test]
    fn nested_structure_with_unsigned_64bit_array() {
        let value = Value::from(
            Structure::with_id("epics:nt/NTScalarArray:1.0")
                .with("value", vec![1u64, 2, u64::MAX])
                .with(
                    "timeStamp",
                    Structure::with_id("time_t")
                        .with("secondsPastEpoch", 1_753_000_000i64)
                        .with("nanoseconds", 250i32)
                        .with("userTag", 0i32),
                ),
        );

        assert_eq!(value.id(), Some("epics:nt/NTScalarArray:1.0"));
        assert_eq!(
            value.get("value"),
            Some(&Value::ScalarArray(ScalarArray::ULong(vec![
                1,
                2,
                u64::MAX
            ])))
        );
        assert_eq!(
            value.get_path("timeStamp.nanoseconds"),
            Some(&Value::from(250i32))
        );
        assert_eq!(
            value.get_path("timeStamp.secondsPastEpoch"),
            Some(&Value::from(1_753_000_000i64))
        );
        assert!(value.get_path("timeStamp.missing").is_none());
        assert!(value.get_path("value.nanoseconds").is_none());

        // And the introspection derived from it, which is a separate type
        let field = value.derive_field().unwrap();
        assert_eq!(
            field,
            Field::Structure(
                StructureField::with_id("epics:nt/NTScalarArray:1.0")
                    .with("value", Field::array_of(ScalarType::ULong))
                    .with(
                        "timeStamp",
                        StructureField::with_id("time_t")
                            .with("secondsPastEpoch", ScalarType::Long)
                            .with("nanoseconds", ScalarType::Int)
                            .with("userTag", ScalarType::Int)
                    )
            )
        );
        let Field::Structure(structure) = &field else {
            panic!("not a structure");
        };
        assert_eq!(
            structure.get_path("timeStamp.userTag"),
            Some(&Field::Scalar(ScalarType::Int))
        );
        assert_eq!(field.id(), Some("epics:nt/NTScalarArray:1.0"));
    }

    #[test]
    fn scalar_types_are_reported() {
        assert_eq!(Value::from(1u8).scalar_type(), Some(ScalarType::UByte));
        assert_eq!(Value::from(1i64).scalar_type(), Some(ScalarType::Long));
        assert_eq!(Value::from("hello").scalar_type(), Some(ScalarType::String));
        assert_eq!(
            Value::from(vec![1.0f32]).scalar_type(),
            Some(ScalarType::Float)
        );
        assert_eq!(Value::from(Structure::new()).scalar_type(), None);

        // Empty arrays still know what they hold
        assert_eq!(
            ScalarArray::empty_of(ScalarType::UInt).element_type(),
            ScalarType::UInt
        );
        assert!(ScalarArray::empty_of(ScalarType::UInt).is_empty());

        assert!(ScalarType::ULong.is_integer());
        assert!(ScalarType::ULong.is_unsigned());
        assert!(!ScalarType::Long.is_unsigned());
        assert!(ScalarType::Double.is_floating());
        assert_eq!(ScalarType::Double.size(), Some(8));
        assert_eq!(ScalarType::String.size(), None);
        assert_eq!(ScalarType::UByte.name(), "ubyte");
    }

    #[test]
    fn scalar_accessors() {
        assert_eq!(Scalar::UInt(7).as_f64(), Some(7.0));
        assert_eq!(Scalar::Double(0.5).as_f64(), Some(0.5));
        assert_eq!(Scalar::Bool(true).as_f64(), None);
        assert_eq!(Scalar::from("x").as_str(), Some("x"));
        assert_eq!(Scalar::Int(1).as_str(), None);
    }

    #[test]
    fn structure_field_order_is_preserved_and_names_unique() {
        let mut structure = Structure::new().with("a", 1i32).with("b", 2i32);
        assert_eq!(structure.insert("a", 3i32), Some(Value::from(1i32)));
        assert_eq!(structure.insert("c", 4i32), None);
        assert_eq!(
            structure.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(structure.get("a"), Some(&Value::from(3i32)));
        assert_eq!(structure.len(), 3);

        *structure.get_mut("b").unwrap() = Value::from("two");
        assert_eq!(structure.get("b"), Some(&Value::from("two")));

        assert_eq!(structure.remove("b"), Some(Value::from("two")));
        assert_eq!(structure.remove("b"), None);
        assert_eq!(
            structure.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            ["a", "c"]
        );

        assert!(Structure::new().is_empty());
        assert_eq!(Structure::new().id(), None);
        let mut named = Structure::new();
        named.set_id(Some("some_t".to_string()));
        assert_eq!(named.id(), Some("some_t"));
    }

    #[test]
    fn native_types_convert_both_ways() {
        let value = Value::from(vec![1i16, 2, 3]);
        assert_eq!(Vec::<i16>::try_from(&value).unwrap(), vec![1, 2, 3]);
        assert!(Vec::<i32>::try_from(&value).is_err());

        // Scalar/single-element-array equivalence, in both directions
        assert_eq!(i32::try_from(&Value::from(vec![9i32])).unwrap(), 9);
        assert_eq!(Vec::<i32>::try_from(&Value::from(9i32)).unwrap(), vec![9]);
        assert!(i32::try_from(&Value::from(vec![1i32, 2])).is_err());

        assert_eq!(
            String::try_from(&Value::from("hello")).unwrap(),
            "hello".to_string()
        );
        assert!(bool::try_from(&Value::from(Structure::new())).is_err());

        let error = u8::try_from(&Value::from(1i8)).unwrap_err();
        assert_eq!(error.expected, ScalarType::UByte);
        assert_eq!(error.found, Some(ScalarType::Byte));
    }

    #[test]
    fn structure_arrays_carry_nulls() {
        let value = Value::StructureArray(vec![
            None,
            Some(Structure::with_id("point_t").with("x", 1.0f64)),
        ]);
        assert_eq!(value.len(), 2);
        assert_eq!(
            value.derive_field(),
            Some(Field::array_of_structure(
                StructureField::with_id("point_t").with("x", ScalarType::Double)
            ))
        );

        // With no populated element there is nothing to derive the shape from
        assert_eq!(Value::StructureArray(vec![None]).derive_field(), None);
        assert_eq!(Value::StructureArray(Vec::new()).derive_field(), None);
    }

    #[test]
    fn unions_select_declared_members_only() {
        let union_type = UnionField::with_id("any_t")
            .with("stringValue", ScalarType::String)
            .with("intValue", ScalarType::Int);
        assert_eq!(union_type.index_of("intValue"), Some(1));
        assert_eq!(
            union_type.get("intValue"),
            Some(&Field::Scalar(ScalarType::Int))
        );
        assert_eq!(union_type.len(), 2);
        assert!(!union_type.is_empty());

        let mut union = UnionValue::new(union_type);
        assert_eq!(union.selected_name(), None);
        assert_eq!(union.value(), None);

        union.select("intValue", 42i32).unwrap();
        assert_eq!(union.selected_index(), Some(1));
        assert_eq!(union.selected_name(), Some("intValue"));
        assert_eq!(union.value(), Some(&Value::from(42i32)));

        assert!(union.select("nope", 1i32).is_err());
        // A failed select leaves the previous selection alone
        assert_eq!(union.selected_name(), Some("intValue"));

        union.deselect();
        assert_eq!(union.selected_name(), None);

        let value = Value::from(union);
        assert_eq!(value.id(), Some("any_t"));
        assert_eq!(
            value.derive_field(),
            Some(Field::Union(
                UnionField::with_id("any_t")
                    .with("stringValue", ScalarType::String)
                    .with("intValue", ScalarType::Int)
            ))
        );
        assert!(value.as_union().is_some());
        assert!(value.as_structure().is_none());
    }

    #[test]
    fn variant_unions_hold_anything() {
        let empty = Value::VariantUnion(None);
        assert_eq!(empty.derive_field(), Some(Field::VariantUnion));

        let nested = Value::VariantUnion(Some(Box::new(Value::from(vec![1u64]))));
        assert_eq!(nested.derive_field(), Some(Field::VariantUnion));
        assert_eq!(nested.len(), 1);
    }

    #[test]
    fn array_sizes_are_a_property_of_the_field() {
        assert_eq!(ArraySize::default(), ArraySize::Variable);

        let bounded = Field::ScalarArray {
            element_type: ScalarType::Double,
            size: ArraySize::Bounded(16),
        };
        let fixed = Field::ScalarArray {
            element_type: ScalarType::Double,
            size: ArraySize::Fixed(16),
        };
        assert!(bounded.is_array());
        assert!(fixed.is_array());
        assert_ne!(bounded, fixed);
        assert_ne!(bounded, Field::array_of(ScalarType::Double));
        assert!(!Field::Scalar(ScalarType::Double).is_array());

        // Deriving from a value can only ever produce the variable form
        assert_eq!(
            Value::from(vec![1.0f64; 16]).derive_field(),
            Some(Field::array_of(ScalarType::Double))
        );
    }

    #[test]
    fn structure_field_is_data_free_but_navigable() {
        let field = StructureField::with_id("epics:nt/NTScalar:1.0")
            .with("value", ScalarType::Double)
            .with(
                "alarm",
                StructureField::with_id("alarm_t")
                    .with("severity", ScalarType::Int)
                    .with("message", ScalarType::String),
            );

        assert_eq!(field.len(), 2);
        assert_eq!(
            field.get_path("alarm.message"),
            Some(&Field::Scalar(ScalarType::String))
        );
        assert!(field.get_path("value.severity").is_none());
        assert!(field.get_path("alarm.nope").is_none());
        assert_eq!(
            field.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            ["value", "alarm"]
        );

        let mut field = field;
        assert_eq!(
            field.insert("value", ScalarType::Float),
            Some(Field::Scalar(ScalarType::Double))
        );
        assert_eq!(field.get("value"), Some(&Field::Scalar(ScalarType::Float)));
        field.set_id(None);
        assert_eq!(field.id(), None);
        assert!(StructureField::new().is_empty());
    }
}
