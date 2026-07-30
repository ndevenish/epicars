//! FieldDesc: the wire encoding of a [`Field`].
//!
//! This is what CA does not have. CA's "introspection" is a `u16` computed as
//! `category * 7 + basic_type` - pure arithmetic, with no registry, no negotiation, and no
//! way to describe a user-defined structure. pvAccess sends the shape of a value
//! separately from the value, recursively, and caches it per connection.
//!
//! # Type codes
//!
//! The first byte selects between four special forms and the ordinary inline one:
//!
//! | Code | Name | Payload |
//! |---|---|---|
//! | `0xFF` | NULL_TYPE_CODE | none - the field is absent |
//! | `0xFE` | ONLY_ID_TYPE_CODE | a `u16` id, resolved against the cache |
//! | `0xFD` | FULL_WITH_ID_TYPE_CODE | a `u16` id, then the FieldDesc, registering the id |
//! | `0xFC` | FULL_TAGGED_ID_TYPE_CODE | id + tag + FieldDesc - see below |
//! | ≤ `0xDF` | FULL_TYPE_CODE | the FieldDesc inline, with no id |
//!
//! An inline type byte packs three things:
//!
//! - **bits 7-5, kind**: `000` boolean, `001` integer, `010` floating-point, `011` string,
//!   `100` complex
//! - **bits 4-3, array form**: `00` scalar, `01` variable-size, `10` bounded-size,
//!   `11` fixed-size
//! - **bits 2-0, detail**: for integers, bit 2 is the unsigned flag and bits 1-0 the width
//!   (`00` byte, `01` short, `10` int, `11` long); for floats, `010` is `f32` and `011` is
//!   `f64`; for complex types, `000` structure, `001` union, `010` variant union,
//!   `011` bounded string
//!
//! ```
//! use epicars::pva::introspection::type_code;
//! use epicars::value::{Field, ScalarType};
//!
//! assert_eq!(type_code(&Field::Scalar(ScalarType::Int)), Some(0x22));
//! assert_eq!(type_code(&Field::Scalar(ScalarType::ULong)), Some(0x27));
//! ```
//!
//! # Caching is somebody else's problem
//!
//! Whether to emit `ONLY_ID` instead of a full description, and what a received id means,
//! is per-connection state. This module expresses that as the [`TypeCache`] trait and
//! provides [`NoCache`], which never caches anything and rejects `ONLY_ID`. The real
//! bidirectional registry is plan item 1.5. Keeping them apart matters because the caching
//! applies at *every* nested field position, not just the top level, so the recursion has
//! to be written once with the cache threaded through it.

use crate::pva::io::{PvaError, PvaReader, PvaWriter};
use crate::value::{ArraySize, Field, ScalarType, StructureField, UnionField};

pub const NULL_TYPE_CODE: u8 = 0xFF;
pub const ONLY_ID_TYPE_CODE: u8 = 0xFE;
pub const FULL_WITH_ID_TYPE_CODE: u8 = 0xFD;
pub const FULL_TAGGED_ID_TYPE_CODE: u8 = 0xFC;
/// The highest byte that is an inline FieldDesc rather than one of the special forms.
pub const MAX_FULL_TYPE_CODE: u8 = 0xDF;

const KIND_SHIFT: u32 = 5;
const KIND_BOOLEAN: u8 = 0;
const KIND_INTEGER: u8 = 1;
const KIND_FLOAT: u8 = 2;
const KIND_STRING: u8 = 3;
const KIND_COMPLEX: u8 = 4;

const ARRAY_SHIFT: u32 = 3;
const ARRAY_SCALAR: u8 = 0;
const ARRAY_VARIABLE: u8 = 1;
const ARRAY_BOUNDED: u8 = 2;
const ARRAY_FIXED: u8 = 3;
const ARRAY_MASK: u8 = 0b11;

const DETAIL_MASK: u8 = 0b111;
const INTEGER_UNSIGNED: u8 = 0b100;

const COMPLEX_STRUCTURE: u8 = 0;
const COMPLEX_UNION: u8 = 1;
const COMPLEX_VARIANT_UNION: u8 = 2;
const COMPLEX_BOUNDED_STRING: u8 = 3;

/// The per-connection introspection cache, as the codec sees it.
///
/// Two of these exist per connection - one per direction - because the link is full-duplex
/// and each side registers ids independently. That structure is plan item 1.5; this trait
/// is only what the recursion needs from it.
///
/// Every method has a do-nothing default, so [`NoCache`] is an empty implementation.
pub trait TypeCache {
    /// The id this field is already registered under on the **send** side, if any.
    #[allow(unused_variables)]
    fn id_for(&self, field: &Field) -> Option<u16> {
        None
    }

    /// Allocate and register a send-side id for `field`, or decline.
    ///
    /// Declining is always legal - it just means a larger message - and is what pvxs does
    /// throughout its handshake.
    #[allow(unused_variables)]
    fn register_for_send(&mut self, field: &Field) -> Option<u16> {
        None
    }

    /// Resolve a **receive**-side id.
    #[allow(unused_variables)]
    fn lookup(&self, id: u16) -> Option<Field> {
        None
    }

    /// Register a receive-side id, replacing any previous binding.
    #[allow(unused_variables)]
    fn remember(&mut self, id: u16, field: Field) {}
}

/// A [`TypeCache`] that caches nothing.
///
/// Encodes everything inline, and treats a received `ONLY_ID` as malformed - correctly, in
/// that there is no state to resolve it against. Useful for unit tests, and for encoding a
/// standalone FieldDesc outside any connection.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCache;

impl TypeCache for NoCache {}

/// The inline type byte for a field, or `None` for the complex forms whose byte depends on
/// more than the kind.
///
/// Complex fields do have a type byte - see [`full_type_code`] - but it is not determined
/// by the field alone in the same one-to-one way, so this returns `None` for them to keep
/// the common scalar case a total function.
pub fn type_code(field: &Field) -> Option<u8> {
    match field {
        Field::Scalar(scalar_type) => Some(scalar_code(*scalar_type, ARRAY_SCALAR)),
        Field::ScalarArray { element_type, size } => {
            Some(scalar_code(*element_type, array_form(*size)))
        }
        _ => None,
    }
}

fn scalar_code(scalar_type: ScalarType, array_form: u8) -> u8 {
    let (kind, detail) = match scalar_type {
        ScalarType::Bool => (KIND_BOOLEAN, 0),
        ScalarType::Byte => (KIND_INTEGER, 0b000),
        ScalarType::Short => (KIND_INTEGER, 0b001),
        ScalarType::Int => (KIND_INTEGER, 0b010),
        ScalarType::Long => (KIND_INTEGER, 0b011),
        ScalarType::UByte => (KIND_INTEGER, INTEGER_UNSIGNED),
        ScalarType::UShort => (KIND_INTEGER, INTEGER_UNSIGNED | 0b001),
        ScalarType::UInt => (KIND_INTEGER, INTEGER_UNSIGNED | 0b010),
        ScalarType::ULong => (KIND_INTEGER, INTEGER_UNSIGNED | 0b011),
        ScalarType::Float => (KIND_FLOAT, 0b010),
        ScalarType::Double => (KIND_FLOAT, 0b011),
        ScalarType::String => (KIND_STRING, 0),
    };
    (kind << KIND_SHIFT) | (array_form << ARRAY_SHIFT) | detail
}

fn array_form(size: ArraySize) -> u8 {
    match size {
        ArraySize::Variable => ARRAY_VARIABLE,
        ArraySize::Bounded(_) => ARRAY_BOUNDED,
        ArraySize::Fixed(_) => ARRAY_FIXED,
    }
}

/// The inline type byte a field encodes to, including the complex forms.
pub fn full_type_code(field: &Field) -> u8 {
    match field {
        Field::Scalar(_) | Field::ScalarArray { .. } => {
            type_code(field).expect("scalar forms always have a code")
        }
        Field::Structure(_) => complex_code(COMPLEX_STRUCTURE, ARRAY_SCALAR),
        Field::StructureArray { size, .. } => complex_code(COMPLEX_STRUCTURE, array_form(*size)),
        Field::Union(_) => complex_code(COMPLEX_UNION, ARRAY_SCALAR),
        Field::VariantUnion => complex_code(COMPLEX_VARIANT_UNION, ARRAY_SCALAR),
    }
}

fn complex_code(detail: u8, array_form: u8) -> u8 {
    (KIND_COMPLEX << KIND_SHIFT) | (array_form << ARRAY_SHIFT) | detail
}

/// Whether a field is worth putting in the introspection cache.
///
/// Only the composite forms: a scalar's whole description is one byte, so an `ONLY_ID`
/// reference to it would be three, and caching it would make messages larger.
fn is_cacheable(field: &Field) -> bool {
    matches!(
        field,
        Field::Structure(_) | Field::StructureArray { .. } | Field::Union(_)
    )
}

/// Encode a field's introspection, consulting `cache` at every nested position.
pub fn encode_field(
    field: &Field,
    writer: &mut PvaWriter,
    cache: &mut dyn TypeCache,
) -> Result<(), PvaError> {
    if is_cacheable(field) {
        if let Some(id) = cache.id_for(field) {
            writer.write_u8(ONLY_ID_TYPE_CODE);
            writer.write_u16(id);
            return Ok(());
        }
        if let Some(id) = cache.register_for_send(field) {
            writer.write_u8(FULL_WITH_ID_TYPE_CODE);
            writer.write_u16(id);
            return encode_inline(field, writer, cache);
        }
    }
    encode_inline(field, writer, cache)
}

/// Encode a field that may be absent, using `NULL_TYPE_CODE` for `None`.
pub fn encode_optional_field(
    field: Option<&Field>,
    writer: &mut PvaWriter,
    cache: &mut dyn TypeCache,
) -> Result<(), PvaError> {
    match field {
        None => {
            writer.write_u8(NULL_TYPE_CODE);
            Ok(())
        }
        Some(field) => encode_field(field, writer, cache),
    }
}

/// Encode the inline form: type byte, then whatever that byte implies.
fn encode_inline(
    field: &Field,
    writer: &mut PvaWriter,
    cache: &mut dyn TypeCache,
) -> Result<(), PvaError> {
    writer.write_u8(full_type_code(field));
    match field {
        Field::Scalar(_) => Ok(()),
        Field::ScalarArray { size, .. } => {
            encode_array_bound(*size, writer);
            Ok(())
        }
        Field::Structure(structure) => encode_structure_body(structure, writer, cache),
        Field::StructureArray { element_type, size } => {
            encode_array_bound(*size, writer);
            // The element type goes through the cache too, so a structure array of a
            // structure already registered costs three bytes
            encode_field(&Field::Structure((**element_type).clone()), writer, cache)
        }
        Field::Union(union) => encode_union_body(union, writer, cache),
        Field::VariantUnion => Ok(()),
    }
}

/// Bounded and fixed arrays carry their bound; variable-size ones do not.
fn encode_array_bound(size: ArraySize, writer: &mut PvaWriter) {
    match size {
        ArraySize::Variable => (),
        ArraySize::Bounded(bound) | ArraySize::Fixed(bound) => {
            writer.write_size(Some(bound as usize))
        }
    }
}

fn encode_structure_body(
    structure: &StructureField,
    writer: &mut PvaWriter,
    cache: &mut dyn TypeCache,
) -> Result<(), PvaError> {
    writer.write_string(structure.id().unwrap_or(""));
    writer.write_size(Some(structure.len()));
    for (name, field) in structure.iter() {
        writer.write_string(name);
        encode_field(field, writer, cache)?;
    }
    Ok(())
}

fn encode_union_body(
    union: &UnionField,
    writer: &mut PvaWriter,
    cache: &mut dyn TypeCache,
) -> Result<(), PvaError> {
    writer.write_string(union.id().unwrap_or(""));
    writer.write_size(Some(union.len()));
    for (name, field) in union.iter() {
        writer.write_string(name);
        encode_field(field, writer, cache)?;
    }
    Ok(())
}

/// Decode a field's introspection. `None` is `NULL_TYPE_CODE`: the field is absent.
pub fn decode_field(
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<Option<Field>, PvaError> {
    let code = reader.read_u8()?;
    match code {
        NULL_TYPE_CODE => Ok(None),
        ONLY_ID_TYPE_CODE => {
            let id = reader.read_u16()?;
            cache.lookup(id).map(Some).ok_or_else(|| {
                PvaError::malformed(format!("introspection id {id} has not been registered"))
            })
        }
        FULL_WITH_ID_TYPE_CODE => {
            let id = reader.read_u16()?;
            let field = decode_inline(reader.read_u8()?, reader, cache)?;
            // Registration is unconditional and overrides: ids are re-definable mid-stream
            cache.remember(id, field.clone());
            Ok(Some(field))
        }
        FULL_TAGGED_ID_TYPE_CODE => {
            // Deliberately refused rather than guessed. Neither pvAccessCPP nor pvxs emits
            // this - the captures contain FULL_WITH_ID and inline FULL only - and the tag's
            // width is not pinned down by the specification prose. Guessing it wrong would
            // not fail here; it would silently desynchronise every following byte on the
            // connection, which is far worse than a clear refusal.
            Err(PvaError::malformed(
                "FULL_TAGGED_ID_TYPE_CODE (0xFC) is not implemented: its tag encoding is \
                 unverified and no reference implementation sends it",
            ))
        }
        code if code <= MAX_FULL_TYPE_CODE => Ok(Some(decode_inline(code, reader, cache)?)),
        other => Err(PvaError::malformed(format!(
            "reserved introspection type code {other:#04x}"
        ))),
    }
}

/// Decode a field that must be present.
pub fn decode_present_field(
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<Field, PvaError> {
    decode_field(reader, cache)?
        .ok_or_else(|| PvaError::malformed("null introspection where a field is required"))
}

fn decode_inline(
    code: u8,
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<Field, PvaError> {
    let kind = code >> KIND_SHIFT;
    let array = (code >> ARRAY_SHIFT) & ARRAY_MASK;
    let detail = code & DETAIL_MASK;

    if kind == KIND_COMPLEX {
        return decode_complex(detail, array, reader, cache);
    }

    let element_type = match (kind, detail) {
        (KIND_BOOLEAN, 0) => ScalarType::Bool,
        (KIND_INTEGER, detail) => match detail {
            0b000 => ScalarType::Byte,
            0b001 => ScalarType::Short,
            0b010 => ScalarType::Int,
            0b011 => ScalarType::Long,
            0b100 => ScalarType::UByte,
            0b101 => ScalarType::UShort,
            0b110 => ScalarType::UInt,
            _ => ScalarType::ULong,
        },
        (KIND_FLOAT, 0b010) => ScalarType::Float,
        (KIND_FLOAT, 0b011) => ScalarType::Double,
        (KIND_STRING, 0) => ScalarType::String,
        _ => {
            return Err(PvaError::malformed(format!(
                "unknown scalar type code {code:#04x}"
            )));
        }
    };

    Ok(match array {
        ARRAY_SCALAR => Field::Scalar(element_type),
        form => Field::ScalarArray {
            element_type,
            size: decode_array_bound(form, reader)?,
        },
    })
}

fn decode_complex(
    detail: u8,
    array: u8,
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<Field, PvaError> {
    match (detail, array) {
        (COMPLEX_STRUCTURE, ARRAY_SCALAR) => {
            Ok(Field::Structure(decode_structure_body(reader, cache)?))
        }
        (COMPLEX_STRUCTURE, form) => {
            let size = decode_array_bound(form, reader)?;
            let element = decode_present_field(reader, cache)?;
            let Field::Structure(element) = element else {
                return Err(PvaError::malformed(
                    "structure array element is not a structure",
                ));
            };
            Ok(Field::StructureArray {
                element_type: Box::new(element),
                size,
            })
        }
        (COMPLEX_UNION, ARRAY_SCALAR) => Ok(Field::Union(decode_union_body(reader, cache)?)),
        (COMPLEX_VARIANT_UNION, ARRAY_SCALAR) => Ok(Field::VariantUnion),
        (COMPLEX_BOUNDED_STRING, form) => {
            // Accepted leniently: `Field` has no bounded-string form, so the bound is read
            // and dropped and this decodes as a plain string. Re-encoding therefore loses
            // the bound. No normative type uses bounded strings, so this exists only so
            // that a peer sending one does not kill the connection.
            let _bound = reader.read_present_size()?;
            Ok(match form {
                ARRAY_SCALAR => Field::Scalar(ScalarType::String),
                form => Field::ScalarArray {
                    element_type: ScalarType::String,
                    size: decode_array_bound(form, reader)?,
                },
            })
        }
        (detail, array) => Err(PvaError::malformed(format!(
            "unsupported complex introspection: detail {detail:#03b}, array form {array:#03b}"
        ))),
    }
}

fn decode_array_bound(form: u8, reader: &mut PvaReader<'_>) -> Result<ArraySize, PvaError> {
    Ok(match form {
        ARRAY_VARIABLE => ArraySize::Variable,
        ARRAY_BOUNDED => ArraySize::Bounded(read_bound(reader)?),
        _ => ArraySize::Fixed(read_bound(reader)?),
    })
}

fn read_bound(reader: &mut PvaReader<'_>) -> Result<u32, PvaError> {
    let bound = reader.read_present_size()?;
    u32::try_from(bound)
        .map_err(|_| PvaError::OutOfRange(format!("array bound {bound} exceeds u32")))
}

fn decode_structure_body(
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<StructureField, PvaError> {
    let id = reader.read_string()?;
    let count = reader.read_present_size()?;
    let mut structure = if id.is_empty() {
        StructureField::new()
    } else {
        StructureField::with_id(id)
    };
    for _ in 0..count {
        let name = reader.read_string()?;
        structure.insert(name, decode_present_field(reader, cache)?);
    }
    Ok(structure)
}

fn decode_union_body(
    reader: &mut PvaReader<'_>,
    cache: &mut dyn TypeCache,
) -> Result<UnionField, PvaError> {
    let id = reader.read_string()?;
    let count = reader.read_present_size()?;
    let mut union = if id.is_empty() {
        UnionField::new()
    } else {
        UnionField::with_id(id)
    };
    for _ in 0..count {
        let name = reader.read_string()?;
        union.insert(name, decode_present_field(reader, cache)?);
    }
    Ok(union)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::io::ByteOrder;

    const ORDERS: [ByteOrder; 2] = [ByteOrder::Little, ByteOrder::Big];

    fn encode(field: &Field, order: ByteOrder) -> Vec<u8> {
        let mut writer = PvaWriter::new(order);
        encode_field(field, &mut writer, &mut NoCache).unwrap();
        writer.into_bytes()
    }

    fn decode(bytes: &[u8], order: ByteOrder) -> Result<Option<Field>, PvaError> {
        let mut reader = PvaReader::new(bytes, order);
        let field = decode_field(&mut reader, &mut NoCache)?;
        assert!(
            reader.is_empty(),
            "decoder left {} bytes",
            reader.remaining()
        );
        Ok(field)
    }

    fn round_trip(field: &Field) {
        for order in ORDERS {
            let bytes = encode(field, order);
            assert_eq!(
                decode(&bytes, order).unwrap(),
                Some(field.clone()),
                "{field:?} in {order:?}"
            );
        }
    }

    const ALL_SCALARS: [ScalarType; 12] = [
        ScalarType::Bool,
        ScalarType::Byte,
        ScalarType::UByte,
        ScalarType::Short,
        ScalarType::UShort,
        ScalarType::Int,
        ScalarType::UInt,
        ScalarType::Long,
        ScalarType::ULong,
        ScalarType::Float,
        ScalarType::Double,
        ScalarType::String,
    ];

    /// The two fixtures the specification gives for the scalar type byte.
    #[test]
    fn scalar_type_byte_fixtures() {
        assert_eq!(type_code(&Field::Scalar(ScalarType::Int)), Some(0x22));
        assert_eq!(type_code(&Field::Scalar(ScalarType::ULong)), Some(0x27));

        // ... and the rest of the layout those two imply
        assert_eq!(type_code(&Field::Scalar(ScalarType::Bool)), Some(0x00));
        assert_eq!(type_code(&Field::Scalar(ScalarType::Byte)), Some(0x20));
        assert_eq!(type_code(&Field::Scalar(ScalarType::Short)), Some(0x21));
        assert_eq!(type_code(&Field::Scalar(ScalarType::Long)), Some(0x23));
        assert_eq!(type_code(&Field::Scalar(ScalarType::UByte)), Some(0x24));
        assert_eq!(type_code(&Field::Scalar(ScalarType::UShort)), Some(0x25));
        assert_eq!(type_code(&Field::Scalar(ScalarType::UInt)), Some(0x26));
        assert_eq!(type_code(&Field::Scalar(ScalarType::Float)), Some(0x42));
        assert_eq!(type_code(&Field::Scalar(ScalarType::Double)), Some(0x43));
        assert_eq!(type_code(&Field::Scalar(ScalarType::String)), Some(0x60));

        // A variable-size array sets bit 3
        assert_eq!(type_code(&Field::array_of(ScalarType::Int)), Some(0x2A));
        assert_eq!(type_code(&Field::array_of(ScalarType::String)), Some(0x68));

        // The complex forms, whose byte depends on more than the kind
        assert_eq!(type_code(&Field::Structure(StructureField::new())), None);
        assert_eq!(
            full_type_code(&Field::Structure(StructureField::new())),
            0x80
        );
        assert_eq!(full_type_code(&Field::Union(UnionField::new())), 0x81);
        assert_eq!(full_type_code(&Field::VariantUnion), 0x82);
        assert_eq!(
            full_type_code(&Field::array_of_structure(StructureField::new())),
            0x88
        );

        // Every code is in the inline range, so none can be mistaken for a special form
        for scalar_type in ALL_SCALARS {
            assert!(scalar_code(scalar_type, ARRAY_SCALAR) <= MAX_FULL_TYPE_CODE);
        }
    }

    /// Every scalar type, in every array form.
    #[test]
    fn every_scalar_type_and_array_form_round_trips() {
        for element_type in ALL_SCALARS {
            round_trip(&Field::Scalar(element_type));
            for size in [
                ArraySize::Variable,
                ArraySize::Bounded(16),
                ArraySize::Fixed(3),
                // A bound past the one-byte size encoding
                ArraySize::Bounded(1000),
            ] {
                round_trip(&Field::ScalarArray { element_type, size });
            }
        }

        // Bounded and fixed are distinguishable, and carry their bound
        let bounded = encode(
            &Field::ScalarArray {
                element_type: ScalarType::Double,
                size: ArraySize::Bounded(16),
            },
            ByteOrder::Little,
        );
        assert_eq!(bounded, [0x53, 0x10]);
        let fixed = encode(
            &Field::ScalarArray {
                element_type: ScalarType::Double,
                size: ArraySize::Fixed(16),
            },
            ByteOrder::Little,
        );
        assert_eq!(fixed, [0x5B, 0x10]);
        // A variable-size array has no bound to carry
        assert_eq!(
            encode(&Field::array_of(ScalarType::Double), ByteOrder::Little),
            [0x4B]
        );
    }

    /// A nested structure with a type ID, which is the case every normative type is.
    #[test]
    fn nested_structure_with_a_type_id_round_trips() {
        let ntscalar = Field::Structure(
            StructureField::with_id("epics:nt/NTScalar:1.0")
                .with("value", ScalarType::Double)
                .with(
                    "alarm",
                    StructureField::with_id("alarm_t")
                        .with("severity", ScalarType::Int)
                        .with("status", ScalarType::Int)
                        .with("message", ScalarType::String),
                )
                .with(
                    "timeStamp",
                    StructureField::with_id("time_t")
                        .with("secondsPastEpoch", ScalarType::Long)
                        .with("nanoseconds", ScalarType::Int)
                        .with("userTag", ScalarType::Int),
                ),
        );
        round_trip(&ntscalar);

        // Field order is part of the encoding, since members are positional
        let decoded = decode(&encode(&ntscalar, ByteOrder::Little), ByteOrder::Little)
            .unwrap()
            .unwrap();
        let Field::Structure(structure) = &decoded else {
            panic!("not a structure");
        };
        assert_eq!(
            structure.iter().map(|(n, _)| n).collect::<Vec<_>>(),
            ["value", "alarm", "timeStamp"]
        );
        assert_eq!(structure.id(), Some("epics:nt/NTScalar:1.0"));
        assert_eq!(
            structure.get_path("timeStamp.secondsPastEpoch"),
            Some(&Field::Scalar(ScalarType::Long))
        );
    }

    /// A structure with no id encodes an empty id string, and comes back with `None`.
    #[test]
    fn structures_without_an_id_round_trip() {
        let field = Field::Structure(
            StructureField::new()
                .with("user", ScalarType::String)
                .with("host", ScalarType::String),
        );
        round_trip(&field);

        // This is pvxs's CONNECTION_VALIDATION introspection, byte for byte
        assert_eq!(
            encode(&field, ByteOrder::Little),
            [
                0x80, // structure, no id form
                0x00, // empty type id
                0x02, // two fields
                0x04, b'u', b's', b'e', b'r', 0x60, // string
                0x04, b'h', b'o', b's', b't', 0x60,
            ]
        );

        assert_eq!(
            encode(&Field::Structure(StructureField::new()), ByteOrder::Little),
            [0x80, 0x00, 0x00]
        );
    }

    #[test]
    fn unions_and_structure_arrays_round_trip() {
        round_trip(&Field::VariantUnion);
        round_trip(&Field::Union(
            UnionField::with_id("any_t")
                .with("stringValue", ScalarType::String)
                .with("intValue", ScalarType::Int),
        ));
        round_trip(&Field::Union(UnionField::new()));

        round_trip(&Field::array_of_structure(
            StructureField::with_id("point_t")
                .with("x", ScalarType::Double)
                .with("y", ScalarType::Double),
        ));
        round_trip(&Field::StructureArray {
            element_type: Box::new(
                StructureField::with_id("point_t").with("x", ScalarType::Double),
            ),
            size: ArraySize::Bounded(8),
        });

        // A union may hold a structure, and a structure a union
        round_trip(&Field::Structure(StructureField::with_id("outer_t").with(
            "choice",
            UnionField::new().with(
                "nested",
                StructureField::with_id("inner_t").with("n", ScalarType::Int),
            ),
        )));
        round_trip(&Field::Union(
            UnionField::new().with("any", Field::VariantUnion),
        ));
    }

    /// The absent-field code, which is distinct from an empty structure.
    #[test]
    fn null_type_code_means_absent() {
        let mut writer = PvaWriter::new(ByteOrder::Little);
        encode_optional_field(None, &mut writer, &mut NoCache).unwrap();
        assert_eq!(writer.as_bytes(), [NULL_TYPE_CODE]);
        assert_eq!(decode(&[NULL_TYPE_CODE], ByteOrder::Little).unwrap(), None);

        // The strict form rejects it
        let mut reader = PvaReader::new(&[NULL_TYPE_CODE], ByteOrder::Little);
        assert!(matches!(
            decode_present_field(&mut reader, &mut NoCache).unwrap_err(),
            PvaError::Malformed(_)
        ));

        // ... and the optional encoder agrees with the plain one when a field is present
        let field = Field::Scalar(ScalarType::Int);
        let mut writer = PvaWriter::new(ByteOrder::Little);
        encode_optional_field(Some(&field), &mut writer, &mut NoCache).unwrap();
        assert_eq!(writer.as_bytes(), [0x22]);
    }

    /// `NoCache` encodes everything inline and cannot resolve an id.
    #[test]
    fn no_cache_encodes_inline_and_refuses_only_id() {
        let field = Field::Structure(StructureField::with_id("s").with("a", ScalarType::Int));
        // Encoded twice, and identical both times - nothing was registered
        assert_eq!(
            encode(&field, ByteOrder::Little),
            encode(&field, ByteOrder::Little)
        );
        assert_eq!(encode(&field, ByteOrder::Little)[0], 0x80);

        // A received ONLY_ID has nothing to resolve against
        let error = decode(&[ONLY_ID_TYPE_CODE, 0x01, 0x00], ByteOrder::Little).unwrap_err();
        assert!(matches!(error, PvaError::Malformed(_)));
        assert!(!error.is_incomplete());
    }

    /// FULL_WITH_ID decodes to the same field as the inline form, whatever the cache does
    /// with the id.
    #[test]
    fn full_with_id_decodes_like_the_inline_form() {
        let field = Field::Structure(
            StructureField::new()
                .with("user", ScalarType::String)
                .with("host", ScalarType::String),
        );
        let inline = encode(&field, ByteOrder::Little);

        // pvAccessCPP's form: fd 0100 then the same bytes
        let mut with_id = vec![FULL_WITH_ID_TYPE_CODE, 0x01, 0x00];
        with_id.extend_from_slice(&inline);
        assert_eq!(
            decode(&with_id, ByteOrder::Little).unwrap(),
            Some(field.clone())
        );

        // The id is byte-order sensitive, being a u16
        let mut big_endian = vec![FULL_WITH_ID_TYPE_CODE, 0x00, 0x01];
        big_endian.extend_from_slice(&encode(&field, ByteOrder::Big));
        assert_eq!(decode(&big_endian, ByteOrder::Big).unwrap(), Some(field));
    }

    /// The tagged form is refused rather than guessed at.
    #[test]
    fn tagged_id_form_is_refused_not_guessed() {
        let error = decode(
            &[FULL_TAGGED_ID_TYPE_CODE, 0x01, 0x00, 0x80],
            ByteOrder::Little,
        )
        .unwrap_err();
        let PvaError::Malformed(message) = &error else {
            panic!("expected malformed, got {error:?}");
        };
        assert!(message.contains("0xFC"), "{message}");
    }

    /// A bounded string is accepted, losing its bound, rather than dropping the connection.
    #[test]
    fn bounded_strings_decode_leniently() {
        // 0x83 = complex, scalar, bounded string; then the bound
        assert_eq!(
            decode(&[0x83, 0x28], ByteOrder::Little).unwrap(),
            Some(Field::Scalar(ScalarType::String))
        );
        // ... and as an array
        assert_eq!(
            decode(&[0x8B, 0x28], ByteOrder::Little).unwrap(),
            Some(Field::array_of(ScalarType::String))
        );
    }

    #[test]
    fn unknown_and_reserved_codes_are_malformed() {
        for bytes in [
            // Boolean with a non-zero detail
            vec![0x01],
            // Float with an integer's width bits
            vec![0x40],
            vec![0x41],
            // String with a non-zero detail
            vec![0x61],
            // Kind 101, 110, 111 - not defined
            vec![0xA0],
            vec![0xC0],
            // Reserved: above the inline range but not a known special form
            vec![0xE0],
            vec![0xFB],
            // A union array, which `Field` cannot express
            vec![0x89, 0x00, 0x00],
            // A variant union array, likewise
            vec![0x8A],
        ] {
            let error = decode(&bytes, ByteOrder::Little).unwrap_err();
            assert!(
                matches!(error, PvaError::Malformed(_)),
                "{bytes:02x?} gave {error:?}"
            );
        }
    }

    /// Truncation anywhere in a nested description is incomplete, not malformed.
    #[test]
    fn truncated_introspection_is_incomplete() {
        let field = Field::Structure(
            StructureField::with_id("epics:nt/NTScalar:1.0")
                .with("value", ScalarType::Double)
                .with(
                    "timeStamp",
                    StructureField::with_id("time_t").with("secondsPastEpoch", ScalarType::Long),
                ),
        );
        let complete = encode(&field, ByteOrder::Little);
        for length in 1..complete.len() {
            let mut reader = PvaReader::new(&complete[..length], ByteOrder::Little);
            let error = decode_field(&mut reader, &mut NoCache).unwrap_err();
            assert!(
                error.is_incomplete(),
                "truncating to {length} bytes gave {error:?}"
            );
        }
        // The full thing is fine
        assert_eq!(decode(&complete, ByteOrder::Little).unwrap(), Some(field));
    }

    /// A structure claiming more fields than follow is incomplete rather than looping.
    #[test]
    fn overlong_field_counts_are_incomplete() {
        // structure, no id, claims 200 fields, provides none
        let error = decode(&[0x80, 0x00, 0xC8], ByteOrder::Little).unwrap_err();
        assert!(error.is_incomplete(), "{error:?}");
    }
}
