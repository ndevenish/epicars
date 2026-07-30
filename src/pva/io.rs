//! Byte-order-aware reading and writing, and this module's error type.
//!
//! **Everything else in `pva` is written against this.** CA is fixed big-endian, so
//! [`crate::messages`] can use nom's `be_*` parsers throughout. pvAccess **negotiates
//! byte order per connection**, via the `SET_BYTE_ORDER` control message and bit 7 of
//! each message's flags byte.
//!
//! # Why this is not nom
//!
//! nom splits number parsing into `be_*` and `le_*` families, so a parser written with it
//! is either duplicated per byte order or made generic over the number parser - and the
//! `::<&[u8], nom::error::Error<&[u8]>>` turbofish noise already visible in about eight
//! places in `messages.rs` gets worse either way. The two alternatives considered were a
//! generic const byte-order parameter, which infects every type signature in the module,
//! and two generated parser sets, which doubles the surface to test.
//!
//! Instead the byte order lives *on the reader and writer*, set once per connection or per
//! message, so every [`PvaDecode`] implementation is written once and is order-agnostic by
//! construction.
//!
//! This is a deliberate, approved departure from the CA module's style, scoped to `pva`.
//! **CA keeps nom; do not migrate it.**
//!
//! ```
//! use epicars::pva::io::{ByteOrder, PvaReader, PvaWriter};
//!
//! // The same logical value, written in each byte order
//! for order in [ByteOrder::Little, ByteOrder::Big] {
//!     let mut writer = PvaWriter::new(order);
//!     writer.write_i32(0x0A0B0C0D);
//!     let mut reader = PvaReader::new(writer.as_bytes(), order);
//!     assert_eq!(reader.read_i32().unwrap(), 0x0A0B0C0D);
//! }
//! ```
//!
//! # Incomplete is not an error
//!
//! [`PvaError`] distinguishes **incomplete** input from **malformed** input, which nom
//! gives for free via `Err::Incomplete` and which has to be explicit here. The framed
//! codec depends on it: incomplete maps to `Ok(None)` - "call me again with more bytes" -
//! while malformed is a real decode error. Confusing the two produces either a connection
//! that drops on every partial read, or one that spins forever on a corrupt frame.

use std::string::FromUtf8Error;

/// Which end of a multi-byte number goes first.
///
/// Negotiated per connection, so it is a runtime value rather than a type parameter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ByteOrder {
    #[default]
    Little,
    Big,
}

impl ByteOrder {
    /// This machine's own byte order, which is what a server should offer.
    pub fn native() -> ByteOrder {
        if cfg!(target_endian = "big") {
            ByteOrder::Big
        } else {
            ByteOrder::Little
        }
    }

    /// The byte order a flags byte with bit 7 set (or clear) selects.
    pub fn from_flag(big_endian: bool) -> ByteOrder {
        if big_endian {
            ByteOrder::Big
        } else {
            ByteOrder::Little
        }
    }

    /// Whether this is the byte order that flags bit 7 marks.
    pub fn is_big(&self) -> bool {
        matches!(self, ByteOrder::Big)
    }
}

/// Anything that can go wrong reading or writing pvAccess data.
///
/// Mirrors the *shape* of [`crate::messages::MessageError`] - the parser library does not
/// transfer, but the error taxonomy does - with one addition that is central rather than
/// incidental: [`PvaError::Incomplete`] means "not enough bytes **yet**", and is not a
/// failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PvaError {
    /// Not a failure: the input ran out mid-value. Read more and try again.
    ///
    /// `needed` is how many further bytes *this* read wanted, which is a lower bound on
    /// what the whole message needs, not a promise.
    #[error("Incomplete input: {needed} more byte(s) needed")]
    Incomplete { needed: usize },
    /// The bytes are present but do not mean anything.
    #[error("Malformed input: {0}")]
    Malformed(String),
    /// A command ID this implementation does not handle.
    ///
    /// Recoverable by design: an unknown command must not kill the connection.
    #[error("Unknown command ID: {0:#04x}")]
    UnknownCommand(u8),
    /// A string field that is not UTF-8. pvAccess specifies string contents as UTF-8.
    #[error("String field is not valid UTF-8: {0}")]
    NotUtf8(String),
    /// A value that cannot be represented in the type it is being decoded into.
    #[error("Value out of range: {0}")]
    OutOfRange(String),
}

impl PvaError {
    /// Whether this means "come back with more bytes" rather than "this is broken".
    pub fn is_incomplete(&self) -> bool {
        matches!(self, PvaError::Incomplete { .. })
    }

    pub fn malformed(message: impl Into<String>) -> PvaError {
        PvaError::Malformed(message.into())
    }
}

impl From<FromUtf8Error> for PvaError {
    fn from(error: FromUtf8Error) -> PvaError {
        PvaError::NotUtf8(error.to_string())
    }
}

/// Reads pvAccess data out of a byte slice in a given byte order.
///
/// Tracks a position rather than reslicing, so [`PvaReader::position`] can be used for the
/// 64-bit alignment that segmentation preserves.
#[derive(Clone, Debug)]
pub struct PvaReader<'a> {
    buf: &'a [u8],
    pos: usize,
    order: ByteOrder,
}

impl<'a> PvaReader<'a> {
    pub fn new(buf: &'a [u8], order: ByteOrder) -> PvaReader<'a> {
        PvaReader { buf, pos: 0, order }
    }

    pub fn order(&self) -> ByteOrder {
        self.order
    }

    /// Change byte order mid-stream, as `SET_BYTE_ORDER` does.
    pub fn set_order(&mut self, order: ByteOrder) {
        self.order = order;
    }

    /// How many bytes have been consumed.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// How many bytes are left.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    /// The unconsumed bytes, without consuming them.
    pub fn peek_remaining(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }

    /// Take `count` bytes.
    pub fn read_bytes(&mut self, count: usize) -> Result<&'a [u8], PvaError> {
        if self.remaining() < count {
            return Err(PvaError::Incomplete {
                needed: count - self.remaining(),
            });
        }
        let bytes = &self.buf[self.pos..self.pos + count];
        self.pos += count;
        Ok(bytes)
    }

    /// Discard `count` bytes.
    pub fn skip(&mut self, count: usize) -> Result<(), PvaError> {
        self.read_bytes(count).map(|_| ())
    }

    /// Discard padding up to the next multiple of `alignment` bytes.
    ///
    /// pvAccess aligns to 64-bit boundaries, and segmentation **preserves the padding
    /// between segments**, so this counts from the start of the buffer it was given.
    pub fn align_to(&mut self, alignment: usize) -> Result<(), PvaError> {
        let overshoot = self.pos % alignment;
        if overshoot == 0 {
            return Ok(());
        }
        self.skip(alignment - overshoot)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], PvaError> {
        let bytes: [u8; N] = self
            .read_bytes(N)?
            .try_into()
            .expect("read_bytes returned the length asked for");
        Ok(match self.order {
            ByteOrder::Big => bytes,
            ByteOrder::Little => {
                let mut reversed = bytes;
                reversed.reverse();
                reversed
            }
        })
    }

    pub fn read_u8(&mut self) -> Result<u8, PvaError> {
        Ok(self.read_bytes(1)?[0])
    }

    pub fn read_i8(&mut self) -> Result<i8, PvaError> {
        Ok(self.read_u8()? as i8)
    }

    /// pvAccess `boolean`: one byte, zero for false.
    pub fn read_bool(&mut self) -> Result<bool, PvaError> {
        Ok(self.read_u8()? != 0)
    }

    pub fn read_u16(&mut self) -> Result<u16, PvaError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    pub fn read_i16(&mut self) -> Result<i16, PvaError> {
        Ok(i16::from_be_bytes(self.read_array()?))
    }

    pub fn read_u32(&mut self) -> Result<u32, PvaError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    pub fn read_i32(&mut self) -> Result<i32, PvaError> {
        Ok(i32::from_be_bytes(self.read_array()?))
    }

    pub fn read_u64(&mut self) -> Result<u64, PvaError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    pub fn read_i64(&mut self) -> Result<i64, PvaError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    pub fn read_f32(&mut self) -> Result<f32, PvaError> {
        Ok(f32::from_be_bytes(self.read_array()?))
    }

    pub fn read_f64(&mut self) -> Result<f64, PvaError> {
        Ok(f64::from_be_bytes(self.read_array()?))
    }

    /// Decode any [`PvaDecode`] type, for symmetry with the explicit readers.
    pub fn read<T: PvaDecode>(&mut self) -> Result<T, PvaError> {
        T::decode(self)
    }
}

/// Writes pvAccess data into a `Vec<u8>` in a given byte order.
#[derive(Clone, Debug)]
pub struct PvaWriter {
    buf: Vec<u8>,
    order: ByteOrder,
}

impl PvaWriter {
    pub fn new(order: ByteOrder) -> PvaWriter {
        PvaWriter {
            buf: Vec::new(),
            order,
        }
    }

    /// A writer that will not reallocate for the first `capacity` bytes.
    pub fn with_capacity(order: ByteOrder, capacity: usize) -> PvaWriter {
        PvaWriter {
            buf: Vec::with_capacity(capacity),
            order,
        }
    }

    pub fn order(&self) -> ByteOrder {
        self.order
    }

    /// Change byte order mid-stream, as `SET_BYTE_ORDER` does.
    pub fn set_order(&mut self, order: ByteOrder) {
        self.order = order;
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        self.buf.extend_from_slice(bytes);
    }

    /// Write zero padding up to the next multiple of `alignment` bytes.
    pub fn align_to(&mut self, alignment: usize) {
        let overshoot = self.buf.len() % alignment;
        if overshoot != 0 {
            self.buf.resize(self.buf.len() + alignment - overshoot, 0);
        }
    }

    fn write_array<const N: usize>(&mut self, bytes: [u8; N]) {
        match self.order {
            ByteOrder::Big => self.buf.extend_from_slice(&bytes),
            ByteOrder::Little => self.buf.extend(bytes.iter().rev()),
        }
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buf.push(value);
    }

    pub fn write_i8(&mut self, value: i8) {
        self.buf.push(value as u8);
    }

    /// pvAccess `boolean`: one byte, 0 or 1.
    pub fn write_bool(&mut self, value: bool) {
        self.buf.push(u8::from(value));
    }

    pub fn write_u16(&mut self, value: u16) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_i16(&mut self, value: i16) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_u32(&mut self, value: u32) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_i32(&mut self, value: i32) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_f32(&mut self, value: f32) {
        self.write_array(value.to_be_bytes());
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write_array(value.to_be_bytes());
    }

    /// Encode any [`PvaEncode`] type, for symmetry with the explicit writers.
    pub fn write<T: PvaEncode + ?Sized>(&mut self, value: &T) -> Result<(), PvaError> {
        value.encode(self)
    }
}

/// Decode a value from a [`PvaReader`].
///
/// Implementations are byte-order-agnostic: the order is the reader's, not the type's.
pub trait PvaDecode: Sized {
    fn decode(reader: &mut PvaReader<'_>) -> Result<Self, PvaError>;

    /// Decode from a complete byte slice, requiring that nothing is left over.
    ///
    /// A convenience for tests and for payloads whose length is already known from the
    /// header.
    fn decode_all(bytes: &[u8], order: ByteOrder) -> Result<Self, PvaError> {
        let mut reader = PvaReader::new(bytes, order);
        let value = Self::decode(&mut reader)?;
        if !reader.is_empty() {
            return Err(PvaError::malformed(format!(
                "{} trailing byte(s) after value",
                reader.remaining()
            )));
        }
        Ok(value)
    }
}

/// Encode a value into a [`PvaWriter`].
pub trait PvaEncode {
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError>;

    /// Encode to a fresh byte vector in the given order.
    fn encode_to_vec(&self, order: ByteOrder) -> Result<Vec<u8>, PvaError> {
        let mut writer = PvaWriter::new(order);
        self.encode(&mut writer)?;
        Ok(writer.into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The point of the whole module: one implementation, both byte orders.
    ///
    /// `decode` is written once and never mentions endianness, so the same logical value
    /// decodes from two different byte sequences.
    #[test]
    fn the_same_value_decodes_from_both_byte_orders() {
        /// A struct whose decoder has no idea which byte order it is reading.
        #[derive(Debug, PartialEq)]
        struct Header {
            magic: u8,
            payload_size: i32,
            marker: u16,
        }
        impl PvaDecode for Header {
            fn decode(reader: &mut PvaReader<'_>) -> Result<Header, PvaError> {
                Ok(Header {
                    magic: reader.read_u8()?,
                    payload_size: reader.read_i32()?,
                    marker: reader.read_u16()?,
                })
            }
        }
        impl PvaEncode for Header {
            fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
                writer.write_u8(self.magic);
                writer.write_i32(self.payload_size);
                writer.write_u16(self.marker);
                Ok(())
            }
        }

        let expected = Header {
            magic: 0xCA,
            payload_size: 0x14,
            marker: 0xBEEF,
        };

        // Byte-for-byte fixtures, so this tests the encoding and not just self-consistency
        let little = [0xCA, 0x14, 0x00, 0x00, 0x00, 0xEF, 0xBE];
        let big = [0xCA, 0x00, 0x00, 0x00, 0x14, 0xBE, 0xEF];

        assert_eq!(
            Header::decode_all(&little, ByteOrder::Little).unwrap(),
            expected
        );
        assert_eq!(Header::decode_all(&big, ByteOrder::Big).unwrap(), expected);
        assert_eq!(expected.encode_to_vec(ByteOrder::Little).unwrap(), little);
        assert_eq!(expected.encode_to_vec(ByteOrder::Big).unwrap(), big);

        // Single bytes are the same in both, so only the multi-byte fields moved
        assert_ne!(little, big);
    }

    /// Every width, both orders, through the writer and back.
    #[test]
    fn every_width_round_trips_in_both_orders() {
        for order in [ByteOrder::Little, ByteOrder::Big] {
            let mut writer = PvaWriter::new(order);
            writer.write_u8(0x81);
            writer.write_i8(-2);
            writer.write_bool(true);
            writer.write_bool(false);
            writer.write_u16(0x8182);
            writer.write_i16(i16::MIN);
            writer.write_u32(0x81828384);
            writer.write_i32(i32::MIN);
            writer.write_u64(0x8182838485868788);
            writer.write_i64(i64::MIN);
            writer.write_f32(-1.5);
            writer.write_f64(f64::MAX);

            let bytes = writer.as_bytes().to_vec();
            let mut reader = PvaReader::new(&bytes, order);
            assert_eq!(reader.read_u8().unwrap(), 0x81);
            assert_eq!(reader.read_i8().unwrap(), -2);
            assert!(reader.read_bool().unwrap());
            assert!(!reader.read_bool().unwrap());
            assert_eq!(reader.read_u16().unwrap(), 0x8182);
            assert_eq!(reader.read_i16().unwrap(), i16::MIN);
            assert_eq!(reader.read_u32().unwrap(), 0x81828384);
            assert_eq!(reader.read_i32().unwrap(), i32::MIN);
            assert_eq!(reader.read_u64().unwrap(), 0x8182838485868788);
            assert_eq!(reader.read_i64().unwrap(), i64::MIN);
            assert_eq!(reader.read_f32().unwrap(), -1.5);
            assert_eq!(reader.read_f64().unwrap(), f64::MAX);
            assert!(reader.is_empty(), "consumed exactly what was written");
        }
    }

    /// Non-zero bytes read as true, and writing normalises to 0/1.
    #[test]
    fn booleans_are_one_byte() {
        let mut reader = PvaReader::new(&[0x00, 0x01, 0xFF], ByteOrder::Little);
        assert!(!reader.read_bool().unwrap());
        assert!(reader.read_bool().unwrap());
        assert!(reader.read_bool().unwrap());

        let mut writer = PvaWriter::new(ByteOrder::Little);
        writer.write_bool(true);
        writer.write_bool(false);
        assert_eq!(writer.as_bytes(), [0x01, 0x00]);
    }

    /// Running out of bytes is `Incomplete`, not `Malformed` - the distinction the framed
    /// codec is built on.
    #[test]
    fn running_out_of_input_is_incomplete_not_malformed() {
        // Three bytes into a four-byte read
        let mut reader = PvaReader::new(&[0x01, 0x02, 0x03], ByteOrder::Big);
        let error = reader.read_i32().unwrap_err();
        assert_eq!(error, PvaError::Incomplete { needed: 1 });
        assert!(error.is_incomplete());
        // A failed read consumes nothing, so it can be retried with more input
        assert_eq!(reader.position(), 0);
        assert_eq!(reader.remaining(), 3);

        // Nothing at all left
        let mut empty = PvaReader::new(&[], ByteOrder::Big);
        assert_eq!(
            empty.read_u8().unwrap_err(),
            PvaError::Incomplete { needed: 1 }
        );
        assert_eq!(
            empty.read_u64().unwrap_err(),
            PvaError::Incomplete { needed: 8 }
        );

        // Malformed is the other thing, and reports itself as such
        assert!(!PvaError::malformed("nope").is_incomplete());
        assert!(!PvaError::UnknownCommand(0x99).is_incomplete());
    }

    /// Trailing bytes are malformed for `decode_all`, which is the "the header told me how
    /// long this was" case.
    #[test]
    fn decode_all_rejects_trailing_bytes() {
        #[derive(Debug)]
        struct One(#[allow(dead_code)] u8);
        impl PvaDecode for One {
            fn decode(reader: &mut PvaReader<'_>) -> Result<One, PvaError> {
                Ok(One(reader.read_u8()?))
            }
        }

        assert!(One::decode_all(&[1], ByteOrder::Big).is_ok());
        let error = One::decode_all(&[1, 2, 3], ByteOrder::Big).unwrap_err();
        assert!(!error.is_incomplete());
        assert_eq!(
            error,
            PvaError::Malformed("2 trailing byte(s) after value".into())
        );
    }

    /// Alignment counts from the start of the buffer, since segmentation preserves the
    /// padding between segments.
    #[test]
    fn alignment_pads_to_the_next_boundary() {
        let mut writer = PvaWriter::new(ByteOrder::Little);
        writer.write_u8(1);
        writer.align_to(8);
        assert_eq!(writer.len(), 8);
        assert_eq!(writer.as_bytes(), [1, 0, 0, 0, 0, 0, 0, 0]);
        // Already aligned: a no-op, not another eight bytes
        writer.align_to(8);
        assert_eq!(writer.len(), 8);

        let bytes = writer.into_bytes();
        let mut reader = PvaReader::new(&bytes, ByteOrder::Little);
        assert_eq!(reader.read_u8().unwrap(), 1);
        reader.align_to(8).unwrap();
        assert_eq!(reader.position(), 8);
        reader.align_to(8).unwrap();
        assert_eq!(reader.position(), 8);
        assert!(reader.is_empty());

        // Aligning past the end of the input is incomplete, like any other read
        let mut short = PvaReader::new(&[1, 2], ByteOrder::Little);
        short.read_u8().unwrap();
        assert_eq!(
            short.align_to(8).unwrap_err(),
            PvaError::Incomplete { needed: 6 }
        );
    }

    /// Byte order can change mid-stream, which is what `SET_BYTE_ORDER` does.
    #[test]
    fn byte_order_can_change_mid_stream() {
        let mut writer = PvaWriter::new(ByteOrder::Big);
        writer.write_u16(0x0102);
        writer.set_order(ByteOrder::Little);
        writer.write_u16(0x0102);
        assert_eq!(writer.as_bytes(), [0x01, 0x02, 0x02, 0x01]);

        let bytes = writer.into_bytes();
        let mut reader = PvaReader::new(&bytes, ByteOrder::Big);
        assert_eq!(reader.read_u16().unwrap(), 0x0102);
        reader.set_order(ByteOrder::Little);
        assert_eq!(reader.read_u16().unwrap(), 0x0102);
        assert_eq!(reader.order(), ByteOrder::Little);
    }

    #[test]
    fn byte_order_maps_to_and_from_the_flag_bit() {
        assert_eq!(ByteOrder::from_flag(true), ByteOrder::Big);
        assert_eq!(ByteOrder::from_flag(false), ByteOrder::Little);
        assert!(ByteOrder::Big.is_big());
        assert!(!ByteOrder::Little.is_big());
        assert_eq!(ByteOrder::default(), ByteOrder::Little);
        // Whatever this machine is, it is one of the two
        assert!([ByteOrder::Big, ByteOrder::Little].contains(&ByteOrder::native()));
    }

    #[test]
    fn raw_bytes_and_skipping() {
        let mut reader = PvaReader::new(&[1, 2, 3, 4, 5], ByteOrder::Little);
        assert_eq!(reader.read_bytes(2).unwrap(), [1, 2]);
        assert_eq!(reader.peek_remaining(), [3, 4, 5]);
        reader.skip(1).unwrap();
        assert_eq!(reader.peek_remaining(), [4, 5]);
        assert_eq!(
            reader.read_bytes(3).unwrap_err(),
            PvaError::Incomplete { needed: 1 }
        );
        assert_eq!(reader.read_bytes(2).unwrap(), [4, 5]);

        let mut writer = PvaWriter::with_capacity(ByteOrder::Big, 16);
        assert!(writer.is_empty());
        writer.write_bytes(&[1, 2, 3]);
        assert_eq!(writer.as_bytes(), [1, 2, 3]);
    }

    /// The generic `read`/`write` helpers dispatch to the traits.
    #[test]
    fn generic_helpers_dispatch_to_the_traits() {
        struct Pair(u16, u16);
        impl PvaDecode for Pair {
            fn decode(reader: &mut PvaReader<'_>) -> Result<Pair, PvaError> {
                Ok(Pair(reader.read_u16()?, reader.read_u16()?))
            }
        }
        impl PvaEncode for Pair {
            fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
                writer.write_u16(self.0);
                writer.write_u16(self.1);
                Ok(())
            }
        }

        let mut writer = PvaWriter::new(ByteOrder::Big);
        writer.write(&Pair(1, 2)).unwrap();
        let bytes = writer.into_bytes();
        assert_eq!(bytes, [0, 1, 0, 2]);

        let mut reader = PvaReader::new(&bytes, ByteOrder::Big);
        let pair: Pair = reader.read().unwrap();
        assert_eq!((pair.0, pair.1), (1, 2));
    }

    #[test]
    fn utf8_errors_convert_into_pva_errors() {
        let error = PvaError::from(String::from_utf8(vec![0xFF, 0xFE]).unwrap_err());
        assert!(matches!(error, PvaError::NotUtf8(_)));
        assert!(!error.is_incomplete());
    }
}
