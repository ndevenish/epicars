//! The four primitive encodings pvAccess builds everything else out of.
//!
//! Size, String, BitSet and Status. The [Data Encoding] document gives byte vectors for
//! each, and those are used directly as test fixtures below - they are the cheapest
//! correctness check available for this layer.
//!
//! Size and String extend [`PvaReader`] and [`PvaWriter`] with further inherent methods,
//! rather than being free functions, so that a decoder reads
//! `reader.read_string()?` alongside `reader.read_i32()?`. The impl blocks are split
//! across two files, which is unusual but keeps the byte-order plumbing in one place and
//! the encodings in another.
//!
//! [Data Encoding]:
//!     https://docs.epics-controls.org/en/latest/pv-access/Protocol-Encoding.html

use crate::pva::io::{PvaDecode, PvaEncode, PvaError, PvaReader, PvaWriter};

/// Marks a null size or string.
const SIZE_NULL: u8 = 0xFF;
/// Marks a size that did not fit in one byte.
const SIZE_ESCAPE: u8 = 0xFE;

impl PvaReader<'_> {
    /// Read pvAccess's variable-length size encoding.
    ///
    /// | Value | Encoding |
    /// |---|---|
    /// | null | `0xFF` |
    /// | 0 - 253 | single byte |
    /// | 254 - 2³¹−2 | `0xFE` then `i32` |
    /// | ≥ 2³¹−1 | `0xFE`, then `i32` = 2³¹−1, then `i64` |
    ///
    /// `None` is null, which is distinct from a size of zero: a null array is absent, an
    /// empty array is present and has no elements.
    pub fn read_size(&mut self) -> Result<Option<usize>, PvaError> {
        let first = self.read_u8()?;
        match first {
            SIZE_NULL => Ok(None),
            SIZE_ESCAPE => {
                let size = self.read_i32()?;
                if size < 0 {
                    return Err(PvaError::malformed(format!("negative size {size}")));
                }
                if size == i32::MAX {
                    // Escalation: the real size is in the i64 that follows
                    let size = self.read_i64()?;
                    if size < 0 {
                        return Err(PvaError::malformed(format!("negative size {size}")));
                    }
                    return usize::try_from(size)
                        .map(Some)
                        .map_err(|_| PvaError::OutOfRange(format!("size {size} exceeds usize")));
                }
                Ok(Some(size as usize))
            }
            small => Ok(Some(small as usize)),
        }
    }

    /// Read a size that must be present, rejecting null.
    pub fn read_present_size(&mut self) -> Result<usize, PvaError> {
        self.read_size()?
            .ok_or_else(|| PvaError::malformed("null size where a size is required"))
    }

    /// Read a UTF-8 string.
    ///
    /// The size prefix is a **byte** count, not a character count, so multi-byte
    /// characters cannot be split by it - and a size that lands mid-character shows up as
    /// [`PvaError::NotUtf8`] rather than as silently truncated text.
    ///
    /// A null string is an error here; use [`PvaReader::read_optional_string`] where null
    /// is meaningful.
    pub fn read_string(&mut self) -> Result<String, PvaError> {
        let size = self.read_present_size()?;
        Ok(String::from_utf8(self.read_bytes(size)?.to_vec())?)
    }

    /// Read a string that may be null, `0xFF` being distinct from an empty string.
    pub fn read_optional_string(&mut self) -> Result<Option<String>, PvaError> {
        let Some(size) = self.read_size()? else {
            return Ok(None);
        };
        Ok(Some(String::from_utf8(self.read_bytes(size)?.to_vec())?))
    }
}

impl PvaWriter {
    /// Write pvAccess's variable-length size encoding. See [`PvaReader::read_size`].
    pub fn write_size(&mut self, size: Option<usize>) {
        match size {
            None => self.write_u8(SIZE_NULL),
            Some(size) if size < 254 => self.write_u8(size as u8),
            Some(size) if size < i32::MAX as usize => {
                self.write_u8(SIZE_ESCAPE);
                self.write_i32(size as i32);
            }
            Some(size) => {
                self.write_u8(SIZE_ESCAPE);
                self.write_i32(i32::MAX);
                self.write_i64(size as i64);
            }
        }
    }

    /// Write a UTF-8 string: a **byte** count, then the bytes.
    ///
    /// An empty string encodes as size 0, which is not the same as null - see
    /// [`PvaWriter::write_optional_string`].
    pub fn write_string(&mut self, value: &str) {
        self.write_size(Some(value.len()));
        self.write_bytes(value.as_bytes());
    }

    /// Write a string or an explicit null.
    pub fn write_optional_string(&mut self, value: Option<&str>) {
        match value {
            None => self.write_size(None),
            Some(value) => self.write_string(value),
        }
    }
}

impl PvaDecode for String {
    fn decode(reader: &mut PvaReader<'_>) -> Result<String, PvaError> {
        reader.read_string()
    }
}

impl PvaEncode for str {
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
        writer.write_string(self);
        Ok(())
    }
}

impl PvaEncode for String {
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
        writer.write_string(self);
        Ok(())
    }
}

/// A set of bit indices, as pvAccess encodes changed-field masks.
///
/// The wire form is a byte count followed by that many bytes, where bit *n* lives in byte
/// *n / 8* at position *n % 8* - so bits serialise in groups of eight in ascending order,
/// LSB to MSB within each byte. Trailing zero bytes are not sent, so the length depends on
/// the highest set bit and an empty set is a single zero byte.
///
/// ```
/// use epicars::pva::encoding::BitSet;
/// use epicars::pva::io::{ByteOrder, PvaEncode};
///
/// // The two fixtures from the specification
/// assert_eq!(
///     BitSet::from_bits([0]).encode_to_vec(ByteOrder::Little).unwrap(),
///     [0x01, 0x01]
/// );
/// assert_eq!(
///     BitSet::from_bits([63]).encode_to_vec(ByteOrder::Little).unwrap(),
///     [0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
/// );
/// ```
///
/// # A note on big-endian connections
///
/// Complete 8-byte groups go through [`PvaWriter::write_u64`], so they follow the
/// connection's byte order, while the ragged tail is always written LSB-first. That is what
/// the specification describes ("zero or more `u64`, then between zero and seven trailing
/// `u8`") and what pvData's *reader* does. pvData's *writer* differs: it emits the final
/// word byte-wise even when that word is complete, so its reader and writer disagree with
/// each other on a big-endian connection. On a little-endian connection - which is what
/// both reference implementations negotiate in practice, and what the captures show - every
/// reading of this is byte-identical, so the divergence is unreachable. Should a
/// big-endian peer ever appear, this is the first place to look.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct BitSet {
    /// Bits 0-63 in `words[0]`, 64-127 in `words[1]`, and so on. Never has a trailing
    /// zero word, so `words.len()` is meaningful.
    words: Vec<u64>,
}

impl BitSet {
    pub fn new() -> BitSet {
        BitSet::default()
    }

    /// A set containing exactly the given bit indices.
    pub fn from_bits(bits: impl IntoIterator<Item = usize>) -> BitSet {
        let mut set = BitSet::new();
        for bit in bits {
            set.set(bit);
        }
        set
    }

    /// A set with bits `0..count` set.
    ///
    /// This is the all-ones changed-mask a monitor update needs when the provider can only
    /// say *that* a PV changed and not which of its fields did.
    pub fn filled(count: usize) -> BitSet {
        BitSet::from_bits(0..count)
    }

    pub fn set(&mut self, bit: usize) {
        let word = bit / 64;
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        self.words[word] |= 1u64 << (bit % 64);
    }

    pub fn clear(&mut self, bit: usize) {
        let word = bit / 64;
        if word < self.words.len() {
            self.words[word] &= !(1u64 << (bit % 64));
            self.trim();
        }
    }

    pub fn get(&self, bit: usize) -> bool {
        self.words
            .get(bit / 64)
            .is_some_and(|word| word & (1u64 << (bit % 64)) != 0)
    }

    pub fn is_empty(&self) -> bool {
        self.words.is_empty()
    }

    /// How many bits are set.
    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// The set bit indices, ascending.
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(index, word)| {
            (0..64)
                .filter(move |bit| word & (1u64 << bit) != 0)
                .map(move |bit| index * 64 + bit)
        })
    }

    /// How many bytes this set occupies on the wire, excluding the size prefix.
    pub fn byte_len(&self) -> usize {
        match self.words.last() {
            None => 0,
            Some(last) => (self.words.len() - 1) * 8 + (8 - (last.leading_zeros() as usize / 8)),
        }
    }

    /// Drop trailing zero words, so `words.len()` always reflects the highest set bit.
    fn trim(&mut self) {
        while self.words.last() == Some(&0) {
            self.words.pop();
        }
    }
}

impl PvaEncode for BitSet {
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
        let bytes = self.byte_len();
        writer.write_size(Some(bytes));
        // Complete 8-byte groups as words, then the ragged tail LSB-first
        for word in &self.words[..bytes / 8] {
            writer.write_u64(*word);
        }
        if !bytes.is_multiple_of(8) {
            let tail = self.words[bytes / 8];
            for shift in 0..bytes % 8 {
                writer.write_u8((tail >> (shift * 8)) as u8);
            }
        }
        Ok(())
    }
}

impl PvaDecode for BitSet {
    fn decode(reader: &mut PvaReader<'_>) -> Result<BitSet, PvaError> {
        let bytes = reader.read_present_size()?;
        let mut words = vec![0u64; bytes.div_ceil(8)];
        for word in words.iter_mut().take(bytes / 8) {
            *word = reader.read_u64()?;
        }
        if !bytes.is_multiple_of(8) {
            let mut tail = 0u64;
            for shift in 0..bytes % 8 {
                tail |= u64::from(reader.read_u8()?) << (shift * 8);
            }
            words[bytes / 8] = tail;
        }
        let mut set = BitSet { words };
        // A peer is free to send trailing zero bytes; normalise so equality is by content
        set.trim();
        Ok(set)
    }
}

/// How serious a [`Status`] is.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StatusType {
    #[default]
    Ok = 0,
    Warning = 1,
    Error = 2,
    Fatal = 3,
}

impl TryFrom<u8> for StatusType {
    type Error = PvaError;

    fn try_from(value: u8) -> Result<StatusType, PvaError> {
        match value {
            0 => Ok(StatusType::Ok),
            1 => Ok(StatusType::Warning),
            2 => Ok(StatusType::Error),
            3 => Ok(StatusType::Fatal),
            other => Err(PvaError::malformed(format!(
                "unknown status type {other:#04x}"
            ))),
        }
    }
}

/// The result of an operation: a severity, plus optional detail.
///
/// The common case - success with nothing to say - has a **one-byte shortcut**, `0xFF`,
/// and that is what a real server sends: the captured `CONNECTION_VALIDATED` message's
/// entire payload is the single byte `ff`.
///
/// ```
/// use epicars::pva::encoding::Status;
/// use epicars::pva::io::{ByteOrder, PvaEncode};
///
/// assert_eq!(Status::ok().encode_to_vec(ByteOrder::Little).unwrap(), [0xFF]);
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Status {
    pub status_type: StatusType,
    pub message: String,
    /// A stack trace or similar, from the peer. Usually empty.
    pub call_tree: String,
}

impl Status {
    /// Success, with nothing to say - the `0xFF` shortcut form.
    pub fn ok() -> Status {
        Status::default()
    }

    pub fn warning(message: impl Into<String>) -> Status {
        Status {
            status_type: StatusType::Warning,
            message: message.into(),
            call_tree: String::new(),
        }
    }

    pub fn error(message: impl Into<String>) -> Status {
        Status {
            status_type: StatusType::Error,
            message: message.into(),
            call_tree: String::new(),
        }
    }

    pub fn fatal(message: impl Into<String>) -> Status {
        Status {
            status_type: StatusType::Fatal,
            message: message.into(),
            call_tree: String::new(),
        }
    }

    /// Whether the operation succeeded. A `Warning` counts as success.
    pub fn is_success(&self) -> bool {
        matches!(self.status_type, StatusType::Ok | StatusType::Warning)
    }

    /// Whether this encodes to the single-byte `0xFF` form.
    pub fn is_shortcut(&self) -> bool {
        matches!(self.status_type, StatusType::Ok)
            && self.message.is_empty()
            && self.call_tree.is_empty()
    }
}

impl PvaEncode for Status {
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
        if self.is_shortcut() {
            writer.write_u8(SIZE_NULL);
            return Ok(());
        }
        writer.write_u8(self.status_type as u8);
        writer.write_string(&self.message);
        writer.write_string(&self.call_tree);
        Ok(())
    }
}

impl PvaDecode for Status {
    fn decode(reader: &mut PvaReader<'_>) -> Result<Status, PvaError> {
        let first = reader.read_u8()?;
        if first == SIZE_NULL {
            return Ok(Status::ok());
        }
        Ok(Status {
            status_type: StatusType::try_from(first)?,
            message: reader.read_string()?,
            call_tree: reader.read_string()?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::io::ByteOrder;

    const ORDERS: [ByteOrder; 2] = [ByteOrder::Little, ByteOrder::Big];

    fn write<F: FnOnce(&mut PvaWriter)>(order: ByteOrder, f: F) -> Vec<u8> {
        let mut writer = PvaWriter::new(order);
        f(&mut writer);
        writer.into_bytes()
    }

    /// Sizes, and the two boundaries where the encoding changes shape.
    #[test]
    fn size_encoding_and_its_boundaries() {
        for order in ORDERS {
            // Null is not zero
            assert_eq!(write(order, |w| w.write_size(None)), [0xFF]);
            assert_eq!(write(order, |w| w.write_size(Some(0))), [0x00]);

            // One byte up to 253, then the escape
            assert_eq!(write(order, |w| w.write_size(Some(253))), [0xFD]);
            let escaped = write(order, |w| w.write_size(Some(254)));
            assert_eq!(escaped.len(), 5);
            assert_eq!(escaped[0], 0xFE);

            for size in [
                None,
                Some(0),
                Some(1),
                Some(253),
                Some(254),
                Some(255),
                Some(1_000_000),
            ] {
                let bytes = write(order, |w| w.write_size(size));
                let mut reader = PvaReader::new(&bytes, order);
                assert_eq!(reader.read_size().unwrap(), size, "{size:?} in {order:?}");
                assert!(reader.is_empty());
            }
        }
    }

    /// The i32 to i64 escalation, at the boundary the specification puts it.
    #[test]
    fn huge_sizes_escalate_to_64_bits() {
        let boundary = i32::MAX as usize;
        for order in ORDERS {
            // Just below the boundary: one i32, no escalation
            let below = write(order, |w| w.write_size(Some(boundary - 1)));
            assert_eq!(below.len(), 5);
            assert_eq!(
                PvaReader::new(&below, order).read_size().unwrap(),
                Some(boundary - 1)
            );

            // At and above it: i32::MAX as a marker, then the real size as an i64
            for size in [boundary, boundary + 1, u32::MAX as usize] {
                let bytes = write(order, |w| w.write_size(Some(size)));
                assert_eq!(bytes.len(), 13, "escalated form is 1 + 4 + 8 bytes");
                assert_eq!(bytes[0], 0xFE);
                let mut reader = PvaReader::new(&bytes, order);
                assert_eq!(reader.read_size().unwrap(), Some(size));
                assert!(reader.is_empty());
            }
        }
    }

    #[test]
    fn negative_sizes_are_malformed() {
        for order in ORDERS {
            // 0xFE followed by a negative i32
            let bytes = write(order, |w| {
                w.write_u8(0xFE);
                w.write_i32(-1);
            });
            assert!(matches!(
                PvaReader::new(&bytes, order).read_size().unwrap_err(),
                PvaError::Malformed(_)
            ));

            // ... and the escalated form with a negative i64
            let bytes = write(order, |w| {
                w.write_u8(0xFE);
                w.write_i32(i32::MAX);
                w.write_i64(-1);
            });
            assert!(matches!(
                PvaReader::new(&bytes, order).read_size().unwrap_err(),
                PvaError::Malformed(_)
            ));
        }

        // A null where a size is required
        assert!(matches!(
            PvaReader::new(&[0xFF], ByteOrder::Little)
                .read_present_size()
                .unwrap_err(),
            PvaError::Malformed(_)
        ));
    }

    /// Strings are byte-counted, so multi-byte characters survive.
    #[test]
    fn strings_are_byte_counted_utf8() {
        for order in ORDERS {
            assert_eq!(write(order, |w| w.write_string("")), [0x00]);
            assert_eq!(
                write(order, |w| w.write_string("anonymous")),
                [0x09, b'a', b'n', b'o', b'n', b'y', b'm', b'o', b'u', b's']
            );

            // Three characters, seven bytes - the size is bytes, not characters
            let text = "aé☃";
            assert_eq!(text.chars().count(), 3);
            let bytes = write(order, |w| w.write_string(text));
            assert_eq!(bytes[0], 6);
            assert_eq!(bytes.len(), 7);
            assert_eq!(
                PvaReader::new(&bytes, order).read_string().unwrap(),
                text.to_string()
            );

            // Null and empty are different things
            assert_eq!(write(order, |w| w.write_optional_string(None)), [0xFF]);
            assert_eq!(write(order, |w| w.write_optional_string(Some(""))), [0x00]);
            let mut reader = PvaReader::new(&[0xFF, 0x00], order);
            assert_eq!(reader.read_optional_string().unwrap(), None);
            assert_eq!(reader.read_optional_string().unwrap(), Some(String::new()));
        }
    }

    #[test]
    fn strings_that_are_not_utf8_are_rejected() {
        // A size that lands mid-character rather than silently truncating
        let bytes = [0x01, 0xC3];
        assert!(matches!(
            PvaReader::new(&bytes, ByteOrder::Little)
                .read_string()
                .unwrap_err(),
            PvaError::NotUtf8(_)
        ));

        // A string longer than the input is incomplete, not malformed
        assert!(
            PvaReader::new(&[0x05, b'a'], ByteOrder::Little)
                .read_string()
                .unwrap_err()
                .is_incomplete()
        );
    }

    /// The two BitSet fixtures from the specification.
    #[test]
    fn bitset_specification_fixtures() {
        assert_eq!(
            BitSet::from_bits([0])
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0x01, 0x01]
        );
        assert_eq!(
            BitSet::from_bits([63])
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80]
        );

        // ... and they decode back
        assert_eq!(
            BitSet::decode_all(&[0x01, 0x01], ByteOrder::Little).unwrap(),
            BitSet::from_bits([0])
        );
        assert_eq!(
            BitSet::decode_all(
                &[0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80],
                ByteOrder::Little
            )
            .unwrap(),
            BitSet::from_bits([63])
        );
    }

    /// Bits go LSB to MSB within each byte, and bytes ascend.
    #[test]
    fn bitset_bit_order_within_and_across_bytes() {
        // Bit 0 is the low bit of byte 0; bit 7 the high bit; bit 8 the low bit of byte 1
        assert_eq!(
            BitSet::from_bits([0, 7, 8])
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0x02, 0x81, 0x01]
        );
        // Trailing zero bytes are not sent
        assert_eq!(
            BitSet::from_bits([1])
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0x01, 0x02]
        );
        // An empty set is a single zero byte
        assert_eq!(
            BitSet::new().encode_to_vec(ByteOrder::Little).unwrap(),
            [0x00]
        );
        assert_eq!(
            BitSet::decode_all(&[0x00], ByteOrder::Little).unwrap(),
            BitSet::new()
        );
    }

    #[test]
    fn bitsets_round_trip_in_both_orders() {
        let sets = [
            BitSet::new(),
            BitSet::from_bits([0]),
            BitSet::from_bits([1, 2, 3]),
            BitSet::from_bits([7, 8]),
            BitSet::from_bits([63]),
            BitSet::from_bits([64]),
            // Spans a whole word and a ragged tail
            BitSet::from_bits([0, 63, 64, 130]),
            BitSet::filled(1),
            BitSet::filled(8),
            BitSet::filled(64),
            BitSet::filled(100),
        ];
        for order in ORDERS {
            for set in &sets {
                let bytes = set.encode_to_vec(order).unwrap();
                assert_eq!(
                    &BitSet::decode_all(&bytes, order).unwrap(),
                    set,
                    "{set:?} in {order:?}"
                );
            }
        }
    }

    #[test]
    fn bitset_accessors() {
        let mut set = BitSet::from_bits([3, 70]);
        assert!(set.get(3) && set.get(70));
        assert!(!set.get(0) && !set.get(4) && !set.get(1000));
        assert_eq!(set.count(), 2);
        assert_eq!(set.iter().collect::<Vec<_>>(), [3, 70]);
        assert_eq!(set.byte_len(), 9);
        assert!(!set.is_empty());

        // Clearing the highest bit shortens the encoding again
        set.clear(70);
        assert_eq!(set.byte_len(), 1);
        assert_eq!(set.iter().collect::<Vec<_>>(), [3]);
        set.clear(3);
        assert!(set.is_empty());
        assert_eq!(set.byte_len(), 0);
        // Clearing a bit that was never set, in a word that does not exist
        set.clear(500);
        assert!(set.is_empty());

        // The all-ones mask a monitor update sends
        assert_eq!(BitSet::filled(3).iter().collect::<Vec<_>>(), [0, 1, 2]);
        assert_eq!(BitSet::filled(0), BitSet::new());
    }

    /// A peer may send trailing zero bytes; the decoded value must still compare equal.
    #[test]
    fn bitsets_normalise_trailing_zeros_on_decode() {
        let padded = BitSet::decode_all(&[0x03, 0x01, 0x00, 0x00], ByteOrder::Little).unwrap();
        assert_eq!(padded, BitSet::from_bits([0]));
        // ... and re-encodes in the short form
        assert_eq!(
            padded.encode_to_vec(ByteOrder::Little).unwrap(),
            [0x01, 0x01]
        );
    }

    /// The Status fixture from the specification, plus the shortcut a real server sends.
    #[test]
    fn status_specification_fixtures() {
        let warning = Status {
            status_type: StatusType::Warning,
            message: "Low memory".to_string(),
            call_tree: String::new(),
        };
        let expected = [
            0x01, 0x0A, 0x4C, 0x6F, 0x77, 0x20, 0x6D, 0x65, 0x6D, 0x6F, 0x72, 0x79, 0x00,
        ];
        assert_eq!(warning.encode_to_vec(ByteOrder::Little).unwrap(), expected);
        assert_eq!(
            Status::decode_all(&expected, ByteOrder::Little).unwrap(),
            warning
        );
        assert_eq!(Status::warning("Low memory"), warning);

        // OK with no message is one byte - the whole CONNECTION_VALIDATED payload
        assert_eq!(
            Status::ok().encode_to_vec(ByteOrder::Little).unwrap(),
            [0xFF]
        );
        assert_eq!(
            Status::decode_all(&[0xFF], ByteOrder::Little).unwrap(),
            Status::ok()
        );
        assert!(Status::ok().is_shortcut());
    }

    #[test]
    fn statuses_round_trip_in_both_orders() {
        let statuses = [
            Status::ok(),
            // An OK that carries a message cannot use the shortcut
            Status {
                status_type: StatusType::Ok,
                message: "fine, but".to_string(),
                call_tree: String::new(),
            },
            Status::warning("Low memory"),
            Status::error("No such channel"),
            Status::fatal("Give up"),
            Status {
                status_type: StatusType::Error,
                message: "failed".to_string(),
                call_tree: "at some::place\nand another".to_string(),
            },
        ];
        for order in ORDERS {
            for status in &statuses {
                let bytes = status.encode_to_vec(order).unwrap();
                assert_eq!(
                    &Status::decode_all(&bytes, order).unwrap(),
                    status,
                    "{status:?} in {order:?}"
                );
            }
        }

        assert!(Status::ok().is_success());
        assert!(Status::warning("x").is_success());
        assert!(!Status::error("x").is_success());
        assert!(!Status::fatal("x").is_success());
        assert!(!Status::warning("x").is_shortcut());
        assert!(StatusType::Ok < StatusType::Fatal);
    }

    #[test]
    fn unknown_status_types_are_malformed() {
        assert!(matches!(
            Status::decode_all(&[0x04, 0x00, 0x00], ByteOrder::Little).unwrap_err(),
            PvaError::Malformed(_)
        ));
        assert!(StatusType::try_from(0).is_ok());
        assert!(StatusType::try_from(3).is_ok());
        assert!(StatusType::try_from(0xFE).is_err());
    }

    /// The generic trait impls for strings, so `reader.read()` works for them too.
    #[test]
    fn strings_implement_the_encode_decode_traits() {
        let bytes = "ca".encode_to_vec(ByteOrder::Big).unwrap();
        assert_eq!(bytes, [0x02, b'c', b'a']);
        assert_eq!(String::decode_all(&bytes, ByteOrder::Big).unwrap(), "ca");
        assert_eq!(
            "ca".to_string().encode_to_vec(ByteOrder::Big).unwrap(),
            bytes
        );
    }
}
