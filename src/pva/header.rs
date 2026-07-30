//! The pvAccess message header: eight bytes, always.
//!
//! Unlike CA's 16-byte header with its 32-byte extended form for large payloads, every
//! pvAccess message starts with exactly this:
//!
//! | Field | Size | Notes |
//! |---|---|---|
//! | magic | 1 | always `0xCA` |
//! | version | 1 | `0x02` on the wire today |
//! | flags | 1 | bit 0 app/control, bits 4-5 segmentation, bit 6 direction, bit 7 endianness |
//! | command | 1 | |
//! | payload size | 4 | `int32`, in the byte order flags bit 7 selects |
//!
//! Two consequences of the flags byte are worth stating plainly, because they remove
//! machinery that CA needs:
//!
//! - **Direction is a flag** (bit 6, set on server→client). CA overloads command IDs by
//!   direction - command 1 is both `EventAddResponse` and `EventCancelResponse`,
//!   disambiguated by a payload-size heuristic - which is why `messages.rs` has both a
//!   `Message` and a `ClientMessage` enum. pvAccess needs one enum.
//! - **Byte order is a flag** (bit 7), so the header is the thing that tells the reader
//!   how to read the rest of itself. [`PvaHeader::decode`] therefore *sets* the reader's
//!   byte order, and [`PvaHeader::encode`] sets the writer's, so the payload that follows
//!   is handled in the right order without the caller having to arrange it.
//!
//! The bytes below are from a real handshake, captured from base 7.0.8.1's `softIocPVA`:
//!
//! ```
//! use epicars::pva::header::{MessageKind, PvaHeader};
//! use epicars::pva::io::{ByteOrder, PvaDecode};
//!
//! // ca 02 41 02  00000000  - SET_BYTE_ORDER, a control message from the server
//! let header = PvaHeader::decode_all(&[0xCA, 0x02, 0x41, 0x02, 0, 0, 0, 0], ByteOrder::Little)
//!     .unwrap();
//! assert_eq!(header.kind, MessageKind::Control);
//! assert!(header.from_server());
//! assert_eq!(header.byte_order, ByteOrder::Little);
//! assert_eq!(header.payload_len(), 0, "control messages carry no payload");
//! ```

use crate::pva::io::{ByteOrder, PvaDecode, PvaEncode, PvaError, PvaReader, PvaWriter};

/// The first byte of every pvAccess message.
pub const PVA_MAGIC: u8 = 0xCA;

/// The protocol version this implementation speaks, and the one seen on the wire.
pub const PVA_VERSION: u8 = 0x02;

/// Every pvAccess header is this long. There is no extended form.
pub const HEADER_SIZE: usize = 8;

const FLAG_CONTROL: u8 = 0x01;
const FLAG_SEGMENT_MASK: u8 = 0x30;
const FLAG_SEGMENT_SHIFT: u32 = 4;
const FLAG_FROM_SERVER: u8 = 0x40;
const FLAG_BIG_ENDIAN: u8 = 0x80;

/// Whether a message is an application message or a transport control message.
///
/// Control messages interleave with application messages and are handled *below* the
/// message enum - they never reach the application. They also have no payload: their size
/// field carries data instead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum MessageKind {
    #[default]
    Application,
    Control,
}

/// Where a message sits in a segmented sequence (flags bits 4-5).
///
/// pvAccess messages align to 64-bit boundaries and may split across frames, **preserving
/// the alignment padding between segments**. Reassembly is 1.6's job; this type is only the
/// encoding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Segmentation {
    #[default]
    NotSegmented = 0,
    First = 1,
    Last = 2,
    Middle = 3,
}

impl Segmentation {
    fn from_bits(bits: u8) -> Segmentation {
        match bits {
            0 => Segmentation::NotSegmented,
            1 => Segmentation::First,
            2 => Segmentation::Last,
            _ => Segmentation::Middle,
        }
    }

    /// Whether this message is one piece of a larger one.
    pub fn is_segmented(&self) -> bool {
        !matches!(self, Segmentation::NotSegmented)
    }
}

/// Which way a message is travelling (flags bit 6).
///
/// Confirmed on the wire: set (`0x40`, `0x41`) on server→client, clear (`0x00`) on
/// client→server.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Direction {
    #[default]
    FromClient,
    FromServer,
}

/// A decoded pvAccess message header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PvaHeader {
    pub version: u8,
    pub kind: MessageKind,
    pub segmentation: Segmentation,
    pub direction: Direction,
    pub byte_order: ByteOrder,
    pub command: u8,
    /// For an application message, the number of payload bytes that follow.
    ///
    /// **For a control message this is not a size**: control messages have no payload, and
    /// the field carries the message's data instead - a byte count for `MARK_TOTAL_BYTE_SENT`
    /// and `ACK_TOTAL_BYTES_RECEIVED`, zero for the rest. Use [`PvaHeader::payload_len`] to
    /// frame, and [`PvaHeader::control_data`] to read it as data.
    pub payload_size: i32,
}

impl PvaHeader {
    /// A header for an application message from a server, in the given byte order.
    pub fn application(command: u8, byte_order: ByteOrder, payload_size: i32) -> PvaHeader {
        PvaHeader {
            version: PVA_VERSION,
            kind: MessageKind::Application,
            segmentation: Segmentation::NotSegmented,
            direction: Direction::FromServer,
            byte_order,
            command,
            payload_size,
        }
    }

    /// A header for a control message, whose size field carries `data` rather than a length.
    pub fn control(command: u8, byte_order: ByteOrder, data: i32) -> PvaHeader {
        PvaHeader {
            kind: MessageKind::Control,
            ..PvaHeader::application(command, byte_order, data)
        }
    }

    /// Builder form: mark this as travelling client→server.
    pub fn from_the_client(mut self) -> PvaHeader {
        self.direction = Direction::FromClient;
        self
    }

    /// Builder form for [`PvaHeader::segmentation`].
    pub fn segmented(mut self, segmentation: Segmentation) -> PvaHeader {
        self.segmentation = segmentation;
        self
    }

    pub fn from_server(&self) -> bool {
        matches!(self.direction, Direction::FromServer)
    }

    pub fn is_control(&self) -> bool {
        matches!(self.kind, MessageKind::Control)
    }

    /// How many payload bytes follow this header.
    ///
    /// Always 0 for a control message, whatever its size field says. Getting this wrong
    /// desynchronises the whole stream, since a `MARK_TOTAL_BYTE_SENT` carrying a byte
    /// count of 17408 would otherwise look like a 17 kB payload.
    ///
    /// Non-negative for any header that came from [`PvaHeader::decode`], which rejects a
    /// negative application payload size.
    pub fn payload_len(&self) -> usize {
        match self.kind {
            MessageKind::Control => 0,
            MessageKind::Application => self.payload_size.max(0) as usize,
        }
    }

    /// The size field read as control-message data; `None` for an application message.
    pub fn control_data(&self) -> Option<i32> {
        match self.kind {
            MessageKind::Control => Some(self.payload_size),
            MessageKind::Application => None,
        }
    }

    /// The flags byte this header encodes to.
    pub fn flags(&self) -> u8 {
        let mut flags = 0u8;
        if self.is_control() {
            flags |= FLAG_CONTROL;
        }
        flags |= (self.segmentation as u8) << FLAG_SEGMENT_SHIFT;
        if self.from_server() {
            flags |= FLAG_FROM_SERVER;
        }
        if self.byte_order.is_big() {
            flags |= FLAG_BIG_ENDIAN;
        }
        flags
    }
}

impl PvaDecode for PvaHeader {
    /// Decode a header, **setting `reader`'s byte order** from flags bit 7 so the payload
    /// that follows is read correctly.
    fn decode(reader: &mut PvaReader<'_>) -> Result<PvaHeader, PvaError> {
        // Peek rather than read, so a short header leaves the reader untouched and can be
        // retried when more bytes arrive
        if reader.remaining() < HEADER_SIZE {
            return Err(PvaError::Incomplete {
                needed: HEADER_SIZE - reader.remaining(),
            });
        }
        let magic = reader.read_u8()?;
        if magic != PVA_MAGIC {
            return Err(PvaError::malformed(format!(
                "expected magic {PVA_MAGIC:#04x}, got {magic:#04x}"
            )));
        }
        let version = reader.read_u8()?;
        let flags = reader.read_u8()?;
        let command = reader.read_u8()?;

        let byte_order = ByteOrder::from_flag(flags & FLAG_BIG_ENDIAN != 0);
        // The header tells us how to read the rest of itself, and the payload after it
        reader.set_order(byte_order);
        let payload_size = reader.read_i32()?;

        let kind = if flags & FLAG_CONTROL != 0 {
            MessageKind::Control
        } else {
            MessageKind::Application
        };
        if matches!(kind, MessageKind::Application) && payload_size < 0 {
            return Err(PvaError::malformed(format!(
                "negative payload size {payload_size}"
            )));
        }

        Ok(PvaHeader {
            version,
            kind,
            segmentation: Segmentation::from_bits(
                (flags & FLAG_SEGMENT_MASK) >> FLAG_SEGMENT_SHIFT,
            ),
            direction: if flags & FLAG_FROM_SERVER != 0 {
                Direction::FromServer
            } else {
                Direction::FromClient
            },
            byte_order,
            command,
            payload_size,
        })
    }
}

impl PvaEncode for PvaHeader {
    /// Encode a header, **setting `writer`'s byte order** to this header's, so the payload
    /// written next matches what the flags byte claims.
    fn encode(&self, writer: &mut PvaWriter) -> Result<(), PvaError> {
        writer.write_u8(PVA_MAGIC);
        writer.write_u8(self.version);
        writer.write_u8(self.flags());
        writer.write_u8(self.command);
        writer.set_order(self.byte_order);
        writer.write_i32(self.payload_size);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every combination of the four defined flags, in both byte orders.
    #[test]
    fn every_flag_combination_round_trips() {
        let kinds = [MessageKind::Application, MessageKind::Control];
        let segments = [
            Segmentation::NotSegmented,
            Segmentation::First,
            Segmentation::Last,
            Segmentation::Middle,
        ];
        let directions = [Direction::FromClient, Direction::FromServer];
        let orders = [ByteOrder::Little, ByteOrder::Big];

        let mut seen_flags = Vec::new();
        for kind in kinds {
            for segmentation in segments {
                for direction in directions {
                    for byte_order in orders {
                        let header = PvaHeader {
                            version: PVA_VERSION,
                            kind,
                            segmentation,
                            direction,
                            byte_order,
                            command: 0x0A,
                            payload_size: 0x0102_0304,
                        };
                        // The writer starts in the *other* order, to prove the header
                        // imposes its own rather than inheriting
                        let mut writer = PvaWriter::new(match byte_order {
                            ByteOrder::Big => ByteOrder::Little,
                            ByteOrder::Little => ByteOrder::Big,
                        });
                        header.encode(&mut writer).unwrap();
                        let bytes = writer.into_bytes();
                        assert_eq!(bytes.len(), HEADER_SIZE);

                        // Likewise the reader is given the wrong order to start with
                        let mut reader = PvaReader::new(&bytes, ByteOrder::default());
                        assert_eq!(PvaHeader::decode(&mut reader).unwrap(), header);
                        assert_eq!(
                            reader.order(),
                            byte_order,
                            "decode must leave the reader ready for the payload"
                        );
                        seen_flags.push(header.flags());
                    }
                }
            }
        }
        // 2 x 4 x 2 x 2 distinct flag bytes, so nothing collided
        seen_flags.sort_unstable();
        seen_flags.dedup();
        assert_eq!(seen_flags.len(), 32);
    }

    /// The four header bytes from the real handshake, in order.
    #[test]
    fn captured_handshake_headers_decode() {
        // 1. server -> client  SET_BYTE_ORDER, control, little-endian
        let header =
            PvaHeader::decode_all(&[0xCA, 0x02, 0x41, 0x02, 0, 0, 0, 0], ByteOrder::Big).unwrap();
        assert_eq!(header.version, PVA_VERSION);
        assert_eq!(header.kind, MessageKind::Control);
        assert_eq!(header.command, 0x02);
        assert_eq!(header.direction, Direction::FromServer);
        assert_eq!(header.byte_order, ByteOrder::Little);
        assert_eq!(header.segmentation, Segmentation::NotSegmented);
        assert_eq!(header.payload_len(), 0);

        // 2. server -> client  CONNECTION_VALIDATION, 20-byte payload read little-endian
        let header = PvaHeader::decode_all(
            &[0xCA, 0x02, 0x40, 0x01, 0x14, 0x00, 0x00, 0x00],
            ByteOrder::Big,
        )
        .unwrap();
        assert_eq!(header.kind, MessageKind::Application);
        assert_eq!(header.command, 0x01);
        assert_eq!(header.payload_len(), 20);
        assert_eq!(header.control_data(), None);

        // 4. server -> client  CONNECTION_VALIDATED, a one-byte Status payload
        let header = PvaHeader::decode_all(
            &[0xCA, 0x02, 0x40, 0x09, 0x01, 0x00, 0x00, 0x00],
            ByteOrder::Big,
        )
        .unwrap();
        assert_eq!(header.command, 0x09);
        assert!(header.from_server(), "0x09 is server -> client");
        assert_eq!(header.payload_len(), 1);

        // ... and all three re-encode to exactly the captured bytes
        assert_eq!(
            PvaHeader::control(0x02, ByteOrder::Little, 0)
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0xCA, 0x02, 0x41, 0x02, 0, 0, 0, 0]
        );
        assert_eq!(
            PvaHeader::application(0x01, ByteOrder::Little, 20)
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0xCA, 0x02, 0x40, 0x01, 0x14, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            PvaHeader::application(0x09, ByteOrder::Little, 1)
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            [0xCA, 0x02, 0x40, 0x09, 0x01, 0x00, 0x00, 0x00]
        );
    }

    /// A client's message has bit 6 clear, which is the whole reason there is no
    /// Message/ClientMessage split here.
    #[test]
    fn direction_is_a_flag_not_a_command_id() {
        let from_client = PvaHeader::application(0x0A, ByteOrder::Little, 0).from_the_client();
        let from_server = PvaHeader::application(0x0A, ByteOrder::Little, 0);
        assert_eq!(from_client.flags(), 0x00);
        assert_eq!(from_server.flags(), 0x40);
        // Same command ID, opposite directions, and nothing else distinguishes them
        assert_eq!(from_client.command, from_server.command);
        assert!(!from_client.from_server() && from_server.from_server());
    }

    /// A control message's size field is data, and must not be framed as a length.
    #[test]
    fn control_size_field_is_data_not_a_length() {
        // MARK_TOTAL_BYTE_SENT carrying a 17408-byte count
        let header = PvaHeader::control(0x00, ByteOrder::Little, 17408);
        assert_eq!(header.control_data(), Some(17408));
        assert_eq!(
            header.payload_len(),
            0,
            "framing 17408 as a payload length would desynchronise the stream"
        );

        // Control data may legitimately be negative, being a wrapping counter
        let bytes = PvaHeader::control(0x01, ByteOrder::Big, -1)
            .encode_to_vec(ByteOrder::Big)
            .unwrap();
        let decoded = PvaHeader::decode_all(&bytes, ByteOrder::Big).unwrap();
        assert_eq!(decoded.control_data(), Some(-1));
        assert_eq!(decoded.payload_len(), 0);
    }

    #[test]
    fn bad_headers_are_rejected_or_reported_incomplete() {
        // Wrong magic is malformed, not incomplete
        let error = PvaHeader::decode_all(&[0xDA, 0x02, 0x40, 0x01, 0, 0, 0, 0], ByteOrder::Little)
            .unwrap_err();
        assert!(!error.is_incomplete());
        assert!(matches!(error, PvaError::Malformed(_)));

        // A negative application payload size cannot be framed
        assert!(matches!(
            PvaHeader::decode_all(
                &[0xCA, 0x02, 0x40, 0x01, 0xFF, 0xFF, 0xFF, 0xFF],
                ByteOrder::Little
            )
            .unwrap_err(),
            PvaError::Malformed(_)
        ));

        // A partial header is incomplete at every length, and consumes nothing
        let full = [0xCA, 0x02, 0x40, 0x01, 0x14, 0x00, 0x00, 0x00];
        for length in 0..HEADER_SIZE {
            let mut reader = PvaReader::new(&full[..length], ByteOrder::Little);
            let error = PvaHeader::decode(&mut reader).unwrap_err();
            assert_eq!(
                error,
                PvaError::Incomplete {
                    needed: HEADER_SIZE - length
                },
                "at length {length}"
            );
            assert_eq!(reader.position(), 0, "at length {length}");
        }
    }

    #[test]
    fn segmentation_bits_map_to_the_documented_order() {
        for (bits, expected) in [
            (0x00, Segmentation::NotSegmented),
            (0x10, Segmentation::First),
            (0x20, Segmentation::Last),
            (0x30, Segmentation::Middle),
        ] {
            let header =
                PvaHeader::decode_all(&[0xCA, 0x02, bits, 0x0A, 0, 0, 0, 0], ByteOrder::Little)
                    .unwrap();
            assert_eq!(header.segmentation, expected);
            assert_eq!(header.flags(), bits);
        }
        assert!(!Segmentation::NotSegmented.is_segmented());
        assert!(Segmentation::First.is_segmented());
        assert!(Segmentation::Middle.is_segmented());
        assert_eq!(Segmentation::default(), Segmentation::NotSegmented);
    }

    /// Reserved bits 1-3 are ignored rather than rejected, and never written.
    #[test]
    fn reserved_flag_bits_are_ignored() {
        let header =
            PvaHeader::decode_all(&[0xCA, 0x02, 0x4E, 0x0A, 0, 0, 0, 0], ByteOrder::Little)
                .unwrap();
        assert_eq!(header.kind, MessageKind::Application);
        assert!(header.from_server());
        assert_eq!(header.flags(), 0x40, "we do not echo bits we do not define");
    }

    /// A version we do not know is carried through rather than rejected: version
    /// negotiation is not the header parser's job.
    #[test]
    fn unknown_versions_are_carried_not_rejected() {
        let header =
            PvaHeader::decode_all(&[0xCA, 0x63, 0x40, 0x0A, 0, 0, 0, 0], ByteOrder::Little)
                .unwrap();
        assert_eq!(header.version, 0x63);
    }
}
