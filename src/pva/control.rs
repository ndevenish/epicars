//! Transport control messages, which live below the application protocol.
//!
//! A control message is a header and nothing else - eight bytes, no payload - and its size
//! field carries data rather than a length. They interleave freely with application
//! messages, including in the middle of a segmented one, which is why they are handled
//! beneath the message enum and never reach the application.
//!
//! | Code | Name | Data field |
//! |---|---|---|
//! | `0x00` | Mark Total Byte Sent | bytes sent so far |
//! | `0x01` | Acknowledge Total Bytes Received | bytes received so far |
//! | `0x02` | Set byte order | unused; the order is flags bit 7 |
//! | `0x03` | Echo request | unused |
//! | `0x04` | Echo response | unused |
//!
//! The first message a real server sends is one of these:
//!
//! ```
//! use epicars::pva::control::ControlMessage;
//! use epicars::pva::header::PvaHeader;
//! use epicars::pva::io::{ByteOrder, PvaDecode};
//!
//! // ca 02 41 02  00000000
//! let header = PvaHeader::decode_all(&[0xCA, 0x02, 0x41, 0x02, 0, 0, 0, 0], ByteOrder::Big)
//!     .unwrap();
//! assert_eq!(
//!     ControlMessage::from_header(&header),
//!     ControlMessage::SetByteOrder(ByteOrder::Little)
//! );
//! ```

use crate::pva::header::{Direction, PVA_VERSION, PvaHeader};
use crate::pva::io::ByteOrder;

pub const MARK_TOTAL_BYTES_SENT: u8 = 0x00;
pub const ACK_TOTAL_BYTES_RECEIVED: u8 = 0x01;
pub const SET_BYTE_ORDER: u8 = 0x02;
pub const ECHO_REQUEST: u8 = 0x03;
pub const ECHO_RESPONSE: u8 = 0x04;

/// A transport control message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    /// "I have sent this many bytes in total" - the flow-control mark a peer acknowledges.
    MarkTotalBytesSent(i32),
    /// "I have received this many bytes in total", answering a mark.
    AcknowledgeTotalBytesReceived(i32),
    /// The byte order to use for this connection.
    ///
    /// The order travels in flags bit 7, not in the data field. Some specification prose
    /// describes a variant where a size field of `0xFFFFFFFF` makes the order permanent
    /// rather than applying to the next message only; that distinction is not modelled,
    /// because this implementation adopts the negotiated order for the whole connection
    /// either way - which is what both reference implementations do - so there is nothing
    /// for it to change.
    SetByteOrder(ByteOrder),
    EchoRequest,
    EchoResponse,
    /// A control command this implementation does not know.
    ///
    /// Kept rather than rejected: a control message is exactly eight bytes, so an unknown
    /// one can always be skipped without losing framing. The transport should log it and
    /// carry on.
    Unknown {
        command: u8,
        data: i32,
    },
}

impl ControlMessage {
    /// Interpret a control header. Total: every command byte maps to something.
    pub fn from_header(header: &PvaHeader) -> ControlMessage {
        let data = header.payload_size;
        match header.command {
            MARK_TOTAL_BYTES_SENT => ControlMessage::MarkTotalBytesSent(data),
            ACK_TOTAL_BYTES_RECEIVED => ControlMessage::AcknowledgeTotalBytesReceived(data),
            SET_BYTE_ORDER => ControlMessage::SetByteOrder(header.byte_order),
            ECHO_REQUEST => ControlMessage::EchoRequest,
            ECHO_RESPONSE => ControlMessage::EchoResponse,
            command => ControlMessage::Unknown { command, data },
        }
    }

    pub fn command(&self) -> u8 {
        match self {
            ControlMessage::MarkTotalBytesSent(_) => MARK_TOTAL_BYTES_SENT,
            ControlMessage::AcknowledgeTotalBytesReceived(_) => ACK_TOTAL_BYTES_RECEIVED,
            ControlMessage::SetByteOrder(_) => SET_BYTE_ORDER,
            ControlMessage::EchoRequest => ECHO_REQUEST,
            ControlMessage::EchoResponse => ECHO_RESPONSE,
            ControlMessage::Unknown { command, .. } => *command,
        }
    }

    /// The data field this message puts in the header's size slot.
    pub fn data(&self) -> i32 {
        match self {
            ControlMessage::MarkTotalBytesSent(n)
            | ControlMessage::AcknowledgeTotalBytesReceived(n) => *n,
            ControlMessage::Unknown { data, .. } => *data,
            // Captured as zero for SET_BYTE_ORDER; unused for the echoes
            ControlMessage::SetByteOrder(_)
            | ControlMessage::EchoRequest
            | ControlMessage::EchoResponse => 0,
        }
    }

    /// Build the eight-byte header that carries this message.
    ///
    /// A [`ControlMessage::SetByteOrder`] declares its own order rather than using
    /// `byte_order`, since announcing an order in a header flagged with a different one
    /// would be self-contradictory.
    pub fn to_header(&self, byte_order: ByteOrder, direction: Direction) -> PvaHeader {
        let byte_order = match self {
            ControlMessage::SetByteOrder(order) => *order,
            _ => byte_order,
        };
        PvaHeader {
            version: PVA_VERSION,
            kind: crate::pva::header::MessageKind::Control,
            segmentation: crate::pva::header::Segmentation::NotSegmented,
            direction,
            byte_order,
            command: self.command(),
            payload_size: self.data(),
        }
    }

    /// Whether this is something the transport should reply to.
    pub fn needs_reply(&self) -> bool {
        matches!(
            self,
            ControlMessage::EchoRequest | ControlMessage::MarkTotalBytesSent(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::header::{MessageKind, Segmentation};
    use crate::pva::io::{PvaDecode, PvaEncode};

    fn round_trip(message: ControlMessage, order: ByteOrder, direction: Direction) {
        let header = message.to_header(order, direction);
        assert_eq!(header.kind, MessageKind::Control);
        assert_eq!(header.segmentation, Segmentation::NotSegmented);
        assert_eq!(header.payload_len(), 0, "control messages have no payload");

        let bytes = header.encode_to_vec(order).unwrap();
        assert_eq!(bytes.len(), 8);
        let decoded_header = PvaHeader::decode_all(&bytes, order).unwrap();
        assert_eq!(decoded_header, header);
        assert_eq!(ControlMessage::from_header(&decoded_header), message);
    }

    #[test]
    fn every_control_message_round_trips() {
        let messages = [
            ControlMessage::MarkTotalBytesSent(0),
            ControlMessage::MarkTotalBytesSent(17408),
            // The counters wrap, so negative values are ordinary
            ControlMessage::MarkTotalBytesSent(-1),
            ControlMessage::AcknowledgeTotalBytesReceived(i32::MIN),
            ControlMessage::SetByteOrder(ByteOrder::Little),
            ControlMessage::SetByteOrder(ByteOrder::Big),
            ControlMessage::EchoRequest,
            ControlMessage::EchoResponse,
            ControlMessage::Unknown {
                command: 0x7F,
                data: 42,
            },
        ];
        for message in messages {
            for order in [ByteOrder::Little, ByteOrder::Big] {
                for direction in [Direction::FromClient, Direction::FromServer] {
                    round_trip(message, order, direction);
                }
            }
        }
    }

    /// The first message a real server sends, byte for byte.
    #[test]
    fn captured_set_byte_order() {
        let captured = [0xCA, 0x02, 0x41, 0x02, 0x00, 0x00, 0x00, 0x00];
        let header = PvaHeader::decode_all(&captured, ByteOrder::Big).unwrap();
        assert_eq!(
            ControlMessage::from_header(&header),
            ControlMessage::SetByteOrder(ByteOrder::Little)
        );
        assert!(header.from_server());

        // ... and we emit exactly those bytes
        let message = ControlMessage::SetByteOrder(ByteOrder::Little);
        assert_eq!(
            message
                .to_header(ByteOrder::Little, Direction::FromServer)
                .encode_to_vec(ByteOrder::Little)
                .unwrap(),
            captured
        );
    }

    /// SET_BYTE_ORDER declares its own order, whatever the connection is currently using.
    #[test]
    fn set_byte_order_overrides_the_connections_order() {
        // Announcing big-endian while still on little-endian: the header must say big
        let header = ControlMessage::SetByteOrder(ByteOrder::Big)
            .to_header(ByteOrder::Little, Direction::FromServer);
        assert_eq!(header.byte_order, ByteOrder::Big);
        assert_eq!(header.flags(), 0xC1, "control, from server, big-endian");

        // Every other message takes the connection's order
        let header =
            ControlMessage::EchoRequest.to_header(ByteOrder::Little, Direction::FromClient);
        assert_eq!(header.byte_order, ByteOrder::Little);
        assert_eq!(header.flags(), 0x01);
    }

    /// An unknown control command is kept, not rejected: it is eight bytes, so skipping it
    /// cannot lose framing.
    #[test]
    fn unknown_control_commands_are_recoverable() {
        let header = PvaHeader::control(0x2A, ByteOrder::Little, 7);
        assert_eq!(
            ControlMessage::from_header(&header),
            ControlMessage::Unknown {
                command: 0x2A,
                data: 7
            }
        );
        assert_eq!(
            ControlMessage::from_header(&header).command(),
            0x2A,
            "it re-encodes as itself"
        );
    }

    #[test]
    fn accessors() {
        assert_eq!(ControlMessage::MarkTotalBytesSent(9).data(), 9);
        assert_eq!(ControlMessage::SetByteOrder(ByteOrder::Big).data(), 0);
        assert_eq!(ControlMessage::EchoRequest.data(), 0);
        assert_eq!(ControlMessage::EchoRequest.command(), ECHO_REQUEST);

        assert!(ControlMessage::EchoRequest.needs_reply());
        assert!(ControlMessage::MarkTotalBytesSent(1).needs_reply());
        assert!(!ControlMessage::EchoResponse.needs_reply());
        assert!(!ControlMessage::SetByteOrder(ByteOrder::Big).needs_reply());
    }
}
