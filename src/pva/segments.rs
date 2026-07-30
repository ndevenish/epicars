//! Reassembling messages that arrive in pieces.
//!
//! A pvAccess message may be split across several framed messages, with flags bits 4-5
//! marking each piece as [`Segmentation::First`], [`Segmentation::Middle`] or
//! [`Segmentation::Last`]. There is no CA analogue to borrow from: CA's large-payload story
//! is a longer header, not a split message.
//!
//! # What reassembly is
//!
//! Concatenating the segment payloads, in order. That is all - and it is worth saying
//! plainly, because the phrase "preserving alignment padding between segments" invites the
//! belief that something has to be stripped or inserted.
//!
//! pvAccess aligns values inside a payload to 64-bit boundaries, and a sender splits on an
//! 8-byte boundary so that those offsets still line up once the pieces are joined. The
//! padding lives *inside* the payload, is part of it, and travels with whichever segment it
//! falls in. Nothing is added or removed at a segment edge, so a reassembled message is
//! byte-identical to the same message sent whole.
//!
//! This reassembler does **not** require non-final segments to be a multiple of eight bytes.
//! Concatenation is correct whatever the split offsets are, so enforcing it would add a way
//! to fail against a peer that is merely being unusual, and gain nothing.
//!
//! # What it refuses
//!
//! A segmented message owns the connection until it finishes: application messages may not
//! interleave with it, and a `Middle` or `Last` for a different command than the `First` is
//! a protocol error rather than something to guess at. Control messages *do* interleave
//! freely, which is why they are handled below this layer and never reach it.

use crate::pva::header::{Direction, PvaHeader, Segmentation};
use crate::pva::io::{ByteOrder, PvaError};

/// The largest logical message this will assemble before giving up, in bytes.
///
/// A peer that sends `First` and then never finishes would otherwise grow this buffer
/// without limit. 16 MiB is far above anything the plan's normative types produce, and far
/// below anything that threatens a server.
pub const DEFAULT_MAX_PAYLOAD: usize = 16 * 1024 * 1024;

/// One message being accumulated.
#[derive(Clone, Debug)]
struct Partial {
    command: u8,
    byte_order: ByteOrder,
    direction: Direction,
    payload: Vec<u8>,
}

/// Joins segmented messages back together, passing whole ones straight through.
///
/// One per connection per direction. Feed it every application message with
/// [`Reassembler::push`]; it yields a logical message when it has one.
#[derive(Clone, Debug)]
pub struct Reassembler {
    partial: Option<Partial>,
    max_payload: usize,
}

impl Default for Reassembler {
    fn default() -> Reassembler {
        Reassembler::new()
    }
}

impl Reassembler {
    pub fn new() -> Reassembler {
        Reassembler::with_max_payload(DEFAULT_MAX_PAYLOAD)
    }

    pub fn with_max_payload(max_payload: usize) -> Reassembler {
        Reassembler {
            partial: None,
            max_payload,
        }
    }

    /// Whether a segmented message is part-way through.
    pub fn in_progress(&self) -> bool {
        self.partial.is_some()
    }

    /// How many payload bytes have accumulated for the message in progress.
    pub fn buffered(&self) -> usize {
        self.partial.as_ref().map_or(0, |p| p.payload.len())
    }

    /// Discard any message in progress, as on a reconnect.
    pub fn reset(&mut self) {
        self.partial = None;
    }

    /// Offer one framed application message.
    ///
    /// Returns the logical message once it is complete: a whole message immediately, a
    /// segmented one on its `Last` segment, and `None` while a segmented one is still
    /// arriving.
    ///
    /// The header returned for a reassembled message is the `First` segment's, with
    /// [`Segmentation::NotSegmented`] and the total payload size - so it is
    /// indistinguishable from the same message having arrived whole.
    pub fn push(
        &mut self,
        header: PvaHeader,
        payload: &[u8],
    ) -> Result<Option<(PvaHeader, Vec<u8>)>, PvaError> {
        match header.segmentation {
            Segmentation::NotSegmented => {
                if self.partial.is_some() {
                    // Abandon the partial: continuing would splice two messages together
                    self.partial = None;
                    return Err(PvaError::malformed(format!(
                        "whole message (command {:#04x}) arrived while a segmented message was \
                         still in progress",
                        header.command
                    )));
                }
                Ok(Some((header, payload.to_vec())))
            }
            Segmentation::First => {
                if self.partial.is_some() {
                    self.partial = None;
                    return Err(PvaError::malformed(
                        "a second first-segment arrived before the previous message finished",
                    ));
                }
                self.check_size(payload.len())?;
                self.partial = Some(Partial {
                    command: header.command,
                    byte_order: header.byte_order,
                    direction: header.direction,
                    payload: payload.to_vec(),
                });
                Ok(None)
            }
            Segmentation::Middle => {
                self.append(&header, payload)?;
                Ok(None)
            }
            Segmentation::Last => {
                self.append(&header, payload)?;
                let partial = self
                    .partial
                    .take()
                    .expect("append leaves the partial in place");
                let size = i32::try_from(partial.payload.len()).map_err(|_| {
                    PvaError::OutOfRange(format!(
                        "reassembled payload of {} bytes does not fit an i32",
                        partial.payload.len()
                    ))
                })?;
                // The first segment's identity wins: a byte order or direction that changed
                // mid-message would be nonsense, and the payload was decoded against the
                // order the first segment declared
                let header = PvaHeader {
                    version: header.version,
                    kind: header.kind,
                    segmentation: Segmentation::NotSegmented,
                    direction: partial.direction,
                    byte_order: partial.byte_order,
                    command: partial.command,
                    payload_size: size,
                };
                Ok(Some((header, partial.payload)))
            }
        }
    }

    fn append(&mut self, header: &PvaHeader, payload: &[u8]) -> Result<(), PvaError> {
        let Some(partial) = self.partial.as_mut() else {
            return Err(PvaError::malformed(format!(
                "{} segment for command {:#04x} with no first segment",
                match header.segmentation {
                    Segmentation::Last => "last",
                    _ => "middle",
                },
                header.command
            )));
        };
        if partial.command != header.command {
            let expected = partial.command;
            self.partial = None;
            return Err(PvaError::malformed(format!(
                "segment for command {:#04x} interleaved into a message of command \
                 {expected:#04x}",
                header.command
            )));
        }
        let total = partial.payload.len() + payload.len();
        if total > self.max_payload {
            self.partial = None;
            return Err(PvaError::malformed(format!(
                "segmented message exceeds the {} byte reassembly limit",
                self.max_payload
            )));
        }
        partial.payload.extend_from_slice(payload);
        Ok(())
    }

    fn check_size(&self, size: usize) -> Result<(), PvaError> {
        if size > self.max_payload {
            return Err(PvaError::malformed(format!(
                "segmented message exceeds the {} byte reassembly limit",
                self.max_payload
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::header::MessageKind;

    /// A 24-byte payload, long enough to split three ways at 8-byte boundaries and to have
    /// distinguishable content at every offset.
    fn payload() -> Vec<u8> {
        (0..24u8).collect()
    }

    fn whole(payload: &[u8]) -> PvaHeader {
        PvaHeader::application(0x0A, ByteOrder::Little, payload.len() as i32)
    }

    fn segment(command: u8, size: usize, segmentation: Segmentation) -> PvaHeader {
        PvaHeader::application(command, ByteOrder::Little, size as i32).segmented(segmentation)
    }

    /// The done-when: split at every offset, and get back exactly the whole message.
    #[test]
    fn every_two_way_split_reassembles_byte_identically() {
        let payload = payload();
        let expected = (whole(&payload), payload.clone());

        for split in 1..payload.len() {
            let (head, tail) = payload.split_at(split);
            let mut reassembler = Reassembler::new();

            assert_eq!(
                reassembler
                    .push(segment(0x0A, head.len(), Segmentation::First), head)
                    .unwrap(),
                None,
                "split at {split}"
            );
            assert!(reassembler.in_progress());
            assert_eq!(reassembler.buffered(), head.len());

            let assembled = reassembler
                .push(segment(0x0A, tail.len(), Segmentation::Last), tail)
                .unwrap()
                .expect("last segment completes the message");
            assert_eq!(assembled, expected, "split at {split}");
            assert!(
                !reassembler.in_progress(),
                "completing must clear the buffer"
            );
        }
    }

    /// Three-way splits, at every pair of offsets, including the non-multiple-of-eight ones
    /// this reassembler deliberately tolerates.
    #[test]
    fn every_three_way_split_reassembles_byte_identically() {
        let payload = payload();
        let expected = (whole(&payload), payload.clone());

        for first in 1..payload.len() - 1 {
            for second in first + 1..payload.len() {
                let mut reassembler = Reassembler::new();
                let pieces = [
                    (&payload[..first], Segmentation::First),
                    (&payload[first..second], Segmentation::Middle),
                    (&payload[second..], Segmentation::Last),
                ];
                let mut assembled = None;
                for (piece, segmentation) in pieces {
                    assembled = reassembler
                        .push(segment(0x0A, piece.len(), segmentation), piece)
                        .unwrap();
                }
                assert_eq!(
                    assembled.unwrap(),
                    expected,
                    "split at {first} and {second}"
                );
            }
        }
    }

    /// Whole messages go straight through, and are indistinguishable from reassembled ones.
    #[test]
    fn unsegmented_messages_pass_through() {
        let payload = payload();
        let mut reassembler = Reassembler::new();
        assert_eq!(
            reassembler.push(whole(&payload), &payload).unwrap(),
            Some((whole(&payload), payload.clone()))
        );
        assert!(!reassembler.in_progress());
        assert_eq!(reassembler.buffered(), 0);

        // An empty payload is a message too
        let empty = PvaHeader::application(0x02, ByteOrder::Little, 0);
        assert_eq!(
            reassembler.push(empty, &[]).unwrap(),
            Some((empty, Vec::new()))
        );
    }

    /// The reassembled header carries the *first* segment's identity, not the last's.
    #[test]
    fn the_reassembled_header_comes_from_the_first_segment() {
        let mut reassembler = Reassembler::new();
        let first = PvaHeader::application(0x0D, ByteOrder::Big, 4)
            .segmented(Segmentation::First)
            .from_the_client();
        reassembler.push(first, &[1, 2, 3, 4]).unwrap();

        // The last segment's byte order and direction are ignored in favour of the first's,
        // since a mid-message change would be nonsense
        let last = PvaHeader::application(0x0D, ByteOrder::Little, 2).segmented(Segmentation::Last);
        let (header, payload) = reassembler.push(last, &[5, 6]).unwrap().unwrap();

        assert_eq!(header.command, 0x0D);
        assert_eq!(header.byte_order, ByteOrder::Big);
        assert_eq!(header.direction, Direction::FromClient);
        assert_eq!(header.segmentation, Segmentation::NotSegmented);
        assert_eq!(header.payload_size, 6);
        assert_eq!(header.kind, MessageKind::Application);
        assert_eq!(payload, [1, 2, 3, 4, 5, 6]);
    }

    /// A segmented message owns the connection until it finishes.
    #[test]
    fn interleaving_is_refused() {
        // A whole message in the middle of a segmented one
        let mut reassembler = Reassembler::new();
        reassembler
            .push(segment(0x0A, 4, Segmentation::First), &[1, 2, 3, 4])
            .unwrap();
        let error = reassembler.push(whole(&[9]), &[9]).unwrap_err();
        assert!(matches!(error, PvaError::Malformed(_)));
        assert!(
            !reassembler.in_progress(),
            "the abandoned partial must not be spliced onto the next message"
        );

        // Another message's segment in the middle of one
        let mut reassembler = Reassembler::new();
        reassembler
            .push(segment(0x0A, 4, Segmentation::First), &[1, 2, 3, 4])
            .unwrap();
        assert!(
            reassembler
                .push(segment(0x0B, 2, Segmentation::Middle), &[5, 6])
                .is_err()
        );
        assert!(!reassembler.in_progress());

        // Two first-segments in a row
        let mut reassembler = Reassembler::new();
        reassembler
            .push(segment(0x0A, 4, Segmentation::First), &[1, 2, 3, 4])
            .unwrap();
        assert!(
            reassembler
                .push(segment(0x0A, 4, Segmentation::First), &[5, 6, 7, 8])
                .is_err()
        );
        assert!(!reassembler.in_progress());
    }

    /// A continuation with nothing to continue is an error, not an empty message.
    #[test]
    fn orphaned_segments_are_refused() {
        for segmentation in [Segmentation::Middle, Segmentation::Last] {
            let mut reassembler = Reassembler::new();
            let error = reassembler
                .push(segment(0x0A, 2, segmentation), &[1, 2])
                .unwrap_err();
            assert!(matches!(error, PvaError::Malformed(_)), "{segmentation:?}");
            assert!(!reassembler.in_progress());
        }
    }

    /// An unfinished message cannot grow without limit.
    #[test]
    fn oversized_reassembly_is_refused() {
        let mut reassembler = Reassembler::with_max_payload(8);
        reassembler
            .push(segment(0x0A, 6, Segmentation::First), &[0; 6])
            .unwrap();
        let error = reassembler
            .push(segment(0x0A, 6, Segmentation::Middle), &[0; 6])
            .unwrap_err();
        assert!(matches!(error, PvaError::Malformed(_)));
        assert!(!reassembler.in_progress(), "the buffer is released");

        // ... including on the very first segment
        let mut reassembler = Reassembler::with_max_payload(8);
        assert!(
            reassembler
                .push(segment(0x0A, 9, Segmentation::First), &[0; 9])
                .is_err()
        );
        assert!(!reassembler.in_progress());

        // Exactly at the limit is fine
        let mut reassembler = Reassembler::with_max_payload(8);
        reassembler
            .push(segment(0x0A, 4, Segmentation::First), &[0; 4])
            .unwrap();
        assert!(
            reassembler
                .push(segment(0x0A, 4, Segmentation::Last), &[0; 4])
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn reset_discards_a_message_in_progress() {
        let mut reassembler = Reassembler::new();
        reassembler
            .push(segment(0x0A, 4, Segmentation::First), &[1, 2, 3, 4])
            .unwrap();
        assert_eq!(reassembler.buffered(), 4);
        reassembler.reset();
        assert!(!reassembler.in_progress());
        assert_eq!(reassembler.buffered(), 0);
        // ... and the connection is usable again
        assert!(reassembler.push(whole(&[1]), &[1]).unwrap().is_some());
    }

    /// Empty segments are legal and contribute nothing.
    #[test]
    fn empty_segments_are_harmless() {
        let mut reassembler = Reassembler::new();
        reassembler
            .push(segment(0x0A, 0, Segmentation::First), &[])
            .unwrap();
        reassembler
            .push(segment(0x0A, 0, Segmentation::Middle), &[])
            .unwrap();
        let (header, payload) = reassembler
            .push(segment(0x0A, 2, Segmentation::Last), &[1, 2])
            .unwrap()
            .unwrap();
        assert_eq!(payload, [1, 2]);
        assert_eq!(header.payload_size, 2);
    }
}
