//! pvAccess protocol support, gated on the `pva` Cargo feature.
//!
//! **Work in progress.** `docs/pvaccess-implementation-plan.md` is the authority for what
//! lands here and in what order; each module below arrives with its numbered plan item.
//!
//! # This module deliberately does not look like [`crate::messages`]
//!
//! Three of the CA module's conventions are departed from here, each as a recorded
//! decision rather than as drift:
//!
//! - **No nom.** pvAccess negotiates byte order per connection, so parsers are written
//!   against a reader/writer pair carrying a runtime byte order. nom's `be_*`/`le_*`
//!   split would mean writing every parser twice. **CA keeps nom.**
//! - **One message enum, not two.** CA needs the `Message`/`ClientMessage` split because
//!   it overloads command IDs by direction; pvAccess puts direction in a header flag bit.
//! - **No `CAMessage`/`TryFrom<RawMessage>` recipe.** The header is 8 bytes with no
//!   extended form, and messages may be segmented across frames.
//!
//! What *does* transfer from the CA module: the shape of its error enum, the
//! decoder-is-also-the-item trick, and the peek → header-len → payload-len → reserve →
//! advance framing pattern.

pub mod control;
pub mod encoding;
pub mod header;
pub mod introspection;
pub mod io;
pub mod registry;
pub mod segments;

pub use control::ControlMessage;
pub use encoding::{BitSet, Status, StatusType};
pub use header::{Direction, MessageKind, PvaHeader, Segmentation};
pub use introspection::{NoCache, TypeCache};
pub use io::{ByteOrder, PvaDecode, PvaEncode, PvaError, PvaReader, PvaWriter};
pub use registry::{ConnectionTypes, IntrospectionRegistry};
pub use segments::Reassembler;
