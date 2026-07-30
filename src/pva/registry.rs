//! The per-connection introspection cache.
//!
//! This is the likeliest single source of "works against my own client, breaks against
//! pvxs" bugs in the whole pvAccess effort, so it is worth being precise about what it is.
//!
//! # There are two registries per connection, not one
//!
//! The link is full-duplex and each direction assigns ids independently. The server's id 1
//! and the client's id 1 are unrelated, and nothing synchronises them. [`ConnectionTypes`]
//! is therefore a *pair* of [`IntrospectionRegistry`] instances, and the codec is handed
//! whichever one matches the direction it is working in.
//!
//! # Ids are re-definable mid-connection
//!
//! A `FULL_WITH_ID` for an id that is already bound replaces the binding. Anything that
//! cached a resolved [`Field`] by id, rather than re-resolving, would silently decode later
//! messages against the old shape.
//!
//! # The two reference implementations already disagree
//!
//! Both of these are the *same* logical `CONNECTION_VALIDATION` response, carrying the same
//! `{ string user; string host; }` structure, captured against one soft IOC:
//!
//! | | pvAccessCPP 7.1.7 | pvxs 1.5.2 |
//! |---|---|---|
//! | payload | 60 bytes | 28 bytes |
//! | introspection | `fd 0100 80 …` | `80 …` |
//! | form | **FULL_WITH_ID**, registering id 1 | **FULL_TYPE_CODE**, inline, no id |
//!
//! Neither is wrong: `0x80` is ≤ `0xDF`, so it is the legitimate no-id form. A server that
//! assumes clients register ids breaks against pvxs; one that assumes they never do breaks
//! against pvAccessCPP. **Accept both.**
//!
//! The consequence that catches people out is that the *receive*-side registry can stay
//! **empty for an entire connection** when the peer is pvxs. "The registry is populated" is
//! therefore not a usable precondition anywhere.

use std::collections::HashMap;

use crate::pva::introspection::TypeCache;
use crate::value::Field;

/// The largest number of ids either side will hold, unless the peer asks for fewer.
///
/// Both reference implementations advertise `0x7FFF` in their `CONNECTION_VALIDATION`
/// exchange, which is what this matches.
pub const DEFAULT_REGISTRY_LIMIT: u16 = 0x7FFF;

/// One direction's introspection cache.
///
/// Which direction is the caller's business - see [`ConnectionTypes`]. A registry used for
/// sending allocates ids; one used for receiving only ever remembers what the peer
/// allocated. Nothing enforces that, because a full-duplex connection uses both halves the
/// same way and the asymmetry is in *who calls what*.
#[derive(Clone, Debug)]
pub struct IntrospectionRegistry {
    by_id: HashMap<u16, Field>,
    /// The next id to hand out. Ids start at 1: the captures show pvAccessCPP registering
    /// id 1 for the first structure it sends, and 0 is avoided as a "no id" sentinel by
    /// convention rather than by the specification.
    next_id: u16,
    limit: u16,
}

impl Default for IntrospectionRegistry {
    fn default() -> IntrospectionRegistry {
        IntrospectionRegistry::new()
    }
}

impl IntrospectionRegistry {
    pub fn new() -> IntrospectionRegistry {
        IntrospectionRegistry::with_limit(DEFAULT_REGISTRY_LIMIT)
    }

    /// A registry that will hold at most `limit` ids, as the peer's advertised maximum.
    ///
    /// A limit of zero means "never register", which is a legal way to behave: everything
    /// then goes inline, exactly as pvxs does.
    pub fn with_limit(limit: u16) -> IntrospectionRegistry {
        IntrospectionRegistry {
            by_id: HashMap::new(),
            next_id: 1,
            limit,
        }
    }

    pub fn limit(&self) -> u16 {
        self.limit
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether nothing has been registered.
    ///
    /// **Not a usable precondition for anything**: against pvxs the receive side stays
    /// empty for the whole connection.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Bind `id` to `field`, replacing any previous binding.
    ///
    /// Overriding is legal and happens mid-connection, so this deliberately does not
    /// complain about a collision.
    pub fn insert(&mut self, id: u16, field: Field) {
        self.by_id.insert(id, field);
    }

    pub fn get(&self, id: u16) -> Option<&Field> {
        self.by_id.get(&id)
    }

    /// The id `field` is bound to, if any. Linear in the number of registered types, which
    /// is a handful in practice.
    pub fn id_of(&self, field: &Field) -> Option<u16> {
        self.by_id
            .iter()
            .find(|(_, known)| *known == field)
            .map(|(id, _)| *id)
    }

    /// Allocate the next id and bind `field` to it, or `None` if the limit is reached.
    pub fn allocate(&mut self, field: Field) -> Option<u16> {
        if self.by_id.len() >= self.limit as usize {
            return None;
        }
        let id = self.next_id;
        self.next_id = self.next_id.checked_add(1)?;
        self.by_id.insert(id, field);
        Some(id)
    }

    /// Forget everything, as on a reconnect.
    pub fn clear(&mut self) {
        self.by_id.clear();
        self.next_id = 1;
    }
}

impl TypeCache for IntrospectionRegistry {
    fn id_for(&self, field: &Field) -> Option<u16> {
        self.id_of(field)
    }

    fn register_for_send(&mut self, field: &Field) -> Option<u16> {
        self.allocate(field.clone())
    }

    fn lookup(&self, id: u16) -> Option<Field> {
        self.get(id).cloned()
    }

    fn remember(&mut self, id: u16, field: Field) {
        self.insert(id, field);
    }
}

/// Both of a connection's introspection registries.
///
/// Held by the transport, one per connection, and reset together on reconnect. The two
/// halves are independent: registering a type for sending says nothing about what the peer
/// has registered for its own messages.
#[derive(Clone, Debug, Default)]
pub struct ConnectionTypes {
    /// Ids this end has allocated, for describing values it sends.
    pub outgoing: IntrospectionRegistry,
    /// Ids the peer has allocated, learned from `FULL_WITH_ID` messages it sent.
    pub incoming: IntrospectionRegistry,
}

impl ConnectionTypes {
    pub fn new() -> ConnectionTypes {
        ConnectionTypes::default()
    }

    /// A pair whose send side honours the peer's advertised registry maximum.
    ///
    /// The receive side keeps the default limit: the peer decides how many ids *it* uses,
    /// and refusing to remember one it has already sent would be a decode failure rather
    /// than a policy.
    pub fn with_outgoing_limit(limit: u16) -> ConnectionTypes {
        ConnectionTypes {
            outgoing: IntrospectionRegistry::with_limit(limit),
            incoming: IntrospectionRegistry::new(),
        }
    }

    /// Forget both directions, as on a reconnect.
    pub fn clear(&mut self) {
        self.outgoing.clear();
        self.incoming.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pva::introspection::{
        FULL_WITH_ID_TYPE_CODE, ONLY_ID_TYPE_CODE, decode_field, encode_field,
    };
    use crate::pva::io::{ByteOrder, PvaError, PvaReader, PvaWriter};
    use crate::value::{Field, ScalarType, StructureField};

    fn auth_structure() -> Field {
        Field::Structure(
            StructureField::new()
                .with("user", ScalarType::String)
                .with("host", ScalarType::String),
        )
    }

    fn encode(field: &Field, cache: &mut dyn TypeCache) -> Vec<u8> {
        let mut writer = PvaWriter::new(ByteOrder::Little);
        encode_field(field, &mut writer, cache).unwrap();
        writer.into_bytes()
    }

    fn decode(bytes: &[u8], cache: &mut dyn TypeCache) -> Result<Option<Field>, PvaError> {
        let mut reader = PvaReader::new(bytes, ByteOrder::Little);
        let field = decode_field(&mut reader, cache)?;
        assert!(reader.is_empty(), "{} bytes left over", reader.remaining());
        Ok(field)
    }

    /// A scripted exchange: introduce an id, refer to it, override it, refer to it again.
    #[test]
    fn ids_are_introduced_referenced_overridden_and_referenced_again() {
        let mut incoming = IntrospectionRegistry::new();
        let first = auth_structure();

        // 1. The peer introduces id 1 with a full description
        let mut introduce = vec![FULL_WITH_ID_TYPE_CODE, 0x01, 0x00];
        introduce.extend_from_slice(&encode(&first, &mut NoCacheForFixtures));
        assert_eq!(
            decode(&introduce, &mut incoming).unwrap(),
            Some(first.clone())
        );
        assert_eq!(incoming.len(), 1);

        // 2. ... and then refers to it by id alone
        assert_eq!(
            decode(&[ONLY_ID_TYPE_CODE, 0x01, 0x00], &mut incoming).unwrap(),
            Some(first.clone())
        );

        // 3. The same id is redefined mid-stream to a different shape
        let second = Field::Structure(
            StructureField::with_id("epics:nt/NTScalar:1.0").with("value", ScalarType::Double),
        );
        let mut override_it = vec![FULL_WITH_ID_TYPE_CODE, 0x01, 0x00];
        override_it.extend_from_slice(&encode(&second, &mut NoCacheForFixtures));
        assert_eq!(
            decode(&override_it, &mut incoming).unwrap(),
            Some(second.clone())
        );
        assert_eq!(incoming.len(), 1, "an override replaces, it does not add");

        // 4. A later reference resolves to the *new* shape
        assert_eq!(
            decode(&[ONLY_ID_TYPE_CODE, 0x01, 0x00], &mut incoming).unwrap(),
            Some(second)
        );
        assert_ne!(incoming.get(1), Some(&first));
    }

    /// A `TypeCache` for building fixture bytes, kept separate so building the fixture
    /// cannot accidentally register anything in the registry under test.
    struct NoCacheForFixtures;
    impl TypeCache for NoCacheForFixtures {}

    /// The send and receive registries are separate objects with separate id spaces.
    #[test]
    fn the_two_registries_are_independent() {
        let mut types = ConnectionTypes::new();
        let ours = Field::Structure(
            StructureField::with_id("epics:nt/NTScalar:1.0").with("value", ScalarType::Int),
        );
        let theirs = auth_structure();

        // We register our own type for sending, and get id 1
        let sent = encode(&ours, &mut types.outgoing);
        assert_eq!(sent[0], FULL_WITH_ID_TYPE_CODE);
        assert_eq!((sent[1], sent[2]), (0x01, 0x00));
        assert_eq!(types.outgoing.len(), 1);
        assert!(
            types.incoming.is_empty(),
            "sending must not touch the receive side"
        );

        // The peer independently registers *its* id 1 for a different type
        let mut introduce = vec![FULL_WITH_ID_TYPE_CODE, 0x01, 0x00];
        introduce.extend_from_slice(&encode(&theirs, &mut NoCacheForFixtures));
        assert_eq!(
            decode(&introduce, &mut types.incoming).unwrap(),
            Some(theirs.clone())
        );

        // Same id, two different meanings, one per direction
        assert_eq!(types.outgoing.get(1), Some(&ours));
        assert_eq!(types.incoming.get(1), Some(&theirs));
        assert_ne!(types.outgoing.get(1), types.incoming.get(1));

        // ... and our second send gets id 2, unaffected by the peer's numbering
        let other = Field::Structure(StructureField::with_id("other_t").with("n", ScalarType::Int));
        let sent = encode(&other, &mut types.outgoing);
        assert_eq!((sent[1], sent[2]), (0x02, 0x00));

        types.clear();
        assert!(types.outgoing.is_empty() && types.incoming.is_empty());
    }

    /// Repeating a type on the send side collapses to a three-byte reference.
    #[test]
    fn repeated_sends_shrink_to_only_id() {
        let mut registry = IntrospectionRegistry::new();
        let field = Field::Structure(
            StructureField::with_id("epics:nt/NTScalar:1.0")
                .with("value", ScalarType::Double)
                .with(
                    "alarm",
                    StructureField::with_id("alarm_t").with("severity", ScalarType::Int),
                ),
        );

        let first = encode(&field, &mut registry);
        assert_eq!(first[0], FULL_WITH_ID_TYPE_CODE);
        assert!(first.len() > 3);

        let second = encode(&field, &mut registry);
        assert_eq!(second, [ONLY_ID_TYPE_CODE, 0x01, 0x00]);

        // The nested alarm_t was registered too, on the way past
        assert!(registry.len() >= 2, "nested structures are cached as well");

        // And a receiver following the same script reconstructs both
        let mut incoming = IntrospectionRegistry::new();
        assert_eq!(decode(&first, &mut incoming).unwrap(), Some(field.clone()));
        assert_eq!(decode(&second, &mut incoming).unwrap(), Some(field));
    }

    /// Both captured handshakes decode to the same logical structure.
    ///
    /// The introspection bytes here are verbatim from `tools/captures/`; the *data* bytes
    /// are not, because the real ones carry the capturing user's username and hostname and
    /// this repository is published. pvxs sends empty strings for both fields anyway, and
    /// the pvAccessCPP payload below substitutes placeholders of the same shape.
    #[test]
    fn both_captured_handshake_forms_decode_alike() {
        // pvxs 1.5.2: FULL_TYPE_CODE, inline, no id registered at all
        let pvxs = [
            0x80, // structure, no id
            0x00, // empty type id
            0x02, // two fields
            0x04, b'u', b's', b'e', b'r', 0x60, //
            0x04, b'h', b'o', b's', b't', 0x60,
        ];
        // pvAccessCPP 7.1.7: FULL_WITH_ID, registering id 1, then the identical body
        let mut pvaccesscpp = vec![FULL_WITH_ID_TYPE_CODE, 0x01, 0x00];
        pvaccesscpp.extend_from_slice(&pvxs);

        let mut from_pvxs = IntrospectionRegistry::new();
        let mut from_pvaccesscpp = IntrospectionRegistry::new();
        let one = decode(&pvxs, &mut from_pvxs).unwrap().unwrap();
        let other = decode(&pvaccesscpp, &mut from_pvaccesscpp)
            .unwrap()
            .unwrap();

        assert_eq!(one, other, "the two forms carry the same logical structure");
        assert_eq!(one, auth_structure());

        // The point of the divergence: against pvxs the registry stays empty for the whole
        // connection, so "the registry is populated" is never a usable precondition
        assert!(from_pvxs.is_empty());
        assert_eq!(from_pvaccesscpp.len(), 1);
    }

    /// A registry that has filled up declines to allocate, and everything goes inline.
    #[test]
    fn a_full_registry_falls_back_to_inline() {
        // Limit zero means "never register", which is legal and is what pvxs does
        let mut never = IntrospectionRegistry::with_limit(0);
        let field = auth_structure();
        assert_eq!(never.register_for_send(&field), None);
        let bytes = encode(&field, &mut never);
        assert_eq!(bytes[0], 0x80, "inline form");
        assert!(never.is_empty());
        assert_eq!(never.limit(), 0);

        // A limit of one: the first structure registers, later distinct ones do not
        let mut one = IntrospectionRegistry::with_limit(1);
        let first = encode(&field, &mut one);
        assert_eq!(first[0], FULL_WITH_ID_TYPE_CODE);
        let other = Field::Structure(StructureField::with_id("other_t").with("n", ScalarType::Int));
        assert_eq!(encode(&other, &mut one)[0], 0x80, "no room, so inline");
        // ... while the one that did register still shortens
        assert_eq!(encode(&field, &mut one), [ONLY_ID_TYPE_CODE, 0x01, 0x00]);

        // The advertised maximum is carried onto the send side only
        let types = ConnectionTypes::with_outgoing_limit(4);
        assert_eq!(types.outgoing.limit(), 4);
        assert_eq!(
            types.incoming.limit(),
            DEFAULT_REGISTRY_LIMIT,
            "we must remember whatever the peer chose to send"
        );
    }

    /// An id nobody registered is a decode error, not a silent empty structure.
    #[test]
    fn unregistered_ids_are_rejected() {
        let mut registry = IntrospectionRegistry::new();
        let error = decode(&[ONLY_ID_TYPE_CODE, 0x07, 0x00], &mut registry).unwrap_err();
        assert!(matches!(error, PvaError::Malformed(_)));
        assert!(!error.is_incomplete());

        // Registering a different id does not help
        registry.insert(1, auth_structure());
        assert!(decode(&[ONLY_ID_TYPE_CODE, 0x07, 0x00], &mut registry).is_err());
        assert!(decode(&[ONLY_ID_TYPE_CODE, 0x01, 0x00], &mut registry).is_ok());
    }

    #[test]
    fn registry_accessors() {
        let mut registry = IntrospectionRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.limit(), DEFAULT_REGISTRY_LIMIT);

        let field = auth_structure();
        assert_eq!(registry.allocate(field.clone()), Some(1));
        assert_eq!(registry.id_of(&field), Some(1));
        assert_eq!(registry.get(1), Some(&field));
        assert_eq!(registry.len(), 1);

        let other = Field::Scalar(ScalarType::Int);
        assert_eq!(registry.id_of(&other), None);
        assert_eq!(registry.allocate(other), Some(2));

        registry.clear();
        assert!(registry.is_empty());
        // Ids restart after a clear
        assert_eq!(registry.allocate(field), Some(1));
    }
}
