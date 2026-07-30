# Adding pvAccess to epicars — implementation plan

**Status:** plan only. Nothing here is implemented. This document assumes the
decision to proceed has been taken; if it has not, read
[`pvaccess-feasibility.md`](pvaccess-feasibility.md) first — that is the
investigation this plan was derived from, and this document cites it (as "§n")
for every "why" rather than restating the argument.

**Scope:** the **server side**, phases 0–3. The end state is a pvAccess server
that answers `pvget`, `pvput` and `pvmonitor` over NTScalar, NTScalarArray and
NTEnum, running alongside the existing CA server over one shared `Provider`. The
pvAccess *client* is out of scope — see [Out of scope](#out-of-scope).

Consistent with the crate's existing position: a standalone implementation, no
dependency on `epics-base`, no external pvAccess library.

Code references are against `b09d638` and will drift; treat line numbers as
signposts, not addresses.

## Important

Please commit after every logical task/distinct subtask. Implement tests for
all new features.

## Contents

1. [How to read this](#how-to-read-this)
2. [Phase 0 — decouple the data model](#phase-0--decouple-the-data-model)
3. [Phase 1 — pvAccess wire layer](#phase-1--pvaccess-wire-layer)
4. [Phase 2 — pvAccess server](#phase-2--pvaccess-server)
5. [Phase 3 — the `Provider` decision](#phase-3--the-provider-decision)
6. [Out of scope](#out-of-scope)
7. [Verification](#verification)
8. [Appendix: pre-existing defects](#appendix-pre-existing-defects-non-blocking)
9. [References](#references)

## How to read this

Each phase is a list of numbered work items. Each item states **what changes**,
**where**, and **how you know it is done**. Items are sized at roughly half a day
to three days.

Items within a phase are mostly independent and can be reordered, with two
exceptions called out explicitly: **1.2** (endianness) must come before everything
else in Phase 1, and **0.1**–**0.2** must come before **0.3**–**0.4**.

Phases are *not* independent. Phase 0 is a prerequisite for Phase 1; Phase 2
depends on Phase 1; Phase 3 depends on Phase 2. Phase 0 alone is worth doing even
if the rest is abandoned (§5).

Rough effort, carried over from the feasibility study: Phase 0 is 1–2 weeks,
Phase 1 is 3–4 weeks, Phase 2 is 2–3 weeks, Phase 3 is about a week. Call it
**7–10 weeks** of focused work, against an estimated 3 000–4 500 new lines.

The [appendix](#appendix-pre-existing-defects-non-blocking) lists pre-existing
defects that sit on this path. **They gate nothing.** They are recorded because a
refactor is the natural time to notice them, and each notes the point at which it
stops being ignorable.

---

## Phase 0 — decouple the data model

*No pvAccess code is written in this phase.*

`dbr.rs` currently does two jobs: it is CA's wire format *and* it is the crate's
data model (§1). pvAccess forces those apart. This phase does the split, on its own,
with **zero intended behaviour change** — the existing test suite is the oracle.

This is the de-risking step, and §5 argues it pays for itself regardless of what
follows.

The approach is Option B from §3.4: a neutral value type with adapters both ways.
Option A (widen `DbrValue` to the union of both type systems) was rejected because
it deepens the conflation that is the underlying problem; Option C (make the neutral
type the *only* internal model and demote DBR to a pure codec) is the clean end
state, to be converged on from B rather than attempted directly.

### 0.1 — Introduce the neutral value model

Create `src/value.rs` with a properly recursive value type:

```rust
pub enum Value {
    Scalar(Scalar),
    ScalarArray(ScalarArray),
    Structure(Structure),              // ordered named fields + optional type id
    StructureArray(Vec<Option<Structure>>),
    Union(Box<UnionValue>),
    VariantUnion(Option<Box<Value>>),
}
```

This must cover what `DbrValue` (`dbr.rs:104`) cannot: `bool`, unsigned widths,
64-bit integers, nesting, unions, structure arrays. Without those a pvAccess server
could not serve NTNDArray, NTTable or a proper NTEnum — which is most of the reason
to want pvAccess at all (§3.2).

**Introspection is a separate type**, not a field on `Value`. Define `Field`
alongside it, describing shape without carrying data. This is deliberate: pvAccess
transmits FieldDesc separately from data and caches it per connection, so the two
must not be welded together the way `DbrType` welds them for CA. §3.1 is the reason
this matters — CA's "introspection" *is* a `u16` identity, computed as pure
arithmetic (`dbr.rs:842-849`), with no registry and no negotiation. Nothing in that
scheme can be extended to carry a user-defined structure.

**Done when:** `Value` and `Field` exist with constructors and accessors, and unit
tests construct a nested structure containing an unsigned 64-bit array — something
inexpressible today.

### 0.2 — Conversions between `DbrValue` and `Value`

- `From<&DbrValue> for Value` — **total**. `DbrValue` is a strict subset.
- `TryFrom<&Value> for DbrValue` — **partial**. Fails on structures, unions,
  structure arrays, 64-bit integers, and unsigned values outside the signed range.

**Done when:** a round-trip test covers every `DbrValue` variant
(`Enum`, `String`, `Char`, `Int`, `Long`, `Float`, `Double`) and returns the
original, and the partial direction has a negative test per rejection reason.

### 0.3 — Neutral metadata

§3.3 is the encouraging finding here: the metadata concepts line up almost exactly.
`Dbr::Time { status, timestamp, value }` — which is what `PV::load_for_ca` already
returns (`intercom.rs:130`) — maps essentially 1:1 onto NTScalar, and CA's
`Graphics`/`Control` categories are precisely NT's `display_t`/`control_t`.

Add neutral `Alarm`, `TimeStamp`, `Display` and `Control` structs, and a carrier:

```rust
pub struct Meta {
    pub alarm: Option<Alarm>,
    pub timestamp: Option<TimeStamp>,
    pub display: Option<Display>,
    pub control: Option<Control>,
}
```

**`Dbr` keeps its variants.** It becomes a *projection* of `(Value, Meta)` at the
wire boundary, via constructors — so none of the ~9 provider call sites in
`server.rs` churn. This is deliberately a smaller step than §3.3's "decompose the
category enum into composable optional fields": full decomposition changes the CA
wire path and cannot be behaviour-neutral. Revisit it in Phase 3, once pvAccess has
shown what the metadata actually needs to do.

**The two protocols do not share an epoch.** CA timestamps count from the EPICS
epoch, 1990-01-01; pvAccess `time_t.secondsPastEpoch` counts from the POSIX epoch,
1970-01-01. The offset is **631 152 000 seconds**. Confirmed against a real IOC —
`pvxget` on an unprocessed record reports `secondsPastEpoch = 631152000`, which is
a zero EPICS timestamp expressed in POSIX terms.

`TimeStamp` must therefore be **epoch-neutral internally** — store `SystemTime`, as
`PV.timestamp` (`intercom.rs:109`) already does, and convert *at each wire
boundary*, not on the way in. Getting this wrong is a 20-year offset that every
client displays without complaint, so it will not surface as an obvious failure.
Note also that pvAccess's `secondsPastEpoch` is a signed **64-bit** value, against
CA's 32-bit `stamp`, and carries a third field CA has no analogue for: `userTag`.

**Done when:** `Dbr` can be built from and projected back to `(Value, Meta)`
losslessly for the `Basic`, `Status` and `Time` categories; a round-trip through
both epochs returns the original `SystemTime`; and the existing suite passes
untouched.

### 0.4 — Move `IntercomProvider`'s storage to `Value`

§2.4 finds `intercom.rs` already about 70% protocol-neutral, with the seam already
half-cut — `load_for_ca` / `store_from_ca` are named for it. The one real leak is
the fan-out payload:

```rust
sender: broadcast::Sender<Dbr>,   // intercom.rs:111 — CA-typed
```

Change `PV.value` to `Arc<Mutex<Value>>` and `sender` to
`broadcast::Sender<(Value, Meta)>`. `load_for_ca` and `store_from_ca` stay exactly
where they are, as the named CA adapters; `load_for_pva` is their natural sibling in
Phase 3.

**Two properties are load-bearing and must survive:**

1. `store()` remains callable from **non-async** code while the server runs on
   tokio. This is the entire point of `Intercom<T>`.
2. `store()` tolerates a full trigger queue — it uses `try_send` and treats
   `TrySendError::Full` as success (`intercom.rs:183-192`), because the value is
   still in the broadcast buffer and the subscriber will pick it up. Do not
   "fix" this into an await.

**Done when:** both properties have explicit tests if they do not already, and the
existing suite passes.

### 0.5 — Cargo features

There are currently no Cargo features at all. Add:

```toml
[features]
default = ["ca"]
ca = []
pva = []
```

and gate the module tree in `lib.rs`, so CA-only users do not pay for pvAccess.
No new dependencies are required at any point in this plan — `nom`, `tokio`,
`tokio-util`, `socket2` and `pnet` cover everything, and the 12-byte server GUID
needs no `uuid` crate (§2.6).

**Done when:** `--no-default-features --features ca`, `--features pva`, and the
default all build and test clean.

### 0.6 — Fix `unique_subscriber_id`

**This is the one Phase 0 item that changes the `Provider` signature.** It is here
rather than in Phase 3 because Phase 2 makes the bug live.

The ID is not generated by any registry. The server computes it at both call sites
(`server.rs:643`, `:676`) as a packed pair:

```rust
(self.id << 32) | channel.server_id as u64
```

`self.id` is a circuit counter local to the accept loop, restarting at 0 for each
`Server`. The provider uses the result purely as an opaque key —
`pv.triggers.insert(id, trigger)` (`intercom.rs:520`).

**Two servers sharing one provider therefore collide.** CA circuit 0 / channel 0 and
PVA circuit 0 / channel 0 produce the same key, and one silently overwrites the
other's trigger sender (§2.3).

Fix by having the **provider allocate and return** an opaque `SubscriberId`, rather
than partitioning the existing space with a protocol tag. Allocation-by-owner is the
correct shape and costs the same to implement.

This is cheap: there are exactly **two** implementors in the tree —
`IntercomProvider`, and the two-method `BlankProvider` used by a single test
(`server.rs:955`). The trait has never been stress-tested by a second serious
implementor, so now is the time.

**Done when:** a test runs two `Server`s over one cloned provider and both receive
their own monitor updates. This test fails today.

---

## Phase 1 — pvAccess wire layer

New module `src/pva/`, gated on the `pva` feature.

**This phase does not touch `messages.rs` at all.** §2.1 establishes why that is
possible: `messages.rs` only ever carries `Vec<u8>` payloads plus a `DbrType` in the
header slot, with all value serialisation living in `dbr.rs`. The sole exception is
`EventAdd::respond` (`messages.rs:1521`), which a pvAccess codec never reaches.

One CA wart does not follow us. The `Message` / `ClientMessage` split exists only
because CA overloads command IDs by direction — command 1 is both `EventAddResponse`
and `EventCancelResponse`, disambiguated by a payload-size heuristic duplicated at
`messages.rs:454` and `:630`. pvAccess puts direction in a header flag bit, so this
phase needs **one** message enum, not two.

What *does* transfer from `messages.rs`: the `impl_from_for!` macro
(`messages.rs:535`, already fully generic), the *shape* of the `MessageError` enum
(`messages.rs:737-789`), the decoder-is-also-the-item trick, and the
peek → header-len → payload-len → reserve → advance pattern.

What does **not** transfer is nom itself. Item **1.2** settles that up front, and
everything else in this phase is written against the reader/writer it defines — so
read 1.2 before starting any other item here.

This whole phase is unit-testable with **no network**.

### 1.1 — The 8-byte header

`src/pva/header.rs`. Unlike CA's 16-byte header with its 32-byte extended form,
pvAccess's header is 8 bytes, always:

| Field | Size | Notes |
|---|---|---|
| magic | 1 | always `0xCA` |
| version | 1 | |
| flags | 1 | bit 0 app/control, bits 4–5 segmentation, bit 6 direction, bit 7 endianness |
| command | 1 | |
| payload size | 4 | `int32`, byte order per flags bit 7 |

**Done when:** header round-trips for every flag combination in both byte orders.

### 1.2 — Byte-order-aware reader/writer — build this first

**Everything downstream depends on this item.** §6 ranks it third among
underestimated risks: cheap to get right at the start, expensive to retrofit. Build
it before 1.1's parsing, before 1.3, before anything.

CA is fixed big-endian, and `messages.rs` uses `be_u16` / `be_u32` throughout
(already with `::<&[u8], nom::error::Error<&[u8]>>` turbofish noise in about eight
places). pvAccess **negotiates byte order per connection**, via the `SET_BYTE_ORDER`
control message and flags bit 7. Copy the existing style naively and you write every
parser twice.

**Decision: `src/pva/` does not use nom.** Instead, a runtime `ByteOrder` carried on
small reader/writer wrappers, with a decode/encode trait pair:

```rust
pub enum ByteOrder { Little, Big }

pub struct PvaReader<'a> { buf: &'a [u8], pos: usize, order: ByteOrder }
pub struct PvaWriter    { buf: Vec<u8>,             order: ByteOrder }

pub trait PvaDecode: Sized {
    fn decode(r: &mut PvaReader<'_>) -> Result<Self, PvaError>;
}
pub trait PvaEncode {
    fn encode(&self, w: &mut PvaWriter) -> Result<(), PvaError>;
}
```

Byte order lives on the reader/writer, so it is set once per connection (or per
message, from flags bit 7) and every `decode` implementation below is written once
and is order-agnostic by construction.

**Why not nom**, which is what the rest of the crate uses: nom's `be_*`/`le_*` split
forces each parser to be either duplicated or made generic over the number parser,
and the turbofish noise already visible in `messages.rs` gets worse. The two
alternatives considered and rejected were a generic const parameter (infects every
type signature in the module) and two generated parser sets (doubles the surface to
test).

This is a **deliberate, approved departure** from `messages.rs`'s style, scoped to
`src/pva/`. **CA keeps nom; do not migrate it.** The inconsistency is the price of
not writing the pvAccess parsers twice, and it is recorded here so it reads as a
decision rather than as drift.

Two consequences to carry through the rest of Phase 1:

- `PvaError` is this module's error type, replacing the nom `ParseError` glue. It
  still mirrors the shape of `MessageError` (`messages.rs:737-789`) — the *shape*
  transfers even though the parser library does not.
- Incomplete input must be distinguishable from malformed input, since 1.9's
  `Decoder` needs "not enough bytes yet, try again" as a non-error. nom gives this
  for free via `Err::Incomplete`; here it has to be an explicit `PvaError` variant.

**Done when:** the reader/writer pair exists with a `PvaError` distinguishing
incomplete from malformed, and a test decodes the same logical message from both
big- and little-endian byte sequences to an identical value.

### 1.3 — Primitive encodings

`src/pva/encoding.rs`. Use the specification's own byte vectors as test fixtures —
they are given in the [Data Encoding](https://docs.epics-controls.org/en/latest/pv-access/Protocol-Encoding.html)
document and are the cheapest correctness check available.

**Size** (variable-length):

| Value | Encoding |
|---|---|
| null | `0xFF` |
| 0 – 253 | single byte |
| 254 – 2³¹−1 | `0xFE` then `i32` |
| ≥ 2³¹−1 | `0xFE`, then `i32` = 2³¹−1, then `i64` |

**String:** size is a **byte** count, not a character count; contents are UTF-8;
multi-byte characters must not be split; an empty string encodes as size 0.

**BitSet:** zero or more `u64`, then between zero and seven trailing `u8`. Bits
serialise in groups of eight in ascending order, LSB to MSB.
Fixtures: `{0}` → `01 01`; `{63}` → `08 00 00 00 00 00 00 00 80`.

**Status:** `0xFF` is the shortcut for OK-with-no-message and occupies a single
byte. Otherwise a type byte — `0x00` OK, `0x01` WARNING, `0x02` ERROR, `0x03` FATAL
— followed by message and callTree strings.
Fixture: `WARNING, "Low memory", ""` → `01 0A 4C 6F 77 20 6D 65 6D 6F 72 79 00`.

**Done when:** every fixture above is a passing test, plus round-trip tests across
the size-encoding boundaries (253/254, and the `i32`→`i64` escalation).

### 1.4 — FieldDesc introspection encoding

`src/pva/introspection.rs`. Encode and decode `Field` (from 0.1).

Type codes:

| Code | Name | Payload |
|---|---|---|
| `0xFF` | NULL_TYPE_CODE | none |
| `0xFE` | ONLY_ID_TYPE_CODE | id — refers to the cache |
| `0xFD` | FULL_WITH_ID_TYPE_CODE | id + FieldDesc |
| `0xFC` | FULL_TAGGED_ID_TYPE_CODE | id + tag + FieldDesc |
| ≤ `0xDF` | FULL_TYPE_CODE | FieldDesc only |

Scalar type byte layout:

- **bits 7–5** kind: `000` boolean, `001` integer, `010` floating-point,
  `011` string, `100` complex
- **bits 4–3** array form: `00` scalar, `01` variable-size, `10` bounded-size,
  `11` fixed-size
- **bits 2–0** detail: for integers, bit 2 is the unsigned flag and bits 1–0 select
  width (`00` byte, `01` short, `10` int, `11` long); for floats, `010` is f32 and
  `011` is f64

Fixtures: signed `i32` scalar = `0x22`; unsigned `u64` scalar = `0x27`.

**Done when:** every scalar type and array form round-trips, and a nested structure
with a type ID (`epics:nt/NTScalar:1.0`) round-trips.

### 1.5 — Per-connection introspection cache

§6 ranks this **the single most underestimated risk** in the whole effort, and the
likeliest source of "works against my own client, breaks against pvxs" bugs. Treat
it accordingly.

It is stateful, per-connection, and **bidirectional** — the link is full-duplex and
each direction caches independently, so there are **two** registries per connection,
not one. IDs are overridable mid-connection.

**The two implementations already disagree, in the first message a client sends.**
Captured with `tools/capture-pva.sh` against one soft IOC — this is the same
`CONNECTION_VALIDATION` response from each client, carrying the same logical
`{ string user; string host; }` structure:

| | pvAccessCPP 7.1.7 | pvxs 1.5.2 |
|---|---|---|
| payload | 60 bytes | 28 bytes |
| introspection | `fd 0100 80 …` | `80 …` |
| form | **FULL_WITH_ID** (`0xFD`), registers ID 1 | **FULL_TYPE_CODE** (`0x80`), inline, no ID |

**Accept both forms on the receive side.** A server that assumes clients always
register IDs breaks against pvxs; one that assumes they never do breaks against
pvAccessCPP. Neither client is wrong — `0x80` is ≤ `0xDF`, so it is the legitimate
no-ID form.

Note this also means the receive-side registry can stay **empty for an entire
connection** with pvxs, so "registry is populated" is not a usable precondition
anywhere.

**Done when:** a test drives a scripted exchange that introduces an ID, refers to it
by `ONLY_ID`, overrides it mid-stream, and refers to it again — the send-side and
receive-side registries are proven independent — and both captured handshakes in
`tools/captures/` decode to the same logical structure.

### 1.6 — Segmentation reassembly

pvAccess messages align to 64-bit boundaries and may split across frames — flags
bits 4–5 encode not-segmented / first / last / middle — **preserving alignment
padding between segments**. The decoder must reassemble before yielding a logical
message. There is no CA analogue to learn from, and §6 ranks it fourth: easy to get
subtly wrong.

**Done when:** a multi-segment message decodes byte-identically to the same message
delivered whole, tested at *every* possible split offset.

### 1.7 — Control messages

Handled **below** the application message enum, since they interleave with
application messages:

| Code | Name |
|---|---|
| `0x00` | Mark Total Byte Sent |
| `0x01` | Acknowledge Total Bytes Received |
| `0x02` | Set byte order |
| `0x03` | Echo request |
| `0x04` | Echo response |

**Done when:** a stream interleaving control and application messages yields only
the application messages upward, with byte order and echo handled beneath.

### 1.8 — The message enum

`src/pva/messages.rs` — a single `PvaMessage`. The commands needed for Phase 2:

| Code | Name | | Code | Name |
|---|---|---|---|---|
| `0x00` | BEACON | | `0x09` | CONNECTION_VALIDATED |
| `0x01` | CONNECTION_VALIDATION | | `0x0A` | GET |
| `0x02` | ECHO | | `0x0B` | PUT |
| `0x03` | SEARCH | | `0x0D` | MONITOR |
| `0x04` | SEARCH_RESPONSE | | `0x0F` | DESTROY_REQUEST |
| `0x07` | CREATE_CHANNEL | | `0x11` | GET_FIELD |
| `0x08` | DESTROY_CHANNEL | | `0x12` | MESSAGE |
| | | | `0x15` | CANCEL_REQUEST |

Reuse `impl_from_for!` (`messages.rs:535`) and mirror the `MessageError` shape.

**Done when:** each listed command round-trips through encode/decode, and unknown
command IDs produce a recoverable error rather than a parse failure that kills the
connection.

### 1.9 — Framed codec

tokio-util `Decoder` / `Encoder`, following the existing
peek → header-len → payload-len → reserve → advance pattern from
`RawMessageDecoder`.

This is where 1.2's incomplete-vs-malformed distinction earns its keep: an
incomplete `PvaError` maps to `Ok(None)` — "call me again with more bytes" — while a
malformed one is a real decode error. Getting these confused produces either a
connection that drops on every partial read or one that spins forever on a
corrupt frame.

**Done when:** the codec is driven over a `tokio_test::io` mock with the stream
chopped at arbitrary boundaries, and yields the same messages regardless.

### 1.10 — Normative Types

`NTScalar`, `NTScalarArray` and `NTEnum` builders over `Value`.

§3.3 lays out how directly this maps:

```
structure "epics:nt/NTScalar:1.0"
    <scalar>  value                                                     <- Value
    alarm_t   alarm     { int severity, int status, string message }    <- Alarm
    time_t    timeStamp { long secondsPastEpoch, int nanoseconds, int userTag }
    display_t display   { limitLow, limitHigh, description, units, precision, form }
    control_t control   { limitLow, limitHigh, minStep }
```

**Note the CA payoff here.** NTEnum's `enum_t { int index; string[] choices }` is
exactly the shape CA needs for `DBR_GR_ENUM` and currently lacks:
`DbrGraphics::Enum` is a **fieldless unit variant** (`dbr.rs:614`), so the
`[[u8;26];16]` choice table has no home, and `MAX_ENUM_STRING_SIZE` /
`MAX_ENUM_STATES` (`dbr.rs:79-80`) are declared and never referenced. Doing this
properly supplies both the motivation and the shape for closing two of the three
known CA gaps in the README — ENUM, and CTRL/GR (§3.3).

Be aware that `load_for_ca` currently always returns `Status::default()`
(`intercom.rs:146`), so **alarm status is never populated by the reference
provider**; NTScalar's `alarm` field will be equally empty until that is addressed.

**Done when:** each NT builder produces bytes that `pvget` renders correctly — see
the note in [Verification](#verification) about starting interop testing in week 1
of this phase, not at the end of Phase 2.

---

## Phase 2 — pvAccess server

`src/pva/server.rs`. §2.5 finds `server.rs`'s *structure* 60–70% reusable and its
*code* essentially 0% — so this phase copies shapes, not lines.

### 2.1 — Server skeleton

`PvaServerBuilder` → `PvaServerHandle`, mirroring `ServerBuilder` / `ServerHandle`.

Reuse in shape, all of it protocol-agnostic already: `ServerHandle`'s
`Drop`-cancels-the-`CancellationToken` semantics (`server.rs:102-106`, so a dropped
handle stops the server), `CancellationToken` propagation, `JoinSet` task
supervision with first-error-wins join (`server.rs:214-226`), `try_bind_ports`
(`server.rs:172`), and the oneshot port-report handshake.

**Done when:** an empty PVA server starts, reports its bound ports, and stops when
its handle drops.

### 2.2 — `EPICS_PVA_*` environment defaults

Add siblings to the `EPICS_CA_*` helpers in `src/utils.rs`. There are no port
collisions with CA (§2.6):

| | CA | PVA |
|---|---|---|
| TCP | 5064 | 5075 |
| UDP search | 5064 | 5076 |
| Beacon | 5065 | 5076 (plus multicast `224.0.0.128:5076`) |
| Env prefix | `EPICS_CA_*` | `EPICS_PVA_*` |

`get_target_broadcast_ips` (`utils.rs:50`) enumerates interfaces via `pnet` and is
directly reusable once parameterised by which env var it reads;
`new_reusable_udp_socket` (`utils.rs:12`) is reusable unchanged.

Read ports through these helpers rather than hardcoding, per the crate convention.

**Done when:** each `EPICS_PVA_*` variable has a test proving it is honoured, and
the defaults match the table.

### 2.3 — UDP search

Listen on 5076 including multicast `224.0.0.128:5076`. Handle SEARCH (`0x03`) and
reply with SEARCH_RESPONSE (`0x04`) carrying the 12-byte server GUID and TCP port,
with duplicate suppression.

§2.5 calls `listen_for_searches` (`server.rs:281-366`) the strongest genericisation
candidate in the codebase: PVA search is also UDP with duplicate suppression, so it
is the same plumbing behind a different codec. Genericise it rather than copying it.

**Done when:** `pvlist` discovers the server, and a duplicated search datagram
produces one response.

### 2.4 — Beacons

BEACON (`0x00`) on 5076, carrying the GUID. **Default to off in tests** — see the
test hygiene requirement in [Verification](#verification).

**Done when:** beacons appear on the wire at the configured period, and
`.beacons(false)` suppresses them completely.

### 2.5 — TCP accept and connection validation

Accept on 5075. The handshake below is **confirmed on the wire**, not inferred —
captured from base 7.0.8.1's `softIocPVA` with `tools/capture-pva.sh`, and identical
against both client implementations. Byte offsets are from the start of each
pvAccess message; the server sent messages 1 and 2 in a single TCP segment.

```
1. server -> client    ca 02 41 02  00000000
                       SET_BYTE_ORDER, a control message (flags bit 0 set).

2. server -> client    ca 02 40 01  14000000  <20 byte payload>
                       CONNECTION_VALIDATION. MUST be the first application
                       message on the connection.
                         00440000        i32  server receive buffer   = 17408
                         ff7f            i16  introspection reg max   = 32767
                         02              size 2 - auth method count
                         09 "anonymous"
                         02 "ca"

3. client -> server    ca 02 00 01  <size>  <payload>
                       CONNECTION_VALIDATION response, selecting a method. The
                       client MUST NOT send anything before receiving step 2.
                       See 1.5 - the two implementations encode this payload's
                       introspection differently.

4. server -> client    ca 02 40 09  01000000  ff
                       CONNECTION_VALIDATED, payload is a Status using the
                       0xFF OK-with-no-message shortcut.
```

Four things that capture settles, all of which apply beyond this item:

- **Direction is flags bit 6**, set on server→client (`0x40`, `0x41`) and clear on
  client→server (`0x00`). This is what removes the need for CA's
  `Message`/`ClientMessage` split.
- **Byte order was little-endian**, flags bit 7 clear, and payload sizes read LE.
- **Protocol version is `0x02`.**
- **`0x09` is server→client**, resolving what this plan previously flagged as an
  open question against ambiguous spec prose.

Support `anonymous` and `ca` auth only; AUTHNZ (`0x05`) is deferred.

**Do not build policy on the client's identity.** pvAccessCPP sends real values for
the `user` and `host` fields of its auth structure; **pvxs sends empty strings for
both**. `get_access_right`'s `client_user_name` / `client_host_name` are already
`Option`, so the types are right — but treat absent-or-empty as the normal case
rather than a fallback.

The TCP accept-loop skeleton at `server.rs:368-410` transfers almost entirely —
§2.5 notes only line 395 is CA-specific.

**Done when:** both `pvinfo` and `pvxinfo` complete a handshake against the server,
and the bytes it emits for steps 1, 2 and 4 match `tools/captures/` exactly.

### 2.6 — Circuits and channels

`PvaCircuit` / `PvaChannel`, handling CREATE_CHANNEL (`0x07`) and DESTROY_CHANNEL
(`0x08`), with the three-arm `tokio::select!` over {cancel, monitor trigger, inbound
message} copied in structure from `server.rs:501`.

**Keep the two-channel pull model, and do not "improve" it into a push model.**
§2.2 argues it is an asset *for pvAccess specifically*: the provider sends a PV
*name* down an `mpsc::Sender<String>` trigger, the circuit wakes, and only then reads
the broadcast receiver and formats per subscription. pvAccess monitors have
pipelined flow control — the client grants a window via `nfree` and the server sends
only while the counter is positive — so a model where the **transport** decides when
to consume suits pvAccess better than a push model would.

**Done when:** channels create and destroy cleanly, and a client disconnecting
mid-operation does not leak a channel or a task.

### 2.7 — GET

GET (`0x0A`), with its three subcommand phases:

- **INIT**, subcommand `0x08` — returns the FieldDesc.
- **EXEC**, subcommand `0x00` (also seen as `0x40` in the wild) — returns a
  changed-BitSet plus the data.
- **DESTROY**, the additional `0x10` mask.

**Done when:** `pvget SOME_PV` returns the correct value for every scalar type.

### 2.8 — PUT

PUT (`0x0B`), same INIT / EXEC / DESTROY phasing, with the write scoped by the
BitSet the client sends.

**Done when:** `pvput` writes each scalar type, and a write to a `read_only` PV is
refused with a sensible `Status` rather than a panic.

### 2.9 — MONITOR

MONITOR (`0x0D`). The subcommand encoding is bit-tested rather than enumerated:

| Test | Meaning |
|---|---|
| `== 0x08` | INIT |
| `== 0x88` | INIT using the pipeline protocol |
| `& 0x44 == 0x44` | Stopped → Running |
| `& 0x44 == 0x04` | Running → Stopped |
| `& 0x10` | terminate the subscription |
| `& 0x80` | acknowledgement incrementing the pipeline flow-control window |

**Known limitation to document in the code:** the trigger carries only a PV name, so
the pvAccess layer cannot know *which fields* changed and must send an **all-ones
changed-BitSet on every monitor update**. This is legal, not optimal, and acceptable
for a first version (§2.2).

This item is the most likely to force one of the appendix defects — specifically
one-subscription-per-channel and the linear trigger name search. See the
[appendix](#appendix-pre-existing-defects-non-blocking).

**Done when:** `pvmonitor` receives updates on `store()`, start/stop transitions
work, and terminating a subscription releases the trigger.

### 2.10 — GET_FIELD

GET_FIELD (`0x11`) — introspection without a value.

**Done when:** `pvinfo SOME_PV` prints the correct structure.

### 2.11 — pvRequest

§6 ranks this **second** among underestimated risks, and it is where interop pain
concentrates.

The specification explicitly leaves the pvRequest structure unspecified for a future
revision. Real clients nonetheless send `field()`, `field(value)`,
`field(value,alarm,timeStamp)` and `record[queueSize=N]`. **Implement the de-facto
behaviour, not the spec.** Acceptance for this item is interop, not conformance.

The CA analogue is `DbrCategory` — Basic / Status / Time / Graphics / Control is in
effect "which subset of the fields do you want", which is what the pvRequest field
mask expresses. Same idea, different mechanism (§3.3).

**Done when:** each of the four forms above is honoured, verified against real
`pvget`/`pvmonitor` invocations rather than self-written clients.

### 2.12 — Lifecycle events

`ServerEvent` (`server.rs:110-158`) has CA-flavoured vocabulary — circuit/channel,
with `channel_id: u32` being the CA SID — but every concept has a pvAccess analogue.
It has **no protocol discriminator field**; add one.

Decide and record in the code: one shared event stream across both servers, or one
per server. Widen or duplicate the port-report tuple accordingly —
`Server::listen` currently reports through a single `oneshot::Sender<(u16, u16)>`
(`server.rs:191-227`).

**Done when:** a consumer of `listen_to_events` can tell which protocol an event
came from.

### 2.13 — Explicitly deferred

Listed so their absence is a recorded decision rather than an oversight. `pvget`,
`pvput` and `pvmonitor` need none of them:

RPC (`0x14`), PUT_GET (`0x0C`), ARRAY (`0x0E`), PROCESS (`0x10`),
ACL_CHANGE (`0x06`), AUTHNZ (`0x05`), MULTIPLE_DATA (`0x13`, deprecated),
ORIGIN_TAG (`0x16`).

Each should return a well-formed error `Status`, not a dropped connection.

### 2.14 — Example

`examples/simple-pva-intercom.rs`, mirroring `examples/simple-intercom.rs`. This is
the interop test harness for the whole phase.

---

## Phase 3 — the `Provider` decision

**The point of deferring this to Phase 3 is that only now is the requirement
actually known.** §5 is explicit about it, and the ordering below reflects that: the
blanket adaptation ships first, and any new trait comes after.

§2.2 finds the contract shape right and the currency wrong — five of six methods
carry CA wire types, and the whole abstraction boundary is **80 lines**
(`src/providers/mod.rs`).

### 3.1 — Blanket adaptation — do this first

Any existing `Provider` becomes automatically servable over pvAccess as
NTScalar / NTScalarArray, **with no user code change**. §5 calls this the
highest-leverage move available, and it is.

**Done when:** the README's `i32` example serves `pvget` with exactly one line added
(`PvaServerBuilder::new(provider.clone()).start().await?`).

### 3.2 — Move `messages::Access`

Lowest-severity leak (§2.2): `{None, Read, Write, ReadWrite}` is already
semantically protocol-neutral, it just lives in the CA module. A move plus a
re-export is enough.

### 3.3 — Replace `MonitorMask` in the trait

Medium severity on paper, **free in practice**. It is threaded through and never
read: the server stores it in `PVSubscription.mask` (`server.rs:442`) and never
consults it, and `IntercomProvider` binds it as `_mask` (`intercom.rs:511`). Dead
weight today, so it can be replaced by a neutral subscription-options type at no
cost.

### 3.4 — Un-smuggle `DBR_CLASS_NAME`

`intercom.rs:132` overloads `requested_type == Some(DBR_CLASS_NAME)` in `read_value`
to mean "give me the EPICS record type name" rather than a value — a CA-only RPC
riding the value channel (§2.2). Replace with an explicit `record_type()` method on
the trait.

### 3.5 — Optional richer trait

An opt-in trait for providers that want to expose real structures — NTTable,
NTNDArray. **Design only if a caller needs it.** Note that the existing
`T: TryFrom<DbrValue>, DbrValue: From<T>` bound on `Intercom<T>` is the right
*shape* to generalise — a `From`/`TryFrom` pair — but it is backed by three macros
and about two dozen hand-written impls over eleven types, with no derive macro and
no struct support. Normative Types will need a derive (§3.4).

### 3.6 — Revisit the trait bounds

Two things to record while the trait is open:

- The **`Default` bound is odd**. It exists only so `Server<L>` can hand-implement
  `Default` (`server.rs:54`). Worth removing (§4).
- The trait is **not object-safe** (`Clone + Default + Sized`), so there is no
  `Box<dyn Provider>`. Everything is monomorphised — `Server<L>`, `Circuit<L>`,
  `ServerBuilder<L>`. Two servers over the same `L` is fine; a *heterogeneous* list
  of providers is not. Anyone designing per-protocol composition needs to know this
  before starting (§2.2).

### 3.7 — Both protocols at once

Document and test the target shape, which the existing `Provider: Clone` bound
already permits (§4):

```rust
let mut provider = IntercomProvider::new();
let value = provider.add_pv("NUMERIC_VALUE", 42i32)?;
let _ca  = ServerBuilder::new(provider.clone()).start().await?;
let _pva = PvaServerBuilder::new(provider).start().await?;
```

`IntercomProvider`'s `Arc<Mutex<HashMap<String, Arc<Mutex<PV>>>>>` means both
servers share state through the clone.

**Done when:** the end-to-end acceptance test in
[Verification](#verification) passes.

---

## Out of scope

**The pvAccess client** (§5 Phase 4, estimated 2–3 weeks on its own).

The reason is not effort, it is that the seam does not exist. The client today has
**zero** trait abstraction: `Client` is a concrete struct with six inherent methods,
typed end to end on `Dbr` / `DbrValue`, hardcoding `DbrCategory::Time`. A unified
two-protocol client means introducing that abstraction from scratch — as a `Client`
trait or as enum dispatch — which is a design question this plan does not answer.

When it is picked up:

- The actor pattern is worth copying as-is: `CircuitRequest` variants each carrying
  a `oneshot::Sender`, with handlers returning `Vec<Message>` for the loop to write.
- `examples/linear-caget.rs` is the best existing step-by-step reference for what a
  protocol client must actually do, in order.

Also out of scope: the deferred commands in [2.13](#213--explicitly-deferred), and
Option C from §3.4 (making `Value` the single internal model and demoting `dbr.rs`
to a pure codec). Option C remains the clean end state; converge on it from this
plan's Option B rather than attempting it directly.

## Verification

**Per-phase gates.** All of these must pass before a phase is called done:

```bash
cargo test --lib --tests                              # unit + integration
cargo test --no-default-features --features ca        # CA-only build
cargo test --features pva                             # with pvAccess
cargo clippy --lib --tests
cargo fmt --check
```

Pre-commit hooks already run `fmt`, `clippy` and `cargo check`, and CI runs
`cargo build` and `cargo test` on push/PR to `main`.

**Phase 0's gate is that the existing suite passes unchanged.** It is a refactor
with no intended behaviour change; any test that needs editing is a signal to look
harder at the change.

**Phase 1's gate is unit tests with no network**, using the specification byte
vectors from [1.3](#13--primitive-encodings) and [1.4](#14--fielddesc-introspection-encoding)
as fixtures, plus the real captures described below.

**Both client implementations are required, not one.** `tools/capture-pva.sh` drives
a loopback `softIocPVA` under `tcpdump` with **both** pvAccessCPP (from epics-base)
and pvxs, writing one pcap per implementation to `tools/captures/`. Run it before
starting item 1.4 — it is what items 1.1–1.5 and 2.5 are checked against, and it
already found the [1.5](#15--per-connection-introspection-cache) divergence, which
no single implementation would have revealed.

> `tools/captures/` is **gitignored**. The pvAccess auth handshake carries the
> capturing user's username and hostname, and this repository is published. The
> script regenerates the captures on demand; do not commit them.

**Phase 2's gate is interop against real tooling, not self-consistency.** Run
`pvget`/`pvput`/`pvmonitor`/`pvinfo`/`pvlist` **and** their pvxs counterparts
`pvxget`/`pvxput`/`pvxmonitor`/`pvxinfo`/`pvxlist` against
`examples/simple-pva-intercom.rs`. Passing one implementation is not passing. Per
§6, **start this in the first week of Phase 1** — the introspection registry is the
likeliest source of "works against my own client, breaks against pvxs" bugs, and
self-written clients will not find them.

**End-to-end acceptance for the whole plan:**

1. One `IntercomProvider`, both servers running over it.
2. `caget NUMERIC_VALUE` and `pvget NUMERIC_VALUE` return the same value.
3. A single `store()` from synchronous code produces an update on both `camonitor`
   and `pvmonitor`.

**Test hygiene requirement — applies to every new test in this plan.** Bind
ephemeral ports and disable beacons: the pvAccess equivalent of
`.connection_port(0).search_port(0).beacons(false)`, as `connected_client_server`
in `tests/server_tests.rs` does. CI runs plain `cargo test`, so a test that binds
5075/5076 or broadcasts on the facility network is both a CI problem and a problem
for everyone else on the subnet.

Note that the existing suite does not fully hold this line (see the appendix), so
there is no template to copy blindly — hold it for new code regardless.

## Appendix: pre-existing defects (non-blocking)

From §7. **These gate nothing in this plan.** They are recorded because they sit
directly on this path and a refactor is the natural time to notice them; each notes
where it stops being ignorable.

| Defect | Where | Becomes pressing at |
|---|---|---|
| 11 `todo!()`s, an `assert!`, and four `.unwrap()`s | `dbr.rs` 168, 643–644, 671–672, 732–733, 743–744, 1081–1082; `assert!` 353; `.unwrap()` 362, 363, 1059, 1109 | **1.10** — NTEnum is where the missing enum choices table gets a home |
| Two circuit-panicking `.unwrap()`s | `server.rs:788` (`read_value`), `server.rs:619` (`convert_to`) | any time a PV disappears between `CreateChannel` and `ReadNotify` |
| One subscription per channel | `Channel.subscription: Option<..>`, `server.rs:434` | **2.9** — pvAccess certainly permits many |
| Linear trigger name resolution | `server.rs:586` — `channels.values_mut().find(..)` | **2.9** — two channels naming one PV mis-resolve and stall the select loop |
| `recv()` awaited inside a select arm | `server.rs:501` region | under monitor load, blocks servicing inbound messages |
| Disagreeing `Char → String` NUL handling | `dbr.rs:445` (`take_while(!= 0)`) vs `dbr.rs:280` (keeps trailing zeros), plus `minimum_length` ratcheting up and never down at `intercom.rs:172` | **0.4** touches this code |
| `test_server` omits `.search_port(0)`, so binds UDP 5064 | `utils.rs:228` | copying it as a pvAccess template |
| `test_random_bind` leaves beacons on; both `lib.rs` doctests bind 5064/5065 and broadcast live | `server.rs`, `lib.rs` | same |
| `ClientError` / `SubscriptionToken` not re-exported | `pub` in `client/client.rs`, absent from `client/mod.rs` — `Client::read_pv`'s error type is unnameable downstream | any downstream user |
| `lib.rs:76` doctest omits `.await.unwrap()`; its `# break` precedes the `sleep` and is unreachable | `lib.rs` — README's version is correct, the two have drifted | next docs pass |
| `SearcherBuilder::search_port` stored and never used | `searcher.rs:64` builds from `get_default_server_port()` instead | next client work |
| `utils::test` is `#[cfg(test)]`, forcing `tests/server_tests.rs` to duplicate `connected_client_server` | `utils.rs:124` | writing pvAccess integration tests |

Several of the `dbr.rs` panics are **remotely reachable**: a client requesting
`DBR_GR_ENUM` or `DBR_CTRL_ENUM` panics the circuit via
`DbrGraphics::default_for(Enum)`; a `caput` of a string onto an enum-native PV panics
via `parse_into`; a `DBR_ENUM` with `data_count > 1` trips the assert; and a 40-byte
string chunk with no NUL panics at `dbr.rs:362`.

## References

- [pvAccess Protocol Specification](https://docs.epics-controls.org/en/latest/pv-access/protocol.html)
- [Protocol messages specification](https://docs.epics-controls.org/en/latest/pv-access/Protocol-Messages.html)
- [Data Encoding](https://docs.epics-controls.org/en/latest/pv-access/Protocol-Encoding.html)
- [EPICS V4 Normative Types](https://docs.epics-controls.org/en/latest/pv-access/Normative-Types-Specification.html)
- [`pvaccess-feasibility.md`](pvaccess-feasibility.md) — the investigation this plan
  derives from, and the rationale for every decision cited above as "§n"
- `server_protocol_compliance.md` / `client_protocol_compliance.md` — the
  compliance-table format that Phase 2's acceptance criteria should eventually be
  restated in, once there is an implementation to audit
