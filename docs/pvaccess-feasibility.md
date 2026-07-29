# Adding pvAccess to epicars — feasibility study

**Status:** investigation only. Nothing here is implemented, and no decision has
been taken to implement it.

**Scope:** a standalone pvAccess implementation, consistent with the crate's
existing "no dependency on epics-base" position. No external pvAccess library is
proposed.

Code references below are against `c3e6c12` and will drift; treat line numbers as
signposts, not addresses.

## Contents

1. [Summary](#1-summary)
2. [Layer-by-layer assessment](#2-layer-by-layer-assessment)
3. [The DBR type system vs pvData](#3-the-dbr-type-system-vs-pvdata)
4. [Serving both protocols at once](#4-serving-both-protocols-at-once)
5. [Proposed sequencing](#5-proposed-sequencing)
6. [Risks](#6-risks)
7. [Pre-existing defects on the path](#7-pre-existing-defects-on-the-path)
8. [References](#8-references)

## 1. Summary

The **layering** — messages / dbr / `Provider` / `Server` — survives a second
protocol. The **currency** flowing through that layering does not: `Dbr`,
`DbrType`, `MonitorMask`, `ErrorCondition` and `messages::Access` are Channel
Access *wire* types appearing in what is meant to be the application-facing
boundary.

The underlying problem is that `dbr.rs` does two jobs at once. It is CA's wire
format *and* it is the crate's data model. pvAccess forces those apart, and that
split is worth doing on its own merits regardless of whether pvAccess ships.

Serving both at once is mechanically straightforward — different ports, different
environment variables, no shared transport state, and `Provider: Clone` already
permits two servers over one provider. The hard part is not concurrency; it is
that a `Provider` today can only *speak* CA.

This would be greenfield. There is no scaffolding, no feature flag, no trait seam,
no branch and no commit in history touching pvAccess. The single mention anywhere
is in `README.md`, arguing for *less* CA investment rather than for a PVA plan.

## 2. Layer-by-layer assessment

| File | Lines | Reusable for PVA? |
|---|---|---|
| `src/messages.rs` (one file, not a directory) | 2381 | ~10% — the macro, the error shape, the decoder *pattern* |
| `src/dbr.rs` | 1367 | Concepts map well, representation does not |
| `src/server.rs` | 1038 | Structure ~60–70%, code ~0% |
| `src/client/client.rs` + `searcher.rs` | 1357 | `searcher.rs` and `utils.rs` are already DBR-free |
| `src/providers/intercom.rs` | 587 | ~70% already protocol-neutral |
| `src/utils.rs` | 289 | Interface enumeration directly reusable |
| `src/providers/mod.rs` | **80** | Contract shape yes, signatures no |
| | **7890 total** | |

The whole abstraction boundary in question is **80 lines**, with exactly **two**
implementations in the tree: `IntercomProvider`, and a two-method `BlankProvider`
(`server.rs:954`) used by a single test. The trait has never been stress-tested by
a second serious implementor.

### 2.1 `messages.rs` — no code reuse, one clean seam, one nasty surprise

`RawMessage` (`messages.rs:69`) is the CA 16-byte header, with the 32-byte extended
form when `payload_size == 0xFFFF`:

```rust
pub struct RawMessage {
    pub command: u16,
    field_1_data_type: u16,
    field_2_data_count: u32,
    field_3_parameter_1: u32,
    field_4_parameter_2: u32,
    payload: Vec<u8>,
}
```

pvAccess's header is unrelated — 8 bytes, always:

```
byte magic       = 0xCA
byte version
byte flags       // bit0 app/control, bits4-5 segmentation, bit6 direction, bit7 endianness
byte command
int  payloadSize
```

`CAMessage` is welded to CA by its supertrait — `pub trait CAMessage: TryFrom<RawMessage>`
(`messages.rs:47`) — so a pvAccess message cannot implement it. Because
`RawMessage`'s four generic fields are crate-private, all 24 message impls must
live in that one file, so lifting `CAMessage` into a generic
`WireMessage { type Raw; .. }` would touch everything. Not worth it. Write a
sibling `src/pva/messages.rs` instead.

**The good news is a genuinely clean seam.** `messages.rs` only ever carries
`Vec<u8>` payloads plus a `DbrType` in the header slot; all value serialisation
lives in `dbr.rs` (`to_bytes` / `write_be` / `from_bytes` / `decode_value`). The
sole exception is `EventAdd::respond` (`messages.rs:1521`). **A pvAccess codec
therefore never needs to touch `messages.rs` at all.**

What does transfer: the `impl_from_for!` macro (`messages.rs:535`, fully generic),
the `MessageError` and nom `ParseError` glue (`messages.rs:737-789`), the
decoder-is-also-the-item trick, and the
peek → header-len → payload-len → reserve → advance pattern.

Three things in PVA have no CA analogue and carry the real risk:

1. **Runtime endianness.** CA is fixed big-endian, and `messages.rs` uses `be_u16`
   / `be_u32` throughout (already with `::<&[u8], nom::error::Error<&[u8]>>`
   turbofish noise in about eight places). PVA negotiates byte order per
   connection via the `SET_BYTE_ORDER` control message and flags bit 7. Copy the
   existing style naively and you write every parser twice. Decide up front:
   generic const parameter, runtime flag threaded through, or two generated parser
   sets.
2. **Segmentation.** PVA messages align to 64-bit boundaries and may split across
   frames (flags bits 4–5 = not / first / last / middle), preserving alignment
   padding between segments. The decoder must reassemble before yielding a logical
   message.
3. **In-band control messages.** `SET_BYTE_ORDER`, `ECHO_REQUEST` /
   `ECHO_RESPONSE` and the flow-control marks interleave with application messages
   and must be handled below the message enum.

One CA wart *disappears* in PVA. The `Message` / `ClientMessage` split exists only
because CA overloads command IDs by direction — command 1 is both
`EventAddResponse` and `EventCancelResponse`, disambiguated by a payload-size
heuristic duplicated at `messages.rs:454` and `:630`. PVA puts direction in a
header flag bit, so a PVA layer needs one enum, not two.

### 2.2 `providers/mod.rs` — right contract, wrong currency

Five of six methods carry CA wire types. Only `provides` is neutral:

```rust
pub trait Provider: Sync + Send + Clone + Default + 'static {
    fn provides(&self, pv_name: &str) -> bool;                                  // neutral
    fn read_value(&self, pv_name: &str, requested_type: Option<DbrType>)
        -> Result<Dbr, ErrorCondition>;                                          // CA
    fn get_access_right(&self, ..) -> messages::Access;                          // CA in name only
    fn write_value(&mut self, pv_name: &str, value: Dbr)
        -> Result<(), ErrorCondition>;                                           // CA
    fn monitor_value(&mut self, pv_name: &str, unique_subscriber_id: u64,
        data_type: DbrType, data_count: usize, mask: MonitorMask,
        trigger: mpsc::Sender<String>)
        -> Result<broadcast::Receiver<Dbr>, ErrorCondition>;                     // CA
    fn cancel_monitor_value(&mut self, pv_name: &str, unique_subscriber_id: u64,
        data_type: DbrType, data_count: usize);                                  // CA
}
```

Severity of each leak, worst first:

- **`Dbr` / `DbrType` / `ErrorCondition` — high.** The CA payload-category enum,
  the CA type-code arithmetic, and the roughly sixty `ECA_*` numeric codes.
- **`data_count` element-count semantics — medium.** CA's "request N elements".
- **`DBR_CLASS_NAME` smuggled through `read_value` — medium.** `intercom.rs:132`
  overloads `requested_type == Some(DBR_CLASS_NAME)` to mean "give me the EPICS
  record type name" rather than a value. A CA-only RPC riding the value channel.
- **`MonitorMask` — medium on paper, free in practice.** It is threaded through and
  never read: the server stores it in `PVSubscription.mask` (`server.rs:442`) and
  never consults it, and `IntercomProvider` binds it as `_mask` (`intercom.rs:511`).
  Dead weight today, so it can be changed at no cost.
- **`messages::Access` — low.** `{None, Read, Write, ReadWrite}` is semantically
  protocol-neutral; it just lives in the CA module. A move is enough.

**The trait is not object-safe** (`Clone + Default + Sized`), so there is no
`Box<dyn Provider>`. Everything is monomorphised — `Server<L>`, `Circuit<L>`,
`ServerBuilder<L>`. Two servers over the same `L` is fine; a heterogeneous list of
providers is not. Worth knowing before designing per-protocol composition.

**The two-channel pull model is an asset.** The provider sends a PV *name* down an
`mpsc::Sender<String>` trigger; the circuit wakes and only then reads a
`broadcast::Receiver<Dbr>` and formats per subscription. PVA monitors have
pipelined flow control — the client grants a window via `nfree`, and the server
sends only while the counter is positive — so a model where the *transport* decides
when to consume suits PVA better than a push model would. Keep it.

`store()` also stays callable from non-async code because the provider uses
`try_send` on triggers and tolerates a full queue (`intercom.rs:183-192`); the
value is still in the broadcast buffer. That property must be preserved.

One gap: the trigger carries only a name, so the PVA layer cannot know *which
fields* changed and must send an all-ones BitSet on every monitor update. Legal,
not optimal, and acceptable for a first version.

### 2.3 `unique_subscriber_id` — a concrete collision, with a concrete fix

It is not generated by any registry. The server computes it at both call sites
(`server.rs:643`, `:676`) as a packed pair:

```rust
(self.id << 32) | channel.server_id as u64
```

`self.id` is a circuit counter local to the accept loop (`server.rs:373`),
restarting at 0 for each `Server`. The provider uses the result purely as an opaque
key — `pv.triggers.insert(id, trigger)` (`intercom.rs:520`) into a
`HashMap<u64, mpsc::Sender<String>>`.

**Two servers sharing one provider will therefore collide.** CA circuit 0 /
channel 0 and PVA circuit 0 / channel 0 produce the same key, and one silently
overwrites the other's trigger sender. Fix by partitioning the space with a
protocol tag, or better, by having the provider allocate and return the ID.

Related pre-existing limits that a second protocol will bump into:

- **One subscription per channel.** `Channel.subscription` is
  `Option<PVSubscription>` (`server.rs:434`), even though CA permits many
  `EventAdd`s with distinct subscription IDs — and PVA certainly does.
- **Trigger resolution is a linear name search.** `handle_monitor_update` does
  `self.channels.values_mut().find(|v| v.name == pv_name)` (`server.rs:586`), so
  two channels on one circuit naming the same PV mis-resolve, and the second
  `.await`s the first's receiver, stalling the circuit's select loop.
- `recv()` is awaited *inside* a select arm, so a monitor pull blocks servicing
  inbound client messages.

### 2.4 `providers/intercom.rs` — already half-decoupled

This is the encouraging find. `PV` (`intercom.rs:100`) keeps a neutral core and
names its CA adapters explicitly:

```rust
struct PV {
    name: String,
    value: Arc<Mutex<DbrValue>>,                   // neutral-ish storage
    minimum_length: Option<usize>,
    timestamp: SystemTime,                         // neutral
    sender: broadcast::Sender<Dbr>,                // <-- the leak
    triggers: HashMap<u64, mpsc::Sender<String>>,  // neutral
    epics_record_type: Option<String>,
    read_only: bool,                               // neutral
}
```

`load_for_ca(&self, requested_type: Option<DbrType>) -> Dbr` (`intercom.rs:130`)
and `store_from_ca(&mut self, value: &DbrValue)` (`intercom.rs:155`) — the naming
shows the seam was already in mind. `load_for_pva` is the natural sibling.

Roughly 70% of the file is protocol-neutral: `Intercom<T>`, `ConverterReceiver<T>`
and its error enums, the registry, `PV::load` / `PV::store`, prefix normalisation,
and the trigger/broadcast fan-out. The CA-specific 30% is `load_for_ca` /
`store_from_ca`, the `epics_record_type` and `DefaultEpicsClass` record names, the
`_RBV` suffix convention, and the `ErrorCondition` returns.

The one real leak is `sender: broadcast::Sender<Dbr>`, a CA-typed fan-out payload.
Note also that `load_for_ca` always returns `Status::default()`, so **alarm status
is never populated by the reference provider**; NTScalar's `alarm` field would be
equally empty until that is addressed.

### 2.5 `server.rs` — structure transfers, code does not

`Server<L: Provider>` is generic over the provider and hardcodes CA everywhere
else. The provider boundary is just **nine call sites**: `provides` (`:340`),
`monitor_value` (`:641`), `cancel_monitor_value` (`:674`), `read_value` (`:788`,
`:827`), `write_value` (`:817`), `get_access_right` (`:839`), plus two `clone()`s.

Reusable *structure*: the `ServerHandle` and its `Drop`-cancels-token semantics
(`:70-106`, entirely protocol-agnostic), `CancellationToken` propagation, the
`JoinSet` task supervision with first-error-wins join, the TCP accept-loop skeleton
(`:368-410`, where only line 395 is CA), `try_bind_ports` (`:172`), the oneshot
port-report handshake, `get_broadcast_ips`, the `broadcast::Sender<ServerEvent>`
fan-out, and the `Circuit` / `Channel` / `PVSubscription` decomposition with its
three-arm `tokio::select!` over {cancel, monitor trigger, inbound message}
(`:501`).

The strongest genericisation candidate is `listen_for_searches` (`:281-366`) — PVA
search is also UDP with duplicate suppression, so it is the same plumbing behind a
different codec.

`Server::listen` (`:191-227`) already spawns three duties into one `JoinSet` and
reports ports through a single `oneshot::Sender<(u16, u16)>`. Adding PVA means a
fourth and fifth duty, a widened port tuple, and a second `Circuit` equivalent
sharing the same `L` clone and the same `ServerEvent` sender.

`ServerEvent` (`:110-158`) has CA-flavoured vocabulary — circuit/channel, with
`channel_id: u32` being the CA SID — but every concept has a PVA analogue. It has
**no protocol discriminator field**; one should be added.

### 2.6 Ports and environment — no collisions

| | CA | PVA |
|---|---|---|
| TCP | 5064 | 5075 |
| UDP search | 5064 | 5076 |
| Beacon / repeater | 5065 | 5076 (plus multicast 224.0.0.128:5076) |
| Env prefix | `EPICS_CA_*` | `EPICS_PVA_*` |

`utils.rs` reads `EPICS_CA_SERVER_PORT`, `EPICS_CA_REPEATER_PORT`,
`EPICS_CA_ADDR_LIST`, `EPICS_CA_AUTO_ADDR_LIST`, `EPICS_CA_CONN_TMO`,
`EPICS_CA_BEACON_PERIOD` and `EPICS_CA_MAX_SEARCH_PERIOD`. Add `EPICS_PVA_*`
siblings. `get_target_broadcast_ips` (`utils.rs:50`) enumerates interfaces via
`pnet` and is directly reusable once parameterised by which env var it reads;
`new_reusable_udp_socket` (`utils.rs:12`) likewise.

**No new dependencies are required.** `nom`, `tokio`, `tokio-util`, `socket2` and
`pnet` cover everything, and the 12-byte server GUID needs no `uuid` crate. There
are currently **no Cargo features at all** — add `default = ["ca"]` and `pva = []`
so CA-only users do not pay for it.

## 3. The DBR type system vs pvData

### 3.1 The CA type space is closed by construction

This is the sharpest way to state the problem. `DbrType` ↔ wire code is pure
arithmetic (`dbr.rs:842-849`):

```rust
impl From<DbrType> for u16 {
    fn from(value: DbrType) -> Self {
        match value {
            DBR_CLASS_NAME => 38,
            value => value.category as u16 * 7 + value.basic_type as u16,
        }
    }
}
```

A fixed 5×7 cross-product plus one special case. There is no registry, no
interning, no type-ID negotiation — the "introspection" *is* a `u16` identity.
That is the exact opposite of pvAccess's `FieldDesc` with a per-connection
introspection cache, and it means **nothing in the current type code can be
extended to carry a user-defined structure.** It is not a matter of adding
variants.

### 3.2 The representational gap

```rust
pub enum DbrValue {           // dbr.rs:104
    Enum(u16),
    String(Vec<String>),
    Char(Vec<i8>),
    Int(Vec<i16>),
    Long(Vec<i32>),
    Float(Vec<f32>),
    Double(Vec<f64>),
}
```

Six scalar types plus a scalar enum index, all flat arrays, all signed. pvData has
`boolean`; `byte`/`ubyte`, `short`/`ushort`, `int`/`uint`, `long`/`ulong`; `float`
and `double`; `string`; `structure` (named, nested, arbitrarily deep, carrying a
type ID such as `epics:nt/NTScalar:1.0`); `union` and `variant union`; and arrays
of all of those including structure arrays, plus bounded and fixed-size array
forms.

`DbrValue` cannot represent unsigned anything, 64-bit integers, booleans, nested
structures, unions, or structure arrays. A PVA server built on it could not serve
NTNDArray, NTTable, or a proper NTEnum — which is most of the reason to want
pvAccess.

### 3.3 But the metadata concepts line up almost exactly

`Dbr::Time { status, timestamp, value }` — what `PV::load_for_ca` already returns —
maps essentially 1:1 onto NTScalar:

```
structure "epics:nt/NTScalar:1.0"
    <scalar>  value                                                      <- DbrValue
    alarm_t   alarm     { int severity, int status, string message }     <- Status
    time_t    timeStamp { long secondsPastEpoch, int nanoseconds, int userTag }  <- SystemTime
    display_t display   { limitLow, limitHigh, description, units, precision, form }
    control_t control   { limitLow, limitHigh, minStep }
```

`display_t` and `control_t` are precisely the `Dbr::Graphics` and `Dbr::Control`
categories that are currently stubs. And `DbrCategory` (Basic / Status / Time /
Graphics / Control) is in effect "which subset of the NT fields do you want" —
which PVA expresses through the pvRequest field mask. Same idea, different
mechanism.

`DbrValue::Enum(u16)` maps to NTEnum's `enum_t { int index; string[] choices }`.
PVA *forces* the choices list, which CA also needs for `DBR_GR_ENUM` and currently
lacks entirely: `DbrGraphics::Enum` is a **unit variant with no fields**
(`dbr.rs:614`), so the `[[u8;26];16]` choice table has no home, and
`MAX_ENUM_STRING_SIZE` / `MAX_ENUM_STATES` (`dbr.rs:79-80`) are declared and never
referenced.

**Implementing pvAccess properly supplies both the motivation and the shape for
closing two of the three known CA gaps in the README — ENUM, and CTRL/GR.**

The structural change that follows: `Dbr`'s five-way category enum should become
composable optional fields — `{ value, Option<Alarm>, Option<TimeStamp>,
Option<Display>, Option<Control> }` — which is nearly what `Dbr::Control` already
spells out inline.

### 3.4 Three options, with a recommendation

**Option A — extend `DbrValue` to the union of both type systems.** Add `Bool`,
`UByte`, `UShort`, `UInt`, `Long64`, `ULong64`, `Structure` and `Union`. Every
match arm in `dbr.rs` must then handle them — `get_count`, `get_type`,
`convert_to`, `parse_into`, `to_bytes`, `resize`, `get_default_record_type`, plus
the macro-generated `From`/`TryFrom` impls (`dbr.rs:421-553`) — and the CA
serialiser must reject what CA cannot express. Invasive, and it deepens the
conflation that is the underlying problem. **Not recommended.**

**Option B — a neutral value type with adapters both ways. Recommended.** A
properly recursive pvData model, with introspection as a *separate* type, since PVA
transmits FieldDesc separately from data and caches it per connection:

```rust
pub enum Value {
    Scalar(Scalar),
    ScalarArray(ScalarArray),
    Structure(Structure),                  // ordered named fields + optional type id
    StructureArray(Vec<Option<Structure>>),
    Union(Box<UnionValue>),
    VariantUnion(Option<Box<Value>>),
}
```

`From<&DbrValue> for Value` is total, since `DbrValue` is a strict subset;
`TryFrom<&Value> for DbrValue` is partial, failing on structures, unions and
out-of-range unsigned values. CA keeps `Dbr`, PVA gets `Value`, and the provider
core stores `Value`.

The existing `T: TryFrom<DbrValue>, DbrValue: From<T>` bound on `Intercom<T>` is
the right *shape* to generalise — a `From`/`TryFrom` pair — but it is backed by
three macros and about two dozen hand-written impls over eleven types, with no
derive macro and no struct support. Normative Types will need a derive.

**Option C — make `Value` the single internal model and demote DBR to a pure
codec.** The clean end state, in which `dbr.rs` becomes only `Value → CA bytes` and
`CA bytes → Value`. Most churn. Converge here from B rather than attempting it
directly.

## 4. Serving both protocols at once

The target shape, which the existing `Provider: Clone` bound already permits:

```rust
let mut provider = IntercomProvider::new();
let value = provider.add_pv("NUMERIC_VALUE", 42i32)?;
let _ca  = ServerBuilder::new(provider.clone()).start().await?;
let _pva = PvaServerBuilder::new(provider).start().await?;
```

`IntercomProvider`'s `Arc<Mutex<HashMap<String, Arc<Mutex<PV>>>>>` means both
servers share state through the clone. This works today at the bound level. The
work items are the `unique_subscriber_id` partition (§2.3), a protocol tag on
`ServerEvent`, a widened port-report tuple, and the value-currency question (§3).

The `Default` bound on `Provider` is an odd requirement — it exists only so
`Server<L>` can hand-implement `Default` (`server.rs:54`) — and is worth revisiting
while the trait is being touched.

## 5. Proposed sequencing

**Phase 0 — decouple, no pvAccess yet. Roughly 1–2 weeks.** Split `dbr.rs` into
wire codec and data model; introduce `Value`; move `PV`'s storage and broadcast
payload to `Value`; decompose `Dbr`'s category enum into optional metadata fields.
Leave `Provider` exactly as it is, implemented over `Value` internally. Zero
behaviour change, covered by the existing suite. This is the de-risking step and it
pays for itself regardless of what follows.

**Phase 1 — PVA wire layer. Roughly 3–4 weeks.** A new `src/pva/`: 8-byte header
codec with runtime endianness, segmentation reassembly, control messages, the size
/ string / BitSet / Status encodings, FieldDesc introspection encode and decode,
and the per-connection introspection registry — **two** registries per connection,
since the link is full-duplex and each direction caches independently. Mostly
mechanical, well specified, and unit-testable with no network. The bulk of the
novel code, and it need not touch `messages.rs` at all (§2.1).

**Phase 2 — PVA server. Roughly 2–3 weeks.** `PvaServer<L>` mirroring `Server<L>`:
UDP search on 5076, TCP accept on 5075, beacons carrying a server GUID,
`SET_BYTE_ORDER` then the `CONNECTION_VALIDATION` handshake, channel create and
destroy, then GET / PUT / MONITOR with their INIT / exec / DESTROY subcommand
phases, plus GET_FIELD. Defer RPC, PUT_GET, ARRAY, PROCESS and AUTHNZ — `pvget`,
`pvput` and `pvmonitor` do not need them.

**Phase 3 — the `Provider` decision.** Only at this point is the requirement
actually known. Ship a **blanket adaptation first**: any existing `Provider` becomes
automatically servable over PVA as NTScalar / NTScalarArray, with no user code
change. That is the highest-leverage move available. Add an opt-in richer trait
afterwards for providers that want to expose real structures.

**Phase 4 — PVA client. Roughly 2–3 weeks. Optional.** Note the client has **zero**
trait abstraction today: `Client` is a concrete struct with six inherent methods,
typed end to end on `Dbr` / `DbrValue`, hardcoding `DbrCategory::Time`. A unified
two-protocol client means introducing that seam from scratch, whether as a `Client`
trait or enum dispatch. The actor pattern itself — `CircuitRequest` variants each
carrying a `oneshot::Sender`, with handlers returning `Vec<Message>` for the loop to
write — is a good model to copy. `examples/linear-caget.rs` is the best existing
step-by-step reference for what a protocol client must do.

**Total: roughly 8–12 weeks of focused work** for a server-side pvAccess serving
`pvget` / `pvput` / `pvmonitor` over NTScalar, NTScalarArray and NTEnum. For scale:
the whole crate today is about 7 900 lines, and a comparable-maturity PVA server is
likely 3 000–4 500 new lines, of which around 1 500 is the encoding layer. The
type-system work is 800–1 200 lines and is the only part that disturbs existing
code.

## 6. Risks

In order of how much they tend to be underestimated.

1. **The introspection registry.** Stateful, per-connection, bidirectional, with
   IDs overridable mid-connection. The likeliest source of "works against my own
   client, breaks against pvxs" bugs. Test against real `pvget` and pvxs from the
   first week of Phase 1.
2. **pvRequest.** The specification explicitly leaves its structure unspecified for
   a future revision, but real clients send `field(value)`,
   `field(value,alarm,timeStamp)` and `record[queueSize=N]`. You must implement the
   de-facto behaviour, not the spec. This is where interop pain concentrates.
3. **Endianness.** See §2.1 — cheap to get right at the start, expensive to
   retrofit.
4. **Segmentation and 64-bit alignment.** Easy to get subtly wrong, and there is no
   CA analogue to learn from.
5. **Test isolation is already leaky.** `tests/server_tests.rs` gets it right with
   `.connection_port(0).search_port(0).beacons(false)`, but
   `utils::test::test_server` omits `.search_port(0)` and so binds UDP 5064,
   `server.rs::test_random_bind` leaves beacons on, and both `lib.rs` doctests bind
   5064/5065 and broadcast live. A PVA server inherits this pattern unless it is
   fixed first, and CI runs plain `cargo test`.

## 7. Pre-existing defects on the path

Not blockers, but they sit directly on this path and a refactor is the natural time
to deal with them.

- **Sixteen `todo!()`s in `dbr.rs`** (lines 168, 643–644, 671–672, 732–733,
  743–744, 1039, 1042, 1081–1082, 1213, 1215), plus an `assert!` at 353 and
  `.unwrap()`s at 362, 363, 1059 and 1109. Several are remotely reachable: a client
  requesting `DBR_GR_ENUM` or `DBR_CTRL_ENUM` panics the circuit via
  `DbrGraphics::default_for(Enum)`; a `caput` of a string onto an enum-native PV
  panics via `parse_into`; a `DBR_ENUM` with `data_count > 1` trips the assert; and
  a 40-byte string chunk with no NUL panics at `dbr.rs:362`.
- `server.rs:788` `read_value(..).unwrap()` panics the circuit if a PV disappears
  between `CreateChannel` and `ReadNotify`. `server.rs:619`
  `.convert_to(..).unwrap()` does likewise on an unsupported subscription
  conversion.
- **Two `Char → String` paths disagree about NUL handling.** `TryFrom<DbrValue> for
  String` (`dbr.rs:445`) does `take_while(|c| *c != 0)`, while
  `convert_to(String)` (`dbr.rs:280`) keeps trailing zeros. Combined with
  `minimum_length` ratcheting up and never down (`intercom.rs:172`), that is the
  likely source of the rogue-NUL note in `test_read_written_strings`.
- `ClientError` and `SubscriptionToken` are `pub` in `client/client.rs` but not
  re-exported from `client/mod.rs`, so `Client::read_pv`'s error type is not
  nameable downstream.
- The `lib.rs` server doctest (`:76`) omits `.await.unwrap()`, so the server future
  is never polled, and its `# break` precedes the `sleep`, making that line
  unreachable. The README version is correct; the two have drifted.
- `SearcherBuilder::search_port` is stored and never used — `searcher.rs:64` builds
  from `get_default_server_port()` instead.
- `utils::test` is `#[cfg(test)]`, so `tests/server_tests.rs` has to duplicate
  `connected_client_server`.

## 8. References

- [pvAccess Protocol Specification](https://docs.epics-controls.org/en/latest/pv-access/protocol.html)
- [Protocol messages specification](https://docs.epics-controls.org/en/latest/pv-access/Protocol-Messages.html)
- [Data Encoding](https://docs.epics-controls.org/en/latest/pv-access/Protocol-Encoding.html)
- [EPICS V4 Normative Types](https://docs.epics-controls.org/en/latest/pv-access/Normative-Types-Specification.html)
