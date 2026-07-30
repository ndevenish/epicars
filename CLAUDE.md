# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`epicars` — a pure-Rust implementation of the [EPICS Channel Access protocol](https://docs.epics-controls.org/en/latest/internal/ca_protocol.html), with no dependency on the C `epics-base`. Rust 2024 edition, tokio-only async. The server side is the mature part; the client is newer. See `README.md` for the list of known gaps (ENUM types, `CTRL`/`GR` DBR categories, repeater support).

The library is pre-1.0 and explicitly reserves the right to break interfaces between versions.

## Commands

```bash
cargo test --lib --tests   # unit + integration tests (fast, ~1s)
cargo test --doc           # doctests in src/lib.rs — these bind real sockets
cargo test                 # everything, incl. compiling examples/
cargo test test_events     # single test by name
cargo clippy --lib --tests
cargo fmt
cargo run --example simple-intercom -- -v   # server exposing test PVs
cargo run --example caget -- SOME_PV        # pure-rust caget
```

Pre-commit hooks run `fmt`, `clippy`, and `cargo check`. CI (`.github/workflows/rust.yml`) runs `cargo build` and `cargo test` on push/PR to `main`.

Doctests and any test that starts a default-configured server will try to bind the real CA ports (5064/5065) and broadcast beacons on the local network. Tests that shouldn't do this pass `.connection_port(0).search_port(0).beacons(false)` — see `connected_client_server` in `tests/server_tests.rs`.

## Architecture

Four layers, deliberately separated so each can be used without the ones above it:

1. **`messages`** (~2400 lines) — wire format. Every CA message is a struct implementing `CAMessage` plus `TryFrom<RawMessage>`. `RawMessage` handles header parsing (including the 32-byte extended header when `payload_size == 0xFFFF`) and 8-byte payload padding; parsing is `nom`-based, and `RawMessageDecoder`/`ClientMessage` implement tokio-util `Decoder` for framed streams.

   Crucially there are **two** top-level enums: `Message` (anything) and `ClientMessage` (only what a server can send to a client). They exist because some command IDs are overloaded — e.g. command `1` is both `EventAddResponse` and `EventCancelResponse`, disambiguated only by a heuristic on payload size and data count. Parse with `Message::read_server_message` on the server side, `ClientMessage`/`Message::read_client_message` on the client side.

2. **`dbr`** (~1370 lines) — the data interchange representation. CA's 35 DBR kinds decompose into a basic type (`DbrBasicType`, held in `DbrValue` as `Vec<i8|i16|i32|f32|f64>`, `u16` enum index, or `Vec<String>`) crossed with a metadata category (`DbrCategory` → the `Dbr` enum: `Basic`/`Status`/`Time`/`Graphics`/`Control`). `DbrType` pairs the two and converts to/from the protocol's `u16`. `DbrValue::convert_to` does the lossless-only numeric coercion that lets a client `caget` an `i8` as an `i32`; `parse_into` handles the string→numeric path that `caput` needs. `Graphics` and `Control` are stubs.

3. **`Provider` trait** (`src/providers/mod.rs`) — the boundary between the server and application data. `provides`/`read_value`/`write_value`/`get_access_right`/`monitor_value`/`cancel_monitor_value`. Bound is `Sync + Send + Clone + Default + 'static`: each circuit gets its own clone, so implementations share state internally (`Arc<Mutex<...>>`).

   Subscriptions use a **two-channel pull model**, not a push of values: the server hands `monitor_value` an `mpsc::Sender<String>` "trigger" and receives back a `broadcast::Receiver<Dbr>`. When the value changes the provider sends the *PV name* down the trigger, the circuit task wakes, and only then reads from the broadcast receiver and formats a response per subscription's requested type/count. `unique_subscriber_id` identifies the (circuit, channel, subscription) tuple for cancellation.

4. **`Server`** (`src/server.rs`) — `ServerBuilder` → `ServerHandle`. Three concurrent duties: a UDP search listener, a TCP accept loop, and (optionally) a beacon broadcaster. Each accepted connection becomes a `Circuit` task running a `tokio::select!` loop over {cancellation, monitor triggers, incoming messages}; a `Circuit` owns `Channel`s, each optionally holding a `PVSubscription`. Shutdown is a `CancellationToken` — `ServerHandle::drop` cancels it, so a dropped handle stops the server. `ServerHandle::listen_to_events` exposes a `broadcast` of `ServerEvent` lifecycle notifications (circuit open/close, channel create/clear, read/write/subscribe/unsubscribe).

**`Client`** (`src/client/`) mirrors this with an actor pattern: `Client` owns a `Searcher` (UDP name resolution, retry with backoff, `EPICS_CA_ADDR_LIST` targets) plus a `HashMap<SocketAddr, Circuit>`. Each `Circuit` is a handle to a spawned `CircuitInternal` task; public methods send a `CircuitRequest` variant carrying a `oneshot::Sender` for the reply. Both `Client` and `Searcher` cancel their tokens on drop.

**`IntercomProvider`** (`src/providers/intercom.rs`) is the reference `Provider`. `add_pv`/`build_pv` return an `Intercom<T>` — a typed, cloneable, `Send` handle over an `Arc<Mutex<DbrValue>>` with synchronous `load()`/`store()`, usable from non-async code while the server runs on tokio. `PVBuilder` sets `read_only`, `class_name`, `rbv` (auto readback), and `minimum_length` (pad short arrays for subscribers).

**`utils`** holds the `EPICS_CA_*` environment-variable defaults (`EPICS_CA_SERVER_PORT`, `EPICS_CA_REPEATER_PORT`, `EPICS_CA_ADDR_LIST`, `EPICS_CA_AUTO_ADDR_LIST`, `EPICS_CA_CONN_TMO`, …). Read ports through these helpers rather than hardcoding. `utils::test` (cfg(test) only) provides `bare_test_client` — a raw framed CA client for driving the server at the message level — and `test_server`.

## Conventions

- Adding a message type means: struct + `TryFrom<RawMessage>` + `impl CAMessage` + a variant in `Message` (and `ClientMessage` if a server can send it) + the command-ID arm in each `TryFrom`/`from_raw_client_message` match + registering in the `impl_from_for!` list.
- `Dbr::Time` is what providers normally return; the server converts to whatever category/type the client asked for.
- Strings: CA natively uses `[u8; 40]`, but in practice this facility's PVs carry strings as `CHAR` arrays. `DbrValue::String` is `Vec<String>` and is converted at the wire boundary; there are known rough edges here (see the `test_read_written_strings` comment).
- Tracing, not `println!`. Tests init `tracing_subscriber` with `TestWriter`; beacons are deliberately `trace!` level to avoid drowning logs.

## pvAccess (in progress)

`docs/pvaccess-implementation-plan.md` is the authority — numbered work items,
each with acceptance criteria. `docs/pvaccess-feasibility.md` is the rationale it
cites. Work one item at a time and commit per item. New code lives in `src/pva/`,
gated on the `pva` Cargo feature; CA is `default`.

**The conventions above describe the CA side. pvAccess deliberately departs from
several of them.** Do not "fix" these back — each is a recorded decision with the
reasoning in the plan item named:

- **`src/pva/` does not use nom** (plan 1.2). pvAccess negotiates byte order per
  connection, so parsers are written against a `PvaReader`/`PvaWriter` carrying a
  runtime `ByteOrder`, via a `PvaDecode`/`PvaEncode` pair. nom's `be_*`/`le_*` split
  would mean writing every parser twice. **CA keeps nom — do not migrate it**, and
  do not apply the `CAMessage`/`TryFrom<RawMessage>` recipe above to PVA messages;
  they are a separate, single enum (direction is a header flag bit, so there is no
  `Message`/`ClientMessage` split).
- **The two-channel pull model stays a pull model** (plan 2.6). It looks like an
  indirection worth removing; it is not. pvAccess monitors have pipelined flow
  control, so the transport must decide when to consume.
- **`store()`'s `try_send` tolerating `TrySendError::Full` is load-bearing**, not a
  dropped-update bug (plan 0.4, `intercom.rs:183-192`). The value is still in the
  broadcast buffer. Do not make it `.await` — `store()` must stay callable from
  non-async code.
- **`Dbr` keeps its variants through Phase 0** (plan 0.3). It becomes a projection
  of `(Value, Meta)`; decomposing the category enum changes the CA wire path and
  cannot be behaviour-neutral. Phase 3 revisits it.
- **New tests bind ephemeral ports and disable beacons.** The existing suite is
  *not* a template — several tests bind real 5064/5065 and broadcast live (see the
  plan's appendix). CI runs plain `cargo test`.

Interop is the acceptance criterion for anything on the wire: test against real
`pvget`/`pvput`/`pvmonitor`/`pvinfo` from pvxs or epics-base, not against
self-written clients. Start this during Phase 1, not at the end of Phase 2 — the
per-connection introspection registry is the likeliest source of "works against my
own client, breaks against pvxs" bugs.
