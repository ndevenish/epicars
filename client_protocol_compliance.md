# CA Protocol Compliance: Client Implementation

Comparison of `src/client/` against the [CA Protocol Specification](ca_protocol.rst).

## Correctly Implemented

| Feature | Spec Requirement | Status |
|---|---|---|
| **CA_PROTO_VERSION exchange** | Client MUST send version as first message on TCP | Implemented — sends version, reads and validates server version (`client.rs:100-101`) |
| **CA_PROTO_CLIENT_NAME / HOST_NAME** | Client SHOULD send after version exchange, before first channel creation | Implemented — sent immediately after version exchange (`client.rs:104-111`) |
| **CA_PROTO_SEARCH (UDP)** | Searches sent as UDP datagrams; datagrams MUST begin with CA_PROTO_VERSION | Implemented — version message prepended to all search packets (`searcher.rs:322, 451`) |
| **Search exponential backoff** | Clients RECOMMENDED to implement exponentially increasing interval for re-sending searches | Implemented — backoff doubles per attempt, capped at 2^9 × 32ms ≈ 16s (`searcher.rs:256-257`) |
| **Search batching** | RECOMMENDED to include as many search requests as possible per datagram | Implemented — multiple searches batched into single packets (`searcher.rs:349-350`) |
| **Circuit reuse** | Client SHOULD reuse existing circuit to same server | Implemented — `ClientInternal` checks for existing circuits before opening new ones (`client.rs:1234-1261`) |
| **CA_PROTO_CREATE_CHAN** | Send CID and PV name to create channel | Implemented — sends CreateChannel with client-allocated CID (`client.rs:486-511`) |
| **CA_PROTO_CREATE_CH_FAIL handling** | Handle server refusal to create channel | Implemented — notifies pending callers with error (`client.rs:708-720`) |
| **CA_PROTO_ACCESS_RIGHTS handling** | Store access rights, sent before CREATE_CHAN response | Implemented — updates channel permissions (`client.rs:681-689`); write checks permissions before sending (`client.rs:239-241`) |
| **CA_PROTO_READ_NOTIFY** | Send SID, data type, count, IOID; receive matching response | Implemented — correctly uses SID and IOID (`client.rs:554-565`) |
| **CA_PROTO_WRITE_NOTIFY** | Send write with IOID, receive response with status | Implemented (`client.rs:586-598`) |
| **CA_PROTO_WRITE** | Fire-and-forget write, no response expected | Implemented — `notify: false` path sends Write without waiting (`client.rs:599-613`) |
| **CA_PROTO_EVENT_ADD** | Subscribe with SID, data type, count, SubscriptionID, mask | Implemented (`client.rs:640-649`) |
| **CA_PROTO_EVENT_CANCEL** | Unsubscribe with matching SID and SubscriptionID | Implemented (`client.rs:657-676`) |
| **EventAdd zero-payload = cancel confirmation** | Zero payload EventAdd response means subscription terminated | Implemented — detects empty data and discards subscription (`client.rs:751-761`) |
| **CA_PROTO_ECHO** | Client MUST send echo before inactivity timer expires | Implemented — sends echo at half the timeout period (`client.rs:450-458`) |
| **Echo response handling** | Receiving echo resets inactivity timer | Implemented — `last_received_message_at` updated on any message including echo (`client.rs:678, 737`) |
| **Inactivity timeout** | Close circuit if no traffic within timeout | Implemented — if no reply after echo, connection assumed dead (`client.rs:454-458`) |
| **Beacon observation** | Clients MAY use beacons to detect new/restarted servers | Implemented — watches beacon port and records server IDs/timestamps (`client.rs:1107-1140`) |
| **IOID overflow** | Overflow of identifiers MUST be handled | Implemented — uses `wrapping_add` for IOIDs and CIDs (`client.rs:489, 548, 578, 628`) |
| **SearchID overflow** | SearchID overflow must be handled | Implemented — `wrapping_add` used for search IDs (`searcher.rs:26-30`) |
| **Subscription auto-unsubscribe** | Subscriptions should be destroyed when no longer needed | Implemented — auto-unsubscribes when all listeners have dropped (`client.rs:779-787`) |
| **Reconnection on disconnect** | Clients SHOULD re-send searches for disconnected PVs | Implemented — subscriptions are requeued for search on circuit close (`client.rs:1312-1322`) |

## Issues

### 1. No repeater registration

**Spec:** "A CA client SHOULD maintain a registration with a Repeater on the local system, (re)starting it as necessary."

**Current:** No repeater registration is implemented. The client binds directly to ephemeral UDP ports for searching and beacon listening. On systems where a CA repeater is running, the client will not receive forwarded beacons and may miss server announcements that other clients see.

---

### 2. CA_PROTO_CLEAR_CHANNEL never sent

**Spec:** When a client is done with a channel, it SHOULD send `CA_PROTO_CLEAR_CHANNEL` (command 12) with SID and CID, and wait for the server's reply before reusing the CID.

**Current:** Channels are created but never explicitly cleared. When a circuit closes (`client.rs:1297-1324`), the channels are simply dropped from internal maps. The server is never notified that individual channels are no longer needed. This leaks server-side channel resources for the lifetime of the circuit.

---

### 3. Version negotiation not enforced

**Spec:** "Valid messages and semantics of the Circuit are determined by the lower of the two minor versions." The client should use the minimum of its own and the server's version.

**Current:** `do_read_check_version` (`client.rs:150-163`) checks `is_compatible()` but the negotiated version is never stored or used to gate behavior. The server's version is discarded after the initial check. Features like dynamic array size (CA_V413) are used unconditionally (data_count=0 sent in reads/subscriptions).

---

### 4. CA_PROTO_SERVER_DISCONN not handled

**Spec:** Server sends `CA_PROTO_SERVER_DISCONN` (command 27) to notify client that a specific channel has been destroyed server-side. The client should close that channel and MAY reuse the CID immediately.

**Current:** Falls through to the catch-all `msg => { debug!("Got unhandled message from server: {msg:?}"); }` at `client.rs:806-809`. The channel would remain in the client's internal map as apparently valid, and subsequent operations on it would fail unexpectedly.

---

### 5. CA_PROTO_ERROR not handled

**Spec:** "Any client message MAY result in a CA_PROTO_ERROR reply." The error payload contains the original request header and a CID identifying which channel the error relates to.

**Current:** `CA_PROTO_ERROR` messages from the server are not matched in `handle_message` and would fall through to the catch-all debug log. Read/write operations that trigger server-side errors via this mechanism would hang waiting for a response that never comes (the error response is silently discarded).

---

### 6. CID scope is per-circuit but reuse is not safe

**Spec:** "A CID MUST be unique for a single Circuit." After a ClearChannel reply or ServerDisconn, the CID MAY be reused immediately. CIDs from one circuit MUST NOT be used on another.

**Current:** CIDs are allocated sequentially per circuit (`client.rs:488-489`), which is correct. However, since ClearChannel is never sent (#2) and ServerDisconn is not handled (#4), CIDs are effectively never freed. The wrapping add on `next_cid` means that after 2^32 channel creations on a single long-lived circuit, CIDs could collide with still-active channels.

---

### 7. Beacons observed but not acted upon

**Spec:** "CA clients MAY use a server's first beacon as a trigger to re-send previously unanswered CA_PROTO_SEARCH messages." Clients should detect new servers (new beacon source) or restarted servers (beacon ID reset).

**Current:** Beacons are received and stored in `observed_beacons` (`client.rs:1107-1140`), but this map is never read. No logic exists to detect new servers or beacon ID resets and trigger search retries. The beacon watching infrastructure is inert.

---

### 8. Subscribe always uses DbrCategory::Basic

**Spec:** Clients may request any DBR type for subscriptions. It is RECOMMENDED that clients not create dynamic monitors for plain DBR_* types and instead promote to DBR_STS_* to avoid ambiguity with zero-element replies.

**Current:** `subscribe()` and `watch()` both hardcode `DbrCategory::Basic` (`client.rs:1607, 1632`). This means:
- Users cannot request status/time/graphic metadata with subscriptions
- The spec-recommended promotion to DBR_STS_* to disambiguate zero-count replies is not done, creating a potential parsing ambiguity

---

### 9. Subscribe always uses MonitorMask::VALUE

**Spec:** The monitor mask defines which events trigger updates (value changes, alarm changes, log events). Several different monitors may be created for each channel with different masks.

**Current:** Both `subscribe()` and `watch()` hardcode `MonitorMask::VALUE` (`client.rs:1610, 1636`). Users cannot subscribe to alarm changes or log events through the public API.

---

### 10. No CA_PROTO_EVENTS_OFF / CA_PROTO_EVENTS_ON support

**Spec:** Clients with slow CPUs can send `EVENTS_OFF` (command 8) to pause subscription updates and `EVENTS_ON` (command 9) to resume them, providing flow control.

**Current:** Not implemented. The client has no mechanism to pause subscription updates if it falls behind processing them.

---

### 11. Search reply server IP not fully handled

**Spec:** Starting with CA_V411, the server's IP address is encoded in the SID/IP field of the search response if it differs from the sender's IP, or `0xffffffff` if it is the same.

**Current:** `searcher.rs:390-394` handles the `server_ip` field and falls back to the sender's IP, which is correct. However, the `0xffffffff` sentinel value handling depends on the message parsing layer — if `server_ip` is parsed as `Some(255.255.255.255)` rather than `None`, the wrong IP would be used.

---

### 12. No timeout on pending reads/writes

**Spec:** While not strictly mandated, the spec implies operations should not hang indefinitely. Circuit inactivity timeout handles dead connections, but individual operations have no timeout.

**Current:** `pending_reads` and `pending_writes` track `Instant` timestamps (`client.rs:415-417`) but these are never checked. If a server silently drops a read/write request (perhaps due to an unhandled error), the caller's oneshot will remain pending until the circuit eventually times out or closes.

---

## Priority Recommendations

| Priority | Issue | Impact |
|---|---|---|
| **High** | #5 — CA_PROTO_ERROR not handled | Operations hang when server sends error via this mechanism |
| **High** | #4 — ServerDisconn not handled | Stale channels after server-side PV removal |
| **High** | #2 — ClearChannel never sent | Server-side resource leak |
| **Medium** | #3 — Version negotiation not enforced | May send version-dependent messages to old servers |
| **Medium** | #8 — Subscribe hardcodes Basic category | Zero-count ambiguity; no metadata access for monitors |
| **Medium** | #7 — Beacons not acted upon | Slower reconnection to new/restarted servers |
| **Medium** | #12 — No operation timeout | Individual operations can hang |
| **Low** | #1 — No repeater registration | May miss beacons on systems with repeater |
| **Low** | #9 — MonitorMask hardcoded to VALUE | No alarm/log subscriptions via public API |
| **Low** | #10 — No EVENTS_OFF/ON | No client-side flow control |
| **Low** | #6 — CID reuse safety | Only matters after 2^32 channel creations |
| **Low** | #11 — Search reply IP sentinel | Depends on message parsing layer |
