# CA Protocol Compliance: `server.rs`

Comparison of `src/server.rs` against the [CA Protocol Specification](ca_protocol.rst).

## Correctly Implemented

| Feature | Spec Requirement | Status |
|---|---|---|
| **CA_PROTO_VERSION exchange** | Both sides MUST send as first message on TCP | Implemented (server sends first, expects reply) |
| **CA_PROTO_ECHO** | Server MUST immediately copy back to client | Implemented |
| **CA_PROTO_SEARCH (UDP)** | Listen on UDP, reply with TCP port; datagram MUST begin with CA_PROTO_VERSION | Implemented (includes version prefix in reply) |
| **CA_PROTO_CREATE_CHAN** | Reply with ACCESS_RIGHTS then CREATE_CHAN response containing native type/count and SID | Implemented |
| **CA_PROTO_CREATE_CH_FAIL** | Reply with failure when PV not found | Implemented |
| **CA_PROTO_READ_NOTIFY** | MUST reply to all requests; send CA_PROTO_ERROR on failure | Implemented |
| **CA_PROTO_WRITE_NOTIFY** | MUST reply to all requests with status code | Implemented |
| **CA_PROTO_WRITE** | Write with no success response | Partially (see issue #5) |
| **CA_PROTO_EVENT_ADD** | Create subscription; SHOULD immediately send current value | Implemented |
| **CA_PROTO_EVENT_CANCEL** | Remove subscription | Implemented (see issue #1 for response format) |
| **CA_PROTO_CLIENT_NAME / HOST_NAME** | One-way messages, no response | Implemented |
| **CA_PROTO_ACCESS_RIGHTS** | Sent before CREATE_CHAN response | Implemented |
| **CA_PROTO_RSRV_IS_UP (Beacons)** | Periodically broadcast with incrementing BeaconID | Implemented (see issue #6 for timing) |
| **Search deduplication** | Duplicate UDP datagrams should be handled | Implemented (see issue #10 for timing) |

## Issues

### 1. CA_PROTO_EVENT_CANCEL response may have wrong format

**Spec:** The response to EVENT_CANCEL uses command ID **1** (`CA_PROTO_EVENT_ADD`) with **zero payload** and **data count = 0**.

**Current:** `server.rs:705` calls `msg.response().into()`. Needs verification that this produces an `EventAddResponse` (command 1) with zero payload, not an `EventCancel` response (command 2).

---

### 2. CA_PROTO_CLEAR_CHANNEL missing response

**Spec:** Server MUST respond with a `CLEAR_CHANNEL` reply (command 12) echoing SID and CID, then release resources. "Server responds immediately and only then releases channel resources."

**Current:** `server.rs:747-755` removes the channel from the map but returns `Ok(Vec::default())` — no response is sent. Clients that wait for the reply will hang or time out.

---

### 3. No inactivity timeout on circuits

**Spec:** Both client and server MUST begin a countdown timer on circuit creation. If no traffic is received before the timer expires, the circuit MUST be closed. Recommended timeout: 30 seconds.

**Current:** `server.rs:415` stores `last_message: Instant` but it is never checked. There is no timeout logic in the event loop. Dead client connections will persist indefinitely.

---

### 4. CA_PROTO_EVENTS_OFF / CA_PROTO_EVENTS_ON not handled

**Spec:** Commands 8 and 9 disable/enable subscription updates on a circuit. No response is sent.

**Current:** The `client_events_on: bool` field exists (`server.rs:419`) but neither command is matched in `handle_message` — they fall through to the catch-all `msg => Err(MessageError::UnexpectedMessage(msg))` at `server.rs:803`, which logs an error and ignores the message. Subscription updates are never suppressed.

---

### 5. CA_PROTO_WRITE sends error response on failure

**Spec:** "There is no response to this command." The server SHOULD make best effort but failures are silent.

**Current:** `server.rs:783-793` sends an `ECAError` response when a write fails. This violates the spec — `CA_PROTO_WRITE` must never generate a response. Only `CA_PROTO_WRITE_NOTIFY` should respond on failure.

---

### 6. Beacon timing does not ramp up

**Spec:** Initial beacon interval of 0.02 seconds, doubling after each beacon up to a maximum of 15 seconds.

**Current:** `server.rs:233` uses a fixed `beacon_period` from `get_default_beacon_period()` with no ramp-up logic. Beacons are sent at a constant rate from startup.

---

### 7. CA_PROTO_SERVER_DISCONN not implemented

**Spec:** When a server destroys a PV or the channel is otherwise invalidated, it MUST send `CA_PROTO_SERVER_DISCONN` (command 27) with the CID for each affected channel.

**Current:** Not implemented. If a PV is removed at runtime, connected clients will not be notified and will only discover the loss on their next operation.

---

### 8. CA_PROTO_READ (legacy, command 3) not handled

**Spec:** Deprecated since protocol version 3.13 but may still be sent by older clients.

**Current:** Would fall through to the `UnexpectedMessage` catch-all. Older clients would see their read requests silently rejected.

---

### 9. Version negotiation not enforced

**Spec:** "Valid messages and semantics of the Circuit are determined by the lower of the two minor versions." For example, dynamic array size (data count = 0) is only valid in CA_V413+.

**Current:** `client_version` is stored (`server.rs:487`) but never consulted. The server does not gate any behavior on the negotiated version. This could cause issues with older clients that don't expect dynamic array sizes or other version-dependent features.

---

### 10. Search deduplication window is very short

**Spec:** UDP datagrams may be duplicated due to multiple network routes.

**Current:** `server.rs:321` uses a 500-microsecond dedup window. On real networks with multiple interfaces, duplicate search packets may arrive with more latency than this, defeating the deduplication.

---

## Priority Recommendations

| Priority | Issue | Impact |
|---|---|---|
| **High** | #2 — Missing ClearChannel response | Clients will hang waiting for reply |
| **High** | #5 — Write sends error response | Protocol violation, may confuse clients |
| **Medium** | #4 — Events Off/On not handled | Flow control broken for slow clients |
| **Medium** | #3 — No inactivity timeout | Dead connections accumulate forever |
| **Medium** | #1 — EventCancel response format | May send wrong command ID |
| **Low** | #6 — Beacon ramp-up | Excessive beacon traffic on startup |
| **Low** | #7 — ServerDisconn | No runtime PV removal notification |
| **Low** | #9 — Version negotiation | Potential issues with old clients |
| **Low** | #8 — Legacy read | Only affects pre-3.13 clients |
| **Low** | #10 — Dedup window | May process duplicate searches |
