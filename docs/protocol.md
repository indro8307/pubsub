# pub-sub Wire Protocol

**Status:** Draft — Phase 3 (networking), minimal v1
**Protocol version:** `1`
**Transport:** TCP (one broker connection per client)

This is the smallest protocol that lets a client subscribe, publish, receive
messages, unsubscribe, and close a connection over the network. Every
client-initiated action gets an **ack** from the broker. Handshakes, keepalives,
and delivery guarantees are left out for now.

It is a transport contract only. Application code keeps using the topic-level
`Publisher` / `Subscriber` API; a client-side network `Dispatcher` encodes that
API into the frames below, and the broker daemon decodes them and calls into the
existing in-process `MessageBroker`.

```
Publisher/Subscriber ─► NetworkDispatcher ─► [wire frames over TCP] ─► Session ─► MessageBroker
      (app API)            (client lib)                                (daemon)     (unchanged core)
```

Mapping onto broker concepts:

| Broker concept | Wire representation |
|----------------|---------------------|
| Topic | length-prefixed UTF-8 string |
| Group (compete vs fan-out) | length-prefixed string in `SUBSCRIBE` |
| Subscription | `subscription_id` (u64), **assigned by the broker** in `SUBSCRIBE_ACK` |
| Request correlation | `request_id` (u32), chosen by the client, echoed in the matching ack |
| `Message` payload | length-prefixed bytes |
| `BrokerMessage` sequence | `sequence` (u64) in `DELIVER` |

> **Fan-out vs compete.** The client supplies the group name on `SUBSCRIBE`.
> Two subscribers using the **same** group compete; subscribers using
> **different** (e.g. unique) groups each get their own queue and see every
> message. A plain `PUBLISH topic` fans out to all groups on the topic.

---

## 1. Conventions

- **Byte order:** all multi-byte integers are **big-endian** (network order).
- **Types:** `u8`, `u16`, `u32`, `u64` are fixed-width unsigned integers.
- **Strings (`str`):** `u16 length` + `length` bytes of UTF-8 (max `65535`).
- **Bytes (`bytes`):** `u32 length` + `length` bytes.
- Topics and groups are compared byte-for-byte (case-sensitive).
- **`request_id`:** client-chosen `u32`, unique among in-flight requests on this
  connection. The broker echoes it in the matching ack so the client can pair
  request and response.

---

## 2. Framing

TCP is a byte stream with no message boundaries, so every frame is
**length-prefixed**: read the 4-byte length, then read exactly that many more
bytes to get one complete frame.

```
+---------------+------------------------------------------------+
| u32 frame_len | frame_len bytes:                               |
|               |  +--------+--------+------------------------+  |
| (excludes the |  | u8 ver | u8 type|  body (type-specific)  |  |
|  4 len bytes) |  +--------+--------+------------------------+  |
+---------------+------------------------------------------------+
```

- `frame_len` counts everything after itself (`ver` + `type` + body).
- `ver` is `1`. A frame with an unsupported version is dropped and the
  connection closed.
- `type` selects the body layout (§4).
- A frame larger than `MAX_FRAME_LEN` (`16 MiB`) is rejected and the connection
  closed before the body is read.

---

## 3. Frame types

| Type | Name | Direction | Purpose |
|------|------|-----------|---------|
| `0x10` | `SUBSCRIBE` | client → broker | join `(topic, group)` |
| `0x12` | `SUBSCRIBE_ACK` | broker → client | subscription accepted; carries broker-assigned `subscription_id` |
| `0x11` | `UNSUBSCRIBE` | client → broker | leave a subscription |
| `0x13` | `UNSUBSCRIBE_ACK` | broker → client | unsubscribe processed |
| `0x20` | `PUBLISH` | client → broker | publish a message to a topic (fans out to all groups) |
| `0x22` | `PUBLISH_ACK` | broker → client | publish result |
| `0x21` | `DELIVER` | broker → client | a message delivered to a subscription (server push) |
| `0x30` | `CLOSE` | client → broker | graceful shutdown of the connection |
| `0x31` | `CLOSE_ACK` | broker → client | shutdown accepted; broker will close the socket |

`DELIVER` is the only broker-initiated frame that is not an ack. Delivery
acks (`ACK`/`NACK` for at-least-once) are deferred to Phase 4 (§7).

---

## 4. Frame bodies

Fields are listed in wire order. The `ver` + `type` header (§2) is omitted here.

### 4.1 `SUBSCRIBE` (0x10) — client → broker

```
u32   request_id
str   topic
str   group              // same group = compete; unique group = fan-out
```

Maps to `MessageBroker::subscribe(topic, group)`. The broker assigns a
`subscription_id` (from its internal `nextSubscriptionId_`) and returns it in
`SUBSCRIBE_ACK`. The client MUST NOT send `DELIVER`-targeting traffic until it
has received the ack.

### 4.2 `SUBSCRIBE_ACK` (0x12) — broker → client

```
u32   request_id        // echoes SUBSCRIBE.request_id
u64   subscription_id   // broker-assigned; use in UNSUBSCRIBE and to match DELIVER
```

After this ack, the broker MAY begin sending `DELIVER` frames for
`subscription_id` on this connection.

### 4.3 `UNSUBSCRIBE` (0x11) — client → broker

```
u32   request_id
u64   subscription_id   // from a prior SUBSCRIBE_ACK
```

Maps to `MessageBroker::unsubscribe(token)`. Unknown ids are a no-op that still
receive `UNSUBSCRIBE_ACK` (idempotent).

### 4.4 `UNSUBSCRIBE_ACK` (0x13) — broker → client

```
u32   request_id        // echoes UNSUBSCRIBE.request_id
u64   subscription_id
```

After this ack the broker guarantees no further `DELIVER` frames for that
`subscription_id`.

### 4.5 `PUBLISH` (0x20) — client → broker

```
u32   request_id
str   topic
bytes payload
```

Maps to `MessageBroker::publish(topic, msg)` — fan out to every group on the
topic.

### 4.6 `PUBLISH_ACK` (0x22) — broker → client

```
u32   request_id        // echoes PUBLISH.request_id
u8    result
```

| `result` | Name | Meaning |
|----------|------|---------|
| `0` | `ACCEPTED` | message enqueued to at least one group |
| `1` | `NO_SUBSCRIBERS` | topic had no groups; message dropped |

Mirrors the `bool` returned by `MessageBroker::publish(topic, msg)`.

### 4.7 `DELIVER` (0x21) — broker → client

```
u64   subscription_id   // which subscription this is for
str   topic
u64   sequence          // BrokerMessage sequence (per-topic, monotonic)
bytes payload
```

Broker-initiated server push, sent when a message is available on the queue
backing `subscription_id`. `sequence` is `BrokerMessage::getSequence()`:

- **Fan-out:** each subscriber sees the full set `1..N` for the topic.
- **Compete:** members of a group collectively see each `sequence` once.

Delivery is fire-and-forget (at-most-once) for now — no `DELIVER_ACK`.

### 4.8 `CLOSE` (0x30) — client → broker

```
u32   request_id
```

The client asks for a graceful shutdown.

### 4.9 `CLOSE_ACK` (0x31) — broker → client

```
u32   request_id        // echoes CLOSE.request_id
```

The broker unsubscribes **all** subscriptions owned by the connection (tearing
down empty groups, as `Subscriber::stop()` does today), sends `CLOSE_ACK`, then
closes the socket. A dropped TCP connection without `CLOSE` is treated the same
way (cleanup on disconnect; no `CLOSE_ACK` in that case).

---

## 5. Connection lifecycle

```
Client                                  Broker
  |  SUBSCRIBE(req=1,"orders","g1") ───► |
  |  ◄──────  SUBSCRIBE_ACK(req=1, sub=42)
  |  ◄──────  DELIVER(sub=42,"orders", seq=1, payload)
  |  ◄──────  DELIVER(sub=42,"orders", seq=2, payload)
  |                                      |
  |  PUBLISH(req=2,"orders", payload) ─► |
  |  ◄──────  PUBLISH_ACK(req=2, ACCEPTED)
  |                                      |
  |  UNSUBSCRIBE(req=3, sub=42)  ───────► |
  |  ◄──────  UNSUBSCRIBE_ACK(req=3, sub=42)
  |                                      |
  |  CLOSE(req=4)  ─────────────────────► |
  |  ◄──────  CLOSE_ACK(req=4)
  |  (broker closes socket)              |
```

Rules:

1. Every client request frame (`SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`, `CLOSE`)
   gets exactly one matching ack before the client should issue the next request
   of the same kind with the same `request_id`.
2. A connection may hold many subscriptions and publish freely; the broker
   demultiplexes `DELIVER` by `subscription_id`.
3. `subscription_id` is assigned by the broker and is unique among active
   subscriptions on that connection.
4. On `CLOSE` or TCP disconnect, the broker unsubscribes everything owned by that
   connection.

---

## 6. Worked example (hex)

`SUBSCRIBE` with `request_id = 1`, topic `orders`, group `g1`:

```
01 10                                   ver=1, type=SUBSCRIBE
00 00 00 01                             request_id = 1
00 06 6F 72 64 65 72 73                 topic = "orders"
00 02 67 31                             group = "g1"
```

Body length = 2 + 4 + 8 + 4 = 18 bytes. Full frame with prefix:

```
00 00 00 12                             frame_len = 18
01 10 00 00 00 01 00 06 6F 72 64 65 72 73 00 02 67 31
```

Broker replies `SUBSCRIBE_ACK` (`request_id = 1`, `subscription_id = 42`):

```
01 12                                   ver=1, type=SUBSCRIBE_ACK
00 00 00 01                             request_id = 1
00 00 00 00 00 00 00 2A                 subscription_id = 42
```

---

## 7. Deferred (add later, in roughly this order)

| Item | Why it can wait |
|------|-----------------|
| `ERROR` frame + error codes | v1 closes the connection on malformed input |
| `HELLO` / `WELCOME` version handshake | fixed at version `1` for now |
| `PING` / `PONG` keepalive | rely on TCP for liveness initially |
| Directed publish `PUBLISH(topic, group)` + `buffer` flag | plain topic broadcast covers compete + fan-out |
| `msg_id` in `PUBLISH`/`DELIVER` | payload-only is enough to start |
| `DELIVER_ACK` / `NACK`, redelivery, offsets (at-least-once) | Phase 4 |
