# pub-sub Wire Protocol

**Status:** Draft — Phase 3 (networking), minimal v1
**Protocol version:** `1`
**Transport:** TCP (one broker connection per client)

This is the smallest protocol that lets a client subscribe, publish, receive
messages, unsubscribe, and close a connection over the network. Acks,
handshakes, keepalives, and delivery guarantees are intentionally **left out**
for now and added in later iterations.

It is a transport contract only. Application code keeps using the topic-level
`Publisher` / `Subscriber` API; a client-side network `Dispatcher` encodes that
API into the frames below, and the broker daemon decodes them and calls into the
existing in-process `MessageBroker`.

```
Publisher/Subscriber ─► NetworkDispatcher ─► [wire frames over TCP] ─► Session ─► MessageBroker
      (app API)            (client lib)                                (daemon)     (unchanged core)
```

Mapping onto broker concepts:


| Broker concept             | Wire representation                               |
| -------------------------- | ------------------------------------------------- |
| Topic                      | length-prefixed UTF-8 string                      |
| Group (compete vs fan-out) | length-prefixed string in `SUBSCRIBE`             |
| Subscription               | `subscription_id` (u64), **chosen by the client** |
| `Message` payload          | length-prefixed bytes                             |
| `BrokerMessage` sequence   | `sequence` (u64) in `DELIVER`                     |


> **Fan-out vs compete without acks.** The client owns the group name.
> Two subscribers using the **same** group compete; subscribers using
> **different** (e.g. unique) groups each get their own queue and see every
> message. A plain `PUBLISH topic` fans out to all groups on the topic, which
> covers both cases — so no directed-publish or group flags are needed yet.

---

## 1. Conventions

- **Byte order:** all multi-byte integers are **big-endian** (network order).
- **Types:** `u8`, `u16`, `u32`, `u64` are fixed-width unsigned integers.
- **Strings (`str`):** `u16 length` + `length` bytes of UTF-8 (max `65535`).
- **Bytes (`bytes`):** `u32 length` + `length` bytes.
- Topics and groups are compared byte-for-byte (case-sensitive).

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


| Type   | Name          | Direction       | Purpose                                                    |
| ------ | ------------- | --------------- | ---------------------------------------------------------- |
| `0x10` | `SUBSCRIBE`   | client → broker | join `(topic, group)` with a client-chosen subscription id |
| `0x11` | `UNSUBSCRIBE` | client → broker | leave a subscription                                       |
| `0x20` | `PUBLISH`     | client → broker | publish a message to a topic (fans out to all groups)      |
| `0x21` | `DELIVER`     | broker → client | a message delivered to a subscription                      |
| `0x30` | `CLOSE`       | client → broker | graceful shutdown of the connection                        |


That's the whole surface for now. Acks, handshake, ping/pong, and error frames
are deferred (§6).

---

## 4. Frame bodies

Fields are listed in wire order. The `ver` + `type` header (§2) is omitted here.

### 4.1 `SUBSCRIBE` (0x10) — client → broker

```
u64   subscription_id    // client-chosen; unique per connection
str   topic
str   group              // same group = compete; unique group = fan-out
```

Maps to `MessageBroker::subscribe(topic, group)`. The broker records that
`subscription_id` on this connection and begins pushing `DELIVER` frames for it.

### 4.2 `UNSUBSCRIBE` (0x11) — client → broker

```
u64   subscription_id    // from a prior SUBSCRIBE on this connection
```

Maps to `MessageBroker::unsubscribe(token)`. Unknown ids are ignored (no-op).
After processing, the broker sends no further `DELIVER` for that id.

### 4.3 `PUBLISH` (0x20) — client → broker

```
str   topic
bytes payload
```

Maps to `MessageBroker::publish(topic, msg)` — fan out to every group on the
topic. If the topic has no subscribers the message is dropped (no reply either
way, since there are no acks yet).

### 4.4 `DELIVER` (0x21) — broker → client

```
u64   subscription_id    // which subscription this is for
str   topic
u64   sequence           // BrokerMessage sequence (per-topic, monotonic)
bytes payload
```

Broker-initiated server push, sent when a message is available on the queue
backing `subscription_id`. `sequence` is `BrokerMessage::getSequence()`:

- **Fan-out:** each subscriber sees the full set `1..N` for the topic.
- **Compete:** members of a group collectively see each `sequence` once.

Delivery is fire-and-forget (at-most-once) for now.

### 4.5 `CLOSE` (0x30) — client → broker

```
(empty body)
```

The client asks for a graceful shutdown. The broker unsubscribes **all**
subscriptions owned by the connection (tearing down empty groups, as
`Subscriber::stop()` does today) and closes the socket. A dropped TCP connection
without `CLOSE` is treated the same way.

---

## 5. Connection lifecycle

```
Client                                  Broker
  |  SUBSCRIBE(sub=1, "orders", "g1") ─► |
  |  ◄──────  DELIVER(sub=1,"orders", seq=1, payload)
  |  ◄──────  DELIVER(sub=1,"orders", seq=2, payload)
  |                                      |
  |  PUBLISH("orders", payload)  ──────► |   (fans out to all groups)
  |                                      |
  |  UNSUBSCRIBE(sub=1)  ──────────────► |
  |                                      |
  |  CLOSE  ───────────────────────────► |
  |  (broker cleans up + closes socket)  |
```

Rules:

1. A connection may hold many subscriptions and publish freely; the broker
  demultiplexes `DELIVER` by `subscription_id`.
2. `subscription_id` only needs to be unique **within one connection**.
3. On `CLOSE` or TCP disconnect, the broker unsubscribes everything owned by that
  connection.

---

## 6. Worked example (hex)

`SUBSCRIBE` with `subscription_id = 1`, topic `orders`, group `g1`:

```
01 10                                   ver=1, type=SUBSCRIBE
00 00 00 00 00 00 00 01                 subscription_id = 1
00 06 6F 72 64 65 72 73                 topic = "orders"
00 02 67 31                             group = "g1"
```

Body length = 2 + 8 + 8 + 4 = 22 bytes. Full frame with prefix:

```
00 00 00 16                             frame_len = 22
01 10 00 00 00 00 00 00 00 01 00 06 6F 72 64 65 72 73 00 02 67 31
```

---

## 7. Deferred (add later, in roughly this order)


| Item                                                     | Why it can wait                                                   |
| -------------------------------------------------------- | ----------------------------------------------------------------- |
| `SUBSCRIBE_ACK` / `PUBLISH_ACK` + `request_id`           | client-assigned ids and fire-and-forget publish work without them |
| `ERROR` frame + error codes                              | v1 just closes the connection on malformed input                  |
| `HELLO` / `WELCOME` version handshake                    | fixed at version `1` for now                                      |
| `PING` / `PONG` keepalive                                | rely on TCP for liveness initially                                |
| Directed publish `PUBLISH(topic, group)` + `buffer` flag | plain topic broadcast covers compete + fan-out                    |
| `msg_id` in `PUBLISH`/`DELIVER`                          | payload-only is enough to start                                   |
| `ACK` / `NACK`, redelivery, offsets (at-least-once)      | Phase 4                                                           |


