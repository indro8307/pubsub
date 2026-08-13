# pub-sub (C++)

**Version:** v0.3 — Networked consumer-group broker (Phase 3)

Apps publish messages to named topics; subscribers receive them. Under the hood
there is one routing engine, `MessageBroker`, built around a **consumer-group**
model (same idea as Kafka groups): each group gets a copy of every message, and
members of the same group compete so only one of them handles each message.
Compete (work queue) and fan-out (classic pub-sub) are just different ways of
assigning groups.

That engine runs in two modes:

1. **In-process** — publishers and subscribers share a `MessageBroker` in one
  process (local demos and most unit tests).
2. **Over TCP** — a server process runs `MessageBroker`; clients talk to it with
  the length-prefixed frames in `[docs/protocol.md](docs/protocol.md)`.

Either way, application code uses `Publisher` / `Subscriber`. A `Dispatcher`
underneath picks the delivery policy (and, for networking, the transport):
`CompeteConsumerDispatcher` and `FanoutDispatcher` for local use,
`NetworkDispatcher` for TCP. Today `NetworkDispatcher` only does fan-out (see
[Known limitations](#known-limitations)).

---



## Table of contents

- [Design goals](#design-goals)
- [Architecture overview](#architecture-overview)
- [Core concepts](#core-concepts)
  - [Consumer-group model](#consumer-group-model)
  - [Dispatcher (policy layer)](#dispatcher-policy-layer)
  - [Network path](#network-path)
  - [Message and BrokerMessage](#message-and-brokermessage)
  - [SubscriptionToken](#subscriptiontoken)
  - [Publish semantics](#publish-semantics)
- [Backpressure](#backpressure)
- [Threading and concurrency model](#threading-and-concurrency-model)
- [Lifecycle and shutdown](#lifecycle-and-shutdown)
- [Sequencing guarantees](#sequencing-guarantees)
- [Trade-offs and rejected alternatives](#trade-offs-and-rejected-alternatives)
- [Known limitations](#known-limitations)
- [Project layout](#project-layout)
- [Build and run](#build-and-run)
- [Testing](#testing)
- [Roadmap](#roadmap)

---



## Design goals

1. **One routing engine, two delivery patterns.** Rather than maintaining
  separate compete and fan-out code paths, the broker implements a single
   consumer-group model. Fan-out and competing consumers are two configurations
   of the same mechanism.
2. **Applications think in topics, not groups.** `Publisher` and `Subscriber`
  expose topics only. Group assignment (the compete-vs-copy decision) lives in
   the `Dispatcher` layer — including over the network.
3. **Safe lifecycles under concurrency.** Subscribers run their own worker
  threads. Shutdown is event-driven (no polling). Queue, thread, and socket
   teardown is meant to stay race-free under ThreadSanitizer.
4. **Bounded memory (in-process queues).** Every `MessageQueue` is bounded with
  a configurable backpressure policy. Network-path backpressure is not wired up
   yet (see limitations).
5. **Same app API on or off process.** `NetworkDispatcher` implements
  `Dispatcher` over TCP so `Publisher` / `Subscriber` call sites stay the same.
6. **Bad wire input should not crash the process.** Decoders check inner
  length fields against the real buffer; a bad frame closes that session (see
   protocol § malformed → close).

---



## Architecture overview



### In-process (Phase 2)

```
 Application        Policy layer            Routing engine              Consumers
 ───────────        ────────────            ──────────────              ─────────
 Publisher  ─┐                          ┌─ Topic "orders"
             ├─►  Dispatcher  ──────────┤    ├─ Group "billing"  ─► MessageQueue ─► worker thread
 Subscriber ─┘   (Compete / Fanout)     │    │                                        (Subscriber)
                                        │    └─ Group "audit"    ─► MessageQueue ─► worker thread
                                        │
                                        └─ MessageBroker owns  topics → groups → queue
```



### Networked (Phase 3 v1)

```
 App process                                              Broker process
 ───────────                                              ──────────────
 Publisher / Subscriber
        │
 NetworkDispatcher ── BrokerClient (TCP) ──► BrokerServer accept
        │                                         │
   local MessageQueue ◄── DELIVER frames ── Session (reader + per-sub deliverers)
        │                                         │
   Subscriber worker                         MessageBroker (same engine as Phase 2)
```


| Component           | Responsibility                                                                                                      |
| ------------------- | ------------------------------------------------------------------------------------------------------------------- |
| `Publisher`         | App-facing publish API. Holds a `Dispatcher&`.                                                                      |
| `Subscriber`        | App-facing consume API. On `subscribe()` starts a worker that dequeues a local queue and calls your handler.        |
| `Dispatcher`        | Delivery policy / transport. Implementations: `CompeteConsumerDispatcher`, `FanoutDispatcher`, `NetworkDispatcher`. |
| `MessageBroker`     | Routing: `topics → groups → queue`, sequencing, subscribe/unsubscribe.                                              |
| `MessageQueue`      | Per-group (or client-local) bounded FIFO + backpressure strategy.                                                   |
| `protocol_frame`    | Length-prefixed frame codec (`[docs/protocol.md](docs/protocol.md)`).                                               |
| `BrokerServer`      | Listen/accept loop; owns sessions.                                                                                  |
| `Session`           | One TCP client: frame read loop, broker calls, one DELIVER thread per subscription.                                 |
| `BrokerClient`      | TCP client: connect/receive thread, `sendFrame` + ack futures, DELIVER / subscribe-ack hooks.                       |
| `NetworkDispatcher` | Owns a `BrokerClient`; maps wire `subscription_id` → local `MessageQueue`.                                          |


Dependency direction: `Publisher` / `Subscriber` → `Dispatcher` → (local broker
**or** network client). Application code never names a group.

---



## Core concepts



### Consumer-group model

A **topic** holds a map of named **groups**; each group owns exactly one
`MessageQueue`. The delivery rule is:

> **Each group gets a copy of every message; members within a group compete for
> each message.**


| Pattern                                           | Group topology                                   | Result                                      |
| ------------------------------------------------- | ------------------------------------------------ | ------------------------------------------- |
| **Competing consumers** (work queue)              | all subscribers share **one** group              | each message consumed by exactly one member |
| **Fan-out** (pub-sub)                             | each subscriber has **its own** group            | each subscriber receives every message      |
| **Mixed** (e.g. billing workers + audit listener) | multiple named groups, some with several members | per-group copy, intra-group competition     |


Groups and queues are created **lazily** on the first `subscribe(topic, group)`.
When a group's `memberCount` drops to zero, its queue is closed and the group is
erased; when a topic has no groups left, the topic goes too (ephemeral,
delete-when-empty).

```cpp
// message_broker.h
struct Group {
    std::shared_ptr<MessageQueue> queue;
    std::size_t memberCount = 0;
};
struct Topic {
    std::map<std::string, Group> groups;
    uint64_t nextSeq = 1;   // per-topic sequence counter
};
```



### Dispatcher (policy layer)

`Dispatcher` hides groups from application code and chooses the delivery pattern
(and, for networking, the transport).

- `CompeteConsumerDispatcher` — uses the topic name as the single fixed
group. Publishes via `broker.publish(topic, group, msg, buffer=true)`.
- `FanoutDispatcher` — unique group per subscriber
(`topic + "_sub_" + <id>`) from a process-wide atomic counter. Publishes via
`broker.publish(topic, msg)`.
- `NetworkDispatcher` — owns a `BrokerClient` (host/port; `start` in the
ctor, `stop` in the dtor). Over the wire it always allocates a **UUID** group
(`topic + "_sub_" + uuid`) so names stay unique across processes/hosts.
Publish / subscribe / unsubscribe send frames and wait for acks. Incoming
`DELIVER` is demuxed into a **local** `MessageQueue` so existing `Subscriber`
workers keep working unchanged.

Subscribe-ack handling runs on the client receive thread **before** the matching
`sendFrame` promise is fulfilled. That way `subscription_id → queue` is in the
map before any following `DELIVER` can race an empty lookup.

### Network path

Wire format is in `[docs/protocol.md](docs/protocol.md)` (big-endian integers,
`u32 frame_len` then header + body).


| Role         | Behavior                                                                                                                                                                                                                      |
| ------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Server**   | `BrokerServer` accepts connections. Each `Session` reads frames and calls `MessageBroker`. After `SUBSCRIBE`, a **deliverer thread** per subscription drains the group queue and writes `DELIVER`.                            |
| **Client**   | `BrokerClient` runs a receive loop. Request/response uses `promise`/`future` keyed by `request_id`.                                                                                                                           |
| **Teardown** | Prefer `shutdown` to wake blocked I/O, `join` the I/O thread, then `close` the FD — avoids `close` racing `recv`/`accept` under TSan. TCP drop or `CLOSE` runs session `cleanupSubscriptions` (join deliverers, unsubscribe). |




### Message and BrokerMessage

Two layers separate the user payload from broker metadata:

```cpp
class Message {                       // user payload
    int id;
    std::vector<uint8_t> payload;     // variable length (no fixed 4 KB buffer)
    size_t size;
};

class BrokerMessage {                 // broker envelope
    uint64_t sequence_;               // broker-assigned, per topic
    std::shared_ptr<const Message> message_;   // shared, never copied per queue
};
```

- **Variable-length payloads.** `Message` holds a `std::vector<uint8_t>`.
- **Shared, immutable payloads.** Queues store `shared_ptr<const BrokerMessage>`.
In fan-out, one `BrokerMessage` is shared across group queues.
- **Sequence lives on the envelope**, not the payload.



### SubscriptionToken

`subscribe()` returns an opaque `SubscriptionToken`; the same token is required
by `unsubscribe()`.


| Field   | Type                            | Purpose                                           |
| ------- | ------------------------------- | ------------------------------------------------- |
| `topic` | `std::string`                   | routing key                                       |
| `group` | `std::string`                   | routing key                                       |
| `id`    | `uint64_t`                      | membership / wire `subscription_id` once acked    |
| `mq`    | `std::shared_ptr<MessageQueue>` | queue the worker (or network demux) dequeues from |


- `shared_ptr`**, not a raw pointer.** The queue stays alive while any
subscriber still holds its token.
- **Id-keyed subscription registry** on the broker (and a parallel map on
`NetworkDispatcher` / `Session` for wire ids).
- `id == 0` **is the invalid/default token.**



### Publish semantics

Two overloads on the in-process broker:


| Overload                                  | Meaning                                      | No subscribers yet                      | Creates a group? |
| ----------------------------------------- | -------------------------------------------- | --------------------------------------- | ---------------- |
| `publish(topic, msg)`                     | broadcast to **all existing groups**         | message is **dropped** (`return false`) | no               |
| `publish(topic, group, msg, buffer=true)` | directed / compete delivery to **one group** | message is **buffered** (`return true`) | yes, lazily      |


`buffer=false` is strict: returns `false` without creating anything if the group
does not already exist. Sequence numbers are assigned under `topic_mtx` before
enqueue.

Over the network, `NetworkDispatcher::publish` sends `PUBLISH` and waits for
`PUBLISH_ACK` (`ACCEPTED` / `NO_SUBSCRIBERS`).

---



## Backpressure

Every in-process `MessageQueue` is **bounded** (`MessageQueueConfig::maxSize`,
default `10000`) and enforces one of three policies via a Strategy pattern:


| Policy                   | When full                                      | Publisher effect          | Data loss            | Use when                          |
| ------------------------ | ---------------------------------------------- | ------------------------- | -------------------- | --------------------------------- |
| **Block**                | wait on `not_full_cv` until space or `closed_` | publish thread stalls     | none (unless closed) | must not lose data                |
| **DropOldest** (default) | drop oldest, push new                          | always succeeds           | oldest evicted       | telemetry / keep producers moving |
| **RejectNew**            | return `false`                                 | `publish` reports failure | new message rejected | non-blocking failure visibility   |


- Backpressure is an **enqueue** concern. Lock / CV / dequeue live in
`MessageQueue`; the strategy runs while the queue already holds its mutex.
- **Fan-out isolates slow subscribers** to their own queues. Under **Block**,
serial fan-out enqueue can still stall the whole publish for later groups —
hence `DropOldest` as the default.
- On the **network path** there is a second hop (broker group queue → wire →
client-local queue). There is no end-to-end backpressure yet: a slow client
does not push load back through TCP in a controlled way, and full queues /
slow deliverers are not coordinated with the publisher.

---



## Threading and concurrency model

**In-process**

- One worker thread per active `Subscriber`.
- Broker `topic_mtx` for structure/sequences; per-queue mutex for enqueue/dequeue.
- Publish snapshots queue `shared_ptr`s under the broker lock, then enqueues
outside it so a slow queue does not hold up subscribe/unsubscribe.

**Network**

- `BrokerServer`: accept thread + per-`Session` reader + per-subscription
deliverer threads.
- `BrokerClient`: one connect/receive thread; app threads call `sendFrame`
(writes serialized under `socket_fd_mutex_`).
- `NetworkDispatcher` maps are guarded by `subscription_tokens_mutex_`.

Handler / deliver callbacks catch exceptions so a bad callback does not take down
the receive or deliverer thread.

---



## Lifecycle and shutdown

**Subscriber (local)** — event-driven, not timeout-polled:

```
state = stopped → wakeConsumers() → worker.join() → dispatcher.unsubscribe() → token_ = {}
```

`join()` before `unsubscribe()` so a departing compete worker cannot steal
siblings’ messages. The broker `close()`s a group queue only when
`memberCount` hits zero.

**Network sockets** — wake, then join, then close:

1. `shutdown(fd, SHUT_RDWR)` unblocks `recv` / `accept`.
2. Join the I/O thread.
3. `close(fd)` only after that thread has stopped using the FD.

Session `CLOSE`, TCP disconnect, or `requestStop` all run
`cleanupSubscriptions` (stop/join deliverers, then `broker.unsubscribe`).

---



## Sequencing guarantees

- Sequences are **broker-assigned and monotonic per topic**, starting at `1`.
- **Fan-out:** one `BrokerMessage` (one sequence) is shared to all group queues;
each fan-out subscriber should see the full set `1..N`.
- **Compete:** the group collectively sees each sequence once.
- **Order across concurrent publishers** is not a hard cross-queue guarantee:
fan-out publish assigns a sequence under the lock, then enqueues to groups
*after* releasing `topic_mtx`, so concurrent publishes can interleave enqueues
(same multiset of sequences, possibly different order per subscriber). Tests
that need identical order serialize publishes (e.g. network stress).

Covered by in-process sequence tests and network fan-out / stress tests.

---



## Trade-offs and rejected alternatives


| Decision                                                | Alternative rejected                        | Why                                                                    |
| ------------------------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------- |
| Single consumer-group engine                            | Separate compete/fan-out maps               | One path; mixed topologies; familiar mental model                      |
| `Dispatcher` as the boundary                            | Parallel `IBroker` stack                    | Network client implements `Dispatcher`; app API unchanged              |
| `NetworkDispatcher` owns `BrokerClient` by value        | Shared/raw client injected everywhere       | Clear lifetime: ctor `start`, dtor `stop`                              |
| UUID fan-out groups on the wire                         | Process-local atomic counter only           | Counters collide across processes/hosts                                |
| Subscribe-ack hook before promise fulfill               | Fulfill then register map                   | Stops first `DELIVER` racing an empty client map                       |
| Per-subscription deliverer threads on `Session`         | Single session thread draining all queues   | Isolates slow subscriptions; one queue ↔ one sub                       |
| Serial fan-out enqueue after snapshot                   | Thread-per-queue (or pool) on every publish | Avoids thread churn; documents Block HOL blocking; pool left as a TODO |
| `shutdown` → join → `close`                             | `close` while another thread is in `recv`   | Fixes TSan FD races; avoids locking across blocking `recv`             |
| `failAllPending(exception_ptr)`                         | `make_exception_ptr(const exception&)`      | Avoids slicing `runtime_error` into a bare `std::exception`            |
| Decoder bounds checks (inner lengths)                   | Trust `frame_len` alone                     | Lying topic/payload sizes caused heap OOB                              |
| Broker-assigned per-topic sequence                      | Publisher-assigned                          | Single serialization point                                             |
| Sequence on `BrokerMessage`                             | Sequence on `Message`                       | Payload stays pure                                                     |
| Backpressure only on enqueue                            | Per-strategy dequeue forks                  | Dequeue is shared; duplication caused Block wakeup bugs                |
| `close()` only on last unsubscribe                      | Close in every `Subscriber::stop()`         | Shared compete queues must stay open for siblings                      |
| Predicate `dequeueUntil`                                | Polling `dequeueFor`                        | Lower shutdown latency                                                 |
| Ephemeral groups                                        | Durable groups                              | MVP simplicity                                                         |
| `DropOldest @ 10k` default                              | `Block` default                             | Avoid wedging publishers under fan-out                                 |
| Stress: serialize network publishes for identical order | Assert order under concurrent pubs          | Current fan-out enqueue is not cross-queue totally ordered             |


---



## Known limitations

- In-memory broker only; no persistence.
- Pub-sub `publish(topic, msg)` drops messages before any subscriber exists.
- **Block** + fan-out: one full queue can stall later groups in the same publish.
- `NetworkDispatcher` **is fan-out only.** Subscribe always allocates a unique
UUID group. Compete-over-TCP (several network clients sharing one group) is
not exposed.
- **No network-layer backpressure.** Client-local and broker queues are still
bounded, but slow consumers / full queues are not coordinated end-to-end over
TCP (no windowing, no pushback that slows the publisher via the wire).
- No per-request ACK timeout (missing ack ⇒ hang until `stop` / disconnect).
- `Publisher` still sends message id `0` in local demos unless the caller passes
an id through `Dispatcher::publish`.
- CMake / `run_tests.py` do not yet expose a first-class `ENABLE_TSAN` / `--tsan`
switch; use `-fsanitize=thread` flags directly (below).
- `run_tests.py`’s built-in suite list may lag CMake — prefer running
`network_tests` / `protocol_frame_tests` binaries from the build dir if needed.

---



## Project layout


| File                                            | Purpose                                                  |
| ----------------------------------------------- | -------------------------------------------------------- |
| `message_queue.h` / `.cpp`                      | `Message`, `BrokerMessage`, `MessageQueue`, backpressure |
| `message_broker.h` / `.cpp`                     | Consumer-group routing, sequencing, lifecycle            |
| `dispatcher.h` / `dispatcher.cpp`               | Compete / Fanout / Network dispatchers                   |
| `publisher.h` / `.cpp`, `subscriber.h` / `.cpp` | App-facing APIs                                          |
| `protocol_frame.h` / `.cpp`                     | Wire codec                                               |
| `docs/protocol.md`                              | Protocol specification                                   |
| `broker_server.h` / `.cpp`                      | TCP accept loop                                          |
| `session.h` / `.cpp`                            | Per-connection reader + deliverers                       |
| `broker_client.h` / `.cpp`                      | TCP client + futures / handlers                          |
| `main.cpp`                                      | In-process compete / fan-out demo                        |
| `run_tests.py`                                  | Build / run GoogleTest suites                            |
| `tests/`                                        | Unit, in-process, protocol, and network suites           |


---



## Build and run

Needs a C++17 compiler, pthreads, and **libuuid** (`uuid-dev` on Debian/Ubuntu).

### CMake (primary)

```bash
cmake -S . -B build
cmake --build build
./build/pubsub
```

The first configure downloads GoogleTest via `FetchContent` (needs network).

### ThreadSanitizer build

There is no `ENABLE_TSAN` CMake option yet. Use a separate build dir and pass the
sanitizer flags (GCC/Clang on Linux/WSL):

```bash
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread -fno-omit-frame-pointer -g" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread" \
  -DCMAKE_GTEST_DISCOVER_TESTS_DISCOVERY_MODE=PRE_TEST
cmake --build build-tsan -j$(nproc)
./build-tsan/network_tests
```

WSL notes:

- If you hit `unexpected memory mapping` error, try `sudo sysctl vm.mmap_rnd_bits=28`.



### g++ (quick, no tests)

```bash
g++ -std=c++17 -O2 -pthread *.cpp -luuid -o pubsub
./pubsub
```

---



## Testing

```bash
python3 run_tests.py --build
python3 run_tests.py -s fanout_tests
# Network / protocol (from build dir if not listed in run_tests.py):
./build-debug/network_tests --gtest_filter='NetworkTests.*'
./build-debug/network_tests --gtest_filter='NetworkStress.*'   # slow
./build-debug/protocol_frame_tests --gtest_filter='ProtocolFrameCorruptLength.*'
```



### Suites


| Suite                        | Focus                                                                                                                                                                     |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `message_broker_tests`       | subscribe/unsubscribe, groups, publish overloads, sequences                                                                                                               |
| `compete_tests`              | one-of-N delivery, multi-publisher sequences, backpressure, lifecycle                                                                                                     |
| `fanout_tests`               | all-receive, isolation, sequences, large payloads, backpressure, in-process stress                                                                                        |
| `subscriber_lifecycle_tests` | stop/join, subscribe-after-stop, handler exceptions                                                                                                                       |
| `protocol_frame_tests`       | codec round-trips; corrupt/truncated inner lengths                                                                                                                        |
| `network_tests`              | TCP connect, subscribe/publish/unsubscribe, fan-out, teardown, malformed frames, session multiplex; `NetworkStress` (50 dispatchers × 100 subs, serialized publish order) |




### Test patterns

1. `std::atomic` / mutex-guarded structures in handlers or consumer threads,
2. `waitUntil(pred, timeout)` for asynchronous conditions,
3. Explicit stop / destructor teardown so threads and sockets exit cleanly.

Each test builds its own `MessageBroker` (and server/client as needed) so suites
do not share state.

---



## Roadmap

- Compete-over-TCP (shared group name API on `NetworkDispatcher`).
- Network-layer / end-to-end backpressure.
- Per-request ACK timeouts without tearing down the connection.
- Optional `ENABLE_TSAN` CMake option and keep `run_tests.py` suite list in sync.
- Persistence / durable groups (deferred from MVP).
- Slow-subscriber isolation under Block (non-serial fan-out enqueue / thread pool).
- Introducing epoll/io_uring event-loop I/O.

