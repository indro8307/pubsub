# pub-sub (C++)

**Version:** v0.2 — In-memory consumer-group broker (Phase 2)

A single-process, in-memory publish–subscribe message broker in C++17. A
`MessageBroker` routes messages through a **consumer-group** model; two
`Dispatcher` implementations expose classic **fan-out** (pub-sub) and
**competing-consumers** (work-queue) delivery on top of that single engine.

---

## Table of contents

- [Design goals](#design-goals)
- [Architecture overview](#architecture-overview)
- [Core concepts](#core-concepts)
  - [Consumer-group model](#consumer-group-model)
  - [Dispatcher (policy layer)](#dispatcher-policy-layer)
  - [Message and BrokerMessage](#message-and-brokermessage)
  - [SubscriptionToken](#subscriptiontoken)
  - [Publish semantics](#publish-semantics)
- [Backpressure](#backpressure)
- [Threading and concurrency model](#threading-and-concurrency-model)
- [Lifecycle and shutdown](#lifecycle-and-shutdown)
- [Sequencing guarantees](#sequencing-guarantees)
- [Trade-offs and rejected alternatives](#trade-offs-and-rejected-alternatives)
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
   the `Dispatcher` layer.
3. **Safe lifecycles under concurrency.** Subscribers run their own worker
   threads. Shutdown is event-driven (no polling), and queue/thread teardown is
   race-free under ThreadSanitizer.
4. **Bounded memory.** Every queue is bounded with a configurable backpressure
   policy, so a slow or stalled consumer cannot exhaust memory.
5. **A seam for networking.** `Dispatcher` is the abstraction boundary a future
   network client would implement, so `Publisher`/`Subscriber` code is unchanged
   when transport moves off-process (Phase 3).

---

## Architecture overview

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

| Component | Responsibility |
|-----------|----------------|
| `Publisher` | Application-facing publish API. Holds a `Dispatcher&`; publishes string payloads to a topic. |
| `Subscriber` | Application-facing consume API. Holds a `Dispatcher&`; on `subscribe()` starts a worker thread that dequeues and invokes a user handler. |
| `Dispatcher` | **Delivery policy.** Translates the topic-level API into broker `(topic, group)` calls. Two implementations: `CompeteConsumerDispatcher`, `FanoutDispatcher`. |
| `MessageBroker` | **Routing mechanism.** Owns `topics → groups → queue`, lazy group/queue creation, subscribe/unsubscribe, and both publish overloads. Assigns per-topic sequence numbers. |
| `MessageQueue` | Per-group thread-safe bounded FIFO. Holds `shared_ptr<const BrokerMessage>` and a pluggable backpressure strategy. |

The dependency direction is strict: `Publisher`/`Subscriber` → `Dispatcher` →
`MessageBroker` → `MessageQueue`. Application code never names a group and never
touches a queue directly.

---

## Core concepts

### Consumer-group model

A **topic** holds a map of named **groups**; each group owns exactly one
`MessageQueue`. The delivery rule is:

> **Each group gets a copy of every message; members within a group compete for
> each message.**

This single rule expresses both classic patterns:

| Pattern | Group topology | Result |
|---------|----------------|--------|
| **Competing consumers** (work queue) | all subscribers share **one** group | each message consumed by exactly one member |
| **Fan-out** (pub-sub) | each subscriber has **its own** group | each subscriber receives every message |
| **Mixed** (e.g. billing workers + audit listener) | multiple named groups, some with several members | per-group copy, intra-group competition |

Groups and queues are created **lazily** by the broker on the first
`subscribe(topic, group)`. When a group's `memberCount` drops to zero, its queue
is closed and the group is erased; when a topic has no groups it is erased too
(ephemeral, delete-when-empty).

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

`Dispatcher` is the abstraction that hides groups from application code and
decides the delivery pattern.

- **`CompeteConsumerDispatcher`** — uses the topic name itself as a single fixed
  group. All subscribers land in that one group and therefore compete. Publishes
  via `broker.publish(topic, group, msg, buffer=true)`.
- **`FanoutDispatcher`** — assigns each subscriber a **unique** group name
  (`topic + "_sub_" + <id>`) from a broker-wide monotonic counter, so every
  subscriber gets its own queue and its own copy. Publishes via
  `broker.publish(topic, msg)`, which fans out to all groups on the topic.

The unique-id counter is a `static std::atomic<uint64_t>` on the dispatcher class
so that multiple `FanoutDispatcher` instances sharing one broker never collide on
a group name.

Because both dispatchers implement the same interface, a future network client
can implement `Dispatcher` over sockets without changing `Publisher`/`Subscriber`.

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

- **Variable-length payloads.** `Message` holds a `std::vector<uint8_t>`, so
  payloads are not truncated and not copied into a fixed buffer.
- **Shared, immutable payloads.** Queues store `shared_ptr<const BrokerMessage>`.
  In fan-out, the broker creates **one** `BrokerMessage` per publish and shares
  it across every group's queue — no N-way payload copy.
- **Sequence lives on the envelope, not the payload.** `BrokerMessage` carries
  the broker-assigned sequence; `Message` stays a pure payload.

### SubscriptionToken

`subscribe()` returns an opaque `SubscriptionToken`; the same token is required
by `unsubscribe()`.

| Field | Type | Purpose |
|-------|------|---------|
| `topic` | `std::string` | routing key |
| `group` | `std::string` | routing key |
| `id` | `uint64_t` | monotonic membership id — proof of who may unsubscribe once |
| `mq` | `std::shared_ptr<MessageQueue>` | the group's queue, for the worker to dequeue from |

Design points:

- **`shared_ptr`, not a raw pointer.** The queue stays alive while any
  subscriber still holds its token, even if the broker resets the group's queue
  during teardown — no dangling pointer into broker-owned containers.
- **Id-keyed subscription registry.** The broker keeps a
  `map<uint64_t, SubscriptionToken>`. `unsubscribe` looks up the id, validates
  that the token's `topic`/`group` match, then decrements the correct group's
  `memberCount`. This prevents double-unsubscribe and forged/stale tokens from
  corrupting an unrelated group.
- **`id == 0` is the invalid/default token** (a `Subscriber` in the idle state).

### Publish semantics

Two overloads with deliberately different semantics:

| Overload | Meaning | No subscribers yet | Creates a group? |
|----------|---------|--------------------|------------------|
| `publish(topic, msg)` | pub-sub broadcast to **all existing groups** | message is **dropped** (`return false`) | no |
| `publish(topic, group, msg, buffer=true)` | directed / compete delivery to **one group** | message is **buffered** in the group queue (`return true`) | yes, lazily |

`buffer=false` makes the directed overload strict: it returns `false` without
creating anything if the group does not already exist. The sequence number is
assigned inside `topic_mtx` in both overloads before enqueue.

---

## Backpressure

Every `MessageQueue` is **bounded** (`MessageQueueConfig::maxSize`, default
`10000`) and enforces one of three policies, selected once at construction via a
**Strategy pattern** (`BackPressureStrategy` with `Block` / `DropOldest` /
`RejectNew` implementations):

| Policy | When full | Publisher effect | Data loss | Use when |
|--------|-----------|------------------|-----------|----------|
| **Block** | wait on `not_full_cv` until space or `closed_` | publish thread stalls (backpressure propagates upstream) | none (unless closed) | must not lose data; producers can slow down |
| **DropOldest** (default) | `pop_front()` then push new | always succeeds | oldest evicted | telemetry / "latest wins"; keeps producers from wedging |
| **RejectNew** | return `false`, queue unchanged | `publish` reports failure | new message rejected | non-blocking APIs that must know about failure |

Design points:

- **Backpressure is an enqueue concern.** The policy decides only "what to do
  when full." The lock, condition variables, and all dequeue logic live in
  `MessageQueue`; the strategy runs while the queue already holds its mutex.
- **Two condition variables.** `not_empty_cv` wakes consumers on enqueue;
  `not_full_cv` wakes blocked producers on dequeue (Block only). Every
  successful dequeue notifies `not_full_cv` so Block producers cannot hang.
- **`enqueue` returns `bool`.** `RejectNew` (and a closed queue) surface as
  `false`, which `MessageBroker::publish` propagates to the caller.
- **Fan-out isolates slow subscribers.** Each fan-out subscriber has its own
  queue, so a slow consumer fills only its own queue. (Under **Block**, however,
  a full queue stalls the publisher for the whole topic, since fan-out enqueues
  to each group sequentially — hence `DropOldest` is the default for demos and
  stress tests.)

---

## Threading and concurrency model

- **One worker thread per active `Subscriber`.** Started on `subscribe()`,
  joined on `stop()`/destructor. The broker and publisher own no consumer
  threads.
- **Two lock levels.**
  - A single `topic_mtx` on the broker guards all structural state: the
    `topics`/`groups` maps, `memberCount`, the subscription registry, and
    per-topic sequence assignment.
  - Each `MessageQueue` has its own internal mutex for enqueue/dequeue.
- **Publish enqueues outside the broker lock.** `publish` snapshots the target
  queue `shared_ptr`(s) under `topic_mtx`, releases the lock, then enqueues. A
  slow or full queue therefore does not stall subscribe/unsubscribe or other
  topics. (The `shared_ptr` keeps each queue alive even if a concurrent
  unsubscribe removes the group mid-enqueue.)
- **Handler exceptions are isolated.** The worker wraps `handler(*m)` in
  `try/catch(...)`; a throwing handler does not kill the worker thread.
- **State is an `atomic<SubscriberState>`** (`idle` → `subscribed` → `stopped`),
  checked both by the worker loop and by the subscribe/stop guard.

---

## Lifecycle and shutdown

Shutdown is **event-driven**, not timeout-polled. It uses two independent
channels that must not be conflated:

1. **Subscriber-local wake (fast `join`).** `stop()` sets state to `stopped` and
   calls `MessageQueue::wakeConsumers()` (a `not_empty_cv.notify_all()` that does
   **not** close the queue). The worker blocks in
   `dequeueUntil(out, predicate)`, whose predicate is
   `closed_ || !queue.empty() || <state != subscribed>`, so it returns
   immediately and the thread exits.
2. **Broker-level `close()` (queue teardown).** `MessageBroker::unsubscribe`
   calls `queue->close()` **only when the last member leaves**
   (`memberCount == 0`). `close()` sets `closed_` and notifies both condition
   variables, so any blocked producer/consumer wakes, observes `closed_`, and
   exits cleanly.

The correct ordering in `Subscriber::stop()` is:

```
state = stopped;  ->  wakeConsumers();  ->  worker.join();  ->  dispatcher.unsubscribe();  ->  token_ = {};
```

**`join()` before `unsubscribe()`** is required: in compete mode several
subscribers share one queue, so unsubscribing before the worker stops could let a
departing worker steal messages from its siblings. Closing the queue only on the
last unsubscribe means one compete subscriber stopping never disturbs the others.

---

## Sequencing guarantees

- Sequence numbers are **broker-assigned and monotonic per topic**, starting at
  `1`, incremented under `topic_mtx` at a single serialization point. They are
  independent of which publisher sent the message.
- **Fan-out:** a single publish produces one `BrokerMessage` shared across all
  group queues, so every fan-out subscriber sees the **same** sequence for that
  message and receives the full set `1..N`.
- **Compete:** members of a group partition the sequence space — collectively the
  group receives every sequence `1..N` exactly once, with no gaps or duplicates.
- Delivery **order** across concurrent publishers is not guaranteed; only the
  completeness/uniqueness of the sequence set is.

These properties are covered by
`FanoutRouting.MultiplePublishers_EachSubscriberReceivesAllSequences` and
`CompeteRouting.MultiplePublishers_CollectivelyReceiveAllSequences`, which track
per-sequence counts to detect gaps and duplicates.

---

## Trade-offs and rejected alternatives

| Decision | Alternative rejected | Why |
|----------|----------------------|-----|
| Single consumer-group engine | Two separate compete/fan-out maps + mutexes | One code path; expresses mixed topologies; matches Kafka/NATS/Pub-Sub |
| `Dispatcher` as the seam | `IBroker` interface with parallel implementations | An interface with one production implementation is speculative; `Dispatcher` is already the boundary a network client needs |
| Broker-assigned per-topic sequence | Publisher-assigned sequence | Single serialization point; no cross-publisher coordination |
| Sequence on `BrokerMessage` envelope | Sequence field on `Message` | Keeps payload pure; envelope carries broker metadata |
| Backpressure only on enqueue | Dequeue variants per strategy | Dequeue is identical across policies; duplicating it caused a Block wakeup bug |
| `close()` only on last unsubscribe | `close()` in every `Subscriber::stop()` | Closing a shared compete queue would break siblings |
| Predicate-based `dequeueUntil` | 100 ms `dequeueFor` polling | Removes shutdown latency and idle wakeups |
| Ephemeral (delete-when-empty) groups | Durable groups | Simpler for MVP; durability deferred to a later phase |
| `DropOldest @ 10k` default | `Block` default | Prevents slow consumers from wedging publishers, especially under fan-out |

Known limitations (current):

- In-memory only; no persistence or crash recovery.
- Pub-sub `publish(topic, msg)` drops messages published before any subscriber
  exists (correct pub-sub semantics, but no buffering).
- Under **Block** + fan-out, one full subscriber queue can stall the publisher
  for the whole topic (serial fan-out enqueue).
- `Publisher` sends message id `0` for all messages (id is not yet a first-class
  field of the public API).

---

## Project layout

| File | Purpose |
|------|---------|
| `message_queue.h` / `message_queue.cpp` | `Message`, `BrokerMessage`, `MessageQueue`, backpressure strategies |
| `message_broker.h` / `message_broker.cpp` | `SubscriptionToken`, `MessageBroker` (consumer-group routing, sequencing, lifecycle) |
| `dispatcher.h` | `Dispatcher` interface, `CompeteConsumerDispatcher`, `FanoutDispatcher` |
| `publisher.h` / `publisher.cpp` | `Publisher` publish API |
| `subscriber.h` / `subscriber.cpp` | `Subscriber` subscribe / stop / worker thread |
| `main.cpp` | End-to-end demo (compete on `orders`, fan-out on `notifications`) |
| `run_tests.py` | Build + run the GoogleTest suites (optionally with TSan) |
| `tests/` | GoogleTest suites (see [Testing](#testing)) |

---

## Build and run

Requires a C++17 compiler and threads support.

### CMake (primary)

```bash
cmake -S . -B build
cmake --build build
./build/pubsub          # or build\pubsub.exe on Windows
```

The first configure downloads GoogleTest via CMake `FetchContent` (needs
network).

### ThreadSanitizer build

TSan is off by default and gated behind the `ENABLE_TSAN` CMake option
(GCC/Clang on Linux/WSL only):

```bash
cmake -S . -B build-debug -DENABLE_TSAN=ON
cmake --build build-debug
```

> If TSan reports `unexpected memory mapping` at runtime under WSL, run from a
> native Linux filesystem (`~/...`) rather than `/mnt/c/...`.

### g++ (quick, no tests)

```bash
g++ -std=c++17 -O2 -pthread *.cpp -o pubsub
./pubsub
```

---

## Testing

`run_tests.py` wraps CMake configure/build and test execution:

```bash
python3 run_tests.py --build                       # configure, build, run all suites via ctest
python3 run_tests.py                               # run all suites from an existing build
python3 run_tests.py -s fanout_tests               # run one suite
python3 run_tests.py -s fanout_tests -- --gtest_filter='FanoutRouting.*'
python3 run_tests.py --build --tsan --build-dir build-debug   # build + run under TSan
```

### Suites

| Suite | Focus |
|-------|-------|
| `message_broker_tests` | subscribe/unsubscribe counts, group lifecycle, both publish overloads, sequence numbers on broadcast |
| `compete_tests` | one-of-N delivery, many-message totals, multi-publisher sequence completeness, backpressure policies, lifecycle |
| `fanout_tests` | all-subscribers-receive, topic isolation, per-subscriber sequence completeness, large payloads, backpressure, stress (100 publishers × 1000 subscribers) |
| `subscriber_lifecycle_tests` | double-subscribe throws, stop without hang, subscribe-after-stop, destructor unsubscribes, handler-throws-worker-continues |

### Test patterns

Because handlers run on background threads, tests use:

1. `std::atomic` counters / mutex-guarded structures inside handlers,
2. `waitUntil(pred, timeout)` to poll for a condition,
3. `Subscriber::stop()` (or destructor) in teardown so threads exit cleanly and
   shared state is safe to read.

Each test constructs its own `MessageBroker` so suites never share state.
