# pub-sub (C++ MVP)

**Version:** v0.1 — In-memory threaded broker

Single-process, in-memory publish–subscribe demo in C++17. A global `MessageBroker` backs two dispatching modes exposed through a `Dispatcher` interface.

## Architecture

```
Publisher  →  Dispatcher  →  MessageBroker  →  MessageQueue(s)  →  Subscriber (worker thread)
```

| Component | Role |
|-----------|------|
| `MessageBroker` | Topic routing, queue storage, subscribe/unsubscribe |
| `CompeteConsumerDispatcher` | One shared queue per topic; consumers compete for each message |
| `FanoutDispatcher` | One queue per subscriber; every subscriber gets a copy |
| `Publisher` | Publishes string payloads to a topic |
| `Subscriber` | Background worker thread with a user handler |

## Current functionality

### Messaging

- `Message` with integer id and fixed-size binary payload (up to 4096 bytes)
- Thread-safe `MessageQueue`: `enqueue`, blocking `dequeue`, and timed `dequeueFor` (for cooperative shutdown)

### Competing consumers

- One `MessageQueue` per topic in `sharedQueues`
- Multiple subscribers on the same topic share that queue; each message is consumed by one worker
- `competeUnsubscribe` is a no-op (queue remains while the topic exists)

### Fan-out

- One `MessageQueue` per subscriber, stored in a `std::list` per topic
- `fanoutPublish` enqueues a copy to every subscriber queue on the topic
- `fanoutUnsubscribe(topic, mq)` removes that subscriber’s queue from the list (matched by address)

### Subscriber lifecycle

- `subscribe(topic, handler)` starts a worker thread that polls the queue with `dequeueFor` (100 ms timeout)
- Graceful shutdown: set `running` false and join the worker (no sentinel messages; safe with shared compete queues)
- `worker.joinable()` guard prevents a second `subscribe()` while a worker is active
- `std::mutex` protects `subscribe()` / `stop()` when used from the same subscriber instance
- `Dispatcher::unsubscribe` is available on the broker/dispatcher API (call after the worker has stopped if you remove fan-out queues)

### Demo (`main.cpp`)

- **Compete:** two publishers, two subscribers on topic `orders`
- **Fan-out:** two publishers, two subscribers on topic `notifications`
- Dispatchers held in `std::unique_ptr` (no leaked `new`)

## Build

Requires a C++17 compiler and pthread support for `std::thread`.

### g++ (Linux / WSL / MinGW)

```bash
g++ -std=c++17 -O2 -pthread *.cpp -o pubsub
./pubsub
```

### Makefile

```bash
make build    # writes compile log to build.log
./pubsub
```

### CMake

```bash
mkdir build && cd build
cmake ..
cmake --build .
./pubsub      # or pubsub.exe on Windows
```

CMake links `message_queue.cpp` and `Threads::Threads` (pthread on GCC/Clang).

## GoogleTest tutorial (quick)

GoogleTest (gtest) is a unit-test framework. Tests live in `tests/pubsub_tests.cpp`.

### Concepts

| Piece | Meaning |
|-------|---------|
| `TEST(SuiteName, TestName)` | One test function; suite groups related tests |
| `EXPECT_*` | Soft assert — failure is reported, other tests still run |
| `ASSERT_*` | Hard assert — stops the current test on failure |
| `EXPECT_THROW(expr, ExceptionType)` | Expects `expr` to throw that exception type |
| `gtest_main` | Supplies `main()` that runs all tests |

### Minimal example

```cpp
#include <gtest/gtest.h>

TEST(Math, Adds) {
    EXPECT_EQ(1 + 1, 2);
}
```

### Async pub-sub tests

Workers run in background threads, so tests use:

1. `std::atomic<int>` counters inside handlers  
2. `waitUntil(lambda, timeout)` — poll until condition or timeout  
3. `Subscriber::stop()` in test teardown so threads exit cleanly  

Each test creates its own `MessageBroker` (injectable broker) so tests do not share state.

### Build and run tests (CMake)

```bash
mkdir -p build && cd build
cmake ..
cmake --build .
ctest --output-on-failure
# or run directly:
./pubsub_tests
```

First configure downloads GoogleTest via CMake `FetchContent` (needs network).

### Tests included

| Test | What it checks |
|------|----------------|
| `CompeteRouting.TwoSubscribers_OneMessage_OnlyOneReceives` | 1 publish → exactly 1 handler invocation across 2 compete subscribers |
| `FanoutRouting.TwoSubscribers_BothReceive` | 1 publish → 2 receives with same payload |
| `SubscriberLifecycle.DoubleSubscribe_Throws` | Second `subscribe()` throws `std::logic_error` |
| `SubscriberLifecycle.Stop_UnsubscribesFanout` | `fanoutSubscriberCount` drops after each `stop()` |
| `SubscriberLifecycle.Stop_NoHang` | `stop()` completes within 2 seconds |
| `SubscriberLifecycle.HandlerThrows_WorkerContinues` | Exception in handler; later message still processed |

## Project layout

| File | Purpose |
|------|---------|
| `message_queue.h` / `message_queue.cpp` | `Message`, `MessageQueue`, `MessageBroker`, global broker accessor |
| `dispatcher.h` | `CompeteConsumerDispatcher`, `FanoutDispatcher` |
| `publisher.cpp` | Publish API |
| `subscriber.cpp` | Subscribe / stop / worker thread |
| `main.cpp` | End-to-end demo |
| `tests/pubsub_tests.cpp` | GoogleTest suite |

## Limitations (MVP)

- `MessageBroker` is created in `main` (or tests) and injected into dispatchers; `getGlobalMessageBroker()` remains optional
- Unbounded queues; no persistence or back-pressure
- Publisher uses message id `0` for all messages
- Demo uses fixed `sleep` to drain queues before `stop()`
- Fan-out unsubscribe must be invoked explicitly (e.g. from `Subscriber::stop()`) if you want broker cleanup on shutdown
