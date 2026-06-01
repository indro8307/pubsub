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

## Project layout

| File | Purpose |
|------|---------|
| `message_queue.h` / `message_queue.cpp` | `Message`, `MessageQueue`, `MessageBroker`, global broker accessor |
| `dispatcher.h` | `CompeteConsumerDispatcher`, `FanoutDispatcher` |
| `publisher.cpp` | Publish API |
| `subscriber.cpp` | Subscribe / stop / worker thread |
| `main.cpp` | End-to-end demo |

## Limitations (MVP)

- Global singleton broker (`getGlobalMessageBroker()`)
- Unbounded queues; no persistence or back-pressure
- Publisher uses message id `0` for all messages
- Demo uses fixed `sleep` to drain queues before `stop()`
- Fan-out unsubscribe must be invoked explicitly (e.g. from `Subscriber::stop()`) if you want broker cleanup on shutdown
