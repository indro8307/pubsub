# pub-sub (C++ MVP)

**Version:** v0.1 — In-memory Threaded Broker

This is a small single-process, in-memory pub-sub demo written in C++17.

## Current functionality

- In-memory `MessageBroker` with named topic queues
- `Publisher` API for publishing messages to a topic
- `Subscriber` API that consumes messages from a topic on a worker thread
- Blocking, thread-safe queue with `enqueue` / `dequeue`
- Graceful subscriber shutdown using a sentinel message
- Minimal demo in `main.cpp` showing multiple publishers and subscribers
- `CMakeLists.txt` for build support and `Makefile` for redirected build logs

Build (with g++ / MinGW):

```bash
g++ -std=c++17 -O2 -pthread *.cpp -o pubsub.exe
./pubsub.exe
```

Or with CMake:

```bash
mkdir build && cd build
cmake ..
cmake --build .
./pubsub
```
