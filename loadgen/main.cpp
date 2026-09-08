// E2E load generator for the TCP pub-sub broker (closed-loop v0).
//
// Setup (scenario):
//   - Assumes a BrokerServer is already listening (see broker_main.cpp).
//   - 1 topic ("test"), 1 FANOUT publisher connection, 1 FANOUT subscriber.
//   - 600 messages × 64-byte payloads, paced at 1 publish / 100ms (~60s).
//   - Correlator: uint32 big-endian id in payload bytes [0,4); timestamps in
//     g_timestamps[id] (intended send, actual send, receive) using steady_clock.
//   - Reports sent/received/missing and E2E p50/p90/p99 (actual_receive - actual_send).
//
// Build:
//   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release \
//         -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG -march=native"
//   cmake --build build-release --target loadgen
//   # (also build broker: cmake --build build-release --target broker)
//
// Sample usage (two terminals; pin cores for cleaner numbers):
//   taskset -c 0 ./build-release/broker 1883
//   taskset -c 1-2 ./build-release/loadgen
//
// If the broker uses a non-default port, change kPort below to match.
//
#include "dispatcher.h"
#include "publisher.h"
#include "subscriber.h"
#include "message_broker.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>

using namespace std::chrono_literals;

namespace {

constexpr const char* kHost = "127.0.0.1";
constexpr int kPort = 1883;
constexpr const char* kTopic = "test";
constexpr int kMessageCount = 600;
constexpr int kPayloadBytes = 64;
constexpr auto kPublishInterval = 100ms;

struct Timestamps {
    int64_t intended_send_us = 0;
    int64_t actual_send_us = 0;
    int64_t actual_receive_us = 0;
};

std::vector<Timestamps> g_timestamps(static_cast<size_t>(kMessageCount));
std::mutex g_timestamps_mutex;
std::atomic<int> g_recv_count{0};

int64_t nowMicros() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntilConnected(NetworkDispatcher& dispatcher,
                        std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (dispatcher.isConnected()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return dispatcher.isConnected();
}

void subscriberReceiveHandler(const BrokerMessage& message) {
    const Message& msg = message.payload();
    if (msg.getSize() < sizeof(uint32_t)) {
        return;
    }
    uint32_t unique_id_be = 0;
    std::memcpy(&unique_id_be, msg.getPayload(), sizeof(unique_id_be));
    const uint32_t unique_id = ntohl(unique_id_be);
    if (unique_id >= static_cast<uint32_t>(kMessageCount)) {
        return;
    }

    const int64_t actual_receive_us = nowMicros();
    {
        std::lock_guard<std::mutex> lock(g_timestamps_mutex);
        g_timestamps[unique_id].actual_receive_us = actual_receive_us;
    }
    g_recv_count.fetch_add(1, std::memory_order_relaxed);
}

void runPublisher() {
    // Prebuild 600 x 64-byte payloads. First 4 bytes = unique id (0..599), big-endian.
    std::vector<std::string> messages(static_cast<size_t>(kMessageCount));
    for (int i = 0; i < kMessageCount; ++i) {
        std::string message(kPayloadBytes, 'x');
        const uint32_t unique_id_be = htonl(static_cast<uint32_t>(i));
        std::memcpy(message.data(), &unique_id_be, sizeof(unique_id_be));
        messages[static_cast<size_t>(i)] = std::move(message);
    }

    NetworkDispatcher dispatcher(kHost, kPort, NetworkDispatcherType::FANOUT);
    if (!waitUntilConnected(dispatcher)) {
        throw std::runtime_error("Publisher BrokerClient failed to connect");
    }
    Publisher publisher(dispatcher);

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kMessageCount; ++i) {
        const auto intended =
            start + i * kPublishInterval;
        // Closed-loop pacing: wait until the intended send instant.
        std::this_thread::sleep_until(intended);

        const int64_t intended_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                intended.time_since_epoch())
                .count();
        const int64_t actual_send_us = nowMicros();
        {
            std::lock_guard<std::mutex> lock(g_timestamps_mutex);
            g_timestamps[static_cast<size_t>(i)].intended_send_us = intended_us;
            g_timestamps[static_cast<size_t>(i)].actual_send_us = actual_send_us;
        }

        publisher.publish(kTopic, messages[static_cast<size_t>(i)]);
    }
}

void runSubscriber() {
    NetworkDispatcher dispatcher(kHost, kPort, NetworkDispatcherType::FANOUT);
    if (!waitUntilConnected(dispatcher)) {
        throw std::runtime_error("Subscriber BrokerClient failed to connect");
    }
    Subscriber subscriber(dispatcher);
    subscriber.subscribe(kTopic, subscriberReceiveHandler);

    // Wait until all messages arrive (or give up after publish window + slack).
    const auto deadline =
        std::chrono::steady_clock::now() + kMessageCount * kPublishInterval + 10s;
    while (g_recv_count.load(std::memory_order_acquire) < kMessageCount &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(100ms);
    }
    subscriber.stop();
}

}  // namespace

int main() {
    // See file-header comment for scenario, build, and usage.

    std::thread sub_thread(runSubscriber);
    // Give subscribe + SUBSCRIBE_ACK a moment before the first publish.
    std::this_thread::sleep_for(1s);

    std::thread pub_thread(runPublisher);
    pub_thread.join();
    sub_thread.join();

    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<size_t>(kMessageCount));
    int missing = 0;
    for (int i = 0; i < kMessageCount; ++i) {
        const Timestamps& ts = g_timestamps[static_cast<size_t>(i)];
        if (ts.actual_receive_us == 0 || ts.actual_send_us == 0) {
            ++missing;
            continue;
        }
        // E2E: subscriber observe time - actual publish send time.
        latencies.push_back(ts.actual_receive_us - ts.actual_send_us);
    }

    std::cout << "sent=" << kMessageCount
              << " received=" << g_recv_count.load()
              << " missing=" << missing << '\n';

    if (latencies.empty()) {
        std::cerr << "No latency samples; is the broker running on " << kHost << ':'
                  << kPort << "?\n";
        return 1;
    }

    std::sort(latencies.begin(), latencies.end());
    const auto pct = [&](double p) -> int64_t {
        const size_t idx =
            static_cast<size_t>(p * static_cast<double>(latencies.size() - 1));
        return latencies[idx];
    };

    std::cout << "P50 latency: " << pct(0.50) << " us\n";
    std::cout << "P90 latency: " << pct(0.90) << " us\n";
    std::cout << "P99 latency: " << pct(0.99) << " us\n";
    return 0;
}
