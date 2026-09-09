// E2E load generator for the TCP pub-sub broker (closed-loop v0).
//
// Setup (scenario):
//   - Assumes a BrokerServer is already listening (see broker_main.cpp).
//   - 1 topic, 1 FANOUT publisher connection, 1 FANOUT subscriber.
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
//   taskset -c 1-2 ./build-release/loadgen -h 127.0.0.1 -p 1883 -t test -n 60000 -b 64 -i 1
//
#include "dispatcher.h"
#include "publisher.h"
#include "subscriber.h"
#include "message_broker.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <unistd.h>

using namespace std::chrono_literals;

namespace {

struct Config {
    std::string host = "127.0.0.1";
    int port = 1883;
    std::string topic = "test";
    int message_count = 60000;
    int payload_bytes = 64;
    std::chrono::milliseconds publish_interval{1};
};

struct Timestamps {
    int64_t intended_send_us = 0;
    int64_t actual_send_us = 0;
    int64_t actual_receive_us = 0;
};

std::vector<Timestamps> g_timestamps;
std::mutex g_timestamps_mutex;
std::atomic<int> g_recv_count{0};
int g_message_count = 0;

void printUsage(const char* argv0) {
    std::cerr
        << "Usage: " << argv0 << " [options]\n"
        << "  -h <host>            broker host (default: 127.0.0.1)\n"
        << "  -p <port>            broker port (default: 1883)\n"
        << "  -t <topic>           topic name (default: test)\n"
        << "  -n <message_count>   number of messages (default: 60000)\n"
        << "  -b <payload_bytes>   payload size in bytes, >= 4 (default: 64)\n"
        << "  -i <interval_ms>     publish interval in ms (default: 1)\n"
        << "  --help               show this help\n";
}

Config parseArgs(int argc, char** argv) {
    Config cfg;
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--help") {
            printUsage(argv[0]);
            std::exit(0);
        }
    }

    optind = 1;
    int opt = 0;
    try {
        while ((opt = ::getopt(argc, argv, "h:p:t:n:b:i:")) != -1) {
            switch (opt) {
                case 'h':
                    cfg.host = optarg;
                    break;
                case 'p':
                    cfg.port = std::stoi(optarg);
                    if (cfg.port <= 0 || cfg.port > 65535) {
                        throw std::out_of_range("port out of range");
                    }
                    break;
                case 't':
                    cfg.topic = optarg;
                    if (cfg.topic.empty()) {
                        throw std::invalid_argument("topic must be non-empty");
                    }
                    break;
                case 'n':
                    cfg.message_count = std::stoi(optarg);
                    if (cfg.message_count <= 0) {
                        throw std::out_of_range("message_count must be > 0");
                    }
                    break;
                case 'b':
                    cfg.payload_bytes = std::stoi(optarg);
                    if (cfg.payload_bytes < static_cast<int>(sizeof(uint32_t))) {
                        throw std::out_of_range("payload_bytes must be >= 4");
                    }
                    break;
                case 'i': {
                    const int interval_ms = std::stoi(optarg);
                    if (interval_ms <= 0) {
                        throw std::out_of_range("interval_ms must be > 0");
                    }
                    cfg.publish_interval = std::chrono::milliseconds(interval_ms);
                    break;
                }
                default:
                    printUsage(argv[0]);
                    std::exit(1);
            }
        }
        if (optind < argc) {
            std::cerr << "Unexpected positional argument: " << argv[optind] << '\n';
            printUsage(argv[0]);
            std::exit(1);
        }
    } catch (const std::exception& e) {
        std::cerr << "Invalid arguments: " << e.what() << '\n';
        printUsage(argv[0]);
        std::exit(1);
    }
    return cfg;
}

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
    if (unique_id >= static_cast<uint32_t>(g_message_count)) {
        return;
    }

    const int64_t actual_receive_us = nowMicros();
    {
        std::lock_guard<std::mutex> lock(g_timestamps_mutex);
        g_timestamps[unique_id].actual_receive_us = actual_receive_us;
    }
    g_recv_count.fetch_add(1, std::memory_order_relaxed);
}

void runPublisher(const Config& cfg) {
    // Prebuild payloads. First 4 bytes = unique id, big-endian.
    std::vector<std::string> messages(static_cast<size_t>(cfg.message_count));
    for (int i = 0; i < cfg.message_count; ++i) {
        std::string message(cfg.payload_bytes, 'x');
        const uint32_t unique_id_be = htonl(static_cast<uint32_t>(i));
        std::memcpy(message.data(), &unique_id_be, sizeof(unique_id_be));
        messages[static_cast<size_t>(i)] = std::move(message);
    }

    /*NetworkDispatcher dispatcher(cfg.host, cfg.port, NetworkDispatcherType::FANOUT);
    if (!waitUntilConnected(dispatcher)) {
        throw std::runtime_error("Publisher BrokerClient failed to connect");
    }
    Publisher publisher(dispatcher);*/
    BrokerClient bclient(cfg.host, cfg.port);
    bclient.start();
    std::this_thread::sleep_for(1s);

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < cfg.message_count; ++i) {
        const auto intended = start + i * cfg.publish_interval;
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

        //publisher.publish(cfg.topic, messages[static_cast<size_t>(i)]);
        std::vector<uint8_t> frame_buffer;
        // Create and encode the frame header
        FrameHeader frame_header;
        frame_header.version = PROTOCOL_VERSION;
        frame_header.type = ProtocolFrameType::PUBLISH;
        encode_frame_header(frame_header, frame_buffer);

        // Create and encode the publish request
        PublishRequest publish_request;
        publish_request.request_id = bclient.generateRequestId();
        publish_request.topic = cfg.topic;
        publish_request.payload.assign(messages[static_cast<size_t>(i)].begin(),
                                       messages[static_cast<size_t>(i)].end());
        encode_publish_request(publish_request, frame_buffer);

        // Send the frame buffer to the broker client
        //std::future<std::shared_ptr<RequestResult>> fut_ret =
        bclient.sendFrame(publish_request.request_id, frame_buffer);
    }
}

void runSubscriber(const Config& cfg) {
    NetworkDispatcher dispatcher(cfg.host, cfg.port, NetworkDispatcherType::FANOUT);
    if (!waitUntilConnected(dispatcher)) {
        throw std::runtime_error("Subscriber BrokerClient failed to connect");
    }
    Subscriber subscriber(dispatcher);
    subscriber.subscribe(cfg.topic, subscriberReceiveHandler);

    // Wait until all messages arrive (or give up after publish window + slack).
    const auto deadline =
        std::chrono::steady_clock::now() +
        cfg.message_count * cfg.publish_interval + 10s;
    while (g_recv_count.load(std::memory_order_acquire) < cfg.message_count &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(100ms);
    }
    subscriber.stop();
}

}  // namespace

int main(int argc, char** argv) {
    const Config cfg = parseArgs(argc, argv);
    g_message_count = cfg.message_count;
    g_timestamps.assign(static_cast<size_t>(cfg.message_count), Timestamps{});

    std::thread sub_thread(runSubscriber, cfg);
    // Give subscribe + SUBSCRIBE_ACK a moment before the first publish.
    std::this_thread::sleep_for(1s);

    std::thread pub_thread(runPublisher, cfg);
    pub_thread.join();
    sub_thread.join();

    std::vector<int64_t> latencies;
    latencies.reserve(static_cast<size_t>(cfg.message_count));
    int missing = 0;
    for (int i = 0; i < cfg.message_count; ++i) {
        const Timestamps& ts = g_timestamps[static_cast<size_t>(i)];
        if (ts.actual_receive_us == 0 || ts.actual_send_us == 0) {
            ++missing;
            continue;
        }
        // E2E: subscriber observe time - actual publish send time.
        latencies.push_back(ts.actual_receive_us - ts.actual_send_us);
    }

    std::cout << "sent=" << cfg.message_count
              << " received=" << g_recv_count.load()
              << " missing=" << missing << '\n';

    if (latencies.empty()) {
        std::cerr << "No latency samples; is the broker running on " << cfg.host << ':'
                  << cfg.port << "?\n";
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
