// Standalone TCP broker process (MessageBroker + epoll BrokerServer).
//
// Build:
//   cmake --build build-release --target broker
//
// Usage:
//   ./broker              # listen on 1883 (default)
//   ./broker 1883         # explicit port
//   ./broker 29000        # custom port (match loadgen / clients)
//
// Stop with Ctrl+C (SIGINT) or SIGTERM.
//
// Example: pin broker to core 0, then run loadgen on other cores:
//   taskset -c 0 ./build-release/broker 1883
//   taskset -c 1-2 ./build-release/loadgen
//
#include "broker_server.h"
#include "message_broker.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::atomic<bool> g_run{true};

void onSignal(int) {
    g_run.store(false, std::memory_order_release);
}

}  // namespace

int main(int argc, char** argv) {
    uint16_t port = 1883;
    if (argc > 1) {
        try {
            const int parsed = std::stoi(argv[1]);
            if (parsed <= 0 || parsed > 65535) {
                throw std::out_of_range("port out of range");
            }
            port = static_cast<uint16_t>(parsed);
        } catch (const std::exception& e) {
            std::cerr << "Invalid port '" << argv[1] << "': " << e.what() << '\n';
            std::cerr << "Usage: " << argv[0] << " [port]\n";
            return 1;
        }
    }

    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);

    MessageBroker broker;
    BrokerServer server(broker, port);
    try {
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Failed to start BrokerServer: " << e.what() << '\n';
        return 1;
    }

    using namespace std::chrono_literals;
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!server.isListening() && g_run.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(10ms);
    }
    if (!server.isListening()) {
        std::cerr << "BrokerServer did not start listening on port " << port << '\n';
        server.stop();
        return 1;
    }

    std::cout << "Broker listening on port " << port << std::endl;

    while (g_run.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(200ms);
    }

    std::cout << "Shutting down..." << std::endl;
    server.stop();
    return 0;
}
