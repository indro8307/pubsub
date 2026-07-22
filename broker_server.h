#ifndef BROKER_SERVER_H
#define BROKER_SERVER_H

#include "message_broker.h"
#include "session.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

// TCP accept loop for the broker daemon (docs/protocol.md).
//
// Ownership:
//   - Owns the listen socket and the accept thread.
//   - Holds a reference to an existing MessageBroker (does not own it and
//     does not start networking inside MessageBroker).
//   - Spawns one Session per accepted connection. Sessions are joined on
//     stop(), not from the accept loop, so accept stays non-blocking w.r.t.
//     client lifetime.
//
// Minimal surface for Phase 3 v1: start / stop only. No TLS, no backlog
// tuning beyond a default, no connection limits yet.
class BrokerServer {
public:
    BrokerServer(MessageBroker& broker, uint16_t port);
    ~BrokerServer();

    BrokerServer(const BrokerServer&) = delete;
    BrokerServer& operator=(const BrokerServer&) = delete;

    // Bind, listen, and spawn the accept thread. Throws on bind/listen failure.
    void start();

    // Stop accepting, requestStop() every live session, join them, then join
    // the accept thread and close the listen socket.
    void stop();

    bool isRunning() const { return running_.load(std::memory_order_acquire); }

private:
    void acceptLoop();

    // Remove finished sessions from |sessions_| (called from acceptLoop or stop).
    void reapFinishedSessions();

    MessageBroker& broker_;
    uint16_t port_;

    int listen_fd_ = -1;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};

    std::mutex sessions_mtx_;
    std::vector<std::shared_ptr<Session>> sessions_;
};

#endif
