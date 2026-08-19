#ifndef BROKER_SERVER_H
#define BROKER_SERVER_H

#include "message_broker.h"
#include "session.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
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

    // True after bind/listen succeed; safe for tests to wait on before connecting.
    bool isListening() const { return listening_.load(std::memory_order_acquire); }

    // Snapshot of sessions accepted so far (includes sessions that may have ended
    // if they have not been reaped yet).
    std::vector<std::shared_ptr<Session>> sessions() const {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        std::vector<std::shared_ptr<Session>> out;
        out.reserve(sessions_.size());
        for (const auto& [fd, session] : sessions_) {
            (void)fd;
            out.push_back(session);
        }
        return out;
    }

private:
    // Dispatch one decoded frame (header + body already in |frame|).
    // |frame| is non-const because the codec decode APIs take a mutable buffer.
    void handleFrame(const FrameHeader& header, std::vector<uint8_t>& frame);

    void handleSubscribe(std::vector<uint8_t>& frame);
    void handleUnsubscribe(std::vector<uint8_t>& frame);
    void handlePublish(std::vector<uint8_t>& frame);
    void handleClose(std::vector<uint8_t>& frame);

    void run();
    void handleClientConnection(int client_socket);
    void handleClientReadable(int client_fd);
    void closeClient(int client_fd);

    // Remove finished sessions from |sessions_| (called from acceptLoop or stop).
    void reapFinishedSessions();

    MessageBroker& broker_;
    uint16_t port_;

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    std::thread accept_thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> listening_{false};

    mutable std::mutex sessions_mtx_;
    std::map<int, std::shared_ptr<Session>> sessions_;
    std::unordered_map<int, std::vector<uint8_t>> read_bufs_;
};

#endif
