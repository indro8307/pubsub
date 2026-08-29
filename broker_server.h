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

// A Subscription is a single subscription to a topic.
// Each subscription is associated with a session/Connection.
class Subscription {
public:
    Subscription(SubscriptionToken token, std::shared_ptr<Session> session);
    ~Subscription();

    Subscription(const Subscription&) = delete;
    Subscription& operator=(const Subscription&) = delete;

    SubscriptionToken token_;
    std::shared_ptr<Session> session_;
};

class Group {
public:
    Group();
    explicit Group(const std::string& group_name);
    ~Group();

    Group(const Group&) = delete;
    Group& operator=(const Group&) = delete;

    std::shared_ptr<Subscription> getNextSubscription() const;
    void addSubscription(std::shared_ptr<Subscription> subscription);
    void removeSubscription(uint64_t subscription_id);

    std::vector<std::shared_ptr<Subscription>> subscriptions_;
    mutable int index_ = 0;  // round-robin cursor; mutable so getNextSubscription can be const
    std::string group_name_;
};


class BrokerServer {
public:
    BrokerServer(MessageBroker& broker, uint16_t port);
    ~BrokerServer();

    BrokerServer(const BrokerServer&) = delete;
    BrokerServer& operator=(const BrokerServer&) = delete;

    // Bind, listen, and spawn the accept thread. Throws on bind/listen failure.
    void start();

    // Stop the reactor thread, then close remaining client sockets and drop
    // in-memory subscription indexes / broker subscriptions.
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
    // Returns false if the connection was closed and handleClientReadable should stop.
    bool handlePublish(int client_fd, std::vector<uint8_t>& frame_data);
    bool handleSubscribe(int client_fd, std::vector<uint8_t>& frame_data);
    bool handleUnsubscribe(int client_fd, std::vector<uint8_t>& frame_data);
    bool handleClose(int client_fd, std::vector<uint8_t>& frame_data);

    void run();
    void handleClientConnection(int client_socket);
    void handleClientReadable(int client_fd);
    void handleClientWritable(int client_fd);
    void closeClient(int client_fd);

    // Remove finished sessions from |sessions_| (called from acceptLoop or stop).
    void reapFinishedSessions();

    // Encode, enqueue, and attempt flush. Does not close the client — callers
    // must handle FlushResult::FLUSH_CLOSE (avoids reentrant closeClient while
    // iterating subscription maps).
    FlushResult buildAndSendFrame(int client_fd, ProtocolFrameType type, const std::vector<uint8_t>& body, 
                                           std::shared_ptr<Session> session);

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
    std::map<std::string, std::vector<std::shared_ptr<Group>>> subscriptions_by_topics_groups_;
    std::map<uint64_t, std::shared_ptr<Subscription>> subscriptions_by_id_;
};

#endif
