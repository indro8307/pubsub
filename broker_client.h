#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include "protocol_frame.h"

#include <atomic>
#include <cstdint>
#include <exception>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct RequestResult {
    ProtocolFrameType type = ProtocolFrameType::SUBSCRIBE_ACK;
    SubscribeAck subscribe_ack{};
    UnsubscribeAck unsubscribe_ack{};
    PublishAck publish_ack{};
    CloseAck close_ack{};
};

class BrokerClient {
public:
    BrokerClient(const std::string& host, int port);
    ~BrokerClient();

    BrokerClient(const BrokerClient&) = delete;
    BrokerClient& operator=(const BrokerClient&) = delete;

    void start();
    void stop();

    std::future<std::shared_ptr<RequestResult>> sendFrame(uint32_t request_id,
                                                          std::vector<uint8_t> data);

    uint32_t generateRequestId();
    bool isConnected() const;

private:
    ssize_t recvExact(int fd, void* buffer, size_t count);
    ssize_t sendExact(int fd, const void* buffer, size_t count);

    void fulfillPromise(uint32_t request_id, std::shared_ptr<RequestResult> result);
    void failAllPending(const std::exception& ex);

    void receive();
    void run();

    std::mutex socket_fd_mutex_;
    int socket_fd_;
    std::string host_;
    int port_;
    std::atomic<uint32_t> request_id_counter_;
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    std::thread connect_thread_;
    std::mutex request_promises_mutex_;
    std::map<uint32_t, std::promise<std::shared_ptr<RequestResult>>> request_promises_;
};

#endif
