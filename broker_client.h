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
#include <functional>

struct RequestResult {
    ProtocolFrameType type = ProtocolFrameType::SUBSCRIBE_ACK;
    SubscribeAck subscribe_ack{};
    UnsubscribeAck unsubscribe_ack{};
    PublishAck publish_ack{};
    CloseAck close_ack{};
};

class BrokerClient {
public:
    BrokerClient(const std::string& host, int port, std::function<void(const DeliverMessage& message)> deliver_message_handler = nullptr);
    ~BrokerClient();

    BrokerClient(const BrokerClient&) = delete;
    BrokerClient& operator=(const BrokerClient&) = delete;

    void start();
    void stop();

    std::future<std::shared_ptr<RequestResult>> sendFrame(uint32_t request_id,
                                                          std::vector<uint8_t> data);
    // |data| is a preallocated wire buffer: first 4 bytes are reserved for the
    // big-endian frame length; |frame_len| is the size of the frame body that
    // already occupies data[4 .. 4+frame_len).
    std::future<std::shared_ptr<RequestResult>> sendFrame(uint32_t request_id,
                                                          char* data,
                                                          size_t frame_len);

    uint32_t generateRequestId();
    bool isConnected() const;

    // hook to handle DELIVER_MESSAGE frame
    void setDeliverMessageHandler(std::function<void(const DeliverMessage& message)> handler);

    // hook to handle SUBSCRIBE_ACK frame
    // Invoked on the receive thread when SUBSCRIBE_ACK arrives, before the
    // matching sendFrame() promise is fulfilled. Lets the dispatcher publish
    // subscription_id → queue before any subsequent DELIVER is handled.
    void setSubscribeAckHandler(std::function<void(const SubscribeAck&)> handler);

private:
    ssize_t recvExact(int fd, void* buffer, size_t count);
    ssize_t sendExact(int fd, const void* buffer, size_t count);

    void fulfillPromise(uint32_t request_id, std::shared_ptr<RequestResult> result);
    void failAllPending(std::exception_ptr ep);

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
    // hook to handle DELIVER MESSAGE frame
    std::function<void(const DeliverMessage& message)> deliver_message_handler_;
    std::function<void(const SubscribeAck&)> subscribe_ack_handler_;
};

#endif
