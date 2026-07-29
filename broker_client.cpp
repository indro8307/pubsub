#include "broker_client.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <exception>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

BrokerClient::BrokerClient(const std::string& host, int port)
    : socket_fd_(-1),
      host_(host),
      port_(port),
      request_id_counter_(0),
      connected_(false),
      running_(false) {}

BrokerClient::~BrokerClient() {
    stop();
}

void BrokerClient::start() {
    std::lock_guard<std::mutex> lock(socket_fd_mutex_);
    if (running_.load(std::memory_order_acquire)) {
        return;
    }
    running_.store(true, std::memory_order_release);
    connect_thread_ = std::thread(&BrokerClient::run, this);
}

void BrokerClient::stop() {
    {
        std::lock_guard<std::mutex> lock(socket_fd_mutex_);
        if (!running_.load(std::memory_order_acquire)) {
            return;
        }
        // Always tear down if start() ran, even when connect has not finished yet.
        running_.store(false, std::memory_order_release);
        connected_.store(false, std::memory_order_release);
        if (socket_fd_ != -1) {
            ::shutdown(socket_fd_, SHUT_RDWR);
            ::close(socket_fd_);
            socket_fd_ = -1;
        }
    }  // release socket_fd_mutex_ before join to avoid deadlock

    if (connect_thread_.joinable()) {
        connect_thread_.join();
    }

    failAllPending(std::runtime_error("BrokerClient stopped"));
}

std::future<std::shared_ptr<RequestResult>> BrokerClient::sendFrame(
    uint32_t request_id, std::vector<uint8_t> data) {
    if (!connected_.load(std::memory_order_acquire)) {
        throw std::runtime_error("BrokerClient not connected");
    }

    std::promise<std::shared_ptr<RequestResult>> promise;
    std::future<std::shared_ptr<RequestResult>> future = promise.get_future();
    {
        std::lock_guard<std::mutex> lock(request_promises_mutex_);
        request_promises_[request_id] = std::move(promise);
    }  // release before send — avoid lock-order deadlock with stop()

    std::vector<uint8_t> wire_frame;
    encode_u32(static_cast<uint32_t>(data.size()), wire_frame);
    wire_frame.insert(wire_frame.end(), data.begin(), data.end());

    const ssize_t bytes_sent =
        sendExact(socket_fd_, wire_frame.data(), wire_frame.size());
    if (bytes_sent < 0 || static_cast<size_t>(bytes_sent) != wire_frame.size()) {
        {
            std::lock_guard<std::mutex> lock(request_promises_mutex_);
            request_promises_.erase(request_id);
        }
        throw std::runtime_error("Failed to send frame");
    }
    return future;
}

uint32_t BrokerClient::generateRequestId() {
    return request_id_counter_.fetch_add(1, std::memory_order_relaxed);
}

bool BrokerClient::isConnected() const {
    return connected_.load(std::memory_order_acquire);
}

ssize_t BrokerClient::recvExact(int fd, void* buffer, size_t count) {
    auto* p = static_cast<char*>(buffer);
    size_t total = 0;
    while (total < count) {
        const ssize_t n = ::recv(fd, p + total, count - total, MSG_WAITALL);
        if (n == 0) {
            return -1;  // peer closed
        }
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);
}

ssize_t BrokerClient::sendExact(int fd, const void* buffer, size_t count) {
    // Serialize writers only; do not hold across recv/join.
    std::lock_guard<std::mutex> lock(socket_fd_mutex_);
    if (fd < 0 || socket_fd_ < 0) {
        return -1;
    }
    auto* p = static_cast<const char*>(buffer);
    size_t total = 0;
    while (total < count) {
        const ssize_t n = ::send(fd, p + total, count - total, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);
}

void BrokerClient::fulfillPromise(uint32_t request_id,
                                  std::shared_ptr<RequestResult> result) {
    std::promise<std::shared_ptr<RequestResult>> promise;
    {
        std::lock_guard<std::mutex> lock(request_promises_mutex_);
        auto it = request_promises_.find(request_id);
        if (it == request_promises_.end()) {
            return;
        }
        promise = std::move(it->second);
        request_promises_.erase(it);
    }
    promise.set_value(std::move(result));
}

void BrokerClient::failAllPending(const std::exception& ex) {
    std::map<uint32_t, std::promise<std::shared_ptr<RequestResult>>> pending;
    {
        std::lock_guard<std::mutex> lock(request_promises_mutex_);
        pending.swap(request_promises_);
    }
    for (auto& [request_id, promise] : pending) {
        (void)request_id;
        try {
            promise.set_exception(std::make_exception_ptr(ex));
        } catch (const std::future_error&) {
            // Already satisfied.
        }
    }
}

void BrokerClient::receive() {
    uint8_t len_buf[4];
    if (recvExact(socket_fd_, len_buf, sizeof(len_buf)) < 0) {
        throw std::runtime_error("Failed to read frame length");
    }
    const uint32_t frame_len =
        (static_cast<uint32_t>(len_buf[0]) << 24) |
        (static_cast<uint32_t>(len_buf[1]) << 16) |
        (static_cast<uint32_t>(len_buf[2]) << 8) |
        static_cast<uint32_t>(len_buf[3]);
    if (frame_len < 2 || frame_len > PROTOCOL_FRAME_MAX_SIZE) {
        throw std::runtime_error("Invalid frame length");
    }

    std::vector<uint8_t> frame_data(frame_len);
    if (recvExact(socket_fd_, frame_data.data(), frame_len) < 0) {
        throw std::runtime_error("Failed to read frame body");
    }

    FrameHeader frame_header;
    decode_frame_header(frame_header, frame_data);
    if (frame_header.version != PROTOCOL_VERSION) {
        throw std::runtime_error("Unsupported protocol version");
    }

    switch (frame_header.type) {
        case ProtocolFrameType::SUBSCRIBE_ACK: {
            SubscribeAck ack;
            decode_subscribe_ack(ack, frame_data);
            auto result = std::make_shared<RequestResult>();
            result->type = ProtocolFrameType::SUBSCRIBE_ACK;
            result->subscribe_ack = ack;
            fulfillPromise(ack.request_id, std::move(result));
            break;
        }
        case ProtocolFrameType::UNSUBSCRIBE_ACK: {
            UnsubscribeAck ack;
            decode_unsubscribe_ack(ack, frame_data);
            auto result = std::make_shared<RequestResult>();
            result->type = ProtocolFrameType::UNSUBSCRIBE_ACK;
            result->unsubscribe_ack = ack;
            fulfillPromise(ack.request_id, std::move(result));
            break;
        }
        case ProtocolFrameType::PUBLISH_ACK: {
            PublishAck ack;
            decode_publish_ack(ack, frame_data);
            auto result = std::make_shared<RequestResult>();
            result->type = ProtocolFrameType::PUBLISH_ACK;
            result->publish_ack = ack;
            fulfillPromise(ack.request_id, std::move(result));
            break;
        }
        case ProtocolFrameType::CLOSE_ACK: {
            CloseAck ack;
            decode_close_ack(ack, frame_data);
            auto result = std::make_shared<RequestResult>();
            result->type = ProtocolFrameType::CLOSE_ACK;
            result->close_ack = ack;
            fulfillPromise(ack.request_id, std::move(result));
            break;
        }
        case ProtocolFrameType::DELIVER: {
            DeliverMessage deliver;
            decode_deliver_message(deliver, frame_data);
            // TODO: hand off to NetworkDispatcher / FrameHandler.
            (void)deliver;
            break;
        }
        default:
            throw std::runtime_error("Unexpected frame type from broker");
    }
}

void BrokerClient::run() {
    int fd = -1;
    try {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd == -1) {
            throw std::runtime_error("Failed to create socket");
        }
        int optval = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
            throw std::runtime_error("Failed to set socket options");
        }

        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(static_cast<uint16_t>(port_));
        server_addr.sin_addr.s_addr = inet_addr(host_.c_str());
        if (connect(fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) ==
            -1) {
            throw std::runtime_error("Failed to connect to broker");
        }

        {
            std::lock_guard<std::mutex> lock(socket_fd_mutex_);
            if (!running_.load(std::memory_order_acquire)) {
                ::close(fd);
                return;
            }
            socket_fd_ = fd;
            fd = -1;  // ownership transferred
        }
        connected_.store(true, std::memory_order_release);

        while (connected_.load(std::memory_order_acquire) &&
               running_.load(std::memory_order_acquire)) {
            receive();
        }
    } catch (...) {
        if (fd != -1) {
            ::close(fd);
        }
        {
            std::lock_guard<std::mutex> lock(socket_fd_mutex_);
            if (socket_fd_ != -1) {
                ::close(socket_fd_);
                socket_fd_ = -1;
            }
        }
        connected_.store(false, std::memory_order_release);
        running_.store(false, std::memory_order_release);
        failAllPending(std::runtime_error("BrokerClient connection/receive failed"));
        // Do not rethrow: escaping the thread would call std::terminate.
    }

    connected_.store(false, std::memory_order_release);
}
