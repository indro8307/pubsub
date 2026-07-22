#include "session.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace {

bool readExact(int fd, void* buf, size_t n) {
    auto* p = static_cast<uint8_t*>(buf);
    size_t got = 0;
    while (got < n) {
        const ssize_t nread = ::read(fd, p + got, n - got);
        if (nread == 0) {
            return false; // peer closed
        }
        if (nread < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        got += static_cast<size_t>(nread);
    }
    return true;
}

bool writeExact(int fd, const void* buf, size_t n) {
    auto* p = static_cast<const uint8_t*>(buf);
    size_t sent = 0;
    while (sent < n) {
        const ssize_t nwritten = ::write(fd, p + sent, n - sent);
        if (nwritten < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        sent += static_cast<size_t>(nwritten);
    }
    return true;
}

}  // namespace

Session::Session(int client_fd, MessageBroker& broker)
    : client_fd_(client_fd), broker_(broker) {}

Session::~Session() {
    requestStop();
    join();
    if (client_fd_ >= 0) {
        ::close(client_fd_);
        client_fd_ = -1;
    }
}

void Session::start() {
    if (reader_.joinable()) {
        throw std::logic_error("Session::start() called while reader is already running");
    }
    running_.store(true, std::memory_order_release);
    reader_ = std::thread(&Session::run, this);
}

void Session::join() {
    if (reader_.joinable()) {
        reader_.join();
    }
}

void Session::requestStop() {
    running_.store(false, std::memory_order_release);
    if (client_fd_ >= 0) {
        // Wake a blocking read; ignore errors if already closed.
        ::shutdown(client_fd_, SHUT_RDWR);
    }
}

void Session::run() {
    try {
        while (running_.load(std::memory_order_acquire)) {
            uint8_t len_buf[4];
            if (!readExact(client_fd_, len_buf, sizeof(len_buf))) {
                break;
            }
            const uint32_t frame_len =
                (static_cast<uint32_t>(len_buf[0]) << 24) |
                (static_cast<uint32_t>(len_buf[1]) << 16) |
                (static_cast<uint32_t>(len_buf[2]) << 8) |
                static_cast<uint32_t>(len_buf[3]);
            if (frame_len < 2 || frame_len > PROTOCOL_FRAME_MAX_SIZE) {
                break;
            }

            std::vector<uint8_t> frame(frame_len);
            if (!readExact(client_fd_, frame.data(), frame_len)) {
                break;
            }

            FrameHeader header;
            decode_frame_header(header, frame);
            if (header.version != PROTOCOL_VERSION) {
                break;
            }
            handleFrame(header, frame);
        }
    } catch (const std::exception&) {
        // Malformed frame or broker error: drop the connection.
    }

    cleanupSubscriptions();
    running_.store(false, std::memory_order_release);

    if (client_fd_ >= 0) {
        ::close(client_fd_);
        client_fd_ = -1;
    }
}

void Session::handleFrame(const FrameHeader& header, std::vector<uint8_t>& frame) {
    switch (header.type) {
        case ProtocolFrameType::SUBSCRIBE:
            handleSubscribe(frame);
            break;
        case ProtocolFrameType::UNSUBSCRIBE:
            handleUnsubscribe(frame);
            break;
        case ProtocolFrameType::PUBLISH:
            handlePublish(frame);
            break;
        case ProtocolFrameType::CLOSE:
            handleClose(frame);
            break;
        default:
            // Unknown or server-only type from client: close the session.
            running_.store(false, std::memory_order_release);
            break;
    }
}

void Session::handleSubscribe(std::vector<uint8_t>& frame) {
    SubscribeRequest request;
    decode_subscribe_request(request, frame);

    SubscriptionToken token = broker_.subscribe(request.topic, request.group);
    {
        std::lock_guard<std::mutex> lock(subs_mtx_);
        subscriptions_[token.id] = token;
    }

    SubscribeAck ack;
    ack.request_id = request.request_id;
    ack.subscription_id = token.id;

    std::vector<uint8_t> body;
    encode_subscribe_ack(ack, body);
    sendFrame(ProtocolFrameType::SUBSCRIBE_ACK, body);
}

void Session::handleUnsubscribe(std::vector<uint8_t>& frame) {
    UnsubscribeRequest request;
    decode_unsubscribe_request(request, frame);

    SubscriptionToken token;
    {
        std::lock_guard<std::mutex> lock(subs_mtx_);
        auto it = subscriptions_.find(request.subscription_id);
        if (it != subscriptions_.end()) {
            token = it->second;
            subscriptions_.erase(it);
        }
    }
    if (token.valid()) {
        broker_.unsubscribe(token);
    }

    UnsubscribeAck ack;
    ack.request_id = request.request_id;
    ack.subscription_id = request.subscription_id;

    std::vector<uint8_t> body;
    encode_unsubscribe_ack(ack, body);
    sendFrame(ProtocolFrameType::UNSUBSCRIBE_ACK, body);
}

void Session::handlePublish(std::vector<uint8_t>& frame) {
    PublishRequest request;
    decode_publish_request(request, frame);

    Message msg(0);
    if (!request.payload.empty()) {
        msg.setPayload(request.payload.data(), request.payload.size());
    }
    const bool accepted = broker_.publish(request.topic, msg);

    PublishAck ack;
    ack.request_id = request.request_id;
    ack.result = accepted ? PublishResult::ACCEPTED : PublishResult::NO_SUBSCRIBERS;

    std::vector<uint8_t> body;
    encode_publish_ack(ack, body);
    sendFrame(ProtocolFrameType::PUBLISH_ACK, body);
}

void Session::handleClose(std::vector<uint8_t>& frame) {
    CloseRequest request;
    decode_close_request(request, frame);

    cleanupSubscriptions();

    CloseAck ack;
    ack.request_id = request.request_id;

    std::vector<uint8_t> body;
    encode_close_ack(ack, body);
    sendFrame(ProtocolFrameType::CLOSE_ACK, body);

    running_.store(false, std::memory_order_release);
}

void Session::cleanupSubscriptions() {
    std::map<uint64_t, SubscriptionToken> to_remove;
    {
        std::lock_guard<std::mutex> lock(subs_mtx_);
        to_remove.swap(subscriptions_);
    }
    for (auto& [id, token] : to_remove) {
        (void)id;
        if (token.valid()) {
            broker_.unsubscribe(token);
        }
    }
}

void Session::sendFrame(ProtocolFrameType type, const std::vector<uint8_t>& body) {
    FrameHeader header;
    header.version = PROTOCOL_VERSION;
    header.type = type;

    std::vector<uint8_t> frame;
    encode_frame_header(header, frame);
    frame.insert(frame.end(), body.begin(), body.end());

    if (frame.size() > PROTOCOL_FRAME_MAX_SIZE) {
        throw std::runtime_error("Outbound frame exceeds PROTOCOL_FRAME_MAX_SIZE");
    }

    std::vector<uint8_t> wire;
    encode_u32(static_cast<uint32_t>(frame.size()), wire);
    wire.insert(wire.end(), frame.begin(), frame.end());

    std::lock_guard<std::mutex> lock(write_mtx_);
    if (client_fd_ < 0) {
        return;
    }
    if (!writeExact(client_fd_, wire.data(), wire.size())) {
        running_.store(false, std::memory_order_release);
    }
}
