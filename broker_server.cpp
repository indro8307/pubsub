#include "broker_server.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

Subscription::Subscription(SubscriptionToken token, std::shared_ptr<Session> session)
    : token_(std::move(token)), session_(std::move(session)) {}

Subscription::~Subscription() = default;

BrokerServer::BrokerServer(MessageBroker& broker, uint16_t port)
    : broker_(broker), port_(port) {}

BrokerServer::~BrokerServer() {
    stop();
}

void BrokerServer::start() {
    running_.store(true, std::memory_order_release);
    accept_thread_ = std::thread(&BrokerServer::run, this);
}

void BrokerServer::stop() {
    running_.store(false, std::memory_order_release);
    listening_.store(false, std::memory_order_release);
    if (listen_fd_ >= 0) {
        // Wake epoll_wait / accept so run() can see running_ == false.
        ::shutdown(listen_fd_, SHUT_RDWR);
    }
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    // Close server socket
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }      
    std::map<int, std::shared_ptr<Session>> sessions;
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        sessions.swap(sessions_);
        read_bufs_.clear();
    }
    for (auto& [fd, session] : sessions) {
        (void)fd;
        if (session) {
            session->requestStop();
        }
    }
    for (auto& [fd, session] : sessions) {
        (void)fd;
        if (session) {
            session->join();
        }
    }
}

void BrokerServer::run() {
    try {
        // Create socket
        int server_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket == -1) {
            throw std::runtime_error("Failed to create socket");
        }
        listen_fd_ = server_socket;

        // Set socket options
        int optval = 1;
        if (::setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
            throw std::runtime_error("Failed to set socket options");
        }
        // Bind socket
        sockaddr_in server_addr{};
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port_);
        if (::bind(server_socket, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
            throw std::runtime_error("Failed to bind socket");
        }
        // Listen for connections
        if (::listen(server_socket, 10) == -1) {
            throw std::runtime_error("Failed to listen for connections");
        }

        int flags = ::fcntl(server_socket, F_GETFL, 0);
        if (flags == -1) {
            throw std::runtime_error("Failed to get listen socket flags");
        }
        if (::fcntl(server_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
            throw std::runtime_error("Failed to set listen socket non-blocking");
        }

        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ == -1) {
            throw std::runtime_error("Failed to create epoll instance");
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) == -1) {
            throw std::runtime_error("Failed to add listen socket to epoll");
        }

        listening_.store(true, std::memory_order_release);

        // Timeout so stop() can join even if shutdown() does not wake a listening
        // socket. We will replace this with an eventfd wakeup later.
        constexpr int kMaxEvents = 64;
        constexpr int kWaitTimeoutMs = 100;
        epoll_event events[kMaxEvents];

        while (running_.load(std::memory_order_acquire)) {
            const int nready = ::epoll_wait(epoll_fd_, events, kMaxEvents, kWaitTimeoutMs);
            if (nready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (!running_.load(std::memory_order_acquire)) {
                    break;
                }
                throw std::runtime_error("epoll_wait failed");
            }
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }

            for (int i = 0; i < nready; ++i) {
                const int fd = events[i].data.fd;
                const uint32_t ev_mask = events[i].events;

                if (fd == listen_fd_) {
                    // event received for the listen socket
                    if (ev_mask & (EPOLLERR | EPOLLHUP)) {
                        if (!running_.load(std::memory_order_acquire)) {
                            break;
                        }
                        throw std::runtime_error("Listen socket error");
                    }
                    if (ev_mask & EPOLLIN) {
                        // Level-triggered: accept until the backlog is empty (EAGAIN).
                        while (true) {
                            sockaddr_in client_addr{};
                            socklen_t client_addr_len = sizeof(client_addr);
                            int client_socket = ::accept(
                                listen_fd_,
                                reinterpret_cast<sockaddr*>(&client_addr),
                                &client_addr_len);
                            if (client_socket == -1) {
                                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                    break;
                                }
                                if (errno == EINTR) {
                                    continue;
                                }
                                if (!running_.load(std::memory_order_acquire)) {
                                    break;
                                }
                                throw std::runtime_error("Failed to accept connection");
                            }
                            handleClientConnection(client_socket);
                        }
                    }
                }
                else
                {
                    // event received for a client fd
                    // Client fd: already in epoll. Do not ignore EPOLLIN (level-triggered
                    // would spin). Read available bytes; parse/handle frames in a later step.
                    if (ev_mask & EPOLLIN) {
                        handleClientReadable(fd);
                    }
                    if (ev_mask & EPOLLOUT) {
                        handleClientWritable(fd);
                    }
                    if (ev_mask & (EPOLLERR | EPOLLHUP)) {
                        closeClient(fd);
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in accept loop: " << e.what() << std::endl;
    }
    listening_.store(false, std::memory_order_release);
}

void BrokerServer::handleClientConnection(int client_socket) {
    int flags = ::fcntl(client_socket, F_GETFL, 0);
    if (flags == -1 || ::fcntl(client_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
        ::close(client_socket);
        return;
    }

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = client_socket;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_socket, &ev) == -1) {
        ::close(client_socket);
        return;
    }

    auto session = std::make_shared<Session>(client_socket);
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        sessions_[client_socket] = std::move(session);
    }
}


// Read whatever is in the socket (batched recv), append it to pending for this
// client, then peel off complete frames. One recv can contain many frames, or
// only part of one frame.
//
// For each recv:
//       - n > 0: append bytes to pending, then loop:
//              if pending is smaller than 4 bytes, we do not have the length yet.
//                    break and wait for the next EPOLLIN.
//              decode frame_len from the first 4 bytes.
//              if frame_len is invalid, close this client.
//              if pending is smaller than 4 + frame_len, we do not have the
//                    full frame yet. break and wait for the next EPOLLIN.
//              copy the next frame_len bytes (header + body, no length prefix)
//                    into frame_data, decode header, handle the type.
//              erase those 4 + frame_len bytes from the front of pending and
//                    try the next frame in the same pending buffer.
//       - after we finish parsing, continue the recv loop (more data may already
//         be in the socket). We only stop on EAGAIN, peer close, or error.
//       - n == 0: peer closed. close this client.
//       - n == -1 EAGAIN / EWOULDBLOCK: no more data right now. return.
//       - n == -1 EINTR: retry recv.
//       - n == -1 anything else: close this client.
//
// TODO: pending.erase(begin, begin + frame_len + 4) after every frame shifts all
// leftover bytes. If one recv holds many small frames that is O(m*n) copies.
// where m is the pending size and n is the number of frames.
// this is a big overhead for many small frames received together since n is large.
// With few big frames, the overhead is much less because the number of copies decreases (so n is small, m stays same for both cases)
// Later: walk pending with an offset and erase (or clear) once at the end or at regular intervals.

void BrokerServer::handleClientReadable(int client_fd) {
    uint8_t buf[4096];
    while (true) {
        const ssize_t n = ::recv(client_fd, buf, sizeof(buf), 0);
        if (n > 0) {
            auto& pending = read_bufs_[client_fd];
            pending.insert(pending.end(), buf, buf + n);
            while(pending.size() > 0) {
                // check if we can decode a frame length
                if(pending.size() < 4) {
                    break;
                }
                uint32_t frame_len = 0;
                frame_len = decode_uint32(pending.data(), 0);
                if (frame_len < 2 || frame_len > PROTOCOL_FRAME_MAX_SIZE) {
                    closeClient(client_fd);
                    return;
                }

                // check if we can read the frame payload
                if(pending.size() < frame_len+4) {
                    break;
                }

                // copy the frame data to a vector
                std::vector<uint8_t> frame_data(pending.begin()+4, pending.begin()+4+frame_len);

                // decode the frame header first
                FrameHeader frame_header;
                decode_frame_header(frame_header, frame_data);
                if (frame_header.version != PROTOCOL_VERSION) {
                    closeClient(client_fd);
                    return;
                }

                switch (frame_header.type) {
                    case ProtocolFrameType::PUBLISH: {
                        // find the session for the client fd for later use
                        auto session_it = sessions_.find(client_fd);
                        if (session_it == sessions_.end()) {
                            closeClient(client_fd);
                            return;
                        }
                        std::shared_ptr<Session> publisher_session = session_it->second;
                        // decode the publish request
                        PublishRequest publish_request;
                        decode_publish_request(publish_request, frame_data);
                        if (publish_request.topic.empty()) {
                            closeClient(client_fd);
                            return;
                        }

                        auto send_publish_ack = [&](PublishResult result) {
                            PublishAck ack;
                            ack.request_id = publish_request.request_id;
                            ack.result = result;
                            std::vector<uint8_t> body;
                            encode_publish_ack(ack, body);
                            buildAndSendFrame(client_fd, ProtocolFrameType::PUBLISH_ACK, body,
                                              publisher_session);
                        };

                        // find the subscriptions for the topic
                        auto topic_it = subscriptions_by_topics_.find(publish_request.topic);
                        if (topic_it == subscriptions_by_topics_.end() ||
                            topic_it->second.empty()) {
                            send_publish_ack(PublishResult::NO_SUBSCRIBERS);
                            break;
                        }

                        // Sequence from MessageBroker only; do not enqueue into group queues.
                        uint64_t sequence = 0;
                        if (!broker_.allocateSequence(publish_request.topic, sequence)) {
                            send_publish_ack(PublishResult::NO_SUBSCRIBERS);
                            break;
                        }

                        DeliverMessage deliver;
                        deliver.topic = publish_request.topic;
                        deliver.sequence = sequence;
                        deliver.payload = publish_request.payload;

                        for (auto& subscription : topic_it->second) {
                            deliver.subscription_id = subscription->token_.id;
                            std::vector<uint8_t> body;
                            encode_deliver_message(deliver, body);
                            const int sub_fd = subscription->session_->fd();
                            buildAndSendFrame(sub_fd, ProtocolFrameType::DELIVER, body,
                                              subscription->session_);
                        }

                        send_publish_ack(PublishResult::ACCEPTED);
                        break;
                    }
                    case ProtocolFrameType::SUBSCRIBE: {
                        // look for an entry in the sessions_ map with the client fd as the key.
                        // Ideally it should be found because the tcp connection is already established.
                        // If not found, close the client.
                        auto it = sessions_.find(client_fd);
                        if (it == sessions_.end()) {
                            closeClient(client_fd);
                            return;
                        }
                        // decode the subscribe request
                        SubscribeRequest subscribe_request;
                        decode_subscribe_request(subscribe_request, frame_data);
                        if (subscribe_request.topic.empty()) {
                            closeClient(client_fd);
                            return;
                        }

                        // subscribe to the topic
                        SubscriptionToken token = broker_.subscribe(subscribe_request.topic, subscribe_request.group);
                        // create a new subscription
                        auto subscription = std::make_unique<Subscription>(token, it->second);
                        // add the subscription to the topic
                        subscriptions_by_topics_[subscribe_request.topic].push_back(std::move(subscription));

                        // generate a subscribe ack
                        SubscribeAck ack;
                        ack.request_id = subscribe_request.request_id;
                        ack.subscription_id = token.id;
                    
                        // encode the subscribe ack
                        std::vector<uint8_t> body;
                        encode_subscribe_ack(ack, body);
                    
                        // enqueue the frame into the write buffer of the session
                        buildAndSendFrame(client_fd, ProtocolFrameType::SUBSCRIBE_ACK, body, it->second);
                        break;
                    }
                    case ProtocolFrameType::UNSUBSCRIBE: {
                        handleUnsubscribe(frame_data);
                        //UnsubscribeRequest unsubscribe_request;
                        //decode_unsubscribe_request(unsubscribe_request, frame_data);
                        //handleUnsubscribeFrame(unsubscribe_frame);
                        break;
                    }
                    default: {
                        closeClient(client_fd);
                        return;
                    }
                }
                // slice away the frame payload from the pending data
                pending.erase(pending.begin(), pending.begin() + frame_len+4);
            }
            continue;
        }
        if (n == 0) {
            closeClient(client_fd);
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        closeClient(client_fd);
        return;
    }
}

void BrokerServer::handleClientWritable(int client_fd) {
    auto it = sessions_.find(client_fd);
    if (it == sessions_.end() || !it->second) {
        return;
    }
    std::shared_ptr<Session> session = it->second;
    FlushResult result = session->flush();
    if (result == FlushResult::FLUSH_EPOLLOUT) {
        // fd is already in epoll with EPOLLIN; MOD so we keep reading and
        // also wake when the socket can accept more writes.
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.fd = client_fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
    } else if (result == FlushResult::FLUSH_SUCCESS) {
        // Queue drained; drop EPOLLOUT so we do not spin on a writable socket.
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
    } else if (result == FlushResult::FLUSH_CLOSE) {
        closeClient(client_fd);
    }
}

void BrokerServer::closeClient(int client_fd) {
    if (epoll_fd_ >= 0) {
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, client_fd, nullptr);
    }
    std::shared_ptr<Session> session;
    std::vector<SubscriptionToken> tokens_to_unsub;
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        auto it = sessions_.find(client_fd);
        if (it == sessions_.end()) {
            return;
        }
        session = std::move(it->second);
        sessions_.erase(it);
        read_bufs_.erase(client_fd);

        for (auto topic_it = subscriptions_by_topics_.begin();
             topic_it != subscriptions_by_topics_.end();) {
            auto& subs = topic_it->second;
            for (auto sub_it = subs.begin(); sub_it != subs.end();) {
                if ((*sub_it)->session_ == session) {
                    tokens_to_unsub.push_back((*sub_it)->token_);
                    sub_it = subs.erase(sub_it);
                } else {
                    ++sub_it;
                }
            }
            if (subs.empty()) {
                topic_it = subscriptions_by_topics_.erase(topic_it);
            } else {
                ++topic_it;
            }
        }
    }
    for (const auto& token : tokens_to_unsub) {
        if (token.valid()) {
            broker_.unsubscribe(token);
        }
    }
}

void BrokerServer::reapFinishedSessions() {
    std::lock_guard<std::mutex> lock(sessions_mtx_);
    for (auto it = sessions_.begin(); it != sessions_.end();) {
        if (it->second && !it->second->isRunning()) {
            read_bufs_.erase(it->first);
            it = sessions_.erase(it);
        } else {
            ++it;
        }
    }
}

void BrokerServer::buildAndSendFrame(int client_fd, ProtocolFrameType type, const std::vector<uint8_t>& body, 
                                           std::shared_ptr<Session> session) 
{
    FrameHeader header;
    header.version = PROTOCOL_VERSION;
    header.type = type;

    std::vector<uint8_t> frame;
    encode_frame_header(header, frame);
    frame.insert(frame.end(), body.begin(), body.end());   // copy 1 

    if (frame.size() > PROTOCOL_FRAME_MAX_SIZE) {
        throw std::runtime_error("Outbound frame exceeds PROTOCOL_FRAME_MAX_SIZE");
    }

    std::vector<uint8_t> wire;
    encode_u32(static_cast<uint32_t>(frame.size()), wire);
    wire.insert(wire.end(), frame.begin(), frame.end());   // copy 2 copy from frame to wire.

    session->enqueueFrame(type, wire);
    FlushResult result = session->flush();
    if (result == FlushResult::FLUSH_EPOLLOUT) {
        // fd is already in epoll with EPOLLIN; MOD so we keep reading and
        // also wake when the socket can accept more writes.
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT;
        ev.data.fd = client_fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
    } else if (result == FlushResult::FLUSH_SUCCESS) {
        // Queue drained; drop EPOLLOUT so we do not spin on a writable socket.
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, client_fd, &ev);
    } else if (result == FlushResult::FLUSH_CLOSE) {
        closeClient(client_fd);
    }
}