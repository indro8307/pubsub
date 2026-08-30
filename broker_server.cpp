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

Group::Group() : index_(0), group_name_("") {}
Group::Group(const std::string& group_name) : index_(0), group_name_(group_name) {}

Group::~Group()
{
    subscriptions_.clear();
}

std::shared_ptr<Subscription> Group::getNextSubscription() const {
    if (subscriptions_.empty()) {
        return nullptr;
    }
    if (index_ < 0 || index_ >= static_cast<int>(subscriptions_.size())) {
        index_ = 0;
    }
    // Return current member, then advance (so the first pick is subscriptions_[0]).
    std::shared_ptr<Subscription> subscription = subscriptions_[static_cast<size_t>(index_)];
    index_ = (index_ + 1) % static_cast<int>(subscriptions_.size());
    return subscription;
}

void Group::addSubscription(std::shared_ptr<Subscription> subscription) {
    subscriptions_.push_back(subscription);
}

void Group::removeSubscription(uint64_t subscription_id) {
    for (auto it = subscriptions_.begin(); it != subscriptions_.end(); ++it) {
        if ((*it)->token_.id == subscription_id) {
            subscriptions_.erase(it);
            break;
        }
    }
}

void BrokerServer::start() {
    running_.store(true, std::memory_order_release);
    accept_thread_ = std::thread(&BrokerServer::run, this);
}

void BrokerServer::stop() {
    // set the running flag to false
    running_.store(false, std::memory_order_release);
    // set the listening flag to false
    listening_.store(false, std::memory_order_release);
    // shutdown the listen socket
    if (listen_fd_ >= 0) {
        // Wake epoll_wait / accept so run() can see running_ == false.
        ::shutdown(listen_fd_, SHUT_RDWR);
    }
    // join the accept thread
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    // close the listen socket
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    // close the epoll fd
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
    // clear the sessions map
    std::map<int, std::shared_ptr<Session>> sessions;
    std::vector<SubscriptionToken> tokens_to_unsub;
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        // copy all the sessions to the local map and clear the BrokerServer::sessions_ map
        sessions.swap(sessions_);
        // clear the read buffers
        read_bufs_.clear();

        // iterate over all the sessions and clear the subscription ids
        for (auto& [fd, session] : sessions) {
            (void)fd;
            if (!session) {
                continue;
            }
            // iterate over all the subscription ids and add the tokens to the tokens_to_unsub vector for unsubscribing from the broker
            for (uint64_t id : session->subscriptionIds()) {
                auto sub_it = subscriptions_by_id_.find(id);
                if (sub_it != subscriptions_by_id_.end()) {
                    tokens_to_unsub.push_back(sub_it->second->token_);
                }
            }
            // clear the subscription ids
            session->clearSubscriptionIds();
        }
        // clear the topic and id maps
        subscriptions_by_topics_groups_.clear();
        subscriptions_by_id_.clear();
    }
    // unsubscribe from all the tokens
    for (const auto& token : tokens_to_unsub) {
        if (token.valid()) {
            broker_.unsubscribe(token);
        }
    }
    // iterate over all the sessions and close the fd and clear the session
    for (auto& [fd, session] : sessions) {
        if (fd >= 0 && session) {
            ::shutdown(fd, SHUT_RDWR);
            ::close(fd);
            session->clearFd();
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
                frame_len = decode_u32(pending, 0);
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
                    case ProtocolFrameType::PUBLISH:
                        if (!handlePublish(client_fd, frame_data)) {
                            return;
                        }
                        break;
                    case ProtocolFrameType::SUBSCRIBE:
                        if (!handleSubscribe(client_fd, frame_data)) {
                            return;
                        }
                        break;
                    case ProtocolFrameType::UNSUBSCRIBE:
                        if (!handleUnsubscribe(client_fd, frame_data)) {
                            return;
                        }
                        break;
                    case ProtocolFrameType::CLOSE:
                        if (!handleClose(client_fd, frame_data)) {
                            return;
                        }
                        break;
                    default:
                        closeClient(client_fd);
                        return;
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

bool BrokerServer::handlePublish(int client_fd, std::vector<uint8_t>& frame_data) {
    // find the session for the client fd for later use
    auto session_it = sessions_.find(client_fd);
    if (session_it == sessions_.end()) {
        closeClient(client_fd);
        return false;
    }
    std::shared_ptr<Session> publisher_session = session_it->second;
    // decode the publish request
    PublishRequest publish_request;
    decode_publish_request(publish_request, frame_data);
    if (publish_request.topic.empty()) {
        closeClient(client_fd);
        return false;
    }

    std::vector<int> fds_to_close;
    auto send_publish_ack = [&](PublishResult result) {
        PublishAck ack;
        ack.request_id = publish_request.request_id;
        ack.result = result;
        std::vector<uint8_t> body;
        encode_publish_ack(ack, body);
        if (buildAndSendFrame(client_fd, ProtocolFrameType::PUBLISH_ACK, body,
                              publisher_session) == FlushResult::FLUSH_CLOSE) {
            fds_to_close.push_back(client_fd);
        }
    };

    auto flush_close_fds = [&](){
        for(auto fd : fds_to_close) {
            closeClient(fd);
        }
    };

    // find the subscriptions for the topic
    std::vector<std::shared_ptr<Group>> groups;
    auto groups_it = subscriptions_by_topics_groups_.find(publish_request.topic);
    if (groups_it == subscriptions_by_topics_groups_.end()) {
        send_publish_ack(PublishResult::NO_SUBSCRIBERS);
        flush_close_fds();
        return sessions_.find(client_fd) != sessions_.end();
    }
    groups = groups_it->second;
    if (groups.empty()) {  // ideally this should not happen, since groups are created when a subscription is added
        send_publish_ack(PublishResult::NO_SUBSCRIBERS);
        flush_close_fds();
        return sessions_.find(client_fd) != sessions_.end();
    }

    // Sequence from MessageBroker only; do not enqueue into group queues.
    uint64_t sequence = 0;
    if (!broker_.allocateSequence(publish_request.topic, sequence)) {
        send_publish_ack(PublishResult::NO_SUBSCRIBERS);
        flush_close_fds();
        return sessions_.find(client_fd) != sessions_.end();
    }

    // ACK before fan-out so the publisher RTT is not held by DELIVER writes
    // (same critical-path shape as enqueue-then-ack in the threaded design).
    send_publish_ack(PublishResult::ACCEPTED);

    DeliverMessage deliver;
    deliver.topic = publish_request.topic;
    deliver.sequence = sequence;
    deliver.payload = publish_request.payload;

    for (auto& group : groups) {
        auto subscription = group->getNextSubscription();
        if (subscription == nullptr) {
            continue;
        }
        deliver.subscription_id = subscription->token_.id;
        std::vector<uint8_t> body;
        encode_deliver_message(deliver, body);
        const int sub_fd = subscription->session_->fd();
        if (sub_fd < 0) {
            continue;
        }
        if (buildAndSendFrame(sub_fd, ProtocolFrameType::DELIVER, body,
                              subscription->session_) == FlushResult::FLUSH_CLOSE) {
            fds_to_close.push_back(sub_fd);
        }
    }

    flush_close_fds();
    return sessions_.find(client_fd) != sessions_.end();
}

bool BrokerServer::handleSubscribe(int client_fd, std::vector<uint8_t>& frame_data) {
    // look for an entry in the sessions_ map with the client fd as the key.
    // Ideally it should be found because the tcp connection is already established.
    // If not found, close the client.
    auto it = sessions_.find(client_fd);
    if (it == sessions_.end()) {
        closeClient(client_fd);
        return false;
    }
    // decode the subscribe request
    SubscribeRequest subscribe_request;
    decode_subscribe_request(subscribe_request, frame_data);
    if (subscribe_request.topic.empty()) {
        closeClient(client_fd);
        return false;
    }

    // subscribe to the topic
    SubscriptionToken token = broker_.subscribe(subscribe_request.topic, subscribe_request.group);
    // create a new subscription
    auto subscription = std::make_shared<Subscription>(token, it->second);
    // Check if topic already exists
    auto groups_it = subscriptions_by_topics_groups_.find(subscribe_request.topic);
    if (groups_it == subscriptions_by_topics_groups_.end()) {
        // This means the topic does not exist yet.
        // create a new topic and group
        auto group = std::make_shared<Group>(subscribe_request.group);
        // add the subscription to the group
        group->addSubscription(subscription);
        // add the group to the topic map
        subscriptions_by_topics_groups_[subscribe_request.topic].push_back(group);
    } else {
        // topic exists. Check if the group exists in the topic.
        std::vector<std::shared_ptr<Group>>& groups = groups_it->second;
        auto group = std::find_if(groups.begin(), groups.end(), [&](const std::shared_ptr<Group>& group) {
            return group->group_name_ == subscribe_request.group;
        });
        if (group != groups.end()) {
            // group exists. Add the subscription to the group
            (*group)->addSubscription(subscription);
        } else {
            // group does not exist. create a new group
            auto new_group = std::make_shared<Group>(subscribe_request.group);
            new_group->addSubscription(subscription);
            groups.push_back(new_group);
        }
    }
    // add the subscription to the id map
    subscriptions_by_id_[token.id] = subscription;
    // add the subscription id to the session
    it->second->addSubscriptionId(token.id);

    // generate a subscribe ack
    SubscribeAck ack;
    ack.request_id = subscribe_request.request_id;
    ack.subscription_id = token.id;

    // encode the subscribe ack
    std::vector<uint8_t> body;
    encode_subscribe_ack(ack, body);

    // enqueue the frame into the write buffer of the session
    if (buildAndSendFrame(client_fd, ProtocolFrameType::SUBSCRIBE_ACK, body,
                          it->second) == FlushResult::FLUSH_CLOSE) {
        closeClient(client_fd);
        return false;
    }
    return true;
}

bool BrokerServer::handleUnsubscribe(int client_fd, std::vector<uint8_t>& frame_data) {
    // find the session for the client fd for later use
    auto session_it = sessions_.find(client_fd);
    if (session_it == sessions_.end()) {
        closeClient(client_fd);
        return false;
    }
    std::shared_ptr<Session> subscriber_session = session_it->second;

    // decode the unsubscribe request
    UnsubscribeRequest unsubscribe_request;
    decode_unsubscribe_request(unsubscribe_request, frame_data);

    auto send_unsubscribe_ack = [&]() -> bool {
        UnsubscribeAck ack;
        ack.request_id = unsubscribe_request.request_id;
        ack.subscription_id = unsubscribe_request.subscription_id;
        std::vector<uint8_t> body;
        encode_unsubscribe_ack(ack, body);
        if (buildAndSendFrame(client_fd, ProtocolFrameType::UNSUBSCRIBE_ACK, body,
                              subscriber_session) == FlushResult::FLUSH_CLOSE) {
            closeClient(client_fd);
            return false;
        }
        return true;
    };

    // id 0 / unknown id: protocol no-op that still acks (do not close)
    if (unsubscribe_request.subscription_id == 0) {
        return send_unsubscribe_ack();
    }

    // find the subscription from the id map
    auto subscription_it = subscriptions_by_id_.find(unsubscribe_request.subscription_id);
    if (subscription_it == subscriptions_by_id_.end()) {
        return send_unsubscribe_ack();
    }
    std::shared_ptr<Subscription> subscription = subscription_it->second;

    // another client's subscription id: do not unsubscribe them
    if (subscription->session_ != subscriber_session) {
        return send_unsubscribe_ack();
    }

    // unsubscribe from the topic
    broker_.unsubscribe(subscription->token_);

    // delete the subscription from the matching (topic, group) only
    auto groups_it = subscriptions_by_topics_groups_.find(subscription->token_.topic);
    if (groups_it != subscriptions_by_topics_groups_.end()) {
        std::vector<std::shared_ptr<Group>>& groups = groups_it->second;
        for (auto it = groups.begin(); it != groups.end(); ++it) {
            if ((*it)->group_name_ != subscription->token_.group) {
                continue;
            }
            (*it)->removeSubscription(unsubscribe_request.subscription_id);
            if ((*it)->subscriptions_.empty()) {
                groups.erase(it);
            }
            break;
        }
        if (groups.empty()) {
            subscriptions_by_topics_groups_.erase(groups_it);
        }
    }

    // delete the subscription from the id map
    subscriptions_by_id_.erase(subscription_it);
    subscriber_session->removeSubscriptionId(unsubscribe_request.subscription_id);

    // generate an unsubscribe ack
    return send_unsubscribe_ack();
}

bool BrokerServer::handleClose(int client_fd, std::vector<uint8_t>& frame_data) {
    // find the session for the client fd for later use
    auto session_it = sessions_.find(client_fd);
    if (session_it == sessions_.end()) {
        closeClient(client_fd);
        return false;
    }
    std::shared_ptr<Session> session = session_it->second;
    // decode the close request
    CloseRequest close_request;
    decode_close_request(close_request, frame_data);

    // generate a close ack
    // The close ack may not be received by the client
    // if the buildAndSendFrame() cant flush the entire frame in one go.
    // We can live with this because the client will close the connection anyway.
    CloseAck ack;
    ack.request_id = close_request.request_id;
    // encode the close ack
    std::vector<uint8_t> body;
    encode_close_ack(ack, body);
    buildAndSendFrame(client_fd, ProtocolFrameType::CLOSE_ACK, body, session);
    closeClient(client_fd);
    return false;
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

        for (uint64_t id : session->subscriptionIds()) {
            auto subscription_it = subscriptions_by_id_.find(id);
            if (subscription_it == subscriptions_by_id_.end()) {
                continue;
            }
            std::shared_ptr<Subscription> subscription = subscription_it->second;
            tokens_to_unsub.push_back(subscription->token_);

            // delete the subscription from the matching (topic, group) only
            auto groups_it = subscriptions_by_topics_groups_.find(subscription->token_.topic);
            if (groups_it != subscriptions_by_topics_groups_.end()) {
                std::vector<std::shared_ptr<Group>>& groups = groups_it->second;
                for (auto git = groups.begin(); git != groups.end(); ++git) {
                    if ((*git)->group_name_ != subscription->token_.group) {
                        continue;
                    }
                    (*git)->removeSubscription(id);
                    if ((*git)->subscriptions_.empty()) {
                        groups.erase(git);
                    }
                    break;
                }
                if (groups.empty()) {
                    subscriptions_by_topics_groups_.erase(groups_it);
                }
            }
            subscriptions_by_id_.erase(subscription_it);
        }
        session->clearSubscriptionIds();
    }
    for (const auto& token : tokens_to_unsub) {
        if (token.valid()) {
            broker_.unsubscribe(token);
        }
    }
    if (client_fd >= 0) {
        ::shutdown(client_fd, SHUT_RDWR);
        ::close(client_fd);
    }
    if (session) {
        session->clearFd();
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

FlushResult BrokerServer::buildAndSendFrame(int client_fd, ProtocolFrameType type, const std::vector<uint8_t>& body, 
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
    }
    // FLUSH_CLOSE: do not closeClient here — caller must, so we never reenter
    // subscription-map mutation from mid fan-out / mid-handler.
    return result;
}