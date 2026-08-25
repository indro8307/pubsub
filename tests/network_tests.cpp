#include <gtest/gtest.h>

#include "broker_client.h"
#include "broker_server.h"
#include "dispatcher.h"
#include "message_broker.h"
#include "protocol_frame.h"
#include "test_helpers.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

using namespace std::chrono_literals;
using test_helpers::waitUntil;

namespace {

constexpr const char* kHost = "127.0.0.1";
constexpr const char* kTopic = "orders";

uint16_t testPort() {
    return static_cast<uint16_t>(21000 + (::getpid() % 1000));
}

// Accepts TCP connections but never replies — keeps sendFrame futures pending.
class AcceptOnlyServer {
public:
    explicit AcceptOnlyServer(uint16_t port) : port_(port) {}
    ~AcceptOnlyServer() { stop(); }

    void start() {
        running_.store(true, std::memory_order_release);
        thread_ = std::thread([this] { acceptLoop(); });
    }

    void stop() {
        running_.store(false, std::memory_order_release);
        listening_.store(false, std::memory_order_release);
        if (listen_fd_ >= 0) {
            ::shutdown(listen_fd_, SHUT_RDWR);
        }
        if (thread_.joinable()) {
            thread_.join();
        }
        for (int fd : client_fds_) {
            if (fd >= 0) {
                ::shutdown(fd, SHUT_RDWR);
                ::close(fd);
            }
        }
        client_fds_.clear();
        if (listen_fd_ >= 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    bool isListening() const { return listening_.load(std::memory_order_acquire); }

private:
    void acceptLoop() {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return;
        }
        int optval = 1;
        ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 ||
            ::listen(listen_fd_, 8) < 0) {
            ::close(listen_fd_);
            listen_fd_ = -1;
            return;
        }
        listening_.store(true, std::memory_order_release);

        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            const int client_fd =
                ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &len);
            if (client_fd < 0) {
                break;
            }
            client_fds_.push_back(client_fd);
        }
        listening_.store(false, std::memory_order_release);
    }

    uint16_t port_;
    int listen_fd_ = -1;
    std::vector<int> client_fds_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<bool> listening_{false};
};

bool waitForOneRunningSession(BrokerServer& server) {
    return waitUntil(
        [&] {
            const auto sessions = server.sessions();
            return sessions.size() == 1 && sessions[0] && sessions[0]->isRunning();
        },
        2s);
}

bool waitForSessionCount(BrokerServer& server, size_t n) {
    return waitUntil(
        [&] { return server.sessions().size() == n; },
        2s);
}

size_t runningSessionCount(BrokerServer& server) {
    size_t n = 0;
    for (const auto& session : server.sessions()) {
        if (session && session->isRunning()) {
            ++n;
        }
    }
    return n;
}

bool waitForRunningSessionCount(BrokerServer& server, size_t n) {
    return waitUntil(
        [&] { return runningSessionCount(server) == n; },
        2s);
}

std::string payloadOf(const BrokerMessage& msg) {
    return std::string(reinterpret_cast<const char*>(msg.payload().getPayload()),
                       msg.payload().getSize());
}

std::vector<uint8_t> encodeSubscribeFrame(uint32_t request_id,
                                          const std::string& topic,
                                          const std::string& group) {
    std::vector<uint8_t> buffer;
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::SUBSCRIBE};
    encode_frame_header(header, buffer);
    SubscribeRequest request{request_id, topic, group};
    encode_subscribe_request(request, buffer);
    return buffer;
}

std::vector<uint8_t> encodeCloseFrame(uint32_t request_id) {
    std::vector<uint8_t> buffer;
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::CLOSE};
    encode_frame_header(header, buffer);
    CloseRequest request{request_id};
    encode_close_request(request, buffer);
    return buffer;
}

int connectRawTcp(const char* host, uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = ::inet_addr(host);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

bool writeAll(int fd, const void* data, size_t n) {
    auto* p = static_cast<const uint8_t*>(data);
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

bool sendRawFrame(int fd, const std::vector<uint8_t>& frame) {
    std::vector<uint8_t> wire;
    encode_u32(static_cast<uint32_t>(frame.size()), wire);
    wire.insert(wire.end(), frame.begin(), frame.end());
    return writeAll(fd, wire.data(), wire.size());
}

}  // namespace

TEST(NetworkTests, ClientConnectsToServer) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s))
        << "BrokerServer did not start listening on port " << port;

    BrokerClient client(kHost, static_cast<int>(port));
    client.start();
    ASSERT_TRUE(waitUntil([&] { return client.isConnected(); }, 2s))
        << "BrokerClient failed to connect to BrokerServer";

    ASSERT_TRUE(waitForOneRunningSession(server))
        << "Expected exactly one running Session after client connect";

    const auto sessions = server.sessions();
    ASSERT_EQ(sessions.size(), 1u);
    ASSERT_NE(sessions[0], nullptr);
    EXPECT_TRUE(sessions[0]->isRunning());
    EXPECT_EQ(sessions[0]->subscriptionIds().size(), 0u);

    client.stop();
    server.stop();
}

TEST(NetworkTests, SubscribeReturnsTokenAndCreatesDeliverer) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));

    EXPECT_EQ(server.sessions()[0]->subscriptionIds().size(), 0u);
    EXPECT_EQ(broker.subscriptionCount(), 0u);

    SubscriptionToken token = dispatcher.subscribe(kTopic);

    ASSERT_TRUE(token.valid());
    ASSERT_NE(token.id, 0u);
    EXPECT_EQ(token.topic, kTopic);

    EXPECT_EQ(broker.subscriptionCount(), 1u);
    EXPECT_TRUE(dispatcher.hasSubscription(token.id));

    ASSERT_TRUE(waitUntil(
        [&] {
            const auto sessions = server.sessions();
            return sessions.size() == 1 && sessions[0] &&
                   sessions[0]->subscriptionIds().size() == 1;
        },
        2s));
    EXPECT_EQ(server.sessions()[0]->subscriptionIds().size(), 1u);

    dispatcher.unsubscribe(token);
    server.stop();
}

TEST(NetworkTests, PublishAfterSubscribeDeliversToClient) {
    const uint16_t port = testPort();
    const std::string payload = "hello-phase3";

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));

    SubscriptionToken token = dispatcher.subscribe(kTopic);
    ASSERT_TRUE(token.valid());
    ASSERT_TRUE(waitUntil(
        [&] { return server.sessions()[0]->subscriptionIds().size() == 1; }, 2s));

    dispatcher.publish(kTopic, /*id=*/7, payload);

    std::shared_ptr<const BrokerMessage> received;
    ASSERT_TRUE(token.mq->dequeueFor(received, 2s))
        << "Timed out waiting for DELIVER into local MessageQueue";
    ASSERT_NE(received, nullptr);
    EXPECT_GE(received->getSequence(), 1u);
    EXPECT_EQ(payloadOf(*received), payload);

    dispatcher.unsubscribe(token);
    server.stop();
}

TEST(NetworkTests, UnsubscribeStopsDelivererAndDropsBrokerSub) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher subscriber(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));

    SubscriptionToken token = subscriber.subscribe(kTopic);
    ASSERT_TRUE(token.valid());
    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 1 &&
                   server.sessions()[0]->subscriptionIds().size() == 1;
        },
        2s));

    const uint64_t subscription_id = token.id;
    subscriber.unsubscribe(token);

    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 0 &&
                   server.sessions()[0]->subscriptionIds().size() == 0;
        },
        2s));
    EXPECT_FALSE(subscriber.hasSubscription(subscription_id));

    // A separate publisher should not deliver into the closed local queue.
    NetworkDispatcher publisher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitUntil(
        [&] { return server.sessions().size() == 2; }, 2s));
    publisher.publish(kTopic, /*id=*/1, "after-unsub");

    std::shared_ptr<const BrokerMessage> received;
    EXPECT_FALSE(token.mq->dequeueFor(received, 200ms))
        << "Unexpected message after unsubscribe";

    server.stop();
}

// C3 — NetworkDispatcher subscribe uses a unique group per client → fan-out.
TEST(NetworkTests, FanoutTwoDispatchersBothReceive) {
    const uint16_t port = testPort();
    const std::string payload = "fanout-hello";

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher subA(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));
    NetworkDispatcher subB(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForSessionCount(server, 2));

    SubscriptionToken tokenA = subA.subscribe(kTopic);
    SubscriptionToken tokenB = subB.subscribe(kTopic);
    ASSERT_TRUE(tokenA.valid());
    ASSERT_TRUE(tokenB.valid());
    ASSERT_NE(tokenA.id, tokenB.id);
    ASSERT_NE(tokenA.group, tokenB.group) << "Fan-out requires distinct groups";

    ASSERT_TRUE(waitUntil(
        [&] { return broker.subscriptionCount() == 2 && broker.groupCount(kTopic) == 2; },
        2s));

    NetworkDispatcher publisher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForSessionCount(server, 3));
    publisher.publish(kTopic, /*id=*/1, payload);

    std::shared_ptr<const BrokerMessage> gotA;
    std::shared_ptr<const BrokerMessage> gotB;
    ASSERT_TRUE(tokenA.mq->dequeueFor(gotA, 2s)) << "Subscriber A missed DELIVER";
    ASSERT_TRUE(tokenB.mq->dequeueFor(gotB, 2s)) << "Subscriber B missed DELIVER";
    ASSERT_NE(gotA, nullptr);
    ASSERT_NE(gotB, nullptr);

    EXPECT_EQ(payloadOf(*gotA), payload);
    EXPECT_EQ(payloadOf(*gotB), payload);
    EXPECT_EQ(gotA->getSequence(), gotB->getSequence());
    EXPECT_GE(gotA->getSequence(), 1u);

    subA.unsubscribe(tokenA);
    subB.unsubscribe(tokenB);
    server.stop();
}

// A5 — TCP disconnect / client stop without UNSUBSCRIBE cleans broker state.
TEST(NetworkTests, ClientDisconnectCleansServerSubscriptions) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    std::shared_ptr<Session> session;
    {
        NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
        ASSERT_TRUE(waitForOneRunningSession(server));
        session = server.sessions()[0];

        SubscriptionToken token = dispatcher.subscribe(kTopic);
        ASSERT_TRUE(token.valid());
        ASSERT_TRUE(waitUntil(
            [&] {
                return broker.subscriptionCount() == 1 && session->subscriptionIds().size() == 1;
            },
            2s));
        // Destructor stops BrokerClient without sending UNSUBSCRIBE.
    }

    ASSERT_TRUE(waitUntil([&] { return broker.subscriptionCount() == 0; }, 2s))
        << "Broker still has subscriptions after client disconnect";
    ASSERT_TRUE(waitUntil(
        [&] {
            return session && !session->isRunning() && session->subscriptionIds().size() == 0;
        },
        2s))
        << "Session did not finish / join deliverers after client disconnect";

    server.stop();
}

// D1 — CLOSE request gets CLOSE_ACK and tears down session subscriptions.
TEST(NetworkTests, CloseFrameAcksAndTearsDownSession) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    BrokerClient client(kHost, static_cast<int>(port));
    client.start();
    ASSERT_TRUE(waitUntil([&] { return client.isConnected(); }, 2s));
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto session = server.sessions()[0];

    const uint32_t sub_req = client.generateRequestId();
    auto sub_fut = client.sendFrame(
        sub_req, encodeSubscribeFrame(sub_req, kTopic, std::string(kTopic) + "_close_test"));
    auto sub_ack = sub_fut.get();
    ASSERT_EQ(sub_ack->type, ProtocolFrameType::SUBSCRIBE_ACK);
    ASSERT_NE(sub_ack->subscribe_ack.subscription_id, 0u);
    ASSERT_TRUE(waitUntil(
        [&] { return broker.subscriptionCount() == 1 && session->subscriptionIds().size() == 1; },
        2s));

    const uint32_t close_req = client.generateRequestId();
    auto close_fut = client.sendFrame(close_req, encodeCloseFrame(close_req));
    auto close_ack = close_fut.get();
    ASSERT_EQ(close_ack->type, ProtocolFrameType::CLOSE_ACK);
    EXPECT_EQ(close_ack->close_ack.request_id, close_req);

    ASSERT_TRUE(waitUntil(
        [&] {
            return !session->isRunning() && broker.subscriptionCount() == 0 &&
                   session->subscriptionIds().size() == 0;
        },
        2s))
        << "CLOSE did not tear down session / subscriptions";

    client.stop();
    server.stop();
}

// D2 — NetworkDispatcher destructor stops the owned client without hanging server stop.
TEST(NetworkTests, NetworkDispatcherDestructorStopsClient) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    std::shared_ptr<Session> session;
    {
        NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
        ASSERT_TRUE(waitForOneRunningSession(server));
        session = server.sessions()[0];
        ASSERT_TRUE(session->isRunning());
    }

    ASSERT_TRUE(waitUntil([&] { return session && !session->isRunning(); }, 2s))
        << "Session still running after NetworkDispatcher destruction";

    // Must complete without deadlock once the client is gone.
    server.stop();
}

// D3 — Session reader exit (requestStop) joins deliverers and unsubscribes on broker.
TEST(NetworkTests, ReaderExitRunsCleanupSubscriptions) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto session = server.sessions()[0];

    SubscriptionToken token = dispatcher.subscribe(kTopic);
    ASSERT_TRUE(token.valid());
    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 1 && session->subscriptionIds().size() == 1;
        },
        2s));

    ::shutdown(session->fd(), SHUT_RDWR);

    ASSERT_TRUE(waitUntil(
        [&] {
            return !session->isRunning() && broker.subscriptionCount() == 0 &&
                   session->subscriptionIds().size() == 0;
        },
        2s))
        << "requestStop did not clean deliverers / broker subscriptions";

    server.stop();
}

// E2 — StopFailsInFlightSendFrameFutures
//
// Purpose: When BrokerClient::stop() runs while a sendFrame() future is still
// waiting for an ACK, that future must be completed with an error (via
// failAllPending). Callers must not hang on get(), and a second stop() must be
// safe (idempotent, no deadlock).
//
// Why AcceptOnlyServer: A real BrokerServer would answer SUBSCRIBE quickly, so
// the future would usually resolve before stop(). The sink accepts TCP and
// never writes a response, so the promise stays pending until stop() fails it.
//
// Flow:
//   1. Connect client to the sink.
//   2. sendFrame(SUBSCRIBE) — returns a future after the bytes are on the wire;
//      do NOT call fut.get() here (that would block forever waiting for an ACK
//      that never comes, and stop() would never run).
//   3. client.stop() — shuts down the socket, joins the receive thread, then
//      failAllPending(...) so every outstanding promise gets set_exception.
//   4. fut.get() must throw (expected: std::runtime_error with a useful message).
//   5. client.stop() again — teardown must tolerate being called twice.
//
// What this is NOT: a per-request "missed ACK / congestion timeout" test. That
// would leave the connection up and fail only one request_id; E2 covers
// connection teardown only.
//
TEST(NetworkTests, StopFailsInFlightSendFrameFutures) {
    const uint16_t port = testPort();

    AcceptOnlyServer sink(port);
    sink.start();
    ASSERT_TRUE(waitUntil([&] { return sink.isListening(); }, 2s));

    BrokerClient client(kHost, static_cast<int>(port));
    client.start();
    ASSERT_TRUE(waitUntil([&] { return client.isConnected(); }, 2s));

    const uint32_t req = client.generateRequestId();
    auto fut = client.sendFrame(req, encodeSubscribeFrame(req, kTopic, "orders_e2"));

    client.stop();
    EXPECT_THROW(fut.get(), std::runtime_error);
    client.stop();  // idempotent — must not deadlock or throw

    sink.stop();
}

// E3 — SubscribeAckHandlerRunsBeforePromise
//
// Purpose: On SUBSCRIBE_ACK, BrokerClient must invoke setSubscribeAckHandler
// *before* fulfillPromise for that request_id. NetworkDispatcher relies on this
// so subscription_id → local queue is published before subscribe()'s waiter
// returns — and before any DELIVER that can legally follow the ACK on the wire
// is handled against an empty map.
//
// How we observe ordering:
//   - The ack handler appends 1 to seq (after a short sleep).
//   - The test thread blocks in fut.get(); when that returns it appends 2.
//   - Correct order: handler finished first → seq == {1, 2}.
//   - Wrong order (fulfill before handler returns): get() can unblock, push 2,
//     then the delayed handler pushes 1 → seq == {2, 1}. The sleep widens that
//     race window so a regression is unlikely to flake green.
//
// Setup uses a real BrokerServer so a genuine SUBSCRIBE_ACK is produced; this
// is an ordering contract on the client receive path, not a server test.
//
TEST(NetworkTests, SubscribeAckHandlerRunsBeforePromise) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    BrokerClient client(kHost, static_cast<int>(port));
    std::mutex seq_mu;
    std::vector<int> seq;
    client.setSubscribeAckHandler([&](const SubscribeAck&) {
        // Delay so a wrong fulfill-before-handler order would let the waiter
        // observe completion first (seq would become {2,1}).
        std::this_thread::sleep_for(50ms);
        std::lock_guard<std::mutex> lock(seq_mu);
        seq.push_back(1);
    });
    client.start();
    ASSERT_TRUE(waitUntil([&] { return client.isConnected(); }, 2s));

    const uint32_t req = client.generateRequestId();
    auto fut = client.sendFrame(req, encodeSubscribeFrame(req, kTopic, "orders_e3"));
    auto ack = fut.get();
    {
        std::lock_guard<std::mutex> lock(seq_mu);
        seq.push_back(2);
    }

    ASSERT_EQ(ack->type, ProtocolFrameType::SUBSCRIBE_ACK);
    ASSERT_NE(ack->subscribe_ack.subscription_id, 0u);
    EXPECT_EQ(seq, (std::vector<int>{1, 2}));

    client.stop();
    server.stop();
}

// G2 — failed subscribe clears pending_by_request_id_ (no leak).
// Without a pendingCount() hook, assert the failure path itself: subscribe must
// throw after disconnect, and a fresh dispatcher can still subscribe successfully.
// Inspect NetworkDispatcher::pending_by_request_id_ after the throw to confirm leak.
TEST(NetworkTests, SubscribeFailureClearsPendingMap) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher dispatcher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto session = server.sessions()[0];

    ::shutdown(session->fd(), SHUT_RDWR);
    ASSERT_TRUE(waitUntil([&] { return session && !session->isRunning(); }, 2s));
    // Allow the client receive thread to observe EOF and clear connected_.
    std::this_thread::sleep_for(100ms);

    // Breakpoint / watch pending_by_request_id_ here after the throw:
    // insert happens before sendFrame; on send/get failure the map entry is leaked
    // unless subscribe() clears it in a catch.
    EXPECT_THROW(dispatcher.subscribe(kTopic), std::runtime_error);

    server.stop();

    // Retry on a fresh connection still works (no sticky process-wide corruption).
    const uint16_t port2 = static_cast<uint16_t>(port + 1);
    MessageBroker broker2;
    BrokerServer server2(broker2, port2);
    server2.start();
    ASSERT_TRUE(waitUntil([&] { return server2.isListening(); }, 2s));

    NetworkDispatcher dispatcher2(kHost, static_cast<int>(port2));
    ASSERT_TRUE(waitForOneRunningSession(server2));
    SubscriptionToken token = dispatcher2.subscribe(kTopic);
    ASSERT_TRUE(token.valid());
    EXPECT_TRUE(dispatcher2.hasSubscription(token.id));

    server2.stop();
}

// B7 — DELIVER that races SUBSCRIBE_ACK is not dropped (map registered first).
TEST(NetworkTests, ImmediatePublishAfterSubscribeDoesNotDropFirstDeliver) {
    const uint16_t port = testPort();
    const int kPublishCount = 20;

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher subscriber(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));
    NetworkDispatcher publisher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForSessionCount(server, 2));

    // Publish as soon as the broker has the subscription — often before the
    // client's subscribe() waiter resumes — to stress ack-before-deliver ordering.
    std::atomic<bool> publish_ok{true};
    std::thread hammer([&] {
        if (!waitUntil([&] { return broker.subscriptionCount() >= 1; }, 2s)) {
            publish_ok.store(false, std::memory_order_release);
            return;
        }
        try {
            for (int i = 0; i < kPublishCount; ++i) {
                publisher.publish(kTopic, i, "race-" + std::to_string(i));
            }
        } catch (...) {
            publish_ok.store(false, std::memory_order_release);
        }
    });

    SubscriptionToken token = subscriber.subscribe(kTopic);
    ASSERT_TRUE(token.valid());
    ASSERT_TRUE(subscriber.hasSubscription(token.id));

    hammer.join();
    ASSERT_TRUE(publish_ok.load(std::memory_order_acquire))
        << "Publisher thread failed before/during race publishes";

    for (int i = 0; i < kPublishCount; ++i) {
        std::shared_ptr<const BrokerMessage> got;
        ASSERT_TRUE(token.mq->dequeueFor(got, 2s))
            << "Missing deliver #" << i << " (ack/map race?)";
        ASSERT_NE(got, nullptr);
        EXPECT_EQ(payloadOf(*got), "race-" + std::to_string(i));
    }

    subscriber.unsubscribe(token);
    server.stop();
}

// F1 — MalformedFrameDropsSessionOnly
//
// Purpose: A single bad length-prefixed frame must end that Session only. The
// accept loop stays healthy so a later well-behaved client can still connect.
//
// Action: raw TCP client sends frame_len == 1 (< minimum header size of 2).
// Session::run breaks out of the reader loop, cleans up, and sets !isRunning().
//
TEST(NetworkTests, MalformedFrameDropsSessionOnly) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    const int bad_fd = connectRawTcp(kHost, port);
    ASSERT_GE(bad_fd, 0);
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto bad_session = server.sessions()[0];

    // frame_len = 1 → rejected by Session::run (needs at least 2-byte header).
    std::vector<uint8_t> bad_len;
    encode_u32(1u, bad_len);
    ASSERT_TRUE(writeAll(bad_fd, bad_len.data(), bad_len.size()));

    ASSERT_TRUE(waitUntil(
        [&] { return bad_session && !bad_session->isRunning(); }, 2s))
        << "Malformed frame did not end the session";
    EXPECT_TRUE(server.isListening());
    ::close(bad_fd);

    BrokerClient good(kHost, static_cast<int>(port));
    good.start();
    ASSERT_TRUE(waitUntil([&] { return good.isConnected(); }, 2s));
    ASSERT_TRUE(waitForRunningSessionCount(server, 1))
        << "Server accept loop unhealthy after malformed-frame session";

    good.stop();
    server.stop();
}

// F2 — UnsupportedProtocolVersionDropsSession
//
// Purpose: A length-valid frame whose FrameHeader.version != PROTOCOL_VERSION
// must drop that session the same way as F1, without killing the server.
//
TEST(NetworkTests, UnsupportedProtocolVersionDropsSession) {
    const uint16_t port = testPort();

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    const int bad_fd = connectRawTcp(kHost, port);
    ASSERT_GE(bad_fd, 0);
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto bad_session = server.sessions()[0];

    std::vector<uint8_t> frame;
    FrameHeader header{static_cast<uint8_t>(PROTOCOL_VERSION + 1),
                       ProtocolFrameType::SUBSCRIBE};
    encode_frame_header(header, frame);
    ASSERT_TRUE(sendRawFrame(bad_fd, frame));

    ASSERT_TRUE(waitUntil(
        [&] { return bad_session && !bad_session->isRunning(); }, 2s))
        << "Unsupported protocol version did not end the session";
    EXPECT_TRUE(server.isListening());
    ::close(bad_fd);

    BrokerClient good(kHost, static_cast<int>(port));
    good.start();
    ASSERT_TRUE(waitUntil([&] { return good.isConnected(); }, 2s));
    ASSERT_TRUE(waitForRunningSessionCount(server, 1));

    good.stop();
    server.stop();
}

// F5 — ManySubscriptionsPerSession
//
// Purpose: One NetworkDispatcher / Session can hold multiple subscriptions
// (each with its own deliverer). Unsubscribing one must not tear down the
// others' deliverers or broker registrations.
//
TEST(NetworkTests, ManySubscriptionsPerSession) {
    const uint16_t port = testPort();
    const char* topic_a = "orders_a";
    const char* topic_b = "orders_b";
    const char* topic_c = "orders_c";
    const std::string payload_a = "keep-a";
    const std::string payload_c = "keep-c";

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    NetworkDispatcher subscriber(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForOneRunningSession(server));
    auto session = server.sessions()[0];

    SubscriptionToken token_a = subscriber.subscribe(topic_a);
    SubscriptionToken token_b = subscriber.subscribe(topic_b);
    SubscriptionToken token_c = subscriber.subscribe(topic_c);
    ASSERT_TRUE(token_a.valid());
    ASSERT_TRUE(token_b.valid());
    ASSERT_TRUE(token_c.valid());
    ASSERT_NE(token_a.id, token_b.id);
    ASSERT_NE(token_b.id, token_c.id);

    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 3 && session->subscriptionIds().size() == 3;
        },
        2s));
    EXPECT_TRUE(subscriber.hasSubscription(token_a.id));
    EXPECT_TRUE(subscriber.hasSubscription(token_b.id));
    EXPECT_TRUE(subscriber.hasSubscription(token_c.id));

    subscriber.unsubscribe(token_b);

    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 2 && session->subscriptionIds().size() == 2;
        },
        2s))
        << "Unsubscribe of one sub did not leave the other deliverers";
    EXPECT_TRUE(subscriber.hasSubscription(token_a.id));
    EXPECT_FALSE(subscriber.hasSubscription(token_b.id));
    EXPECT_TRUE(subscriber.hasSubscription(token_c.id));

    NetworkDispatcher publisher(kHost, static_cast<int>(port));
    ASSERT_TRUE(waitForSessionCount(server, 2));
    publisher.publish(topic_a, /*id=*/1, payload_a);
    publisher.publish(topic_c, /*id=*/2, payload_c);
    publisher.publish(topic_b, /*id=*/3, "should-not-arrive");

    std::shared_ptr<const BrokerMessage> got_a;
    std::shared_ptr<const BrokerMessage> got_c;
    ASSERT_TRUE(token_a.mq->dequeueFor(got_a, 2s));
    ASSERT_TRUE(token_c.mq->dequeueFor(got_c, 2s));
    EXPECT_EQ(payloadOf(*got_a), payload_a);
    EXPECT_EQ(payloadOf(*got_c), payload_c);

    std::shared_ptr<const BrokerMessage> stray;
    EXPECT_FALSE(token_b.mq->dequeueFor(stray, 100ms))
        << "Unsubscribed queue still received delivers";

    subscriber.unsubscribe(token_a);
    subscriber.unsubscribe(token_c);
    server.stop();
}

// Network stress — 50 NetworkDispatchers shared by 100 fan-out subscribers
// (2 subscribe()s each) and 50 publisher threads × 100 messages on one topic.
//
// Publishes run concurrently (no global publish lock). Each subscriber must
// receive every message with strictly increasing sequences; cross-subscriber
// delivery order is not required to match.
// Dispatchers are reused for both subscribe and publish (multiplexed sessions).
//
TEST(NetworkStress, FiftyDispatchers_HundredSubs_ConcurrentPublish) {
    constexpr int kDispatchers = 50;
    constexpr int kSubsPerDispatcher = 2;
    constexpr int kSubscribers = kDispatchers * kSubsPerDispatcher;  // 100
    constexpr int kPublishers = 50;
    constexpr int kMessagesPerPublisher = 100;
    constexpr int kTotalMessages = kPublishers * kMessagesPerPublisher;  // 5000

    const uint16_t port = testPort();
    const std::string topic = "network-stress";

    MessageBroker broker;
    BrokerServer server(broker, port);
    server.start();
    ASSERT_TRUE(waitUntil([&] { return server.isListening(); }, 2s));

    std::vector<std::unique_ptr<NetworkDispatcher>> dispatchers;
    dispatchers.reserve(static_cast<size_t>(kDispatchers));
    for (int i = 0; i < kDispatchers; ++i) {
        dispatchers.push_back(
            std::make_unique<NetworkDispatcher>(kHost, static_cast<int>(port)));
    }
    ASSERT_TRUE(waitUntil(
        [&] { return runningSessionCount(server) == static_cast<size_t>(kDispatchers); },
        10s))
        << "Expected " << kDispatchers << " running sessions";

    std::vector<SubscriptionToken> tokens;
    tokens.reserve(static_cast<size_t>(kSubscribers));
    for (auto& dispatcher : dispatchers) {
        for (int s = 0; s < kSubsPerDispatcher; ++s) {
            SubscriptionToken token = dispatcher->subscribe(topic);
            ASSERT_TRUE(token.valid()) << "subscribe failed";
            tokens.push_back(std::move(token));
        }
    }
    ASSERT_EQ(tokens.size(), static_cast<size_t>(kSubscribers));
    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == static_cast<size_t>(kSubscribers) &&
                   broker.groupCount(topic) == static_cast<size_t>(kSubscribers);
        },
        10s));

    std::vector<std::vector<uint64_t>> sequences(static_cast<size_t>(kSubscribers));
    std::vector<std::thread> consumers;
    consumers.reserve(static_cast<size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        consumers.emplace_back([&, i] {
            auto& seqs = sequences[static_cast<size_t>(i)];
            seqs.reserve(static_cast<size_t>(kTotalMessages));
            while (seqs.size() < static_cast<size_t>(kTotalMessages)) {
                std::shared_ptr<const BrokerMessage> msg;
                if (!tokens[static_cast<size_t>(i)].mq->dequeueFor(msg, 5s) || !msg) {
                    return;
                }
                seqs.push_back(msg->getSequence());
            }
        });
    }

    std::atomic<bool> publish_ok{true};
    std::vector<std::thread> publishers;
    publishers.reserve(static_cast<size_t>(kPublishers));
    for (int pubIdx = 0; pubIdx < kPublishers; ++pubIdx) {
        publishers.emplace_back([&, pubIdx] {
            NetworkDispatcher& dispatcher =
                *dispatchers[static_cast<size_t>(pubIdx % kDispatchers)];
            try {
                for (int msgIdx = 0; msgIdx < kMessagesPerPublisher; ++msgIdx) {
                    dispatcher.publish(
                        topic,
                        /*id=*/msgIdx,
                        "p" + std::to_string(pubIdx) + "_" + std::to_string(msgIdx));
                }
            } catch (...) {
                publish_ok.store(false, std::memory_order_release);
            }
        });
    }

    for (auto& t : publishers) {
        t.join();
    }
    ASSERT_TRUE(publish_ok.load(std::memory_order_acquire)) << "Publisher thread failed";

    for (auto& t : consumers) {
        t.join();
    }

    for (int i = 0; i < kSubscribers; ++i) {
        ASSERT_EQ(sequences[static_cast<size_t>(i)].size(),
                  static_cast<size_t>(kTotalMessages))
            << "subscriber " << i << " incomplete (dequeue timeout?)";
        const auto& seqs = sequences[static_cast<size_t>(i)];
        for (size_t j = 1; j < seqs.size(); ++j) {
            EXPECT_LT(seqs[j - 1], seqs[j])
                << "subscriber " << i << " sequences not strictly increasing at index " << j;
        }
    }

    for (size_t i = 0; i < tokens.size(); ++i) {
        dispatchers[i / static_cast<size_t>(kSubsPerDispatcher)]->unsubscribe(tokens[i]);
    }
    server.stop();
}


