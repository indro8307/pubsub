#include <gtest/gtest.h>

#include "broker_client.h"
#include "broker_server.h"
#include "dispatcher.h"
#include "message_broker.h"
#include "test_helpers.h"

#include <chrono>
#include <string>
#include <unistd.h>

using namespace std::chrono_literals;
using test_helpers::waitUntil;

namespace {

constexpr const char* kHost = "127.0.0.1";
constexpr const char* kTopic = "orders";

uint16_t testPort() {
    return static_cast<uint16_t>(21000 + (::getpid() % 1000));
}

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

std::string payloadOf(const BrokerMessage& msg) {
    return std::string(reinterpret_cast<const char*>(msg.payload().getPayload()),
                       msg.payload().getSize());
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
    EXPECT_EQ(sessions[0]->delivererCount(), 0u);

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

    EXPECT_EQ(server.sessions()[0]->delivererCount(), 0u);
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
                   sessions[0]->delivererCount() == 1;
        },
        2s));
    EXPECT_EQ(server.sessions()[0]->delivererCount(), 1u);

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
        [&] { return server.sessions()[0]->delivererCount() == 1; }, 2s));

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
                   server.sessions()[0]->delivererCount() == 1;
        },
        2s));

    const uint64_t subscription_id = token.id;
    subscriber.unsubscribe(token);

    ASSERT_TRUE(waitUntil(
        [&] {
            return broker.subscriptionCount() == 0 &&
                   server.sessions()[0]->delivererCount() == 0;
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
