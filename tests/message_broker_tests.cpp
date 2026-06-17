#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "message_queue.h"
#include "test_helpers.h"

using namespace std::chrono_literals;
using test_helpers::waitUntil;

TEST(MessageBrokerTest, Subscribe_IncrementsSubscriptionCount) {
    MessageBroker broker;
    const std::string topic = "orders";
    const std::string group = "orders";

    broker.subscribe(topic, group);
    broker.subscribe(topic, group);

    EXPECT_EQ(broker.subscriptionCount(), 2u);
}

TEST(MessageBrokerTest, Compete_TwoSubs_OneGroup) {
    MessageBroker broker;
    const std::string topic = "orders";
    const std::string group = "orders";

    broker.subscribe(topic, group);
    broker.subscribe(topic, group);

    EXPECT_EQ(broker.groupCount(topic), 1u);
    EXPECT_EQ(broker.subscriptionCount(), 2u);
}

TEST(MessageBrokerTest, Fanout_TwoSubs_TwoGroups) {
    MessageBroker broker;
    const std::string topic = "notifications";

    broker.subscribe(topic, "sub_1");
    broker.subscribe(topic, "sub_2");

    EXPECT_EQ(broker.groupCount(topic), 2u);
    EXPECT_EQ(broker.subscriptionCount(), 2u);
    EXPECT_EQ(broker.totalGroupCount(), 2u);
}

TEST(MessageBrokerTest, Unsubscribe_RemovesSubscriptionAndGroup) {
    MessageBroker broker;
    const std::string topic = "cleanup";
    const std::string group = "g1";

    const SubscriptionToken token = broker.subscribe(topic, group);
    ASSERT_EQ(broker.subscriptionCount(), 1u);
    ASSERT_EQ(broker.groupCount(topic), 1u);

    broker.unsubscribe(token);

    EXPECT_EQ(broker.subscriptionCount(), 0u);
    EXPECT_EQ(broker.groupCount(topic), 0u);
    EXPECT_EQ(broker.totalGroupCount(), 0u);
}

TEST(MessageBrokerTest, Unsubscribe_InvalidToken_NoOp) {
    MessageBroker broker;
    const std::string topic = "cleanup";
    const std::string group = "g1";

    const SubscriptionToken token = broker.subscribe(topic, group);

    SubscriptionToken bad = token;
    bad.topic = "wrong-topic";
    broker.unsubscribe(bad);
    EXPECT_EQ(broker.subscriptionCount(), 1u);
    EXPECT_EQ(broker.groupCount(topic), 1u);

    bad = token;
    bad.group = "wrong-group";
    broker.unsubscribe(bad);
    EXPECT_EQ(broker.subscriptionCount(), 1u);

    SubscriptionToken unknown("", "g", 9999, nullptr);
    broker.unsubscribe(unknown);
    EXPECT_EQ(broker.subscriptionCount(), 1u);

    broker.unsubscribe(token);
    EXPECT_EQ(broker.subscriptionCount(), 0u);
}

TEST(MessageBrokerTest, PublishToGroup_BufferFalse_MissingTopic_ReturnsFalse) {
    MessageBroker broker;
    Message msg(1);
    msg.setPayload("x", 1);

    EXPECT_FALSE(broker.publish("missing-topic", "missing-group", msg, false));
}

TEST(MessageBrokerTest, PublishToGroup_BufferFalse_MissingGroup_ReturnsFalse) {
    MessageBroker broker;
    const std::string topic = "orders";
    const std::string group = "orders";
    broker.subscribe(topic, group);

    Message msg(1);
    msg.setPayload("x", 1);
    EXPECT_FALSE(broker.publish(topic, "other-group", msg, false));
}

TEST(MessageBrokerTest, PublishToTopic_NoSubscribers_ReturnsFalse) {
    MessageBroker broker;
    Message msg(1);
    msg.setPayload("x", 1);

    EXPECT_FALSE(broker.publish("missing-topic", msg));
}

TEST(MessageBrokerTest, PublishToTopic_BroadcastsToAllGroups) {
    MessageBroker broker;
    const std::string topic = "broadcast";

    const SubscriptionToken sub1 = broker.subscribe(topic, "g1");
    const SubscriptionToken sub2 = broker.subscribe(topic, "g2");
    ASSERT_TRUE(sub1.valid());
    ASSERT_TRUE(sub2.valid());

    Message msg(42);
    msg.setPayload("hello", 5);
    ASSERT_TRUE(broker.publish(topic, msg));

    Message received1;
    Message received2;
    ASSERT_TRUE(sub1.mq->dequeueFor(received1, 500ms));
    ASSERT_TRUE(sub2.mq->dequeueFor(received2, 500ms));

    EXPECT_EQ(received1.getId(), 42);
    EXPECT_EQ(received2.getId(), 42);
    EXPECT_EQ(std::string(received1.getPayload(), received1.getSize()), "hello");
    EXPECT_EQ(std::string(received2.getPayload(), received2.getSize()), "hello");

    broker.unsubscribe(sub1);
    broker.unsubscribe(sub2);
}
