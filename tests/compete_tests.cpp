#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dispatcher.h"
#include "message_queue.h"
#include "publisher.h"
#include "subscriber.h"
#include "test_helpers.h"

using namespace std::chrono_literals;
using test_helpers::waitUntil;

TEST(CompeteRouting, TwoSubscribers_OneMessage_OnlyOneReceives) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> sub1Count{0};
    std::atomic<int> sub2Count{0};

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe("orders", [&](const Message& m) {
        std::string payload(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
        if (payload == "only-one") {
            ++sub1Count;
        }
    });
    s2.subscribe("orders", [&](const Message& m) {
        std::string payload(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
        if (payload == "only-one") {
            ++sub2Count;
        }
    });

    std::this_thread::sleep_for(50ms);
    pub.publish("orders", "only-one");

    std::this_thread::sleep_for(2s);
    EXPECT_EQ(sub1Count.load() + sub2Count.load(), 1);

    s1.stop();
    s2.stop();
}

TEST(CompeteRouting, ManyMessages_TotalDeliveriesEqualsPublishCount) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    constexpr int kMessages = 10;
    std::atomic<int> sub1Count{0};
    std::atomic<int> sub2Count{0};

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe("orders", [&](const Message&) { ++sub1Count; });
    s2.subscribe("orders", [&](const Message&) { ++sub2Count; });

    std::this_thread::sleep_for(50ms);
    for (int i = 0; i < kMessages; ++i) {
        pub.publish("orders", "msg-" + std::to_string(i));
    }

    ASSERT_TRUE(waitUntil([&] {
        return sub1Count.load() + sub2Count.load() == kMessages;
    }));

    EXPECT_EQ(sub1Count.load() + sub2Count.load(), kMessages);

    s1.stop();
    s2.stop();
}

TEST(CompeteRouting, AfterOneStops_RemainingGetsAll) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    constexpr int kMessages = 5;
    std::atomic<int> sub1Count{0};
    std::atomic<int> sub2Count{0};

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe("orders", [&](const Message&) { ++sub1Count; });
    s2.subscribe("orders", [&](const Message&) { ++sub2Count; });

    std::this_thread::sleep_for(50ms);
    s1.stop();

    for (int i = 0; i < kMessages; ++i) {
        pub.publish("orders", "msg-" + std::to_string(i));
    }

    ASSERT_TRUE(waitUntil([&] {
        return sub2Count.load() == kMessages;
    }));

    EXPECT_EQ(sub1Count.load(), 0);
    EXPECT_EQ(sub2Count.load(), kMessages);

    s2.stop();
}

TEST(CompeteRouting, Stop_UnsubscribesCompete) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    const std::string topic = "cleanup";

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe(topic, [](const Message&) {});
    s2.subscribe(topic, [](const Message&) {});

    ASSERT_EQ(broker.groupCount(topic), 1u);
    ASSERT_EQ(broker.subscriptionCount(), 2u);

    s1.stop();
    EXPECT_EQ(broker.groupCount(topic), 1u);
    EXPECT_EQ(broker.subscriptionCount(), 1u);

    s2.stop();
    EXPECT_EQ(broker.groupCount(topic), 0u);
    EXPECT_EQ(broker.subscriptionCount(), 0u);
}

TEST(CompeteStress, ManyPublishersSubscribers_TotalDeliveriesMatch) {
    constexpr int kPublishers = 10;
    constexpr int kSubscribers = 50;
    constexpr int kMessagesPerPublisher = 2;
    constexpr int kTotalMessages = kPublishers * kMessagesPerPublisher;

    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    const std::string topic = "stress-compete";

    std::atomic<int> totalDelivered{0};
    std::vector<std::unique_ptr<Subscriber>> subscribers;
    subscribers.reserve(static_cast<std::size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        subscribers.push_back(std::make_unique<Subscriber>(dispatcher));
        subscribers.back()->subscribe(topic, [&totalDelivered](const Message&) {
            totalDelivered.fetch_add(1, std::memory_order_relaxed);
        });
    }

    std::this_thread::sleep_for(100ms);
    ASSERT_EQ(broker.groupCount(topic), 1u);
    ASSERT_EQ(broker.subscriptionCount(), static_cast<std::size_t>(kSubscribers));

    std::vector<std::unique_ptr<Publisher>> publishers;
    publishers.reserve(static_cast<std::size_t>(kPublishers));
    for (int i = 0; i < kPublishers; ++i) {
        publishers.push_back(std::make_unique<Publisher>(dispatcher));
    }

    std::vector<std::thread> publishThreads;
    publishThreads.reserve(static_cast<std::size_t>(kPublishers));
    for (int pubIdx = 0; pubIdx < kPublishers; ++pubIdx) {
        publishThreads.emplace_back([&publishers, &topic, pubIdx]() {
            for (int msgIdx = 0; msgIdx < kMessagesPerPublisher; ++msgIdx) {
                publishers[static_cast<std::size_t>(pubIdx)]->publish(
                    topic,
                    "pub" + std::to_string(pubIdx) + "_msg" + std::to_string(msgIdx));
            }
        });
    }
    for (auto& t : publishThreads) {
        t.join();
    }

    ASSERT_TRUE(waitUntil([&] {
        return totalDelivered.load(std::memory_order_relaxed) == kTotalMessages;
    }, 30s));

    EXPECT_EQ(totalDelivered.load(std::memory_order_relaxed), kTotalMessages);

    for (auto& sub : subscribers) {
        sub->stop();
    }

    EXPECT_EQ(broker.groupCount(topic), 0u);
    EXPECT_EQ(broker.subscriptionCount(), 0u);
}
