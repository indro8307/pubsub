#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "dispatcher.h"
#include "message_broker.h"
#include "publisher.h"
#include "subscriber.h"
#include "test_helpers.h"

using namespace std::chrono_literals;
using test_helpers::waitUntil;

namespace {

MessageQueueConfig backpressureConfig(BackpressurePolicy policy, size_t maxSize) {
    MessageQueueConfig config;
    config.backpressurePolicy = policy;
    config.maxSize = maxSize;
    return config;
}

std::string payloadToString(const Message& m) {
    return std::string(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
}

}  // namespace

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

TEST(CompeteRouting, ThreeSubscribers_LargePayload_OnlyOneReceives) {
    constexpr std::size_t kPayloadSize = 5 * 1024;
    constexpr int kSubscribers = 3;

    std::string payload(kPayloadSize, '\0');
    for (std::size_t i = 0; i < kPayloadSize; ++i) {
        payload[i] = static_cast<char>(i % 256);
    }

    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "large-payload";

    auto receivedCounts =
        std::make_unique<std::atomic<int>[]>(static_cast<std::size_t>(kSubscribers));
    std::vector<std::string> receivedPayloads(static_cast<std::size_t>(kSubscribers));

    std::vector<std::unique_ptr<Subscriber>> subscribers;
    subscribers.reserve(static_cast<std::size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        receivedCounts[static_cast<std::size_t>(i)].store(0);
        subscribers.push_back(std::make_unique<Subscriber>(dispatcher));
        const int subIdx = i;
        subscribers.back()->subscribe(topic, [&, subIdx](const Message& m) {
            if (m.getSize() != kPayloadSize) {
                return;
            }
            const std::string received(
                reinterpret_cast<const char*>(m.getPayload()), m.getSize());
            if (received == payload) {
                receivedCounts[static_cast<std::size_t>(subIdx)].fetch_add(1);
                receivedPayloads[static_cast<std::size_t>(subIdx)] = std::move(received);
            }
        });
    }

    std::this_thread::sleep_for(50ms);
    pub.publish(topic, payload);

    ASSERT_TRUE(waitUntil([&]() {
        int total = 0;
        for (int i = 0; i < kSubscribers; ++i) {
            total += receivedCounts[static_cast<std::size_t>(i)].load();
        }
        return total == 1;
    }));

    int receivers = 0;
    int receiverIdx = -1;
    for (int i = 0; i < kSubscribers; ++i) {
        const int count = receivedCounts[static_cast<std::size_t>(i)].load();
        if (count == 0) {
            continue;
        }
        EXPECT_EQ(count, 1) << "subscriber " << i;
        ++receivers;
        receiverIdx = i;
    }

    EXPECT_EQ(receivers, 1);
    ASSERT_GE(receiverIdx, 0);
    EXPECT_EQ(receivedPayloads[static_cast<std::size_t>(receiverIdx)].size(), kPayloadSize);
    EXPECT_EQ(receivedPayloads[static_cast<std::size_t>(receiverIdx)], payload);

    for (auto& sub : subscribers) {
        sub->stop();
    }
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

TEST(CompeteBackpressure, DropOldest_DropsOldestWhenFull) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::DropOldest, kMaxSize));
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-drop-oldest";

    std::atomic<bool> processingPaused{true};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const Message& m) {
        const int invocation = handlerInvocations.fetch_add(1);
        if (invocation == 0) {
            while (processingPaused.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(1ms);
            }
        }
        std::lock_guard<std::mutex> lock(receivedMtx);
        received.push_back(payloadToString(m));
    });

    std::this_thread::sleep_for(50ms);
    pub.publish(topic, "msg-0");

    ASSERT_TRUE(waitUntil([&] { return handlerInvocations.load() >= 1; }));

    for (int i = 1; i < kPublishCount; ++i) {
        pub.publish(topic, "msg-" + std::to_string(i));
    }

    processingPaused.store(false, std::memory_order_release);

    ASSERT_TRUE(waitUntil([&] {
        std::lock_guard<std::mutex> lock(receivedMtx);
        return received.size() == 4u;
    }));

    {
        std::lock_guard<std::mutex> lock(receivedMtx);
        ASSERT_EQ(received.size(), 4u);
        EXPECT_EQ(received[0], "msg-0");
        EXPECT_EQ(received[1], "msg-2");
        EXPECT_EQ(received[2], "msg-3");
        EXPECT_EQ(received[3], "msg-4");
    }

    sub.stop();
}

TEST(CompeteBackpressure, RejectNew_RejectsWhenFull) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::RejectNew, kMaxSize));
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-reject-new";

    std::atomic<bool> processingPaused{true};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const Message& m) {
        const int invocation = handlerInvocations.fetch_add(1);
        if (invocation == 0) {
            while (processingPaused.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(1ms);
            }
        }
        std::lock_guard<std::mutex> lock(receivedMtx);
        received.push_back(payloadToString(m));
    });

    std::this_thread::sleep_for(50ms);
    pub.publish(topic, "msg-0");

    ASSERT_TRUE(waitUntil([&] { return handlerInvocations.load() >= 1; }));

    for (int i = 1; i < kPublishCount; ++i) {
        pub.publish(topic, "msg-" + std::to_string(i));
    }

    processingPaused.store(false, std::memory_order_release);

    ASSERT_TRUE(waitUntil([&] {
        std::lock_guard<std::mutex> lock(receivedMtx);
        return received.size() == 4u;
    }));

    {
        std::lock_guard<std::mutex> lock(receivedMtx);
        ASSERT_EQ(received.size(), 4u);
        EXPECT_EQ(received[0], "msg-0");
        EXPECT_EQ(received[1], "msg-1");
        EXPECT_EQ(received[2], "msg-2");
        EXPECT_EQ(received[3], "msg-3");
    }

    sub.stop();
}

TEST(CompeteBackpressure, Block_WaitsUntilSpace) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::Block, kMaxSize));
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-block";

    std::atomic<bool> processingPaused{true};
    std::atomic<bool> publishDone{false};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const Message& m) {
        const int invocation = handlerInvocations.fetch_add(1);
        if (invocation == 0) {
            while (processingPaused.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(1ms);
            }
        }
        std::lock_guard<std::mutex> lock(receivedMtx);
        received.push_back(payloadToString(m));
    });

    std::this_thread::sleep_for(50ms);
    pub.publish(topic, "msg-0");

    ASSERT_TRUE(waitUntil([&] { return handlerInvocations.load() >= 1; }));

    std::thread publishThread([&] {
        for (int i = 1; i < kPublishCount; ++i) {
            pub.publish(topic, "msg-" + std::to_string(i));
        }
        publishDone.store(true, std::memory_order_release);
    });

    processingPaused.store(false, std::memory_order_release);

    ASSERT_TRUE(waitUntil([&] {
        std::lock_guard<std::mutex> lock(receivedMtx);
        return publishDone.load(std::memory_order_acquire) && received.size() == 5u;
    }, 10s));

    {
        std::lock_guard<std::mutex> lock(receivedMtx);
        ASSERT_EQ(received.size(), 5u);
        for (int i = 0; i < kPublishCount; ++i) {
            EXPECT_EQ(received[static_cast<std::size_t>(i)], "msg-" + std::to_string(i));
        }
    }

    publishThread.join();
    sub.stop();
}
