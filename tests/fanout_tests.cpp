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

std::string payloadToString(const BrokerMessage& bm) {
    const Message& m = bm.payload();
    return std::string(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
}

std::vector<int> makeSequenceCounts(int expectedCount) {
    return std::vector<int>(static_cast<std::size_t>(expectedCount), 0);
}

void recordSequence(std::vector<int>& counts, uint64_t sequence) {
    const std::size_t index = static_cast<std::size_t>(sequence - 1);
    ASSERT_LT(index, counts.size());
    ++counts[index];
}

int totalRecorded(const std::vector<int>& counts) {
    int total = 0;
    for (int count : counts) {
        total += count;
    }
    return total;
}

void expectEachSequenceOnce(const std::vector<int>& counts) {
    for (std::size_t i = 0; i < counts.size(); ++i) {
        EXPECT_EQ(counts[i], 1) << "sequence " << (i + 1);
    }
}

}  // namespace

TEST(FanoutRouting, TwoSubscribers_BothReceive) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> sub1Count{0};
    std::atomic<int> sub2Count{0};
    std::string sub1Payload;
    std::string sub2Payload;

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe("notifications", [&](const BrokerMessage& bm) {
        const Message& m = bm.payload();
        ++sub1Count;
        sub1Payload = std::string(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
    });
    s2.subscribe("notifications", [&](const BrokerMessage& bm) {
        const Message& m = bm.payload();
        ++sub2Count;
        sub2Payload = std::string(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
    });

    std::this_thread::sleep_for(50ms);
    pub.publish("notifications", "broadcast");

    ASSERT_TRUE(waitUntil([&] {
        return sub1Count.load() == 1 && sub2Count.load() == 1;
    }));

    s1.stop();
    s2.stop();

    EXPECT_EQ(sub1Count.load(), 1);
    EXPECT_EQ(sub2Count.load(), 1);
    EXPECT_EQ(sub1Payload, "broadcast");
    EXPECT_EQ(sub2Payload, "broadcast");
}

TEST(FanoutRouting, MultiplePublishers_EachSubscriberReceivesAllSequences) {
    constexpr int kPublishers = 5;
    constexpr int kMessagesPerPublisher = 10;
    constexpr int kSubscribers = 3;
    constexpr int kTotalMessages = kPublishers * kMessagesPerPublisher;

    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    const std::string topic = "seq-fanout";

    std::vector<std::mutex> seqMutexes(static_cast<std::size_t>(kSubscribers));
    std::vector<std::vector<int>> sequenceCounts(static_cast<std::size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        sequenceCounts[static_cast<std::size_t>(i)] = makeSequenceCounts(kTotalMessages);
    }

    std::vector<std::unique_ptr<Subscriber>> subscribers;
    subscribers.reserve(static_cast<std::size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        subscribers.push_back(std::make_unique<Subscriber>(dispatcher));
        const int subIdx = i;
        subscribers.back()->subscribe(topic, [&, subIdx](const BrokerMessage& bm) {
            std::lock_guard<std::mutex> lock(seqMutexes[static_cast<std::size_t>(subIdx)]);
            recordSequence(sequenceCounts[static_cast<std::size_t>(subIdx)], bm.getSequence());
        });
    }

    std::this_thread::sleep_for(50ms);

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

    ASSERT_TRUE(waitUntil([&]() {
        for (int i = 0; i < kSubscribers; ++i) {
            std::lock_guard<std::mutex> lock(seqMutexes[static_cast<std::size_t>(i)]);
            if (totalRecorded(sequenceCounts[static_cast<std::size_t>(i)]) != kTotalMessages) {
                return false;
            }
        }
        return true;
    }));

    for (auto& sub : subscribers) {
        sub->stop();
    }

    for (int i = 0; i < kSubscribers; ++i) {
        expectEachSequenceOnce(sequenceCounts[static_cast<std::size_t>(i)]);
    }
}

TEST(FanoutRouting, TenSubscribers_LargePayload_AllReceive) {
    constexpr std::size_t kPayloadSize = 5 * 1024;
    constexpr int kSubscribers = 10;

    std::string payload(kPayloadSize, '\0');
    for (std::size_t i = 0; i < kPayloadSize; ++i) {
        payload[i] = static_cast<char>(i % 256);
    }

    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
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
        subscribers.back()->subscribe(topic, [&, subIdx](const BrokerMessage& bm) {
            receivedCounts[static_cast<std::size_t>(subIdx)].fetch_add(1);
            const Message& m = bm.payload();
            receivedPayloads[static_cast<std::size_t>(subIdx)] =
                std::string(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
        });
    }

    std::this_thread::sleep_for(50ms);
    pub.publish(topic, payload);

    ASSERT_TRUE(waitUntil([&]() {
        for (int i = 0; i < kSubscribers; ++i) {
            if (receivedCounts[static_cast<std::size_t>(i)].load() != 1) {
                return false;
            }
        }
        return true;
    }));

    for (auto& sub : subscribers) {
        sub->stop();
    }

    for (int i = 0; i < kSubscribers; ++i) {
        EXPECT_EQ(receivedCounts[static_cast<std::size_t>(i)].load(), 1) << "subscriber " << i;
        EXPECT_EQ(receivedPayloads[static_cast<std::size_t>(i)].size(), kPayloadSize)
            << "subscriber " << i;
        EXPECT_EQ(receivedPayloads[static_cast<std::size_t>(i)], payload) << "subscriber " << i;
    }
}

TEST(FanoutRouting, PublishBeforeSubscribe_MessageLost) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    pub.publish("notifications", "early");

    std::atomic<int> count{0};
    Subscriber sub(dispatcher);
    sub.subscribe("notifications", [&](const BrokerMessage&) { ++count; });

    std::this_thread::sleep_for(500ms);
    sub.stop();
    EXPECT_EQ(count.load(), 0);
}

TEST(FanoutRouting, AfterStop_NoFurtherDelivery) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> count{0};
    Subscriber sub(dispatcher);
    sub.subscribe("notifications", [&](const BrokerMessage&) { ++count; });

    std::this_thread::sleep_for(50ms);
    pub.publish("notifications", "first");

    ASSERT_TRUE(waitUntil([&] { return count.load() == 1; }));

    sub.stop();
    pub.publish("notifications", "after-stop");

    std::this_thread::sleep_for(500ms);
    EXPECT_EQ(count.load(), 1);
}

TEST(FanoutRouting, TopicIsolation) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> topicACount{0};
    std::atomic<int> topicBCount{0};

    Subscriber subA(dispatcher);
    Subscriber subB(dispatcher);

    subA.subscribe("topic-a", [&](const BrokerMessage&) { ++topicACount; });
    subB.subscribe("topic-b", [&](const BrokerMessage&) { ++topicBCount; });

    std::this_thread::sleep_for(50ms);
    pub.publish("topic-a", "for-a");

    ASSERT_TRUE(waitUntil([&] { return topicACount.load() == 1; }));

    subA.stop();
    subB.stop();

    EXPECT_EQ(topicACount.load(), 1);
    EXPECT_EQ(topicBCount.load(), 0);
}

TEST(FanoutRouting, UniqueGroupsPerSubscriber) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);

    const SubscriptionToken token1 = dispatcher.subscribe("notifications");
    const SubscriptionToken token2 = dispatcher.subscribe("notifications");

    EXPECT_NE(token1.group, token2.group);
    EXPECT_EQ(broker.groupCount("notifications"), 2u);

    dispatcher.unsubscribe(token1);
    dispatcher.unsubscribe(token2);
}

TEST(FanoutRouting, Stop_UnsubscribesFanout) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    const std::string topic = "cleanup";

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe(topic, [](const BrokerMessage&) {});
    s2.subscribe(topic, [](const BrokerMessage&) {});

    ASSERT_EQ(broker.groupCount(topic), 2u);

    s1.stop();
    EXPECT_EQ(broker.groupCount(topic), 1u);

    s2.stop();
    EXPECT_EQ(broker.groupCount(topic), 0u);
}

TEST(FanoutStress, HundredPublishers_ThousandSubscribers_AllReceive) {
    constexpr int kPublishers = 100;
    constexpr int kSubscribers = 1000;
    constexpr int kMessagesPerPublisher = 1;
    constexpr int kTotalMessages = kPublishers * kMessagesPerPublisher;

    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    const std::string topic = "stress-fanout";

    auto receivedCounts =
        std::make_unique<std::atomic<int>[]>(static_cast<std::size_t>(kSubscribers));
    for (std::size_t i = 0; i < static_cast<std::size_t>(kSubscribers); ++i) {
        receivedCounts[i].store(0);
    }

    std::vector<std::unique_ptr<Subscriber>> subscribers;
    subscribers.reserve(static_cast<std::size_t>(kSubscribers));
    for (int i = 0; i < kSubscribers; ++i) {
        subscribers.push_back(std::make_unique<Subscriber>(dispatcher));
        const int subIdx = i;
        subscribers.back()->subscribe(topic, [&receivedCounts, subIdx](const BrokerMessage&) {
            receivedCounts[static_cast<std::size_t>(subIdx)].fetch_add(
                1, std::memory_order_relaxed);
        });
    }

    std::this_thread::sleep_for(200ms);
    ASSERT_EQ(broker.groupCount(topic), static_cast<std::size_t>(kSubscribers));
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

    ASSERT_TRUE(waitUntil([&]() {
        for (int i = 0; i < kSubscribers; ++i) {
            if (receivedCounts[static_cast<std::size_t>(i)].load(
                    std::memory_order_relaxed) != kTotalMessages) {
                return false;
            }
        }
        return true;
    }, 120s));

    for (auto& sub : subscribers) {
        sub->stop();
    }

    for (int i = 0; i < kSubscribers; ++i) {
        EXPECT_EQ(
            receivedCounts[static_cast<std::size_t>(i)].load(std::memory_order_relaxed),
            kTotalMessages)
            << "subscriber " << i;
    }

    subscribers.clear();

    EXPECT_EQ(broker.groupCount(topic), 0u);
    EXPECT_EQ(broker.totalGroupCount(), 0u);
    EXPECT_EQ(broker.subscriptionCount(), 0u);
}

TEST(FanoutBackpressure, DropOldest_DropsOldestWhenFull) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::DropOldest, kMaxSize));
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-drop-oldest";

    std::atomic<bool> processingPaused{true};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const BrokerMessage& m) {
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

TEST(FanoutBackpressure, RejectNew_RejectsWhenFull) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::RejectNew, kMaxSize));
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-reject-new";

    std::atomic<bool> processingPaused{true};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const BrokerMessage& m) {
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

TEST(FanoutBackpressure, Block_WaitsUntilSpace) {
    constexpr size_t kMaxSize = 3;
    constexpr int kPublishCount = 5;

    MessageBroker broker(backpressureConfig(BackpressurePolicy::Block, kMaxSize));
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);
    const std::string topic = "bp-block";

    std::atomic<bool> processingPaused{true};
    std::atomic<bool> publishDone{false};
    std::atomic<int> handlerInvocations{0};
    std::vector<std::string> received;
    std::mutex receivedMtx;

    Subscriber sub(dispatcher);
    sub.subscribe(topic, [&](const BrokerMessage& m) {
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
