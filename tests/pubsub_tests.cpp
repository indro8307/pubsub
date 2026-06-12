#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "dispatcher.h"
#include "message_queue.h"
#include "publisher.h"
#include "subscriber.h"

namespace {

using namespace std::chrono_literals;

template <typename Pred>
bool waitUntil(Pred pred, std::chrono::milliseconds timeout = 3s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

}  // namespace

// --- Compete: one message, two subscribers, exactly one delivery ---

TEST(CompeteRouting, TwoSubscribers_OneMessage_OnlyOneReceives) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> sub1Count{0};
    std::atomic<int> sub2Count{0};

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe("orders", [&](const Message& m) {
        std::string payload(m.getPayload(), m.getSize());
        if (payload == "only-one") {
            ++sub1Count;
        }
    });
    s2.subscribe("orders", [&](const Message& m) {
        std::string payload(m.getPayload(), m.getSize());
        if (payload == "only-one") {
            ++sub2Count;
        }
    });

    std::this_thread::sleep_for(50ms);
    pub.publish("orders", "only-one");

    std::this_thread::sleep_for(2s); // wait for the message to be processed
    EXPECT_EQ(sub1Count.load() + sub2Count.load(), 1);

    s1.stop();
    s2.stop();
}

// --- Fan-out: one message, both subscribers receive same payload ---

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

    s1.subscribe("notifications", [&](const Message& m) {
        ++sub1Count;
        sub1Payload = std::string(m.getPayload(), m.getSize());
    });
    s2.subscribe("notifications", [&](const Message& m) {
        ++sub2Count;
        sub2Payload = std::string(m.getPayload(), m.getSize());
    });

    std::this_thread::sleep_for(50ms);
    pub.publish("notifications", "broadcast");

    std::this_thread::sleep_for(2s); // wait for the message to be processed

    EXPECT_EQ(sub1Count.load(), 1);
    EXPECT_EQ(sub2Count.load(), 1);
    EXPECT_EQ(sub1Payload, "broadcast");
    EXPECT_EQ(sub2Payload, "broadcast");

    s1.stop();
    s2.stop();
}

// --- Subscriber: double subscribe throws ---

TEST(SubscriberLifecycle, DoubleSubscribe_Throws) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    sub.subscribe("t", [](const Message&) {});

    EXPECT_THROW(sub.subscribe("t", [](const Message&) {}), std::logic_error);

    sub.stop();
}

// --- Subscriber: stop removes fan-out queue from broker ---

TEST(SubscriberLifecycle, Stop_UnsubscribesFanout) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    const std::string topic = "cleanup";

    Subscriber s1(dispatcher);
    Subscriber s2(dispatcher);

    s1.subscribe(topic, [](const Message&) {});
    s2.subscribe(topic, [](const Message&) {});

    ASSERT_EQ(broker.fanoutSubscriberCount(topic), 2u);

    s1.stop();
    EXPECT_EQ(broker.fanoutSubscriberCount(topic), 1u);

    s2.stop();
    EXPECT_EQ(broker.fanoutSubscriberCount(topic), 0u);
}

// --- Subscriber: stop returns within bounded time ---

TEST(SubscriberLifecycle, Stop_NoHang) {
    MessageBroker broker;
    CompeteConsumerDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    sub.subscribe("hang-test", [](const Message&) {
        std::this_thread::sleep_for(5ms);
    });

    const auto start = std::chrono::steady_clock::now();
    sub.stop();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, 2s);
}

// --- Subscriber: handler exception does not stop processing ---

TEST(SubscriberLifecycle, HandlerThrows_WorkerContinues) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Publisher pub(dispatcher);

    std::atomic<int> handlerCalls{0};
    std::atomic<int> successfulAfterThrow{0};

    Subscriber sub(dispatcher);
    sub.subscribe("errors", [&](const Message&) {
        const int n = ++handlerCalls;
        if (n == 1) {
            throw std::runtime_error("simulated handler failure");
        }
        ++successfulAfterThrow;
    });

    std::this_thread::sleep_for(50ms);
    pub.publish("errors", "first");
    pub.publish("errors", "second");

    ASSERT_TRUE(waitUntil([&] {
        return successfulAfterThrow.load() >= 1;
    }));

    EXPECT_GE(handlerCalls.load(), 2);
    EXPECT_EQ(successfulAfterThrow.load(), 1);

    sub.stop();
}

// --- Fan-out stress: many publishers, many subscribers, full cleanup ---

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
        subscribers.back()->subscribe(topic, [&receivedCounts, subIdx](const Message&) {
            receivedCounts[static_cast<std::size_t>(subIdx)].fetch_add(
                1, std::memory_order_relaxed);
        });
    }

    std::this_thread::sleep_for(200ms);
    ASSERT_EQ(broker.fanoutSubscriberCount(topic), static_cast<std::size_t>(kSubscribers));
    ASSERT_EQ(broker.fanoutSubscriptionCount(), static_cast<std::size_t>(kSubscribers));

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

    for (int i = 0; i < kSubscribers; ++i) {
        EXPECT_EQ(
            receivedCounts[static_cast<std::size_t>(i)].load(std::memory_order_relaxed),
            kTotalMessages)
            << "subscriber " << i;
    }

    for (auto& sub : subscribers) {
        sub->stop();
    }
    subscribers.clear();

    EXPECT_EQ(broker.fanoutSubscriberCount(topic), 0u);
    EXPECT_EQ(broker.fanoutTotalQueueCount(), 0u);
    EXPECT_EQ(broker.fanoutSubscriptionCount(), 0u);
}
