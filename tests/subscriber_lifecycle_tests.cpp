#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "dispatcher.h"
#include "message_queue.h"
#include "publisher.h"
#include "subscriber.h"
#include "test_helpers.h"

using namespace std::chrono_literals;
using test_helpers::waitUntil;

TEST(SubscriberLifecycle, DoubleSubscribe_Throws) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    sub.subscribe("t", [](const Message&) {});

    EXPECT_THROW(sub.subscribe("t", [](const Message&) {}), std::logic_error);

    sub.stop();
}

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

TEST(SubscriberLifecycle, StopWithoutSubscribe_NoHang) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    const auto start = std::chrono::steady_clock::now();
    sub.stop();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, 2s);
}

TEST(SubscriberLifecycle, DoubleStop_NoHang) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    sub.subscribe("t", [](const Message&) {});

    sub.stop();

    const auto start = std::chrono::steady_clock::now();
    sub.stop();
    const auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, 2s);
}

TEST(SubscriberLifecycle, SubscribeAfterStop_Throws) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    Subscriber sub(dispatcher);

    sub.subscribe("t", [](const Message&) {});
    sub.stop();

    EXPECT_THROW(sub.subscribe("t", [](const Message&) {}), std::logic_error);
}

TEST(SubscriberLifecycle, DestructorUnsubscribes) {
    MessageBroker broker;
    FanoutDispatcher dispatcher(broker);
    const std::string topic = "cleanup";

    {
        auto sub = std::make_unique<Subscriber>(dispatcher);
        sub->subscribe(topic, [](const Message&) {});
        ASSERT_EQ(broker.groupCount(topic), 1u);
    }

    EXPECT_EQ(broker.groupCount(topic), 0u);
    EXPECT_EQ(broker.subscriptionCount(), 0u);
}

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
