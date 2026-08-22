#ifndef MESSAGE_BROKER_H
#define MESSAGE_BROKER_H
#pragma once

#include "message_queue.h"

#include <atomic>
#include <cstddef>
#include <map>
#include <mutex>
#include <string>

struct SubscriptionToken {
    std::string topic;
    std::string group;
    uint64_t id;
    std::shared_ptr<MessageQueue> mq;
    SubscriptionToken()
        : topic(""), group(""), id(0), mq(nullptr) {}
    SubscriptionToken(std::string topic, std::string group, uint64_t id, std::shared_ptr<MessageQueue> mq)
        : topic(std::move(topic)), group(std::move(group)), id(id), mq(mq) {}
    bool valid() const { return mq != nullptr; }
};

class MessageBroker {
    struct Group {
        std::shared_ptr<MessageQueue> queue;
        std::size_t memberCount = 0;
    };
    struct Topic {
        std::map<std::string, Group> groups;
        uint64_t nextSeq = 1;
    };
    std::map<uint64_t, SubscriptionToken> subscriptions;
    std::map<std::string, Topic> topics;
    mutable std::mutex topic_mtx;
    std::atomic<uint64_t> nextSubscriptionId_ = 1;
    MessageQueueConfig config;

public:
    explicit MessageBroker(const MessageQueueConfig& config = MessageQueueConfig());

    SubscriptionToken subscribe(const std::string topic, const std::string group);
    void unsubscribe(const SubscriptionToken& token);
    bool publish(const std::string topic, const std::string group, const Message& msg, bool buffer = true);
    bool publish(const std::string topic, const Message& msg);

    // Assign the next per-topic sequence under topic_mtx without enqueuing.
    // Returns false if the topic does not exist (same as publish(topic, msg)
    // with no subscribers). On success, writes the assigned sequence to |out_seq|.
    bool allocateSequence(const std::string& topic, uint64_t& out_seq);

    std::size_t groupCount(const std::string& topic) const;
    std::size_t subscriptionCount() const;
    std::size_t totalGroupCount() const;

private:
    void decrementMemberCount(Group& group);
};

MessageBroker& getGlobalMessageBroker();

#endif
