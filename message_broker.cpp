#include "message_broker.h"

#include <stdexcept>
#include <utility>
#include <thread>
#include <vector>

MessageBroker::MessageBroker(const MessageQueueConfig& config) : config(config) {
    if (this->config.maxSize == 0) {
        this->config.maxSize = 10000;
    }
}

SubscriptionToken MessageBroker::subscribe(const std::string topic, const std::string group) {
    std::unique_lock<std::mutex> lock(topic_mtx);
    auto [topic_it, topic_inserted] = topics.try_emplace(topic);
    auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
    group_it->second.memberCount++;
    uint64_t subscriptionId = nextSubscriptionId_.fetch_add(1);
    if (group_it->second.queue == nullptr) {
        group_it->second.queue = std::make_shared<MessageQueue>(config);
    }
    subscriptions[subscriptionId] = SubscriptionToken(topic, group, subscriptionId, group_it->second.queue);
    return subscriptions[subscriptionId];
}

void MessageBroker::decrementMemberCount(Group& group) {
    if (group.memberCount == 0) {
        throw std::logic_error("decrement MemberCount has come when it is already 0.");
    }
    group.memberCount--;
}

void MessageBroker::unsubscribe(const SubscriptionToken& token) {
    std::unique_lock<std::mutex> lock(topic_mtx);
    auto subscription_it = subscriptions.find(token.id);
    if (subscription_it == subscriptions.end()) {
        return;
    }
    if (subscription_it->second.topic != token.topic || subscription_it->second.group != token.group) {
        return;
    }
    auto topic_it = topics.find(token.topic);
    if (topic_it == topics.end()) {
        subscriptions.erase(subscription_it);
        return;
    }
    auto group_it = topic_it->second.groups.find(token.group);
    if (group_it == topic_it->second.groups.end()) {
        subscriptions.erase(subscription_it);
        return;
    }
    decrementMemberCount(group_it->second);
    if (group_it->second.memberCount == 0) {
        group_it->second.queue->close();
        group_it->second.queue.reset();
        topic_it->second.groups.erase(group_it);
    }
    if (topic_it->second.groups.empty()) {
        topics.erase(topic_it);
    }
    subscriptions.erase(subscription_it);
}

bool MessageBroker::publish(const std::string topic, const std::string group, const Message& msg, bool buffer) {
    // this method is called by the publisher to publish a message to a group which means only one queue.
    auto payload = std::make_shared<const Message>(msg);
    std::shared_ptr<const BrokerMessage> brokerMessage;
    std::shared_ptr<MessageQueue> mq;
    {
        std::unique_lock<std::mutex> lock(topic_mtx);

        if (buffer) {
            // if buffer is true it means we need to store the message even if no subscribers are present.
            // so we need to create a new topic and group if they don't exist.
            // and create a new message queue if it doesn't exist.
            // and enqueue the message to the message queue.
            auto [topic_it, topic_inserted] = topics.try_emplace(topic);
            auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
            if (group_it->second.queue == nullptr) {
                group_it->second.queue = std::make_shared<MessageQueue>(config);
            }
            const uint64_t sequence = topic_it->second.nextSeq++;
            brokerMessage = std::make_shared<const BrokerMessage>(sequence, payload);
            mq = group_it->second.queue;
        }
        else {
            auto topic_it = topics.find(topic);
            if (topic_it == topics.end()) {
                return false;
            }
            auto group_it = topic_it->second.groups.find(group);
            if (group_it == topic_it->second.groups.end()) {
                return false;
            }
            const uint64_t sequence = topic_it->second.nextSeq++;
            brokerMessage = std::make_shared<const BrokerMessage>(sequence, payload);
            mq = group_it->second.queue;
        }
    }
    // enqueue the message to the message queue out of the lock 
    // so that other threads can publish messages to other groups/topics.
    // so if publish blocks in one group, it will not block other groups.
    mq->enqueue(brokerMessage);
    return true;
}

bool MessageBroker::publish(const std::string topic, const Message& msg) {
    // Snapshot the target queues under topic_mtx, then enqueue outside the lock so a
    // slow/full queue does not stall other publishers or subscribe/unsubscribe calls.
    //
    // TRADE-OFF (serial fan-out): queues are enqueued one at a time. With
    // BackpressurePolicy::Block, a full queue blocks here until its consumer drains, which
    // delays delivery to the queues later in iteration order (head-of-line blocking within
    // a single publish). This is acceptable for now; non-blocking policies (DropOldest /
    // RejectNew) are unaffected.
    //
    // TODO: if slow-subscriber isolation under Block becomes a requirement, fan out enqueues
    // via a bounded thread pool (NOT a thread per queue, which does not scale to many
    // subscribers) so one slow queue cannot hold up the rest.
    auto payload = std::make_shared<const Message>(msg);
    std::shared_ptr<const BrokerMessage> brokerMessage;
    std::vector<std::shared_ptr<MessageQueue>> mqs;
    {
        std::unique_lock<std::mutex> lock(topic_mtx);
        auto topic_it = topics.find(topic);
        if (topic_it == topics.end()) {
            return false;
        }
        const uint64_t sequence = topic_it->second.nextSeq++;
        brokerMessage = std::make_shared<const BrokerMessage>(sequence, payload);
        // Copy the queue shared_ptrs locally. The shared_ptr keeps each queue alive even if
        // a concurrent unsubscribe removes the group from the map while we enqueue below.
        for (auto& [group_name, group] : topic_it->second.groups) {
            if (group.queue) {
                mqs.push_back(group.queue);
            }
        }
    }

    // Lock released: each MessageQueue has its own internal mutex, so concurrent enqueues
    // are safe without holding topic_mtx.
    for (auto& mq : mqs) {
        mq->enqueue(brokerMessage);
    }
    return true;
}

std::size_t MessageBroker::groupCount(const std::string& topic) const {
    std::unique_lock<std::mutex> lock(topic_mtx);
    auto topic_it = topics.find(topic);
    if (topic_it == topics.end()) {
        return 0;
    }
    return topic_it->second.groups.size();
}

std::size_t MessageBroker::subscriptionCount() const {
    std::unique_lock<std::mutex> lock(topic_mtx);
    return subscriptions.size();
}

std::size_t MessageBroker::totalGroupCount() const {
    std::unique_lock<std::mutex> lock(topic_mtx);
    std::size_t total = 0;
    for (const auto& entry : topics) {
        total += entry.second.groups.size();
    }
    return total;
}

MessageBroker& getGlobalMessageBroker() {
    static MessageBroker broker;
    return broker;
}
