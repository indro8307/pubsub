#include "message_broker.h"

#include <stdexcept>
#include <utility>

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
        group_it->second.queue.reset();
        topic_it->second.groups.erase(group_it);
    }
    if (topic_it->second.groups.empty()) {
        topics.erase(topic_it);
    }
    subscriptions.erase(subscription_it);
}

bool MessageBroker::publish(const std::string topic, const std::string group, const Message& msg, bool buffer) {
    std::unique_lock<std::mutex> lock(topic_mtx);
    if (buffer) {
        auto [topic_it, topic_inserted] = topics.try_emplace(topic);
        auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
        if (group_it->second.queue == nullptr) {
            group_it->second.queue = std::make_shared<MessageQueue>(config);
        }
        group_it->second.queue->enqueue(std::make_shared<const Message>(msg));
        return true;
    }
    auto topic_it = topics.find(topic);
    if (topic_it == topics.end()) {
        return false;
    }
    auto group_it = topic_it->second.groups.find(group);
    if (group_it == topic_it->second.groups.end()) {
        return false;
    }
    group_it->second.queue->enqueue(std::make_shared<const Message>(msg));
    return true;
}

bool MessageBroker::publish(const std::string topic, const Message& msg) {
    std::unique_lock<std::mutex> lock(topic_mtx);
    if (topics.find(topic) == topics.end()) {
        return false;
    }
    for (auto& group : topics[topic].groups) {
        group.second.queue->enqueue(std::make_shared<const Message>(msg));
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
