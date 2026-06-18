#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#pragma once

#include <string>
#include <map>
#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <atomic>
#include <stdexcept>

class Message {
public:
    Message(int id = 0): id(id), size(0) {}
    Message(const uint8_t* data, size_t len, int id = 0): id(id), payload(data, data + len), size(len) {}
    int getId() const { return id; }
    void setPayload(const uint8_t* data, size_t len){
        payload.assign(data, data + len);
        size = len;
    }
    const uint8_t* getPayload() const { return payload.data(); }
    size_t getSize() const { return size; }

private:
    int id;
    std::vector<uint8_t> payload;
    size_t size;
};

class MessageQueue {
public:
    void enqueue(std::shared_ptr<const Message> msg){
        std::unique_lock<std::mutex> lock(mtx);
        queue.push_back(msg);
        cv.notify_one();
    }
    std::shared_ptr<const Message> dequeue(){
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]{ return !queue.empty(); });
        std::shared_ptr<const Message> m = queue.front();
        queue.pop_front();
        return m;
    }
    bool dequeueFor(std::shared_ptr<const Message>& out, std::chrono::milliseconds timeout){
        std::unique_lock<std::mutex> lock(mtx);
        if (!cv.wait_for(lock, timeout, [this]{ return !queue.empty(); })) {
            return false;
        }
        out = queue.front();
        queue.pop_front();
        return true;
    }
private:
    std::list<std::shared_ptr<const Message>> queue;
    std::mutex mtx;
    std::condition_variable cv;
};

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
    };
    std::map<uint64_t, SubscriptionToken> subscriptions;
    std::map<std::string, Topic> topics;
    mutable std::mutex topic_mtx;
    std::atomic<uint64_t> nextSubscriptionId_ = 1;
public:
    SubscriptionToken subscribe(const std::string topic, const std::string group)
    {
        std::unique_lock<std::mutex> lock(topic_mtx);
        auto [topic_it, topic_inserted] = topics.try_emplace(topic);
        auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
        group_it->second.memberCount++;
        uint64_t subscriptionId = nextSubscriptionId_.fetch_add(1);
        if (group_it->second.queue == nullptr) {
            group_it->second.queue = std::make_shared<MessageQueue>();
        }
        subscriptions[subscriptionId] = SubscriptionToken(topic, group, subscriptionId, group_it->second.queue);
        return subscriptions[subscriptionId];
    }

    inline void decrementMemberCount(Group& group)
    {
        if (group.memberCount == 0) {
            // decrement member count has come when it is already 0.
            // Must be wrong logic. Throw an exception.
            throw std::logic_error("decrement MemberCount has come when it is already 0.");
        }
        group.memberCount--;
    }

    void unsubscribe(const SubscriptionToken& token) 
    { 
        std::unique_lock<std::mutex> lock(topic_mtx);
        auto subscription_it = subscriptions.find(token.id);
        if (subscription_it == subscriptions.end()) {
            return;
        }
        if(subscription_it->second.topic != token.topic || subscription_it->second.group != token.group) {
            return;
        }
        auto topic_it = topics.find(token.topic);
        if (topic_it == topics.end()) {
            // clear the subscription
            subscriptions.erase(subscription_it);
            return;
        }   
        auto group_it = topic_it->second.groups.find(token.group);
        if (group_it == topic_it->second.groups.end()) {
            // clear the subscription
            subscriptions.erase(subscription_it);
            return;
        }
        // decrement the member count
        decrementMemberCount(group_it->second);
        // if the member count is 0, delete the message queue and erase the group
        if (group_it->second.memberCount == 0) {
            group_it->second.queue.reset();
            topic_it->second.groups.erase(group_it);
        }
        // if the topic has no groups, erase the topic
        if (topic_it->second.groups.empty()) {
            topics.erase(topic_it);
        }

        // erase the subscription
        subscriptions.erase(subscription_it);
    }

    bool publish(const std::string topic, const std::string group, const Message& msg, bool buffer = true)
    {
        // if buffer is true then create topic , group and message queue and store the message in the queue if they don't exist.
        // if buffer is false then return false if the topic or group does not exist.
        std::unique_lock<std::mutex> lock(topic_mtx);
        if (buffer) {
            auto [topic_it, topic_inserted] = topics.try_emplace(topic);
            auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
            if (group_it->second.queue == nullptr) {
                group_it->second.queue = std::make_shared<MessageQueue>();
            }
            group_it->second.queue->enqueue(std::make_shared<const Message>(std::move(msg)));
            return true;
        }
        else{
            auto topic_it = topics.find(topic);
            if (topic_it == topics.end()) {
                return false;
            }
            auto group_it = topic_it->second.groups.find(group);
            if (group_it == topic_it->second.groups.end()) {
                return false;
            }

            group_it->second.queue->enqueue(std::make_shared<const Message>(std::move(msg)));
            return true;
        }
    }

    bool publish(const std::string topic, const Message& msg) {
        // publish to all groups in the topic
        std::unique_lock<std::mutex> lock(topic_mtx);
        if (topics.find(topic) == topics.end()) {
            // topic not found which means no subscribers yet. Message will be lost.
            return false;
        }
        // enqueue the message to all groups in the topic
        for (auto& group : topics[topic].groups) {
            group.second.queue->enqueue(std::make_shared<const Message>(std::move(msg)));
        }
        return true;
    }

    // Test / observability: topic-group subscription state.
    std::size_t groupCount(const std::string& topic) const {
        std::unique_lock<std::mutex> lock(topic_mtx);
        auto topic_it = topics.find(topic);
        if (topic_it == topics.end()) {
            return 0;
        }
        return topic_it->second.groups.size();
    }

    std::size_t subscriptionCount() const {
        std::unique_lock<std::mutex> lock(topic_mtx);
        return subscriptions.size();
    }

    std::size_t totalGroupCount() const {
        std::unique_lock<std::mutex> lock(topic_mtx);
        std::size_t total = 0;
        for (const auto& entry : topics) {
            total += entry.second.groups.size();
        }
        return total;
    }

};

// Optional convenience accessor; prefer injecting MessageBroker& from the composition root.
MessageBroker& getGlobalMessageBroker();

#endif