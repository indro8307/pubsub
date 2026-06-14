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

class Message {
public:
    Message(int id = 0): id(id), size(0) {}
    int getId() const { return id; }
    void setPayload(const char* data, size_t len){
        size = (len < sizeof(payload)) ? len : sizeof(payload);
        memcpy(payload, data, size);
    }
    const char* getPayload() const { return payload; }
    size_t getSize() const { return size; }

private:
    int id;
    char payload[4096];
    size_t size;
};

class MessageQueue {
public:
    void enqueue(const Message& msg){
        std::unique_lock<std::mutex> lock(mtx);
        queue.push_back(msg);
        cv.notify_one();
    }
    Message dequeue(){
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]{ return !queue.empty(); });
        Message m = queue.front();
        queue.pop_front();
        return m;
    }
    bool dequeueFor(Message& out, std::chrono::milliseconds timeout){
        std::unique_lock<std::mutex> lock(mtx);
        if (!cv.wait_for(lock, timeout, [this]{ return !queue.empty(); })) {
            return false;
        }
        out = queue.front();
        queue.pop_front();
        return true;
    }
private:
    std::list<Message> queue;
    std::mutex mtx;
    std::condition_variable cv;
};

struct SubscriptionToken {
    std::string topic;
    std::string group;
    uint64_t id;
    std::shared_ptr<MessageQueue> mq;
    SubscriptionToken(std::string topic, std::string group, uint64_t id, std::shared_ptr<MessageQueue> mq)
        : topic(std::move(topic)), group(std::move(group)), id(id), mq(mq) {}
    bool valid() const { return mq != nullptr; }
};

class MessageBroker {
public:
    void fanoutPublish(const std::string& topic, const Message& msg) {
        std::unique_lock<std::mutex> lock(fo_mtx);
        auto it = fanoutQueues.find(topic);
        if (it != fanoutQueues.end()) {
            for (auto& q : it->second) {
                q.enqueue(msg);
            }
        }
        else{
            // topic not found in fanoutQueues. Create it. Message will be lost since no subscribers yet, but that's acceptable in a pub-sub system.
            fanoutQueues[topic] = std::list<MessageQueue>();
        }
    }

    SubscriptionToken fanoutSubscribe(const std::string& topic) {
        std::unique_lock<std::mutex> lock(fo_mtx);
        auto it = fanoutQueues.find(topic);
        if (it == fanoutQueues.end()) {
            // topic not found in fanoutQueues. Create it.
            fanoutQueues[topic] = std::list<MessageQueue>();
        }
        fanoutQueues[topic].emplace_back();
        auto& mq = fanoutQueues[topic].back();
        //insert the std::list<MessageQueue>::iterator of the last element into the map
        const uint64_t id = fanoutSubscriptionId.fetch_add(1);
        fanoutSubscriptions[id] = std::prev(fanoutQueues[topic].end());
        return SubscriptionToken(topic, id, SubscriptionToken::Type::Fanout, &mq);
    }

    SubscriptionToken subscribe(const std::string topic, const std::string group)
    {
        std::unique_lock<std::mutex> lock(topic_mtx);
        auto [topic_it, topic_inserted] = topics.try_emplace(topic);
        auto [group_it, group_inserted] = topic_it->second.groups.try_emplace(group);
        group_it->second.memberCount++;
        return SubscriptionToken(topic, group_it->second.memberCount, SubscriptionToken::Type::Compete, group_it->second.queue.get());
    }
    void unsubscribe(const SubscriptionToken& token) { topics.erase(token.topic); }
    void competePublish(const std::string& topic, const Message& msg) { topics[topic].enqueue(msg); }

    void competeUnsubscribe(const SubscriptionToken& token) {
        // nothing to do here as the message queue is shared between all subscribers.
    }

    void fanoutUnsubscribe(const SubscriptionToken& token) {
        std::unique_lock<std::mutex> lock(fo_mtx);
        auto topicIt = fanoutQueues.find(token.topic);
        if (topicIt == fanoutQueues.end()) {
            return;
        }
        auto subIt = fanoutSubscriptions.find(token.id);
        if (subIt == fanoutSubscriptions.end()) {
            return;
        }
        topicIt->second.erase(subIt->second);
        fanoutSubscriptions.erase(subIt);
        if (topicIt->second.empty()) {
            fanoutQueues.erase(topicIt);
        }
    }

    // Test / observability: number of fan-out subscriber queues for a topic.
    std::size_t fanoutSubscriberCount(const std::string& topic) {
        std::unique_lock<std::mutex> lock(fo_mtx);
        auto it = fanoutQueues.find(topic);
        if (it == fanoutQueues.end()) {
            return 0;
        }
        return it->second.size();
    }

    std::size_t fanoutSubscriptionCount() {
        std::unique_lock<std::mutex> lock(fo_mtx);
        return fanoutSubscriptions.size();
    }

    std::size_t fanoutTotalQueueCount() {
        std::unique_lock<std::mutex> lock(fo_mtx);
        std::size_t total = 0;
        for (const auto& entry : fanoutQueues) {
            total += entry.second.size();
        }
        return total;
    }



private:
    struct Group{
        std::shared_ptr<MessageQueue> queue;
        std::size_t memberCount = 0
    };
    struct Topic{
        std::map<std::string, Group> groups;
    };
    std::map<std::string, Topic> topics;
    std::mutex topic_mtx;
    //std::map<std::string, MessageQueue> sharedQueues;
    //std::map<std::string, std::list<MessageQueue>> fanoutQueues; // for fanout topic
    std::map<uint64_t, std::list<MessageQueue>::iterator> fanoutSubscriptions;
    std::mutex sq_mtx;
    std::mutex fo_mtx;
    std::atomic<uint64_t> fanoutSubscriptionId;
    std::atomic<uint64_t> competeSubscriptionId;
};

// Optional convenience accessor; prefer injecting MessageBroker& from the composition root.
MessageBroker& getGlobalMessageBroker();

#endif