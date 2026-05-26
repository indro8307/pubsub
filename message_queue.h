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
private:
    std::list<Message> queue;
    std::mutex mtx;
    std::condition_variable cv;
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
            fanoutQueues[topic] = std::vector<MessageQueue>();
        }
    }

    MessageQueue& fanoutSubscribe(const std::string& topic) {
        std::unique_lock<std::mutex> lock(fo_mtx);
        auto it = fanoutQueues.find(topic);
        if (it == fanoutQueues.end()) {
            // topic not found in fanoutQueues. Create it.
            fanoutQueues[topic] = std::vector<MessageQueue>();
        }
        MessageQueue newQueue;
        fanoutQueues[topic].push_back(newQueue);
        return fanoutQueues[topic].back();
    }

    void competePublish(const std::string& topic, const Message& msg) {
        std::unique_lock<std::mutex> lock(sq_mtx);
        auto it = sharedQueues.find(topic);
        if (it != sharedQueues.end()) {
            it->second.enqueue(msg);
        }
        else{
            // topic not found in sharedQueues. Create a new topic and add insert a message queue.
            // messge will not be lost since it is enqueued.
            sharedQueues.emplace(topic, MessageQueue());
            sharedQueues[topic].enqueue(msg);
        }
    }

    MessageQueue& competeSubscribe(const std::string& topic) {
        std::unique_lock<std::mutex> lock(sq_mtx);
        auto it = sharedQueues.find(topic);
        if (it == sharedQueues.end()) {
            // topic not found in sharedQueues. Create a new topic and add insert a message queue.
            sharedQueues.emplace(topic, MessageQueue());
        }
        return sharedQueues[topic];
    }
private:
    std::map<std::string, MessageQueue> sharedQueues;
    std::map<std::string, std::vector<MessageQueue>> fanoutQueues; // for fanout topic
    //std::map<std::string, ITopic*> topics;
    std::mutex sq_mtx;
    std::mutex fo_mtx;
};

MessageBroker& getGlobalMessageBroker();

#endif