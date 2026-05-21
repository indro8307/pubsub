#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#pragma once

#include <string>
#include <map>
#include <list>
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
    void createQueue(const std::string& key){
        std::unique_lock<std::mutex> lock(broker_mtx);
        queues.try_emplace(key);
    }
    MessageQueue& getQueue(const std::string& key){
        std::unique_lock<std::mutex> lock(broker_mtx);
        if (queues.find(key) == queues.end()) {
            queues.try_emplace(key);
        }
        return queues[key];
    }
    void createTopic(const std::string& topic, ITopic* topicObj) {
        std::unique_lock<std::mutex> lock(topic_mtx);
        topics[topic] = topicObj;
    }   
    ITopic* getTopic(const std::string& topic) {
        std::unique_lock<std::mutex> lock(topic_mtx);
        if (topics.find(topic) == topics.end()) {
            return nullptr;
        }
        return topics[topic];
    }
private:
    std::map<std::string, MessageQueue> queues;
    std::map<std::string, ITopic*> topics;
    std::mutex broker_mtx;
    std::mutex topic_mtx;
};


#endif