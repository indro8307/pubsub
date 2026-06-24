#ifndef MESSAGE_QUEUE_H
#define MESSAGE_QUEUE_H
#pragma once

#include <list>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <stdexcept>
#include <memory>

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

enum class BackpressurePolicy {
    Block,
    DropOldest,
    RejectNew
};

class MessageQueue;

class MessageQueueConfig {
public:
    BackpressurePolicy backpressurePolicy = BackpressurePolicy::DropOldest;
    size_t maxSize = 10000;
};

class BackPressureStrategy {
public:
    virtual bool try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) = 0;
    virtual std::shared_ptr<const Message> try_dequeue(MessageQueue& mq) = 0;
    virtual bool try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) = 0;
    virtual ~BackPressureStrategy() = default;
};

class BlockBackPressureStrategy : public BackPressureStrategy {
public:
    bool try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) override;
    std::shared_ptr<const Message> try_dequeue(MessageQueue& mq) override;
    bool try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) override;
};

class DropOldestBackPressureStrategy : public BackPressureStrategy {
public:
    bool try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) override;
    std::shared_ptr<const Message> try_dequeue(MessageQueue& mq) override;
    bool try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) override;
};

class RejectNewBackPressureStrategy : public BackPressureStrategy {
public:
    bool try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) override;
    std::shared_ptr<const Message> try_dequeue(MessageQueue& mq) override;
    bool try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) override;
};

class MessageQueue {
public:
    friend class BlockBackPressureStrategy;
    friend class DropOldestBackPressureStrategy;
    friend class RejectNewBackPressureStrategy;

    MessageQueue(const MessageQueueConfig& config = MessageQueueConfig()) : config(config) 
    {
        switch(config.backpressurePolicy) {
            case BackpressurePolicy::Block:
                strategy_ = std::make_unique<BlockBackPressureStrategy>();
                break;
            case BackpressurePolicy::DropOldest:
                strategy_ = std::make_unique<DropOldestBackPressureStrategy>();
                break;
            case BackpressurePolicy::RejectNew:
                strategy_ = std::make_unique<RejectNewBackPressureStrategy>();
                break;
            default:
                throw std::logic_error("Invalid backpressure policy");
        }
    }
    bool enqueue(std::shared_ptr<const Message> msg){
        return strategy_->try_enqueue(*this, msg);
    }
    std::shared_ptr<const Message> dequeue(){
        return strategy_->try_dequeue(*this);
    }
    bool dequeueFor(std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout){
        return strategy_->try_dequeueFor(*this, out, timeout);
    }
private:
    std::list<std::shared_ptr<const Message>> queue;
    MessageQueueConfig config;
    std::mutex mtx;
    std::condition_variable not_empty_cv;
    std::condition_variable not_full_cv;
    std::unique_ptr<BackPressureStrategy> strategy_;
};

#endif