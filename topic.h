#ifndef TOPIC_H
#define TOPIC_H

#include "message_queue.h"
#include "subscriber.h"

class ITopic {
public:
virtual void publish(const Message& msg) = 0;
virtual MessageQueue& subscribe(Subscriber& sub) = 0;
};

/*
Topic
 └── One shared queue
      ├── Subscriber A dequeues
      └── Subscriber B dequeues

One topic, one queue, multiple subscribers. Each message is consumed by only one subscriber.
Helpful for task distribution.
      */
class CompeteConsumerTopic : public ITopic {
public:
    CompeteConsumerTopic(MessageBroker& broker, const std::string& topic)
        : sharedQueue(broker.getQueue(topic)) {}

    void publish(const Message& msg) override {
        // Implementation for publishing a message to the topic
        sharedQueue.enqueue(msg);
    }

    MessageQueue& subscribe(Subscriber& sub) override {
        // Implementation for subscribing to the topic
        return sharedQueue; // All subscribers share the same queue, so we just return the reference to it. The subscriber will keep listening for and dequeue messages from this shared queue.
    }
private:
    // shared queue
    MessageQueue& sharedQueue;
    std::string topic;
};

/*
Topic
 ├── Subscriber A -> Queue A
 ├── Subscriber B -> Queue B
 └── Subscriber C -> Queue C
 
 each subscriber has its own queue. Each message is delivered to all subscribers.
 Helpful for event broadcasting.
 */
class FanoutTopic: public ITopic {
public:
    FanoutTopic(MessageBroker& broker, const std::string& topic)
        : bro(broker) {}

    void publish(const Message& msg) override {
        // Implementation for publishing a message to the topic
        for(auto& q : subQueues) {
            q.enqueue(msg);
        }
    }
    MessageQueue& subscribe(Subscriber& sub) override {
        // Implementation for subscribing to the topic
        subQueues.push_back(bro.getQueue(sub.getSubName())); // Create a queue for this subscriber
        return subQueues.back();
    }
private:
    // list of queues for each subscriber
    std::vector<MessageQueue&> subQueues;
    MessageBroker& bro;
    std::string topic;
};

#endif