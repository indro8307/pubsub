#ifndef TOPIC_H
#define TOPIC_H

#include "message_queue.h"
#include "subscriber.h"

class ITopic {
public:
virtual void publish(const Message& msg) = 0;
virtual void subscribe(Subscriber& sub) = 0;
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
    void publish(const Message& msg) override {
        // Implementation for publishing a message to the topic
    }
    void subscribe(Subscriber& sub) override {
        // Implementation for subscribing to the topic
    }
private:
    // shared queue
    MessageBroker broker;
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
    void publish(const Message& msg) override {
        // Implementation for publishing a message to the topic
    }
    void subscribe(Subscriber& sub) override {
        // Implementation for subscribing to the topic
    }
private:
    // list of subscribers subscribed to this topic
    std::vector<Subscriber&> subscribers;
};

#endif