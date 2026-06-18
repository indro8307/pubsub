#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "message_queue.h"
//#include "subscriber.h"
//#include "publisher.h"

class Dispatcher {
public:
virtual void publish(const std::string& topic, int id, const std::string& payload) = 0;
virtual SubscriptionToken subscribe(const std::string& topic) = 0;
virtual void unsubscribe(const SubscriptionToken& token) = 0;
};

class CompeteConsumerDispatcher : public Dispatcher {
public:
    explicit CompeteConsumerDispatcher(MessageBroker& broker) : bro(broker) {}

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
        // for compete consumer, generate a group name based on the topic. 
        // We will use the topic name as the group name.
        std::string group = topic;
        bro.publish(topic, group, msg, true);
    }

    SubscriptionToken subscribe(const std::string& topic) override {
        // for compete consumer, the topic name already acts as the group name
        std::string group = topic;
        return bro.subscribe(topic, group);
    }

    void unsubscribe(const SubscriptionToken& token) override {
        bro.unsubscribe(token);
    }
private:
    MessageBroker& bro;
};

class FanoutDispatcher : public Dispatcher {
public:
    explicit FanoutDispatcher(MessageBroker& broker) : bro(broker) {}

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
        // for fanout publish, group is not needed. Message will be broadcast to all subscribers.
        bro.publish(topic, msg);
    }
    SubscriptionToken subscribe(const std::string& topic) override {
        // for fanout subscribe generate a unique group name for each subscriber.
        uint64_t subscriberId = nextSubscriberId_.fetch_add(1);
        std::string group = topic + "_" + "sub_" + std::to_string(subscriberId);
        return bro.subscribe(topic, group);
    }
    void unsubscribe(const SubscriptionToken& token) override {
        bro.unsubscribe(token);
    }
private:   
     MessageBroker& bro;
     inline static std::atomic<uint64_t> nextSubscriberId_{1};
};

#endif