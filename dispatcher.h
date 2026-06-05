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
        msg.setPayload(payload.c_str(), payload.size());
        bro.competePublish(topic, msg);
    }
    SubscriptionToken subscribe(const std::string& topic) override {
        return bro.competeSubscribe(topic);
    }
    void unsubscribe(const SubscriptionToken& token) override {
        bro.competeUnsubscribe(token);
    }
private:
    MessageBroker& bro;
};

class FanoutDispatcher : public Dispatcher {
public:
    explicit FanoutDispatcher(MessageBroker& broker) : bro(broker) {}

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(payload.c_str(), payload.size());
        bro.fanoutPublish(topic, msg);
    }
    SubscriptionToken subscribe(const std::string& topic) override {
        return bro.fanoutSubscribe(topic);
    }
    void unsubscribe(const SubscriptionToken& token) override {
        bro.fanoutUnsubscribe(token);
    }
private:   
     MessageBroker& bro;
};

#endif