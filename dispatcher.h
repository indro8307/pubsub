#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "message_queue.h"
//#include "subscriber.h"
//#include "publisher.h"
class Dispatcher {
public:
virtual void publish(const std::string& topic, int id, const std::string& payload) = 0;
virtual MessageQueue& subscribe(const std::string& topic) = 0;
virtual void unsubscribe(const std::string& topic, MessageQueue& mq) = 0;
};

class CompeteConsumerDispatcher : public Dispatcher {
public:
    explicit CompeteConsumerDispatcher(MessageBroker& broker) : bro(broker) {}

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(payload.c_str(), payload.size());
        bro.competePublish(topic, msg);
    }
    MessageQueue& subscribe(const std::string& topic) override {
        return bro.competeSubscribe(topic);
    }
    void unsubscribe(const std::string& topic, MessageQueue& mq) override {
        bro.competeUnsubscribe(topic, mq);
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
    MessageQueue& subscribe(const std::string& topic) override {
        return bro.fanoutSubscribe(topic);
    }
    void unsubscribe(const std::string& topic, MessageQueue& mq) override {
        bro.fanoutUnsubscribe(topic, mq);
    }
private:   
     MessageBroker& bro;
};

#endif