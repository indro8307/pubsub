#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "message_queue.h"
//#include "subscriber.h"
//#include "publisher.h"
class Dispatcher {
public:
virtual void publish(const std::string& topic, int id, const std::string& payload) = 0;
virtual MessageQueue& subscribe(const std::string& topic) = 0;
};

class CompeteConsumerDispatcher : public Dispatcher {
public:
    CompeteConsumerDispatcher() : bro(getGlobalMessageBroker()) {} 

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(payload.c_str(), payload.size());
        bro.competePublish(topic, msg);
    }
    MessageQueue& subscribe(const std::string& topic) override {
        return bro.competeSubscribe(topic); // Assuming MessageQueue has a getId() method to return the subscriber ID
    }
private:
    MessageBroker& bro;
};

class FanoutDispatcher : public Dispatcher {
public:
    FanoutDispatcher() : bro(getGlobalMessageBroker()) {}

    void publish(const std::string& topic, int id, const std::string& payload) override {
        Message msg(id);
        msg.setPayload(payload.c_str(), payload.size());
        bro.fanoutPublish(topic, msg);
    }
    MessageQueue& subscribe(const std::string& topic) override {
        return bro.fanoutSubscribe(topic);
    }
private:   
     MessageBroker& bro;
};

#endif