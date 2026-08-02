#ifndef DISPATCHER_H
#define DISPATCHER_H

#include "message_broker.h"
#include "broker_client.h"
#include "protocol_frame.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

class Dispatcher {
public:
    virtual ~Dispatcher() = default;
    virtual void publish(const std::string& topic, int id, const std::string& payload) = 0;
    virtual SubscriptionToken subscribe(const std::string& topic) = 0;
    virtual void unsubscribe(const SubscriptionToken& token) = 0;
};

class CompeteConsumerDispatcher : public Dispatcher {
public:
    explicit CompeteConsumerDispatcher(MessageBroker& broker);

    void publish(const std::string& topic, int id, const std::string& payload) override;
    SubscriptionToken subscribe(const std::string& topic) override;
    void unsubscribe(const SubscriptionToken& token) override;

private:
    MessageBroker& bro;
};

class FanoutDispatcher : public Dispatcher {
public:
    explicit FanoutDispatcher(MessageBroker& broker);

    void publish(const std::string& topic, int id, const std::string& payload) override;
    SubscriptionToken subscribe(const std::string& topic) override;
    void unsubscribe(const SubscriptionToken& token) override;

private:
    MessageBroker& bro;
    inline static std::atomic<uint64_t> nextSubscriberId_{1};
};

class NetworkDispatcher : public Dispatcher {
public:
    NetworkDispatcher(const std::string& host, int port);
    ~NetworkDispatcher() override;

    void handle_deliver_message(const DeliverMessage& message);

    void publish(const std::string& topic, int id, const std::string& payload) override;
    SubscriptionToken subscribe(const std::string& topic) override;
    void unsubscribe(const SubscriptionToken& token) override;

private:
    void on_subscribe_ack(const SubscribeAck& ack);

    BrokerClient broker_client_;
    std::atomic<uint64_t> next_subscription_id_{1};
    std::map<uint64_t, SubscriptionToken> subscription_tokens_;
    std::map<uint32_t, SubscriptionToken> pending_by_request_id_;
    std::mutex subscription_tokens_mutex_;
};

#endif
