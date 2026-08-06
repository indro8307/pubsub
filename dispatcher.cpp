#include "dispatcher.h"

#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include <uuid/uuid.h>

namespace {

std::string randomUuidV4() {
    uuid_t id;
    uuid_generate_random(id);
    char str[UUID_STR_LEN];
    uuid_unparse_lower(id, str);
    return std::string(str);
}

}  // namespace

CompeteConsumerDispatcher::CompeteConsumerDispatcher(MessageBroker& broker) : bro(broker) {}

void CompeteConsumerDispatcher::publish(const std::string& topic, int id, const std::string& payload) {
    Message msg(id);
    msg.setPayload(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    // for compete consumer, generate a group name based on the topic.
    // We will use the topic name as the group name.
    std::string group = topic;
    bro.publish(topic, group, msg, true);
}

SubscriptionToken CompeteConsumerDispatcher::subscribe(const std::string& topic) {
    // for compete consumer, the topic name already acts as the group name
    std::string group = topic;
    return bro.subscribe(topic, group);
}

void CompeteConsumerDispatcher::unsubscribe(const SubscriptionToken& token) {
    bro.unsubscribe(token);
}

FanoutDispatcher::FanoutDispatcher(MessageBroker& broker) : bro(broker) {}

void FanoutDispatcher::publish(const std::string& topic, int id, const std::string& payload) {
    Message msg(id);
    msg.setPayload(reinterpret_cast<const uint8_t*>(payload.data()), payload.size());
    // for fanout publish, group is not needed. Message will be broadcast to all subscribers.
    bro.publish(topic, msg);
}

SubscriptionToken FanoutDispatcher::subscribe(const std::string& topic) {
    // for fanout subscribe generate a unique group name for each subscriber.
    uint64_t subscriberId = nextSubscriberId_.fetch_add(1);
    std::string group = topic + "_" + "sub_" + std::to_string(subscriberId);
    return bro.subscribe(topic, group);
}

void FanoutDispatcher::unsubscribe(const SubscriptionToken& token) {
    bro.unsubscribe(token);
}

NetworkDispatcher::NetworkDispatcher(const std::string& host, int port) : broker_client_(host, port) {
    broker_client_.setDeliverMessageHandler(
        [this](const DeliverMessage& message) { handle_deliver_message(message); });
    broker_client_.setSubscribeAckHandler(
        [this](const SubscribeAck& ack) { on_subscribe_ack(ack); });
    broker_client_.start();
}

NetworkDispatcher::~NetworkDispatcher() {
    broker_client_.stop();
    broker_client_.setSubscribeAckHandler(nullptr);
    broker_client_.setDeliverMessageHandler(nullptr);
}

void NetworkDispatcher::handle_deliver_message(const DeliverMessage& message) {
    // create a new Message pointer and a new BrokerMessage pointer
    std::shared_ptr<Message> msg = std::make_shared<Message>();
    msg->setPayload(reinterpret_cast<const uint8_t*>(message.payload.data()), message.payload.size());
    std::shared_ptr<BrokerMessage> broker_message = std::make_shared<BrokerMessage>(message.sequence, msg);

    // find the subscription token from the map
    SubscriptionToken subscription_token;
    {
        std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
        auto it = subscription_tokens_.find(message.subscription_id);
        if (it != subscription_tokens_.end()) {
            subscription_token = it->second;
        } else {
            // TODO: handle unknown subscription id - buffer it.
            return;
        }
    }
    // enqueue the broker message to the message queue
    subscription_token.mq->enqueue(broker_message);
}

void NetworkDispatcher::publish(const std::string& topic, int id, const std::string& payload) {
    try {
        std::vector<uint8_t> frame_buffer;
        // Create and encode the frame header
        FrameHeader frame_header;
        frame_header.version = PROTOCOL_VERSION;
        frame_header.type = ProtocolFrameType::PUBLISH;
        encode_frame_header(frame_header, frame_buffer);

        // Create and encode the publish request
        PublishRequest publish_request;
        publish_request.request_id = broker_client_.generateRequestId();
        publish_request.topic = topic;
        publish_request.payload.assign(payload.begin(), payload.end());
        // TODO: Multiple copied happens here. Need to optimize.
        // 1. payload.assign - 1st copy
        // 2. copy inside encode - 2nd copy
        encode_publish_request(publish_request, frame_buffer);

        // Send the frame buffer to the broker client
        std::future<std::shared_ptr<RequestResult>> fut_ret =
            broker_client_.sendFrame(publish_request.request_id, frame_buffer);
        std::shared_ptr<RequestResult> ack = fut_ret.get();
        if (ack->type == ProtocolFrameType::PUBLISH_ACK) {
            PublishAck publish_ack = ack->publish_ack;
            if (publish_ack.result == PublishResult::ACCEPTED) {
                return;
            } else if (publish_ack.result == PublishResult::NO_SUBSCRIBERS) {
                // TODO: handle no subscribers
                return;
            } else {
                throw std::runtime_error(
                    "Unexpected publish status: " +
                    std::to_string(static_cast<int>(publish_ack.result)));
            }
        } else {
            throw std::runtime_error(
                "Unexpected response type: " + std::to_string(static_cast<int>(ack->type)));
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to publish message: " << e.what() << std::endl;
        throw std::runtime_error("Failed to publish message: " + std::string(e.what()));
    }
}

SubscriptionToken NetworkDispatcher::subscribe(const std::string& topic) {
    std::vector<uint8_t> frame_buffer;
    // create and encode the frame header
    FrameHeader frame_header;
    frame_header.version = PROTOCOL_VERSION;
    frame_header.type = ProtocolFrameType::SUBSCRIBE;
    encode_frame_header(frame_header, frame_buffer);

    // create and encode the subscribe request
    SubscribeRequest subscribe_request;
    subscribe_request.request_id = broker_client_.generateRequestId();
    subscribe_request.topic = topic;
    // Fan-out: unique group per subscriber (UUID so uniqueness holds across
    // processes and hosts). Distinct from the broker-assigned subscription_id.
    subscribe_request.group = topic + "_sub_" + randomUuidV4();
    encode_subscribe_request(subscribe_request, frame_buffer);

    // Pre-create the queue and register as pending by request_id so the
    // receive-thread subscribe_ack handler can publish subscription_id →
    // token before fulfillPromise (and thus before any following DELIVER).
    SubscriptionToken subscription_token;
    subscription_token.topic = topic;
    subscription_token.group = subscribe_request.group;
    subscription_token.mq = std::make_shared<MessageQueue>();
    {
        std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
        pending_by_request_id_[subscribe_request.request_id] = subscription_token;
    }

    std::future<std::shared_ptr<RequestResult>> fut_ret =
        broker_client_.sendFrame(subscribe_request.request_id, frame_buffer);
    std::shared_ptr<RequestResult> ack = fut_ret.get();

    auto clear_pending = [&]() {
        std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
        pending_by_request_id_.erase(subscribe_request.request_id);
    };

    if (ack->type == ProtocolFrameType::SUBSCRIBE_ACK) {
        SubscribeAck subscribe_ack = ack->subscribe_ack;
        if (subscribe_ack.subscription_id != 0) {
            std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
            // Should already have been moved by on_subscribe_ack; erase any stray pending.
            pending_by_request_id_.erase(subscribe_request.request_id);
            auto it = subscription_tokens_.find(subscribe_ack.subscription_id);
            if (it == subscription_tokens_.end()) {
                throw std::runtime_error(
                    "Subscribe ack handler did not register subscription for topic: " + topic);
            }
            return it->second;
        }
        clear_pending();
        throw std::runtime_error("Subscribe failed: Subscription id is 0 for topic: " + topic);
    }
    clear_pending();
    throw std::runtime_error(
        "Unexpected response type: " + std::to_string(static_cast<int>(ack->type)));
}

void NetworkDispatcher::unsubscribe(const SubscriptionToken& token) {
    if (token.id == 0) {
        return;
    }
    // Drop local routing first so in-flight DELIVERs cannot enqueue, then ask the broker to stop delivering.
    {
        std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
        subscription_tokens_.erase(token.id);
    }
    if (token.mq) {
        token.mq->close();
    }

    std::vector<uint8_t> frame_buffer;
    // create and encode the frame header
    FrameHeader frame_header;
    frame_header.version = PROTOCOL_VERSION;
    frame_header.type = ProtocolFrameType::UNSUBSCRIBE;
    encode_frame_header(frame_header, frame_buffer);

    // create and encode the unsubscribe request
    UnsubscribeRequest unsubscribe_request;
    unsubscribe_request.request_id = broker_client_.generateRequestId();
    unsubscribe_request.subscription_id = token.id;
    encode_unsubscribe_request(unsubscribe_request, frame_buffer);

    // send the frame buffer to the broker client
    std::future<std::shared_ptr<RequestResult>> fut_ret =
        broker_client_.sendFrame(unsubscribe_request.request_id, frame_buffer);
    std::shared_ptr<RequestResult> ack = fut_ret.get();
    if (ack->type == ProtocolFrameType::UNSUBSCRIBE_ACK) {
        if (ack->unsubscribe_ack.subscription_id != token.id) {
            throw std::runtime_error("UNSUBSCRIBE_ACK subscription_id mismatch");
        }
        return;
    }
    throw std::runtime_error(
        "Unexpected response type: " + std::to_string(static_cast<int>(ack->type)));
}

void NetworkDispatcher::on_subscribe_ack(const SubscribeAck& ack) {
    std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
    auto pending_it = pending_by_request_id_.find(ack.request_id);
    if (pending_it == pending_by_request_id_.end()) {
        return;
    }
    if (ack.subscription_id == 0) {
        // Leave pending for subscribe() to clean up and throw.
        return;
    }
    SubscriptionToken token = pending_it->second;
    token.id = ack.subscription_id;
    subscription_tokens_[ack.subscription_id] = token;
    pending_by_request_id_.erase(pending_it);
}

bool NetworkDispatcher::hasSubscription(uint64_t subscription_id) const {
    std::lock_guard<std::mutex> lock(subscription_tokens_mutex_);
    return subscription_tokens_.find(subscription_id) != subscription_tokens_.end();
}
