#include "subscriber.h"
#include "dispatcher.h"
#include "message_queue.h"
#include <iostream>
#include <stdexcept>
#include <chrono>

Subscriber::Subscriber(Dispatcher& dispatcher)
    : dispatcher(dispatcher), token_(), state(SubscriberState::idle)
{}

// subscribe to a topic and register a handler to process the messages
void Subscriber::subscribe(const std::string& topic, Handler handler)
{
    if (state.load(std::memory_order_acquire) != SubscriberState::idle) {
        throw std::logic_error("Subscriber::subscribe() called while a worker is already active");
    }
    std::unique_lock<std::mutex> lock(subscriber_mtx);
    token_ = dispatcher.subscribe(topic);
    state.store(SubscriberState::subscribed, std::memory_order_release);
    MessageQueue* const mq = token_.mq.get();
    worker = std::thread([this, handler, mq]() {
        while (state.load(std::memory_order_acquire) == SubscriberState::subscribed) {
            std::shared_ptr<const Message> m;
            const bool gotMessage = mq->dequeueFor(m, std::chrono::milliseconds(100));
            if (!gotMessage) {
                continue;
            }
            try {
                handler(*m);
            } catch (...) {
                // Handler errors must not terminate the worker thread.
            }
        }
    });
}

void Subscriber::stop(){
    std::unique_lock<std::mutex> lock(subscriber_mtx);
    state.store(SubscriberState::stopped, std::memory_order_release);
    if (worker.joinable()) {
        worker.join();
    }
    dispatcher.unsubscribe(token_);
    token_ = SubscriptionToken{};
}

Subscriber::~Subscriber(){
    stop();
}
