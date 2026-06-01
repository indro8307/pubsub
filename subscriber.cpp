#include "subscriber.h"
#include "dispatcher.h"
#include "message_queue.h"
#include <iostream>
#include <stdexcept>
#include <chrono>

Subscriber::Subscriber(Dispatcher& dispatcher)
    : dispatcher(dispatcher), running(false)
{}

// subscribe to a topic and register a handler to process the messages
void Subscriber::subscribe(const std::string& topic, Handler handler)
{
    if (worker.joinable()) {
        throw std::logic_error("Subscriber::subscribe() called while a worker is already active");
    }
    std::unique_lock<std::mutex> lock(subscriber_mtx);
    this->topic = topic;
    mq = &(dispatcher.subscribe(topic));
    running = true;
    worker = std::thread([this,handler](){
        while (running.load(std::memory_order_acquire)) {
            Message m;
            const bool gotMessage = mq->dequeueFor(m, std::chrono::milliseconds(100));
            if (!gotMessage) {
                continue;
            }
            handler(m);
        }
    });
}

void Subscriber::stop(){
    std::unique_lock<std::mutex> lock(subscriber_mtx);
    running.store(false, std::memory_order_release);
    if (worker.joinable()) {
        worker.join();
        dispatcher.unsubscribe(topic, *mq);
        mq = nullptr;
        topic.clear();
    }
}

Subscriber::~Subscriber(){
    stop();
}
