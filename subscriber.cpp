#include "subscriber.h"
#include "dispatcher.h"
#include "message_queue.h"
#include <iostream>
#include <stdexcept>
#include <chrono>

Subscriber::Subscriber(Dispatcher& dispatcher)
    : dispatcher(dispatcher), running(false)
{}

void Subscriber::subscribe(const std::string& topic, Handler handler)
{
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
    running.store(false, std::memory_order_release);
    if (worker.joinable()) {
        worker.join();
    }
}

Subscriber::~Subscriber(){
    stop();
}
