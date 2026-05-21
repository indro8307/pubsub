#include "subscriber.h"
#include <iostream>
#include <stdexcept>

Subscriber::Subscriber(MessageBroker& broker, const std::string& topic)
    : broker(broker), topic(topic), running(false)
{}

void Subscriber::start(Handler handler){
    auto* topicPtr = broker.getTopic(topic);
    if (!topicPtr) {
        throw std::runtime_error("Topic does not exist: " + topic);
    }
    
    MessageQueue& queue = topicPtr->subscribe(*this);
    running = true;
    worker = std::thread([this,handler,&queue](){
        while (running) {
            Message m = queue.dequeue();
            handler(m);
        }
    });
}

void Subscriber::stop(){
    running = false;
    // Enqueue a sentinel (dummy) message to unblock the worker thread.
    // The worker thread is blocked in dequeue() waiting on cv.wait().
    // Without this, join() would hang indefinitely because the thread never
    // wakes up to check the running flag. The dummy message (id=-1) acts as
    // a signal to wake the thread, allowing it to receive the sentinel and
    // then check while(running), which is now false, and exit gracefully.
    // This is a classic sentinel pattern for graceful thread shutdown in producer-consumer systems.
    Message dummy(-1);
    broker.getQueue(topic).enqueue(dummy);
    if (worker.joinable()) worker.join();
}

Subscriber::~Subscriber(){
    stop();
}
