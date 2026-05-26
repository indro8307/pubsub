#include "subscriber.h"
#include "dispatcher.h"
#include "message_queue.h"
#include <iostream>
#include <stdexcept>

Subscriber::Subscriber(Dispatcher& dispatcher)
    : dispatcher(dispatcher), running(false)
{}

void Subscriber::subscribe(const std::string& topic, Handler handler)
{
    this->topic = topic;
    mq = &(dispatcher.subscribe(topic));
    running = true;
    worker = std::thread([this,handler](){
        while (running) {
            Message m = mq->dequeue();
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
    mq->enqueue(dummy);
    // dispatcher.unsubscribe(topic); // Assuming this method exists to clean up the subscription
    if (worker.joinable()) worker.join();
}

Subscriber::~Subscriber(){
    stop();
}
