#include "topic.h"
#include <iostream>

void CompeteConsumerTopic::publish(const Message& msg) {
    // Implementation for publishing a message to the shared queue
    broker.getQueue(topic).enqueue(msg);
}


void CompeteConsumerTopic::subscribe(Subscriber& sub) {
    // Implementation for subscribing to the topic
    // In a compete consumer model, all subscribers share the same queue,
    // so we just need to ensure the subscriber is aware of the topic's queue.
    // The subscriber will keep listening for and dequeue messages from this shared queue.

    sub.start();
}