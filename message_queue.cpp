#include "message_queue.h"

MessageBroker& getGlobalMessageBroker() {
    static MessageBroker broker;
    return broker;
}
