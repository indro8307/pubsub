#include "message_queue.h"

// Legacy singleton for quick demos; main and tests should own a MessageBroker and inject it.
MessageBroker& getGlobalMessageBroker() {
    static MessageBroker broker;
    return broker;
}
