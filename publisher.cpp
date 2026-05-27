#include "publisher.h"

Publisher::Publisher(Dispatcher& dispatcher)
	: dispatcher(dispatcher) {}

void Publisher::publish(const std::string& topic, const std::string& payload)
{
	dispatcher.publish(topic, 0, payload); // Using 0 as a placeholder for message ID
}
