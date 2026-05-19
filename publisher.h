#ifndef PUBLISHER_H
#define PUBLISHER_H

#include <string>
#include "message_queue.h"

class Publisher {
public:
	Publisher(MessageBroker& broker, const std::string& topic);
	void publish(int id, const std::string& payload);
private:
	MessageBroker& broker;
	std::string topic;
};
 
#endif

