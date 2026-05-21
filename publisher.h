#ifndef PUBLISHER_H
#define PUBLISHER_H

#include <string>
//#include "message_queue.h"
#include "topic.h"

class Publisher {
public:
	Publisher(MessageBroker& broker, const std::string& topic);
	void publish(int id, const std::string& payload);
private:
    ITopic& topic;
};
 
#endif

