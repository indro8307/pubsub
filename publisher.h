#ifndef PUBLISHER_H
#define PUBLISHER_H

#include <string>
//#include "message_queue.h"
#include "dispatcher.h"

class Publisher {
public:
	Publisher(Dispatcher& dispatcher);;
	void publish(const std::string& topic, const std::string& payload);

private:
    Dispatcher& dispatcher;
};
 
#endif

