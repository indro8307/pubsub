#ifndef PUBLISHER_H
#define PUBLISHER_H

#include "message_queue.h"
#include "topic.h"
#include <string>
#include <functional>
#include <thread>
#include <atomic>

class Subscriber {
public:
	using Handler = std::function<void(const Message&)>;
	Subscriber(MessageBroker& broker, const std::string& topic);
	void start(Handler handler);
	void stop();   	
	std::string getSubName() const { return subName; }
	~Subscriber();
private:
	MessageBroker& broker;
	std::thread worker;
	std::atomic<bool> running;
	std::string topic;
	std::string subName;
};

#endif

