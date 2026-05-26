#ifndef SUBSCRIBER_H
#define SUBSCRIBER_H

#include "message_queue.h"
#include <string>
#include <functional>
#include <thread>
#include <atomic>

class Subscriber {
public:
	using Handler = std::function<void(const Message&)>;
	Subscriber(Dispatcher& dispatcher);
	void subscribe(const std::string& topic, Handler handler);
	void stop();
	~Subscriber();
private:
	MessageQueue* mq;
	Dispatcher& dispatcher;
	std::thread worker;
	std::atomic<bool> running;
	std::string topic;
};

#endif

