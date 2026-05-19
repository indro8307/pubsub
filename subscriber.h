#pragma once
#include "message_queue.h"
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
	~Subscriber();
private:
	MessageBroker& broker;
	std::string topic;
	std::thread worker;
	std::atomic<bool> running;
};

