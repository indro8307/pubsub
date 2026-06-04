#ifndef SUBSCRIBER_H
#define SUBSCRIBER_H

#include "message_queue.h"
#include "dispatcher.h"
#include <string>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>

class Subscriber {
public:
	using Handler = std::function<void(const Message&)>;
	Subscriber(Dispatcher& dispatcher);
	void subscribe(const std::string& topic, Handler handler);
	void stop();
	~Subscriber();
private:
	SubscriptionToken token_;
	Dispatcher& dispatcher;
	std::thread worker;
	std::atomic<bool> running;
	std::mutex subscriber_mtx;
};

#endif
