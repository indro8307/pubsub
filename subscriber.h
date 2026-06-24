#ifndef SUBSCRIBER_H
#define SUBSCRIBER_H

#include "message_broker.h"
#include "dispatcher.h"
#include <string>
#include <functional>
#include <thread>
#include <atomic>
#include <mutex>

enum class SubscriberState { idle, subscribed, stopped };

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
	std::mutex subscriber_mtx;
	std::atomic<SubscriberState> state;
};

#endif
