#include "publisher.h"

Publisher::Publisher(MessageBroker& broker, const std::string& topic)
	: broker(broker), topic(topic)
{
	broker.createQueue(topic);
}

void Publisher::publish(int id, const std::string& payload){
	Message m(id);
	m.setPayload(payload.c_str(), payload.size());
	broker.getQueue(topic).enqueue(m);
}
