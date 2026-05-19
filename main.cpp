#include <iostream>
#include <thread>
#include <chrono>
#include "message_queue.h"
#include "publisher.h"
#include "subscriber.h"

int main(){
    MessageBroker broker;

    Publisher pub1(broker, "topic1");
    Publisher pub2(broker, "topic1");

    Subscriber sub1(broker, "topic1");
    Subscriber sub2(broker, "topic1");

    sub1.start([](const Message& m){
        if (m.getId() >= 0) {
            std::string s(m.getPayload(), m.getSize());
            std::cout << "Subscriber1 received id=" << m.getId() << " payload=" << s << std::endl;
        }
    });

    sub2.start([](const Message& m){
        if (m.getId() >= 0) {
            std::string s(m.getPayload(), m.getSize());
            std::cout << "Subscriber2 received id=" << m.getId() << " payload=" << s << std::endl;
        }
    });

    std::thread t1([&](){
        for (int i = 0; i < 10; ++i) {
            std::cout << "Publisher1 publishing id=" << i << std::endl;
            pub1.publish(i, std::string("hello from pub1 ") + std::to_string(i));
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    std::thread t2([&](){
        for (int i = 0; i < 10; ++i) {
            std::cout << "Publisher2 publishing id=" << 100 + i << std::endl;
            pub2.publish(100 + i, std::string("pub2 says ") + std::to_string(i));
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
    });

    t1.join();
    t2.join();

    // allow subscribers to process remaining messages
    std::this_thread::sleep_for(std::chrono::seconds(1));

    sub1.stop();
    sub2.stop();
    return 0;
}


