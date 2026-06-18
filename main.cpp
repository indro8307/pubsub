#include <iostream>
#include <memory>
#include <thread>
#include <chrono>
#include "message_queue.h"
#include "publisher.h"
#include "subscriber.h"

int main(){
    
    std::cout << "=== Competing Consumer System ===" << std::endl;
    
    // Competing Consumer Pattern: Multiple subscribers compete for messages on the same topic
    // Only one subscriber receives each message
    MessageBroker competeBroker;
    auto competeDispatcher1 = std::make_unique<CompeteConsumerDispatcher>(competeBroker);
    auto competeDispatcher2 = std::make_unique<CompeteConsumerDispatcher>(competeBroker);

    Publisher pub1(*competeDispatcher1);
    Publisher pub2(*competeDispatcher2);

    Subscriber sub1(*competeDispatcher1);
    Subscriber sub2(*competeDispatcher1);

    sub1.subscribe("orders", [](const Message& m){
        if (m.getId() >= 0) {
            std::string s(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
            std::cout << "[Compete] Subscriber1 received payload=" << s << std::endl;
        }
    });

    sub2.subscribe("orders", [](const Message& m){
        if (m.getId() >= 0) {
            std::string s(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
            std::cout << "[Compete] Subscriber2 received payload=" << s << std::endl;
        }
    });

    std::thread t1([&](){
        for (int i = 0; i < 5; ++i) {
            std::cout << "[Compete] Publisher1 publishing message " << i << std::endl;
            pub1.publish("orders", std::string("order_") + std::to_string(i) + "_from_pub1");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    std::thread t2([&](){
        for (int i = 0; i < 5; ++i) {
            std::cout << "[Compete] Publisher2 publishing message " << i << std::endl;
            pub2.publish("orders", std::string("order_") + std::to_string(i) + "_from_pub2");
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
    });

    t1.join();
    t2.join();

    // allow subscribers to process remaining messages
    std::this_thread::sleep_for(std::chrono::seconds(1));

    sub1.stop();
    sub2.stop();

    std::cout << "\n=== Fan-Out System ===" << std::endl;
    
    // Fan-Out Pattern: All subscribers receive copies of every message on the topic
    MessageBroker fanoutBroker;
    auto fanoutDispatcher1 = std::make_unique<FanoutDispatcher>(fanoutBroker);
    auto fanoutDispatcher2 = std::make_unique<FanoutDispatcher>(fanoutBroker);

    Publisher pub3(*fanoutDispatcher1);
    Publisher pub4(*fanoutDispatcher2);

    Subscriber sub3(*fanoutDispatcher1);
    Subscriber sub4(*fanoutDispatcher1);

    sub3.subscribe("notifications", [](const Message& m){
        if (m.getId() >= 0) {
            std::string s(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
            std::cout << "[FanOut] Subscriber3 received payload=" << s << std::endl;
        }
    });

    sub4.subscribe("notifications", [](const Message& m){
        if (m.getId() >= 0) {
            std::string s(reinterpret_cast<const char*>(m.getPayload()), m.getSize());
            std::cout << "[FanOut] Subscriber4 received payload=" << s << std::endl;
        }
    });

    std::thread t3([&](){
        for (int i = 0; i < 5; ++i) {
            std::cout << "[FanOut] Publisher3 publishing message " << i << std::endl;
            pub3.publish("notifications", std::string("notification_") + std::to_string(i) + "_from_pub3");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    std::thread t4([&](){
        for (int i = 0; i < 5; ++i) {
            std::cout << "[FanOut] Publisher4 publishing message " << i << std::endl;
            pub4.publish("notifications", std::string("notification_") + std::to_string(i) + "_from_pub4");
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
        }
    });

    t3.join();
    t4.join();

    // allow subscribers to process remaining messages
    std::this_thread::sleep_for(std::chrono::seconds(1));

    sub3.stop();
    sub4.stop();

    std::cout << "\n=== Demo Complete ===" << std::endl;
    return 0;
}


