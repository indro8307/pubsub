cmake_minimum_required(VERSION 3.14)
project(pubsub_demo)
set(CMAKE_CXX_STANDARD 17)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

find_path(UUID_INCLUDE_DIR NAMES uuid/uuid.h)
find_library(UUID_LIBRARY NAMES uuid)
if(NOT UUID_INCLUDE_DIR OR NOT UUID_LIBRARY)
    message(FATAL_ERROR "libuuid not found (need uuid/uuid.h and -luuid). On Debian/Ubuntu: sudo apt install uuid-dev")
endif()

set(PUBSUB_SOURCES
    publisher.cpp
    subscriber.cpp
    message_queue.cpp
    message_broker.cpp
    protocol_frame.cpp
    session.cpp
    broker_server.cpp
    broker_client.cpp
    dispatcher.cpp
)

add_executable(pubsub main.cpp ${PUBSUB_SOURCES})
target_include_directories(pubsub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${UUID_INCLUDE_DIR})
target_link_libraries(pubsub PRIVATE Threads::Threads ${UUID_LIBRARY})

# E2E load generator (expects an already-running BrokerServer).
add_executable(loadgen loadgen/main.cpp ${PUBSUB_SOURCES})
target_include_directories(loadgen PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${UUID_INCLUDE_DIR})
target_link_libraries(loadgen PRIVATE Threads::Threads ${UUID_LIBRARY})

# Standalone TCP broker process (BrokerServer + MessageBroker).
add_executable(broker broker_main.cpp ${PUBSUB_SOURCES})
target_include_directories(broker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${UUID_INCLUDE_DIR})
target_link_libraries(broker PRIVATE Threads::Threads ${UUID_LIBRARY})

# --- GoogleTest (FetchContent) ---
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows: prevent overriding parent project's runtime settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)
enable_testing()

set(PUBSUB_TEST_SUITES
    message_broker_tests
    compete_tests
    fanout_tests
    subscriber_lifecycle_tests
    protocol_frame_tests
    network_tests
)

foreach(test_suite IN LISTS PUBSUB_TEST_SUITES)
    add_executable(${test_suite} tests/${test_suite}.cpp ${PUBSUB_SOURCES})
    target_include_directories(${test_suite} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR} ${UUID_INCLUDE_DIR})
    target_link_libraries(${test_suite} PRIVATE GTest::gtest_main Threads::Threads ${UUID_LIBRARY})
    gtest_discover_tests(${test_suite})
endforeach()
