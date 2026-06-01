cmake_minimum_required(VERSION 3.10)
project(pubsub_demo)
set(CMAKE_CXX_STANDARD 17)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(pubsub
    main.cpp
    publisher.cpp
    subscriber.cpp
    message_queue.cpp
)
target_include_directories(pubsub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pubsub PRIVATE Threads::Threads)
