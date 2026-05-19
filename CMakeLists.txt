cmake_minimum_required(VERSION 3.10)
project(pubsub_demo)
set(CMAKE_CXX_STANDARD 17)

add_executable(pubsub main.cpp publisher.cpp subscriber.cpp)
target_include_directories(pubsub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
