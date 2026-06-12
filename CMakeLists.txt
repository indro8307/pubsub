cmake_minimum_required(VERSION 3.14)
project(pubsub_demo)
set(CMAKE_CXX_STANDARD 17)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(PUBSUB_SOURCES
    publisher.cpp
    subscriber.cpp
    message_queue.cpp
)

add_executable(pubsub main.cpp ${PUBSUB_SOURCES})
target_include_directories(pubsub PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pubsub PRIVATE Threads::Threads)

# --- GoogleTest (FetchContent) ---
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows: prevent overriding parent project's runtime settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(pubsub_tests tests/pubsub_tests.cpp ${PUBSUB_SOURCES})
target_include_directories(pubsub_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(pubsub_tests PRIVATE GTest::gtest_main Threads::Threads)

include(GoogleTest)
gtest_discover_tests(pubsub_tests)
