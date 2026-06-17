#ifndef TEST_HELPERS_H
#define TEST_HELPERS_H

#include <chrono>
#include <thread>

namespace test_helpers {

using namespace std::chrono_literals;

template <typename Pred>
bool waitUntil(Pred pred, std::chrono::milliseconds timeout = 3s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(10ms);
    }
    return pred();
}

}  // namespace test_helpers

#endif
