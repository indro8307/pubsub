#include "message_queue.h"

bool BlockBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const BrokerMessage>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_full_cv.wait(lock, [&mq]{ return mq.isClosed() || mq.queue.size() < mq.config.maxSize; });
    if (mq.isClosed()) {
        return false;
    }
    mq.queue.push_back(msg);
    mq.not_empty_cv.notify_one();
    return true;
}

std::shared_ptr<const BrokerMessage> BlockBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return mq.isClosed() || !mq.queue.empty(); });
    if (mq.isClosed()) {
        return nullptr;
    }
    if (mq.queue.empty()) {
        return nullptr;
    }
    std::shared_ptr<const BrokerMessage> msg = mq.queue.front();
    mq.queue.pop_front();
    mq.not_full_cv.notify_one();
    return msg;
}

bool BlockBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return mq.isClosed() || !mq.queue.empty(); })) {
        return false;
    }
    if (mq.isClosed()) {
        return false;
    }
    if (mq.queue.empty()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    mq.not_full_cv.notify_one();
    return true;
}

bool BlockBackPressureStrategy::try_dequeueUntil(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::function<bool()>& predicate) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq, &predicate]{ return mq.isClosed() || !mq.queue.empty() || predicate(); });
    if (mq.isClosed() || mq.queue.empty() || predicate()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    mq.not_full_cv.notify_one();
    return true;
}

bool DropOldestBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const BrokerMessage>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (mq.isClosed()) {
        return false;
    }
    if (mq.queue.size() > mq.config.maxSize) {
        throw std::logic_error("Queue size is greater than the max size");
    }
    if (mq.queue.size() == mq.config.maxSize) {
        mq.queue.pop_front();
    }
    mq.queue.push_back(msg);
    mq.not_empty_cv.notify_one();
    return true;
}

std::shared_ptr<const BrokerMessage> DropOldestBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return mq.isClosed() || !mq.queue.empty(); });
    if (mq.isClosed()) {
        return nullptr;
    }
    if (mq.queue.empty()) {
        return nullptr;
    }
    std::shared_ptr<const BrokerMessage> msg = mq.queue.front();
    mq.queue.pop_front();
    return msg;
}

bool DropOldestBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return mq.isClosed() || !mq.queue.empty(); })) {
        return false;
    }
    if (mq.isClosed()) {
        return false;
    }
    if (mq.queue.empty()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}

bool DropOldestBackPressureStrategy::try_dequeueUntil(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::function<bool()>& predicate) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq, &predicate]{ return mq.isClosed() || !mq.queue.empty() || predicate(); });
    if (mq.isClosed() || mq.queue.empty() || predicate()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}

bool RejectNewBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const BrokerMessage>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (mq.isClosed()) {
        return false;
    }
    if (mq.queue.size() >= mq.config.maxSize) {
        return false;
    }
    mq.queue.push_back(msg);
    mq.not_empty_cv.notify_one();
    return true;
}

std::shared_ptr<const BrokerMessage> RejectNewBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return mq.isClosed() || !mq.queue.empty(); });
    if (mq.isClosed()) {
        return nullptr;
    }
    if (mq.queue.empty()) {
        return nullptr;
    }
    std::shared_ptr<const BrokerMessage> msg = mq.queue.front();
    mq.queue.pop_front();
    return msg;
}

bool RejectNewBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return mq.isClosed() || !mq.queue.empty(); })) {
        return false;
    }
    if (mq.isClosed()) {
        return false;
    }
    if (mq.queue.empty()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}

bool RejectNewBackPressureStrategy::try_dequeueUntil(MessageQueue& mq, std::shared_ptr<const BrokerMessage>& out, const std::function<bool()>& predicate) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq, &predicate]{ return mq.isClosed() || !mq.queue.empty() || predicate(); });
    if (mq.isClosed() || mq.queue.empty() || predicate()) {
        out = nullptr;
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}   