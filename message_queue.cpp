#include "message_queue.h"

bool BlockBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_full_cv.wait(lock, [&mq]{ return mq.queue.size() < mq.config.maxSize; });
    mq.queue.push_back(msg);
    mq.not_empty_cv.notify_one();
    return true;
}

std::shared_ptr<const Message> BlockBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return !mq.queue.empty(); });
    std::shared_ptr<const Message> msg = mq.queue.front();
    mq.queue.pop_front();
    mq.not_full_cv.notify_one();
    return msg;
}

bool BlockBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return !mq.queue.empty(); })) {
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    mq.not_full_cv.notify_one();
    return true;
}

bool DropOldestBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
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

std::shared_ptr<const Message> DropOldestBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return !mq.queue.empty(); });
    std::shared_ptr<const Message> msg = mq.queue.front();
    mq.queue.pop_front();
    return msg;
}

bool DropOldestBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return !mq.queue.empty(); })) {
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}

bool RejectNewBackPressureStrategy::try_enqueue(MessageQueue& mq, const std::shared_ptr<const Message>& msg) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (mq.queue.size() >= mq.config.maxSize) {
        return false;
    }
    mq.queue.push_back(msg);
    mq.not_empty_cv.notify_one();
    return true;
}

std::shared_ptr<const Message> RejectNewBackPressureStrategy::try_dequeue(MessageQueue& mq) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    mq.not_empty_cv.wait(lock, [&mq]{ return !mq.queue.empty(); });
    std::shared_ptr<const Message> msg = mq.queue.front();
    mq.queue.pop_front();
    return msg;
}

bool RejectNewBackPressureStrategy::try_dequeueFor(MessageQueue& mq, std::shared_ptr<const Message>& out, const std::chrono::milliseconds& timeout) {
    std::unique_lock<std::mutex> lock(mq.mtx);
    if (!mq.not_empty_cv.wait_for(lock, timeout, [&mq]{ return !mq.queue.empty(); })) {
        return false;
    }
    out = mq.queue.front();
    mq.queue.pop_front();
    return true;
}
