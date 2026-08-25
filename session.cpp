#include "session.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

Session::Session(int client_fd)
    : client_fd_(client_fd), offset_(0) {
    running_.store(true, std::memory_order_release);
}

Session::~Session() {
    if (client_fd_ >= 0) {
        ::close(client_fd_);
        client_fd_ = -1;
    }
}

void Session::removeSubscriptionId(uint64_t id) {
    for (auto it = subscription_ids_.begin(); it != subscription_ids_.end(); ++it) {
        if (*it == id) {
            subscription_ids_.erase(it);
            return;
        }
    }
}

void Session::enqueueFrame(ProtocolFrameType type, const std::vector<uint8_t>& body) {
    auto encoded_frame = std::make_shared<EncodedFrame>();
    encoded_frame->type_ = type;
    encoded_frame->body_ = body;
    queue_.push_back(std::move(encoded_frame));
}

FlushResult Session::flush()
{
    // Look at the first frame in the queue. Send the bytes that are still left:
    // remaining = frame size - offset. If offset is 0 we are starting at the
    // beginning of the frame.
    //
    // Each frame is one send()/write() call (we are not using writev). If that
    // send finishes the whole frame, do not go back to epoll_wait yet — dequeue
    // it and try the next frame in this same flush(). We only stop when the
    // queue is empty or send cannot take more data right now.
    //
    // What send() returns:
    //
    // if n == requested size, the frame was sent completely. In this case we:
    //       - dequeue the frame
    //       - move to the next frame and repeat
    // if the queue becomes empty after that, we are done. In this case we:
    //       - tell the caller to drop EPOLLOUT (otherwise we keep waking up
    //         even though there is nothing left to write)
    //       - return
    //
    // if 0 < n < requested size, only part of the frame went out. In this case we:
    //       - do not dequeue the frame
    //       - offset += n  (so the next send starts where we left off)
    //       - tell the caller to register EPOLLOUT
    //       - return and wait for epoll_wait
    //
    // if n == -1, check errno (not every error means "try again later"):
    //       EAGAIN / EWOULDBLOCK: the socket send buffer is full, no bytes
    //           were written. Keep the frame and offset. Tell the caller to
    //           register EPOLLOUT, then return and wait for epoll_wait.
    //       EINTR: we got interrupted by a signal. Retry the same send.
    //           Do not register EPOLLOUT.
    //       EPIPE / ECONNRESET / any other error: the connection is dead.
    //           Tell the caller to close it. Do not wait for EPOLLOUT.
    //
    // if n == 0, treat it like a dead connection (same as the fatal errors).
    //
    // flush() itself does not call epoll_ctl. Session / BrokerServer should
    // add or remove EPOLLOUT based on what flush() tells it.

    while (!queue_.empty())
    {
        const auto& frame = queue_.front();
        size_t remaining = frame->body_.size() - offset_;
        if (offset_ >= frame->body_.size() || remaining == 0)
        {
            queue_.erase(queue_.begin());
            offset_ = 0;
            continue;
        }
        ssize_t n = send(client_fd_, frame->body_.data() + offset_, remaining, MSG_NOSIGNAL|MSG_DONTWAIT);
        if (n == -1)
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return FlushResult::FLUSH_EPOLLOUT;
            }
            else
            {
                queue_.clear();
                return FlushResult::FLUSH_CLOSE;
            }
        }
        else if (n == 0)
        {
            queue_.clear();
            return FlushResult::FLUSH_CLOSE;
        }
        else
        {
            offset_ += n;
            if (offset_ == frame->body_.size())
            {
                queue_.erase(queue_.begin());
                offset_ = 0;
            }
            else
            {
                return FlushResult::FLUSH_EPOLLOUT;
            }
        }
    }
    return FlushResult::FLUSH_SUCCESS;
}    
