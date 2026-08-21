#ifndef SESSION_H
#define SESSION_H

#include "message_broker.h"
#include "protocol_frame.h"

#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

struct EncodedFrame {
    ProtocolFrameType type_;
    std::vector<uint8_t> body_;
};

enum class FlushResult {
    FLUSH_SUCCESS = 1, // the frame was sent completely
    FLUSH_EPOLLOUT = -1, // the frame was not sent completely, we need to register EPOLLOUT event for the socket
    FLUSH_CLOSE = 0, // the connection is dead, we need to close the connection
};

// A Session is a single TCP connection to the broker daemon.
class Session {
public:
    Session(int client_fd);
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    FlushResult flush();

    bool isRunning() const { return running_.load(std::memory_order_acquire); }

    // Enqueue a frame to be sent to the client.
    void enqueueFrame(ProtocolFrameType type, const std::vector<uint8_t>& body);    


private:

    int client_fd_;
    std::atomic<bool> running_{false};

    // write buffer for sending frames to the client
    std::list<std::shared_ptr<const EncodedFrame>> queue_;
    size_t offset_;  // this indicates how many bytes have been written to the socket for the current frame
};

#endif
