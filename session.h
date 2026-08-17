#ifndef SESSION_H
#define SESSION_H

#include "message_broker.h"
#include "protocol_frame.h"

#include <atomic>
#include <cstdint>
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


// One TCP client connection to the broker daemon (docs/protocol.md).
//
// Ownership:
//   - Owns the client socket fd.
//   - Spawns a reader thread that receives frames, decodes them, and calls
//     into MessageBroker. Acks are written back on the same socket.
//   - Tracks subscriptions created on this connection so CLOSE / disconnect
//     can unsubscribe them all.
//
// DELIVER (broker → client) is intentionally not started here yet; a
// deliverer thread (or equivalent) can be added once the request path works.
//
// Socket type is a plain int for now (POSIX fd). Windows SOCKET can be
// introduced behind a typedef when the .cpp is written.
class Session {
public:
    Session(int client_fd);
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    FlushResult flush();

    bool isRunning() const { return running_.load(std::memory_order_acquire); }


private:


    // Unsubscribe every token owned by this connection (protocol §5 rule 4).
    void cleanupSubscriptions();

    // Encode |body| under |type|, prefix with frame_len, write to the socket.
    // Serialized with write_mtx_ so a future deliverer can share the fd.
    void enqueueFrame(ProtocolFrameType type, const std::vector<uint8_t>& body);

    int client_fd_;
    std::atomic<bool> running_{false};

    // write buffer for sending frames to the client
    std::list<std::shared_ptr<const EncodedFrame>> queue_;
    size_t offset_;  // this indicates how many bytes have been written to the socket for the current frame
};

#endif
