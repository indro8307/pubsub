#ifndef SESSION_H
#define SESSION_H

#include "message_broker.h"
#include "protocol_frame.h"

#include <atomic>
#include <cstdint>
#include <map>
#include <mutex>
#include <thread>
#include <vector>

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
    Session(int client_fd, MessageBroker& broker);
    ~Session();

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    // Spawn the reader thread. Returns immediately.
    void start();

    // Block until the reader thread exits (after CLOSE or disconnect).
    void join();

    // Ask the reader to stop (e.g. server shutdown). Closes the socket so
    // a blocking recv wakes up; the thread then cleans up and exits.
    void requestStop();

private:
    // Reader thread entry: loop on recv frame_len + body until stop/error.
    void run();

    // Dispatch one decoded frame (header + body already in |frame|).
    // |frame| is non-const because the codec decode APIs take a mutable buffer.
    void handleFrame(const FrameHeader& header, std::vector<uint8_t>& frame);

    void handleSubscribe(std::vector<uint8_t>& frame);
    void handleUnsubscribe(std::vector<uint8_t>& frame);
    void handlePublish(std::vector<uint8_t>& frame);
    void handleClose(std::vector<uint8_t>& frame);

    // Unsubscribe every token owned by this connection (protocol §5 rule 4).
    void cleanupSubscriptions();

    // Encode |body| under |type|, prefix with frame_len, write to the socket.
    // Serialized with write_mtx_ so a future deliverer can share the fd.
    void sendFrame(ProtocolFrameType type, const std::vector<uint8_t>& body);

    int client_fd_;
    MessageBroker& broker_;

    std::thread reader_;
    std::atomic<bool> running_{false};

    // Protects socket writes (acks now; DELIVER later).
    std::mutex write_mtx_;

    // subscription_id (wire) → broker token for this connection.
    std::mutex subs_mtx_;
    std::map<uint64_t, SubscriptionToken> subscriptions_;
};

#endif
