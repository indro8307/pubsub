#ifndef PROTOCOL_FRAME_H
#define PROTOCOL_FRAME_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// Wire frame type opcodes (docs/protocol.md §3).
enum class ProtocolFrameType : uint8_t {
    SUBSCRIBE = 0x10,
    UNSUBSCRIBE = 0x11,
    SUBSCRIBE_ACK = 0x12,
    UNSUBSCRIBE_ACK = 0x13,
    PUBLISH = 0x20,
    DELIVER = 0x21,
    PUBLISH_ACK = 0x22,
    CLOSE = 0x30,
    CLOSE_ACK = 0x31,
};

// PUBLISH_ACK.result values (docs/protocol.md §4.6).
enum class PublishResult : uint8_t {
    ACCEPTED = 0,
    NO_SUBSCRIBERS = 1,
};

constexpr uint8_t PROTOCOL_VERSION = 1;
constexpr uint32_t PROTOCOL_FRAME_MAX_SIZE = 16u * 1024u * 1024u; // 16 MiB
constexpr uint16_t PROTOCOL_MAX_STRING_LEN = 65535;              // u16 length prefix


// Fixed part of an on-wire frame after the u32 frame_len prefix (§2).
struct FrameHeader {
    uint8_t version = PROTOCOL_VERSION;
    ProtocolFrameType type = ProtocolFrameType::SUBSCRIBE;
};

// --- Client → broker ---

struct SubscribeRequest {
    uint32_t request_id = 0;
    std::string topic;
    std::string group;
};

struct UnsubscribeRequest {
    uint32_t request_id = 0;
    uint64_t subscription_id = 0;
};

struct PublishRequest {
    uint32_t request_id = 0;
    std::string topic;
    std::vector<uint8_t> payload;
};

struct CloseRequest {
    uint32_t request_id = 0;
};

// --- Broker → client ---

struct SubscribeAck {
    uint32_t request_id = 0;
    uint64_t subscription_id = 0;
};

struct UnsubscribeAck {
    uint32_t request_id = 0;
    uint64_t subscription_id = 0;
};

struct PublishAck {
    uint32_t request_id = 0;
    PublishResult result = PublishResult::ACCEPTED;
};

struct DeliverMessage {
    uint64_t subscription_id = 0;
    std::string topic;
    uint64_t sequence = 0;
    std::vector<uint8_t> payload;
};

struct CloseAck {
    uint32_t request_id = 0;
};

// Big-endian integer helpers (docs/protocol.md §1). Encode appends to |buffer|;
// decode reads from |buffer| at |offset|.
void encode_u16(uint16_t value, std::vector<uint8_t>& buffer);
void encode_u16(uint16_t value, char* buffer);
void encode_u32(uint32_t value, std::vector<uint8_t>& buffer);
void encode_u32(uint32_t value, char* buffer);
void encode_u64(uint64_t value, std::vector<uint8_t>& buffer);
void encode_u64(uint64_t value, char* buffer);

uint16_t decode_u16(const std::vector<uint8_t>& buffer, size_t offset);
uint32_t decode_u32(const std::vector<uint8_t>& buffer, size_t offset);
uint64_t decode_u64(const std::vector<uint8_t>& buffer, size_t offset);

// Frame codec. Encode appends to |buffer|; decode expects a full frame
// (header + body) and reads the body starting after the 2-byte header.
void encode_frame_header(FrameHeader& header, std::vector<uint8_t>& buffer);
void encode_frame_header(FrameHeader& header, char* buffer);
size_t encode_frame_header_size();
void decode_frame_header(FrameHeader& header, std::vector<uint8_t>& buffer);

void encode_subscribe_request(SubscribeRequest& request, std::vector<uint8_t>& buffer);
void decode_subscribe_request(SubscribeRequest& request, std::vector<uint8_t>& buffer);

void encode_subscribe_ack(SubscribeAck& ack, std::vector<uint8_t>& buffer);
void decode_subscribe_ack(SubscribeAck& ack, std::vector<uint8_t>& buffer);

void encode_unsubscribe_request(UnsubscribeRequest& request, std::vector<uint8_t>& buffer);
void decode_unsubscribe_request(UnsubscribeRequest& request, std::vector<uint8_t>& buffer);

void encode_unsubscribe_ack(UnsubscribeAck& ack, std::vector<uint8_t>& buffer);
void decode_unsubscribe_ack(UnsubscribeAck& ack, std::vector<uint8_t>& buffer);

void encode_publish_request(PublishRequest& request, std::vector<uint8_t>& buffer);
void encode_publish_request(PublishRequest& request, char* buffer);
size_t encode_publish_request_size(const PublishRequest& request);
void decode_publish_request(PublishRequest& request, std::vector<uint8_t>& buffer);

void encode_publish_ack(PublishAck& ack, std::vector<uint8_t>& buffer);
void decode_publish_ack(PublishAck& ack, std::vector<uint8_t>& buffer);

void encode_deliver_message(DeliverMessage& message, std::vector<uint8_t>& buffer);
void decode_deliver_message(DeliverMessage& message, std::vector<uint8_t>& buffer);

void encode_close_request(CloseRequest& request, std::vector<uint8_t>& buffer);
void decode_close_request(CloseRequest& request, std::vector<uint8_t>& buffer);

void encode_close_ack(CloseAck& ack, std::vector<uint8_t>& buffer);
void decode_close_ack(CloseAck& ack, std::vector<uint8_t>& buffer);

#endif
