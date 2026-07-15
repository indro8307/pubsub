#include "protocol_frame.h"

#include <cstddef>
#include <stdexcept>

constexpr size_t kFrameHeaderSize = 2; // version + type

void encode_u16(uint16_t value, std::vector<uint8_t>& buffer) {
    buffer.push_back(static_cast<uint8_t>(value >> 8));
    buffer.push_back(static_cast<uint8_t>(value));
}

void encode_u32(uint32_t value, std::vector<uint8_t>& buffer) {
    buffer.push_back(static_cast<uint8_t>(value >> 24));
    buffer.push_back(static_cast<uint8_t>(value >> 16));
    buffer.push_back(static_cast<uint8_t>(value >> 8));
    buffer.push_back(static_cast<uint8_t>(value));
}

void encode_u64(uint64_t value, std::vector<uint8_t>& buffer) {
    buffer.push_back(static_cast<uint8_t>(value >> 56));
    buffer.push_back(static_cast<uint8_t>(value >> 48));
    buffer.push_back(static_cast<uint8_t>(value >> 40));
    buffer.push_back(static_cast<uint8_t>(value >> 32));
    buffer.push_back(static_cast<uint8_t>(value >> 24));
    buffer.push_back(static_cast<uint8_t>(value >> 16));
    buffer.push_back(static_cast<uint8_t>(value >> 8));
    buffer.push_back(static_cast<uint8_t>(value));
}

uint16_t decode_u16(const std::vector<uint8_t>& buffer, size_t offset) {
    const uint16_t value =
        (static_cast<uint16_t>(buffer[offset]) << 8) |
        static_cast<uint16_t>(buffer[offset + 1]);
    return value;
}

uint32_t decode_u32(const std::vector<uint8_t>& buffer, size_t offset) {
    const uint32_t value =
        (static_cast<uint32_t>(buffer[offset]) << 24) |
        (static_cast<uint32_t>(buffer[offset + 1]) << 16) |
        (static_cast<uint32_t>(buffer[offset + 2]) << 8) |
        static_cast<uint32_t>(buffer[offset + 3]);
    return value;
}

uint64_t decode_u64(const std::vector<uint8_t>& buffer, size_t offset) {
    const uint64_t value =
        (static_cast<uint64_t>(buffer[offset]) << 56) |
        (static_cast<uint64_t>(buffer[offset + 1]) << 48) |
        (static_cast<uint64_t>(buffer[offset + 2]) << 40) |
        (static_cast<uint64_t>(buffer[offset + 3]) << 32) |
        (static_cast<uint64_t>(buffer[offset + 4]) << 24) |
        (static_cast<uint64_t>(buffer[offset + 5]) << 16) |
        (static_cast<uint64_t>(buffer[offset + 6]) << 8) |
        static_cast<uint64_t>(buffer[offset + 7]);
    return value;
}

// encode a frame header
void encode_frame_header(FrameHeader& header, std::vector<uint8_t>& buffer) {
    buffer.push_back(header.version);
    buffer.push_back(static_cast<uint8_t>(header.type));
}

// decode a frame header
void decode_frame_header(FrameHeader& header, std::vector<uint8_t>& buffer) {
    if(buffer.size() < kFrameHeaderSize) {
        throw std::runtime_error("Invalid frame header size");
    }
    header.version = buffer[0];
    header.type = static_cast<ProtocolFrameType>(buffer[1]);
}

// encode a subscribe request
void encode_subscribe_request(SubscribeRequest& request, std::vector<uint8_t>& buffer) {
    encode_u32(request.request_id, buffer);
    encode_u16(static_cast<uint16_t>(request.topic.size()), buffer);
    buffer.insert(buffer.end(), request.topic.begin(), request.topic.end());
    encode_u16(static_cast<uint16_t>(request.group.size()), buffer);
    buffer.insert(buffer.end(), request.group.begin(), request.group.end());
}

// decode a subscribe request
void decode_subscribe_request(SubscribeRequest& request, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    if(buffer.size() < offset + 4) {
        throw std::runtime_error("Invalid subscribe request size");
    }
    request.request_id = decode_u32(buffer, offset);
    offset += 4;
    if(buffer.size() < offset + 2) {
        throw std::runtime_error("Invalid subscribe request size. Not enough data for topic size");
    }
    const uint16_t topic_size = decode_u16(buffer, offset);
    offset += 2;
    if(buffer.size() < offset + topic_size) {
        throw std::runtime_error("Invalid subscribe request size. Not enough data for topic");
    }
    request.topic = std::string(buffer.begin() + static_cast<std::ptrdiff_t>(offset),
                                buffer.begin() + static_cast<std::ptrdiff_t>(offset + topic_size));
    offset += topic_size;
    if(buffer.size() < offset + 2) {
        throw std::runtime_error("Invalid subscribe request size. Not enough data for group size");
    }
    const uint16_t group_size = decode_u16(buffer, offset);
    offset += 2;
    if(buffer.size() < offset + group_size) {
        throw std::runtime_error("Invalid subscribe request size. Not enough data for group");
    }
    request.group = std::string(buffer.begin() + static_cast<std::ptrdiff_t>(offset),
                                buffer.begin() + static_cast<std::ptrdiff_t>(offset + group_size));
}

// encode a subscribe ack
void encode_subscribe_ack(SubscribeAck& ack, std::vector<uint8_t>& buffer) {
    encode_u32(ack.request_id, buffer);
    encode_u64(ack.subscription_id, buffer);
}

// decode a subscribe ack
void decode_subscribe_ack(SubscribeAck& ack, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    ack.request_id = decode_u32(buffer, offset);
    offset += 4;
    ack.subscription_id = decode_u64(buffer, offset);
}

// encode a unsubscribe request
void encode_unsubscribe_request(UnsubscribeRequest& request, std::vector<uint8_t>& buffer) {
    encode_u32(request.request_id, buffer);
    encode_u64(request.subscription_id, buffer);
}

// decode a unsubscribe request
void decode_unsubscribe_request(UnsubscribeRequest& request, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    request.request_id = decode_u32(buffer, offset);
    offset += 4;
    request.subscription_id = decode_u64(buffer, offset);
}

// encode a publish request
void encode_publish_request(PublishRequest& request, std::vector<uint8_t>& buffer) {
    encode_u32(request.request_id, buffer);
    encode_u16(static_cast<uint16_t>(request.topic.size()), buffer);
    buffer.insert(buffer.end(), request.topic.begin(), request.topic.end());
    encode_u32(static_cast<uint32_t>(request.payload.size()), buffer);
    buffer.insert(buffer.end(), request.payload.begin(), request.payload.end());
}

// decode a publish request
void decode_publish_request(PublishRequest& request, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    request.request_id = decode_u32(buffer, offset);
    offset += 4;
    const uint16_t topic_size = decode_u16(buffer, offset);
    offset += 2;
    request.topic = std::string(buffer.begin() + static_cast<std::ptrdiff_t>(offset),
                                buffer.begin() + static_cast<std::ptrdiff_t>(offset + topic_size));
    offset += topic_size;
    const uint32_t payload_size = decode_u32(buffer, offset);
    offset += 4;
    request.payload = std::vector<uint8_t>(
        buffer.begin() + static_cast<std::ptrdiff_t>(offset),
        buffer.begin() + static_cast<std::ptrdiff_t>(offset + payload_size));
}

// encode a publish ack
void encode_publish_ack(PublishAck& ack, std::vector<uint8_t>& buffer) {
    encode_u32(ack.request_id, buffer);
    buffer.push_back(static_cast<uint8_t>(ack.result));
}

// decode a publish ack
void decode_publish_ack(PublishAck& ack, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    ack.request_id = decode_u32(buffer, offset);
    offset += 4;
    ack.result = static_cast<PublishResult>(buffer[offset]);
}

// encode a deliver message
void encode_deliver_message(DeliverMessage& message, std::vector<uint8_t>& buffer) {
    encode_u64(message.subscription_id, buffer);
    encode_u16(static_cast<uint16_t>(message.topic.size()), buffer);
    buffer.insert(buffer.end(), message.topic.begin(), message.topic.end());
    encode_u64(message.sequence, buffer);
    encode_u32(static_cast<uint32_t>(message.payload.size()), buffer);
    buffer.insert(buffer.end(), message.payload.begin(), message.payload.end());
}

// decode a deliver message
void decode_deliver_message(DeliverMessage& message, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    message.subscription_id = decode_u64(buffer, offset);
    offset += 8;
    const uint16_t topic_size = decode_u16(buffer, offset);
    offset += 2;
    message.topic = std::string(buffer.begin() + static_cast<std::ptrdiff_t>(offset),
                                buffer.begin() + static_cast<std::ptrdiff_t>(offset + topic_size));
    offset += topic_size;
    message.sequence = decode_u64(buffer, offset);
    offset += 8;
    const uint32_t payload_size = decode_u32(buffer, offset);
    offset += 4;
    message.payload = std::vector<uint8_t>(
        buffer.begin() + static_cast<std::ptrdiff_t>(offset),
        buffer.begin() + static_cast<std::ptrdiff_t>(offset + payload_size));
}

// encode a close request
void encode_close_request(CloseRequest& request, std::vector<uint8_t>& buffer) {
    encode_u32(request.request_id, buffer);
}

// decode a close request
void decode_close_request(CloseRequest& request, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    request.request_id = decode_u32(buffer, offset);
}

// encode a close ack
void encode_close_ack(CloseAck& ack, std::vector<uint8_t>& buffer) {
    encode_u32(ack.request_id, buffer);
}

// decode a close ack
void decode_close_ack(CloseAck& ack, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    ack.request_id = decode_u32(buffer, offset);
}

// encode a unsubscribe ack
void encode_unsubscribe_ack(UnsubscribeAck& ack, std::vector<uint8_t>& buffer) {
    encode_u32(ack.request_id, buffer);
    encode_u64(ack.subscription_id, buffer);
}

// decode a unsubscribe ack
void decode_unsubscribe_ack(UnsubscribeAck& ack, std::vector<uint8_t>& buffer) {
    size_t offset = kFrameHeaderSize;
    ack.request_id = decode_u32(buffer, offset);
    offset += 4;
    ack.subscription_id = decode_u64(buffer, offset);
}
