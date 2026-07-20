#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "protocol_frame.h"

namespace {

void expectHeader(const FrameHeader& decoded, ProtocolFrameType type) {
    EXPECT_EQ(decoded.version, PROTOCOL_VERSION);
    EXPECT_EQ(decoded.type, type);
}

}  // namespace

TEST(ProtocolFrameRoundTrip, SubscribeRequest) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::SUBSCRIBE};
    SubscribeRequest original{42, "orders", "workers"};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_subscribe_request(original, buffer);

    FrameHeader decoded_header;
    SubscribeRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_subscribe_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::SUBSCRIBE);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_EQ(decoded.group, original.group);
}

TEST(ProtocolFrameRoundTrip, SubscribeRequest_EmptyStrings) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::SUBSCRIBE};
    SubscribeRequest original{1, "", ""};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_subscribe_request(original, buffer);

    FrameHeader decoded_header;
    SubscribeRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_subscribe_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::SUBSCRIBE);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_TRUE(decoded.topic.empty());
    EXPECT_TRUE(decoded.group.empty());
}

TEST(ProtocolFrameRoundTrip, SubscribeAck) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::SUBSCRIBE_ACK};
    SubscribeAck original{7, 0xABCDEF0123456789ULL};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_subscribe_ack(original, buffer);

    FrameHeader decoded_header;
    SubscribeAck decoded;
    decode_frame_header(decoded_header, buffer);
    decode_subscribe_ack(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::SUBSCRIBE_ACK);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.subscription_id, original.subscription_id);
}

TEST(ProtocolFrameRoundTrip, UnsubscribeRequest) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::UNSUBSCRIBE};
    UnsubscribeRequest original{99, 123456789ULL};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_unsubscribe_request(original, buffer);

    FrameHeader decoded_header;
    UnsubscribeRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_unsubscribe_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::UNSUBSCRIBE);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.subscription_id, original.subscription_id);
}

TEST(ProtocolFrameRoundTrip, UnsubscribeAck) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::UNSUBSCRIBE_ACK};
    UnsubscribeAck original{11, 987654321ULL};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_unsubscribe_ack(original, buffer);

    FrameHeader decoded_header;
    UnsubscribeAck decoded;
    decode_frame_header(decoded_header, buffer);
    decode_unsubscribe_ack(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::UNSUBSCRIBE_ACK);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.subscription_id, original.subscription_id);
}

TEST(ProtocolFrameRoundTrip, PublishRequest) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::PUBLISH};
    PublishRequest original{55, "events", {0x01, 0x02, 0xFF, 0x00}};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_publish_request(original, buffer);

    FrameHeader decoded_header;
    PublishRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_publish_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::PUBLISH);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_EQ(decoded.payload, original.payload);
}

TEST(ProtocolFrameRoundTrip, PublishRequest_EmptyPayload) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::PUBLISH};
    PublishRequest original{2, "heartbeat", {}};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_publish_request(original, buffer);

    FrameHeader decoded_header;
    PublishRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_publish_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::PUBLISH);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_TRUE(decoded.payload.empty());
}

TEST(ProtocolFrameRoundTrip, PublishAck_Accepted) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::PUBLISH_ACK};
    PublishAck original{3, PublishResult::ACCEPTED};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_publish_ack(original, buffer);

    FrameHeader decoded_header;
    PublishAck decoded;
    decode_frame_header(decoded_header, buffer);
    decode_publish_ack(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::PUBLISH_ACK);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.result, PublishResult::ACCEPTED);
}

TEST(ProtocolFrameRoundTrip, PublishAck_NoSubscribers) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::PUBLISH_ACK};
    PublishAck original{4, PublishResult::NO_SUBSCRIBERS};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_publish_ack(original, buffer);

    FrameHeader decoded_header;
    PublishAck decoded;
    decode_frame_header(decoded_header, buffer);
    decode_publish_ack(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::PUBLISH_ACK);
    EXPECT_EQ(decoded.request_id, original.request_id);
    EXPECT_EQ(decoded.result, PublishResult::NO_SUBSCRIBERS);
}

TEST(ProtocolFrameRoundTrip, DeliverMessage) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::DELIVER};
    DeliverMessage original{1001, "notifications", 42, {'h', 'e', 'l', 'l', 'o'}};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_deliver_message(original, buffer);

    FrameHeader decoded_header;
    DeliverMessage decoded;
    decode_frame_header(decoded_header, buffer);
    decode_deliver_message(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::DELIVER);
    EXPECT_EQ(decoded.subscription_id, original.subscription_id);
    EXPECT_EQ(decoded.topic, original.topic);
    EXPECT_EQ(decoded.sequence, original.sequence);
    EXPECT_EQ(decoded.payload, original.payload);
}

TEST(ProtocolFrameRoundTrip, CloseRequest) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::CLOSE};
    CloseRequest original{88};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_close_request(original, buffer);

    FrameHeader decoded_header;
    CloseRequest decoded;
    decode_frame_header(decoded_header, buffer);
    decode_close_request(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::CLOSE);
    EXPECT_EQ(decoded.request_id, original.request_id);
}

TEST(ProtocolFrameRoundTrip, CloseAck) {
    FrameHeader header{PROTOCOL_VERSION, ProtocolFrameType::CLOSE_ACK};
    CloseAck original{88};

    std::vector<uint8_t> buffer;
    encode_frame_header(header, buffer);
    encode_close_ack(original, buffer);

    FrameHeader decoded_header;
    CloseAck decoded;
    decode_frame_header(decoded_header, buffer);
    decode_close_ack(decoded, buffer);

    expectHeader(decoded_header, ProtocolFrameType::CLOSE_ACK);
    EXPECT_EQ(decoded.request_id, original.request_id);
}

TEST(ProtocolFrameRoundTrip, IntegerHelpers_BigEndian) {
    std::vector<uint8_t> buffer;
    encode_u16(0x1234, buffer);
    encode_u32(0xABCDEF01u, buffer);
    encode_u64(0x0123456789ABCDEFULL, buffer);

    ASSERT_EQ(buffer.size(), 14u);
    EXPECT_EQ(decode_u16(buffer, 0), 0x1234);
    EXPECT_EQ(decode_u32(buffer, 2), 0xABCDEF01u);
    EXPECT_EQ(decode_u64(buffer, 6), 0x0123456789ABCDEFULL);

    // Spot-check wire byte order (MSB first).
    EXPECT_EQ(buffer[0], 0x12);
    EXPECT_EQ(buffer[1], 0x34);
    EXPECT_EQ(buffer[2], 0xAB);
    EXPECT_EQ(buffer[5], 0x01);
    EXPECT_EQ(buffer[6], 0x01);
    EXPECT_EQ(buffer[13], 0xEF);
}
