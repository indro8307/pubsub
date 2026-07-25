#ifndef BROKER_CLIENT_H
#define BROKER_CLIENT_H

#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>


class ClientSubscription {
public:
    ClientSubscription(): topic_(""), group_(""), subscription_id_(0) {};
    ~ClientSubscription() {};

    const std::string& getTopic() const { return topic_; }
    void setTopic(const std::string& topic) { topic_ = topic; }
    const std::string& getGroup() const { return group_; }
    void setGroup(const std::string& group) { group_ = group; }
    uint64_t getSubscriptionId() const { return subscription_id_; }
    void setSubscriptionId(uint64_t subscription_id) { subscription_id_ = subscription_id; }

private:
    std::string topic_;
    std::string group_;
    uint64_t subscription_id_;
};

class BrokerClient {
public:
    BrokerClient(const std::string& host, int port);
    ~BrokerClient();

    void start()
    {
        running_ = true;
        connect_thread_ = std::thread(&BrokerClient::run, this);
    }

    void run()
    {
        socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (socket_fd_ == -1) {
            throw std::runtime_error("Failed to create socket");
        }
        // set socket options
        int optval = 1;
        if (setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
            throw std::runtime_error("Failed to set socket options");
        }
        // connect to broker
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port_);
        server_addr.sin_addr.s_addr = inet_addr(host_.c_str());
        if (connect(socket_fd_, (sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
            throw std::runtime_error("Failed to connect to broker");
        }
        connected_ = true;
        while (connected_) {
            receive();
        }
    }

    ssize_t recvExact(int fd, void* buffer, size_t count)
    {
        // read in a loop until the desired number of bytes are read.
        ssize_t total_bytes_read = 0;
        while(total_bytes_read < count) {
        {
            ssize_t bytes_read = recv(fd, (char*)buffer + total_bytes_read, count - total_bytes_read, MSG_WAITALL);
            if(bytes_read == -1) {
                return -1;
            }
            total_bytes_read += bytes_read;
        }
        return total_bytes_read;
    }

    ssize_t sendExact(int fd, const void* buffer, size_t count)
    {
        // write in a loop until the desired number of bytes are written.
        ssize_t total_bytes_written = 0;
        while(total_bytes_written < count) {
            ssize_t bytes_written = send(fd, (const char*)buffer + total_bytes_written, count - total_bytes_written, MSG_NOSIGNAL);
            if(bytes_written == -1) {
                return -1;
            }
            total_bytes_written += bytes_written;
        }
        return total_bytes_written;
    }

    void receive()
    {
        // start by reading the first 4 bytes to get the frame size.
        // then copy the rest of the frame into a buffer.
        uint8_t frame_size[4];
        ssize_t bytes_read = recvExact(socket_fd_, frame_size, sizeof(frame_size));
        if (bytes_read == -1) {
            throw std::runtime_error("Failed to read from socket");
        }
        uint32_t frame_size_int = frame_size[0] <<24 | frame_size[1] <<16 | frame_size[2] <<8 | frame_size[3];
        std::vector<uint8_t> frame_data(frame_size_int);
        bytes_read = recvExact(socket_fd_, frame_data.data(), frame_size_int);
        if (bytes_read == -1) {
            throw std::runtime_error("Failed to read from socket");
        }

        // decode the frame header.
        FrameHeader frame_header;
        decode_frame_header(frame_header, frame_data);
        switch (frame_header.type) {
            case ProtocolFrameType::SUBSCRIBE_ACK:
                // decode subscribe ack frame.
                SubscribeAckFrame subscribe_ack_frame;
                decode_subscribe_ack_frame(subscribe_ack_frame, frame_data);
                // handle subscribe ack frame.
                handle_subscribe_ack_frame(subscribe_ack_frame);
                break;
            case ProtocolFrameType::UNSUBSCRIBE_ACK:
                // decode unsubscribe ack frame.
                UnsubscribeAckFrame unsubscribe_ack_frame;
                decode_unsubscribe_ack_frame(unsubscribe_ack_frame, frame_data);
                // handle unsubscribe ack frame.
                handle_unsubscribe_ack_frame(unsubscribe_ack_frame);
                break;
            case ProtocolFrameType::PUBLISH_ACK:
                // decode publish ack frame.
                PublishAckFrame publish_ack_frame;
                decode_publish_ack_frame(publish_ack_frame, frame_data);
                // handle publish ack frame.
                handle_publish_ack_frame(publish_ack_frame);
                break;
            case ProtocolFrameType::CLOSE_ACK:
                // decode close ack frame.
                CloseAckFrame close_ack_frame;
                decode_close_ack_frame(close_ack_frame, frame_data);
                // handle close ack frame.
                handle_close_ack_frame(close_ack_frame);
                break;
            case ProtocolFrameType::DELIVER:
                // decode deliver frame.
                DeliverFrame deliver_frame;
                decode_deliver_frame(deliver_frame, frame_data);
                // handle deliver frame.
                handle_deliver_frame(deliver_frame);
                break;
            default:
                throw std::runtime_error("Invalid frame type: " + std::to_string(frame_header.type));
        }
    }

    void join()
    {
        connect_thread_.join();
    }
    void requestStop()
    {
        connected_ = false;

    void disconnect();
    void sendFrame(const std::string frameType, conts std::string topic, std::string group, std::vector<uint8_t> data);
    std::string receive();

private:
    int socket_fd_;
    std::string host_;
    int port_;
    std::atomic<bool> connected_;
    std::atomic<bool> running_;
    std::thread connect_thread_;
};

#endif