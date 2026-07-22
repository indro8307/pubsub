#include "broker_server.h"

BrokerServer::BrokerServer(MessageBroker& broker, uint16_t port)
    : broker_(broker), port_(port) {}

BrokerServer::~BrokerServer() {
    stop();
}

void BrokerServer::start() {
    running_.store(true, std::memory_order_release);
    accept_thread_ = std::thread(&BrokerServer::acceptLoop, this);
}

void BrokerServer::stop() {
    running_.store(false, std::memory_order_release);
    accept_thread_.join();
}

void BrokerServer::acceptLoop() {
    try {
        // Create socket
        int server_socket = socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket == -1) {
            throw std::runtime_error("Failed to create socket");
        }
        // Set socket options
        int optval = 1;
        if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
            throw std::runtime_error("Failed to set socket options");
        }
        // Bind socket
        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port_);
        if (bind(server_socket, (sockaddr*)&server_addr, sizeof(server_addr)) == -1) {
            throw std::runtime_error("Failed to bind socket");
        }
        // Listen for connections
        if (listen(server_socket, 10) == -1) {
            throw std::runtime_error("Failed to listen for connections");
        }
        // Accept connections
        while (running_.load(std::memory_order_acquire)) {
            sockaddr_in client_addr;
            socklen_t client_addr_len = sizeof(client_addr);
            int client_socket = accept(server_socket, (sockaddr*)&client_addr, &client_addr_len);
            if (client_socket == -1) {
                if (!running_.load(std::memory_order_acquire)) {
                    break;
                }
                throw std::runtime_error("Failed to accept connection");
            }
            // Handle client connection
            handleClientConnection(client_socket);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in accept loop: " << e.what() << std::endl;
    }
    // Close server socket
    close(server_socket);
}

void BrokerServer::handleClientConnection(int client_socket) {
    // Create a new session instance
    std::shared_ptr<Session> session = std::make_shared<Session>(client_socket, broker_);
    // Add session to sessions vector
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        sessions_.push_back(session);
    }
    // Start the session thread
    session->start();
}

void BrokerServer::reapFinishedSessions() {
    std::lock_guard<std::mutex> lock(sessions_mtx_);
    sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(), [](const std::shared_ptr<Session>& session) {
        return !session->isRunning();
    }), sessions_.end());
}

bool BrokerServer::isRunning() const {
    return running_.load(std::memory_order_acquire);
}
