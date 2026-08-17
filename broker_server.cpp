#include "broker_server.h"

#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

BrokerServer::BrokerServer(MessageBroker& broker, uint16_t port)
    : broker_(broker), port_(port) {}

BrokerServer::~BrokerServer() {
    stop();
}

void BrokerServer::start() {
    running_.store(true, std::memory_order_release);
    accept_thread_ = std::thread(&BrokerServer::run, this);
}

void BrokerServer::stop() {
    running_.store(false, std::memory_order_release);
    listening_.store(false, std::memory_order_release);
    if (listen_fd_ >= 0) {
        // Wake epoll_wait / accept so run() can see running_ == false.
        ::shutdown(listen_fd_, SHUT_RDWR);
    }
    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }
    // Close server socket
    if (listen_fd_ >= 0) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }      
    std::vector<std::shared_ptr<Session>> sessions;
    {
        std::lock_guard<std::mutex> lock(sessions_mtx_);
        sessions.swap(sessions_);
    }
    for (auto& session : sessions) {
        if (session) {
            session->requestStop();
        }
    }
    for (auto& session : sessions) {
        if (session) {
            session->join();
        }
    }
}

void BrokerServer::run() {
    try {
        // Create socket
        int server_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (server_socket == -1) {
            throw std::runtime_error("Failed to create socket");
        }
        listen_fd_ = server_socket;

        // Set socket options
        int optval = 1;
        if (::setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) == -1) {
            throw std::runtime_error("Failed to set socket options");
        }
        // Bind socket
        sockaddr_in server_addr{};
        std::memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_addr.s_addr = INADDR_ANY;
        server_addr.sin_port = htons(port_);
        if (::bind(server_socket, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1) {
            throw std::runtime_error("Failed to bind socket");
        }
        // Listen for connections
        if (::listen(server_socket, 10) == -1) {
            throw std::runtime_error("Failed to listen for connections");
        }

        int flags = ::fcntl(server_socket, F_GETFL, 0);
        if (flags == -1) {
            throw std::runtime_error("Failed to get listen socket flags");
        }
        if (::fcntl(server_socket, F_SETFL, flags | O_NONBLOCK) == -1) {
            throw std::runtime_error("Failed to set listen socket non-blocking");
        }

        epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd_ == -1) {
            throw std::runtime_error("Failed to create epoll instance");
        }

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev) == -1) {
            throw std::runtime_error("Failed to add listen socket to epoll");
        }

        listening_.store(true, std::memory_order_release);

        // Timeout so stop() can join even if shutdown() does not wake a listening
        // socket. We will replace this with an eventfd wakeup later.
        constexpr int kMaxEvents = 16;
        constexpr int kWaitTimeoutMs = 100;
        epoll_event events[kMaxEvents];

        while (running_.load(std::memory_order_acquire)) {
            const int nready = ::epoll_wait(epoll_fd_, events, kMaxEvents, kWaitTimeoutMs);
            if (nready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (!running_.load(std::memory_order_acquire)) {
                    break;
                }
                throw std::runtime_error("epoll_wait failed");
            }
            if (!running_.load(std::memory_order_acquire)) {
                break;
            }

            for (int i = 0; i < nready; ++i) {
                const int fd = events[i].data.fd;
                const uint32_t ev_mask = events[i].events;

                // Step 2: only the listen socket is in epoll. Client fds come later.
                if (fd != listen_fd_) {
                    continue;
                }
                if (ev_mask & (EPOLLERR | EPOLLHUP)) {
                    if (!running_.load(std::memory_order_acquire)) {
                        break;
                    }
                    throw std::runtime_error("Listen socket error");
                }
                if (ev_mask & EPOLLIN) {
                    // Level-triggered: accept until the backlog is empty (EAGAIN).
                    while (true) {
                        sockaddr_in client_addr{};
                        socklen_t client_addr_len = sizeof(client_addr);
                        int client_socket = ::accept(
                            listen_fd_,
                            reinterpret_cast<sockaddr*>(&client_addr),
                            &client_addr_len);
                        if (client_socket == -1) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                                break;
                            }
                            if (errno == EINTR) {
                                continue;
                            }
                            if (!running_.load(std::memory_order_acquire)) {
                                break;
                            }
                            throw std::runtime_error("Failed to accept connection");
                        }
                        handleClientConnection(client_socket);
                    }
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in accept loop: " << e.what() << std::endl;
    }
    listening_.store(false, std::memory_order_release);
}

// TODO: This may not be required anymore because when ever epoll_wait returns a 
// new client connection, it will be added to the epoll_wait list and handled there
// we need not start a new thread for each client connection
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
    sessions_.erase(std::remove_if(sessions_.begin(), sessions_.end(),
                                   [](const std::shared_ptr<Session>& session) {
                                       return session && !session->isRunning();
                                   }),
                    sessions_.end());
}
