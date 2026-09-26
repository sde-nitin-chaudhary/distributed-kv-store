#pragma once

#include "lru_cache.hpp"
#include "resp_parser.hpp"
#include <string>
#include <atomic>
#include <thread>
#include <vector>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    using socket_t = SOCKET;
    #define INVALID_SOCKET_VALUE INVALID_SOCKET
    #define SOCKET_CLOSE(s) closesocket(s)
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <unistd.h>
    using socket_t = int;
    #define INVALID_SOCKET_VALUE (-1)
    #define SOCKET_CLOSE(s) close(s)
#endif

namespace kvstore {

class KvServer {
public:
    explicit KvServer(int port = 6380, size_t cache_capacity = 100000, EvictionPolicy policy = EvictionPolicy::LRU);
    ~KvServer();

    void start();
    void stop();

private:
    void init_networking();
    void cleanup_networking();
    void accept_loop();
    void handle_client(socket_t client_fd);
    RespObject execute_command(const std::vector<RespObject>& args);

    int port_;
    socket_t server_fd_{INVALID_SOCKET_VALUE};
    std::atomic<bool> is_running_{false};
    ThreadSafeCache cache_;

    std::thread accept_thread_;
    std::vector<std::thread> worker_threads_;
};

} // namespace kvstore
