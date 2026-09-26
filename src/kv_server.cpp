#include "kv_server.hpp"
#include <iostream>
#include <algorithm>
#include <sstream>

#ifdef _WIN32
    #pragma comment(lib, "Ws2_32.lib")
#endif

namespace kvstore {

KvServer::KvServer(int port, size_t cache_capacity, EvictionPolicy policy)
    : port_(port), cache_(cache_capacity, policy) {
    init_networking();
}

KvServer::~KvServer() {
    stop();
    cleanup_networking();
}

void KvServer::init_networking() {
#ifdef _WIN32
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
}

void KvServer::cleanup_networking() {
#ifdef _WIN32
    WSACleanup();
#endif
}

void KvServer::start() {
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == INVALID_SOCKET_VALUE) {
        std::cerr << "[ERROR] Failed to create socket\n";
        return;
    }

    int opt = 1;
#ifdef _WIN32
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));
#else
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port_);

    if (bind(server_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "[ERROR] Failed to bind to port " << port_ << "\n";
        SOCKET_CLOSE(server_fd_);
        return;
    }

    if (listen(server_fd_, 128) < 0) {
        std::cerr << "[ERROR] Failed to listen on socket\n";
        SOCKET_CLOSE(server_fd_);
        return;
    }

    is_running_ = true;
    std::cout << "========================================================\n";
    std::cout << " Distributed KV Store & Cache Server Started on Port " << port_ << "\n";
    std::cout << " Protocol: Redis RESP2 | Concurrency: Multithreaded\n";
    std::cout << " Connect using redis-cli: redis-cli -p " << port_ << "\n";
    std::cout << "========================================================\n";

    accept_thread_ = std::thread(&KvServer::accept_loop, this);
}

void KvServer::stop() {
    if (!is_running_.exchange(false)) return;

    if (server_fd_ != INVALID_SOCKET_VALUE) {
        SOCKET_CLOSE(server_fd_);
        server_fd_ = INVALID_SOCKET_VALUE;
    }

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    for (auto& t : worker_threads_) {
        if (t.joinable()) t.join();
    }
    worker_threads_.clear();
    std::cout << "[INFO] Server stopped gracefully.\n";
}

void KvServer::accept_loop() {
    while (is_running_) {
        sockaddr_in client_addr{};
#ifdef _WIN32
        int addr_len = sizeof(client_addr);
#else
        socklen_t addr_len = sizeof(client_addr);
#endif
        socket_t client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &addr_len);
        if (client_fd == INVALID_SOCKET_VALUE) {
            if (!is_running_) break;
            continue;
        }

        worker_threads_.emplace_back(&KvServer::handle_client, this, client_fd);
    }
}

void KvServer::handle_client(socket_t client_fd) {
    std::string buffer;
    char read_buf[4096];

    while (is_running_) {
        int bytes_read = recv(client_fd, read_buf, sizeof(read_buf), 0);
        if (bytes_read <= 0) {
            break;
        }

        buffer.append(read_buf, bytes_read);

        // Parse RESP command(s) from buffer
        size_t bytes_consumed = 0;
        while (!buffer.empty()) {
            auto resp_opt = RespParser::parse(buffer, bytes_consumed);
            if (!resp_opt.has_value() || bytes_consumed == 0) {
                break; // Incomplete packet, wait for more bytes
            }

            buffer.erase(0, bytes_consumed);

            if (resp_opt->type == RespType::Array) {
                RespObject response = execute_command(resp_opt->array_val);
                std::string serialized = response.serialize();
                send(client_fd, serialized.data(), static_cast<int>(serialized.size()), 0);
            }
        }
    }

    SOCKET_CLOSE(client_fd);
}

RespObject KvServer::execute_command(const std::vector<RespObject>& args) {
    if (args.empty()) return RespObject::Error("ERR empty command");

    std::string cmd = args[0].str_val;
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    if (cmd == "PING") {
        if (args.size() > 1) {
            return RespObject::BulkString(args[1].str_val);
        }
        return RespObject::SimpleString("PONG");
    }

    if (cmd == "SET") {
        if (args.size() < 3) return RespObject::Error("ERR wrong number of arguments for 'set' command");
        const std::string& key = args[1].str_val;
        const std::string& val = args[2].str_val;

        std::optional<std::chrono::milliseconds> ttl = std::nullopt;
        if (args.size() >= 5) {
            std::string opt = args[3].str_val;
            std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
            if (opt == "EX") {
                ttl = std::chrono::milliseconds(std::stoll(args[4].str_val) * 1000);
            } else if (opt == "PX") {
                ttl = std::chrono::milliseconds(std::stoll(args[4].str_val));
            }
        }

        cache_.put(key, val, ttl);
        return RespObject::SimpleString("OK");
    }

    if (cmd == "GET") {
        if (args.size() != 2) return RespObject::Error("ERR wrong number of arguments for 'get' command");
        auto val = cache_.get(args[1].str_val);
        if (val.has_value()) {
            return RespObject::BulkString(*val);
        }
        return RespObject::NullBulkString();
    }

    if (cmd == "DEL") {
        if (args.size() < 2) return RespObject::Error("ERR wrong number of arguments for 'del' command");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); ++i) {
            if (cache_.remove(args[i].str_val)) {
                count++;
            }
        }
        return RespObject::Integer(count);
    }

    if (cmd == "EXISTS") {
        if (args.size() < 2) return RespObject::Error("ERR wrong number of arguments for 'exists' command");
        int64_t count = 0;
        for (size_t i = 1; i < args.size(); ++i) {
            if (cache_.exists(args[i].str_val)) {
                count++;
            }
        }
        return RespObject::Integer(count);
    }

    if (cmd == "INFO" || cmd == "STATS") {
        auto stats = cache_.get_stats();
        std::ostringstream oss;
        oss << "# Stats\r\n";
        oss << "keys:" << stats.current_size << "\r\n";
        oss << "capacity:" << stats.capacity << "\r\n";
        oss << "hits:" << stats.total_hits << "\r\n";
        oss << "misses:" << stats.total_misses << "\r\n";
        oss << "evictions:" << stats.total_evictions << "\r\n";
        double hit_rate = (stats.total_hits + stats.total_misses > 0)
            ? (100.0 * stats.total_hits / (stats.total_hits + stats.total_misses))
            : 0.0;
        oss << "hit_rate_pct:" << hit_rate << "\r\n";
        return RespObject::BulkString(oss.str());
    }

    if (cmd == "COMMAND") {
        // Dummy response for redis-cli compatibility
        return RespObject::Array({});
    }

    return RespObject::Error("ERR unknown command '" + cmd + "'");
}

} // namespace kvstore
