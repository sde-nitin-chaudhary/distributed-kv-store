#include "kv_server.hpp"
#include <iostream>
#include <sstream>
#include <algorithm>
#include <cstring>
#include <chrono>
#include <random>
#include <charconv>

namespace kvstore {

namespace {

struct ParsedTtl {
    bool valid{true};
    std::string error_msg{""};
    std::optional<std::chrono::milliseconds> ttl{std::nullopt};
    int64_t abs_expire_ms{0};
};

ParsedTtl parse_ttl_args(const std::vector<RespObject>& args, size_t opt_idx) {
    ParsedTtl res;
    if (args.size() <= opt_idx) return res;
    if (args.size() <= opt_idx + 1) {
        res.valid = false;
        res.error_msg = "ERR syntax error";
        return res;
    }
    std::string opt = args[opt_idx].str_val;
    std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
    int64_t val = 0;
    std::string_view sv = args[opt_idx + 1].str_val;
    if (sv.empty()) {
        res.valid = false;
        res.error_msg = "ERR value is not an integer or out of range";
        return res;
    }
    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
    if (ec != std::errc() || ptr != sv.data() + sv.size() || val <= 0) {
        res.valid = false;
        res.error_msg = "ERR value is not an integer or out of range";
        return res;
    }

    auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    if (opt == "EX") {
        res.ttl = std::chrono::milliseconds(val * 1000);
        res.abs_expire_ms = now_ms + (val * 1000);
    } else if (opt == "PX") {
        res.ttl = std::chrono::milliseconds(val);
        res.abs_expire_ms = now_ms + val;
    } else if (opt == "EXAT") {
        int64_t target_ms = val * 1000;
        int64_t rem = target_ms - now_ms;
        res.ttl = std::chrono::milliseconds(rem > 0 ? rem : 0);
        res.abs_expire_ms = target_ms;
    } else if (opt == "PXAT") {
        int64_t rem = val - now_ms;
        res.ttl = std::chrono::milliseconds(rem > 0 ? rem : 0);
        res.abs_expire_ms = val;
    } else {
        res.valid = false;
        res.error_msg = "ERR syntax error";
    }
    return res;
}

} // anonymous namespace

KvServer::KvServer(int port, size_t cache_capacity, EvictionPolicy policy, size_t num_shards)
    : KvServer(ServerConfig{port, cache_capacity, policy, num_shards}) {}

KvServer::KvServer(const ServerConfig& config)
    : config_(config),
      cache_(config.cache_capacity, config.policy, config.num_shards),
      ring_(150),
      wal_(config.aof_path, config.fsync_policy) {

    // If cluster secret not explicitly set, auto-generate a secure random 32-char hex token
    if (config_.cluster_secret.empty()) {
        std::random_device rd;
        std::mt19937_64 gen(rd());
        std::uniform_int_distribution<uint64_t> dis;
        char buf[33];
        std::snprintf(buf, sizeof(buf), "%016llx%016llx",
                      static_cast<unsigned long long>(dis(gen)),
                      static_cast<unsigned long long>(dis(gen)));
        config_.cluster_secret = std::string(buf);
    }

    // Self-registration on ConsistentHashRing
    ring_.add_node(config_.node_id);
    peers_map_[config_.node_id] = {"127.0.0.1", config_.port};

    // Parse configured peers: "node-1@127.0.0.1:6380,node-2@127.0.0.1:6381"
    if (!config_.peers.empty()) {
        std::stringstream ss(config_.peers);
        std::string peer_item;
        while (std::getline(ss, peer_item, ',')) {
            size_t at_pos = peer_item.find('@');
            size_t colon_pos = peer_item.find(':');
            if (at_pos != std::string::npos && colon_pos != std::string::npos && colon_pos > at_pos) {
                std::string peer_id = peer_item.substr(0, at_pos);
                std::string host = peer_item.substr(at_pos + 1, colon_pos - at_pos - 1);
                int port = std::stoi(peer_item.substr(colon_pos + 1));

                ring_.add_node(peer_id);
                peers_map_[peer_id] = {host, port};
            }
        }
    }

    is_cluster_mode_ = (ring_.total_nodes() > 1);
}

KvServer::~KvServer() {
    stop();
}

void KvServer::init_networking() {
#ifdef _WIN32
    WSADATA wsa_data;
    if (WSAStartup(MAKEWORD(2, 2), &wsa_data) != 0) {
        std::cerr << "[ERROR] WSAStartup failed.\n";
    }
#endif
}

void KvServer::cleanup_networking() {
#ifdef _WIN32
    WSACleanup();
#endif
}

void KvServer::start() {
    if (is_running_) return;

    init_networking();

    // 1. Durability: Replay Write-Ahead Log (AOF) on startup before opening network
    if (wal_.is_enabled()) {
        size_t replayed = wal_.replay([this](const std::vector<RespObject>& args) {
            if (args.empty()) return;
            std::string cmd = args[0].str_val;
            std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);
            if (cmd == "SET" && args.size() >= 3) {
                std::optional<std::chrono::milliseconds> ttl = std::nullopt;
                if (args.size() >= 5) {
                    std::string opt = args[3].str_val;
                    std::transform(opt.begin(), opt.end(), opt.begin(), ::toupper);
                    int64_t val = 0;
                    std::string_view sv = args[4].str_val;
                    auto [ptr, ec] = std::from_chars(sv.data(), sv.data() + sv.size(), val);
                    if (ec == std::errc() && ptr == sv.data() + sv.size()) {
                        auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
                        if (opt == "PXAT") {
                            int64_t rem = val - now_ms;
                            if (rem <= 0) return; // Key expired while server was offline; do not load
                            ttl = std::chrono::milliseconds(rem);
                        } else if (opt == "EXAT") {
                            int64_t rem = (val * 1000) - now_ms;
                            if (rem <= 0) return; // Key expired while offline
                            ttl = std::chrono::milliseconds(rem);
                        } else if (opt == "EX") {
                            ttl = std::chrono::milliseconds(val * 1000);
                        } else if (opt == "PX") {
                            ttl = std::chrono::milliseconds(val);
                        }
                    }
                }
                cache_.put(args[1].str_val, args[2].str_val, ttl);
            } else if (cmd == "DEL" && args.size() >= 2) {
                for (size_t i = 1; i < args.size(); ++i) {
                    cache_.remove(args[i].str_val);
                }
            }
        });
        std::cout << "[INFO] AOF Write-Ahead Log: Replayed " << replayed << " state operations from " << wal_.path() << "\n";
    }

    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ == INVALID_SOCKET_VALUE) {
        std::cerr << "[ERROR] Failed to create socket\n";
        return;
    }

    int opt = 1;
#ifdef _WIN32
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
    setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(static_cast<uint16_t>(config_.port));

    if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
        std::cerr << "[ERROR] Failed to bind to port " << config_.port << "\n";
        SOCKET_CLOSE(server_fd_);
        server_fd_ = INVALID_SOCKET_VALUE;
        return;
    }

    if (listen(server_fd_, 128) < 0) {
        std::cerr << "[ERROR] Failed to listen on socket\n";
        SOCKET_CLOSE(server_fd_);
        server_fd_ = INVALID_SOCKET_VALUE;
        return;
    }

    is_running_ = true;
    std::cout << "========================================================\n";
    std::cout << " Distributed KV Store & Cache Server Started on Port " << config_.port << "\n";
    std::cout << " Node ID: " << config_.node_id << " | Cluster Mode: " << (is_cluster_mode_ ? "ENABLED" : "STANDALONE") << "\n";
    if (is_cluster_mode_) {
        std::cout << " Cluster Nodes: " << ring_.total_nodes() << " | Routing: " << config_.routing_mode
                  << " | Replication: R=" << config_.replication_factor << "\n";
    }
    std::cout << " Protocol: Redis RESP2 | Shards: " << cache_.num_shards() << " (alignas(64))\n";
    std::cout << " Durability: " << (wal_.is_enabled() ? ("AOF Enabled (" + wal_.path() + ")") : "IN-MEMORY ONLY") << "\n";
    std::cout << " Connect using redis-cli: redis-cli -p " << config_.port << "\n";
    std::cout << "========================================================\n";

    accept_thread_ = std::thread(&KvServer::accept_loop, this);
    ttl_sweeper_thread_ = std::thread(&KvServer::ttl_sweeper_loop, this);
    if (is_cluster_mode_ && config_.replication_factor > 1) {
        replication_thread_ = std::thread(&KvServer::replication_worker_loop, this);
    }
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

    if (ttl_sweeper_thread_.joinable()) {
        ttl_sweeper_thread_.join();
    }

    if (replication_thread_.joinable()) {
        replication_thread_.join();
    }

    {
        std::lock_guard<std::mutex> lock(threads_mutex_);
        for (auto& worker : worker_threads_) {
            if (worker.th.joinable()) {
                worker.th.join();
            }
        }
        worker_threads_.clear();
    }

    cleanup_networking();
    std::cout << "[INFO] Server stopped gracefully.\n";
}

void KvServer::reap_finished_threads() {
    std::lock_guard<std::mutex> lock(threads_mutex_);
    for (auto it = worker_threads_.begin(); it != worker_threads_.end(); ) {
        if (it->done->load(std::memory_order_relaxed)) {
            if (it->th.joinable()) {
                it->th.join();
            }
            it = worker_threads_.erase(it);
        } else {
            ++it;
        }
    }
}

void KvServer::ttl_sweeper_loop() {
    while (is_running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        if (!is_running_) break;

        size_t loop_count = 0;
        while (is_running_ && loop_count++ < 10) {
            size_t expired = cache_.active_expire_step(20);
            if (expired < 5) break;
        }
    }
}

void KvServer::record_latency(uint64_t latency_us) {
    total_requests_.fetch_add(1, std::memory_order_relaxed);
    if (latency_us < 50) lat_under_50us_.fetch_add(1, std::memory_order_relaxed);
    else if (latency_us < 100) lat_under_100us_.fetch_add(1, std::memory_order_relaxed);
    else if (latency_us < 250) lat_under_250us_.fetch_add(1, std::memory_order_relaxed);
    else if (latency_us < 500) lat_under_500us_.fetch_add(1, std::memory_order_relaxed);
    else if (latency_us < 1000) lat_under_1ms_.fetch_add(1, std::memory_order_relaxed);
    else if (latency_us < 5000) lat_under_5ms_.fetch_add(1, std::memory_order_relaxed);
    else lat_over_5ms_.fetch_add(1, std::memory_order_relaxed);
}

void KvServer::accept_loop() {
    while (is_running_) {
        reap_finished_threads();

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

        int nodelay_flag = 1;
#ifdef _WIN32
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char*>(&nodelay_flag), sizeof(nodelay_flag));
#else
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay_flag, sizeof(nodelay_flag));
#endif

        auto done_flag = std::make_shared<std::atomic<bool>>(false);
        std::lock_guard<std::mutex> lock(threads_mutex_);
        worker_threads_.push_back({
            std::thread(&KvServer::handle_client, this, client_fd, done_flag),
            done_flag
        });
    }
}

void KvServer::handle_client(socket_t client_fd, std::shared_ptr<std::atomic<bool>> done_flag) {
    active_clients_.fetch_add(1, std::memory_order_relaxed);
    total_connections_.fetch_add(1, std::memory_order_relaxed);

    std::string buffer;
    buffer.reserve(8192);
    char read_buf[8192];

    while (is_running_) {
        int bytes_read = recv(client_fd, read_buf, sizeof(read_buf), 0);
        if (bytes_read <= 0) {
            break;
        }

        buffer.append(read_buf, bytes_read);

        size_t read_cursor = 0;
        std::string batch_out;

        while (read_cursor < buffer.size()) {
            std::string_view remaining_view(buffer.data() + read_cursor, buffer.size() - read_cursor);
            size_t bytes_consumed = 0;
            auto resp_opt = RespParser::parse(remaining_view, bytes_consumed);
            if (!resp_opt.has_value() || bytes_consumed == 0) {
                break;
            }

            read_cursor += bytes_consumed;

            if (resp_opt->type == RespType::Array) {
                auto start_time = std::chrono::high_resolution_clock::now();
                RespObject response = execute_command(resp_opt->array_val);
                auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::high_resolution_clock::now() - start_time).count();
                record_latency(static_cast<uint64_t>(elapsed_us));

                response.serialize_to(batch_out);
            }
        }

        if (!batch_out.empty()) {
            size_t total_sent = 0;
            while (total_sent < batch_out.size()) {
                int sent = send(client_fd, batch_out.data() + total_sent,
                                static_cast<int>(batch_out.size() - total_sent), 0);
                if (sent <= 0) break;
                total_sent += static_cast<size_t>(sent);
            }
        }

        if (read_cursor > 0) {
            if (read_cursor >= buffer.size()) {
                buffer.clear();
            } else {
                buffer.erase(0, read_cursor);
            }
        }
    }

    active_clients_.fetch_sub(1, std::memory_order_relaxed);
    SOCKET_CLOSE(client_fd);
    done_flag->store(true, std::memory_order_release);
}

RespObject KvServer::forward_to_peer(const std::string& host, int port, const std::vector<RespObject>& args) {
    socket_t peer_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (peer_sock == INVALID_SOCKET_VALUE) {
        return RespObject::Error("ERR cluster proxy failed to create socket");
    }

    sockaddr_in peer_addr{};
    peer_addr.sin_family = AF_INET;
    peer_addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, host.c_str(), &peer_addr.sin_addr);

    if (connect(peer_sock, (struct sockaddr*)&peer_addr, sizeof(peer_addr)) < 0) {
        SOCKET_CLOSE(peer_sock);
        return RespObject::Error("ERR cluster node unreachable");
    }

    std::string serialized = RespObject::Array(args).serialize();
    size_t total_sent = 0;
    while (total_sent < serialized.size()) {
        int sent = send(peer_sock, serialized.data() + total_sent,
                        static_cast<int>(serialized.size() - total_sent), 0);
        if (sent <= 0) {
            SOCKET_CLOSE(peer_sock);
            return RespObject::Error("ERR cluster proxy failed to send command");
        }
        total_sent += static_cast<size_t>(sent);
    }

    std::string response_buf;
    char chunk[4096];
    size_t consumed = 0;
    std::optional<RespObject> parsed = std::nullopt;

    while (true) {
        int read_bytes = recv(peer_sock, chunk, sizeof(chunk), 0);
        if (read_bytes <= 0) break;
        response_buf.append(chunk, read_bytes);

        consumed = 0;
        parsed = RespParser::parse(response_buf, consumed);
        if (parsed.has_value() && consumed > 0) {
            break;
        }
    }

    SOCKET_CLOSE(peer_sock);

    if (!parsed.has_value() || consumed == 0) {
        if (response_buf.empty()) {
            return RespObject::Error("ERR cluster node empty reply");
        }
        return RespObject::Error("ERR malformed peer response");
    }

    return *parsed;
}

void KvServer::replicate_async(const std::string& key, const std::vector<RespObject>& args) {
    if (!is_cluster_mode_ || config_.replication_factor <= 1) return;

    auto replicas = ring_.get_replica_nodes(key, config_.replication_factor);

    // Wrap the command in a REPLWRITE sentinel with cluster secret so the receiving
    // replica can verify authorization and skip both check_routing() and replicate_async(),
    // breaking the replication loop.
    std::vector<RespObject> wrapped;
    wrapped.reserve(args.size() + 2);
    wrapped.push_back(RespObject::BulkString("REPLWRITE"));
    wrapped.push_back(RespObject::BulkString(config_.cluster_secret));
    for (const auto& a : args) wrapped.push_back(a);

    std::vector<std::pair<std::string, int>> target_peers;
    {
        std::lock_guard<std::mutex> lock(peers_mutex_);
        for (size_t i = 1; i < replicas.size(); ++i) {
            const std::string& replica_node = replicas[i];
            if (replica_node == config_.node_id) continue;

            auto peer_it = peers_map_.find(replica_node);
            if (peer_it != peers_map_.end()) {
                target_peers.push_back(peer_it->second);
            }
        }
    }

    if (!target_peers.empty()) {
        std::lock_guard<std::mutex> lock(repl_mutex_);
        for (const auto& peer : target_peers) {
            repl_queue_.push_back({peer.first, peer.second, wrapped, 0, std::chrono::steady_clock::now()});
        }
    }
}

void KvServer::replication_worker_loop() {
    while (is_running_) {
        std::vector<ReplicationTask> batch;
        auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(repl_mutex_);
            auto it = repl_queue_.begin();
            while (it != repl_queue_.end() && batch.size() < 50) {
                if (it->next_retry <= now) {
                    batch.push_back(std::move(*it));
                    it = repl_queue_.erase(it);
                } else {
                    ++it;
                }
            }
        }

        if (batch.empty()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            continue;
        }

        std::vector<ReplicationTask> failed_tasks;
        for (auto& task : batch) {
            RespObject resp = forward_to_peer(task.peer_host, task.peer_port, task.cmd);
            if (resp.type == RespType::Error) {
                if (task.retry_count < 3) {
                    task.retry_count++;
                    int backoff_ms = 50 * (1 << task.retry_count); // 100ms, 200ms, 400ms
                    task.next_retry = std::chrono::steady_clock::now() + std::chrono::milliseconds(backoff_ms);
                    failed_tasks.push_back(std::move(task));
                } else {
                    replication_failures_.fetch_add(1, std::memory_order_relaxed);
                    std::cerr << "[REPL ERROR] Replication dropped after 3 retries to "
                              << task.peer_host << ":" << task.peer_port
                              << " (" << resp.str_val << ")\n";
                }
            } else {
                replicated_ops_.fetch_add(1, std::memory_order_relaxed);
            }
        }

        if (!failed_tasks.empty()) {
            std::lock_guard<std::mutex> lock(repl_mutex_);
            repl_queue_.insert(repl_queue_.end(),
                               std::make_move_iterator(failed_tasks.begin()),
                               std::make_move_iterator(failed_tasks.end()));
        }
    }
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

    // ----------------------------------------------------------------------
    // REPLWRITE: Internal replication sentinel Ã¢â‚¬â€ apply locally, no re-routing.
    // When the primary replicates a write to a replica it prepends REPLWRITE so
    // the replica bypasses check_routing() (which would proxy back to the
    // primary, creating a loop) and skips replicate_async() (the replica must
    // not fan-out again).
    // ----------------------------------------------------------------------
    if (cmd == "REPLWRITE") {
        if (args.size() < 3 || args[1].str_val != config_.cluster_secret) {
            return RespObject::Error("ERR unknown command 'REPLWRITE'");
        }
        // Strip the REPLWRITE and cluster_secret prefix and execute the inner command locally.
        std::vector<RespObject> inner(args.begin() + 2, args.end());
        if (inner.empty()) return RespObject::Error("ERR REPLWRITE missing inner command");
        std::string inner_cmd = inner[0].str_val;
        std::transform(inner_cmd.begin(), inner_cmd.end(), inner_cmd.begin(), ::toupper);

        if (inner_cmd == "SET" && inner.size() >= 3) {
            ParsedTtl parsed = parse_ttl_args(inner, 3);
            if (!parsed.valid) return RespObject::Error(parsed.error_msg);

            std::vector<RespObject> wal_cmd;
            if (parsed.ttl.has_value() && parsed.abs_expire_ms > 0) {
                wal_cmd = {
                    RespObject::BulkString("SET"),
                    RespObject::BulkString(inner[1].str_val),
                    RespObject::BulkString(inner[2].str_val),
                    RespObject::BulkString("PXAT"),
                    RespObject::BulkString(std::to_string(parsed.abs_expire_ms))
                };
            } else {
                wal_cmd = {
                    RespObject::BulkString("SET"),
                    RespObject::BulkString(inner[1].str_val),
                    RespObject::BulkString(inner[2].str_val)
                };
            }

            if (!wal_.append_command(wal_cmd)) {
                return RespObject::Error("ERR replica write-ahead log append failed");
            }
            cache_.put(inner[1].str_val, inner[2].str_val, parsed.ttl);
            return RespObject::SimpleString("OK");
        }
        if (inner_cmd == "DEL" && inner.size() >= 2) {
            if (!wal_.append_command(inner)) {
                return RespObject::Error("ERR replica write-ahead log append failed");
            }
            int64_t count = 0;
            for (size_t i = 1; i < inner.size(); ++i) {
                if (cache_.remove(inner[i].str_val)) count++;
            }
            return RespObject::Integer(count);
        }
        return RespObject::Error("ERR REPLWRITE unsupported inner command");
    }

    // ----------------------------------------------------------------------
    // Cluster Management & Introspection Commands
    // ----------------------------------------------------------------------
    if (cmd == "CLUSTER") {
        if (args.size() < 2) return RespObject::Error("ERR wrong number of arguments for 'cluster' command");
        std::string subcmd = args[1].str_val;
        std::transform(subcmd.begin(), subcmd.end(), subcmd.begin(), ::toupper);

        if (subcmd == "KEYSLOT") {
            if (args.size() < 3) return RespObject::Error("ERR wrong number of arguments for 'cluster keyslot'");
            return RespObject::Integer(ring_.get_key_slot(args[2].str_val));
        }

        if (subcmd == "NODES") {
            std::lock_guard<std::mutex> lock(peers_mutex_);
            std::ostringstream oss;
            for (const auto& [nid, endpoint] : peers_map_) {
                oss << nid << " " << endpoint.first << ":" << endpoint.second << " ";
                if (nid == config_.node_id) {
                    oss << "myself,master connected 0-16383\n";
                } else {
                    oss << "master connected\n";
                }
            }
            return RespObject::BulkString(oss.str());
        }

        if (subcmd == "MEET" || subcmd == "ADDNODE") {
            if (args.size() < 4) return RespObject::Error("ERR syntax error for cluster meet <node-id> <host:port>");
            std::string new_node_id = args[2].str_val;
            std::string endpoint = args[3].str_val;
            size_t colon = endpoint.find(':');
            if (colon == std::string::npos) return RespObject::Error("ERR host:port format required");
            std::string host = endpoint.substr(0, colon);
            int port = std::stoi(endpoint.substr(colon + 1));

            {
                std::lock_guard<std::mutex> lock(peers_mutex_);
                ring_.add_node(new_node_id);
                peers_map_[new_node_id] = {host, port};
                is_cluster_mode_ = true;
            }
            return RespObject::SimpleString("OK");
        }

        return RespObject::Error("ERR unknown cluster subcommand '" + subcmd + "'");
    }

    // ----------------------------------------------------------------------
    // Key-Routing & Cluster Sharding Gate
    // ----------------------------------------------------------------------
    auto check_routing = [this](const std::string& key, const std::vector<RespObject>& orig_args) -> std::optional<RespObject> {
        if (!is_cluster_mode_) return std::nullopt;

        std::string owner = ring_.get_node(key);
        if (owner.empty() || owner == config_.node_id) {
            return std::nullopt; // Local execution
        }

        std::lock_guard<std::mutex> lock(peers_mutex_);
        auto it = peers_map_.find(owner);
        if (it == peers_map_.end()) return std::nullopt;

        if (config_.routing_mode == "redirect") {
            uint32_t slot = ring_.get_key_slot(key);
            return RespObject::Error("MOVED " + std::to_string(slot) + " " + it->second.first + ":" + std::to_string(it->second.second));
        } else {
            // Transparent Proxy Mode
            return forward_to_peer(it->second.first, it->second.second, orig_args);
        }
    };

    if (cmd == "SET") {
        if (args.size() < 3) return RespObject::Error("ERR wrong number of arguments for 'set' command");
        const std::string& key = args[1].str_val;
        const std::string& val = args[2].str_val;

        auto routed = check_routing(key, args);
        if (routed.has_value()) return *routed;

        ParsedTtl parsed = parse_ttl_args(args, 3);
        if (!parsed.valid) return RespObject::Error(parsed.error_msg);

        // Prepare durable WAL command with absolute PXAT expiration to avoid restart drift
        std::vector<RespObject> wal_cmd;
        if (parsed.ttl.has_value() && parsed.abs_expire_ms > 0) {
            wal_cmd = {
                RespObject::BulkString("SET"),
                RespObject::BulkString(key),
                RespObject::BulkString(val),
                RespObject::BulkString("PXAT"),
                RespObject::BulkString(std::to_string(parsed.abs_expire_ms))
            };
        } else {
            wal_cmd = {
                RespObject::BulkString("SET"),
                RespObject::BulkString(key),
                RespObject::BulkString(val)
            };
        }

        // Durability: Log append MUST succeed before in-memory cache mutation
        if (!wal_.append_command(wal_cmd)) {
            return RespObject::Error("ERR write-ahead log append failed");
        }

        cache_.put(key, val, parsed.ttl);
        replicate_async(key, args);
        return RespObject::SimpleString("OK");
    }

    if (cmd == "GET") {
        if (args.size() != 2) return RespObject::Error("ERR wrong number of arguments for 'get' command");
        const std::string& key = args[1].str_val;

        auto routed = check_routing(key, args);
        if (routed.has_value()) return *routed;

        auto val = cache_.get(key);
        if (val.has_value()) {
            return RespObject::BulkString(*val);
        }
        return RespObject::NullBulkString();
    }

    if (cmd == "DEL") {
        if (args.size() < 2) return RespObject::Error("ERR wrong number of arguments for 'del' command");
        if (is_cluster_mode_) {
            uint32_t first_slot = ring_.get_key_slot(args[1].str_val);
            for (size_t i = 2; i < args.size(); ++i) {
                if (ring_.get_key_slot(args[i].str_val) != first_slot) {
                    return RespObject::Error("CROSSSLOT Keys in request don't hash to the same slot");
                }
            }
        }
        const std::string& key = args[1].str_val;

        auto routed = check_routing(key, args);
        if (routed.has_value()) return *routed;

        if (!wal_.append_command(args)) {
            return RespObject::Error("ERR write-ahead log append failed");
        }

        int64_t count = 0;
        for (size_t i = 1; i < args.size(); ++i) {
            if (cache_.remove(args[i].str_val)) {
                count++;
            }
        }
        replicate_async(key, args);
        return RespObject::Integer(count);
    }

    if (cmd == "EXISTS") {
        if (args.size() < 2) return RespObject::Error("ERR wrong number of arguments for 'exists' command");
        if (is_cluster_mode_) {
            uint32_t first_slot = ring_.get_key_slot(args[1].str_val);
            for (size_t i = 2; i < args.size(); ++i) {
                if (ring_.get_key_slot(args[i].str_val) != first_slot) {
                    return RespObject::Error("CROSSSLOT Keys in request don't hash to the same slot");
                }
            }
        }
        const std::string& key = args[1].str_val;

        auto routed = check_routing(key, args);
        if (routed.has_value()) return *routed;

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
        oss << "shards:" << stats.num_shards << "\r\n";
        oss << "hits:" << stats.total_hits << "\r\n";
        oss << "misses:" << stats.total_misses << "\r\n";
        oss << "evictions:" << stats.total_evictions << "\r\n";
        oss << "expired_keys:" << stats.total_expired_keys << "\r\n";
        double hit_rate = (stats.total_hits + stats.total_misses > 0)
            ? (100.0 * stats.total_hits / (stats.total_hits + stats.total_misses))
            : 0.0;
        oss << "hit_rate_pct:" << hit_rate << "\r\n";
        oss << "active_clients:" << active_clients_.load(std::memory_order_relaxed) << "\r\n";
        oss << "total_connections:" << total_connections_.load(std::memory_order_relaxed) << "\r\n";

        // Latency Telemetry
        uint64_t tot = total_requests_.load(std::memory_order_relaxed);
        oss << "total_requests:" << tot << "\r\n";
        oss << "lat_under_50us:" << lat_under_50us_.load(std::memory_order_relaxed) << "\r\n";
        oss << "lat_under_100us:" << lat_under_100us_.load(std::memory_order_relaxed) << "\r\n";
        oss << "lat_under_250us:" << lat_under_250us_.load(std::memory_order_relaxed) << "\r\n";
        oss << "lat_under_1ms:" << lat_under_1ms_.load(std::memory_order_relaxed) << "\r\n";
        oss << "lat_over_5ms:" << lat_over_5ms_.load(std::memory_order_relaxed) << "\r\n";

        // Cluster Telemetry
        oss << "# Cluster\r\n";
        oss << "cluster_enabled:" << (is_cluster_mode_ ? 1 : 0) << "\r\n";
        oss << "node_id:" << config_.node_id << "\r\n";
        oss << "cluster_nodes:" << ring_.total_nodes() << "\r\n";
        oss << "routing_mode:" << config_.routing_mode << "\r\n";
        oss << "replication_factor:" << config_.replication_factor << "\r\n";
        oss << "replicated_ops:" << replicated_ops_.load(std::memory_order_relaxed) << "\r\n";

        // Persistence Telemetry
        oss << "# Persistence\r\n";
        oss << "aof_enabled:" << (wal_.is_enabled() ? 1 : 0) << "\r\n";
        oss << "aof_path:" << wal_.path() << "\r\n";
        oss << "aof_bytes:" << wal_.bytes_written() << "\r\n";
        oss << "aof_syncs:" << wal_.sync_count() << "\r\n";

        return RespObject::BulkString(oss.str());
    }

    if (cmd == "COMMAND") {
        return RespObject::Array({});
    }

    return RespObject::Error("ERR unknown command '" + cmd + "'");
}

} // namespace kvstore
