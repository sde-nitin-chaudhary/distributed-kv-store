#pragma once

#include "lru_cache.hpp"
#include "resp_parser.hpp"
#include "consistent_hash.hpp"
#include "wal.hpp"
#include <string>
#include <atomic>
#include <thread>
#include <vector>
#include <memory>
#include <mutex>
#include <map>
#include <cstdint>
#include <chrono>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    using socket_t = SOCKET;
    #define INVALID_SOCKET_VALUE INVALID_SOCKET
    #define SOCKET_CLOSE(s) closesocket(s)
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <netinet/tcp.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    using socket_t = int;
    #define INVALID_SOCKET_VALUE (-1)
    #define SOCKET_CLOSE(s) close(s)
#endif

namespace kvstore {

struct ServerConfig {
    int port{6380};
    size_t cache_capacity{100000};
    EvictionPolicy policy{EvictionPolicy::LRU};
    size_t num_shards{64};
    std::string node_id{"node-1"};
    std::string peers{""}; // e.g. "node-1@127.0.0.1:6380,node-2@127.0.0.1:6381"
    std::string routing_mode{"proxy"}; // "proxy" or "redirect"
    size_t replication_factor{1};
    std::string aof_path{""};
    FsyncPolicy fsync_policy{FsyncPolicy::EverySec};
    std::string cluster_secret{""};
};

class KvServer {
public:
    explicit KvServer(int port = 6380,
                      size_t cache_capacity = 100000,
                      EvictionPolicy policy = EvictionPolicy::LRU,
                      size_t num_shards = 64);

    explicit KvServer(const ServerConfig& config);
    ~KvServer();

    void start();
    void stop();

    bool is_running() const { return is_running_.load(); }
    size_t active_clients() const { return active_clients_.load(); }
    size_t total_connections() const { return total_connections_.load(); }
    uint64_t replicated_ops() const { return replicated_ops_.load(); }
    uint64_t replication_failures() const { return replication_failures_.load(); }

    const ConsistentHashRing& hash_ring() const { return ring_; }
    const WriteAheadLog& wal() const { return wal_; }

private:
    void init_networking();
    void cleanup_networking();
    void accept_loop();
    void handle_client(socket_t client_fd, std::shared_ptr<std::atomic<bool>> done_flag);
    void ttl_sweeper_loop();
    void replication_worker_loop();
    void reap_finished_threads();

    RespObject execute_command(const std::vector<RespObject>& args);
    RespObject forward_to_peer(const std::string& host, int port, const std::vector<RespObject>& args);
    void replicate_async(const std::string& key, const std::vector<RespObject>& args);
    void record_latency(uint64_t latency_us);

    ServerConfig config_;
    socket_t server_fd_{INVALID_SOCKET_VALUE};
    std::atomic<bool> is_running_{false};
    ThreadSafeCache cache_;
    ConsistentHashRing ring_{150};
    WriteAheadLog wal_;

    // Cluster peer registry: node_id -> pair<host, port>
    std::map<std::string, std::pair<std::string, int>> peers_map_;
    mutable std::mutex peers_mutex_;
    std::atomic<bool> is_cluster_mode_{false};

    std::thread accept_thread_;
    std::thread ttl_sweeper_thread_;
    std::thread replication_thread_;

    struct WorkerEntry {
        std::thread th;
        std::shared_ptr<std::atomic<bool>> done;
    };
    std::vector<WorkerEntry> worker_threads_;
    std::mutex threads_mutex_;

    // Asynchronous replication queue
    struct ReplicationTask {
        std::string peer_host;
        int peer_port;
        std::vector<RespObject> cmd;
        int retry_count{0};
        std::chrono::steady_clock::time_point next_retry{std::chrono::steady_clock::now()};
    };
    std::vector<ReplicationTask> repl_queue_;
    std::mutex repl_mutex_;

    std::atomic<uint64_t> active_clients_{0};
    std::atomic<uint64_t> total_connections_{0};
    std::atomic<uint64_t> replicated_ops_{0};
    std::atomic<uint64_t> replication_failures_{0};

    // Latency histogram buckets (microseconds)
    std::atomic<uint64_t> lat_under_50us_{0};
    std::atomic<uint64_t> lat_under_100us_{0};
    std::atomic<uint64_t> lat_under_250us_{0};
    std::atomic<uint64_t> lat_under_500us_{0};
    std::atomic<uint64_t> lat_under_1ms_{0};
    std::atomic<uint64_t> lat_under_5ms_{0};
    std::atomic<uint64_t> lat_over_5ms_{0};
    std::atomic<uint64_t> total_requests_{0};
};

} // namespace kvstore
