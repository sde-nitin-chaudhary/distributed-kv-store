#include "kv_server.hpp"
#include <iostream>
#include <csignal>
#include <memory>
#include <chrono>
#include <thread>

std::unique_ptr<kvstore::KvServer> g_server;

void handle_signal(int signal) {
    if (g_server) {
        std::cout << "\n[INFO] Received signal " << signal << ", shutting down...\n";
        g_server->stop();
    }
    std::exit(0);
}

int main(int argc, char* argv[]) {
    kvstore::ServerConfig config;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            config.port = std::stoi(argv[++i]);
        } else if (arg == "--capacity" && i + 1 < argc) {
            config.cache_capacity = std::stoull(argv[++i]);
        } else if (arg == "--shards" && i + 1 < argc) {
            config.num_shards = std::stoull(argv[++i]);
        } else if (arg == "--policy" && i + 1 < argc) {
            std::string pol = argv[++i];
            if (pol == "lfu" || pol == "LFU") config.policy = kvstore::EvictionPolicy::LFU;
            else config.policy = kvstore::EvictionPolicy::LRU;
        } else if (arg == "--node-id" && i + 1 < argc) {
            config.node_id = argv[++i];
        } else if (arg == "--peers" && i + 1 < argc) {
            config.peers = argv[++i];
        } else if (arg == "--routing-mode" && i + 1 < argc) {
            config.routing_mode = argv[++i];
        } else if (arg == "--replication-factor" && i + 1 < argc) {
            config.replication_factor = std::stoull(argv[++i]);
        } else if (arg == "--aof-path" && i + 1 < argc) {
            config.aof_path = argv[++i];
        } else if (arg == "--fsync-policy" && i + 1 < argc) {
            std::string fpol = argv[++i];
            if (fpol == "always") config.fsync_policy = kvstore::FsyncPolicy::Always;
            else if (fpol == "no") config.fsync_policy = kvstore::FsyncPolicy::No;
            else config.fsync_policy = kvstore::FsyncPolicy::EverySec;
        } else if (arg == "--cluster-secret" && i + 1 < argc) {
            config.cluster_secret = argv[++i];
        }
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    g_server = std::make_unique<kvstore::KvServer>(config);
    g_server->start();

    // Main thread waits indefinitely until signal
    std::cout << "[INFO] Server listening. Press Ctrl+C to terminate.\n";
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
