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
    int port = 6380;
    size_t capacity = 100000;
    kvstore::EvictionPolicy policy = kvstore::EvictionPolicy::LRU;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--port" && i + 1 < argc) {
            port = std::stoi(argv[++i]);
        } else if (arg == "--capacity" && i + 1 < argc) {
            capacity = std::stoull(argv[++i]);
        } else if (arg == "--policy" && i + 1 < argc) {
            std::string pol = argv[++i];
            if (pol == "lfu" || pol == "LFU") policy = kvstore::EvictionPolicy::LFU;
            else policy = kvstore::EvictionPolicy::LRU;
        }
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    g_server = std::make_unique<kvstore::KvServer>(port, capacity, policy);
    g_server->start();

    // Main thread waits indefinitely until signal
    std::cout << "[INFO] Server listening. Press Ctrl+C to terminate.\n";
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    return 0;
}
