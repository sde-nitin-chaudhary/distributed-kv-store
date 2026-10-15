#pragma once

#include "resp_parser.hpp"
#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include <atomic>
#include <functional>
#include <thread>
#include <chrono>
#include <cstdint>

namespace kvstore {

enum class FsyncPolicy {
    Always,
    EverySec,
    No
};

// ==============================================================================
// WriteAheadLog (AOF Engine): Crash-Durability Write-Ahead Logging
//
// Invariants:
//   - Appends mutating commands (SET, DEL) in RESP2 wire byte format.
//   - Supports fsync policies: Always, EverySec (default background flusher), No.
//   - Provides startup replay to fully reconstruct cache state from disk.
// ==============================================================================
class WriteAheadLog {
public:
    explicit WriteAheadLog(const std::string& aof_path = "", FsyncPolicy policy = FsyncPolicy::EverySec);
    ~WriteAheadLog();

    bool is_enabled() const { return enabled_; }
    bool is_open() const;
    const std::string& path() const { return path_; }
    uint64_t bytes_written() const { return bytes_written_.load(std::memory_order_relaxed); }
    uint64_t sync_count() const { return sync_count_.load(std::memory_order_relaxed); }

    // Appends a mutating command to the WAL file; returns false on disk failure
    bool append_command(const std::vector<RespObject>& args);

    // Replays existing AOF log into the server cache on startup
    size_t replay(const std::function<void(const std::vector<RespObject>&)>& command_handler);

    // Flushes in-memory buffer and fsyncs to physical disk
    void flush_and_sync();

private:
    void fsync_thread_loop();

    std::string path_;
    FsyncPolicy policy_;
    bool enabled_{false};

    std::mutex file_mutex_;
    std::ofstream file_stream_;

    std::atomic<bool> is_running_{false};
    std::thread fsync_thread_;

    std::atomic<uint64_t> bytes_written_{0};
    std::atomic<uint64_t> sync_count_{0};
};

} // namespace kvstore
