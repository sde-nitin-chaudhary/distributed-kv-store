#include "wal.hpp"
#include <iostream>
#include <filesystem>
#ifdef _WIN32
#  include <windows.h>
#  include <io.h>
#  include <fcntl.h>
#else
#  include <unistd.h>
#  include <fcntl.h>
#  include <sys/stat.h>
#endif

namespace kvstore {

WriteAheadLog::WriteAheadLog(const std::string& aof_path, FsyncPolicy policy)
    : path_(aof_path), policy_(policy), enabled_(!aof_path.empty()) {

    if (enabled_) {
        is_running_ = true;
        try {
            std::filesystem::path p(path_);
            if (p.has_parent_path()) {
                std::filesystem::create_directories(p.parent_path());
            }
        } catch (const std::exception& e) {
            std::cerr << "[WAL ERROR] Failed to create directories for: " << path_ << " (" << e.what() << ")\n";
        }

        {
            std::lock_guard<std::mutex> lock(file_mutex_);
            file_stream_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
            if (!file_stream_.is_open() || file_stream_.fail()) {
                std::cerr << "[WAL ERROR] Failed to open AOF file: " << path_ << "\n";
                throw std::runtime_error("Fatal: failed to open configured WAL file: " + path_);
            }
        }
        if (policy_ == FsyncPolicy::EverySec) {
            fsync_thread_ = std::thread(&WriteAheadLog::fsync_thread_loop, this);
        }
    }
}

WriteAheadLog::~WriteAheadLog() {
    if (is_running_.exchange(false)) {
        if (fsync_thread_.joinable()) {
            fsync_thread_.join();
        }
        flush_and_sync();
        std::lock_guard<std::mutex> lock(file_mutex_);
        if (file_stream_.is_open()) {
            file_stream_.close();
        }
    }
}

bool WriteAheadLog::is_open() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(file_mutex_));
    return file_stream_.is_open() && !file_stream_.fail();
}

bool WriteAheadLog::append_command(const std::vector<RespObject>& args) {
    if (!enabled_ || args.empty()) return true;

    RespObject array_obj = RespObject::Array(args);
    std::string serialized = array_obj.serialize();

    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!file_stream_.is_open() || file_stream_.fail()) return false;

    file_stream_.write(serialized.data(), static_cast<std::streamsize>(serialized.size()));
    if (file_stream_.fail()) return false;
    bytes_written_.fetch_add(serialized.size(), std::memory_order_relaxed);

    if (policy_ == FsyncPolicy::Always) {
        file_stream_.flush();
        if (file_stream_.fail()) return false;
        // OS-level fsync for FsyncPolicy::Always — flush_and_sync() handles EverySec.
#ifdef _WIN32
        HANDLE h = CreateFileA(path_.c_str(), GENERIC_WRITE,
                               FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                               OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h == INVALID_HANDLE_VALUE) return false;
        BOOL ok = FlushFileBuffers(h);
        CloseHandle(h);
        if (!ok) return false;
#else
        int fd2 = ::open(path_.c_str(), O_WRONLY | O_APPEND);
        if (fd2 < 0) return false;
        int r = ::fsync(fd2);
        ::close(fd2);
        if (r != 0) return false;
#endif
        sync_count_.fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

void WriteAheadLog::flush_and_sync() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!enabled_ || !file_stream_.is_open()) return;
    file_stream_.flush();
    // OS-level fsync: flush() only drains the C++ stream buffer; kernel page
    // cache pages are not written to stable storage until fsync/FlushFileBuffers.
#ifdef _WIN32
    HANDLE h = CreateFileA(path_.c_str(), GENERIC_WRITE,
                           FILE_SHARE_READ | FILE_SHARE_WRITE, nullptr,
                           OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (h != INVALID_HANDLE_VALUE) { FlushFileBuffers(h); CloseHandle(h); }
#else
    {
        int fd = ::open(path_.c_str(), O_WRONLY | O_APPEND);
        if (fd >= 0) { ::fsync(fd); ::close(fd); }
    }
#endif
    sync_count_.fetch_add(1, std::memory_order_relaxed);
}

void WriteAheadLog::fsync_thread_loop() {
    while (is_running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!is_running_) break;
        flush_and_sync();
    }
}

size_t WriteAheadLog::replay(const std::function<void(const std::vector<RespObject>&)>& command_handler) {
    if (!enabled_ || path_.empty()) return 0;

    // Close the write-stream opened by the constructor so we can (a) read the
    // file cleanly via ifstream and (b) truncate it on Windows where a file
    // open for writing cannot be truncated by another handle.
    {
        std::lock_guard<std::mutex> lock(file_mutex_);
        if (file_stream_.is_open()) {
            file_stream_.flush();
            file_stream_.close();
        }
    }

    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) {
        // File does not exist yet; re-open for appending and return.
        std::lock_guard<std::mutex> lock(file_mutex_);
        file_stream_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
        return 0;
    }

    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    size_t cursor = 0;
    size_t replayed = 0;
    while (cursor < content.size()) {
        std::string_view view(content.data() + cursor, content.size() - cursor);
        size_t bytes_consumed = 0;
        auto resp = RespParser::parse(view, bytes_consumed);
        if (!resp.has_value() || bytes_consumed == 0) break;
        cursor += bytes_consumed;

        if (resp->type == RespType::Array) {
            command_handler(resp->array_val);
            replayed++;
        }
    }

    // Bug fix: truncate any partial tail record before re-opening for append.
    // Without truncation, the garbage bytes after `cursor` stay on disk and
    // permanently make every subsequent write unreachable on the next replay
    // (the parser stops at the corrupt tail on next startup).
    {
        std::lock_guard<std::mutex> lock(file_mutex_);
        if (cursor < content.size()) {
#ifdef _WIN32
            FILE* fp = fopen(path_.c_str(), "r+b");
            if (fp) {
                _chsize_s(_fileno(fp), static_cast<__int64>(cursor));
                fclose(fp);
            }
#else
            ::truncate(path_.c_str(), static_cast<off_t>(cursor));
#endif
        }
        file_stream_.open(path_, std::ios::out | std::ios::app | std::ios::binary);
        // Track only the bytes that were fully parsed (not the raw file size).
        bytes_written_.store(cursor, std::memory_order_relaxed);
    }

    return replayed;
}

} // namespace kvstore
