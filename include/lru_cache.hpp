#pragma once

#include <string>
#include <unordered_map>
#include <list>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <atomic>
#include <optional>
#include <chrono>

namespace kvstore {

enum class EvictionPolicy {
    LRU,
    LFU
};

struct CacheStats {
    uint64_t total_hits{0};
    uint64_t total_misses{0};
    uint64_t total_evictions{0};
    size_t current_size{0};
    size_t capacity{0};
};

class ThreadSafeCache {
public:
    explicit ThreadSafeCache(size_t capacity = 100000, EvictionPolicy policy = EvictionPolicy::LRU);

    std::optional<std::string> get(const std::string& key);
    void put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl = std::nullopt);
    bool remove(const std::string& key);
    bool exists(const std::string& key);
    size_t size() const;
    CacheStats get_stats() const;
    void clear();

private:
    struct CacheItem {
        std::string key;
        std::string value;
        uint64_t frequency{1};
        std::chrono::steady_clock::time_point expire_at{std::chrono::steady_clock::time_point::max()};
    };

    bool is_expired(const CacheItem& item) const;
    void evict_one_locked();

    size_t capacity_;
    EvictionPolicy policy_;

    // Mutex protecting data structures
    mutable std::shared_mutex rw_mutex_;

    // Storage for LRU: list of keys (most recent at front, oldest at back)
    std::list<std::string> lru_list_;
    // Map: key -> {CacheItem, iterator into lru_list_}
    struct LruEntry {
        CacheItem item;
        std::list<std::string>::iterator list_it;
    };
    std::unordered_map<std::string, LruEntry> lru_map_;

    // Storage for LFU: frequency -> list of keys
    std::unordered_map<std::string, CacheItem> lfu_items_;
    std::unordered_map<std::string, std::list<std::string>::iterator> lfu_node_map_;
    std::map<uint64_t, std::list<std::string>> lfu_freq_map_;
    uint64_t min_freq_{0};

    // Atomic telemetry counters
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> evictions_{0};
};

} // namespace kvstore
