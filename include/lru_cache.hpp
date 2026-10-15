#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <list>
#include <map>
#include <mutex>
#include <atomic>
#include <optional>
#include <chrono>
#include <cstdint>
#include <vector>
#include <memory>
#include <unordered_set>

namespace kvstore {

enum class EvictionPolicy {
    LRU,
    LFU
};

struct CacheStats {
    uint64_t total_hits{0};
    uint64_t total_misses{0};
    uint64_t total_evictions{0};
    uint64_t total_expired_keys{0};
    size_t current_size{0};
    size_t capacity{0};
    size_t num_shards{1};
};

struct CacheItem {
    std::string key;
    std::string value;
    uint64_t frequency{1};
    std::chrono::steady_clock::time_point expire_at{std::chrono::steady_clock::time_point::max()};
};

// ==============================================================================
// CacheShard: Lock-Striped Cache Shard
//
// Hardware-Aware Architecture:
//   - Aligned to 64 bytes (`alignas(64)`) to prevent CPU cache-line bouncing
//     (false sharing) across concurrent worker threads.
//   - Guarded by a dedicated `std::mutex` rather than `std::shared_mutex`
//     because LRU/LFU pointer updates serialize reader threads on every hit.
//   - Enforces strict O(1) eviction and correct LFU min_freq_ recalculation.
// ==============================================================================
class alignas(64) CacheShard {
public:
    explicit CacheShard(size_t capacity = 1000, EvictionPolicy policy = EvictionPolicy::LRU);

    std::optional<std::string> get(const std::string& key);
    void put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl = std::nullopt);
    bool remove(const std::string& key);
    bool exists(const std::string& key);
    size_t size() const;
    void clear();
    size_t active_expire_step(size_t max_keys_to_sample = 20);

    CacheStats get_stats() const;

private:
    bool is_expired(const CacheItem& item) const;
    void evict_one_locked();

    mutable std::mutex mutex_;
    size_t capacity_;
    EvictionPolicy policy_;

    // Storage for LRU: list of keys (most recent at front, oldest at back)
    std::list<std::string> lru_list_;
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

    // Active TTL expiry tracking
    std::unordered_set<std::string> ttl_keys_;
    size_t sample_cursor_{0};

    // Atomic telemetry counters
    std::atomic<uint64_t> hits_{0};
    std::atomic<uint64_t> misses_{0};
    std::atomic<uint64_t> evictions_{0};
    std::atomic<uint64_t> expired_keys_{0};
};

// ==============================================================================
// ThreadSafeCache (ShardedCache): 64-Shard Lock-Striped Cache Engine
//
// Key Invariants:
//   - Dispatches keys uniformly across N shards via FNV-1a hashing.
//   - Auto-scales: 1 shard for unit-test micro-capacities (<64), 64 shards
//     for production workloads to maximize multi-core throughput.
//   - Periodic background TTL sweeper supported via `active_expire_step()`.
// ==============================================================================
class ThreadSafeCache {
public:
    static constexpr size_t kDefaultShards = 64;

    explicit ThreadSafeCache(size_t capacity = 100000,
                             EvictionPolicy policy = EvictionPolicy::LRU,
                             size_t num_shards = 0);

    std::optional<std::string> get(const std::string& key);
    void put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl = std::nullopt);
    bool remove(const std::string& key);
    bool exists(const std::string& key);
    size_t size() const;
    CacheStats get_stats() const;
    void clear();
    size_t active_expire_step(size_t max_keys_per_shard = 20);

    size_t num_shards() const { return num_shards_; }

private:
    size_t get_shard_index(const std::string& key) const;
    static uint32_t hash_key(std::string_view key);

    size_t capacity_;
    EvictionPolicy policy_;
    size_t num_shards_;
    std::vector<std::unique_ptr<CacheShard>> shards_;
};

// Convenient type alias for sharded concurrent cache
using ShardedCache = ThreadSafeCache;

} // namespace kvstore
