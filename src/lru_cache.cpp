#include "lru_cache.hpp"
#include <algorithm>

namespace kvstore {

// ==============================================================================
// CacheShard Implementation (Single Mutex per Shard, Hardware-Aligned)
// ==============================================================================

CacheShard::CacheShard(size_t capacity, EvictionPolicy policy)
    : capacity_(capacity > 0 ? capacity : 1), policy_(policy) {}

bool CacheShard::is_expired(const CacheItem& item) const {
    if (item.expire_at == std::chrono::steady_clock::time_point::max()) {
        return false;
    }
    return std::chrono::steady_clock::now() > item.expire_at;
}

std::optional<std::string> CacheShard::get(const std::string& key) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) {
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        if (is_expired(it->second.item)) {
            lru_list_.erase(it->second.list_it);
            ttl_keys_.erase(key);
            lru_map_.erase(it);
            expired_keys_.fetch_add(1, std::memory_order_relaxed);
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        // Move accessed key to front of LRU list
        lru_list_.erase(it->second.list_it);
        lru_list_.push_front(key);
        it->second.list_it = lru_list_.begin();

        hits_.fetch_add(1, std::memory_order_relaxed);
        return it->second.item.value;
    }
    else { // LFU
        auto it = lfu_items_.find(key);
        if (it == lfu_items_.end()) {
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        if (is_expired(it->second)) {
            uint64_t freq = it->second.frequency;
            lfu_freq_map_[freq].erase(lfu_node_map_[key]);
            if (lfu_freq_map_[freq].empty()) {
                lfu_freq_map_.erase(freq);
                // Correctly update min_freq_ when expired item empties the min_freq bucket
                if (min_freq_ == freq) {
                    min_freq_ = lfu_freq_map_.empty() ? 0 : lfu_freq_map_.begin()->first;
                }
            }
            lfu_node_map_.erase(key);
            ttl_keys_.erase(key);
            lfu_items_.erase(it);
            expired_keys_.fetch_add(1, std::memory_order_relaxed);
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        uint64_t old_freq = it->second.frequency;
        uint64_t new_freq = old_freq + 1;
        it->second.frequency = new_freq;

        lfu_freq_map_[old_freq].erase(lfu_node_map_[key]);
        if (lfu_freq_map_[old_freq].empty()) {
            lfu_freq_map_.erase(old_freq);
            if (min_freq_ == old_freq) {
                min_freq_ = new_freq;
            }
        }

        lfu_freq_map_[new_freq].push_front(key);
        lfu_node_map_[key] = lfu_freq_map_[new_freq].begin();

        hits_.fetch_add(1, std::memory_order_relaxed);
        return it->second.value;
    }
}

void CacheShard::put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl) {
    std::unique_lock<std::mutex> lock(mutex_);

    auto expire_time = ttl.has_value()
        ? std::chrono::steady_clock::now() + *ttl
        : std::chrono::steady_clock::time_point::max();

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it != lru_map_.end()) {
            // Update existing entry
            it->second.item.value = value;
            it->second.item.expire_at = expire_time;
            lru_list_.erase(it->second.list_it);
            lru_list_.push_front(key);
            it->second.list_it = lru_list_.begin();
            if (ttl.has_value()) ttl_keys_.insert(key);
            else ttl_keys_.erase(key);
            return;
        }

        // Evict if at capacity
        if (lru_map_.size() >= capacity_) {
            evict_one_locked();
        }

        lru_list_.push_front(key);
        CacheItem item{key, value, 1, expire_time};
        lru_map_[key] = LruEntry{std::move(item), lru_list_.begin()};
        if (ttl.has_value()) ttl_keys_.insert(key);
    }
    else { // LFU
        auto it = lfu_items_.find(key);
        if (it != lfu_items_.end()) {
            it->second.value = value;
            it->second.expire_at = expire_time;
            uint64_t old_freq = it->second.frequency;
            uint64_t new_freq = old_freq + 1;
            it->second.frequency = new_freq;

            lfu_freq_map_[old_freq].erase(lfu_node_map_[key]);
            if (lfu_freq_map_[old_freq].empty()) {
                lfu_freq_map_.erase(old_freq);
                if (min_freq_ == old_freq) {
                    min_freq_ = new_freq;
                }
            }
            lfu_freq_map_[new_freq].push_front(key);
            lfu_node_map_[key] = lfu_freq_map_[new_freq].begin();
            if (ttl.has_value()) ttl_keys_.insert(key);
            else ttl_keys_.erase(key);
            return;
        }

        // Evict if at capacity
        if (lfu_items_.size() >= capacity_) {
            evict_one_locked();
        }

        CacheItem item{key, value, 1, expire_time};
        lfu_items_[key] = std::move(item);
        min_freq_ = 1;
        lfu_freq_map_[1].push_front(key);
        lfu_node_map_[key] = lfu_freq_map_[1].begin();
        if (ttl.has_value()) ttl_keys_.insert(key);
    }
}

void CacheShard::evict_one_locked() {
    evictions_.fetch_add(1, std::memory_order_relaxed);

    if (policy_ == EvictionPolicy::LRU) {
        if (lru_list_.empty()) return;
        const std::string oldest_key = lru_list_.back();
        ttl_keys_.erase(oldest_key);
        lru_map_.erase(oldest_key);
        lru_list_.pop_back();
    }
    else { // LFU
        if (lfu_freq_map_.empty()) return;

        // Defensive guard: Ensure min_freq_ points to a valid non-empty frequency bucket
        auto min_it = lfu_freq_map_.find(min_freq_);
        if (min_it == lfu_freq_map_.end() || min_it->second.empty()) {
            min_freq_ = lfu_freq_map_.begin()->first;
            min_it = lfu_freq_map_.begin();
        }

        auto& min_list = min_it->second;
        if (min_list.empty()) return;

        std::string evict_key = min_list.back();
        min_list.pop_back();
        if (min_list.empty()) {
            lfu_freq_map_.erase(min_it);
            min_freq_ = lfu_freq_map_.empty() ? 0 : lfu_freq_map_.begin()->first;
        }
        lfu_node_map_.erase(evict_key);
        ttl_keys_.erase(evict_key);
        lfu_items_.erase(evict_key);
    }
}

bool CacheShard::remove(const std::string& key) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) return false;
        lru_list_.erase(it->second.list_it);
        ttl_keys_.erase(key);
        lru_map_.erase(it);
        return true;
    }
    else {
        auto it = lfu_items_.find(key);
        if (it == lfu_items_.end()) return false;
        uint64_t freq = it->second.frequency;
        lfu_freq_map_[freq].erase(lfu_node_map_[key]);
        if (lfu_freq_map_[freq].empty()) {
            lfu_freq_map_.erase(freq);
            if (min_freq_ == freq) {
                min_freq_ = lfu_freq_map_.empty() ? 0 : lfu_freq_map_.begin()->first;
            }
        }
        lfu_node_map_.erase(key);
        ttl_keys_.erase(key);
        lfu_items_.erase(it);
        return true;
    }
}

bool CacheShard::exists(const std::string& key) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) return false;
        if (is_expired(it->second.item)) {
            lru_list_.erase(it->second.list_it);
            ttl_keys_.erase(key);
            lru_map_.erase(it);
            expired_keys_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        return true;
    } else {
        auto it = lfu_items_.find(key);
        if (it == lfu_items_.end()) return false;
        if (is_expired(it->second)) {
            uint64_t freq = it->second.frequency;
            lfu_freq_map_[freq].erase(lfu_node_map_[key]);
            if (lfu_freq_map_[freq].empty()) {
                lfu_freq_map_.erase(freq);
                if (min_freq_ == freq) {
                    min_freq_ = lfu_freq_map_.empty() ? 0 : lfu_freq_map_.begin()->first;
                }
            }
            lfu_node_map_.erase(key);
            ttl_keys_.erase(key);
            lfu_items_.erase(it);
            expired_keys_.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        return true;
    }
}

size_t CacheShard::size() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return policy_ == EvictionPolicy::LRU ? lru_map_.size() : lfu_items_.size();
}

void CacheShard::clear() {
    std::unique_lock<std::mutex> lock(mutex_);
    lru_list_.clear();
    lru_map_.clear();
    lfu_items_.clear();
    lfu_node_map_.clear();
    lfu_freq_map_.clear();
    ttl_keys_.clear();
    sample_cursor_ = 0;
    min_freq_ = 0;
}

size_t CacheShard::active_expire_step(size_t max_keys_to_sample) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (ttl_keys_.empty() || max_keys_to_sample == 0) return 0;

    size_t expired_count = 0;
    auto now = std::chrono::steady_clock::now();

    // Sample from ttl_keys_ using rotating cursor to eliminate starvation
    std::vector<std::string> sample_keys;
    sample_keys.reserve(std::min(max_keys_to_sample, ttl_keys_.size()));

    auto it = ttl_keys_.begin();
    if (sample_cursor_ < ttl_keys_.size()) {
        std::advance(it, sample_cursor_);
    } else {
        sample_cursor_ = 0;
    }

    while (sample_keys.size() < max_keys_to_sample && it != ttl_keys_.end()) {
        sample_keys.push_back(*it);
        ++it;
        sample_cursor_++;
    }

    // Wrap around if more samples needed
    if (sample_keys.size() < max_keys_to_sample && sample_cursor_ >= ttl_keys_.size()) {
        sample_cursor_ = 0;
        it = ttl_keys_.begin();
        while (sample_keys.size() < max_keys_to_sample && it != ttl_keys_.end()) {
            sample_keys.push_back(*it);
            ++it;
            sample_cursor_++;
        }
    }

    for (const auto& key : sample_keys) {
        if (policy_ == EvictionPolicy::LRU) {
            auto map_it = lru_map_.find(key);
            if (map_it != lru_map_.end() && map_it->second.item.expire_at != std::chrono::steady_clock::time_point::max()) {
                if (now > map_it->second.item.expire_at) {
                    lru_list_.erase(map_it->second.list_it);
                    lru_map_.erase(map_it);
                    ttl_keys_.erase(key);
                    expired_count++;
                    expired_keys_.fetch_add(1, std::memory_order_relaxed);
                }
            } else {
                ttl_keys_.erase(key);
            }
        } else { // LFU
            auto curr = lfu_items_.find(key);
            if (curr != lfu_items_.end() &&
                curr->second.expire_at != std::chrono::steady_clock::time_point::max() &&
                now > curr->second.expire_at) {
                uint64_t freq = curr->second.frequency;
                lfu_freq_map_[freq].erase(lfu_node_map_[key]);
                if (lfu_freq_map_[freq].empty()) {
                    lfu_freq_map_.erase(freq);
                    if (min_freq_ == freq) {
                        min_freq_ = lfu_freq_map_.empty() ? 0 : lfu_freq_map_.begin()->first;
                    }
                }
                lfu_node_map_.erase(key);
                lfu_items_.erase(curr);
                ttl_keys_.erase(key);
                expired_count++;
                expired_keys_.fetch_add(1, std::memory_order_relaxed);
            } else if (curr == lfu_items_.end()) {
                ttl_keys_.erase(key);
            }
        }
    }
    if (ttl_keys_.empty()) sample_cursor_ = 0;
    return expired_count;
}

CacheStats CacheShard::get_stats() const {
    CacheStats s;
    s.total_hits = hits_.load(std::memory_order_relaxed);
    s.total_misses = misses_.load(std::memory_order_relaxed);
    s.total_evictions = evictions_.load(std::memory_order_relaxed);
    s.total_expired_keys = expired_keys_.load(std::memory_order_relaxed);
    s.current_size = size();
    s.capacity = capacity_;
    s.num_shards = 1;
    return s;
}

// ==============================================================================
// ThreadSafeCache (ShardedCache) Implementation
// ==============================================================================

uint32_t ThreadSafeCache::hash_key(std::string_view key) {
    uint32_t hash = 2166136261u;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

ThreadSafeCache::ThreadSafeCache(size_t capacity, EvictionPolicy policy, size_t num_shards)
    : capacity_(capacity > 0 ? capacity : 1), policy_(policy) {

    if (num_shards == 0) {
        // Auto-shard: Use 1 shard if total capacity < 64 (ideal for unit-tests),
        // else 64 shards for production workloads to minimize mutex contention.
        num_shards_ = (capacity_ < kDefaultShards) ? 1 : kDefaultShards;
    } else {
        num_shards_ = std::min(num_shards, std::max<size_t>(1, capacity_));
    }

    size_t base_cap = capacity_ / num_shards_;
    size_t remainder = capacity_ % num_shards_;
    shards_.reserve(num_shards_);
    for (size_t i = 0; i < num_shards_; ++i) {
        size_t shard_cap = base_cap + (i < remainder ? 1 : 0);
        shards_.emplace_back(std::make_unique<CacheShard>(shard_cap, policy_));
    }
}

size_t ThreadSafeCache::get_shard_index(const std::string& key) const {
    if (num_shards_ == 1) return 0;
    return hash_key(key) % num_shards_;
}

std::optional<std::string> ThreadSafeCache::get(const std::string& key) {
    return shards_[get_shard_index(key)]->get(key);
}

void ThreadSafeCache::put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl) {
    shards_[get_shard_index(key)]->put(key, value, ttl);
}

bool ThreadSafeCache::remove(const std::string& key) {
    return shards_[get_shard_index(key)]->remove(key);
}

bool ThreadSafeCache::exists(const std::string& key) {
    return shards_[get_shard_index(key)]->exists(key);
}

size_t ThreadSafeCache::size() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->size();
    }
    return total;
}

void ThreadSafeCache::clear() {
    for (auto& shard : shards_) {
        shard->clear();
    }
}

size_t ThreadSafeCache::active_expire_step(size_t max_keys_per_shard) {
    size_t total_expired = 0;
    for (auto& shard : shards_) {
        total_expired += shard->active_expire_step(max_keys_per_shard);
    }
    return total_expired;
}

CacheStats ThreadSafeCache::get_stats() const {
    CacheStats agg;
    agg.num_shards = num_shards_;
    agg.capacity = capacity_;

    for (const auto& shard : shards_) {
        CacheStats s = shard->get_stats();
        agg.total_hits += s.total_hits;
        agg.total_misses += s.total_misses;
        agg.total_evictions += s.total_evictions;
        agg.total_expired_keys += s.total_expired_keys;
        agg.current_size += s.current_size;
    }
    return agg;
}

} // namespace kvstore
