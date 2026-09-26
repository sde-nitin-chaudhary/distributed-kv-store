#include "lru_cache.hpp"

namespace kvstore {

ThreadSafeCache::ThreadSafeCache(size_t capacity, EvictionPolicy policy)
    : capacity_(capacity > 0 ? capacity : 1), policy_(policy) {}

bool ThreadSafeCache::is_expired(const CacheItem& item) const {
    if (item.expire_at == std::chrono::steady_clock::time_point::max()) {
        return false;
    }
    return std::chrono::steady_clock::now() > item.expire_at;
}

std::optional<std::string> ThreadSafeCache::get(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) {
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        if (is_expired(it->second.item)) {
            lru_list_.erase(it->second.list_it);
            lru_map_.erase(it);
            misses_.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        // Move key to front of list
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
            }
            lfu_node_map_.erase(key);
            lfu_items_.erase(it);
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

void ThreadSafeCache::put(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    auto expire_time = ttl.has_value() 
        ? std::chrono::steady_clock::now() + *ttl 
        : std::chrono::steady_clock::time_point::max();

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it != lru_map_.end()) {
            // Update existing
            it->second.item.value = value;
            it->second.item.expire_at = expire_time;
            lru_list_.erase(it->second.list_it);
            lru_list_.push_front(key);
            it->second.list_it = lru_list_.begin();
            return;
        }

        // Evict if at capacity
        if (lru_map_.size() >= capacity_) {
            evict_one_locked();
        }

        lru_list_.push_front(key);
        CacheItem item{key, value, 1, expire_time};
        lru_map_[key] = LruEntry{std::move(item), lru_list_.begin()};
    } 
    else { // LFU
        auto it = lfu_items_.find(key);
        if (it != lfu_items_.end()) {
            it->second.value = value;
            it->second.expire_at = expire_time;
            // update frequency
            uint64_t old_freq = it->second.frequency;
            uint64_t new_freq = old_freq + 1;
            it->second.frequency = new_freq;

            lfu_freq_map_[old_freq].erase(lfu_node_map_[key]);
            if (lfu_freq_map_[old_freq].empty()) {
                lfu_freq_map_.erase(old_freq);
                if (min_freq_ == old_freq) min_freq_ = new_freq;
            }
            lfu_freq_map_[new_freq].push_front(key);
            lfu_node_map_[key] = lfu_freq_map_[new_freq].begin();
            return;
        }

        if (lfu_items_.size() >= capacity_) {
            evict_one_locked();
        }

        CacheItem item{key, value, 1, expire_time};
        lfu_items_[key] = std::move(item);
        min_freq_ = 1;
        lfu_freq_map_[1].push_front(key);
        lfu_node_map_[key] = lfu_freq_map_[1].begin();
    }
}

void ThreadSafeCache::evict_one_locked() {
    evictions_.fetch_add(1, std::memory_order_relaxed);

    if (policy_ == EvictionPolicy::LRU) {
        if (lru_list_.empty()) return;
        const std::string& oldest_key = lru_list_.back();
        lru_map_.erase(oldest_key);
        lru_list_.pop_back();
    } 
    else { // LFU
        if (lfu_freq_map_.empty()) return;
        auto& min_list = lfu_freq_map_[min_freq_];
        if (min_list.empty()) return;

        std::string evict_key = min_list.back();
        min_list.pop_back();
        if (min_list.empty()) {
            lfu_freq_map_.erase(min_freq_);
        }
        lfu_node_map_.erase(evict_key);
        lfu_items_.erase(evict_key);
    }
}

bool ThreadSafeCache::remove(const std::string& key) {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);

    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) return false;
        lru_list_.erase(it->second.list_it);
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
        }
        lfu_node_map_.erase(key);
        lfu_items_.erase(it);
        return true;
    }
}

bool ThreadSafeCache::exists(const std::string& key) {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    if (policy_ == EvictionPolicy::LRU) {
        auto it = lru_map_.find(key);
        if (it == lru_map_.end()) return false;
        return !is_expired(it->second.item);
    } 
    else {
        auto it = lfu_items_.find(key);
        if (it == lfu_items_.end()) return false;
        return !is_expired(it->second);
    }
}

size_t ThreadSafeCache::size() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    return policy_ == EvictionPolicy::LRU ? lru_map_.size() : lfu_items_.size();
}

CacheStats ThreadSafeCache::get_stats() const {
    std::shared_lock<std::shared_mutex> lock(rw_mutex_);
    CacheStats stats;
    stats.total_hits = hits_.load(std::memory_order_relaxed);
    stats.total_misses = misses_.load(std::memory_order_relaxed);
    stats.total_evictions = evictions_.load(std::memory_order_relaxed);
    stats.current_size = policy_ == EvictionPolicy::LRU ? lru_map_.size() : lfu_items_.size();
    stats.capacity = capacity_;
    return stats;
}

void ThreadSafeCache::clear() {
    std::unique_lock<std::shared_mutex> lock(rw_mutex_);
    lru_map_.clear();
    lru_list_.clear();
    lfu_items_.clear();
    lfu_node_map_.clear();
    lfu_freq_map_.clear();
    min_freq_ = 0;
}

} // namespace kvstore
