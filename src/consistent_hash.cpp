#include "consistent_hash.hpp"
#include <algorithm>
#include <sstream>
#include <mutex>

namespace kvstore {

ConsistentHashRing::ConsistentHashRing(size_t virtual_nodes_per_node)
    : virtual_nodes_(virtual_nodes_per_node > 0 ? virtual_nodes_per_node : 100) {}

uint32_t ConsistentHashRing::fnv1a_hash(const std::string& key) {
    uint32_t hash = 2166136261u;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

void ConsistentHashRing::add_node(const std::string& node_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    // Check if node already exists
    if (std::find(physical_nodes_.begin(), physical_nodes_.end(), node_id) != physical_nodes_.end()) {
        return;
    }

    physical_nodes_.push_back(node_id);

    // Insert virtual nodes
    for (size_t i = 0; i < virtual_nodes_; ++i) {
        std::string vnode_key = node_id + "#vnode_" + std::to_string(i);
        uint32_t hash_val = fnv1a_hash(vnode_key);
        ring_[hash_val] = node_id;
    }
}

void ConsistentHashRing::remove_node(const std::string& node_id) {
    std::unique_lock<std::shared_mutex> lock(mutex_);

    auto it = std::find(physical_nodes_.begin(), physical_nodes_.end(), node_id);
    if (it == physical_nodes_.end()) return;

    physical_nodes_.erase(it);

    for (size_t i = 0; i < virtual_nodes_; ++i) {
        std::string vnode_key = node_id + "#vnode_" + std::to_string(i);
        uint32_t hash_val = fnv1a_hash(vnode_key);
        ring_.erase(hash_val);
    }
}

std::string ConsistentHashRing::get_node(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);

    if (ring_.empty()) {
        return "";
    }

    uint32_t hash_val = fnv1a_hash(key);

    // Binary search on ring for the first virtual node >= hash_val
    auto it = ring_.lower_bound(hash_val);

    // If beyond the last node, wrap around to the first node on the ring
    if (it == ring_.end()) {
        it = ring_.begin();
    }

    return it->second;
}

size_t ConsistentHashRing::total_nodes() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return physical_nodes_.size();
}

size_t ConsistentHashRing::total_virtual_nodes() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return ring_.size();
}

} // namespace kvstore
