#pragma once

#include <string>
#include <map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <cstdint>
#include <unordered_set>

namespace kvstore {

// ==============================================================================
// ConsistentHashRing: High-Availability Distributed Sharding Ring
//
// Invariants:
//   - Disperses each physical node into V virtual nodes (default 150)
//     across a 32-bit FNV-1a hash ring to eliminate hot spots.
//   - Bounds key rebalancing to ~1/N during node membership changes.
//   - Provides clockwise replica collection for replication_factor >= 2.
// ==============================================================================
class ConsistentHashRing {
public:
    static constexpr uint32_t HASH_SLOTS = 16384; // Redis Cluster hash slots

    explicit ConsistentHashRing(size_t virtual_nodes_per_node = 150);

    void add_node(const std::string& node_id);
    void remove_node(const std::string& node_id);
    std::string get_node(const std::string& key) const;

    // Collects up to `count` distinct physical nodes clockwise on the ring starting from key's hash
    std::vector<std::string> get_replica_nodes(const std::string& key, size_t count) const;

    uint32_t get_key_slot(const std::string& key) const;
    std::vector<std::string> get_all_nodes() const;
    bool has_node(const std::string& node_id) const;

    size_t total_nodes() const;
    size_t total_virtual_nodes() const;

    static uint32_t fnv1a_hash(const std::string& key);
    static uint16_t crc16(const char* buf, int len);
    static std::string extract_hash_tag(const std::string& key);

private:
    size_t virtual_nodes_;
    mutable std::shared_mutex mutex_;
    
    // Ring mapping: hash_value -> physical_node_id
    std::map<uint32_t, std::string> ring_;
    std::vector<std::string> physical_nodes_;
};

} // namespace kvstore
