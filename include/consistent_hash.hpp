#pragma once

#include <string>
#include <map>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <cstdint>

namespace kvstore {

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(size_t virtual_nodes_per_node = 150);

    void add_node(const std::string& node_id);
    void remove_node(const std::string& node_id);
    std::string get_node(const std::string& key) const;
    size_t total_nodes() const;
    size_t total_virtual_nodes() const;

private:
    static uint32_t fnv1a_hash(const std::string& key);

    size_t virtual_nodes_;
    mutable std::shared_mutex mutex_;
    
    // Ring mapping: hash_value -> physical_node_id
    std::map<uint32_t, std::string> ring_;
    std::vector<std::string> physical_nodes_;
};

} // namespace kvstore
