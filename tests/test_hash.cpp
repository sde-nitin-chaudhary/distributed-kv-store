#include "consistent_hash.hpp"
#include <iostream>
#include <cassert>
#include <map>
#include <cstdint>
#include <unordered_set>

void test_consistent_hash_distribution() {
    std::cout << "[TEST] Running test_consistent_hash_distribution...\n";
    kvstore::ConsistentHashRing ring(150);

    ring.add_node("node-us-east-1");
    ring.add_node("node-us-west-1");
    ring.add_node("node-eu-central-1");

    assert(ring.total_nodes() == 3);
    assert(ring.total_virtual_nodes() == 450);

    // Distribution of 10,000 keys
    std::map<std::string, int> distribution;
    for (int i = 0; i < 10000; ++i) {
        std::string key = "session_token_" + std::to_string(i);
        std::string assigned_node = ring.get_node(key);
        distribution[assigned_node]++;
    }

    std::cout << "[INFO] Key Distribution across 3 nodes (10,000 keys):\n";
    for (const auto& [node, count] : distribution) {
        double pct = (count * 100.0) / 10000.0;
        std::cout << "  - " << node << ": " << count << " keys (" << pct << "%)\n";
        // Each node should hold roughly 25% - 42% (near the ideal 33.3%)
        assert(pct >= 25.0 && pct <= 42.0);
    }

    std::cout << "[PASS] Consistent hash distribution test passed within uniform bounds!\n";
}

void test_minimal_rebalancing() {
    std::cout << "[TEST] Running test_minimal_rebalancing on node addition...\n";
    kvstore::ConsistentHashRing ring(150);

    ring.add_node("node-1");
    ring.add_node("node-2");
    ring.add_node("node-3");

    std::map<std::string, std::string> initial_placement;
    for (int i = 0; i < 10000; ++i) {
        std::string key = "order_id_" + std::to_string(i);
        initial_placement[key] = ring.get_node(key);
    }

    // Add 4th node
    ring.add_node("node-4");

    int rebalanced_keys = 0;
    for (int i = 0; i < 10000; ++i) {
        std::string key = "order_id_" + std::to_string(i);
        std::string new_node = ring.get_node(key);
        if (new_node != initial_placement[key]) {
            rebalanced_keys++;
        }
    }

    double rebalance_pct = (rebalanced_keys * 100.0) / 10000.0;
    std::cout << "[INFO] When 4th node added, only " << rebalanced_keys << " / 10000 keys (" 
              << rebalance_pct << "%) rebalanced! (Theoretical ideal ~25%)\n";

    // With 4 nodes, ideal rebalancing is ~25%. Must be < 32% (far better than naive mod-N which rebalances ~75%)
    assert(rebalance_pct <= 32.0);
    std::cout << "[PASS] Minimal key rebalancing verified!\n";
}

void test_replica_nodes() {
    std::cout << "[TEST] Running test_replica_nodes (clockwise multi-node replication)...\n";
    kvstore::ConsistentHashRing ring(150);

    ring.add_node("node-1");
    ring.add_node("node-2");
    ring.add_node("node-3");

    // For any key with replication_factor 2, we must get 2 distinct physical nodes
    for (int i = 0; i < 100; ++i) {
        std::string key = "user_account_" + std::to_string(i);
        auto replicas = ring.get_replica_nodes(key, 2);
        assert(replicas.size() == 2);
        assert(replicas[0] != replicas[1]);
        assert(ring.has_node(replicas[0]));
        assert(ring.has_node(replicas[1]));
        assert(replicas[0] == ring.get_node(key)); // Primary must match get_node()
    }

    // Asking for 3 replicas across 3 nodes gives all 3 distinct nodes
    auto all_replicas = ring.get_replica_nodes("any_key", 3);
    assert(all_replicas.size() == 3);
    std::unordered_set<std::string> unique_nodes(all_replicas.begin(), all_replicas.end());
    assert(unique_nodes.size() == 3);

    std::cout << "[PASS] Multi-node replica collection verified!\n";
}

void test_key_slot() {
    std::cout << "[TEST] Running test_key_slot (Redis Cluster 16384 slots)...\n";
    kvstore::ConsistentHashRing ring(150);

    uint32_t slot = ring.get_key_slot("mykey");
    assert(slot < 16384);
    assert(ring.get_key_slot("mykey") == slot); // Deterministic

    // Redis hash tag verification
    uint32_t tagged_slot1 = ring.get_key_slot("{user100}:profile");
    uint32_t tagged_slot2 = ring.get_key_slot("{user100}:orders");
    assert(tagged_slot1 == tagged_slot2);
    assert(tagged_slot1 == ring.get_key_slot("user100"));

    std::cout << "[PASS] Hash slot calculation verified!\n";
}

int main() {
    test_consistent_hash_distribution();
    test_minimal_rebalancing();
    test_replica_nodes();
    test_key_slot();
    std::cout << "\n>>> ALL CONSISTENT HASHING TESTS PASSED! <<<\n";
    return 0;
}
