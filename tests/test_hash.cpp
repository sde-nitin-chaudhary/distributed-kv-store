#include "consistent_hash.hpp"
#include <iostream>
#include <cassert>
#include <map>

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

int main() {
    test_consistent_hash_distribution();
    test_minimal_rebalancing();
    std::cout << "\n>>> ALL CONSISTENT HASHING TESTS PASSED! <<<\n";
    return 0;
}
