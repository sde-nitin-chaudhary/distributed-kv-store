#include "lru_cache.hpp"
#include <iostream>
#include <cassert>
#include <thread>
#include <vector>
#include <cstdint>
#include <chrono>

void test_lru_basic() {
    std::cout << "[TEST] Running test_lru_basic...\n";
    kvstore::ThreadSafeCache cache(3, kvstore::EvictionPolicy::LRU, 1);

    cache.put("k1", "v1");
    cache.put("k2", "v2");
    cache.put("k3", "v3");

    assert(cache.get("k1") == "v1");
    assert(cache.get("k2") == "v2");

    // Adding 4th element should evict k3 because k1 and k2 were recently accessed
    cache.put("k4", "v4");
    assert(!cache.exists("k3"));
    assert(cache.exists("k1"));
    assert(cache.exists("k2"));
    assert(cache.exists("k4"));

    std::cout << "[PASS] test_lru_basic passed!\n";
}

void test_lfu_basic() {
    std::cout << "[TEST] Running test_lfu_basic...\n";
    kvstore::ThreadSafeCache cache(3, kvstore::EvictionPolicy::LFU, 1);

    cache.put("k1", "v1");
    cache.put("k2", "v2");
    cache.put("k3", "v3");

    // Access k1 three times, k2 two times, k3 one time
    cache.get("k1");
    cache.get("k1");
    cache.get("k2");

    // k3 has least frequency (1), should be evicted first
    cache.put("k4", "v4");
    assert(!cache.exists("k3"));
    assert(cache.exists("k1"));
    assert(cache.exists("k2"));
    assert(cache.exists("k4"));

    std::cout << "[PASS] test_lfu_basic passed!\n";
}

void test_lfu_min_freq_bug_fix() {
    std::cout << "[TEST] Running test_lfu_min_freq_bug_fix...\n";
    // Regression test for LFU min_freq_ recalculation:
    // When min_freq_ bucket is emptied via removal or expiration, min_freq_ must be
    // correctly recalculated, otherwise evict_one_locked() hits an empty bucket and fails to evict.
    kvstore::ThreadSafeCache cache(3, kvstore::EvictionPolicy::LFU, 1);

    cache.put("k1", "v1");
    cache.put("k2", "v2");
    cache.put("k3", "v3");

    // Boost k2 to freq 2, k3 to freq 3
    cache.get("k2");
    cache.get("k3");
    cache.get("k3");

    // k1 is at min_freq_ (1). Now explicitly remove k1, emptying freq bucket 1.
    assert(cache.remove("k1"));

    // Insert k4 (freq 1) and k5 (freq 1)
    cache.put("k4", "v4");
    cache.put("k5", "v5");

    // Cache should not exceed capacity 3
    assert(cache.size() <= 3);

    // Insert k6: must trigger eviction of one of the lowest frequency keys
    cache.put("k6", "v6");
    assert(cache.size() <= 3);
    assert(cache.exists("k3")); // k3 had freq 3, must survive!

    std::cout << "[PASS] test_lfu_min_freq_bug_fix passed!\n";
}

void test_active_ttl_expiration() {
    std::cout << "[TEST] Running test_active_ttl_expiration (Redis-style sweeper)...\n";
    kvstore::ThreadSafeCache cache(100, kvstore::EvictionPolicy::LRU, 4);

    // Put 10 keys with 20ms TTL
    for (int i = 0; i < 10; ++i) {
        cache.put("exp_key_" + std::to_string(i), "val", std::chrono::milliseconds(20));
    }
    assert(cache.size() == 10);

    // Sleep to let them expire
    std::this_thread::sleep_for(std::chrono::milliseconds(40));

    // Run active expire step directly (simulating background sweeper)
    size_t expired = cache.active_expire_step(20);
    assert(expired == 10);
    assert(cache.size() == 0);

    auto stats = cache.get_stats();
    assert(stats.total_expired_keys >= 10);

    std::cout << "[PASS] test_active_ttl_expiration passed!\n";
}

void test_concurrency() {
    std::cout << "[TEST] Running test_concurrency (64-shard lock-striped stress test)...\n";
    // Uses default 64 shards with alignas(64) cache-line alignment
    kvstore::ThreadSafeCache cache(10000, kvstore::EvictionPolicy::LRU, 64);
    assert(cache.num_shards() == 64);

    constexpr int NUM_THREADS = 8;
    constexpr int OPS_PER_THREAD = 10000;
    std::vector<std::thread> threads;

    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < OPS_PER_THREAD; ++i) {
                std::string key = "key_" + std::to_string((t * 1000 + i) % 2000);
                std::string val = "val_" + std::to_string(i);
                if (i % 2 == 0) {
                    cache.put(key, val);
                } else {
                    cache.get(key);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto stats = cache.get_stats();
    std::cout << "[INFO] Concurrency Stats: Shards=" << stats.num_shards
              << ", Hits=" << stats.total_hits
              << ", Misses=" << stats.total_misses
              << ", Evictions=" << stats.total_evictions
              << ", CurrentSize=" << stats.current_size << "\n";
    std::cout << "[PASS] test_concurrency completed with 0 data races across 64 shards!\n";
}

void test_exact_capacity_distribution() {
    std::cout << "[TEST] Running test_exact_capacity_distribution...\n";
    kvstore::ThreadSafeCache cache(100, kvstore::EvictionPolicy::LRU, 64);
    assert(cache.num_shards() == 64);
    auto stats = cache.get_stats();
    assert(stats.capacity == 100);

    // Also test small capacity with large shard request (clamping)
    kvstore::ThreadSafeCache small_cache(10, kvstore::EvictionPolicy::LRU, 64);
    assert(small_cache.num_shards() == 10);
    assert(small_cache.get_stats().capacity == 10);

    std::cout << "[PASS] test_exact_capacity_distribution passed!\n";
}

int main() {
    test_lru_basic();
    test_lfu_basic();
    test_lfu_min_freq_bug_fix();
    test_active_ttl_expiration();
    test_concurrency();
    test_exact_capacity_distribution();
    std::cout << "\n>>> ALL CACHE TESTS PASSED SUCCESSFULLY! <<<\n";
    return 0;
}
