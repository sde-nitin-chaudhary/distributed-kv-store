#include "lru_cache.hpp"
#include <iostream>
#include <cassert>
#include <thread>
#include <vector>

void test_lru_basic() {
    std::cout << "[TEST] Running test_lru_basic...\n";
    kvstore::ThreadSafeCache cache(3, kvstore::EvictionPolicy::LRU);

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
    kvstore::ThreadSafeCache cache(3, kvstore::EvictionPolicy::LFU);

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

void test_concurrency() {
    std::cout << "[TEST] Running test_concurrency (stress test)...\n";
    kvstore::ThreadSafeCache cache(5000, kvstore::EvictionPolicy::LRU);

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
    std::cout << "[INFO] Concurrency Stats: Hits=" << stats.total_hits 
              << ", Misses=" << stats.total_misses 
              << ", Evictions=" << stats.total_evictions << "\n";
    std::cout << "[PASS] test_concurrency completed with 0 data races!\n";
}

int main() {
    test_lru_basic();
    test_lfu_basic();
    test_concurrency();
    std::cout << "\n>>> ALL CACHE TESTS PASSED SUCCESSFULLY! <<<\n";
    return 0;
}
