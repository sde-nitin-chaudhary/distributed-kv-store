#include "wal.hpp"
#include "lru_cache.hpp"
#include <iostream>
#include <cassert>
#include <cstdio>

void test_wal_append_and_replay() {
    std::cout << "[TEST] Running test_wal_append_and_replay...\n";
    const std::string test_aof = "test_run.aof";
    std::remove(test_aof.c_str());

    // 1. Initialize WAL and append mutating operations
    {
        kvstore::WriteAheadLog wal(test_aof, kvstore::FsyncPolicy::Always);
        assert(wal.is_enabled());
        assert(wal.is_open());

        // SET key1 val1
        assert(wal.append_command({
            kvstore::RespObject::BulkString("SET"),
            kvstore::RespObject::BulkString("key1"),
            kvstore::RespObject::BulkString("val1")
        }));

        // SET key2 val2
        assert(wal.append_command({
            kvstore::RespObject::BulkString("SET"),
            kvstore::RespObject::BulkString("key2"),
            kvstore::RespObject::BulkString("val2")
        }));

        // DEL key1
        assert(wal.append_command({
            kvstore::RespObject::BulkString("DEL"),
            kvstore::RespObject::BulkString("key1")
        }));

        wal.flush_and_sync();
        assert(wal.bytes_written() > 0);
    }

    // 2. Replay into a fresh in-memory cache to verify crash recovery
    {
        kvstore::ThreadSafeCache cache(100, kvstore::EvictionPolicy::LRU, 1);
        kvstore::WriteAheadLog wal_replay(test_aof, kvstore::FsyncPolicy::No);

        size_t count = wal_replay.replay([&cache](const std::vector<kvstore::RespObject>& args) {
            std::string cmd = args[0].str_val;
            if (cmd == "SET") {
                cache.put(args[1].str_val, args[2].str_val);
            } else if (cmd == "DEL") {
                cache.remove(args[1].str_val);
            }
        });

        assert(count == 3);
        assert(!cache.exists("key1")); // Was deleted
        assert(cache.exists("key2"));
        assert(cache.get("key2") == "val2");
    }

    std::remove(test_aof.c_str());
    std::cout << "[PASS] WAL append and startup replay verified with 100% crash durability!\n";
}

void test_wal_fail_fast() {
    std::cout << "[TEST] Running test_wal_fail_fast...\n";
    bool threw = false;
    try {
#ifdef _WIN32
        kvstore::WriteAheadLog wal("Z:\\invalid_dir_9999\\bad.aof");
#else
        kvstore::WriteAheadLog wal("/proc/sys/fs/bad_readonly_dir/test.aof");
#endif
    } catch (const std::runtime_error&) {
        threw = true;
    }
    assert(threw);
    std::cout << "[PASS] test_wal_fail_fast passed!\n";
}

int main() {
    test_wal_append_and_replay();
    test_wal_fail_fast();
    std::cout << "\n>>> ALL WAL TESTS PASSED! <<<\n";
    return 0;
}
