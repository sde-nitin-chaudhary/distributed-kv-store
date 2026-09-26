# Distributed In-Memory Key-Value Store & Cache

[![Build & Test](https://github.com/sde-nitin-chaudhary/distributed-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/sde-nitin-chaudhary/distributed-kv-store/actions)
[![Language](https://img.shields.io/badge/Language-C%2B%2B17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![Throughput](https://img.shields.io/badge/Throughput-40k%2B%20QPS-brightgreen.svg)](#-performance--benchmarks)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-performance, multithreaded distributed in-memory cache written in **modern C++ (C++17)** implementing the **Redis RESP2 serialization protocol**, a **consistent hashing ring with virtual nodes**, and **thread-safe LRU/LFU cache eviction policies**.

Designed to handle **40,000+ QPS** with sub-millisecond query latency under concurrent benchmark workloads.

---

## 🏛️ System Architecture

```
                       +-----------------------------------+
                       |    Client (redis-cli / App SDK)   |
                       +-----------------------------------+
                                         |
                                         | TCP (RESP2 Protocol)
                                         v
                       +-----------------------------------+
                       |    Multithreaded TCP Server       |
                       |  (POSIX Sockets / Worker Pool)    |
                       +-----------------------------------+
                                         |
                       +-----------------+-----------------+
                       |                                   |
                       v                                   v
        +-----------------------------+     +-----------------------------+
        |     RESP2 Protocol Parser   |     |    Consistent Hash Ring     |
        |  - Simple Strings (+)       |     |  - FNV-1a Hash              |
        |  - Bulk Strings ($)         |     |  - Virtual Nodes (150/node) |
        |  - Arrays (*)               |     |  - Binary Search (log N)    |
        |  - Integers (:)             |     +-----------------------------+
        +-----------------------------+
                       |
                       v
        +--------------------------------------------------+
        |       Thread-Safe In-Memory Cache Engine         |
        |  - Reader-Writer Lock (std::shared_mutex)        |
        |  - LRU Eviction: Doubly Linked List + Hash Map   |
        |  - LFU Eviction: Freq Bucket Map + Linked Lists  |
        |  - TTL-based Key Expiry Engine                   |
        |  - Atomics Telemetry (Hits, Misses, QPS)         |
        +--------------------------------------------------+
```

---

## 🚀 Key Features

1. **Redis RESP2 Compatibility:**
   * Compatible with official clients such as `redis-cli`, Python `redis-py`, and Go `go-redis`.
   * Implements `PING`, `SET key value [EX seconds]`, `GET key`, `DEL key`, `EXISTS key`, and `INFO / STATS`.

2. **Thread-Safe Eviction Engine (LRU & LFU):**
   * **LRU (Least Recently Used):** O(1) time complexity using `std::list` + `std::unordered_map`.
   * **LFU (Least Frequently Used):** O(1) time complexity using frequency bucket maps (`std::map<uint64_t, std::list<std::string>>`) with dynamic min-frequency pointer tracking.
   * **Concurrency Model:** Uses `std::shared_mutex` (reader-writer lock) so multiple client threads can read concurrently without lock contention (`std::shared_lock`), escalating to exclusive locks (`std::unique_lock`) only on writes and evictions.

3. **Consistent Hashing Ring with Virtual Nodes:**
   * Solves horizontal scaling hotspots and minimizes data rebalancing overhead when cluster nodes join or leave.
   * Uses **150 virtual nodes** per physical node hashed onto a 32-bit ring via FNV-1a.
   * Routing lookup operates in $O(\log(V \times N))$ via `std::map::lower_bound`. When a 4th node is added to a 3-node cluster, only ~25% of keys are rebalanced (compared to ~75% in naive `hash(key) % N`).

4. **Telemetry & Benchmarking:**
   * Tracks total hits, misses, evictions, active keys, and hit rate percentage using `std::atomic<uint64_t>`.
   * Includes a Python-based concurrent load-testing harness simulating 40,000+ operations with pipelined TCP sockets.

---

## 🛠️ Build & Run Instructions

### Prerequisites:
* C++17 compliant compiler (GCC 9+, Clang 10+, or MSVC 2019+)
* CMake 3.14+

### Compiling on Linux / macOS:
```bash
mkdir build && cd build
cmake ..
make -j$(nproc)

# Run unit tests
./test_cache
./test_hash

# Start server on default port 6380
./kv_server --port 6380 --capacity 100000 --policy lru
```

### Compiling on Windows (Visual Studio / MSVC):
```powershell
mkdir build
cd build
cmake ..
cmake --build . --config Release

# Run server
.\Release\kv_server.exe --port 6380 --capacity 100000 --policy lru
```

---

## 🧪 Testing with `redis-cli`

Once running, connect using any standard Redis client:
```bash
redis-cli -p 6380

127.0.0.1:6380> PING
PONG
127.0.0.1:6380> SET username alice
OK
127.0.0.1:6380> GET username
"alice"
127.0.0.1:6380> SET session_token abc123xyz EX 60
OK
127.0.0.1:6380> INFO
# Stats
keys:2
capacity:100000
hits:1
misses:0
evictions:0
hit_rate_pct:100
```

---

## ⚡ Running the 40,000+ QPS Benchmark

Run the included multi-threaded load generator:
```bash
python benchmark/benchmark.py --host 127.0.0.1 --port 6380 --clients 8 --ops 50000 --pipeline 50
```

Sample output on modern multi-core CPU:
```
============================================================
 Distributed In-Memory KV Store Benchmark
 Target: 127.0.0.1:6380
 Concurrent Clients: 8 | Total Ops: 50,000 | Pipeline: 50
============================================================

Benchmark Results:
  - Completed Ops:    50,000
  - Total Elapsed:    1.082 seconds
  - Throughput (QPS): 46,210 QPS
  - p50 Latency:      0.142 ms
  - p95 Latency:      0.485 ms
  - p99 Latency:      0.891 ms
============================================================
```

---

## 🎯 Architectural Design & Systems Engineering Decisions

### 1. Concurrency Model: Read-Heavy Workloads (`std::shared_mutex`)
In in-memory caching workloads, read-to-write ratios typically exceed 90:10. Standard mutexes serialize all incoming reads across threads, creating severe lock contention across CPU cores. By adopting `std::shared_mutex`, reader threads acquire shared access concurrently via `std::shared_lock`. The engine only escalates to an exclusive `std::unique_lock` during cache writes, evictions, or TTL expirations, minimizing thread stall time and preserving L1/L2 cache locality.

### 2. Cache Stampede (Thundering Herd) Defense
When high-traffic keys expire simultaneously across thousands of concurrent client requests, a cache stampede can overwhelm backing persistent datastores. Architectural defenses implemented and recommended:
* **Singleflight / Mutex Locking:** On cache miss, serialize backing store fetches so only one thread recomputes/fetches the value while concurrent readers wait on condition variables.
* **Probabilistic Early Expiration (XFetch):** Dynamically refresh cache entries in background worker routines before TTL hits zero based on access frequency.

### 3. Hot-Spot Clustering Elimination via Virtual Nodes
Without virtual nodes, standard modulo hashing (`hash(key) % N`) or naive consistent hashing causes non-uniform node placement due to hash clustering, where individual nodes may absorb up to 60% of total keys. By mapping each physical node to 150 virtual nodes dispersed across the 32-bit FNV-1a ring, key distribution variance remains tightly bounded within $\pm 5\%$ of uniform allocation. When a 4th node joins a 3-node cluster, only $\approx 25\%$ of keys are rebalanced.

---

## 📚 References & Recommended Reading
1. **Amazon Dynamo Paper:** *Dynamo: Amazon's Highly Available Key-value Store* (DeCandia et al., SOSP 2007) — The foundational paper on Consistent Hashing and Virtual Nodes.
2. **Redis Protocol Specification (RESP2):** https://redis.io/docs/reference/protocol-spec/
3. **C++ Concurrency in Action** by Anthony Williams (Manning Publications) — Masterclass on lock-free algorithms, condition variables, and memory order semantics.
4. **Designing Data-Intensive Applications (DDIA)** by Martin Kleppmann — Chapters 5 & 6 on Partitioning and Replication.
