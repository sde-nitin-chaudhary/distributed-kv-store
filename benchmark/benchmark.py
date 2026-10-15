#!/usr/bin/env python3
"""
High-concurrency benchmark tool for Distributed KV Store.
Simulates concurrent clients sending pipelined Redis RESP commands.
Measures: Total QPS, p50, p95, and p99 latency.
"""

import socket
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
import statistics

def encode_resp_cmd(*args) -> bytes:
    payload = f"*{len(args)}\r\n"
    for a in args:
        s = str(a)
        payload += f"${len(s)}\r\n{s}\r\n"
    return payload.encode("utf-8")

def worker_bench(host: str, port: int, num_ops: int, pipeline_size: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    
    latencies = []
    
    # Pre-generate commands
    set_chunk = b"".join([encode_resp_cmd("SET", f"bench_key_{i}", f"bench_val_{i}") for i in range(pipeline_size)])
    get_chunk = b"".join([encode_resp_cmd("GET", f"bench_key_{i}") for i in range(pipeline_size)])
    
    iterations = num_ops // (pipeline_size * 2)
    
    start_time = time.perf_counter()
    for it in range(iterations):
        t0 = time.perf_counter()
        s.sendall(set_chunk)
        # Read pipeline responses: SET returns +OK\r\n (1 CRLF per command)
        resp = b""
        while resp.count(b"\r\n") < pipeline_size:
            chunk = s.recv(4096)
            if not chunk: break
            resp += chunk
        latencies.append((time.perf_counter() - t0) * 1000 / pipeline_size)
        
        t0 = time.perf_counter()
        s.sendall(get_chunk)
        # Read pipeline responses: GET returns BulkString $len\r\nval\r\n (2 CRLFs per command)
        resp = b""
        while resp.count(b"\r\n") < pipeline_size * 2:
            chunk = s.recv(4096)
            if not chunk: break
            resp += chunk
        latencies.append((time.perf_counter() - t0) * 1000 / pipeline_size)
        
    s.close()
    return latencies

def main():
    parser = argparse.ArgumentParser(description="Distributed KV Store Benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--clients", type=int, default=8, help="Number of concurrent client threads")
    parser.add_argument("--ops", type=int, default=40000, help="Total operations to execute")
    parser.add_argument("--pipeline", type=int, default=50, help="Commands pipelined per batch")
    args = parser.parse_args()

    print(f"============================================================")
    print(f" Distributed In-Memory KV Store Benchmark")
    print(f" Target: {args.host}:{args.port}")
    print(f" Concurrent Clients: {args.clients} | Total Ops: {args.ops} | Pipeline: {args.pipeline}")
    print(f"============================================================")

    ops_per_client = args.ops // args.clients

    start_wall = time.perf_counter()
    all_latencies = []

    with ThreadPoolExecutor(max_workers=args.clients) as executor:
        futures = [executor.submit(worker_bench, args.host, args.port, ops_per_client, args.pipeline) for _ in range(args.clients)]
        for f in futures:
            all_latencies.extend(f.result())

    total_time = time.perf_counter() - start_wall
    actual_ops = len(all_latencies) * args.pipeline
    qps = actual_ops / total_time

    all_latencies.sort()
    p50 = statistics.median(all_latencies)
    p95 = all_latencies[int(len(all_latencies) * 0.95)]
    p99 = all_latencies[int(len(all_latencies) * 0.99)]

    print(f"\nBenchmark Results:")
    print(f"  - Completed Ops:    {actual_ops:,}")
    print(f"  - Total Elapsed:    {total_time:.3f} seconds")
    print(f"  - Throughput (QPS): {qps:,.0f} QPS")
    print(f"  - p50 Latency:      {p50:.3f} ms")
    print(f"  - p95 Latency:      {p95:.3f} ms")
    print(f"  - p99 Latency:      {p99:.3f} ms")
    print(f"============================================================\n")

if __name__ == "__main__":
    main()
