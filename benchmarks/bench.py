"""Micro-benchmarks for the storage engine.

Run:  python3 benchmarks/bench.py
"""

import random
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from minikv.storage import StorageEngine  # noqa: E402

N = 50_000
VALUE = b"x" * 100


def timed(label: str, fn, ops: int) -> None:
    start = time.perf_counter()
    fn()
    elapsed = time.perf_counter() - start
    print(f"{label:<34} {ops / elapsed:>12,.0f} ops/s   ({elapsed:.2f}s)")


def main() -> None:
    tmp = tempfile.mkdtemp(prefix="minikv-bench-")
    engine = StorageEngine(tmp)
    keys = [f"key:{i:08d}".encode() for i in range(N)]

    print(f"MiniKV benchmark — {N:,} keys, 100-byte values\n")

    timed("sequential writes",
          lambda: [engine.put(k, VALUE) for k in keys], N)

    shuffled = keys[:]
    random.shuffle(shuffled)
    timed("random reads (hot keydir)",
          lambda: [engine.get(k) for k in shuffled], N)

    timed("overwrites",
          lambda: [engine.put(k, VALUE) for k in keys[: N // 2]], N // 2)

    # Read latency distribution
    sample = random.sample(keys, 1_000)
    latencies = []
    for k in sample:
        t0 = time.perf_counter()
        engine.get(k)
        latencies.append((time.perf_counter() - t0) * 1e6)
    latencies.sort()
    print(f"\nread latency  p50={statistics.median(latencies):.1f}µs"
          f"  p99={latencies[int(len(latencies) * 0.99)]:.1f}µs")

    start = time.perf_counter()
    stats = engine.compact()
    elapsed = time.perf_counter() - start
    reclaimed = stats["bytes_before"] - stats["bytes_after"]
    print(f"compaction    merged {stats['files_merged']} files, "
          f"reclaimed {reclaimed / 1024:,.0f} KiB in {elapsed:.2f}s")

    engine.close()
    start = time.perf_counter()
    engine = StorageEngine(tmp)
    print(f"cold restart  rebuilt index for {len(engine):,} keys "
          f"in {time.perf_counter() - start:.2f}s")

    engine.close()
    shutil.rmtree(tmp)


if __name__ == "__main__":
    main()
