# MiniKV

**A persistent key-value database built from scratch in pure Python — no dependencies, just the standard library.**

MiniKV is a log-structured storage engine (in the style of [Bitcask](https://riak.com/assets/bitcask-intro.pdf), the engine behind Riak) wrapped in a Redis-compatible TCP server. It exists to answer one question properly: *how does a database actually store, recover, and serve your data?*

```
$ python3 -m minikv.server &
MiniKV listening on 127.0.0.1:6479

$ python3 -m minikv.cli
127.0.0.1:6479> SET user:1 ada EX 3600
OK
127.0.0.1:6479> GET user:1
"ada"
127.0.0.1:6479> TTL user:1
(integer) 3599
```

Because it speaks real [RESP](https://redis.io/docs/reference/protocol-spec/), `redis-cli -p 6479` works against it out of the box.

## Try it in 60 seconds

No install, no dependencies — just Python 3.10+:

```bash
git clone https://github.com/<you>/minikv && cd minikv
python3 demo.py
```

The demo saves data over a real TCP connection, then **simulates a power-cut crash** by writing half-finished garbage into the data file — and shows MiniKV restart, detect the corruption via checksums, and recover every key intact:

```
[3] 💥 Simulating a crash: killing the server and writing
    half-finished garbage to the end of the data file

[4] Restarting... MiniKV replays its log, checks every
    record's checksum, and throws away the corrupt tail.

    GET user:1   -> b'Ada Lovelace'
    GET user:2   -> b'Alan Turing'
    DBSIZE       -> 4 keys recovered

✅ All data survived the crash.
```

## Features

- **Durable writes** — every write is appended to a write-ahead log and flushed before the call returns
- **Crash recovery** — on startup the engine replays its log files, drops torn/partial records at the tail (verified by CRC32 checksums), and rebuilds the in-memory index
- **Corruption detection** — every record is checksummed; bit-flips are detected and the damaged record is rejected rather than served as garbage
- **Log compaction** — stale versions and tombstones are merged away on demand, reclaiming disk space
- **Key expiry (TTL)** — lazy expiry on read, plus purging at compaction time
- **Redis-compatible wire protocol** — `GET`, `SET` (with `EX/PX/NX/XX`), `DEL`, `EXPIRE`, `TTL`, `INCR`, `APPEND`, `KEYS` globbing, and more
- **Transactions** — `MULTI`/`EXEC`/`DISCARD`: queue commands and run the batch atomically
- **Replication** — start a second server with `--replicaof`; it pulls a full copy of the dataset, then receives every write live
- **Snapshots** — `SAVE` writes a point-in-time copy of all live keys to a single restorable file
- **Two interchangeable storage engines** — the default Bitcask-style log engine, or an LSM-tree (`--engine lsm`) with sorted SSTables, sparse indexes, and **bloom filters** so reads skip files that can't contain the key
- **mmap reads** — sealed data files are memory-mapped; reads are zero-copy slices, no seek/read syscalls
- **Concurrent** — asyncio server handles many simultaneous clients; the engine itself is thread-safe
- **Zero dependencies** — runs anywhere Python 3.10+ runs

## Architecture

```
                 ┌─────────────────────────────────────────┐
   TCP clients   │              minikv.server              │
  (redis-cli,    │   asyncio loop · RESP parser · command  │
   minikv.cli) ──┤   dispatch (GET/SET/EXPIRE/INCR/...)    │
                 └──────────────────┬──────────────────────┘
                                    │
                 ┌──────────────────▼──────────────────────┐
                 │           minikv.storage                │
                 │                                         │
                 │   keydir (in-memory hash index)         │
                 │   key → (file_id, offset, size, expiry) │
                 │                                         │
                 │   ┌───────────┐ ┌───────────┐ ┌───────┐ │
                 │   │ 000…0.mkv │ │ 000…1.mkv │ │active │ │
                 │   │  (sealed) │ │  (sealed) │ │ file  │ │
                 │   └───────────┘ └───────────┘ └───────┘ │
                 │        append-only data files           │
                 └─────────────────────────────────────────┘
```

**Writes** append a record to the active file and update the keydir — O(1), one sequential disk write.

**Reads** look the key up in the keydir and read the record at that exact offset — O(1), at most one disk seek.

**Record format** (little-endian):

```
┌──────────┬─────────────┬───────────┬─────────┬─────────┬─────┬───────┐
│ crc32 4B │ timestamp 8B│ expiry 8B │ klen 4B │ vlen 4B │ key │ value │
└──────────┴─────────────┴───────────┴─────────┴─────────┴─────┴───────┘
```

A delete writes a *tombstone* (a record with a sentinel value length). Tombstones must persist until compaction — otherwise a deleted key could "resurrect" from an older file after a restart.

## Design decisions & trade-offs

| Decision | Why | Trade-off |
|---|---|---|
| Append-only log | Sequential writes are fast and crash-safe; no in-place updates means no partially-overwritten records | Disk usage grows until compaction |
| In-memory hash index | One seek per read, O(1) lookups | All keys must fit in RAM; no efficient range scans (an LSM-tree or B-tree would fix this) |
| CRC32 per record | Detects torn writes and bit rot at recovery time | ~4 bytes + a checksum pass per record |
| Lazy TTL expiry | No background timer thread to coordinate | Expired keys occupy memory until read or compacted |
| `flush()` per write, not `fsync()` | Survives process crashes with good throughput | An OS-level crash can lose the last few writes; `fsync` would trade ~100× throughput for that guarantee — the classic durability dial |

## Benchmarks

50,000 keys, 100-byte values, single thread (measured on a modest container — run `python3 benchmarks/bench.py` yourself):

```
sequential writes      ~129,000 ops/s
random reads            ~39,000 ops/s   (p50 21µs, p99 65µs)
overwrites             ~239,000 ops/s
cold restart            50,000 keys re-indexed in 0.16s
```

## Getting started

```bash
git clone https://github.com/<you>/minikv && cd minikv

# start the server (add --engine lsm for the LSM-tree backend)
python3 -m minikv.server --dir ./data --port 6479

# talk to it
python3 -m minikv.cli

# optional: start a live read replica in another terminal
python3 -m minikv.server --dir ./replica --port 6480 --replicaof 127.0.0.1 6479
```

Or embed the engine directly — it's just a library:

```python
from minikv import StorageEngine

with StorageEngine("./data") as db:
    db.put(b"answer", b"42")
    print(db.get(b"answer"))   # b'42'
```

## Testing

73 tests cover the full stack, including the failure modes that matter for a database:

```bash
python3 -m unittest discover tests -v
```

- **Crash simulation** — garbage is appended to a log file to simulate a torn write mid-crash; recovery must truncate it without losing earlier data
- **Corruption injection** — a bit is flipped in the middle of a stored record; the CRC must catch it
- **Restart cycles** — data, deletes, and TTLs must all survive a process restart
- **Concurrency** — 8 threads hammering the engine, 5 parallel TCP clients hammering the server
- **Protocol fuzzing-lite** — RESP requests delivered one byte at a time, pipelined requests, binary-unsafe payloads
- **Replication, end-to-end** — a real leader and replica on separate ports; full sync, live write streaming, and replicated deletes are all verified over TCP
- **Transactions** — atomic batches, DISCARD, errors mid-batch
- **Bloom filter maths** — zero false negatives over 1,000 members; false-positive rate within bounds over 10,000 probes

## Project layout

```
minikv/
├── minikv/
│   ├── storage.py    # Bitcask engine: log, keydir, mmap reads, recovery, compaction, snapshots
│   ├── lsm.py        # LSM-tree engine: memtable, WAL, sorted SSTables, sparse index
│   ├── bloom.py      # bloom filter used by the LSM engine's SSTables
│   ├── resp.py       # incremental RESP protocol parser/encoder
│   ├── server.py     # asyncio TCP server, transactions, replication
│   ├── client.py     # synchronous client library
│   └── cli.py        # interactive REPL
├── tests/            # 48 tests: unit, integration, crash/corruption
├── benchmarks/       # reproducible micro-benchmarks
└── .github/workflows # CI: tests on 3 Python versions
```

## What I'd build next

- **Leader election / failover** — replicas exist; promoting one automatically when the leader dies is the next step (hello, Raft)
- **Range queries over the wire** — the LSM engine already keeps keys sorted; expose `SCAN start end` as a command
- **Leveled compaction** — the LSM engine currently merges everything at once; real LSM trees compact in tiers
- **Hint files** — snapshot the keydir at compaction so cold starts don't re-scan every record
- **`fsync` batching / group commit** — full durability without paying per-write

## License

MIT
