# MiniKV

**a persistent key-value database built from scratch in pure python — no dependencies, just the standard library.**

MiniKV is a log-structured storage engine (in the style of Bitcask, the engine behind Riak) wrapped in a Redis-compatible TCP server. it exists to answer one question properly: *how does a database actually store, recover, and serve your data?*



```bash
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

because it speaks real RESP, `redis-cli -p 6479` works against it out of the box.

## try it in 60 seconds

no install, no dependencies — just python 3.10+:

```bash
git clone https://github.com/redonculous/minikv && cd minikv
python3 demo.py
```

the demo saves data over a real TCP connection, then **simulates a power-cut crash** by writing half-finished garbage into the data file — and shows MiniKV restart, detect the corruption via checksums, and recover every key intact:

```text
[3] 💥 Simulating a crash: killing the server and writing
    half-finished garbage to the end of the data file

[4] Restarting... MiniKV replays its log, checks every
    record's checksum, and throws away the corrupt tail.

    GET user:1   -> b'Ada Lovelace'
    GET user:2   -> b'Alan Turing'
    DBSIZE       -> 4 keys recovered

✅ All data survived the crash.
```

## features

* **durable writes** — every write is appended to a write-ahead log and flushed before the call returns
* **crash recovery** — on startup the engine replays its log files, drops torn/partial records at the tail (verified by CRC32 checksums), and rebuilds the in-memory index
* **corruption detection** — every record is checksummed; bit-flips are detected and the damaged record is rejected rather than served as garbage
* **log compaction** — stale versions and tombstones are merged away on demand, reclaiming disk space
* **key expiry (TTL)** — lazy expiry on read, plus purging at compaction time
* **redis-compatible wire protocol** — `GET`, `SET` (with `EX/PX/NX/XX`), `DEL`, `EXPIRE`, `TTL`, `INCR`, `APPEND`, `KEYS` globbing, and more
* **concurrent** — asyncio server handles many simultaneous clients; the engine itself is thread-safe
* **zero dependencies** — runs anywhere python 3.10+ runs

## architecture

```text
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

**writes** append a record to the active file and update the keydir — O(1), one sequential disk write.

**reads** look the key up in the keydir and read the record at that exact offset — O(1), at most one disk seek.

**record format** (little-endian):

```text
┌──────────┬─────────────┬───────────┬─────────┬─────────┬─────┬───────┐
│ crc32 4B │ timestamp 8B│ expiry 8B │ klen 4B │ vlen 4B │ key │ value │
└──────────┴─────────────┴───────────┴─────────┴─────────┴─────┴───────┘
```

a delete writes a *tombstone* (a record with a sentinel value length). tombstones must persist until compaction — otherwise a deleted key could "resurrect" from an older file after a restart.

## design decisions & trade-offs

| decision                           | why                                                                                                   | trade-off                                                                                                                             |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| append-only log                    | sequential writes are fast and crash-safe; no in-place updates means no partially-overwritten records | disk usage grows until compaction                                                                                                     |
| in-memory hash index               | one seek per read, O(1) lookups                                                                       | all keys must fit in RAM; no efficient range scans (an LSM-tree or B-tree would fix this)                                             |
| CRC32 per record                   | detects torn writes and bit rot at recovery time                                                      | ~4 bytes + a checksum pass per record                                                                                                 |
| lazy TTL expiry                    | no background timer thread to coordinate                                                              | expired keys occupy memory until read or compacted                                                                                    |
| `flush()` per write, not `fsync()` | survives process crashes with good throughput                                                         | an OS-level crash can lose the last few writes; `fsync` would trade ~100× throughput for that guarantee — the classic durability dial |

## benchmarks

50,000 keys, 100-byte values, single thread (measured on a modest container — run `python3 benchmarks/bench.py` yourself):

```text
sequential writes      ~129,000 ops/s
random reads            ~39,000 ops/s   (p50 21µs, p99 65µs)
overwrites             ~239,000 ops/s
cold restart            50,000 keys re-indexed in 0.16s
```

## getting started

```bash
git clone https://github.com/redonculous/minikv && cd minikv

# start the server
python3 -m minikv.server --dir ./data --port 6479

# talk to it
python3 -m minikv.cli
```

or embed the engine directly — it's just a library:

```python
from minikv import StorageEngine

with StorageEngine("./data") as db:
    db.put(b"answer", b"42")
    print(db.get(b"answer"))   # b'42'
```

## testing

48 tests cover the full stack, including the failure modes that matter for a database:

```bash
python3 -m unittest discover tests -v
```

* **crash simulation** — garbage is appended to a log file to simulate a torn write mid-crash; recovery must truncate it without losing earlier data
* **corruption injection** — a bit is flipped in the middle of a stored record; the CRC must catch it
* **restart cycles** — data, deletes, and TTLs must all survive a process restart
* **concurrency** — 8 threads hammering the engine, 5 parallel TCP clients hammering the server
* **protocol fuzzing-lite** — RESP requests delivered one byte at a time, pipelined requests, binary-unsafe payloads

## project layout

```text
minikv/
├── minikv/
│   ├── storage.py    # the engine: log, keydir, recovery, compaction
│   ├── resp.py       # incremental RESP protocol parser/encoder
│   ├── server.py     # asyncio TCP server + command handlers
│   ├── client.py     # synchronous client library
│   └── cli.py        # interactive REPL
├── tests/            # 48 tests: unit, integration, crash/corruption
├── benchmarks/       # reproducible micro-benchmarks
└── .github/workflows # CI: tests on 3 python versions
```

## license

MIT
