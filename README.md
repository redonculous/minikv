# minikv

![python](https://img.shields.io/badge/python-3.10%2B-blue)
![tests](https://img.shields.io/badge/tests-73%20passing-brightgreen)
![license](https://img.shields.io/badge/license-mit-green)
![dependencies](https://img.shields.io/badge/dependencies-0-orange)

> a persistent key-value database built from scratch in pure python.

i wanted to learn how databases actually work, so i built one.

minikv is a redis-compatible database with two storage engines:

* **bitcask-style append-only log engine**
* **lsm-tree engine** with sstables, bloom filters and compaction

---

## features

* durable storage
* crash recovery
* corruption detection
* ttl expiry
* transactions (`multi` / `exec`)
* replication
* snapshots
* log compaction
* concurrent clients
* redis-compatible protocol
* zero dependencies

---

## quick start

```bash
git clone https://github.com/redonculous/minikv
cd minikv

python3 -m minikv.server
```

connect with:

```bash
python3 -m minikv.cli
```

or:

```bash
redis-cli -p 6479
```

---

## storage engines

| engine  | features                                               |
| ------- | ------------------------------------------------------ |
| bitcask | append-only log, keydir, crc32, mmap reads, compaction |
| lsm     | memtable, wal, sstables, sparse indexes, bloom filters |

---

## example

```text
127.0.0.1:6479> set user:1 ada
OK

127.0.0.1:6479> get user:1
"ada"
```

---

## testing

currently includes tests for:

* crash recovery
* corruption handling
* replication
* transactions
* concurrency
* protocol edge cases
* bloom filter correctness

**73 tests total**

---

## performance

```text
sequential writes   ~129k ops/s
random reads         ~39k ops/s
overwrites          ~239k ops/s
cold restart         50k keys in 0.16s
```

---

## why?

mostly because databases are cool.

i wanted to understand how systems like redis, bitcask, leveldb and rocksdb actually work instead of treating them like magic boxes.

---

## what's next

* raft / leader election
* failover
* range scans
* leveled compaction
* hint files
* fsync batching

or maybe i'll get distracted and build something else.

---

## repository

https://github.com/redonculous/minikv

## license

mit
