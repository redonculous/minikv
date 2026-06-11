"""
An LSM-tree storage engine — the alternative backend.

This is the design used by LevelDB, RocksDB and Cassandra, simplified:

* Writes go to an in-memory **memtable** (plus a write-ahead log so
  nothing is lost on a crash).
* When the memtable grows past a threshold it is **flushed** to disk as
  an immutable, *sorted* **SSTable** file.
* Reads check the memtable first, then each SSTable newest-to-oldest.
  Every SSTable carries a **Bloom filter** so files that definitely
  don't contain the key are skipped without any disk access, plus a
  sparse index so a lookup only scans one small block.
* **Compaction** merges all SSTables (and the memtable) into a single
  fresh table, dropping overwritten values, tombstones and expired keys.

Because SSTables are sorted, this engine — unlike the Bitcask one —
keeps keys in order, which is the foundation for efficient range scans.

SSTable file layout:

    [ sorted records ... ][ sparse index ][ bloom filter ][ footer ]
                                                  footer = index_off u64,
                                                  bloom_off u64, magic 8B
"""

from __future__ import annotations

import mmap
import struct
import threading
import time
from bisect import bisect_right
from pathlib import Path
from typing import Iterator, Optional

from .bloom import BloomFilter
from .storage import CorruptRecordError, _decode_at, _encode

SST_MAGIC = b"MKVSST1\x00"
SPARSE_EVERY = 16  # index one key in every N records


# --------------------------------------------------------------- SSTable
def write_sstable(path: Path, items: list[tuple[bytes, tuple]]) -> None:
    """Write sorted ``(key, (value, ts, expiry, tombstone))`` pairs."""
    bloom = BloomFilter(len(items))
    index: list[tuple[bytes, int]] = []
    pos = 0
    with open(path, "wb") as out:
        for i, (key, (value, ts, expiry, tomb)) in enumerate(items):
            record = _encode(key, value, ts, expiry, tombstone=tomb)
            if i % SPARSE_EVERY == 0:
                index.append((key, pos))
            out.write(record)
            bloom.add(key)
            pos += len(record)
        index_off = pos
        out.write(struct.pack("<I", len(index)))
        pos += 4
        for key, off in index:
            out.write(struct.pack("<IQ", len(key), off) + key)
            pos += 12 + len(key)
        bloom_off = pos
        out.write(bloom.to_bytes())
        out.write(struct.pack("<QQ", index_off, bloom_off) + SST_MAGIC)


class SSTable:
    """A read-only, memory-mapped sorted table on disk."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        with open(self.path, "rb") as fh:
            self.mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
        if self.mm[-8:] != SST_MAGIC:
            raise CorruptRecordError(f"{self.path} is not a valid SSTable")
        index_off, bloom_off = struct.unpack_from("<QQ", self.mm,
                                                  len(self.mm) - 24)
        self.data_end = index_off

        count, = struct.unpack_from("<I", self.mm, index_off)
        pos = index_off + 4
        self.index_keys: list[bytes] = []
        self.index_offsets: list[int] = []
        for _ in range(count):
            klen, off = struct.unpack_from("<IQ", self.mm, pos)
            pos += 12
            self.index_keys.append(bytes(self.mm[pos:pos + klen]))
            self.index_offsets.append(off)
            pos += klen

        self.bloom = BloomFilter.from_bytes(self.mm[bloom_off:len(self.mm) - 24])

    def get(self, key: bytes) -> Optional[tuple]:
        """Return (value, ts, expiry, tombstone) or None if absent."""
        if key not in self.bloom:          # definite miss: zero disk reads
            return None
        i = bisect_right(self.index_keys, key) - 1
        if i < 0:
            return None
        pos = self.index_offsets[i]
        end = (self.index_offsets[i + 1]
               if i + 1 < len(self.index_offsets) else self.data_end)
        while pos < end:
            k, value, ts, expiry, tomb, size = _decode_at(self.mm, pos)
            if k == key:
                return value, ts, expiry, tomb
            if k > key:                    # records are sorted: stop early
                return None
            pos += size
        return None

    def scan(self) -> Iterator[tuple]:
        """Yield every (key, value, ts, expiry, tombstone) in key order."""
        pos = 0
        while pos < self.data_end:
            key, value, ts, expiry, tomb, size = _decode_at(self.mm, pos)
            yield key, value, ts, expiry, tomb
            pos += size

    def close(self) -> None:
        self.mm.close()


# ------------------------------------------------------------- LSM engine
class LSMEngine:
    """Drop-in alternative to ``StorageEngine`` with the same API."""

    def __init__(self, directory: str | Path,
                 memtable_bytes: int = 4 * 1024 * 1024):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.memtable_bytes = memtable_bytes
        self._lock = threading.RLock()
        self._mem: dict[bytes, tuple] = {}   # key -> (value, ts, exp, tomb)
        self._mem_size = 0
        self._wal_path = self.dir / "wal.log"

        self._sstables = [SSTable(p) for p in
                          sorted(self.dir.glob("*.sst"), reverse=True)]
        self._next_id = (
            int(self._sstables[0].path.stem.split("-")[1]) + 1
            if self._sstables else 0
        )
        self._replay_wal()
        self._wal = open(self._wal_path, "ab")

    # ----------------------------------------------------------- recovery
    def _replay_wal(self) -> None:
        if not self._wal_path.exists():
            return
        buf = self._wal_path.read_bytes()
        offset = 0
        while offset < len(buf):
            try:
                key, value, ts, expiry, tomb, size = _decode_at(buf, offset)
            except CorruptRecordError:
                with open(self._wal_path, "r+b") as fh:
                    fh.truncate(offset)
                break
            self._mem[key] = (value, ts, expiry, tomb)
            self._mem_size += size
            offset += size

    # ---------------------------------------------------------------- api
    def put(self, key: bytes, value: bytes, expiry: int = 0) -> None:
        ts = time.time_ns()
        record = _encode(key, value, ts, expiry)
        with self._lock:
            self._wal.write(record)
            self._wal.flush()
            self._mem[key] = (value, ts, expiry, False)
            self._mem_size += len(record)
            if self._mem_size >= self.memtable_bytes:
                self._flush()

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            found = self._mem.get(key)
            if found is None:
                for sst in self._sstables:        # newest first
                    found = sst.get(key)
                    if found is not None:
                        break
            if found is None:
                return None
            value, _ts, expiry, tomb = found
            if tomb or (expiry and expiry <= time.time()):
                return None
            return value

    def delete(self, key: bytes) -> bool:
        with self._lock:
            if self.get(key) is None:
                return False
            ts = time.time_ns()
            record = _encode(key, b"", ts, 0, tombstone=True)
            self._wal.write(record)
            self._wal.flush()
            self._mem[key] = (b"", ts, 0, True)
            self._mem_size += len(record)
            return True

    def expiry_of(self, key: bytes) -> Optional[int]:
        with self._lock:
            found = self._mem.get(key)
            if found is None:
                for sst in self._sstables:
                    found = sst.get(key)
                    if found is not None:
                        break
            if found is None or found[3]:
                return None
            return found[2]

    # ----------------------------------------------------- merged iteration
    def _merged(self) -> dict[bytes, tuple]:
        merged: dict[bytes, tuple] = {}
        for sst in reversed(self._sstables):      # oldest first
            for key, value, ts, expiry, tomb in sst.scan():
                merged[key] = (value, ts, expiry, tomb)
        merged.update(self._mem)                  # memtable wins
        return merged

    def keys(self) -> list[bytes]:
        now = time.time()
        with self._lock:
            return sorted(
                k for k, (_v, _ts, exp, tomb) in self._merged().items()
                if not tomb and not (exp and exp <= now)
            )

    def items(self) -> Iterator[tuple[bytes, bytes]]:
        now = time.time()
        with self._lock:
            merged = self._merged()
        for key in sorted(merged):
            value, _ts, expiry, tomb = merged[key]
            if not tomb and not (expiry and expiry <= now):
                yield key, value

    def __len__(self) -> int:
        return len(self.keys())

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    # -------------------------------------------------------------- flush
    def _flush(self) -> None:
        if not self._mem:
            return
        path = self.dir / f"sst-{self._next_id:010d}.sst"
        self._next_id += 1
        write_sstable(path, sorted(self._mem.items()))
        self._sstables.insert(0, SSTable(path))
        self._mem.clear()
        self._mem_size = 0
        self._wal.close()
        self._wal = open(self._wal_path, "wb")    # truncate
        self._wal.close()
        self._wal = open(self._wal_path, "ab")

    def flush(self) -> None:
        with self._lock:
            self._flush()

    # ---------------------------------------------------------- compaction
    def compact(self) -> dict:
        with self._lock:
            paths = list(self.dir.glob("*.sst"))
            before = sum(p.stat().st_size for p in paths)
            before += self._wal_path.stat().st_size if self._wal_path.exists() else 0

            now = time.time()
            live = sorted(
                (k, t) for k, t in self._merged().items()
                if not t[3] and not (t[2] and t[2] <= now)
            )
            path = self.dir / f"sst-{self._next_id:010d}.sst"
            self._next_id += 1
            write_sstable(path, live)

            merged_count = len(self._sstables)
            for sst in self._sstables:
                sst.close()
                sst.path.unlink()
            self._sstables = [SSTable(path)]
            self._mem.clear()
            self._mem_size = 0
            self._wal.close()
            self._wal = open(self._wal_path, "wb")
            self._wal.close()
            self._wal = open(self._wal_path, "ab")
            return {"files_merged": merged_count, "bytes_before": before,
                    "bytes_after": path.stat().st_size}

    # ------------------------------------------------------------ snapshot
    def snapshot(self, path: str | Path) -> Path:
        path = Path(path)
        with self._lock, open(path, "wb") as out:
            for key, value in self.items():
                out.write(_encode(key, value, time.time_ns(),
                                  self.expiry_of(key) or 0))
        return path

    # --------------------------------------------------------------- close
    def close(self) -> None:
        with self._lock:
            self._flush()
            self._wal.close()
            for sst in self._sstables:
                sst.close()

    def __enter__(self) -> "LSMEngine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
