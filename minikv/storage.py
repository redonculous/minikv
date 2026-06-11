"""
Bitcask-style log-structured storage engine.

Design
------
All writes are appended to an active data file. An in-memory hash index
(the "keydir") maps every live key to the exact file/offset of its most
recent value, so reads cost at most one disk seek.

Record layout (on disk, little-endian):

    +----------+-----------+----------+---------+---------+-----+-------+
    | crc32 4B | tstamp 8B | expiry 8B| klen 4B | vlen 4B | key | value |
    +----------+-----------+----------+---------+---------+-----+-------+

* ``crc32`` covers everything after itself; corrupt/torn tail records are
  detected and dropped during recovery.
* ``expiry`` is a unix timestamp (0 = never expires).
* A delete is written as a tombstone record (``vlen == TOMBSTONE``); the
  key disappears from the keydir and the tombstone is purged at the next
  compaction.

When the active file exceeds ``max_file_bytes`` it is sealed and a new
active file is started. ``compact()`` merges all sealed files into fresh
ones containing only the latest live value of each key.
"""

from __future__ import annotations

import mmap
import os
import struct
import threading
import time
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

HEADER = struct.Struct("<IQQII")  # crc32, timestamp, expiry, key_len, val_len
TOMBSTONE = 0xFFFFFFFF
DATA_SUFFIX = ".mkv"


class CorruptRecordError(Exception):
    """Raised when a record fails its CRC check."""


@dataclass(frozen=True)
class KeyDirEntry:
    file_id: int
    offset: int          # offset of the record header
    record_size: int     # full record size in bytes
    timestamp: int       # nanoseconds, used to break ties during recovery
    expiry: int          # unix seconds, 0 = never


def _encode(key: bytes, value: bytes, timestamp: int, expiry: int,
            tombstone: bool = False) -> bytes:
    vlen = TOMBSTONE if tombstone else len(value)
    body = HEADER.pack(0, timestamp, expiry, len(key), vlen)[4:] + key + value
    crc = zlib.crc32(body)
    return struct.pack("<I", crc) + body


def _decode_at(buf: bytes, offset: int) -> tuple[bytes, bytes, int, int, bool, int]:
    """Decode one record. Returns (key, value, timestamp, expiry, is_tombstone, size)."""
    if offset + HEADER.size > len(buf):
        raise CorruptRecordError("truncated header")
    crc, ts, expiry, klen, vlen = HEADER.unpack_from(buf, offset)
    tomb = vlen == TOMBSTONE
    real_vlen = 0 if tomb else vlen
    end = offset + HEADER.size + klen + real_vlen
    if end > len(buf):
        raise CorruptRecordError("truncated body")
    body = buf[offset + 4:end]
    if zlib.crc32(body) != crc:
        raise CorruptRecordError("crc mismatch")
    key = buf[offset + HEADER.size: offset + HEADER.size + klen]
    value = buf[offset + HEADER.size + klen: end]
    return key, value, ts, expiry, tomb, end - offset


class StorageEngine:
    """Thread-safe Bitcask-style key-value store."""

    def __init__(self, directory: str | Path, max_file_bytes: int = 32 * 1024 * 1024):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self.max_file_bytes = max_file_bytes
        self._lock = threading.RLock()
        self._keydir: dict[bytes, KeyDirEntry] = {}
        self._readers: dict[int, "os.PathLike | object"] = {}
        self._active_id = 0
        self._active = None  # type: Optional[object]
        self._recover()
        self._open_active(self._active_id)

    # ------------------------------------------------------------- paths
    def _path(self, file_id: int) -> Path:
        return self.dir / f"{file_id:010d}{DATA_SUFFIX}"

    def _data_file_ids(self) -> list[int]:
        return sorted(
            int(p.stem) for p in self.dir.glob(f"*{DATA_SUFFIX}")
        )

    # ---------------------------------------------------------- recovery
    def _recover(self) -> None:
        """Rebuild the keydir by replaying every data file oldest-first."""
        ids = self._data_file_ids()
        for fid in ids:
            buf = self._path(fid).read_bytes()
            offset = 0
            while offset < len(buf):
                try:
                    key, _val, ts, expiry, tomb, size = _decode_at(buf, offset)
                except CorruptRecordError:
                    # Torn write at the tail (e.g. crash mid-append):
                    # truncate the file to the last good record.
                    with open(self._path(fid), "r+b") as fh:
                        fh.truncate(offset)
                    break
                existing = self._keydir.get(key)
                if existing is None or ts >= existing.timestamp:
                    if tomb:
                        self._keydir.pop(key, None)
                    else:
                        self._keydir[key] = KeyDirEntry(fid, offset, size, ts, expiry)
                offset += size
        self._active_id = (ids[-1] if ids else 0)

    def _open_active(self, file_id: int) -> None:
        self._active_id = file_id
        path = self._path(file_id)
        self._active = open(path, "ab")
        self._active_size = path.stat().st_size if path.exists() else 0

    def _reader(self, file_id: int):
        """Sealed files are immutable, so map them into memory once and
        serve every read as a zero-copy slice (no seek/read syscalls)."""
        mm = self._readers.get(file_id)
        if mm is None:
            with open(self._path(file_id), "rb") as fh:
                mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
            self._readers[file_id] = mm
        return mm

    # --------------------------------------------------------------- api
    def put(self, key: bytes, value: bytes, expiry: int = 0) -> None:
        ts = time.time_ns()
        record = _encode(key, value, ts, expiry)
        with self._lock:
            self._maybe_rotate(len(record))
            offset = self._active_size
            self._active.write(record)
            self._active.flush()
            self._active_size += len(record)
            self._keydir[key] = KeyDirEntry(
                self._active_id, offset, len(record), ts, expiry
            )

    def get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            entry = self._keydir.get(key)
            if entry is None:
                return None
            if entry.expiry and entry.expiry <= time.time():
                # Lazy expiry: remove on read.
                self._delete_internal(key)
                return None
            if entry.file_id == self._active_id:
                self._active.flush()
                with open(self._path(entry.file_id), "rb") as fh:
                    fh.seek(entry.offset)
                    buf = fh.read(entry.record_size)
            else:
                mm = self._reader(entry.file_id)
                buf = mm[entry.offset:entry.offset + entry.record_size]
            _key, value, _ts, _exp, tomb, _size = _decode_at(buf, 0)
            return None if tomb else value

    def delete(self, key: bytes) -> bool:
        with self._lock:
            if key not in self._keydir:
                return False
            self._delete_internal(key)
            return True

    def _delete_internal(self, key: bytes) -> None:
        record = _encode(key, b"", time.time_ns(), 0, tombstone=True)
        self._maybe_rotate(len(record))
        self._active.write(record)
        self._active.flush()
        self._active_size += len(record)
        self._keydir.pop(key, None)

    def keys(self) -> list[bytes]:
        now = time.time()
        with self._lock:
            return [k for k, e in self._keydir.items()
                    if not (e.expiry and e.expiry <= now)]

    def expiry_of(self, key: bytes) -> Optional[int]:
        with self._lock:
            entry = self._keydir.get(key)
            return None if entry is None else entry.expiry

    def __len__(self) -> int:
        return len(self.keys())

    def __contains__(self, key: bytes) -> bool:
        return self.get(key) is not None

    # ---------------------------------------------------------- rotation
    def _maybe_rotate(self, incoming: int) -> None:
        if self._active_size + incoming > self.max_file_bytes and self._active_size > 0:
            self._active.close()
            self._open_active(self._active_id + 1)

    # -------------------------------------------------------- compaction
    def compact(self) -> dict:
        """Merge all data into fresh files containing only live records.

        Returns a small stats dict (bytes before/after, files merged).
        """
        with self._lock:
            before = sum(self._path(f).stat().st_size for f in self._data_file_ids())
            old_ids = self._data_file_ids()
            new_first = (old_ids[-1] if old_ids else 0) + 1

            # Close everything before rewriting.
            self._active.close()
            for fh in self._readers.values():
                fh.close()
            self._readers.clear()

            now = time.time()
            new_keydir: dict[bytes, KeyDirEntry] = {}
            out_id = new_first
            out = open(self._path(out_id), "ab")
            out_size = 0
            for key, entry in self._keydir.items():
                if entry.expiry and entry.expiry <= now:
                    continue
                with open(self._path(entry.file_id), "rb") as fh:
                    fh.seek(entry.offset)
                    buf = fh.read(entry.record_size)
                if out_size + len(buf) > self.max_file_bytes and out_size > 0:
                    out.close()
                    out_id += 1
                    out = open(self._path(out_id), "ab")
                    out_size = 0
                new_keydir[key] = KeyDirEntry(
                    out_id, out_size, len(buf), entry.timestamp, entry.expiry
                )
                out.write(buf)
                out_size += len(buf)
            out.flush()
            out.close()

            for fid in old_ids:
                self._path(fid).unlink()

            self._keydir = new_keydir
            self._open_active(out_id)
            after = sum(self._path(f).stat().st_size for f in self._data_file_ids())
            return {"files_merged": len(old_ids),
                    "bytes_before": before, "bytes_after": after}

    # ------------------------------------------------------------ snapshot
    def snapshot(self, path: str | Path) -> Path:
        """Write a point-in-time copy of every live key to a single file.

        The output uses the normal record format, so a snapshot can be
        restored simply by renaming it to ``0000000000.mkv`` inside a
        fresh data directory.
        """
        path = Path(path)
        with self._lock, open(path, "wb") as out:
            for key, value in self.items():
                out.write(_encode(key, value, time.time_ns(),
                                  self.expiry_of(key) or 0))
        return path

    # ------------------------------------------------------------- close
    def close(self) -> None:
        with self._lock:
            if self._active:
                self._active.flush()
                self._active.close()
                self._active = None
            for fh in self._readers.values():
                fh.close()
            self._readers.clear()

    def __enter__(self) -> "StorageEngine":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def items(self) -> Iterator[tuple[bytes, bytes]]:
        for key in self.keys():
            value = self.get(key)
            if value is not None:
                yield key, value
