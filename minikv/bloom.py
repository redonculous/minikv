"""
A Bloom filter: a tiny, probabilistic "have I seen this key?" structure.

Used by the LSM engine so that a read can skip an entire SSTable file
without touching it, when the filter says the key definitely isn't there.
False positives are possible (tunable via ``fp_rate``); false negatives
are not — if the filter says "no", the key is guaranteed absent.

Implementation: classic m-bit array with k hash probes, using the
Kirsch–Mitzenmacher double-hashing trick over a single BLAKE2b digest.
"""

from __future__ import annotations

import hashlib
import math
import struct


class BloomFilter:
    def __init__(self, expected_items: int, fp_rate: float = 0.01,
                 _bits: bytearray | None = None, _k: int | None = None):
        if _bits is not None:
            self.bits = _bits
            self.k = _k or 1
            self.m = len(_bits) * 8
            return
        expected_items = max(1, expected_items)
        m = int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2))
        self.m = max(64, (m + 7) // 8 * 8)
        self.k = max(1, round(self.m / expected_items * math.log(2)))
        self.bits = bytearray(self.m // 8)

    def _positions(self, key: bytes):
        digest = hashlib.blake2b(key, digest_size=16).digest()
        h1, h2 = struct.unpack("<QQ", digest)
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def add(self, key: bytes) -> None:
        for pos in self._positions(key):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def __contains__(self, key: bytes) -> bool:
        return all((self.bits[pos // 8] >> (pos % 8)) & 1
                   for pos in self._positions(key))

    # -------------------------------------------------------- serialization
    def to_bytes(self) -> bytes:
        return struct.pack("<II", self.k, len(self.bits)) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        k, nbytes = struct.unpack_from("<II", data)
        return cls(1, _bits=bytearray(data[8:8 + nbytes]), _k=k)
