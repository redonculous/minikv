import os
import tempfile
import time
import unittest
from pathlib import Path

from minikv.bloom import BloomFilter
from minikv.lsm import LSMEngine, SSTable, write_sstable


class TestBloomFilter(unittest.TestCase):
    def test_no_false_negatives(self):
        bf = BloomFilter(expected_items=1000)
        keys = [f"key-{i}".encode() for i in range(1000)]
        for k in keys:
            bf.add(k)
        for k in keys:
            self.assertIn(k, bf)  # a bloom filter must NEVER miss a member

    def test_false_positive_rate_reasonable(self):
        bf = BloomFilter(expected_items=1000, fp_rate=0.01)
        for i in range(1000):
            bf.add(f"member-{i}".encode())
        false_hits = sum(
            f"absent-{i}".encode() in bf for i in range(10_000)
        )
        self.assertLess(false_hits / 10_000, 0.05)  # generous bound

    def test_serialization_roundtrip(self):
        bf = BloomFilter(expected_items=100)
        bf.add(b"hello")
        restored = BloomFilter.from_bytes(bf.to_bytes())
        self.assertIn(b"hello", restored)
        self.assertNotIn(b"definitely-not-added-xyz", restored)


class TestSSTable(unittest.TestCase):
    def test_write_read_scan(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "t.sst"
            items = sorted(
                (f"k{i:04d}".encode(),
                 (f"v{i}".encode(), i, 0, False)) for i in range(500)
            )
            write_sstable(path, items)
            sst = SSTable(path)
            # point lookups
            self.assertEqual(sst.get(b"k0000")[0], b"v0")
            self.assertEqual(sst.get(b"k0499")[0], b"v499")
            self.assertIsNone(sst.get(b"missing"))
            # full ordered scan
            scanned = [k for k, *_ in sst.scan()]
            self.assertEqual(scanned, sorted(scanned))
            self.assertEqual(len(scanned), 500)
            sst.close()


class LSMTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = LSMEngine(self.tmp.name, memtable_bytes=8 * 1024)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def reopen(self):
        self.engine.close()
        self.engine = LSMEngine(self.tmp.name, memtable_bytes=8 * 1024)


class TestLSMEngine(LSMTestCase):
    def test_put_get_delete(self):
        self.engine.put(b"k", b"v")
        self.assertEqual(self.engine.get(b"k"), b"v")
        self.assertTrue(self.engine.delete(b"k"))
        self.assertIsNone(self.engine.get(b"k"))

    def test_overwrite_across_flush(self):
        self.engine.put(b"k", b"old")
        self.engine.flush()              # old value now lives in an SSTable
        self.engine.put(b"k", b"new")    # new value in the memtable
        self.assertEqual(self.engine.get(b"k"), b"new")

    def test_delete_shadows_sstable_value(self):
        self.engine.put(b"k", b"v")
        self.engine.flush()
        self.engine.delete(b"k")         # tombstone in memtable
        self.assertIsNone(self.engine.get(b"k"))
        self.engine.flush()
        self.assertIsNone(self.engine.get(b"k"))

    def test_wal_replay_after_crashless_restart(self):
        self.engine.put(b"k", b"v")      # only in memtable + WAL
        self.engine._wal.flush()
        # Simulate abrupt stop: skip close(), reopen from disk.
        self.engine._wal.close()
        for sst in self.engine._sstables:
            sst.close()
        self.engine = LSMEngine(self.tmp.name, memtable_bytes=8 * 1024)
        self.assertEqual(self.engine.get(b"k"), b"v")

    def test_torn_wal_truncated(self):
        self.engine.put(b"safe", b"v")
        self.engine._wal.flush()
        self.engine._wal.close()
        for sst in self.engine._sstables:
            sst.close()
        with open(Path(self.tmp.name) / "wal.log", "ab") as fh:
            fh.write(b"\x01garbage")
        self.engine = LSMEngine(self.tmp.name, memtable_bytes=8 * 1024)
        self.assertEqual(self.engine.get(b"safe"), b"v")

    def test_many_keys_across_multiple_sstables(self):
        for i in range(1000):
            self.engine.put(f"key{i:05d}".encode(), os.urandom(64))
        self.assertGreater(len(list(Path(self.tmp.name).glob("*.sst"))), 1)
        self.reopen()
        self.assertEqual(len(self.engine), 1000)
        self.assertIsNotNone(self.engine.get(b"key00500"))

    def test_keys_are_sorted(self):
        for name in (b"zebra", b"apple", b"mango"):
            self.engine.put(name, b"x")
        self.engine.flush()
        self.engine.put(b"banana", b"x")
        self.assertEqual(self.engine.keys(),
                         [b"apple", b"banana", b"mango", b"zebra"])

    def test_compaction_drops_garbage(self):
        for _ in range(20):
            self.engine.put(b"hot", os.urandom(512))
        self.engine.put(b"doomed", b"v")
        self.engine.delete(b"doomed")
        stats = self.engine.compact()
        self.assertLessEqual(stats["bytes_after"], stats["bytes_before"])
        self.assertEqual(len(list(Path(self.tmp.name).glob("*.sst"))), 1)
        self.assertIsNotNone(self.engine.get(b"hot"))
        self.assertIsNone(self.engine.get(b"doomed"))

    def test_ttl(self):
        self.engine.put(b"flash", b"v", expiry=int(time.time()) - 1)
        self.engine.put(b"keep", b"v", expiry=int(time.time()) + 60)
        self.assertIsNone(self.engine.get(b"flash"))
        self.assertEqual(self.engine.get(b"keep"), b"v")
        self.assertNotIn(b"flash", self.engine.keys())

    def test_snapshot(self):
        self.engine.put(b"a", b"1")
        self.engine.put(b"b", b"2")
        snap = Path(self.tmp.name) / "snap.bin"
        self.engine.snapshot(snap)
        self.assertGreater(snap.stat().st_size, 0)

    def test_bloom_filter_actually_skips(self):
        for i in range(200):
            self.engine.put(f"present{i}".encode(), b"v")
        self.engine.flush()
        sst = self.engine._sstables[0]
        # An absent key should usually be rejected by the bloom filter
        misses = sum(f"absent{i}".encode() in sst.bloom for i in range(1000))
        self.assertLess(misses, 100)


if __name__ == "__main__":
    unittest.main()
