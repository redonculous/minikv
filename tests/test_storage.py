import os
import tempfile
import threading
import time
import unittest
from pathlib import Path

from minikv.storage import StorageEngine, TOMBSTONE


class StorageTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.engine = StorageEngine(self.tmp.name)

    def tearDown(self):
        self.engine.close()
        self.tmp.cleanup()

    def reopen(self):
        """Simulate a process restart."""
        self.engine.close()
        self.engine = StorageEngine(self.tmp.name)


class TestBasicOperations(StorageTestCase):
    def test_put_get(self):
        self.engine.put(b"name", b"ada")
        self.assertEqual(self.engine.get(b"name"), b"ada")

    def test_get_missing_returns_none(self):
        self.assertIsNone(self.engine.get(b"ghost"))

    def test_overwrite_returns_latest(self):
        self.engine.put(b"k", b"v1")
        self.engine.put(b"k", b"v2")
        self.assertEqual(self.engine.get(b"k"), b"v2")

    def test_delete(self):
        self.engine.put(b"k", b"v")
        self.assertTrue(self.engine.delete(b"k"))
        self.assertIsNone(self.engine.get(b"k"))
        self.assertFalse(self.engine.delete(b"k"))

    def test_empty_value(self):
        self.engine.put(b"empty", b"")
        self.assertEqual(self.engine.get(b"empty"), b"")

    def test_binary_safe(self):
        key = bytes(range(256))
        value = os.urandom(4096)
        self.engine.put(key, value)
        self.assertEqual(self.engine.get(key), value)

    def test_keys_and_len(self):
        for i in range(5):
            self.engine.put(f"k{i}".encode(), b"v")
        self.engine.delete(b"k0")
        self.assertEqual(len(self.engine), 4)
        self.assertNotIn(b"k0", self.engine.keys())

    def test_items_iteration(self):
        data = {f"k{i}".encode(): f"v{i}".encode() for i in range(10)}
        for k, v in data.items():
            self.engine.put(k, v)
        self.assertEqual(dict(self.engine.items()), data)


class TestPersistence(StorageTestCase):
    def test_survives_restart(self):
        self.engine.put(b"persistent", b"yes")
        self.reopen()
        self.assertEqual(self.engine.get(b"persistent"), b"yes")

    def test_latest_value_wins_after_restart(self):
        self.engine.put(b"k", b"old")
        self.engine.put(b"k", b"new")
        self.reopen()
        self.assertEqual(self.engine.get(b"k"), b"new")

    def test_deletes_survive_restart(self):
        self.engine.put(b"k", b"v")
        self.engine.delete(b"k")
        self.reopen()
        self.assertIsNone(self.engine.get(b"k"))

    def test_recovery_across_many_files(self):
        self.engine.close()
        self.engine = StorageEngine(self.tmp.name, max_file_bytes=512)
        for i in range(200):
            self.engine.put(f"key{i}".encode(), f"value{i}".encode())
        self.assertGreater(len(list(Path(self.tmp.name).glob("*.mkv"))), 1)
        self.reopen()
        for i in range(200):
            self.assertEqual(self.engine.get(f"key{i}".encode()),
                             f"value{i}".encode())


class TestCrashRecovery(StorageTestCase):
    def test_torn_write_is_truncated(self):
        """A partial record at the tail (crash mid-write) must be dropped
        without losing the records before it."""
        self.engine.put(b"safe", b"data")
        self.engine.close()

        files = sorted(Path(self.tmp.name).glob("*.mkv"))
        with open(files[-1], "ab") as fh:
            fh.write(b"\x00\x01garbage-partial-record")

        self.engine = StorageEngine(self.tmp.name)
        self.assertEqual(self.engine.get(b"safe"), b"data")
        # Engine must still accept writes after truncating the bad tail.
        self.engine.put(b"after", b"crash")
        self.reopen()
        self.assertEqual(self.engine.get(b"after"), b"crash")

    def test_bitflip_detected_by_crc(self):
        self.engine.put(b"k", b"important")
        self.engine.close()

        files = sorted(Path(self.tmp.name).glob("*.mkv"))
        data = bytearray(files[-1].read_bytes())
        data[len(data) // 2] ^= 0xFF  # flip a bit mid-record
        files[-1].write_bytes(data)

        self.engine = StorageEngine(self.tmp.name)
        # The corrupted record is dropped rather than served as garbage.
        self.assertIsNone(self.engine.get(b"k"))


class TestCompaction(StorageTestCase):
    def test_compaction_reclaims_space(self):
        self.engine.close()
        self.engine = StorageEngine(self.tmp.name, max_file_bytes=1024)
        for _ in range(50):
            self.engine.put(b"hot-key", os.urandom(256))
        stats = self.engine.compact()
        self.assertLess(stats["bytes_after"], stats["bytes_before"])
        self.assertEqual(self.engine.get(b"hot-key")[:0], b"")  # readable

    def test_data_intact_after_compaction(self):
        for i in range(100):
            self.engine.put(f"k{i}".encode(), f"v{i}".encode())
        for i in range(0, 100, 2):
            self.engine.delete(f"k{i}".encode())
        self.engine.compact()
        for i in range(100):
            expected = None if i % 2 == 0 else f"v{i}".encode()
            self.assertEqual(self.engine.get(f"k{i}".encode()), expected)

    def test_compaction_then_restart(self):
        for i in range(50):
            self.engine.put(f"k{i}".encode(), b"x" * 100)
        self.engine.compact()
        self.reopen()
        self.assertEqual(len(self.engine), 50)

    def test_tombstones_purged(self):
        self.engine.put(b"doomed", b"v")
        self.engine.delete(b"doomed")
        self.engine.compact()
        size = sum(p.stat().st_size for p in Path(self.tmp.name).glob("*.mkv"))
        self.assertEqual(size, 0)


class TestTTL(StorageTestCase):
    def test_expired_key_is_gone(self):
        self.engine.put(b"flash", b"v", expiry=int(time.time()) - 1)
        self.assertIsNone(self.engine.get(b"flash"))
        self.assertNotIn(b"flash", self.engine.keys())

    def test_future_expiry_still_readable(self):
        self.engine.put(b"k", b"v", expiry=int(time.time()) + 60)
        self.assertEqual(self.engine.get(b"k"), b"v")

    def test_expired_keys_dropped_at_compaction(self):
        self.engine.put(b"old", b"v", expiry=int(time.time()) - 1)
        self.engine.put(b"keep", b"v")
        self.engine.compact()
        self.reopen()
        self.assertIsNone(self.engine.get(b"old"))
        self.assertEqual(self.engine.get(b"keep"), b"v")


class TestConcurrency(StorageTestCase):
    def test_parallel_writers_distinct_keys(self):
        errors = []

        def worker(worker_id: int):
            try:
                for i in range(100):
                    key = f"w{worker_id}-{i}".encode()
                    self.engine.put(key, key[::-1])
                    if self.engine.get(key) != key[::-1]:
                        errors.append(key)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(self.engine), 800)


if __name__ == "__main__":
    unittest.main()
