"""End-to-end tests for transactions (MULTI/EXEC), replication and SAVE."""

import asyncio
import tempfile
import threading
import time
import unittest
from pathlib import Path

from minikv import resp
from minikv.client import Client
from minikv.server import Server
from minikv.storage import StorageEngine

LEADER_PORT = 16601
REPLICA_PORT = 16602


def run_in_thread(server: Server):
    loop = asyncio.new_event_loop()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(0.3)
    return loop, thread


def stop(loop, thread, server):
    asyncio.run_coroutine_threadsafe(server.stop(), loop).result(timeout=2)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=2)


class TestTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.engine = StorageEngine(cls.tmp.name)
        cls.server = Server(cls.engine, port=LEADER_PORT)
        cls.loop, cls.thread = run_in_thread(cls.server)

    @classmethod
    def tearDownClass(cls):
        stop(cls.loop, cls.thread, cls.server)
        cls.engine.close()
        cls.tmp.cleanup()

    def setUp(self):
        self.c = Client(port=LEADER_PORT)
        self.c.execute("FLUSHDB")

    def tearDown(self):
        self.c.close()

    def test_multi_exec(self):
        self.assertEqual(self.c.execute("MULTI"), "OK")
        self.assertEqual(self.c.execute("SET", "a", "1"), "QUEUED")
        self.assertEqual(self.c.execute("INCR", "a"), "QUEUED")
        self.assertEqual(self.c.execute("GET", "a"), "QUEUED")
        results = self.c.execute("EXEC")
        self.assertEqual(results, ["OK", 2, b"2"])
        self.assertEqual(self.c.get("a"), b"2")

    def test_discard(self):
        self.c.execute("MULTI")
        self.c.execute("SET", "ghost", "boo")
        self.assertEqual(self.c.execute("DISCARD"), "OK")
        self.assertIsNone(self.c.get("ghost"))

    def test_exec_without_multi(self):
        reply = self.c.execute("EXEC")
        self.assertIsInstance(reply, resp.RespError)

    def test_nested_multi_rejected(self):
        self.c.execute("MULTI")
        reply = self.c.execute("MULTI")
        self.assertIsInstance(reply, resp.RespError)
        self.c.execute("DISCARD")

    def test_error_inside_exec_does_not_abort_batch(self):
        self.c.execute("MULTI")
        self.c.execute("SET", "n", "notanumber")
        self.c.execute("INCR", "n")        # will error
        self.c.execute("SET", "after", "ok")
        results = self.c.execute("EXEC")
        self.assertEqual(results[0], "OK")
        self.assertIsInstance(results[1], resp.RespError)
        self.assertEqual(results[2], "OK")
        self.assertEqual(self.c.get("after"), b"ok")

    def test_save_snapshot(self):
        self.c.set("snap", "data")
        reply = self.c.execute("SAVE")
        self.assertTrue(str(reply).startswith("OK"))
        snapshots = list(Path(self.tmp.name).glob("snapshot-*.snap"))
        self.assertEqual(len(snapshots), 1)
        self.assertGreater(snapshots[0].stat().st_size, 0)
        snapshots[0].unlink()


class TestReplication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ltmp = tempfile.TemporaryDirectory()
        cls.rtmp = tempfile.TemporaryDirectory()
        cls.leader_engine = StorageEngine(cls.ltmp.name)
        cls.replica_engine = StorageEngine(cls.rtmp.name)

        # Seed the leader BEFORE the replica connects, to prove full-sync.
        cls.leader_engine.put(b"seeded", b"before-replica-existed")

        cls.leader = Server(cls.leader_engine, port=LEADER_PORT + 10)
        cls.lloop, cls.lthread = run_in_thread(cls.leader)

        cls.replica = Server(cls.replica_engine, port=REPLICA_PORT + 10,
                             replicaof=("127.0.0.1", LEADER_PORT + 10))
        cls.rloop, cls.rthread = run_in_thread(cls.replica)
        time.sleep(0.5)  # allow full sync to complete

    @classmethod
    def tearDownClass(cls):
        stop(cls.rloop, cls.rthread, cls.replica)
        stop(cls.lloop, cls.lthread, cls.leader)
        cls.leader_engine.close()
        cls.replica_engine.close()
        cls.ltmp.cleanup()
        cls.rtmp.cleanup()

    def test_full_sync_copies_existing_data(self):
        with Client(port=REPLICA_PORT + 10) as c:
            self.assertEqual(c.get("seeded"), b"before-replica-existed")

    def test_live_writes_stream_to_replica(self):
        with Client(port=LEADER_PORT + 10) as leader:
            leader.set("live", "streamed")
            leader.execute("INCR", "counter")
            leader.execute("INCR", "counter")
        deadline = time.time() + 3
        while time.time() < deadline:
            with Client(port=REPLICA_PORT + 10) as replica:
                if replica.get("live") == b"streamed":
                    self.assertEqual(replica.get("counter"), b"2")
                    return
            time.sleep(0.1)
        self.fail("write never reached the replica")

    def test_deletes_replicate(self):
        with Client(port=LEADER_PORT + 10) as leader:
            leader.set("temp", "x")
            leader.delete("temp")
        deadline = time.time() + 3
        while time.time() < deadline:
            with Client(port=REPLICA_PORT + 10) as replica:
                if replica.get("temp") is None:
                    return
            time.sleep(0.1)
        self.fail("delete never reached the replica")

    def test_replica_reports_role(self):
        with Client(port=REPLICA_PORT + 10) as c:
            info = c.execute("INFO").decode()
        self.assertIn("role:replica", info)


if __name__ == "__main__":
    unittest.main()
