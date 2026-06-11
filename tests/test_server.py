"""End-to-end tests: real asyncio server, real TCP socket, real client."""

import asyncio
import tempfile
import threading
import time
import unittest

from minikv import resp
from minikv.client import Client
from minikv.server import Server
from minikv.storage import StorageEngine

PORT = 16479


class ServerTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.engine = StorageEngine(cls.tmp.name)
        cls.loop = asyncio.new_event_loop()
        cls.server = Server(cls.engine, port=PORT)
        cls.thread = threading.Thread(target=cls._run, daemon=True)
        cls.thread.start()
        time.sleep(0.3)  # allow the listener to bind

    @classmethod
    def _run(cls):
        asyncio.set_event_loop(cls.loop)
        cls.loop.run_until_complete(cls.server.start())
        cls.loop.run_forever()

    @classmethod
    def tearDownClass(cls):
        cls.loop.call_soon_threadsafe(cls.loop.stop)
        cls.thread.join(timeout=2)
        cls.engine.close()
        cls.tmp.cleanup()

    def setUp(self):
        self.client = Client(port=PORT)
        self.client.execute("FLUSHDB")

    def tearDown(self):
        self.client.close()

    # ------------------------------------------------------------- tests
    def test_ping(self):
        self.assertEqual(self.client.execute("PING"), "PONG")
        self.assertEqual(self.client.execute("PING", "hi"), b"hi")

    def test_set_get_del(self):
        self.assertEqual(self.client.set("k", "v"), "OK")
        self.assertEqual(self.client.get("k"), b"v")
        self.assertEqual(self.client.delete("k"), 1)
        self.assertIsNone(self.client.get("k"))

    def test_set_nx_xx(self):
        self.assertEqual(self.client.execute("SET", "k", "a", "NX"), "OK")
        self.assertIsNone(self.client.execute("SET", "k", "b", "NX"))
        self.assertEqual(self.client.get("k"), b"a")
        self.assertEqual(self.client.execute("SET", "k", "c", "XX"), "OK")
        self.assertIsNone(self.client.execute("SET", "nope", "x", "XX"))

    def test_ttl_lifecycle(self):
        self.client.set("k", "v", ex=100)
        ttl = self.client.execute("TTL", "k")
        self.assertTrue(0 < ttl <= 100)
        self.assertEqual(self.client.execute("PERSIST", "k"), 1)
        self.assertEqual(self.client.execute("TTL", "k"), -1)
        self.assertEqual(self.client.execute("TTL", "missing"), -2)

    def test_expired_key_unreadable(self):
        self.client.set("flash", "v", ex=1)
        time.sleep(1.2)
        self.assertIsNone(self.client.get("flash"))

    def test_incr_decr(self):
        self.assertEqual(self.client.execute("INCR", "n"), 1)
        self.assertEqual(self.client.execute("INCRBY", "n", 10), 11)
        self.assertEqual(self.client.execute("DECR", "n"), 10)
        self.client.set("s", "not-a-number")
        reply = self.client.execute("INCR", "s")
        self.assertIsInstance(reply, resp.RespError)

    def test_append_strlen(self):
        self.assertEqual(self.client.execute("APPEND", "k", "foo"), 3)
        self.assertEqual(self.client.execute("APPEND", "k", "bar"), 6)
        self.assertEqual(self.client.execute("STRLEN", "k"), 6)

    def test_keys_glob(self):
        for name in ("user:1", "user:2", "session:9"):
            self.client.set(name, "x")
        keys = self.client.execute("KEYS", "user:*")
        self.assertEqual(sorted(keys), [b"user:1", b"user:2"])

    def test_exists_multi(self):
        self.client.set("a", "1")
        self.client.set("b", "1")
        self.assertEqual(self.client.execute("EXISTS", "a", "b", "ghost"), 2)

    def test_dbsize_flushdb(self):
        self.client.set("x", "1")
        self.assertEqual(self.client.execute("DBSIZE"), 1)
        self.assertEqual(self.client.execute("FLUSHDB"), "OK")
        self.assertEqual(self.client.execute("DBSIZE"), 0)

    def test_unknown_command(self):
        reply = self.client.execute("BLORP")
        self.assertIsInstance(reply, resp.RespError)
        self.assertIn("unknown command", reply.message)

    def test_compact_over_network(self):
        for _ in range(20):
            self.client.set("hot", "x" * 500)
        reply = self.client.execute("COMPACT")
        self.assertTrue(str(reply).startswith("OK"))
        self.assertEqual(self.client.get("hot"), b"x" * 500)

    def test_concurrent_clients(self):
        errors = []

        def hammer(n: int):
            try:
                with Client(port=PORT) as c:
                    for i in range(50):
                        key = f"c{n}-{i}"
                        c.set(key, key)
                        if c.get(key) != key.encode():
                            errors.append(key)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=hammer, args=(n,)) for n in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(self.client.execute("DBSIZE"), 250)


if __name__ == "__main__":
    unittest.main()
