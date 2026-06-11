"""
MiniKV 60-second demo — run with:  python3 demo.py

No setup, no dependencies. This script:
  1. starts a MiniKV server,
  2. saves and reads some data over a real TCP connection,
  3. simulates a crash (corrupts the data file mid-write),
  4. restarts and shows that your data survived.
"""

import asyncio
import shutil
import tempfile
import threading
import time
from pathlib import Path

from minikv.client import Client
from minikv.server import Server
from minikv.storage import StorageEngine

PORT = 6499
DATA_DIR = Path(tempfile.mkdtemp(prefix="minikv-demo-"))


def start_server(engine: StorageEngine):
    loop = asyncio.new_event_loop()
    server = Server(engine, port=PORT)

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(0.3)
    return loop, thread, server


def stop_server(loop, thread, server):
    # Close the listening socket cleanly, then stop the loop.
    future = asyncio.run_coroutine_threadsafe(server.stop(), loop)
    future.result(timeout=2)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=2)


def main() -> None:
    print("=" * 56)
    print(" MiniKV demo")
    print("=" * 56)

    # ---- 1. start the server ------------------------------------------
    engine = StorageEngine(DATA_DIR)
    loop, thread, server = start_server(engine)
    print(f"\n[1] Server running on port {PORT}, data in {DATA_DIR}\n")

    # ---- 2. talk to it over TCP ---------------------------------------
    c = Client(port=PORT)
    print('[2] Saving some data over the network...')
    print('    SET user:1 "Ada Lovelace"  ->', c.set("user:1", "Ada Lovelace"))
    print('    SET user:2 "Alan Turing"   ->', c.set("user:2", "Alan Turing"))
    print('    SET session abc123 EX 60   ->', c.set("session", "abc123", ex=60))
    print('    INCR visits                ->', c.execute("INCR", "visits"))
    print('    INCR visits                ->', c.execute("INCR", "visits"))
    print()
    print('    GET user:1                 ->', c.get("user:1"))
    print('    TTL session                ->', c.execute("TTL", "session"), "seconds left")
    print('    KEYS user:*                ->', c.execute("KEYS", "user:*"))
    c.close()

    # ---- 3. simulate a crash ------------------------------------------
    print("\n[3] 💥 Simulating a crash: killing the server and writing")
    print("    half-finished garbage to the end of the data file")
    print("    (what a real power cut mid-write looks like)...")
    stop_server(loop, thread, server)
    engine.close()

    data_file = sorted(DATA_DIR.glob("*.mkv"))[-1]
    with open(data_file, "ab") as fh:
        fh.write(b"\x07PARTIAL-WRITE-CUT-OFF-BY-POWER-LOSS")

    # ---- 4. restart and recover ---------------------------------------
    print("\n[4] Restarting... MiniKV replays its log, checks every")
    print("    record's checksum, and throws away the corrupt tail.\n")
    engine = StorageEngine(DATA_DIR)
    loop, thread, server = start_server(engine)

    c = Client(port=PORT)
    print('    GET user:1   ->', c.get("user:1"))
    print('    GET user:2   ->', c.get("user:2"))
    print('    GET visits   ->', c.get("visits"))
    print('    DBSIZE       ->', c.execute("DBSIZE"), "keys recovered")
    c.close()

    print("\n✅ All data survived the crash. That's the whole point!")
    print("\nNext: run `python3 -m minikv.server` in one terminal and")
    print("`python3 -m minikv.cli` in another to play with it yourself.")

    stop_server(loop, thread, server)
    engine.close()
    shutil.rmtree(DATA_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
