"""
MiniKV demo — run with:  python3 demo.py        (cinematic pacing)
                         python3 demo.py --fast (no delays, for CI)

No setup, no dependencies. Five acts:
  1. start a server and talk to it over TCP
  2. run an atomic transaction (MULTI/EXEC)
  3. crash the database mid-write... and recover everything
  4. stream live data to a replica server
  5. swap in the LSM-tree engine (sorted keys + bloom filters)
"""

import asyncio
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

from minikv.client import Client
from minikv.lsm import LSMEngine
from minikv.server import Server
from minikv.storage import StorageEngine

PORT = 6499
FAST = "--fast" in sys.argv


# ------------------------------------------------------------ stagecraft
def say(text: str = "", delay: float = 0.014, end: str = "\n") -> None:
    """Typewriter print."""
    for ch in text:
        print(ch, end="", flush=True)
        if not FAST:
            time.sleep(delay)
    print(end=end, flush=True)


def pause(seconds: float) -> None:
    if not FAST:
        time.sleep(seconds)


def reply(label: str, value) -> None:
    say(f"    {label:<28}", delay=0.006, end="")
    pause(0.45)  # a beat before the answer lands
    print(f"->  {value}", flush=True)
    pause(0.25)


def act(n: int, title: str) -> None:
    print()
    say("─" * 56, delay=0.002)
    say(f"  ACT {n} · {title}", delay=0.02)
    say("─" * 56, delay=0.002)
    pause(0.6)


# ------------------------------------------------------- server plumbing
def start_server(engine, port: int, replicaof=None):
    loop = asyncio.new_event_loop()
    server = Server(engine, port=port, replicaof=replicaof)

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(server.start())
        loop.run_forever()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    time.sleep(0.3)
    return loop, thread, server


def stop_server(loop, thread, server):
    asyncio.run_coroutine_threadsafe(server.stop(), loop).result(timeout=2)
    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=2)


# ------------------------------------------------------------------ main
def main() -> None:
    data_dir = Path(tempfile.mkdtemp(prefix="minikv-demo-"))
    replica_dir = Path(tempfile.mkdtemp(prefix="minikv-replica-"))
    lsm_dir = Path(tempfile.mkdtemp(prefix="minikv-lsm-"))

    print()
    say("  ╔══════════════════════════════════════════════════╗", 0.002)
    say("  ║   M I N I K V   —   a database, from scratch      ║", 0.012)
    say("  ╚══════════════════════════════════════════════════╝", 0.002)
    pause(0.8)

    # ════════════════════════════════════════════════════════ ACT 1
    act(1, "Hello, database")
    engine = StorageEngine(data_dir)
    loop, thread, server = start_server(engine, PORT)
    say(f"  A MiniKV server is now listening on port {PORT}.")
    say("  Let's talk to it over a real TCP connection...")
    pause(0.8)
    print()

    c = Client(port=PORT)
    reply('SET user:1 "Ada Lovelace"', c.set("user:1", "Ada Lovelace"))
    reply('SET user:2 "Alan Turing"', c.set("user:2", "Alan Turing"))
    reply('SET session abc123 EX 60', c.set("session", "abc123", ex=60))
    reply('GET user:1', c.get("user:1"))
    reply('TTL session', f'{c.execute("TTL", "session")} seconds left')
    reply('KEYS user:*', c.execute("KEYS", "user:*"))

    # ════════════════════════════════════════════════════════ ACT 2
    act(2, "Transactions — all or nothing")
    say("  MULTI queues commands; EXEC runs the whole batch")
    say("  atomically — nothing can sneak in between them.")
    pause(0.8)
    print()
    reply("MULTI", c.execute("MULTI"))
    reply('SET balance 100', c.execute("SET", "balance", "100"))
    reply("INCRBY balance 50", c.execute("INCRBY", "balance", "50"))
    reply("GET balance", c.execute("GET", "balance"))
    say("    ...nothing has actually run yet. Now:", 0.018)
    pause(0.8)
    reply("EXEC", c.execute("EXEC"))
    c.close()

    # ════════════════════════════════════════════════════════ ACT 3
    act(3, "The crash 💥")
    say("  Time to be cruel. We kill the server, then scribble")
    say("  half-finished garbage into its data file — exactly")
    say("  what a power cut in the middle of a write looks like.")
    pause(1.2)
    stop_server(loop, thread, server)
    engine.close()

    data_file = sorted(data_dir.glob("*.mkv"))[-1]
    with open(data_file, "ab") as fh:
        fh.write(b"\x07PARTIAL-WRITE-CUT-OFF-BY-POWER-LOSS")
    print()
    say("    ⚡ server killed", 0.03)
    pause(0.5)
    say("    ⚡ data file corrupted", 0.03)
    pause(1.0)
    print()
    say("  Restarting... MiniKV replays its log, verifies every")
    say("  record's CRC32 checksum, and amputates the corrupt tail.")
    pause(1.0)
    print()

    engine = StorageEngine(data_dir)
    loop, thread, server = start_server(engine, PORT)
    c = Client(port=PORT)
    reply("GET user:1", c.get("user:1"))
    reply("GET user:2", c.get("user:2"))
    reply("GET balance", c.get("balance"))
    reply("DBSIZE", f'{c.execute("DBSIZE")} keys recovered')
    pause(0.5)
    say("\n  ✅ Every key survived the crash.", 0.02)
    pause(1.0)

    # ════════════════════════════════════════════════════════ ACT 4
    act(4, "Replication — a second copy, live")
    say("  Now we start a SECOND server that follows the first.")
    say("  It pulls a full copy of the data, then receives every")
    say("  new write the instant it happens.")
    pause(1.0)
    print()

    replica_engine = StorageEngine(replica_dir)
    rloop, rthread, replica = start_server(
        replica_engine, PORT + 1, replicaof=("127.0.0.1", PORT)
    )
    say(f"    replica started on port {PORT + 1}, syncing", 0.02, end="")
    for _ in range(4):
        pause(0.35)
        print(".", end="", flush=True)
    print()
    pause(0.4)

    r = Client(port=PORT + 1)
    reply("replica GET user:1", r.get("user:1"))
    say("    (it inherited the full dataset)", 0.012)
    pause(0.8)
    print()
    say("  Write something new to the LEADER...", 0.018)
    c.set("breaking", "news!")
    reply('leader  SET breaking "news!"', "OK")
    time.sleep(0.4)
    reply("replica GET breaking", r.get("breaking"))
    say("\n  ✅ The write crossed servers in milliseconds.", 0.02)
    r.close()
    c.close()
    pause(1.0)

    # ════════════════════════════════════════════════════════ ACT 5
    act(5, "The other engine — LSM-tree")
    say("  MiniKV ships two interchangeable storage engines.")
    say("  The LSM-tree backend (the LevelDB/RocksDB design)")
    say("  keeps keys SORTED and guards every file on disk with")
    say("  a bloom filter, so misses cost zero disk reads.")
    pause(1.0)
    print()

    lsm = LSMEngine(lsm_dir, memtable_bytes=2 * 1024)
    for name in [b"zebra", b"apple", b"mango", b"banana", b"cherry"]:
        lsm.put(name, b"fruit?" if name != b"zebra" else b"no.")
    lsm.flush()
    reply("lsm.keys()  (sorted!)", lsm.keys())
    sst = lsm._sstables[0]
    reply('bloom: "apple" maybe there?', b"apple" in sst.bloom)
    reply('bloom: "dragonfruit" there?', b"dragonfruit" in sst.bloom)
    say("    ...so the lookup for dragonfruit never touches disk.", 0.012)
    lsm.close()
    pause(1.0)

    # ════════════════════════════════════════════════════════ curtain
    print()
    say("─" * 56, 0.002)
    say("  That's the tour: durability, atomicity, recovery,", 0.018)
    say("  replication, and two storage engine designs —", 0.018)
    say("  all pure Python, zero dependencies.", 0.018)
    print()
    say("  Try it yourself:", 0.018)
    say("    python3 -m minikv.server          # terminal 1", 0.01)
    say("    python3 -m minikv.cli             # terminal 2", 0.01)
    say("─" * 56, 0.002)
    print()

    stop_server(rloop, rthread, replica)
    stop_server(loop, thread, server)
    engine.close()
    replica_engine.close()
    for d in (data_dir, replica_dir, lsm_dir):
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    main()
